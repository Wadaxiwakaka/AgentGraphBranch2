# Step 1a：上下文字符预算与配对安全裁剪

> 文档日期：2026-08-05
>
> 前置阅读：[00-gap-analysis.md](00-gap-analysis.md) 第 4、5 节。
>
> 状态：已实现并验收。实现顺序：先写 RED 测试，再最小实现，全量回归后
> 单独 commit。验收记录见文末第 11 节。

## 1. 背景与问题（代码事实）

`ChatSpace`（`core.py`）维护双视图：

- `messages`：面向人类/前端的 `user/assistant` 文本，`/history` 与归档用它；
- `context_items`：面向 Responses API 的完整回放视图（message、reasoning、
  function_call、function_call_output）。

三个写入点单调追加，从不删减：

| 方法 | 触发场景 | 写入内容 |
| --- | --- | --- |
| `add_msg()` | 收到 user 消息 / root 收到回复 | 双视图各一条 |
| `append_response_items()` | 模型每步返回 output | 完整 item 进 `context_items`，文本另进 `messages` |
| `append_tool_output()` | 工具执行完成 | `function_call_output` 进 `context_items` |

`_run_response_loop`（`AgentRemote.py`）的每一步都把完整
`chat.get_context_messages()` 作为 `input` 传给 `responses.create`。因此：

1. 长对话：`context_items` 线性增长，最终超过模型上下文窗口；
2. 深工具循环：单轮最多 200 次工具调用、256 步，每次输出全部留存，
   一轮之内就可能超窗；
3. 超窗后模型请求失败，`MODEL_ERROR`，会话不可继续。

**这是正确性缺口，且 Step 2（Skill 注入指令）与 Step 3（知识库注入检索结果）
都会放大它，所以排在第一。**

## 2. 目标

- 给普通 Agent 的会话上下文设一个可配置的字符预算，超限时裁掉最旧内容，
  使长会话与深工具循环不会超过模型窗口；
- 裁剪永远保持 HANDOFF 5.2 红线：`context_items` 中每个 `function_call`
  恰好有一个同 `call_id` 的 `function_call_output`，反之亦然；
- 未配置预算的 Agent 行为与现在完全一致（零变化，既有测试不改动即通过）。

## 3. 非目标

- 不按 token 精确计量：不引入 tokenizer 依赖，用序列化字符数近似
  （见第 6 节"已知简化"）；
- 不做摘要压缩（compact）：被裁内容直接丢弃，摘要替代留给 Step 1b；
- 不保留"首条任务消息永不裁"等特殊规则：一律从最旧开始裁；
- 不改动 `messages` 人类视图与归档语义：`/history` 与 `save()` 中可读历史
  仍是完整的（`save()` 保存的 `context_items` 反映裁剪后状态，见第 7 节）；
- 不改 root/`User`、HTTP 契约、工具系统。

## 4. 配置设计

`AgentConfig`（`core.py`）新增可选字段，命名与校验风格对齐现有
`max_tool_calls_per_turn` 等字段：

```python
max_context_chars: int | None = Field(default=None, gt=0)
```

- 默认 `None`：不裁剪，保持现状（既有配置与测试零影响）；
- 设为正整数：启用裁剪，预算作用于单个 `ChatSpace.context_items` 的
  序列化字符总量；
- root 配置中出现该字段时与其它 Agent 专属字段同样处理（root 不调模型，
  该字段无意义；沿用现有 root 字段校验策略，见实现时对照 `_validate_relationships`）。

## 5. 裁剪算法

### 5.1 计量单位

预算按 `json.dumps(item, ensure_ascii=False)` 的字符数逐 item 累计。
这是 token 数的廉价近似：不引依赖、确定性、可测试。

### 5.2 裁剪单元（trim unit）

`context_items` 被划分为有序单元序列，**单元是不可分割的最小裁剪单位**：

| item 类型 | 单元 |
| --- | --- |
| `function_call`（call_id=X） | 该 item + 其对应 `function_call_output`（同 call_id），无论两者在列表中相距多远 |
| `function_call_output` | 并入其 call 的单元（不单独成单元） |
| 其它（message / reasoning / 未来新类型） | 单 item 一个单元 |

配对保证因此是结构性的：裁剪只能整单元移除，call 与 output 同生共死，
不可能出现孤儿。未配对的 output（理论不应存在，见 5.4）视为独立单元处理，
不抛异常。

### 5.3 算法（`ChatSpace` 新方法）

```
trim_context(max_chars: int | None) -> int
    返回被移除的单元数；max_chars 为 None 时直接返回 0。

1. 若 max_chars 为 None：返回 0（不裁剪）。
2. 计算各单元及总字符数（json.dumps 累计）。
3. 若总数 <= max_chars：返回 0。
4. 从最旧单元开始整单元移除，直到总数 <= max_chars 或只剩最后一个单元。
5. 永远保留最后一个（最新）单元：宁可超预算也不清空上下文。
6. 原地重建 self.context_items（保持剩余 item 的相对顺序）。
```

### 5.4 调用点与时机

`_run_response_loop` 每轮 `while` 迭代顶部、构造 `create_arguments` 之前调用：

```python
chat.trim_context(self.config.max_context_chars)
```

为什么这个时机安全：工具循环内所有 `function_call` 都会在下一步迭代前写完
配对 output（含 TOOL_CALL_LIMIT_EXCEEDED 拒绝路径与取消路径，均通过
`_append_tool_results_atomic` / 批次回滚保证无孤儿）。因此在迭代顶部看到的
`context_items` 必然配对完整，裁剪不会切到"调用已入上下文、结果还没回来"的
中间态。

每次 talk 进入循环前同样生效（第一次迭代顶部就会执行）。root/`User` 不调用
模型，不引入裁剪。

### 5.5 与 `_preflight_function_calls` 的交互

该预检用现有 `context_items` 中的 call_id 集合查重。裁剪只会从集合中移除
call_id，不会制造重复；被裁掉的 call_id 若被模型再次发出，会通过查重并正常
执行——这是可接受语义（新调用有新 output 配对，不破坏一对一不变量）。

## 6. 已知简化（故意为之）

- **字符数 ≠ token 数**：按经验 1 token ≈ 3~4 字符，配置时需自行留余量。
  换 tokenizer 是 Step 1b 之后的事（`# ponytail:` 注释会标注此处）。
- **裁剪不看内容语义**：被裁的可能恰是任务目标描述。模型仍可从后续对话推断；
  若实际使用中发现丢任务，Step 1b 的摘要方案解决。
- **每步迭代重算全部序列化长度**：O(n) 每步，MVP 规模（数百 item）无感。
  会话超大时再增量缓存。

## 7. 持久化影响

`save()` 归档反映裁剪后的 `context_items` 与完整的 `messages`。即：归档文件中
模型可回放上下文是裁剪后的，人类可读历史是完整的。两者本来就不是一一对应
（reasoning 等 item 从不进入 `messages`），此差异符合双视图设计初衷。

## 8. 测试计划（先 RED）

新增到 `tests/test_chat_space.py`：

1. `trim_context(None)` 返回 0 且不改 `context_items`；
2. 未超预算时返回 0、顺序不变；
3. 超预算时从最旧 message 开始裁，最新 item 保留；
4. **配对安全**：构造含多组 call/output（含 call 与 output 中间隔着其它 item）
   的上下文，裁剪后断言每个保留的 `function_call` 恰有一个同 call_id 的
   output，且被裁的 call 与 output 一起消失；
5. 单个超大单元独占上下文时不清空（保留最后一个单元）；
6. `messages` 视图与 `instructions`、`tools` 不受裁剪影响；
7. 裁剪后 `save()` 成功且归档 `context_items` 与内存一致。

新增到 `tests/test_config.py`：

8. `max_context_chars` 接受正整数、拒绝 0/负数/非法类型；默认为 `None`。

新增到 `tests/test_agent_remote.py`（用既有 fake Responses 状态机）：

9. 配置小预算 + 多轮工具调用：循环全程上下文不超过预算（在 fake 模型侧
   断言每次收到的 `input` 长度），且会话最终正常返回文本；
10. 裁剪发生时调用 id 查重仍工作（模型重复发已裁 call_id 不报错）。

## 9. 验收标准

- 全部新测试 GREEN；完整离线测试（`-m "not live"`）原有测试零修改通过；
- `max_context_chars` 未配置的 Agent 行为与实现前逐字节一致；
- 实现 diff 涉及：`core.py`（配置字段 + `ChatSpace.trim_context`）、
  `AgentRemote.py`（循环顶部一行调用）、两个测试文件，无其它文件改动。

## 10. 开放问题

无。若实现中发现 root 校验策略对 `None` 默认值有歧义，按"root 视为未配置"
处理并在 commit message 中记录。

实际处理：root 不调用模型，`User` 从不读取该字段，无需额外校验。

## 11. 实现与验收记录（2026-08-05）

- 13 个新测试先 RED 后 GREEN：`tests/test_chat_space.py` 8 个、
  `tests/test_config.py` 2 个（拒绝用例参数化展开为 2 项，合计 3 项）、
  `tests/test_agent_remote.py` 2 个。
- 实现落在：`core.py`（`AgentConfig.max_context_chars` 字段、
  `_ContextTrimUnit`、`ChatSpace._build_trim_units()`、
  `ChatSpace.trim_context()`）、`AgentRemote.py`（循环迭代顶部一行调用）。
- 与本设计的偏差：裁剪单元记录的是 item **下标**而非内容引用，重建时按下标
  过滤原列表——因为 call 与其 output 之间可能夹着其它 item，直接按单元展平
  会改变剩余 item 的相对顺序（RED 阶段实测发现，已在测试 4 覆盖）。
- 测试阶段两处测试自身修正：fixture 改经 `append_response_items` 维护双视图；
  reissue 测试的终态预期改为只剩最新配对单元（预算算术上不可能同时保留两对，
  从最旧裁剪的设计行为即如此）。
- 全量离线回归：`1171 passed, 1 deselected`；唯一失败
  `tests/test_integration_chain.py` 的示例配置契约断言
  （`openai_baseurl` 期望 `:20128`，基线提交中的 `agents_setting/Agent1.json`
  为 `:1234`）。经 stash 复跑验证：**干净基线同样失败，与本次改动无关**。
  端口差异原因（2026-08-24 使用者说明）：`20128` 是原维护者（师兄）机器的
  模型服务端口，`1234` 是当前使用者本机端口，属本机运行需要的本地修改。
  处置选项：跑验收时按 HANDOFF 建议改用 clean HEAD 配置副本，或保持本地
  失败为已知状态；测试期望以 README 文档的 `20128` 为仓库契约值，改动需
  使用者自行决定。
- `compileall` 通过；README 配置表已补充 `max_context_chars` 行。

## 12. 完成范围与待完善清单（2026-08-24 更新）

### 已完成（Step 1a）

- 配置项 `max_context_chars`（默认 `None` = 不启用，存量 Agent 行为零变化）；
- `ChatSpace.trim_context()`：最旧优先、`function_call`/`function_call_output`
  按 call_id 绑定为不可分割单元、保持剩余 item 相对顺序、永不清空；
- `_run_response_loop` 每次迭代顶部接入裁剪（含拒绝/取消路径的安全性论证，
  见 §5.4）；
- 13 个专项测试 + 全量回归 + README 配置行。

### 未完成 / 待完善（按优先级）

| 项 | 现状 | 记录位置 |
| --- | --- | --- |
| Step 1b 摘要压缩（compact） | 被裁内容直接丢弃，无摘要替代 | 本文 §3 非目标；[00 路线图](00-gap-analysis.md) Step 1b |
| token 精确计量 | 字符数近似（约 3~4 字符/token），配置需自行留余量 | 本文 §6；`core.py` 内 `ponytail:` 注释 |
| 语义感知裁剪 | 不看内容，可能裁掉任务目标描述 | 本文 §6 |
| 增量长度缓存 | 每步迭代 O(n) 全量重算 | 本文 §6；`core.py` 内 `ponytail:` 注释 |
| `instructions`/`tools` 不计入预算 | instructions 含 peer metadata JSON，Step 2 Skill 注入后会亚长 | 本节（本次补录） |
| `messages` 可读视图不受预算约束 | 只影响内存与归档体积，不影响模型调用量 | 本节（本次补录） |

其中后两项是验收复盘时发现的原设计盲区，升级时机：Skill（Step 2）落地后
若 instructions 总长显著增长，再把 instructions 纳入预算或做压缩。
