"""search_knowledge 扩展工具的契约测试。

覆盖 docs/plans/03-knowledge-base-design.md 第 7 节与第 10 节第 6-12 项：
schema 严格性、索引缺失时优雅隐藏、坏索引启动拒绝、top-3 排序、不可信数据
框架、查询维度不符转 TOOL_EXECUTION_ERROR、客户端生命周期。

embeddings 客户端全部使用 fake 注入（monkeypatch 模块级 AsyncOpenAI 工厂），
不产生任何真实网络调用；fake 记录调用参数供断言。
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import SecretStr

import ToolExtension.search_knowledge as search_knowledge_module
from ToolExtension import EXTENSION_TOOLS
from ToolExtension.search_knowledge import (
    SearchKnowledgeArguments,
    SearchKnowledgeTool,
)
from tool_system.contract import ToolStateError
from tool_system.registry import ToolRegistry


def _write_index(
    path: Path,
    chunks: list[dict[str, Any]],
    *,
    model: str = "mock-embed",
    dimension: int | None = None,
    schema_version: int = 1,
) -> Path:
    if dimension is None:
        dimension = len(chunks[0]["embedding"]) if chunks else 1
    payload = {
        "schema_version": schema_version,
        "embedding_model": model,
        "dimension": dimension,
        "generated_at": "2026-08-05T00:00:00+00:00",
        "chunks": chunks,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _chunk(
    source: str,
    chunk_index: int,
    text: str,
    embedding: list[float],
) -> dict[str, Any]:
    return {
        "source": source,
        "chunk_index": chunk_index,
        "text": text,
        "embedding": embedding,
    }


class _FakeEmbeddingsClient:
    """记录调用的 fake embeddings 客户端；vector 为异常时抛出。"""

    def __init__(self, vector: list[float] | Exception) -> None:
        self.calls: list[dict[str, Any]] = []
        self.closed = 0
        self._vector = vector
        self.embeddings = SimpleNamespace(create=self._create)

    async def _create(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        if isinstance(self._vector, Exception):
            raise self._vector
        return SimpleNamespace(
            data=[SimpleNamespace(index=0, embedding=list(self._vector))]
        )

    async def close(self) -> None:
        self.closed += 1


class _FakeAgent:
    """提供工具所需最小 agent 表面：config 与可选 knowledge_index_path。"""

    def __init__(self, index_path: Path | None = None) -> None:
        self.config = SimpleNamespace(
            openai_baseurl="http://kb.test/v1",
            openai_key=SecretStr("test-key"),
            openai_timeout_seconds=30.0,
        )
        if index_path is not None:
            self.knowledge_index_path = index_path


def _install_fake_client(
    monkeypatch: pytest.MonkeyPatch,
    vector: list[float] | Exception,
) -> tuple[_FakeEmbeddingsClient, list[dict[str, Any]]]:
    fake = _FakeEmbeddingsClient(vector)
    factory_arguments: list[dict[str, Any]] = []

    def factory(**kwargs: Any) -> _FakeEmbeddingsClient:
        factory_arguments.append(kwargs)
        return fake

    monkeypatch.setattr(search_knowledge_module, "AsyncOpenAI", factory)
    return fake, factory_arguments


def _build_registry(agent: _FakeAgent, enabled: str | list[str]) -> ToolRegistry:
    return ToolRegistry.build(
        agent=agent,
        builtin_tool_classes=(),
        extension_tool_classes=EXTENSION_TOOLS,
        enabled_extensions=enabled,
    )


# ---------------------------------------------------------------------------
# 6. schema 严格性
# ---------------------------------------------------------------------------


def test_arguments_schema_is_strict_single_query_field() -> None:
    """schema 仅含必填 query（1..1000 字符），额外字段禁止。"""

    schema = SearchKnowledgeTool.spec.arguments_model.model_json_schema()
    assert set(schema["properties"]) == {"query"}
    assert schema["required"] == ["query"]
    assert schema["additionalProperties"] is False
    assert schema["properties"]["query"]["minLength"] == 1
    assert schema["properties"]["query"]["maxLength"] == 1000


def test_arguments_reject_empty_and_overlong_query() -> None:
    """空字符串与 1001 字符 query 被 Pydantic 拒绝。"""

    with pytest.raises(ValueError):
        SearchKnowledgeArguments(query="")
    with pytest.raises(ValueError):
        SearchKnowledgeArguments(query="x" * 1001)
    assert SearchKnowledgeArguments(query="x" * 1000).query == "x" * 1000


def test_catalog_lists_search_knowledge_last_in_stable_order() -> None:
    """工具在目录末尾追加，既有三项顺序不变。"""

    assert EXTENSION_TOOLS[-1] is SearchKnowledgeTool


# ---------------------------------------------------------------------------
# 7. 可用性与注册表隐藏
# ---------------------------------------------------------------------------


def test_unavailable_when_agent_lacks_path_or_index_missing(
    tmp_path: Path,
) -> None:
    """agent 未声明索引路径或索引文件缺失时 is_available 为 False。"""

    agent_without_path = _FakeAgent()
    assert SearchKnowledgeTool(agent_without_path).is_available() is False

    agent_missing_file = _FakeAgent(index_path=tmp_path / "missing.json")
    assert SearchKnowledgeTool(agent_missing_file).is_available() is False


def test_available_when_index_parses_with_schema_version_1(
    tmp_path: Path,
) -> None:
    """索引存在且 schema_version==1 时 is_available 为 True。"""

    index = _write_index(
        tmp_path / "index.json",
        [_chunk("a.md", 0, "文本", [1.0, 0.0])],
    )
    agent = _FakeAgent(index_path=index)
    assert SearchKnowledgeTool(agent).is_available() is True


def test_unavailable_when_index_payload_is_not_schema_v1(
    tmp_path: Path,
) -> None:
    """索引存在但 schema_version 不是 1（或非 JSON）时不可用。"""

    index = _write_index(
        tmp_path / "bad.json",
        [_chunk("a.md", 0, "文本", [1.0, 0.0])],
        schema_version=2,
    )
    agent = _FakeAgent(index_path=index)
    assert SearchKnowledgeTool(agent).is_available() is False

    (tmp_path / "corrupt.json").write_text("{not json", encoding="utf-8")
    corrupt_agent = _FakeAgent(index_path=tmp_path / "corrupt.json")
    assert SearchKnowledgeTool(corrupt_agent).is_available() is False


def test_registry_all_selection_hides_tool_without_index() -> None:
    """"all" 选择在 agent 未声明索引路径时隐藏本工具，其余工具不受影响。"""

    registry = _build_registry(_FakeAgent(), "all")
    names = [schema["name"] for schema in registry.schemas()]
    assert "search_knowledge" not in names
    assert {"text_stats", "agent_info", "get_weather"} <= set(names)


def test_registry_named_selection_includes_tool_with_index(
    tmp_path: Path,
) -> None:
    """索引就绪时按名启用仅暴露 search_knowledge。"""

    index = _write_index(
        tmp_path / "index.json",
        [_chunk("a.md", 0, "文本", [1.0, 0.0])],
    )
    agent = _FakeAgent(index_path=index)
    registry = _build_registry(agent, ["search_knowledge"])
    names = [schema["name"] for schema in registry.schemas()]
    assert names == ["search_knowledge"]


# ---------------------------------------------------------------------------
# 8. 启动校验
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_startup_rejects_invalid_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """维度不齐、空 chunks、缺 embedding_model 的索引启动即 ToolStateError。"""

    dimension_mismatch = tmp_path / "dim.json"
    _write_index(
        dimension_mismatch,
        [_chunk("a.md", 0, "文本", [1.0, 0.0])],
        dimension=3,
    )
    tool = SearchKnowledgeTool(_FakeAgent(index_path=dimension_mismatch))
    with pytest.raises(ToolStateError):
        await tool.startup()

    empty_chunks = _write_index(tmp_path / "empty.json", [])
    tool = SearchKnowledgeTool(_FakeAgent(index_path=empty_chunks))
    with pytest.raises(ToolStateError):
        await tool.startup()

    no_model = tmp_path / "nomodel.json"
    no_model.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "dimension": 1,
                "chunks": [_chunk("a.md", 0, "文本", [1.0])],
            }
        ),
        encoding="utf-8",
    )
    tool = SearchKnowledgeTool(_FakeAgent(index_path=no_model))
    with pytest.raises(ToolStateError):
        await tool.startup()


@pytest.mark.asyncio
async def test_startup_builds_client_from_agent_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """startup 用 agent config 的 baseurl、key 与 timeout 构造客户端。"""

    index = _write_index(
        tmp_path / "index.json",
        [_chunk("a.md", 0, "文本", [1.0, 0.0])],
        model="mock-embed",
    )
    fake, factory_arguments = _install_fake_client(monkeypatch, [1.0, 0.0])
    tool = SearchKnowledgeTool(_FakeAgent(index_path=index))
    await tool.startup()
    assert factory_arguments == [
        {
            "base_url": "http://kb.test/v1",
            "api_key": "test-key",
            "timeout": 30.0,
        }
    ]
    await tool.shutdown()
    assert fake.closed == 1


@pytest.mark.asyncio
async def test_execute_before_startup_raises_state_error(
    tmp_path: Path,
) -> None:
    """未 startup 直接 execute 抛 ToolStateError。"""

    index = _write_index(
        tmp_path / "index.json",
        [_chunk("a.md", 0, "文本", [1.0, 0.0])],
    )
    tool = SearchKnowledgeTool(_FakeAgent(index_path=index))
    with pytest.raises(ToolStateError):
        await tool.execute(SearchKnowledgeArguments(query="任何"))


# ---------------------------------------------------------------------------
# 9. top-3 排序与结果
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_returns_top3_in_score_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """top-3 按余弦得分降序，并列时按索引顺序稳定排序。"""

    chunks = [
        _chunk("doc.md", 0, "c0-text", [1.0, 0.0]),
        _chunk("doc.md", 1, "c1-text", [0.0, 1.0]),
        _chunk("doc.md", 2, "c2-text", [1.0, 1.0]),
        _chunk("doc.md", 3, "c3-text", [0.0, -1.0]),
        _chunk("doc.md", 4, "c4-text", [0.5, 0.5]),
    ]
    index = _write_index(tmp_path / "index.json", chunks, model="mock-embed")
    fake, _ = _install_fake_client(monkeypatch, [0.0, 1.0])
    tool = SearchKnowledgeTool(_FakeAgent(index_path=index))
    await tool.startup()
    try:
        result = await tool.execute(
            SearchKnowledgeArguments(query="最相关问题")
        )
    finally:
        await tool.shutdown()
    # 得分：c1=1.0，c2=c4≈0.7071（并列，索引小者在前），c0=0，c3=-1。
    assert [item["text"] for item in result["results"]] == [
        "c1-text",
        "c2-text",
        "c4-text",
    ]
    assert all(
        set(item) == {"source", "chunk_index", "text"}
        for item in result["results"]
    )
    # 查询向量化使用索引记录的模型名。
    assert fake.calls == [{"model": "mock-embed", "input": ["最相关问题"]}]


# ---------------------------------------------------------------------------
# 10. 不可信数据框架
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_output_carries_untrusted_framing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """结果含 ok=True 与不可信数据声明 note。"""

    index = _write_index(
        tmp_path / "index.json",
        [_chunk("a.md", 0, "文本", [1.0, 0.0])],
    )
    _install_fake_client(monkeypatch, [1.0, 0.0])
    tool = SearchKnowledgeTool(_FakeAgent(index_path=index))
    await tool.startup()
    try:
        result = await tool.execute(SearchKnowledgeArguments(query="问题"))
    finally:
        await tool.shutdown()
    assert result["ok"] is True
    assert "不可信数据" in result["note"]
    assert "指令" in result["note"]


# ---------------------------------------------------------------------------
# 11. 查询维度不符与端点失败 → TOOL_EXECUTION_ERROR
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_dimension_mismatch_returns_execution_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """查询向量维度与索引不符时，registry 返回稳定 TOOL_EXECUTION_ERROR。"""

    index = _write_index(
        tmp_path / "index.json",
        [_chunk("a.md", 0, "文本", [1.0, 0.0, 0.0])],
        dimension=3,
    )
    _install_fake_client(monkeypatch, [1.0, 0.0])
    agent = _FakeAgent(index_path=index)
    registry = _build_registry(agent, ["search_knowledge"])
    await registry.startup()
    try:
        result = await registry.dispatch(
            "search_knowledge", json.dumps({"query": "问题"})
        )
    finally:
        await registry.shutdown()
    assert result == {
        "ok": False,
        "code": "TOOL_EXECUTION_ERROR",
        "message": "工具执行失败",
    }


@pytest.mark.asyncio
async def test_dispatch_endpoint_failure_returns_execution_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """查询向量化端点失败同样转为 TOOL_EXECUTION_ERROR，不透传异常。"""

    index = _write_index(
        tmp_path / "index.json",
        [_chunk("a.md", 0, "文本", [1.0])],
    )
    _install_fake_client(monkeypatch, RuntimeError("endpoint down"))
    agent = _FakeAgent(index_path=index)
    registry = _build_registry(agent, ["search_knowledge"])
    await registry.startup()
    try:
        result = await registry.dispatch(
            "search_knowledge", json.dumps({"query": "问题"})
        )
    finally:
        await registry.shutdown()
    assert result["ok"] is False
    assert result["code"] == "TOOL_EXECUTION_ERROR"


# ---------------------------------------------------------------------------
# 12. 生命周期收尾
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_shutdown_closes_client_and_clears_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """shutdown 关闭客户端一次并清空引用；重复 shutdown 安全。"""

    index = _write_index(
        tmp_path / "index.json",
        [_chunk("a.md", 0, "文本", [1.0])],
    )
    fake, _ = _install_fake_client(monkeypatch, [1.0])
    tool = SearchKnowledgeTool(_FakeAgent(index_path=index))
    await tool.startup()
    await tool.shutdown()
    await tool.shutdown()
    assert fake.closed == 1
    assert tool._client is None  # noqa: SLF001  生命周期状态断言
