/* AgentGraph root 前端：零依赖静态页面，只调用 root HTTP API。
 * 契约见 README「HTTP API」：统一 {"data"} / {"error"} envelope。
 */
"use strict";

const $ = (id) => document.getElementById(id);

const agentList = $("agentList");
const input = $("input");
const sendBtn = $("sendBtn");

const state = {
  topology: null,        // {nodes, edges, errors}
  nodeById: new Map(),
  directIds: new Set(),
  chats: {},             // agentId -> {messages, pending, pendingStart, loaded}
  current: null,
  sup: { available: true, agents: [] },
  supBusy: new Set(),    // 正在执行启动/停止请求的 agent id
};

/* ---------- 通用 ---------- */

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

async function api(path, options) {
  let res;
  try {
    res = await fetch(path, options);
  } catch {
    throw { code: "NETWORK_ERROR", message: "无法连接 root 服务" };
  }
  let body = null;
  try { body = await res.json(); } catch { /* 非 JSON 响应 */ }
  if (body && body.error) throw body.error;
  if (!res.ok || !body || !("data" in body)) {
    throw { code: "HTTP_" + res.status, message: "root 服务返回了无效响应" };
  }
  return body.data;
}

let toastTimer = 0;
function toast(msg, isErr) {
  const t = $("toast");
  t.textContent = msg;
  t.className = "mono show" + (isErr ? " error" : "");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => (t.className = "mono"), 3200);
}

/* ---------- 健康检查（只探 root 自身） ---------- */

async function ping() {
  try {
    await api("/healthz");
    $("connDot").className = "conn-dot on";
    $("connText").textContent = "在线";
  } catch {
    $("connDot").className = "conn-dot off";
    $("connText").textContent = "离线";
  }
}

/* ---------- 拓扑 ---------- */

async function loadTopology() {
  agentList.innerHTML = "";
  agentList.appendChild(el("div", "empty-mini mono", "加载拓扑中…"));
  try {
    const data = await api("/v1/user/topology");
    state.topology = data;
    state.nodeById = new Map((data.nodes || []).map((n) => [n.id, n]));
    state.directIds = new Set(
      (data.edges || []).filter((e) => e.from_id === "root").map((e) => e.to_id),
    );
    renderSidebar();
    renderTopology();
    renderChatHead();
  } catch (err) {
    agentList.innerHTML = "";
    agentList.appendChild(el("div", "empty-mini mono", `拓扑加载失败 ${err.code || ""}`));
    toast(`拓扑加载失败：${err.message || ""}`, true);
  }
}

/* ---------- 进程监督 ---------- */

async function loadSupervisor() {
  if (!state.sup.available) return;
  let data;
  try {
    data = await api("/v1/user/agents");
  } catch (err) {
    if (err.code === "HTTP_404") state.sup.available = false; // 旧后端，退回拓扑视图
    return;
  }
  const prev = new Map(state.sup.agents.map((a) => [a.id, a.state]));
  state.sup.agents = data.agents || [];
  const revived = state.sup.agents.some(
    (a) => a.state === "running" && prev.has(a.id) && prev.get(a.id) !== "running",
  );
  renderSidebar();
  setComposer();
  if (revived) loadTopology();
}

async function toggleAgent(id, action) {
  state.supBusy.add(id);
  renderSidebar();
  try {
    const r = await api(
      `/v1/user/agents/${encodeURIComponent(id)}/${action}`,
      { method: "POST" },
    );
    if (action === "start" && r && r.started === false) toast(`${id} 已在运行`);
    if (action === "stop" && r && r.stopped === false) toast(`${id} 未在运行`);
  } catch (err) {
    toast(`${err.code} — ${err.message}`, true);
  }
  state.supBusy.delete(id);
  await loadSupervisor();
}

/* ---------- 侧栏 roster（拓扑 × 监督者合并） ---------- */

function roster() {
  const map = new Map();
  for (const n of state.nodeById.values()) {
    if (n.id === "root") continue;
    map.set(n.id, {
      id: n.id,
      introduction: n.introduction || "",
      topoStatus: n.status,
      direct: state.directIds.has(n.id),
      inTopology: true,
      sup: null,
    });
  }
  for (const a of state.sup.agents) {
    const row = map.get(a.id) || {
      id: a.id,
      introduction: "",
      topoStatus: null,
      direct: false,
      inTopology: false,
    };
    row.sup = a;
    if (!row.introduction && a.introduction) row.introduction = a.introduction;
    map.set(a.id, row);
  }
  return [...map.values()];
}

function dotClass(row) {
  if (row.sup) {
    if (row.sup.state === "running") return "reachable";
    if (row.sup.state === "starting") return "starting";
    if (row.sup.state === "crashed") return "unreachable";
    return "stopped";
  }
  return row.topoStatus === "reachable" ? "reachable" : "unreachable";
}

function controlButton(row) {
  const a = row.sup;
  if (!a) return null;
  if (state.supBusy.has(a.id)) {
    const b = el("button", "mini", "…");
    b.disabled = true;
    return b;
  }
  if (a.state === "running") {
    if (!a.ours) return el("span", "tag", "外部");
    const b = el("button", "mini stop", "停止");
    b.addEventListener("click", (e) => {
      e.stopPropagation();
      toggleAgent(a.id, "stop");
    });
    return b;
  }
  if (a.state === "starting") {
    const b = el("button", "mini", "启动中");
    b.disabled = true;
    return b;
  }
  const b = el("button", "mini run", a.state === "crashed" ? "重启" : "启动");
  b.addEventListener("click", (e) => {
    e.stopPropagation();
    toggleAgent(a.id, "start");
  });
  return b;
}

function agentRow(row) {
  const div = el(
    "div",
    "agent" + (row.direct ? "" : " indirect") + (row.id === state.current ? " active" : ""),
  );
  div.appendChild(el("span", "dot " + dotClass(row)));
  const meta = el("div", "meta");
  const line1 = el("div", "l1");
  line1.appendChild(el("span", "name", row.id));
  const chat = state.chats[row.id];
  if (chat && chat.pending) line1.appendChild(el("span", "busy", "busy"));
  meta.appendChild(line1);
  const sub = [];
  if (row.direct) sub.push("直接");
  else if (row.inTopology) sub.push("间接");
  if (row.sup) sub.push(":" + row.sup.port);
  const subDiv = el("div", "sub mono", sub.join(" · "));
  if (row.sup && row.sup.state === "crashed") {
    subDiv.appendChild(document.createTextNode(" · "));
    subDiv.appendChild(el("span", "crash", `exit ${row.sup.exit_code ?? "?"}`));
    div.title = (row.sup.last_log || "").trim().split("\n").slice(-3).join("\n");
  } else if (row.introduction) {
    div.title = row.introduction;
  }
  meta.appendChild(subDiv);
  div.appendChild(meta);
  const btn = controlButton(row);
  if (btn) div.appendChild(btn);
  if (row.direct) div.addEventListener("click", () => selectAgent(row.id));
  return div;
}

function renderSidebar() {
  agentList.innerHTML = "";
  const rows = roster();
  const byId = (a, b) => a.id.localeCompare(b.id);
  const direct = rows.filter((r) => r.direct).sort(byId);
  const indirect = rows.filter((r) => !r.direct).sort(byId);
  const online = direct.filter((r) => !r.sup || r.sup.state === "running").length;
  $("agentCount").textContent = `· 直接 ${online}/${direct.length} 在线`;
  for (const r of direct) agentList.appendChild(agentRow(r));
  if (indirect.length) {
    agentList.appendChild(el("div", "panel-head mono", "INDIRECT"));
    for (const r of indirect) agentList.appendChild(agentRow(r));
  }
}

/* ---------- 拓扑图（原生 SVG，BFS 分层布局） ---------- */

const NS = "http://www.w3.org/2000/svg";
const COL_W = 172, ROW_H = 62, PAD = 16, BOX_H = 32;

function svgEl(tag, attrs) {
  const node = document.createElementNS(NS, tag);
  for (const [k, v] of Object.entries(attrs || {})) node.setAttribute(k, v);
  return node;
}

function boxWidth(id) { return id.length * 7.2 + 30; }

function layoutTopology() {
  const seen = new Set();
  const edgeList = [];
  for (const e of state.topology.edges || []) {
    const key = e.from_id + ">" + e.to_id;
    if (seen.has(key)) continue;
    seen.add(key);
    edgeList.push(e);
  }
  const adj = new Map();
  for (const e of edgeList) {
    if (!adj.has(e.from_id)) adj.set(e.from_id, []);
    adj.get(e.from_id).push(e.to_id);
  }
  const level = new Map([["root", 0]]);
  let frontier = ["root"];
  while (frontier.length) {
    const next = [];
    for (const from of frontier) {
      for (const to of adj.get(from) || []) {
        if (level.has(to)) continue;
        level.set(to, level.get(from) + 1);
        next.push(to);
      }
    }
    frontier = next;
  }
  let maxLevel = 0;
  for (const l of level.values()) maxLevel = Math.max(maxLevel, l);
  const cols = [];
  for (const n of state.topology.nodes || []) {
    const l = level.has(n.id) ? level.get(n.id) : maxLevel + 1;
    (cols[l] ||= []).push(n);
  }
  return { cols: cols.filter(Boolean), edgeList };
}

function renderTopology() {
  const svg = $("topoSvg");
  const errBox = $("topoErrors");
  svg.innerHTML = "";
  errBox.innerHTML = "";
  if (!state.topology) return;
  for (const e of state.topology.errors || []) {
    errBox.appendChild(el("div", "topo-err", `${e.code} · ${e.peer_id}`));
  }
  if (!state.topology.nodes.length) return;

  const { cols, edgeList } = layoutTopology();
  if (!cols.length) return;
  const maxRows = Math.max(...cols.map((c) => c.length));
  const W = PAD * 2 + cols.length * COL_W;
  const H = PAD * 2 + maxRows * ROW_H;
  svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
  svg.setAttribute("width", W);
  svg.setAttribute("height", H);

  const defs = svgEl("defs");
  for (const [mid, fill] of [["arrow", "#4a443a"], ["arrowA", "#6b5530"]]) {
    const m = svgEl("marker", {
      id: mid, viewBox: "0 0 8 8", refX: 7, refY: 4,
      markerWidth: 7, markerHeight: 7, orient: "auto-start-reverse",
    });
    m.appendChild(svgEl("path", { d: "M0 0 L8 4 L0 8 z", fill }));
    defs.appendChild(m);
  }
  svg.appendChild(defs);

  const pos = new Map();
  cols.forEach((col, ci) => {
    const x = PAD + ci * COL_W;
    const startY = (H - col.length * ROW_H) / 2;
    col.forEach((n, ri) => {
      pos.set(n.id, { x, y: startY + ri * ROW_H + (ROW_H - BOX_H) / 2, w: boxWidth(n.id) });
    });
  });

  for (const e of edgeList) {
    const a = pos.get(e.from_id), b = pos.get(e.to_id);
    if (!a || !b) continue;
    const x1 = a.x + a.w, y1 = a.y + BOX_H / 2;
    const x2 = b.x, y2 = b.y + BOX_H / 2;
    let d;
    if (x2 > x1 + 8) {
      d = `M ${x1} ${y1} L ${x2 - 2} ${y2}`;
    } else {
      // 回边/同列边：向下绕行，避免与节点框重叠
      const my = Math.max(a.y, b.y) + BOX_H + 18;
      d = `M ${x1} ${y1} C ${x1 + 24} ${my} ${x2 - 24} ${my} ${x2 - 2} ${y2}`;
    }
    svg.appendChild(svgEl("path", {
      d, class: "t-edge" + (e.from_id === "root" ? " direct" : ""),
      "marker-end": `url(#${e.from_id === "root" ? "arrowA" : "arrow"})`,
    }));
  }

  for (const [id, p] of pos) {
    const n = state.nodeById.get(id) || { id, status: "reachable" };
    const direct = state.directIds.has(id) && id !== "root";
    const g = svgEl("g", {
      class: "t-node" +
        (id === "root" ? " root" : "") +
        (direct ? " direct clickable" : "") +
        (n.status !== "reachable" ? " unreachable" : ""),
    });
    g.appendChild(svgEl("rect", { x: p.x, y: p.y, width: p.w, height: BOX_H, rx: 5 }));
    g.appendChild(svgEl("circle", {
      cx: p.x + 13, cy: p.y + BOX_H / 2, r: 3.2,
      class: n.status === "reachable" ? "ok" : "bad",
    }));
    const text = svgEl("text", { x: p.x + 22, y: p.y + BOX_H / 2 + 4 });
    text.textContent = id;
    g.appendChild(text);
    if (n.introduction) {
      const title = svgEl("title");
      title.textContent = n.introduction;
      g.appendChild(title);
    }
    if (direct) g.addEventListener("click", () => selectAgent(id));
    svg.appendChild(g);
  }
}

/* ---------- 会话 ---------- */

function ensureChat(id) {
  return (state.chats[id] ||= { messages: [], pending: false, pendingStart: 0, loaded: false });
}

async function selectAgent(id) {
  if (!state.directIds.has(id)) {
    toast("只能与 root 直接可见的 Agent 对话");
    return;
  }
  state.current = id;
  renderSidebar();
  renderChatHead();
  $("chatHead").classList.remove("hidden");
  $("composer").classList.remove("hidden");
  const chat = ensureChat(id);
  setComposer();
  renderMessages();
  if (!chat.loaded) {
    try {
      chat.messages = await api(`/v1/user/chats/${encodeURIComponent(id)}`);
      chat.loaded = true;
    } catch (err) {
      chat.messages.push({ role: "error", content: `历史加载失败 ${err.code} — ${err.message}` });
      chat.loaded = true;
    }
    if (state.current === id) renderMessages();
  }
  input.focus();
}

function renderChatHead() {
  const id = state.current;
  if (!id) return;
  $("chatAgentId").textContent = id;
  const n = state.nodeById.get(id);
  $("chatIntro").textContent = n && n.introduction ? n.introduction : "";
}

function emptyBox(glyph, hint) {
  const wrap = el("div", "empty");
  wrap.appendChild(el("div", "glyph", glyph));
  wrap.appendChild(el("div", "hint", hint));
  return wrap;
}

function renderMessages() {
  const box = $("messages");
  const nearBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 90;
  box.innerHTML = "";
  const id = state.current;
  if (!id) {
    box.appendChild(emptyBox("◇", "从左侧选择一个直接可见的 Agent 开始对话"));
    return;
  }
  const chat = ensureChat(id);
  if (!chat.messages.length && !chat.pending) {
    box.appendChild(emptyBox("·", "会话为空，发送第一条消息"));
  }
  for (const m of chat.messages) {
    const div = el("div", "msg " + (m.role === "user" ? "user" : m.role === "error" ? "err" : ""));
    div.appendChild(el("div", "who", m.role === "user" ? "YOU" : m.role === "error" ? "ERROR" : id));
    div.appendChild(el("div", "body", m.content));
    box.appendChild(div);
  }
  if (chat.pending) {
    const row = el("div", "thinking");
    const pulse = el("span", "pulse");
    pulse.appendChild(el("i"));
    pulse.appendChild(el("i"));
    pulse.appendChild(el("i"));
    row.appendChild(pulse);
    row.appendChild(el("span", null, id));
    row.appendChild(el("span", null, "处理中"));
    row.appendChild(el("span", null, "0s"));
    row.lastChild.className = "elapsed";
    box.appendChild(row);
  }
  if (nearBottom) box.scrollTop = box.scrollHeight;
}

function elapsedTick() {
  const chat = state.current && state.chats[state.current];
  if (!chat || !chat.pending) return;
  const node = document.querySelector(".thinking .elapsed");
  if (node) node.textContent = Math.floor((Date.now() - chat.pendingStart) / 1000) + "s";
}

function setComposer() {
  const chat = state.current && state.chats[state.current];
  const busy = !!(chat && chat.pending);
  const sup = state.sup.available
    ? state.sup.agents.find((a) => a.id === state.current)
    : null;
  const notRunning = !!(sup && sup.state !== "running");
  sendBtn.disabled = busy || notRunning;
  input.disabled = busy || notRunning;
  input.placeholder = busy
    ? "Agent 处理中…同会话消息会在后端排队，可先切换其它 Agent"
    : notRunning
      ? "Agent 未运行，请在左侧启动"
      : "输入消息，Enter 发送，Shift+Enter 换行";
}

/* ---------- 发送与关闭 ---------- */

async function sendMessage() {
  const id = state.current;
  const chat = id && ensureChat(id);
  const text = input.value.trim();
  if (!id || !chat || chat.pending || !text) return;
  input.value = "";
  autosize();
  chat.messages.push({ role: "user", content: text });
  chat.pending = true;
  chat.pendingStart = Date.now();
  setComposer();
  renderMessages();
  renderSidebar();
  try {
    const reply = await api(`/v1/user/chats/${encodeURIComponent(id)}/messages`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: text, request_id: crypto.randomUUID() }),
    });
    chat.messages.push({ role: "assistant", content: String(reply) });
  } catch (err) {
    chat.messages.push({ role: "error", content: `${err.code} — ${err.message}` });
  } finally {
    chat.pending = false;
    setComposer();
    renderMessages();
    renderSidebar();
  }
}

async function closeChat() {
  const id = state.current;
  if (!id) return;
  if (!confirm(`关闭与 ${id} 的会话？远端上下文将归档。`)) return;
  try {
    const r = await api(`/v1/user/chats/${encodeURIComponent(id)}`, { method: "DELETE" });
    const chat = ensureChat(id);
    chat.messages = [];
    chat.loaded = true;
    renderMessages();
    toast(`会话已关闭（closed=${r.closed} · saved=${r.saved}）`);
  } catch (err) {
    toast(`${err.code} — ${err.message}`, true);
  }
}

/* ---------- 输入框 ---------- */

function autosize() {
  input.style.height = "auto";
  input.style.height = Math.min(input.scrollHeight, 180) + "px";
}

/* ---------- 初始化 ---------- */

$("hostLabel").textContent = location.host;
$("refreshBtn").addEventListener("click", () => {
  loadSupervisor();
  loadTopology();
});
$("closeChatBtn").addEventListener("click", closeChat);
sendBtn.addEventListener("click", sendMessage);
input.addEventListener("keydown", (e) => {
  if (e.key === "Enter" && !e.shiftKey) {
    e.preventDefault();
    sendMessage();
  }
});
input.addEventListener("input", autosize);
$("topoToggle").addEventListener("click", () => {
  const hidden = document.body.classList.toggle("no-topo");
  $("topoToggle").setAttribute("aria-pressed", String(!hidden));
  $("topoToggle").textContent = hidden ? "显示拓扑" : "隐藏拓扑";
});

ping();
setInterval(ping, 15000);
setInterval(elapsedTick, 1000);
loadSupervisor();
setInterval(loadSupervisor, 5000);
loadTopology();
