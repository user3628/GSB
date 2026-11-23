const app = {
  state: null,
  meta: null,
  samples: [],
  intents: [],
  reports: [],
  versions: [],
  currentView: "overview",
  selectedImpact: null,
  intentFilter: "",
  lastStudioError: null,
  drillTimer: null,
};

const VIEW_META = {
  overview: ["OPERATIONS / OVERVIEW", "故障恢复态势"],
  studio: ["CONTROL PLANE / CHANGE STUDIO", "增量变更实验室"],
  drill: ["EXECUTION / DRILL LAB", "恢复演练"],
  evidence: ["AUDIT / EVIDENCE", "审计与恢复证据"],
};

document.addEventListener("DOMContentLoaded", init);

async function init() {
  wireEvents();
  const initial = location.hash.replace("#", "");
  if (VIEW_META[initial]) switchView(initial, false);
  try {
    await refreshAll();
    seedStudio("resource", false);
    await loadEvidence();
    updateEndpointStatus();
  } catch (error) {
    showErrorToast(error);
    setConnection(false, error.message);
  }
}

function wireEvents() {
  document.querySelectorAll(".nav-item").forEach((button) => {
    button.addEventListener("click", () => switchView(button.dataset.view));
  });
  document.querySelectorAll("[data-goto]").forEach((button) => {
    button.addEventListener("click", () => switchView(button.dataset.goto));
  });
  document.getElementById("refreshBtn").addEventListener("click", refreshAllAndToast);
  document.getElementById("quickLoadBtn").addEventListener("click", quickLoadSample);
  document.getElementById("impactSelect").addEventListener("change", (event) => {
    app.selectedImpact = event.target.value;
    renderImpact();
  });
  document.getElementById("graphWrap").addEventListener("click", (event) => {
    const node = event.target.closest("[data-component]");
    if (!node) return;
    app.selectedImpact = node.dataset.component;
    document.getElementById("impactSelect").value = app.selectedImpact;
    renderImpact();
    renderGraph();
  });
  document.getElementById("verifyConsistencyBtn").addEventListener("click", verifyConsistency);
  document.getElementById("presetStrip").addEventListener("click", (event) => {
    const button = event.target.closest("[data-preset]");
    if (!button) return;
    seedStudio(button.dataset.preset, true);
  });
  document.getElementById("studioKey").addEventListener("input", syncStudioKey);
  document.getElementById("newKeyBtn").addEventListener("click", () => {
    const key = makeKey("ui");
    document.getElementById("studioKey").value = key;
    syncStudioKey();
  });
  const editor = document.getElementById("studioPayload");
  editor.addEventListener("input", validateEditor);
  editor.addEventListener("keydown", (event) => {
    if (event.key === "Tab") {
      event.preventDefault();
      const start = editor.selectionStart;
      editor.value = editor.value.slice(0, start) + "  " + editor.value.slice(editor.selectionEnd);
      editor.selectionStart = editor.selectionEnd = start + 2;
    }
  });
  document.getElementById("formatJsonBtn").addEventListener("click", formatEditor);
  document.getElementById("submitIncrementBtn").addEventListener("click", submitIncrement);
  document.getElementById("validateSamplesBtn").addEventListener("click", loadSamples);
  document.getElementById("sampleList").addEventListener("click", handleSampleAction);
  document.getElementById("runDrillBtn").addEventListener("click", runDrill);
  document.getElementById("reloadEvidenceBtn").addEventListener("click", loadEvidence);
  document.getElementById("loadLineageBtn").addEventListener("click", loadLineage);
  document.getElementById("intentFilters").addEventListener("click", (event) => {
    const button = event.target.closest("[data-state]");
    if (!button) return;
    document.querySelectorAll("#intentFilters button").forEach((item) => item.classList.remove("active"));
    button.classList.add("active");
    app.intentFilter = button.dataset.state;
    loadIntents();
  });
  document.getElementById("modalClose").addEventListener("click", closeModal);
  document.getElementById("modalBackdrop").addEventListener("click", (event) => {
    if (event.target.id === "modalBackdrop") closeModal();
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") closeModal();
  });
  document.getElementById("studioResult").addEventListener("click", (event) => {
    if (event.target.closest("[data-show-error]") && app.lastStudioError) {
      openModal("错误上下文", app.lastStudioError);
    }
  });
}

async function api(path, options = {}) {
  const config = { ...options, headers: { ...(options.headers || {}) } };
  if (config.body && !config.headers["Content-Type"]) config.headers["Content-Type"] = "application/json";
  let response;
  try {
    response = await fetch(path, config);
  } catch (error) {
    const wrapped = new Error(`本地 API 不可达: ${error.message}`);
    wrapped.payload = { error: { code: "E_NETWORK", message: wrapped.message, context: {} } };
    throw wrapped;
  }
  let payload;
  try {
    payload = await response.json();
  } catch (error) {
    throw new Error(`HTTP ${response.status}: 响应不是 JSON`);
  }
  if (!response.ok || payload.ok === false) {
    const wrapped = new Error(payload.error?.message || `HTTP ${response.status}`);
    wrapped.payload = payload;
    wrapped.status = response.status;
    throw wrapped;
  }
  return payload.data;
}

async function refreshAll() {
  const [state, meta] = await Promise.all([api("/api/v1/state"), api("/api/v1/meta")]);
  app.state = state;
  app.meta = meta;
  renderState();
  setConnection(true);
  document.getElementById("lastRefresh").textContent = new Date().toLocaleTimeString("zh-CN", { hour12: false });
}

async function refreshAllAndToast() {
  const button = document.getElementById("refreshBtn");
  button.disabled = true;
  try {
    await refreshAll();
    await loadEvidence();
    toast("状态与审计证据已刷新", "success");
  } catch (error) {
    showErrorToast(error);
  } finally {
    button.disabled = false;
  }
}

function renderState() {
  const state = app.state;
  if (!state) return;
  document.getElementById("headerVersion").textContent = `v${state.version}`;
  document.getElementById("studioBaseVersion").textContent = `v${state.version}`;
  document.getElementById("metricComponents").textContent = state.components.length;
  document.getElementById("metricEdges").textContent = `${state.edges.length} 依赖边`;
  document.getElementById("metricBatches").textContent = state.derivation.batch_groups.length;
  document.getElementById("metricRefs").textContent = state.reference_count;
  document.getElementById("metricDiagnostics").textContent = state.derivation.diagnostics.length;
  document.getElementById("metricDerivedDigest").textContent = `派生 ${shortDigest(state.derivation.derived_digest, 12)}`;
  document.getElementById("sideEngineStatus").textContent = `已连接 · v${state.version}`;
  renderConsistency();
  renderGraph();
  renderImpactSelect();
  renderImpact();
  renderBatchLanes();
  renderConsistencyRows();
  renderFailedPicker();
  document.getElementById("drillVersion").value = `v${state.version}`;
  if (!document.getElementById("drillKey").value) document.getElementById("drillKey").value = makeKey("drill");
}

function setConnection(ok, message = "") {
  document.getElementById("sideEngineStatus").textContent = ok ? `已连接 · v${app.state?.version ?? 0}` : `离线${message ? " · " + message : ""}`;
}

function renderConsistency() {
  const result = app.state.derivation.consistency;
  const scorePermille = Math.round(result.score_micro / 1000);
  const gauge = document.getElementById("consistencyGauge");
  gauge.style.setProperty("--score", Math.max(0, Math.min(100, result.score_micro / 10000)).toFixed(3));
  document.getElementById("consistencyScore").textContent = scorePermille;
  const decision = document.getElementById("consistencyDecision");
  const decisionLabels = { met: "阈值内达标", met_within_tolerance: "容差内达标", below: "低于阈值" };
  decision.textContent = decisionLabels[result.decision] || result.decision;
  decision.classList.toggle("below", result.decision === "below");
  document.getElementById("thresholdValue").textContent = `${result.threshold_permille}‰`;
  document.getElementById("toleranceValue").textContent = `${result.relative_tolerance_ppm} ppm`;
  document.getElementById("roundingSummary").textContent = `compensated=${result.compensated_sum} · 误差 ${result.cumulative_rounding_error_micro}µ · ${result.rounding_error_ppb} ppb · tie-break ${result.tie_break}`;
  document.getElementById("consistencyDigest").textContent = shortDigest(result.digest, 34);
  document.getElementById("consistencyDigest").title = result.digest;
}

function renderGraph() {
  const state = app.state;
  const wrap = document.getElementById("graphWrap");
  if (!state.components.length) {
    wrap.innerHTML = '<div class="empty-state">尚未提交组件快照</div>';
    return;
  }
  const levels = state.derivation.topology_batches || {};
  const blocked = state.derivation.blocked || {};
  const groups = new Map();
  const blockedNodes = [];
  for (const component of state.components) {
    if (blocked[component.id]) {
      blockedNodes.push(component.id);
      continue;
    }
    const level = levels[component.id] ?? 0;
    if (!groups.has(level)) groups.set(level, []);
    groups.get(level).push(component.id);
  }
  if (blockedNodes.length) groups.set(Math.max(0, ...groups.keys(), 0) + 1, blockedNodes.sort());
  const nodeW = 150;
  const nodeH = 56;
  const gapX = 188;
  const gapY = 80;
  const positions = new Map();
  let maxRows = 1;
  for (const [level, ids] of [...groups.entries()].sort((a, b) => a[0] - b[0])) {
    maxRows = Math.max(maxRows, ids.length);
    ids.sort().forEach((id, index) => positions.set(id, { x: 24 + level * gapX, y: 20 + index * gapY }));
  }
  const width = Math.max(720, 24 + (Math.max(...groups.keys(), 0) + 1) * gapX + nodeW);
  const height = Math.max(340, 35 + maxRows * gapY);
  let edges = "";
  for (const edge of state.edges) {
    const from = positions.get(edge.from);
    const to = positions.get(edge.to);
    if (!from || !to) continue;
    const x1 = from.x + nodeW;
    const y1 = from.y + nodeH / 2;
    const x2 = to.x;
    const y2 = to.y + nodeH / 2;
    const bend = Math.max(36, (x2 - x1) * 0.45);
    const active = app.selectedImpact === edge.from || app.selectedImpact === edge.to;
    edges += `<path class="graph-edge" d="M${x1} ${y1} C${x1 + bend} ${y1},${x2 - bend} ${y2},${x2 - 2} ${y2}" fill="none" stroke="${active ? "rgba(66,223,202,.72)" : "rgba(115,151,190,.22)"}" stroke-width="${active ? 1.8 : 1.1}" marker-end="url(#arrow)"/>`;
  }
  let nodes = "";
  for (const component of state.components) {
    const point = positions.get(component.id);
    if (!point) continue;
    const observation = state.observations[component.id]?.state || "unknown";
    const isBlocked = Boolean(blocked[component.id]);
    const color = isBlocked ? "#9b87ff" : observationColor(observation);
    const selected = app.selectedImpact === component.id;
    const level = levels[component.id];
    const batchLabel = isBlocked ? "BLOCKED" : `BATCH ${level ?? "?"}`;
    nodes += `<g class="graph-node" data-component="${esc(component.id)}">
      <rect x="${point.x}" y="${point.y}" width="${nodeW}" height="${nodeH}" rx="12" fill="${selected ? "#122c41" : "#0d1d31"}" stroke="${selected ? color : "rgba(137,171,215,.22)"}" stroke-width="${selected ? 1.7 : 1}"/>
      <circle cx="${point.x + 14}" cy="${point.y + 18}" r="4" fill="${color}"/>
      <text x="${point.x + 26}" y="${point.y + 22}" fill="#dfeaf7" font-size="11" font-family="Segoe UI, sans-serif" font-weight="600">${esc(trimText(component.id, 17))}</text>
      <text x="${point.x + 14}" y="${point.y + 42}" fill="${isBlocked ? "#a897ff" : "#6f86a1"}" font-size="7.5" letter-spacing="1.1" font-family="Cascadia Code, monospace">${batchLabel} · ${esc(observation.toUpperCase())}</text>
      <title>${esc(component.id)} · ${esc(observation)} · ${esc(batchLabel)}</title>
    </g>`;
  }
  wrap.innerHTML = `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="组件依赖图"><defs><marker id="arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="rgba(133,165,201,.48)"/></marker></defs>${edges}${nodes}</svg>`;
}

function renderImpactSelect() {
  const select = document.getElementById("impactSelect");
  const ids = app.state.components.map((component) => component.id);
  if (!app.selectedImpact || !ids.includes(app.selectedImpact)) app.selectedImpact = ids.includes("gateway") ? "gateway" : ids[0] || null;
  select.innerHTML = ids.map((id) => `<option value="${esc(id)}" ${id === app.selectedImpact ? "selected" : ""}>${esc(id)}</option>`).join("");
}

function renderImpact() {
  const container = document.getElementById("impactContent");
  if (!app.state || !app.selectedImpact) {
    container.innerHTML = '<div class="empty-state">选择组件查看下游传播</div>';
    return;
  }
  const impact = app.state.derivation.impacts[app.selectedImpact];
  if (!impact) {
    container.innerHTML = '<div class="empty-state">无影响面数据</div>';
    return;
  }
  const rows = impact.affected.map((row) => {
    const batch = row.blocked ? "受阻" : `批次 ${row.recovery_batch ?? "?"}`;
    return `<div class="impact-row"><div class="impact-hop">${row.hop}</div><div><strong>${esc(row.component)}</strong><small>拓扑层 ${row.topology_level ?? "—"} · ${row.blocked ? esc(row.blocked_reason || "blocked") : "可排程"}</small></div><span class="batch-chip ${row.blocked ? "blocked" : ""}">${esc(batch)}</span></div>`;
  }).join("");
  container.innerHTML = `<div class="impact-summary"><div><span>受影响组件</span><strong>${impact.affected_count}</strong></div><div><span>最大传播跳数</span><strong>${impact.max_hop}</strong></div></div><div class="impact-list">${rows}</div>`;
}

function renderBatchLanes() {
  const container = document.getElementById("batchLanes");
  const groups = app.state.derivation.batch_groups || [];
  const blocked = Object.keys(app.state.derivation.blocked || {}).sort();
  let html = groups.map((group) => `<div class="batch-lane"><div class="batch-index">B${group.batch}</div><div class="batch-members">${group.components.map((id) => `<span class="member-chip">${esc(id)}</span>`).join("")}</div></div>`).join("");
  if (blocked.length) html += `<div class="batch-lane"><div class="batch-index" style="color:var(--red);background:var(--red-soft);border-color:rgba(255,115,123,.25)">!</div><div class="batch-members">${blocked.map((id) => `<span class="member-chip" style="color:#ff9ba0">${esc(id)}</span>`).join("")}</div></div>`;
  container.innerHTML = html || '<div class="empty-state small">尚无批次</div>';
}

function renderConsistencyRows() {
  const body = document.getElementById("consistencyRows");
  const steps = app.state.derivation.consistency.steps || [];
  if (!steps.length) {
    body.innerHTML = '<tr><td colspan="5" class="muted-cell">暂无数据</td></tr>';
    return;
  }
  body.innerHTML = steps.map((step) => `<tr><td class="name">${esc(step.component_id)}</td><td><span class="state-dot ${esc(step.probe_state)}"></span>${esc(step.probe_state)}</td><td><div style="display:flex;align-items:center;gap:7px"><span>${(step.resource_headroom_micro / 1000).toFixed(1)}%</span><div class="micro-bar"><i style="width:${(step.resource_headroom_micro / 10000).toFixed(1)}%"></i></div></div></td><td class="mono">${(step.local_score_micro / 1000).toFixed(0)}‰ <span class="micro-bar" style="display:inline-block;vertical-align:middle"><i style="width:${Math.max(0, Math.min(100, step.local_score_micro / 10000)).toFixed(1)}%"></i></span></td><td class="mono">${step.consistency_weight_permille}</td></tr>`).join("");
}

function renderFailedPicker() {
  const picker = document.getElementById("failedPicker");
  const checked = new Set([...picker.querySelectorAll("input:checked")].map((input) => input.value));
  if (!checked.size) checked.add("gateway");
  const available = new Set(app.state.components.map((component) => component.id));
  const selected = [...checked].filter((id) => available.has(id));
  if (!selected.length && available.size) selected.push([...available][0]);
  picker.innerHTML = app.state.components.map((component) => `<label class="component-choice"><input type="checkbox" value="${esc(component.id)}" ${selected.includes(component.id) ? "checked" : ""}><span>${esc(component.id)}</span></label>`).join("");
}

function observationColor(state) {
  return { healthy: "#42dfca", degraded: "#f4bc62", unhealthy: "#ff737b", unknown: "#6f829a" }[state] || "#6f829a";
}

function seedStudio(preset, notify) {
  const version = app.state?.version ?? 0;
  const operationSets = {
    resource: { note: "调整 api 的 CPU 资源需求并保持容量不变", operations: [{ type: "patch_resource", component_id: "api", resources: { cpu_millicores: 3000, memory_bytes: 3221225472 }, capacity: { cpu_millicores: 5000, memory_bytes: 8589934592 } }], base_version: version },
    probe: { note: "收紧 reporter 的探测与恢复截止时间", operations: [{ type: "patch_probe", component_id: "reporter", health_probe: { type: "http", url: "http://127.0.0.1:19090/health?mode=timeout&component=reporter", method: "GET", timeout_ms: 350 }, recovery_request: { url: "http://127.0.0.1:19090/recover?mode=timeout&component=reporter", method: "POST", timeout_ms: 350, body: "{\"component\":\"reporter\"}" } }], base_version: version },
    edge: { note: "为 worker 增加 cache 上游依赖", operations: [{ type: "add_edge", from: "cache", to: "worker" }], base_version: version },
    cycle: { note: "该依赖边将闭合环，预期返回 E_DEPENDENCY_CYCLE 且不改变当前版本", operations: [{ type: "add_edge", from: "notifier", to: "database" }], base_version: version },
    stale: { note: "故意使用陈旧基版本，预期返回 E_STALE_BASE", operations: [{ type: "patch_resource", component_id: "cache", resources: { cpu_millicores: 1500, memory_bytes: 4294967296 } }], base_version: Math.max(0, version - 1) },
  };
  const selected = operationSets[preset] || operationSets.resource;
  const key = makeKey(preset);
  document.getElementById("studioKey").value = key;
  document.getElementById("studioPayload").value = JSON.stringify({ idempotency_key: key, generated_at: new Date().toISOString(), ...selected }, null, 2);
  document.querySelectorAll("#presetStrip button").forEach((button) => button.classList.toggle("active", button.dataset.preset === preset));
  validateEditor();
  if (notify) toast(`已载入「${presetLabel(preset)}」变更模板`, "success");
}

function presetLabel(preset) {
  return { resource: "资源调整", probe: "探测参数", edge: "新增依赖", cycle: "依赖环拒绝", stale: "陈旧基线" }[preset] || preset;
}

function syncStudioKey() {
  const key = document.getElementById("studioKey").value.trim();
  const editor = document.getElementById("studioPayload");
  try {
    const payload = JSON.parse(editor.value);
    payload.idempotency_key = key;
    editor.value = JSON.stringify(payload, null, 2);
  } catch (_) {}
  validateEditor();
}

function validateEditor() {
  const status = document.getElementById("editorStatus");
  try {
    JSON.parse(document.getElementById("studioPayload").value);
    status.textContent = "VALID JSON";
    status.style.color = "var(--teal)";
    return true;
  } catch (error) {
    status.textContent = "INVALID JSON";
    status.style.color = "var(--red)";
    return false;
  }
}

function formatEditor() {
  const editor = document.getElementById("studioPayload");
  try {
    editor.value = JSON.stringify(JSON.parse(editor.value), null, 2);
    validateEditor();
  } catch (error) {
    toast(`JSON 无法格式化: ${error.message}`, "error");
  }
}

async function submitIncrement() {
  const button = document.getElementById("submitIncrementBtn");
  const editor = document.getElementById("studioPayload");
  if (!validateEditor()) {
    toast("增量 JSON 不合法，未发送请求", "error");
    return;
  }
  let payload;
  try {
    payload = JSON.parse(editor.value);
  } catch (error) {
    toast(error.message, "error");
    return;
  }
  payload.idempotency_key = document.getElementById("studioKey").value.trim();
  if (!payload.idempotency_key) {
    toast("幂等键不能为空", "error");
    return;
  }
  if (!payload.generated_at) payload.generated_at = new Date().toISOString();
  editor.value = JSON.stringify(payload, null, 2);
  const previousScore = app.state?.derivation?.consistency?.score_micro ?? 0;
  setStudioStatus("WORKING", "working");
  button.disabled = true;
  try {
    const result = await api("/api/v1/increments", { method: "POST", body: JSON.stringify(payload) });
    renderReceipt(result, previousScore);
    setStudioStatus(result.replayed ? "REPLAYED" : "COMMITTED");
    toast(result.replayed ? "幂等重放：复用已有提交结果" : `版本 v${result.version} 已原子发布`, "success");
    await refreshAll();
    await loadEvidence();
    advanceStudioBase(app.state?.version ?? result.version);
  } catch (error) {
    app.lastStudioError = error.payload || { error: { code: "E_CLIENT", message: error.message } };
    renderReceiptError(error);
    setStudioStatus(error.payload?.error?.code || "ERROR", "error");
    showErrorToast(error);
  } finally {
    button.disabled = false;
  }
}

function advanceStudioBase(version) {
  const editor = document.getElementById("studioPayload");
  try {
    const payload = JSON.parse(editor.value);
    payload.base_version = version;
    payload.idempotency_key = makeKey("ui");
    payload.generated_at = new Date().toISOString();
    editor.value = JSON.stringify(payload, null, 2);
    document.getElementById("studioKey").value = payload.idempotency_key;
    validateEditor();
  } catch (_) {
    // Keep malformed editor content intact; the next submit will surface the parse error.
  }
}

function setStudioStatus(text, variant = "") {
  const status = document.getElementById("studioResultStatus");
  status.textContent = text;
  status.className = `status-indicator ${variant}`;
}

function renderReceipt(result, previousScore) {
  const cascade = result.cascade;
  const score = result.consistency;
  const delta = score.score_micro - previousScore;
  document.getElementById("studioResult").innerHTML = `<div class="receipt">
    <div class="receipt-head"><div class="receipt-glyph"><svg><use href="#i-check"/></svg></div><div><h4>原子提交完成</h4><p>${esc(result.replayed ? "幂等重放回执" : "所有派生视图已切换至新版本")}</p></div><div class="receipt-version"><small>VERSION</small><strong>v${result.version}</strong></div></div>
    <div class="receipt-stats"><div class="receipt-stat"><span>INVALIDATED</span><strong>${cascade.invalidated.length}</strong></div><div class="receipt-stat"><span>REBUILT</span><strong>${cascade.rebuilt.length}</strong></div><div class="receipt-stat"><span>Δ CONSISTENCY</span><strong style="color:${delta < 0 ? "var(--red)" : "var(--teal)"}">${delta >= 0 ? "+" : ""}${(delta / 1000).toFixed(1)}‰</strong></div></div>
    <div class="receipt-row"><span>BASE → TARGET</span><code>v${result.base_version} → v${result.version}</code></div>
    <div class="receipt-row"><span>CONSISTENCY</span><code>${(score.score_micro / 1000).toFixed(3)}‰ · ${score.decision}</code></div>
    <div class="receipt-row"><span>CHECKSUM</span><code title="${esc(result.checksum_sha256)}">${esc(shortDigest(result.checksum_sha256, 30))}</code></div>
    <div class="receipt-row"><span>DERIVED DIGEST</span><code title="${esc(result.derived_digest)}">${esc(shortDigest(result.derived_digest, 30))}</code></div>
    <div class="receipt-row"><span>INTENT</span><code title="${esc(result.intent_id)}">${esc(shortDigest(result.intent_id, 32))}</code></div>
    <div class="receipt-row"><span>CASCADE STRATEGY</span><code>${esc(cascade.strategy)}</code></div>
  </div>`;
  renderCascade(cascade);
}

function renderReceiptError(error) {
  const payload = error.payload || {};
  const details = payload.error || { code: "E_CLIENT", message: error.message, context: {} };
  const partial = payload.partial_result;
  document.getElementById("studioResult").innerHTML = `<div class="receipt receipt-error">
    <div class="receipt-head"><div class="receipt-glyph" style="color:var(--red);background:var(--red-soft)"><svg><use href="#i-alert"/></svg></div><div><h4>提交被拒绝</h4><p>当前已提交版本保持不变</p></div></div>
    <div class="error-code">${esc(details.code)}</div><div class="error-message">${esc(details.message || error.message)}</div>
    <pre class="error-context">${esc(JSON.stringify(details.context || {}, null, 2))}</pre>
    ${partial ? `<button class="text-button" data-show-error style="margin-top:12px">查看部分推导结果 →</button>` : ""}</div>`;
  document.getElementById("cascadeDetail").innerHTML = `<div class="cascade-empty">${partial ? "候选版本产生依赖环；环内与下游组件标记受阻，未提交。完整部分推导可在回执中查看。" : "本次操作未发布派生结构。"}</div>`;
}

function renderCascade(cascade) {
  const changed = cascade.changed_refs || [];
  const invalidated = cascade.invalidated || [];
  const rebuilt = cascade.rebuilt || [];
  const refs = (items, className) => items.slice(0, 120).map((item) => `<span class="ref-chip ${className}">${esc(typeof item === "string" ? item : `${item.kind}:${item.key}:${item.field}`)}</span>`).join("");
  document.getElementById("cascadeDetail").innerHTML = `<div class="cascade-block">
    <div class="ref-group"><h4>Changed refs · ${changed.length}</h4><div class="ref-list">${refs(changed, "") || '<span class="ref-chip">none</span>'}</div></div>
    <div class="ref-group"><h4>Invalidated views · ${invalidated.length}</h4><div class="ref-list">${refs(invalidated, "invalidated") || '<span class="ref-chip">initial snapshot</span>'}</div></div>
    <div class="ref-group"><h4>Rebuilt views · ${rebuilt.length}</h4><div class="ref-list">${refs(rebuilt, "rebuilt")}</div></div></div>`;
}

async function loadSamples() {
  try {
    const data = await api("/api/v1/samples");
    app.samples = data.items;
    renderSamples();
  } catch (error) {
    showErrorToast(error);
  }
}

function renderSamples() {
  const container = document.getElementById("sampleList");
  if (!app.samples.length) {
    container.innerHTML = '<div class="empty-state small">无可用样本</div>';
    return;
  }
  container.innerHTML = app.samples.map((sample) => `<div class="sample-row"><div><strong>${esc(sample.name)}</strong><small>${esc(sample.description)}</small><em>${esc(sample.kind.toUpperCase())}</em></div><div class="sample-actions">${sample.kind === "snapshot" ? `<button data-sample="${esc(sample.name)}" data-action="load">载入</button>` : ""}${["snapshot", "invalid", "increment"].includes(sample.kind) ? `<button data-sample="${esc(sample.name)}" data-action="validate">校验</button>` : ""}<button data-sample="${esc(sample.name)}" data-action="raw">查看</button></div></div>`).join("");
}

async function handleSampleAction(event) {
  const button = event.target.closest("[data-sample]");
  if (!button) return;
  const name = button.dataset.sample;
  const action = button.dataset.action;
  try {
    if (action === "raw") {
      const response = await fetch(`/api/v1/samples/raw?name=${encodeURIComponent(name)}`);
      openModal(name, await response.json());
    } else if (action === "validate") {
      const data = await api("/api/v1/samples/validate", { method: "POST", body: JSON.stringify({ name }) });
      openModal(`校验通过 · ${name}`, data);
      toast("样本通过规范校验", "success");
    } else if (action === "load") {
      const data = await api("/api/v1/samples/load", { method: "POST", body: JSON.stringify({ name, idempotency_key: makeKey("sample") }) });
      toast(`样本已载入为 v${data.version}`, "success");
      await refreshAll();
      await loadEvidence();
    }
  } catch (error) {
    if (error.payload) openModal(`校验拒绝 · ${name}`, error.payload);
    showErrorToast(error);
  }
}

async function quickLoadSample() {
  const button = document.getElementById("quickLoadBtn");
  if ((app.state?.version ?? 0) > 0) {
    toast("数据库已有提交版本；完整快照仅用于空库初始化。请使用变更实验室或独立数据库。", "error");
    return;
  }
  button.disabled = true;
  try {
    const data = await api("/api/v1/samples/load", { method: "POST", body: JSON.stringify({ name: "full-snapshot.json", idempotency_key: makeKey("bootstrap") }) });
    toast(`固定样本已原子载入为 v${data.version}`, "success");
    await refreshAll();
    await loadEvidence();
  } catch (error) {
    showErrorToast(error);
  } finally {
    button.disabled = false;
  }
}

async function runDrill() {
  const button = document.getElementById("runDrillBtn");
  const selected = [...document.querySelectorAll("#failedPicker input:checked")].map((input) => input.value);
  if (!selected.length) {
    toast("至少选择一个故障组件", "error");
    return;
  }
  let key = document.getElementById("drillKey").value.trim();
  if (!key) {
    key = makeKey("drill");
    document.getElementById("drillKey").value = key;
  }
  const concurrency = Math.max(1, Math.min(16, Number(document.getElementById("drillConcurrency").value) || 2));
  const payload = { idempotency_key: key, version: app.state.version, failed_components: selected, max_concurrency: concurrency };
  document.getElementById("drillStatus").textContent = "RUNNING";
  document.getElementById("drillStatus").className = "status-indicator working";
  document.getElementById("drillResult").innerHTML = '<div class="empty-state"><div><div class="result-orbit" style="margin:0 auto 18px"><span></span><span></span><strong>…</strong></div>真实 HTTP 探测与恢复请求执行中</div></div>';
  button.disabled = true;
  startDrillPolling();
  try {
    const result = await api("/api/v1/drills", { method: "POST", body: JSON.stringify(payload) });
    renderDrill(result);
    document.getElementById("drillStatus").textContent = result.replayed ? "REPLAYED" : "COMPLETED";
    document.getElementById("drillStatus").className = "status-indicator";
    toast(result.all_operations_succeeded ? "演练完成，全部操作成功" : `演练完成，诊断码 ${result.operation_codes.join(" / ")}`, result.all_operations_succeeded ? "success" : "error");
  } catch (error) {
    document.getElementById("drillStatus").textContent = error.payload?.error?.code || "ERROR";
    document.getElementById("drillStatus").className = "status-indicator error";
    document.getElementById("drillResult").innerHTML = `<div class="empty-state"><div><div class="error-code">${esc(error.payload?.error?.code || "E_CLIENT")}</div><div class="error-message">${esc(error.message)}</div><button class="text-button" id="drillErrorDetail">查看上下文</button></div></div>`;
    document.getElementById("drillErrorDetail")?.addEventListener("click", () => openModal("演练错误", error.payload || error.message));
    showErrorToast(error);
  } finally {
    stopDrillPolling();
    button.disabled = false;
    await updateRuntimeEvidence();
    updateEndpointStatus();
  }
}

function renderDrill(result) {
  const batches = result.recovery_batches.map((batch) => {
    const components = batch.results.map((row) => {
      const op = (label, value) => `<div class="op-card ${value.ok ? "" : "failed"}"><span>${label}</span><code>${esc(value.code)}</code><small>${value.http_status ? `HTTP ${value.http_status} · ` : ""}${value.duration_ms}ms / ${value.timeout_ms}ms · ${value.connection_closed ? "closed" : "open"}</small></div>`;
      return `<div class="drill-component"><div class="drill-component-title"><strong>${esc(row.component_id)}</strong><span class="release-tag">${row.released ? "RESERVATION RELEASED" : "HELD"}</span></div><div class="op-grid">${op("HEALTH PROBE", row.probe)}${op("RECOVERY REQUEST", row.recovery)}</div></div>`;
    }).join("");
    return `<div class="drill-batch"><div class="drill-batch-head"><span>B${batch.batch}</span><strong>${esc(batch.components.join(" · "))}</strong></div>${components}</div>`;
  }).join("");
  document.getElementById("drillResult").innerHTML = `<div class="drill-summary"><div><span>BATCHES</span><strong>${result.recovery_batches.length}</strong></div><div><span>OPERATIONS</span><strong>${Object.values(result.code_counts).reduce((sum, value) => sum + value, 0)}</strong></div><div><span>DISTINCT CODES</span><strong>${result.operation_codes.length}</strong></div><div><span>RELEASED</span><strong>${result.reservations_released ? "YES" : "NO"}</strong></div></div><div class="drill-batches">${batches}</div>`;
}

function startDrillPolling() {
  stopDrillPolling();
  updateRuntimeEvidence();
  app.drillTimer = setInterval(updateRuntimeEvidence, 450);
}

function stopDrillPolling() {
  if (app.drillTimer) clearInterval(app.drillTimer);
  app.drillTimer = null;
}

async function updateRuntimeEvidence() {
  try {
    const data = await api("/api/v1/runtime/reservations");
    const container = document.getElementById("runtimeEvidence");
    if (!container) return;
    container.innerHTML = `<div><span>ACTIVE RESERVATIONS</span><strong style="color:${data.active.length ? "var(--amber)" : "var(--teal)"}">${data.active.length}</strong></div><div><span>RELEASED TOTAL</span><strong>${data.released_total}</strong></div><div><span>TIMEOUT CONTRACT</span><strong>CLOSE + RELEASE</strong></div><div><span>PUBLICATION</span><strong>ONE TRANSACTION</strong></div>`;
  } catch (_) {}
}

async function updateEndpointStatus() {
  const status = document.getElementById("endpointStatus");
  const dot = status.querySelector(".live-dot");
  const small = status.querySelector("small");
  try {
    const response = await fetch("http://127.0.0.1:19090/stats", { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const stats = await response.json();
    dot.style.background = "var(--teal)";
    small.textContent = `online · ${stats.completed} completed · ${stats.cancelled} cancelled`;
  } catch (error) {
    dot.style.background = "var(--amber)";
    small.textContent = "未运行 · 启动 cmd/testendpoint 以执行真实探测";
  }
}

async function loadEvidence() {
  try {
    const [versions, intents, reports] = await Promise.all([
      api("/api/v1/versions?limit=50"),
      api(`/api/v1/intents?limit=100${app.intentFilter ? `&state=${encodeURIComponent(app.intentFilter)}` : ""}`),
      api("/api/v1/recovery-reports?limit=20"),
    ]);
    app.versions = versions.items;
    app.intents = intents.items;
    app.reports = reports.items;
    renderVersions();
    renderReports();
    renderIntents();
  } catch (error) {
    showErrorToast(error);
  }
}

async function loadIntents() {
  try {
    const data = await api(`/api/v1/intents?limit=100${app.intentFilter ? `&state=${encodeURIComponent(app.intentFilter)}` : ""}`);
    app.intents = data.items;
    renderIntents();
  } catch (error) {
    showErrorToast(error);
  }
}

function renderVersions() {
  const body = document.getElementById("versionsRows");
  if (!app.versions.length) {
    body.innerHTML = '<tr><td colspan="3" class="muted-cell">尚无提交版本</td></tr>';
    return;
  }
  body.innerHTML = app.versions.map((version) => `<tr><td class="name">v${version.version}</td><td>${esc(formatDate(version.generated_at))}</td><td><code class="mono" title="${esc(version.checksum_sha256)}">${esc(shortDigest(version.checksum_sha256, 28))}</code></td></tr>`).join("");
}

function renderReports() {
  const container = document.getElementById("recoveryReport");
  const report = app.reports[0];
  if (!report) {
    container.innerHTML = '<div class="empty-state small">暂无报告</div>';
    return;
  }
  container.innerHTML = `<div class="recovery-report"><div class="report-head"><strong>启动扫描 #${report.id}</strong><span class="state-badge ${report.scanned_intents ? "aborted" : "committed"}">${report.scanned_intents} ABORTED</span></div><div class="report-grid"><div><span>LAST COMMITTED</span><strong>v${report.last_committed_version}</strong></div><div><span>SCANNED INTENTS</span><strong>${report.scanned_intents}</strong></div><div><span>STARTED AT</span><strong>${esc(formatDate(report.started_at))}</strong></div><div><span>REPORT BASIS</span><strong>PERSISTED ONLY</strong></div></div><pre class="report-facts">${esc(JSON.stringify(report.facts, null, 2))}</pre></div>`;
}

function renderIntents() {
  const body = document.getElementById("intentsRows");
  if (!app.intents.length) {
    body.innerHTML = '<tr><td colspan="7" class="muted-cell">无匹配意图</td></tr>';
    return;
  }
  body.innerHTML = app.intents.map((intent) => `<tr><td class="mono" title="${esc(intent.id)}">${esc(shortDigest(intent.id, 20))}</td><td>${esc(intent.kind)}</td><td>v${intent.base_version}</td><td>v${intent.target_version}</td><td>${esc(intent.phase)}</td><td><span class="state-badge ${esc(intent.state)}">${esc(intent.state)}</span></td><td>${esc(formatDate(intent.created_at))}</td></tr>`).join("");
}

async function loadLineage() {
  try {
    const data = await api(`/api/v1/lineage?version=${app.state.version}`);
    const byKind = {};
    const byRef = {};
    for (const ref of data.refs) {
      byKind[ref.derived_kind] = (byKind[ref.derived_kind] || 0) + 1;
      byRef[ref.ref_kind] = (byRef[ref.ref_kind] || 0) + 1;
    }
    const cards = Object.entries(byKind).sort().map(([kind, count]) => `<div class="lineage-card"><span>${esc(kind)}</span><strong>${count}</strong><small>引用边</small></div>`).join("");
    const refs = Object.entries(byRef).sort().map(([kind, count]) => `<span class="ref-chip">${esc(kind)} · ${count}</span>`).join("");
    document.getElementById("lineageSummary").innerHTML = `<div class="lineage-grid">${cards}</div><div class="ref-group" style="margin-top:12px"><h4>Ref kinds · total ${data.count}</h4><div class="ref-list">${refs}</div></div>`;
  } catch (error) {
    showErrorToast(error);
  }
}

async function verifyConsistency() {
  try {
    const data = await api("/api/v1/consistency?repeat=3");
    openModal(`固定点重复验证 · v${data.version}`, { reproducible: data.reproducible, score_micro: data.result.score_micro, decision: data.result.decision, tolerance_micro: data.result.tolerance_micro, delta_to_threshold_micro: data.result.delta_to_threshold_micro, denominator: data.result.denominator, compensation: data.result.compensation, rounding_error_ppb: data.result.rounding_error_ppb, digests: data.digests });
    toast(data.reproducible ? "三次重算摘要逐位一致" : "重算摘要不一致", data.reproducible ? "success" : "error");
  } catch (error) {
    showErrorToast(error);
  }
}

function switchView(name, updateHash = true) {
  if (!VIEW_META[name]) return;
  app.currentView = name;
  document.querySelectorAll(".view").forEach((view) => view.classList.toggle("active", view.id === `view-${name}`));
  document.querySelectorAll(".nav-item").forEach((button) => button.classList.toggle("active", button.dataset.view === name));
  document.getElementById("pageEyebrow").textContent = VIEW_META[name][0];
  document.getElementById("pageTitle").textContent = VIEW_META[name][1];
  if (updateHash) history.replaceState(null, "", `#${name}`);
  if (name === "evidence") loadEvidence();
  if (name === "drill") updateRuntimeEvidence();
  window.scrollTo({ top: 0, behavior: "smooth" });
}

function makeKey(prefix) {
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function formatDate(value) {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString("zh-CN", { hour12: false });
}

function shortDigest(value, length) {
  if (!value) return "—";
  const text = String(value);
  const max = length || 20;
  return text.length <= max ? text : `${text.slice(0, max)}…`;
}

function trimText(value, length) {
  return value.length <= length ? value : `${value.slice(0, length - 1)}…`;
}

function esc(value) {
  return String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#039;");
}

function toast(message, kind = "") {
  const stack = document.getElementById("toastStack");
  const node = document.createElement("div");
  node.className = `toast ${kind}`;
  node.textContent = message;
  stack.appendChild(node);
  setTimeout(() => {
    node.style.opacity = "0";
    node.style.transform = "translateY(6px)";
    setTimeout(() => node.remove(), 200);
  }, 4600);
}

function showErrorToast(error) {
  const code = error.payload?.error?.code;
  toast(code ? `${code}: ${error.payload.error.message}` : error.message, "error");
}

function openModal(title, value) {
  document.getElementById("modalTitle").textContent = title;
  document.getElementById("modalBody").textContent = typeof value === "string" ? value : JSON.stringify(value, null, 2);
  document.getElementById("modalBackdrop").classList.add("open");
}

function closeModal() {
  document.getElementById("modalBackdrop").classList.remove("open");
}
