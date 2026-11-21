const state = {
  health: null,
  policies: [],
  batches: [],
  batch: null,
  compare: null,
  recovery: null,
  selectedBatchId: null,
  selectedMode: "FULL",
  selectedPolicy: null,
  selectedWindow: null,
  activeTab: "overview",
  busy: false,
  error: null,
};

const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

class ApiError extends Error {
  constructor(status, payload) {
    const error = payload?.error || {};
    super(error.message || `HTTP ${status}`);
    this.name = "ApiError";
    this.status = status;
    this.code = error.code || "E_HTTP";
    this.context = error.context || {};
    this.requestId = payload?.request_id;
  }
}

async function api(path, options = {}) {
  const init = { method: options.method || "GET", headers: { Accept: "application/json" } };
  if (options.body !== undefined) {
    init.headers["Content-Type"] = "application/json";
    init.body = JSON.stringify(options.body);
  }
  let response;
  try {
    response = await fetch(path, init);
  } catch (error) {
    throw new ApiError(0, {
      error: { code: "E_NETWORK", message: "无法连接本地服务", context: { detail: String(error) } },
    });
  }
  let payload;
  try {
    payload = await response.json();
  } catch {
    payload = { ok: false, error: { code: "E_PROTOCOL", message: "服务返回了非 JSON 响应" } };
  }
  if (!response.ok || !payload.ok) throw new ApiError(response.status, payload);
  return payload.data;
}

function esc(value) {
  return String(value ?? "—")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function fmt(value, digits = 4) {
  if (value === null || value === undefined || value === "") return "—";
  const number = Number(value);
  if (!Number.isFinite(number)) return String(value);
  return number.toLocaleString("en-US", { maximumFractionDigits: digits });
}

function short(value, length = 10) {
  const text = String(value ?? "");
  return text.length > length ? `${text.slice(0, length)}…` : text;
}

function formatTime(ms) {
  if (ms === null || ms === undefined) return "—";
  const date = new Date(Number(ms));
  if (Number.isNaN(date.getTime())) return String(ms);
  return date.toISOString().replace("T", " ").replace(".000Z", "Z");
}

function riskTone(level) {
  return { LOW: "mint", MEDIUM: "amber", HIGH: "red", CRITICAL: "red" }[level] || "blue";
}

const STATUS_LABELS = {
  OBSERVING: "观察中",
  PASSED: "已放行",
  ROLLING_BACK: "回滚中",
  ROLLED_BACK: "已回滚",
  ESCALATED: "已升级",
};
const ACTION_LABELS = {
  RELEASE: "放行",
  AUTO_ROLLBACK: "自动回滚",
  ESCALATE: "升级人工",
  CONTINUE_OBSERVING: "继续观察",
  REQUIRES_FULL_REVIEW: "需完整复核",
  FAST_RELEASE_ADVISORY: "放行建议（只读）",
  FAST_CONTINUE_OBSERVING: "继续观察建议",
};
const CHANGE_LABELS = {
  CONFIG_APPLY: "配置下发",
  IMAGE_UPGRADE: "镜像升级",
  SCALE: "扩缩容",
};
const FIELD_LABELS = {
  risk_score: "风险分",
  risk_level: "风险等级",
  decision_action: "裁决动作",
  sample_count: "样本数",
  error_rate: "错误率",
  success_rate: "成功率",
  p90_latency_ms: "P90 延迟 (ms)",
};

function toast(title, message = "", tone = "success") {
  const node = document.createElement("div");
  node.className = `toast ${tone}`;
  node.innerHTML = `<strong>${esc(title)}</strong><span>${esc(message)}</span>`;
  $("#toast-stack").appendChild(node);
  setTimeout(() => node.remove(), 5200);
}

function setBusy(busy) {
  state.busy = busy;
  $$(".btn, .mode-option, .tabs button").forEach((button) => {
    if (button.id !== "recovery-close") button.disabled = busy;
  });
}

async function boot() {
  bindEvents();
  await loadAll();
}

async function loadAll(preferredBatchId = null) {
  setBusy(true);
  try {
    const [health, policyData, batchData, recovery] = await Promise.all([
      api("/api/health"),
      api("/api/policies"),
      api("/api/batches"),
      api("/api/recovery/report"),
    ]);
    state.health = health;
    state.policies = policyData.policies || [];
    state.batches = batchData.batches || [];
    state.recovery = recovery;
    renderGlobal();
    renderPolicySelect();
    renderBatchList();
    if (!state.batches.length) {
      state.batch = null;
      state.compare = null;
      $("#empty-state").hidden = false;
      $("#batch-view").hidden = true;
      return;
    }
    $("#empty-state").hidden = true;
    $("#batch-view").hidden = false;
    const target =
      preferredBatchId ||
      state.selectedBatchId ||
      state.batches.find((item) => item.status === "OBSERVING")?.batch_id ||
      state.batches[0].batch_id;
    await selectBatch(target, { skipBusy: true });
  } catch (error) {
    showFatal(error);
  } finally {
    setBusy(false);
  }
}

function renderGlobal() {
  const health = state.health;
  const pill = $("#health-pill");
  pill.className = "live-pill ok";
  pill.innerHTML = `<span></span> local · ${health.batch_count} batches`;
  $("#db-badge").textContent = (health.database_path || "—").split(/[\\/]/).pop();
  const latest = state.policies[0];
  $("#policy-badge").textContent = latest ? `v${latest.version} · ${short(latest.digest, 8)}` : "none";
}

function renderPolicySelect() {
  const select = $("#policy-select");
  select.innerHTML = state.policies
    .map(
      (policy) =>
        `<option value="${policy.version}">v${policy.version} · digest ${short(policy.digest, 10)}</option>`,
    )
    .join("");
  if (state.selectedPolicy !== null) select.value = String(state.selectedPolicy);
  else if (state.policies[0]) select.value = String(state.policies[0].version);
  renderPolicyDigest();
}

function renderPolicyDigest() {
  const version = Number($("#policy-select").value);
  const policy = state.policies.find((item) => item.version === version);
  $("#policy-digest").textContent = policy ? `SHA-256 ${policy.digest}` : "—";
  if (policy && (!state.selectedPolicy || state.selectedPolicy === version)) {
    state.selectedPolicy = version;
  }
}

function renderBatchList() {
  $("#batch-count").textContent = String(state.batches.length);
  const container = $("#batch-list");
  if (!state.batches.length) {
    container.innerHTML = `<div class="batch-service">尚无批次</div>`;
    return;
  }
  container.innerHTML = state.batches
    .map((batch) => {
      const latest = batch.latest_decision;
      const risk = latest ? latest.risk_score : null;
      const active = batch.batch_id === state.selectedBatchId ? " active" : "";
      return `
        <button type="button" class="batch-item${active}" data-batch-id="${esc(batch.batch_id)}">
          <div class="batch-line">
            <span class="batch-id">${esc(batch.batch_id)}</span>
            <span class="status-badge status-${esc(batch.status)}">${esc(STATUS_LABELS[batch.status] || batch.status)}</span>
          </div>
          <div class="batch-service">${esc(batch.service)} · ${esc(CHANGE_LABELS[batch.change_type] || batch.change_type)}</div>
          <div class="batch-risk"><span>risk ${risk === null ? "—" : esc(fmt(risk, 3))}</span><span>${batch.windows.reduce((sum, item) => sum + item.sample_count, 0)} samples</span></div>
        </button>`;
    })
    .join("");
}

async function selectBatch(batchId, { skipBusy = false } = {}) {
  if (!skipBusy) setBusy(true);
  try {
    const batch = await api(`/api/batches/${encodeURIComponent(batchId)}`);
    state.selectedBatchId = batchId;
    state.batch = batch;
    state.selectedPolicy = batch.policy_version;
    const windows = batch.windows || [];
    state.selectedWindow = windows.length ? windows[windows.length - 1].window_index : null;
    $("#policy-select").value = String(state.selectedPolicy);
    renderPolicyDigest();
    $("#decision-time").value = String(Date.now());
    renderBatchBase();
    renderWindows();
    renderTrajectory();
    renderDecisions();
    renderPolicyPanel();
    renderBatchList();
    await refreshComparison();
  } catch (error) {
    showFatal(error);
    state.error = error;
    renderError();
  } finally {
    if (!skipBusy) setBusy(false);
  }
}

function renderBatchBase() {
  const batch = state.batch;
  if (!batch) return;
  $("#batch-eyebrow").textContent = `${batch.service} · ${CHANGE_LABELS[batch.change_type] || batch.change_type}`;
  $("#batch-title").textContent = batch.batch_id;
  const status = $("#batch-status");
  status.className = `status-badge status-${batch.status}`;
  status.textContent = STATUS_LABELS[batch.status] || batch.status;
  const samples = (batch.windows || []).reduce((sum, window) => sum + window.sample_count, 0);
  $("#batch-meta").innerHTML = [
    `规模 <strong>${esc(batch.batch_size)}</strong>`,
    `策略 <strong>v${esc(batch.policy_version)}</strong>`,
    `窗口 <strong>${esc(batch.window_ms)} ms</strong>`,
    `观测 <strong>${esc(samples)}</strong>`,
    `epoch <strong>${esc(batch.window_epoch)}</strong>`,
  ]
    .map((item) => `<span>${item}</span>`)
    .join("");
  const windowSelect = $("#window-select");
  windowSelect.innerHTML = (batch.windows || [])
    .map(
      (window) =>
        `<option value="${window.window_index}">#${window.window_index} · v${window.version} · ${window.state}</option>`,
    )
    .join("");
  if (state.selectedWindow !== null) windowSelect.value = String(state.selectedWindow);
  renderModeControl();
}

function renderModeControl() {
  $$(".mode-option").forEach((button) => {
    button.classList.toggle("active", button.dataset.mode === state.selectedMode);
  });
  const button = $("#decide-btn");
  if (state.selectedMode === "FAST") {
    button.textContent = "提交快速只读裁决";
  } else {
    button.textContent = "提交完整裁决";
  }
}

async function refreshComparison() {
  if (!state.selectedBatchId) return;
  state.error = null;
  hideBanners();
  try {
    const params = new URLSearchParams();
    if (state.selectedPolicy !== null) params.set("policy_version", String(state.selectedPolicy));
    if (state.selectedWindow !== null) params.set("window_index", String(state.selectedWindow));
    const compare = await api(
      `/api/batches/${encodeURIComponent(state.selectedBatchId)}/compare?${params.toString()}`,
    );
    state.compare = compare;
    renderComparison();
  } catch (error) {
    state.compare = null;
    state.error = error;
    renderComparison();
    renderError();
  }
}

function renderComparison() {
  if (!state.compare) {
    renderRiskCards(null, state.error);
    $("#difference-table").innerHTML = "";
    $("#confidence-panel").innerHTML = "";
    $("#hero-risk").textContent = "—";
    return;
  }
  const { full, fast } = state.compare;
  renderRiskCards(state.compare, null);
  $("#hero-risk").textContent = fmt(full.risk_score, 5);
  $("#hero-threshold").textContent = `threshold ${fmt(full.risk_threshold, 2)} · ${full.risk_level}`;
  $("#hero-risk").style.color = full.risk_level === "LOW" ? "var(--mint)" : full.risk_level === "MEDIUM" ? "var(--amber)" : "var(--red)";
  $("#integrity-chip").textContent =
    Object.values(state.compare.assertions).every(Boolean) ? "同一版本快照" : "版本校验失败";
  renderDifferences();
  renderConfidence();
  renderComponents();
  renderLedger();
  renderPolicyPanel();
}

function renderRiskCards(compare, error) {
  const fullContainer = $("#full-card");
  const fastContainer = $("#fast-card");
  if (error) {
    fullContainer.innerHTML = errorCard("FULL", "完整裁决", error);
    fastContainer.innerHTML = errorCard("FAST", "快速裁决", error);
    return;
  }
  if (!compare) {
    fullContainer.innerHTML = loadingCard("FULL", "完整裁决");
    fastContainer.innerHTML = loadingCard("FAST", "快速裁决");
    return;
  }
  fullContainer.innerHTML = riskCardHTML("FULL", "完整裁决", compare.full, false);
  fastContainer.innerHTML = riskCardHTML("FAST", "快速裁决", compare.fast, true);
}

function loadingCard(mode, title) {
  return `<div class="risk-card-head"><div><div class="mode-label">${mode}</div><h3 class="mode-title">${title}</h3></div></div><div class="skeleton" style="height:150px;margin-top:24px"></div>`;
}

function errorCard(mode, title, error) {
  const context = JSON.stringify(error.context || {}, null, 2);
  return `
    <div class="risk-card-head"><div><div class="mode-label">${mode}</div><h3 class="mode-title">${title}</h3></div><span class="risk-level level-HIGH">${esc(error.code)}</span></div>
    <div class="review-box" style="margin-top:22px;min-height:130px">
      <strong>${esc(error.message)}</strong>
      <pre class="code-block" style="margin-top:10px;max-height:180px">${esc(context)}</pre>
    </div>`;
}

function riskCardHTML(mode, title, risk, isFast) {
  const level = risk.risk_level || "—";
  const tone = riskTone(level);
  const value = Math.max(0, Math.min(100, Number(risk.risk_score) || 0));
  const color = tone === "mint" ? "var(--mint)" : tone === "amber" ? "var(--amber)" : "var(--red)";
  const action = risk.decision?.action || "—";
  const actionClass = ["AUTO_ROLLBACK", "ESCALATE"].includes(action)
    ? "danger"
    : ["REQUIRES_FULL_REVIEW", "FAST_CONTINUE_OBSERVING"].includes(action)
      ? "warn"
      : "";
  const confidence = risk.confidence || {};
  const lower = Number(confidence.lower);
  const upper = Number(confidence.upper);
  const bandHTML = Number.isFinite(lower) && Number.isFinite(upper)
    ? `<div class="confidence-band"><i style="left:${Math.max(0, lower)}%;width:${Math.max(0, Math.min(100, upper) - Math.max(0, lower))}%"></i></div>
       <div class="confidence-caption"><span>CI ${esc(fmt(lower, 3))}</span><span>${esc(fmt(upper, 3))}</span></div>`
    : "";
  return `
    <div class="risk-card-head">
      <div><div class="mode-label">${mode} · ${isFast ? "ADVISORY ONLY" : "EXHAUSTIVE"}</div><h3 class="mode-title">${title}</h3></div>
      <span class="risk-level level-${esc(level)}">${esc(level)}</span>
    </div>
    <div class="risk-body">
      <div class="gauge" style="--value:${value};--gauge-color:${color}">
        <div class="gauge-center"><strong>${esc(fmt(risk.risk_score, 5))}</strong><small>risk / 100</small></div>
      </div>
      <div class="risk-facts">
        <div class="fact-row"><span>裁决</span><strong>${esc(ACTION_LABELS[action] || action)}</strong></div>
        <div class="fact-row"><span>样本</span><strong>${esc(risk.candidate_stats?.sample_count)} / ${esc(risk.population_stats?.sample_count)}</strong></div>
        <div class="fact-row"><span>错误率</span><strong>${esc(fmt(risk.candidate_stats?.error_rate, 5))}</strong></div>
        <div class="fact-row"><span>P90</span><strong>${esc(fmt(risk.candidate_stats?.p90_latency_ms, 3))} ms</strong></div>
        <div class="fact-row"><span>窗口 v / epoch</span><strong>${esc(risk.window_versions?.window_version)} / ${esc(risk.window_versions?.batch_window_epoch)}</strong></div>
      </div>
    </div>
    ${bandHTML}
    <div class="decision-strip">
      <div><strong class="action-tag ${actionClass}">${esc(ACTION_LABELS[action] || action)}</strong><small>${esc(risk.decision?.rationale || risk.decision?.narrative || "—")}</small></div>
      <span class="mono-chip">${isFast ? "NO WRITE" : "ATOMIC"}</span>
    </div>`;
}

function renderDifferences() {
  const differences = state.compare?.differences || [];
  const rows = differences
    .map((item) => {
      const delta = item.delta_fast_minus_full;
      const deltaClass = delta === null ? "" : Number(delta) > 0 ? "delta-pos" : Number(delta) < 0 ? "delta-neg" : "";
      return `<tr>
        <td>${esc(FIELD_LABELS[item.field] || item.field)}</td>
        <td class="mono">${esc(item.full)}</td>
        <td class="mono">${esc(item.fast)}</td>
        <td class="mono ${deltaClass}">${esc(delta ?? "—")}</td>
        <td><span class="${item.same ? "same-yes" : "same-no"}">${item.same ? "一致" : "有差异"}</span></td>
      </tr>`;
    })
    .join("");
  $("#difference-table").innerHTML = `
    <table><thead><tr><th>字段</th><th>FULL</th><th>FAST</th><th>Δ</th><th>核对</th></tr></thead><tbody>${rows}</tbody></table>`;
}

function renderConfidence() {
  const { full, fast } = state.compare;
  const confidence = fast.confidence || {};
  const detail = fast.decision || {};
  const safe = !detail.requires_full_review;
  const reasons = (detail.reasons || []).map((reason) => `<li>${esc(reason)}</li>`).join("");
  $("#confidence-panel").innerHTML = `
    <div class="panel-head"><div><span class="eyebrow">CONFIDENCE</span><h3>快速结论可核对性</h3></div><span class="mono-chip">${esc(confidence.confidence_level || "—")}</span></div>
    <div class="confidence-metric"><span>点估计风险</span><strong>${esc(fast.risk_score)}</strong></div>
    <div class="confidence-metric"><span>置信区间</span><strong>[${esc(fmt(confidence.lower, 5))}, ${esc(fmt(confidence.upper, 5))}]</strong></div>
    <div class="confidence-metric"><span>置信上界间距 / 配置上限</span><strong>${esc(fmt(detail.confidence_upper_gap, 5))} / ${esc(fmt(detail.confidence_upper_bound, 2))}</strong></div>
    <div class="confidence-metric"><span>抽样率</span><strong>${esc(fast.sampling?.sample_rate || "—")} · ${esc(fast.sampling?.method || "—")}</strong></div>
    <div class="confidence-metric"><span>窗口状态</span><strong>${esc(fast.window?.state || "—")}</strong></div>
    <div class="review-box ${safe ? "safe" : ""}">
      <strong>${safe ? "快速预判未越阈" : "必须完整复核"}</strong>
      <ul>${reasons}</ul>
    </div>
    <div class="formula-text">FULL 使用 ${esc(full.candidate_stats?.sample_count)} 条全量观测；FAST 使用 ${esc(fast.candidate_stats?.sample_count)} 条。两者策略版本 ${esc(full.policy_version)}、窗口版本 ${esc(full.window_versions?.window_version)}、epoch ${esc(full.window_versions?.batch_window_epoch)} 对齐。</div>`;
}

function renderComponents() {
  const risk = state.compare?.full;
  if (!risk) {
    $("#component-grid").innerHTML = "";
    $("#formula-panel").innerHTML = "";
    return;
  }
  const labels = {
    error_rate: ["错误率压力", "candidate / baseline"],
    p90_latency: ["P90 延迟偏离", "candidate / baseline"],
    success_rate: ["成功率下降", "baseline / candidate"],
  };
  const rows = Object.entries(risk.components || {})
    .map(([key, component]) => {
      const pressure = Number(component.normalized_pressure) * 100;
      const [label, sub] = labels[key] || [key, ""];
      const left = key === "error_rate" ? component.candidate : key === "p90_latency" ? `${component.candidate_ms} ms` : component.candidate;
      const right = key === "error_rate" ? component.baseline : key === "p90_latency" ? `${component.baseline_ms} ms` : component.baseline;
      return `<div class="component-row">
        <div class="component-head"><strong>${esc(label)}</strong><span>${esc(fmt(component.points, 5))} pts</span></div>
        <div class="bar ${pressure >= 80 ? "red" : ""}"><i style="width:${Math.min(100, pressure)}%"></i></div>
        <div class="component-foot"><span>${esc(sub)} ${esc(left)} / ${esc(right)}</span><span>${esc(fmt(pressure, 2))}% pressure</span></div>
      </div>`;
    })
    .join("");
  $("#component-grid").innerHTML = rows;
  const rounding = risk.rounding || {};
  $("#formula-panel").innerHTML = `
    <div class="panel-head"><div><span class="eyebrow">EXPLAIN</span><h3>计算契约</h3></div></div>
    <div class="formula-text">${esc(risk.formula)}</div>
    <div class="mini-metrics">
      <div class="mini-metric"><span>P90 方法</span><strong style="font-size:10px">${esc(risk.p90_contract?.method)}</strong></div>
      <div class="mini-metric"><span>样本规模</span><strong>${esc(risk.p90_contract?.sample_size)}</strong></div>
      <div class="mini-metric"><span>定点比例</span><strong>${esc(rounding.fixed_point_scale)}</strong></div>
      <div class="mini-metric"><span>累计舍入误差</span><strong style="font-size:10px">${esc(rounding.cumulative_rounding_error)}</strong></div>
    </div>
    <div class="formula-text">${esc(risk.p90_contract?.definition || "—")}</div>`;
}

function renderLedger() {
  const risk = state.compare?.full;
  if (!risk) {
    $("#ledger-table").innerHTML = "";
    return;
  }
  const rounding = risk.rounding || {};
  $("#ledger-total").textContent = `cum err ${rounding.cumulative_rounding_error_micros} µ`;
  const rows = (rounding.steps || [])
    .map(
      (step) => `<tr>
        <td class="mono">${esc(step.id)}</td>
        <td class="mono">${esc(step.exact)}</td>
        <td class="mono">${esc(step.rounded)}</td>
        <td class="mono">${esc(step.fixed_point_value)}</td>
        <td class="mono">${esc(step.absolute_error)}</td>
        <td>${esc(step.rounding)}</td>
      </tr>`,
    )
    .join("");
  $("#ledger-table").innerHTML = `
    <table><thead><tr><th>步骤</th><th>精确值</th><th>舍入值</th><th>定点整数</th><th>绝对误差</th><th>舍入规则</th></tr></thead><tbody>${rows}</tbody></table>`;
}

function renderWindows() {
  const windows = state.batch?.windows || [];
  if (!windows.length) {
    $("#window-list").innerHTML = `<div class="batch-service">尚无窗口</div>`;
    return;
  }
  $("#window-list").innerHTML = windows
    .map(
      (window) => `<div class="window-item">
        <div class="window-top"><span class="window-title">WINDOW #${esc(window.window_index)}</span><span class="window-state ${esc(window.state)}">${esc(window.state)}</span></div>
        <div class="window-grid">
          <div>版本<strong>v${esc(window.version)}</strong></div>
          <div>样本<strong>${esc(window.sample_count)}</strong></div>
          <div>错误率<strong>${esc(fmt(window.error_rate, 4))}</strong></div>
          <div>成功率<strong>${esc(fmt(window.success_rate, 4))}</strong></div>
          <div>窗口起<strong>${esc(formatTime(window.start_ms))}</strong></div>
          <div>窗口止<strong>${esc(formatTime(window.end_ms))}</strong></div>
          <div>关闭边界<strong>${esc(formatTime(window.close_boundary_ms))}</strong></div>
          <div>watermark<strong>${esc(formatTime(window.watermark_ms))}</strong></div>
        </div>
      </div>`,
    )
    .join("");
}

function renderTrajectory() {
  const trajectory = state.batch?.transitions || [];
  $("#trajectory-list").innerHTML = trajectory
    .map(
      (item) => `<div class="trajectory-item">
        <strong>${esc(STATUS_LABELS[item.from_status] || item.from_status || "∅")} → ${esc(STATUS_LABELS[item.to_status] || item.to_status)}</strong>
        <small>${esc(item.action)} · policy v${esc(item.policy_version)} · window ${esc(item.window_version)} · epoch ${esc(item.window_epoch)} · risk ${esc(fmt(item.risk_score, 4))}</small>
      </div>`,
    )
    .join("");
}

function renderDecisions() {
  const decisions = state.batch?.recent_decisions || [];
  if (!decisions.length) {
    $("#decision-table").innerHTML = `<div class="batch-service">尚无持久化裁决</div>`;
    return;
  }
  const rows = decisions
    .map(
      (decision) => `<tr>
        <td class="mono">${esc(short(decision.decision_id, 18))}</td>
        <td>${esc(decision.mode)}</td>
        <td class="mono">${esc(decision.risk_score)}</td>
        <td>${esc(ACTION_LABELS[decision.action] || decision.action)}</td>
        <td>${decision.applied ? "已应用" : "只读"}</td>
        <td class="mono">${esc(decision.window_version)} / ${esc(decision.window_epoch)}</td>
        <td class="mono">${esc(formatTime(decision.decision_time_ms))}</td>
      </tr>`,
    )
    .join("");
  $("#decision-table").innerHTML = `<table><thead><tr><th>决策</th><th>模式</th><th>风险</th><th>动作</th><th>应用</th><th>窗口 v/epoch</th><th>裁决时间</th></tr></thead><tbody>${rows}</tbody></table>`;
}

function renderPolicyPanel() {
  const version = Number(state.selectedPolicy ?? state.batch?.policy_version);
  const policy = state.policies.find((item) => item.version === version);
  if (!policy) return;
  $("#policy-version-chip").textContent = `v${policy.version}`;
  const facts = [
    ["摘要", short(policy.digest, 18)],
    ["窗口 / 容忍", `${policy.window_ms} / ${policy.late_tolerance_ms} ms`],
    ["最小样本", policy.min_samples],
    ["风险阈值", policy.risk_threshold],
    ["自动回滚", policy.auto_rollback_enabled ? "开启" : "关闭"],
    ["冷却期", `${policy.cooldown_ms} ms`],
    ["错误率阈值", policy.thresholds.error_rate],
    ["P90 偏离阈值", policy.thresholds.p90_latency_increase],
    ["成功率下降阈值", policy.thresholds.success_rate_drop],
    ["快速采样上限", policy.fast.sample_cap],
    ["置信水平", policy.fast.confidence_level],
  ];
  $("#policy-facts").innerHTML = facts
    .map(([key, value]) => `<div class="policy-fact"><span>${esc(key)}</span><strong>${esc(value)}</strong></div>`)
    .join("");
  $("#policy-json").textContent = JSON.stringify(policy, null, 2);
}

function renderError() {
  const banner = $("#error-banner");
  if (!state.error) {
    banner.hidden = true;
    return;
  }
  banner.hidden = false;
  banner.innerHTML = `<strong>${esc(state.error.code)}</strong>${esc(state.error.message)}<br><span class="mono">${esc(JSON.stringify(state.error.context))}</span>`;
}

function hideBanners() {
  $("#error-banner").hidden = true;
  $("#notice-banner").hidden = true;
}

function showNotice(title, message) {
  const banner = $("#notice-banner");
  banner.hidden = false;
  banner.innerHTML = `<strong>${esc(title)}</strong>${esc(message)}`;
}

function showFatal(error) {
  const pill = $("#health-pill");
  pill.className = "live-pill bad";
  pill.innerHTML = `<span></span> ${esc(error.code || "offline")}`;
  state.error = error;
  renderError();
  toast("请求失败", `${error.code}: ${error.message}`, "error");
}

async function submitDecision() {
  if (!state.selectedBatchId || state.busy) return;
  setBusy(true);
  hideBanners();
  try {
    const body = {
      mode: state.selectedMode,
      policy_version: Number(state.selectedPolicy),
      decision_time_ms: Number($("#decision-time").value),
    };
    if (state.selectedWindow !== null) body.window_index = Number(state.selectedWindow);
    const result = await api(`/api/batches/${encodeURIComponent(state.selectedBatchId)}/decision`, {
      method: "POST",
      body,
    });
    const action = ACTION_LABELS[result.action] || result.action;
    const message = result.applied
      ? `状态 ${result.batch.status}，动作 ${action}`
      : `${action}；未修改批次状态`;
    showNotice("裁决已提交", message);
    toast("裁决已提交", message, result.mode === "FAST" ? "success" : "success");
    await loadAll(state.selectedBatchId);
    showNotice("裁决已提交", message);
  } catch (error) {
    state.error = error;
    renderError();
    let suffix = error.message;
    if (error.code === "E_ROLLBACK_COOLDOWN" && error.context?.remaining_ms !== undefined) {
      suffix = `${error.message}，剩余 ${error.context.remaining_ms} ms`;
    }
    toast(error.code || "裁决失败", suffix, "error");
  } finally {
    setBusy(false);
  }
}

async function seedDemo() {
  if (state.busy) return;
  setBusy(true);
  try {
    const result = await api("/api/demo/seed", { method: "POST" });
    const created = result.actions.filter((action) => action.result === "CREATED").length;
    toast("固定样本已装载", `${result.batch_count} 个批次，${created} 个新动作`);
    await loadAll();
  } catch (error) {
    toast("装载失败", `${error.code}: ${error.message}`, "error");
    showFatal(error);
  } finally {
    setBusy(false);
  }
}

function openRecovery() {
  $("#recovery-modal").hidden = false;
  renderRecovery();
}

function closeRecovery() {
  $("#recovery-modal").hidden = true;
}

async function loadRecovery() {
  try {
    state.recovery = await api("/api/recovery/report");
    renderRecovery();
  } catch (error) {
    toast("恢复报告失败", `${error.code}: ${error.message}`, "error");
  }
}

function renderRecovery() {
  const report = state.recovery;
  if (!report) {
    $("#recovery-content").innerHTML = `<div class="batch-service">尚无报告</div>`;
    return;
  }
  const intents = (report.intents || [])
    .map(
      (intent) => `<div class="intent-block">
        <h4>${esc(intent.intent_id)} · ${esc(intent.operation)} → ${esc(intent.status_after_scan)}</h4>
        <div class="intent-detail">
          <div class="fact-row"><span>批次</span><strong>${esc(intent.batch_id || "—")}</strong></div>
          <div class="fact-row"><span>边界一致</span><strong>${intent.boundary_match ? "是" : "否"}</strong></div>
          <div class="formula-text" style="margin-top:10px">${esc(intent.note)}</div>
          <details style="margin-top:9px"><summary class="batch-service">最后完整边界 / 观测状态</summary><pre class="code-block">${esc(JSON.stringify({ last_committed_boundary: intent.last_committed_boundary, observed_persisted_state: intent.observed_persisted_state }, null, 2))}</pre></details>
        </div>
      </div>`,
    )
    .join("");
  $("#recovery-content").innerHTML = `
    <div class="recovery-summary">
      <div class="recovery-stat"><span>Pending found</span><strong>${esc(report.pending_found)}</strong></div>
      <div class="recovery-stat"><span>Aborted intents</span><strong>${esc(report.aborted_count)}</strong></div>
      <div class="recovery-stat"><span>Boundary mismatch</span><strong>${esc(report.boundary_mismatch_count)}</strong></div>
    </div>
    <div class="recovery-note">${esc(report.caveat)}<br><br>恢复依据：${esc(report.recovery_basis)}。持久化事实：${esc(JSON.stringify(report.persisted_fact_counts))}。</div>
    ${intents || `<div class="batch-service" style="margin-top:14px">本次启动未发现未完成意图。</div>`}`;
}

function bindEvents() {
  $("#seed-btn").addEventListener("click", seedDemo);
  $("#empty-seed-btn").addEventListener("click", seedDemo);
  $("#refresh-btn").addEventListener("click", () => loadAll(state.selectedBatchId));
  $("#compare-btn").addEventListener("click", refreshComparison);
  $("#decide-btn").addEventListener("click", submitDecision);
  $("#recovery-btn").addEventListener("click", async () => {
    await loadRecovery();
    openRecovery();
  });
  $("#recovery-close").addEventListener("click", closeRecovery);
  $("#recovery-modal").addEventListener("click", (event) => {
    if (event.target === $("#recovery-modal")) closeRecovery();
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") closeRecovery();
  });
  $("#batch-list").addEventListener("click", (event) => {
    const button = event.target.closest("[data-batch-id]");
    if (button) selectBatch(button.dataset.batchId);
  });
  $$(".mode-option").forEach((button) => {
    button.addEventListener("click", () => {
      state.selectedMode = button.dataset.mode;
      renderModeControl();
    });
  });
  $$(".tabs button").forEach((button) => {
    button.addEventListener("click", () => {
      state.activeTab = button.dataset.tab;
      $$(".tabs button").forEach((item) => item.classList.toggle("active", item === button));
      $$(".tab-panel").forEach((panel) => panel.classList.toggle("active", panel.dataset.panel === state.activeTab));
    });
  });
  $("#policy-select").addEventListener("change", () => {
    state.selectedPolicy = Number($("#policy-select").value);
    renderPolicyDigest();
    renderPolicyPanel();
    refreshComparison();
  });
  $("#window-select").addEventListener("change", () => {
    state.selectedWindow = Number($("#window-select").value);
    refreshComparison();
  });
}

document.addEventListener("DOMContentLoaded", boot);
