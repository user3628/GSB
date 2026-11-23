const state = { diagnostics: null, activeRunID: null };

const $ = (id) => document.getElementById(id);
const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const text = await response.text();
  let body;
  try { body = text ? JSON.parse(text) : null; } catch { body = { raw: text }; }
  if (!response.ok) {
    const error = new Error(body?.message || `HTTP ${response.status}`);
    error.body = body;
    throw error;
  }
  return body;
}

function showError(error) {
  const panel = $("error-panel");
  panel.classList.remove("hidden");
  panel.innerHTML = `<h2>${esc(error.body?.code || "REQUEST_FAILED")}</h2><p>${esc(error.message)}</p><pre>${esc(JSON.stringify(error.body || {}, null, 2))}</pre>`;
  if (error.body?.context?.run) renderRun(error.body.context.run);
}

function clearError() { $("error-panel").classList.add("hidden"); }
function fmtBytes(bytes) {
  if (bytes === undefined || bytes === null) return "—";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let n = Number(bytes), i = 0;
  while (Math.abs(n) >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return `${n.toFixed(i ? 2 : 0)} ${units[i]} (${bytes})`;
}
function fmtUS(us) {
  if (us === undefined || us === null) return "—";
  return `${us}us / ${(us / 1000).toFixed(3)}ms`;
}
function statusClass(status) { return `status-${String(status || "").replace(/[^a-z_]/gi, "")}`; }

async function boot() {
  try {
    const health = await api("/healthz");
    $("health").textContent = `${health.status.toUpperCase()} · 127.0.0.1`;
    $("health").className = "health ok";
  } catch (error) {
    $("health").textContent = "DISCONNECTED";
    $("health").className = "health bad";
  }
  await refreshDiagnostics();
  try {
    const report = await api("/api/v1/recovery/report");
    renderRecovery(report);
  } catch (error) {
    if (error.body?.code !== "E_NOT_FOUND") showError(error);
  }
}

async function refreshDiagnostics() {
  state.diagnostics = await api("/api/v1/diagnostics");
  const select = $("trace-select");
  const previous = select.value;
  select.innerHTML = "";
  for (const trace of state.diagnostics.traces || []) {
    const option = document.createElement("option");
    option.value = `${trace.trace_id}::${trace.version}`;
    option.textContent = `${trace.trace_id} / ${trace.version} · ${trace.request_count} requests`;
    select.appendChild(option);
  }
  if (previous) select.value = previous;
  renderRuns(state.diagnostics.runs || []);
}

function selectedTrace() {
  const [traceID, version] = $("trace-select").value.split("::");
  return { traceID, version };
}

function renderRuns(runs) {
  $("run-count").textContent = `${runs.length} runs`;
  const list = $("run-list");
  list.innerHTML = "";
  for (const run of runs.slice(0, 60)) {
    const item = document.createElement("div");
    item.className = `run-item ${run.run_id === state.activeRunID ? "active" : ""}`;
    item.innerHTML = `<div class="run-id">${esc(run.run_id)}</div>
      <div class="run-meta"><span class="${statusClass(run.status)}">${esc(run.status)}</span><span>${esc(run.mode)}</span><span>${run.batch_count} batches</span><span>deferred ${run.deferred_request_count}</span></div>`;
    item.onclick = () => loadRun(run.run_id);
    list.appendChild(item);
  }
  if (!runs.length) list.innerHTML = `<p class="small">尚无运行。提交轨迹后组织一次。</p>`;
}

async function loadRun(runID) {
  try {
    clearError();
    const run = await api(`/api/v1/runs/${encodeURIComponent(runID)}`);
    state.activeRunID = runID;
    renderRun(run);
    const active = document.querySelector(".run-item.active");
    if (!active && state.diagnostics) renderRuns(state.diagnostics.runs || []);
  } catch (error) { showError(error); }
}

function renderRun(run) {
  $("run-panel").classList.remove("hidden");
  $("run-title").textContent = `运行 ${run.run_id}`;
  $("run-summary").innerHTML = [
    ["状态", run.status], ["模式", `${run.mode} / ${run.mode_label}`], ["组织顺序", run.options?.organization_order || "arrival"], ["批次", run.batches?.length ?? 0],
    ["选中请求", run.selected_request_count], ["推迟请求", run.deferred_request_count], ["样本守恒", run.sample_conservation_ok ? "PASS" : "FAIL"],
    ["显存", fmtBytes(run.used_vram_bytes)], ["节点预算", fmtBytes(run.available_vram_bytes)],
    ["精度", run.precision_ok ? "PASS" : "BREACH"], ["延迟红线", run.latency_ok ? "PASS" : "EXCEEDED"],
    ["plan hash", run.plan_hash], ["result hash", run.result_hash],
  ].map(([k, v]) => `<div class="stat"><div class="k">${esc(k)}</div><div class="v ${statusClass(v)}">${esc(v)}</div></div>`).join("");
  const flags = [];
  if (run.options?.allow_mixed_models) flags.push(["混合模型已声明", ""]);
  if (run.options?.faults?.vram_shortfall_bytes) flags.push([`注入显存缺口 ${fmtBytes(run.options.faults.vram_shortfall_bytes)}`, "warn"]);
  if (run.mix_rejections?.length) flags.push([`拒绝混批 ${run.mix_rejections.length}`, "warn"]);
  if (run.deferred_requests?.length) flags.push([`推迟 ${run.deferred_requests.length}`, "warn"]);
  if (run.failure_code) flags.push([run.failure_code, "bad"]);
  $("run-flags").innerHTML = flags.map(([text, cls]) => `<span class="flag ${cls}">${esc(text)}</span>`).join("");

  const list = $("batch-list");
  list.innerHTML = "";
  for (const batch of run.batches || []) list.appendChild(batchCard(batch, run));
  if (!(run.batches || []).length) list.innerHTML = `<p class="small">该运行没有已提交批次；查看延迟策略或恢复报告。</p>`;
  if (run.deferred_requests?.length) {
    const details = document.createElement("div");
    details.className = "precision-note";
    details.innerHTML = `<strong>被推迟请求：</strong>` + run.deferred_requests.map((d) => `${esc(d.request_id)} → ${esc(d.reason)} (${esc(d.detail)})`).join(" · ");
    list.appendChild(details);
  }
}

function batchCard(batch, run) {
  const card = document.createElement("article");
  card.className = "batch-card";
  const members = (batch.members || []).map((m) => `<tr><td>${esc(m.request_id)}</td><td>${esc(m.model_id)}</td><td class="num">${m.allocated_samples}</td><td class="num">${m.original_samples}</td><td>${esc(m.allocation_reason)}</td></tr>`).join("");
  const quantiles = (batch.quantiles || []).map((q) => `<div class="quantile ${q.available ? "" : "unavailable"}"><div class="q">P${q.quantile} · nearest-rank · n=${q.sample_count}</div><div class="v">${q.available ? `${esc(q.latency_ms)} ms` : esc(q.error_code)}</div><div class="r">${q.available ? `rank ${q.rank} · ${q.latency_us}us` : esc(q.reason)}</div></div>`).join("");
  const p = batch.precision || {};
  card.innerHTML = `<div class="batch-head"><div><div class="batch-title">${esc(batch.batch_id)}</div><div class="batch-sub">seq ${batch.batch_sequence} · root ${batch.root_batch_sequence} · split ${batch.split_index} · priority ${batch.priority} · ${esc(batch.split_reason)}</div></div><div class="${statusClass(batch.status)} batch-title">${esc(batch.status)}</div></div>
    <div class="batch-body">
      <div class="stat-grid">
        <div class="stat"><div class="k">samples</div><div class="v">${batch.total_samples}</div></div>
        <div class="stat"><div class="k">vram</div><div class="v">${esc(fmtBytes(batch.vram_bytes))}</div></div>
        <div class="stat"><div class="k">latency</div><div class="v">${esc(fmtUS(batch.latency_us))}</div></div>
        <div class="stat"><div class="k">limit</div><div class="v ${batch.latency_ok ? "status-pass" : "status-FAIL"}">${esc(fmtUS(batch.latency_limit_us))}</div></div>
        <div class="stat"><div class="k">rounding err</div><div class="v">${p.rounding_error_us ?? "—"}us</div></div>
        <div class="stat"><div class="k">abs dev</div><div class="v ${p.precision_ok ? "status-pass" : "status-FAIL"}">${p.absolute_deviation_us ?? "—"}us</div></div>
      </div>
      <div class="table-wrap"><table><thead><tr><th>request</th><th>model</th><th>allocated</th><th>original</th><th>basis</th></tr></thead><tbody>${members}</tbody></table></div>
      <div class="quantiles">${quantiles || `<span class="small">分位结果尚未提交</span>`}</div>
      <div class="precision-note">${esc(p.explanation || "精度事实尚未提交。")}<br>truth=${p.truth_latency_us ?? "—"}us · reported=${p.reported_latency_us ?? "—"}us · rel=${p.relative_deviation_ppm ?? "—"}ppm · cumulative rounding=${p.cumulative_rounding_error_us ?? "—"}us</div>
      <div class="button-row compact"><button data-batch="${esc(batch.batch_id)}" class="verify-batch">真值核对此批</button></div>
      <div class="batch-verify small" data-verify-for="${esc(batch.batch_id)}"></div>
    </div>`;
  card.querySelector(".verify-batch").onclick = async () => {
    const target = card.querySelector(`[data-verify-for="${CSS.escape(batch.batch_id)}"]`);
    try {
      const v = await api(`/api/v1/batches/${encodeURIComponent(batch.batch_id)}/verify`);
      target.textContent = `${v.conclusion} · precision=${v.precision_ok} latency=${v.latency_ok} available=${v.quantiles_available.join(",") || "none"} unavailable=${v.quantiles_unavailable.join(",") || "none"}`;
      target.className = `batch-verify small ${v.precision_ok && v.latency_ok ? "status-pass" : "status-FAIL"}`;
    } catch (error) { showError(error); }
  };
  return card;
}

function buildOptions(mode) {
  const lateIDs = $("late-ids").value.split(",").map((s) => s.trim()).filter(Boolean);
  const interruptRaw = $("interrupt").value.trim();
  const options = {
    mode,
    organization_order: $("organization-order").value,
    node_vram_bytes: $("node-vram").value.trim(),
    organization_budget_ms: Number($("budget").value || 0),
    allow_mixed_models: $("mixed").checked,
    latency_decimals: Number($("decimals").value || 0),
    max_abs_deviation_us: 100,
    max_relative_deviation_ppm: 10000,
    late_policy: $("late-policy").value,
    faults: {},
  };
  if (lateIDs.length) options.faults.late_request_ids = lateIDs;
  const offset = Number($("precision-offset").value || 0);
  if (offset) options.faults.precision_offset_us = offset;
  if (interruptRaw !== "") options.faults.interrupt_after_batches = Number(interruptRaw);
  return options;
}

async function organize(mode) {
  clearError();
  const { traceID, version } = selectedTrace();
  if (!traceID) return showError(new Error("请先提交并选择轨迹"));
  try {
    const run = await api(`/api/v1/traces/${encodeURIComponent(traceID)}/organize?version=${encodeURIComponent(version)}`, { method: "POST", body: JSON.stringify(buildOptions(mode)) });
    state.activeRunID = run.run_id;
    renderRun(run);
    await refreshDiagnostics();
  } catch (error) { showError(error); }
}

async function compareLatest() {
  clearError();
  const { traceID, version } = selectedTrace();
  try {
    const comparison = await api(`/api/v1/traces/${encodeURIComponent(traceID)}/comparison?version=${encodeURIComponent(version)}`);
    $("compare-panel").classList.remove("hidden");
    $("compare-summary").innerHTML = Object.entries(comparison.summary || {}).map(([k, v]) => `<div class="stat"><div class="k">${esc(k.replaceAll("_", " "))}</div><div class="v">${esc(v)}</div></div>`).join("");
    $("compare-list").innerHTML = (comparison.batch_pairs || []).map((p) => `<div class="compare-row"><div class="idx">#${p.index + 1}</div><div><div>${esc(p.left_batch_id || "—")}</div><div class="small">${esc((p.left_models || []).join(" + "))} · ${p.left_samples} samples · ${esc(fmtUS(p.left_latency_us))}</div></div><div class="right"><div>${esc(p.right_batch_id || "—")}</div><div class="small">${esc((p.right_models || []).join(" + "))} · ${p.right_samples} samples · ${esc(fmtUS(p.right_latency_us))}</div></div></div>`).join("");
  } catch (error) { showError(error); }
}

function renderRecovery(report) {
  const stateText = report.clean ? "CLEAN START" : "RECOVERED INTERRUPTION";
  $("recovery-panel").innerHTML = `<p><strong class="${report.clean ? "status-pass" : "status-FAIL"}">${stateText}</strong></p>
    <p>报告 ${esc(report.report_id)}</p>
    <p>中止运行 ${report.aborted_runs?.length || 0} · 中止批次 ${report.aborted_batches?.length || 0} · 保留完整批次 ${report.preserved_complete_batches?.length || 0}</p>
    <p>${esc(report.conclusion)}</p><div class="precision-note">${esc(report.completion_claim)}</div>`;
}

async function loadRecovery() {
  try { renderRecovery(await api("/api/v1/recovery/report")); } catch (error) { showError(error); }
}

async function loadIntents() {
  try {
    const response = await api("/api/v1/intents?limit=100");
    $("intents-panel").classList.remove("hidden");
    $("intents-list").innerHTML = (response.intents || []).map((i) => `<div class="intent"><span>#${i.id}</span><span>${esc(i.kind)}</span><span class="${i.state === "committed" ? "status-pass" : i.state === "aborted" ? "status-FAIL" : ""}">${esc(i.state)}</span><span class="muted">${esc(i.run_id || i.batch_id || i.trace_id || "")}</span></div>`).join("");
  } catch (error) { showError(error); }
}

$("organize").onclick = () => organize($("mode").value);
$("run-fast").onclick = () => organize("fast");
$("run-full").onclick = () => organize("full");
$("compare-latest").onclick = compareLatest;
$("load-recovery").onclick = loadRecovery;
$("load-intents").onclick = loadIntents;
$("trace-select").onchange = () => { state.activeRunID = null; };
$("verify-run").onclick = async () => {
  if (!state.activeRunID) return;
  try {
    const run = await api(`/api/v1/runs/${encodeURIComponent(state.activeRunID)}`);
    for (const batch of run.batches || []) {
      const button = document.querySelector(`[data-batch="${CSS.escape(batch.batch_id)}"]`);
      if (button) button.click();
    }
  } catch (error) { showError(error); }
};

boot().catch(showError);
