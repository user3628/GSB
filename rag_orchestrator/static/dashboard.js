const state = { workspace: null, requests: [], selected: null, result: null, results: {} };

const $ = (id) => document.getElementById(id);

function fmt(value, digits = 0) {
  return Number(value || 0).toLocaleString(undefined, { maximumFractionDigits: digits });
}

function shortHash(value, size = 14) {
  return value ? `${value.slice(0, size)}…` : "—";
}

function toast(message) {
  const old = document.querySelector(".toast");
  if (old) old.remove();
  const div = document.createElement("div");
  div.className = "toast";
  div.textContent = message;
  document.body.appendChild(div);
  setTimeout(() => div.remove(), 2600);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const data = await response.json();
  return data;
}

async function loadWorkspace() {
  state.workspace = await api("/api/workspace");
  const req = await api("/api/requests");
  state.requests = req.requests || [];
  if (!state.selected && state.requests.length) {
    state.selected = state.requests[0].query_id;
  }
  renderMetrics();
  renderRequests();
  renderSelect();
}

function renderMetrics() {
  const w = state.workspace;
  if (!w) return;
  const latest = Object.values(w.latest_results || {});
  const ok = latest.filter((item) => item.status === "ok" || item.status === "degraded").length;
  $("metrics").innerHTML = [
    ["文档记录", w.counts.document_records, `${w.counts.unique_doc_ids} unique doc_id · ${w.counts.shards} shards`],
    ["请求样本", w.counts.requests, "JSONL · deterministic"],
    ["预算配置", w.budget_config_version, shortHash(w.config_fingerprint, 22)],
    ["已运行", `${ok}/${latest.length || 0}`, w.run_all_summary_sha256 ? `run-all ${shortHash(w.run_all_summary_sha256, 18)}` : "等待运行"],
  ].map(([label, value, sub]) => `
    <div class="metric">
      <div class="metric-label">${label}</div>
      <div class="metric-value">${value}</div>
      <div class="metric-sub">${sub}</div>
    </div>`).join("");
  $("health").innerHTML = `<span class="dot green"></span> engine ${w.engine_version}`;
}

function renderRequests() {
  const list = $("request-list");
  $("request-count").textContent = String(state.requests.length);
  list.innerHTML = state.requests.map((request) => {
    const result = state.results[request.query_id];
    const status = result ? result.status : "pending";
    return `
      <div class="request-card ${request.query_id === state.selected ? "selected" : ""}" data-query="${request.query_id}">
        <div class="request-id">${request.query_id}</div>
        <div class="request-query">${escapeHtml(request.query || "(empty)")}</div>
        <div class="request-meta">${status} · ${request.locale || ""} · ${request.channel || ""}</div>
      </div>`;
  }).join("");
  list.querySelectorAll(".request-card").forEach((node) => {
    node.onclick = () => {
      state.selected = node.dataset.query;
      renderRequests();
      renderSelect();
      loadResult(state.selected);
    };
  });
}

function renderSelect() {
  const select = $("request-select");
  select.innerHTML = state.requests.map((request) => (
    `<option value="${request.query_id}" ${request.query_id === state.selected ? "selected" : ""}>${request.query_id} · ${escapeHtml((request.query || "(empty)").slice(0, 48))}</option>`
  )).join("");
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (char) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[char]));
}

async function loadResult(queryId) {
  if (!queryId) return;
  try {
    const data = await api(`/api/results/${encodeURIComponent(queryId)}`);
    state.result = data;
    state.results[queryId] = data;
  } catch (error) {
    state.result = null;
  }
  renderResult();
  renderRequests();
}

async function runOne() {
  const queryId = $("request-select").value || state.selected;
  if (!queryId) return;
  state.selected = queryId;
  const mode = $("mode-select").value;
  const stop = $("stop-select").value || null;
  $("control-hint").textContent = `运行 ${queryId} · ${mode}${stop ? ` · stop after ${stop}` : ""}`;
  const data = await api("/api/run", {
    method: "POST",
    body: JSON.stringify({ query_id: queryId, mode, stop_after_stage: stop }),
  });
  state.result = data;
  state.results[queryId] = data;
  await loadWorkspace();
  await loadResult(queryId);
  toast(`${queryId}: ${data.status} ${data.error ? data.error.code : ""}`);
}

async function runAll() {
  const mode = $("mode-select").value;
  $("control-hint").textContent = `全部运行 · ${mode}`;
  const response = await api("/api/run-all", { method: "POST", body: JSON.stringify({ mode }) });
  for (const result of response.results || []) {
    state.results[result.query_id] = result;
  }
  const newest = response.results?.[0]?.query_id || state.selected;
  state.selected = newest;
  await loadWorkspace();
  await loadResult(newest);
  toast(`run-all ${response.request_count} requests · ${shortHash(response.summary_sha256, 18)}`);
}

async function stopRun() {
  const data = await api("/api/stop", { method: "POST" });
  $("control-hint").textContent = data.message;
  toast("停止请求已设置");
}

async function reloadConfig() {
  state.workspace = await api("/api/reload", { method: "POST" });
  renderMetrics();
  toast("配置与样本已重载");
}

function renderResult() {
  const result = state.result;
  if (!result) {
    $("pipeline").innerHTML = "";
    $("budget-view").className = "budget-view empty";
    $("budget-view").textContent = "运行请求后显示全局、通道和延迟预算。";
    $("diagnostics").className = "diagnostics empty";
    $("diagnostics").textContent = "配置指纹、缓存复用、重建阶段与错误码会显示在这里。";
    $("evidence-view").className = "evidence-list empty";
    $("evidence-view").textContent = "暂无证据。";
    $("answer-view").className = "answer empty";
    $("answer-view").textContent = "暂无答案。";
    $("raw-json").textContent = "{}";
    $("stable-hash").textContent = "stable: —";
    $("result-status").textContent = "—";
    $("budget-status").textContent = "—";
    return;
  }
  $("result-status").textContent = result.status;
  $("stable-hash").textContent = `stable: ${shortHash(result.diagnostics?.stable_sha256, 18)}`;
  renderPipeline(result);
  renderBudget(result);
  renderDiagnostics(result);
  renderEvidence(result);
  $("raw-json").textContent = JSON.stringify(result, null, 2);
}

function renderPipeline(result) {
  const stages = ["normalize", "dispatch", "rerank", "assemble"];
  const labels = { normalize: "规范化", dispatch: "派发", rerank: "重排", assemble: "组装" };
  const cache = result.diagnostics?.cache_usage || {};
  const fps = result.version?.stage_fingerprints || {};
  const writes = result.diagnostics?.checkpoint_writes || [];
  $("pipeline").innerHTML = stages.map((stage, index) => {
    const cacheState = cache[stage]?.status || (result.status === "error" && index === lastStageIndex(result) ? "error" : "reused");
    const write = writes.find((item) => item.stage === stage);
    const dot = cacheState === "rebuilt" ? "cyan" : cacheState === "reused" ? "green" : "red";
    return `
      <div class="stage ${cacheState}">
        <div class="stage-top">
          <span class="stage-index">0${index + 1}</span>
          <span class="dot ${dot}"></span>
        </div>
        <div class="stage-name">${labels[stage]}</div>
        <div class="stage-state">${cacheState}${write ? ` · seq ${write.seq}` : ""}</div>
        <div class="stage-fp">${shortHash(fps[stage], 38)}</div>
      </div>`;
  }).join("");
}

function lastStageIndex(result) {
  const fps = result.version?.stage_fingerprints || {};
  const stages = ["normalize", "dispatch", "rerank", "assemble"];
  return Math.max(0, stages.filter((stage) => fps[stage]).length - 1);
}

function renderBudget(result) {
  const budget = result.budget || result.partial?.budget;
  if (!budget) {
    $("budget-view").className = "budget-view empty";
    $("budget-view").textContent = result.error ? `预算阶段未完成：${result.error.code}` : "该阶段尚无预算明细。";
    $("budget-status").textContent = result.status;
    return;
  }
  $("budget-view").className = "budget-view";
  $("budget-status").textContent = `${budget.accepted_count} accepted / ${budget.rejected_count} rejected`;
  const percent = Math.min(100, (budget.global_used / Math.max(1, budget.global_quota)) * 100);
  const channels = (budget.channels || []).map((channel) => {
    const p = Math.min(100, (channel.used / Math.max(1, channel.quota)) * 100);
    return `
      <div class="channel">
        <div class="bar-label"><span>channel ${channel.channel}</span><span>${channel.used}/${channel.quota}</span></div>
        <div class="bar"><div class="bar-fill" style="width:${p}%"></div></div>
        <div class="evidence-meta">indices ${channel.accepted_subquery_indices.join(", ") || "—"}</div>
      </div>`;
  }).join("");
  const rejected = (budget.rejected || []).map((item) => (
    `<div class="evidence-meta"><span class="bad">${item.reason}</span> subquery ${item.subquery_index} · ${item.estimate_credits} credits</div>`
  )).join("");
  $("budget-view").innerHTML = `
    <div class="budget-summary">
      <span>global <strong>${budget.global_used}/${budget.global_quota}</strong></span>
      <span>per channel <strong>${budget.per_channel_quota}</strong></span>
      <span>latency <strong>${fmt(budget.estimated_latency_ms, 2)} / ${fmt(budget.latency_budget_ms, 2)} ms</strong></span>
      <span>doc shards <strong>${budget.document_shard_count}</strong></span>
    </div>
    <div class="bar-block">
      <div class="bar-label"><span>global quota</span><span>${percent.toFixed(1)}%</span></div>
      <div class="bar"><div class="bar-fill" style="width:${percent}%"></div></div>
    </div>
    <div class="channel-grid">${channels}</div>
    ${rejected ? `<div class="diag-card"><div class="diag-title">Unprocessed</div>${rejected}</div>` : ""}`;
}

function renderDiagnostics(result) {
  const inv = result.invalidation || {};
  const cache = result.diagnostics?.cache_usage || {};
  const reasons = (inv.reasons || []).map((reason) => (
    `<div class="evidence-meta"><span class="warn">${reason.stage}</span> ${reason.reason} · ${(reason.changed_components || []).join(", ") || "—"}</div>`
  )).join("");
  const error = result.error ? `
    <div class="diag-card">
      <div class="diag-title">Stable error</div>
      <div class="bad mono">${result.error.code}</div>
      <div class="evidence-meta">${escapeHtml(result.error.message || "")}</div>
    </div>` : "";
  $("diagnostics").className = "diagnostics";
  $("diagnostics").innerHTML = `
    ${error}
    <div class="diag-card">
      <div class="diag-title">Earliest invalidation</div>
      <div>${inv.earliest_stage ? `<span class="warn">${inv.earliest_stage}</span>` : `<span class="good">none / fresh or fully reusable</span>`}</div>
      ${reasons}
    </div>
    <div class="diag-card">
      <div class="diag-title">Fingerprints</div>
      <div class="mono">config ${shortHash(result.version?.config_fingerprint, 26)}</div>
      <div class="mono">budget ${result.version?.budget_config_version || "—"}</div>
    </div>
    <div class="diag-card">
      <div class="diag-title">Cache / checkpoint</div>
      <div class="mono">${Object.entries(cache).map(([k, v]) => `${k}:${v.status}`).join(" · ") || "—"}</div>
      <div class="mono">${(result.diagnostics?.checkpoint_writes || []).map((item) => `${item.stage}#${item.seq}`).join(" · ") || "no writes (reused)"}</div>
    </div>`;
}

function renderEvidence(result) {
  const evidence = result.evidence || result.partial?.evidence || [];
  $("evidence-count").textContent = `${evidence.length} evidence`;
  if (!evidence.length) {
    $("evidence-view").className = "evidence-list empty";
    $("evidence-view").textContent = result.error ? `未产生证据：${result.error.code}` : "暂无证据。";
  } else {
    $("evidence-view").className = "evidence-list";
    $("evidence-view").innerHTML = evidence.map((item) => `
      <article class="evidence">
        <div class="evidence-head">
          <span class="citation">${item.citation_id || `[${item.evidence_index}]`}</span>
          <span class="mono">${item.doc_id} · ${item.source}</span>
        </div>
        <div class="evidence-head" style="margin-top:7px"><strong>${escapeHtml(item.title)}</strong></div>
        <div class="evidence-text">${escapeHtml(item.text)}</div>
        <div class="evidence-meta">
          <span>rerank ${fmt(item.rerank_score, 3)}</span>
          <span>coverage ${fmt(item.term_coverage, 3)}</span>
          <span>chars ${item.char_count}</span>
          <span>shards ${(item.source_shards || []).join(", ")}</span>
        </div>
      </article>`).join("");
  }
  const answer = result.answer;
  $("answer-hash").textContent = answer ? `sha: ${shortHash(answer.summary_sha256, 18)}` : "sha: —";
  if (answer) {
    $("answer-view").className = "answer";
    $("answer-view").textContent = answer.text;
  } else {
    $("answer-view").className = "answer empty";
    $("answer-view").textContent = result.status === "stopped" ? "请求已停止，可在同一工作区继续运行以复用检查点。" : "暂无答案。";
  }
}

async function loadInitialResults() {
  const data = await api("/api/results");
  state.results = data.results || {};
  state.selected = state.selected || Object.keys(state.results)[0] || null;
  if (state.selected) {
    await loadResult(state.selected);
  }
}

$("run-one").onclick = () => runOne().catch((error) => toast(String(error)));
$("run-all").onclick = () => runAll().catch((error) => toast(String(error)));
$("stop-run").onclick = () => stopRun().catch((error) => toast(String(error)));
$("reload").onclick = () => reloadConfig().catch((error) => toast(String(error)));
$("request-select").onchange = (event) => {
  state.selected = event.target.value;
  renderRequests();
  loadResult(state.selected);
};
$("copy-json").onclick = async () => {
  await navigator.clipboard.writeText($("raw-json").textContent);
  toast("JSON 已复制");
};

(async function init() {
  await loadWorkspace();
  await loadInitialResults();
  renderResult();
})();
