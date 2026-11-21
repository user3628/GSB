const state = {
  jobs: [],
  samples: null,
  status: null,
  jobId: "",
  tab: "overview",
  details: {},
  busy: new Set(),
};

const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function shortHash(value, head = 12, tail = 8) {
  if (!value) return "—";
  const text = String(value);
  if (text.length <= head + tail + 1) return text;
  return `${text.slice(0, head)}…${text.slice(-tail)}`;
}

function code(value) {
  return `<code class="code-chip">${escapeHtml(value)}</code>`;
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  let body = null;
  try { body = await response.json(); } catch (_) { body = null; }
  if (!response.ok) {
    const error = body?.error || { code: `HTTP_${response.status}`, message: response.statusText, details: {} };
    const exception = new Error(error.message || error.code);
    exception.code = error.code;
    exception.details = error.details || {};
    throw exception;
  }
  return body;
}

function toast(message, kind = "") {
  const node = document.createElement("div");
  node.className = `toast ${kind}`;
  node.textContent = message;
  $("#toastStack").appendChild(node);
  setTimeout(() => node.remove(), 4600);
}

function showError(error) {
  const details = error?.details && Object.keys(error.details).length ? ` · ${JSON.stringify(error.details)}` : "";
  toast(`${error?.code || "ERROR"}: ${error?.message || error}${details}`, "error");
}

async function loadSamples() {
  try { state.samples = await api("/api/samples"); } catch (error) { showError(error); }
}

async function loadJobs() {
  try {
    const value = await api("/api/jobs");
    state.jobs = value.jobs || [];
    const select = $("#jobSelect");
    const previous = state.jobId;
    select.innerHTML = state.jobs.length
      ? state.jobs.map(job => `<option value="${escapeHtml(job.job_id)}">${escapeHtml(job.job_id)} · ${escapeHtml(job.shard_id)}</option>`).join("")
      : '<option value="">尚无作业</option>';
    if (previous && state.jobs.some(job => job.job_id === previous)) state.jobId = previous;
    else if (state.jobs.length) state.jobId = state.jobs[0].job_id;
    else state.jobId = "";
    select.value = state.jobId;
  } catch (error) { showError(error); }
}

async function loadStatus() {
  if (!state.jobId) {
    state.status = null;
    renderShell();
    return;
  }
  try {
    state.status = await api(`/api/jobs/${encodeURIComponent(state.jobId)}/status`);
  } catch (error) {
    state.status = null;
    showError(error);
  }
  renderShell();
  if (
    state.status?.complete &&
    !state.details.export &&
    !state.details.exportLoading
  ) {
    state.details.exportLoading = true;
    try {
      state.details.export = await api(`/api/jobs/${encodeURIComponent(state.jobId)}/export`);
      if (state.tab === "overview" || state.tab === "output") renderTab();
    } catch (error) {
      // The output tab surface reports the actionable error; polling remains quiet.
    } finally {
      state.details.exportLoading = false;
    }
  }
}

function renderShell() {
  const hasJob = Boolean(state.status);
  $("#emptyState").classList.toggle("hidden", hasJob);
  $("#workspace").classList.toggle("hidden", !hasJob);
  if (!hasJob) {
    renderSamples();
    return;
  }
  renderKpis();
  renderPipeline();
  renderTab();
}

function renderSamples() {
  if (!state.samples) return;
  $("#sampleGrid").innerHTML = state.samples.shards.map(sample => `
    <div class="sample-card">
      <strong>${escapeHtml(sample.id)}</strong>
      <code>${escapeHtml(sample.path)}</code>
      <p>${escapeHtml(sample.layout)} · ${escapeHtml(JSON.stringify(sample.shape))} → ${escapeHtml(JSON.stringify(sample.target_shape))}<br>${escapeHtml(sample.notes || "")}</p>
    </div>`).join("");
}

function renderKpis() {
  const status = state.status;
  const stages = status.stages;
  const completeStages = Object.values(stages).filter(stage => stage.complete).length;
  const totalBlocks = Object.values(stages).reduce((sum, stage) => sum + stage.valid_blocks, 0);
  const totalExpected = Object.values(stages).reduce((sum, stage) => sum + stage.expected_blocks, 0);
  const alerts = status.alerts || 0;
  $("#kpiPipeline").textContent = `${completeStages}/3`;
  $("#kpiPipelineSub").textContent = status.complete ? "全部阶段完成" : status.run_state?.status === "stopped" ? "已优雅停止，可恢复" : "部分阶段可用";
  $("#kpiBlocks").textContent = `${totalBlocks}`;
  $("#kpiBlocksSub").textContent = `连续块 / ${totalExpected} 预期`;
  $("#kpiAlerts").textContent = `${alerts}`;
  $("#kpiAlertsSub").textContent = alerts ? "E_NUMERIC 已保留诊断" : "无数值异常";
  $("#kpiFingerprint").textContent = status.version_fingerprint || "—";
  $("#kpiFingerprint").title = status.version_fingerprint || "";
  $("#kpiShape").textContent = `source ${JSON.stringify(status.source_shape)} → target ${JSON.stringify(status.actual_target_shape)}`;
  $("#diagCount").textContent = alerts;
  $("#diagCount").classList.toggle("hidden", !alerts);
  const runState = status.run_state;
  $("#runMeta").textContent = runState
    ? `${runState.status || "idle"} · computed ${runState.computed_blocks ?? 0} · reused ${runState.reused_blocks ?? 0}`
    : "空闲";
}

function renderPipeline() {
  const status = state.status;
  const job = status.job;
  const descriptions = {
    activation: `${job.layout === "4d_bhsd" ? "4-D BHSD" : "2-D matrix"} · block activation`,
    reorder: state.details.fingerprints ? "Transpose / tile mapping" : "Transpose / tile mapping",
    reduce: "Overflow-safe aggregate",
  };
  for (const stageName of ["activation", "reorder", "reduce"]) {
    const stage = status.stages[stageName];
    const card = $(`.stage-card[data-stage="${stageName}"]`);
    if (!card) continue;
    const percent = stage.expected_blocks ? Math.round(stage.valid_blocks / stage.expected_blocks * 100) : 0;
    card.classList.toggle("complete", stage.complete);
    card.classList.toggle("active", !stage.complete && stage.valid_blocks > 0);
    const statusNode = $('[data-role="status"]', card);
    statusNode.className = "stage-status " + (stage.complete ? (stage.alerts ? "alert" : "complete") : stage.valid_blocks ? "running" : "");
    statusNode.textContent = stage.complete ? (stage.alerts ? `E_NUMERIC ×${stage.alerts}` : "完成") : stage.valid_blocks ? "进行中" : "待运行";
    $('[data-role="desc"]', card).textContent = descriptions[stageName];
    $('[data-role="bar"]', card).style.width = `${percent}%`;
    $('[data-role="blocks"]', card).textContent = `${stage.valid_blocks} / ${stage.expected_blocks} blocks`;
    $('[data-role="epoch"]', card).textContent = `epoch ${stage.epoch} · next ${stage.next_sequence}`;
    const hashNode = $('[data-role="hash"]', card);
    hashNode.textContent = `content ${shortHash(stage.content_digest, 10, 6)}`;
    hashNode.title = stage.content_digest || "";
  }
}

function renderTab() {
  $$(".tab", $("#tabStrip")).forEach(button => button.classList.toggle("active", button.dataset.tab === state.tab));
  $$(".nav-item", $("#railNav")).forEach(button => button.classList.toggle("active", button.dataset.tab === state.tab));
  const content = $("#tabContent");
  if (!state.status) { content.innerHTML = ""; return; }
  if (state.tab === "overview") renderOverview(content);
  else if (state.tab === "blocks") renderBlocks(content);
  else if (state.tab === "fingerprints") renderFingerprints(content);
  else if (state.tab === "invalidations") renderInvalidations(content);
  else if (state.tab === "diagnostics") renderDiagnostics(content);
  else if (state.tab === "output") renderOutput(content);
}

function renderOverview(content) {
  const status = state.status;
  const job = status.job;
  const summary = state.details.export?.summary;
  const reduction = summary?.reduction;
  content.innerHTML = `
    <div class="content-grid">
      <div class="panel">
        <h3>作业拓扑与指纹</h3>
        <div class="detail-grid">
          <div class="detail-item"><span>作业</span><span>${escapeHtml(job.job_id)}</span></div>
          <div class="detail-item"><span>分片</span><span>${escapeHtml(job.shard_id)}</span></div>
          <div class="detail-item"><span>输入布局</span><span>${escapeHtml(job.layout)}</span></div>
          <div class="detail-item"><span>输入摘要</span><span title="${escapeHtml(status.input_digest)}">${escapeHtml(shortHash(status.input_digest))}</span></div>
          <div class="detail-item"><span>源形状</span><span>${escapeHtml(JSON.stringify(status.source_shape))}</span></div>
          <div class="detail-item"><span>目标形状</span><span>${escapeHtml(JSON.stringify(status.actual_target_shape))}</span></div>
          <div class="detail-item"><span>配置版本</span><span title="${escapeHtml(status.config_version)}">${escapeHtml(shortHash(status.config_version))}</span></div>
          <div class="detail-item"><span>检查点纪元</span><span>${escapeHtml(JSON.stringify(status.stage_epochs))}</span></div>
        </div>
      </div>
      <div class="panel">
        <h3>运行控制</h3>
        <div class="big-number">${status.complete ? "READY" : status.run_state?.status === "stopped" ? "PAUSED" : "PENDING"}</div>
        <div class="muted" style="font-size:11px;line-height:1.65">${status.complete ? "三阶段均已达连续一致状态，可导出逐字节一致的摘要。" : status.stop_requested ? "已收到停止请求：当前块完成后退出。" : "可运行全部阶段，或逐阶段推进以观察检查点。"}</div>
        <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:18px">
          <button class="btn btn-primary btn-sm" data-action="run-all">运行全部</button>
          <button class="btn btn-ghost btn-sm" data-action="resume">恢复</button>
          <button class="btn btn-stop btn-sm" data-action="stop">停止</button>
        </div>
        <div style="margin-top:18px" class="detail-item"><span>最后运行</span><span>${escapeHtml(status.run_state?.status || "—")}</span></div>
        <div class="detail-item"><span>重算 / 复用</span><span>${status.run_state?.computed_blocks ?? "—"} / ${status.run_state?.reused_blocks ?? "—"}</span></div>
      </div>
    </div>
    <div class="panel" style="margin-top:14px">
      <div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start">
        <div><h3>结果归约</h3><div class="muted" style="font-size:10px">避免直接连乘；NaN/Inf 保留为异常诊断，不静默替换为零。</div></div>
        ${reduction ? `<span class="status-chip">${escapeHtml(reduction.strategy)}</span>` : '<span class="status-chip pending">等待 summary</span>'}
      </div>
      <div class="detail-grid" style="margin-top:12px">
        <div class="detail-item"><span>归约值</span><span>${reduction ? escapeHtml(String(reduction.value)) : "—"}</span></div>
        <div class="detail-item"><span>有限元素</span><span>${reduction ? reduction.finite_count : "—"}</span></div>
        <div class="detail-item"><span>非有限元素</span><span>${reduction ? reduction.nonfinite_count : "—"}</span></div>
        <div class="detail-item"><span>全零行</span><span>${reduction ? reduction.zero_rows?.count ?? 0 : "—"}</span></div>
      </div>
    </div>`;
}

async function ensureBlocks() {
  if (state.details.blocks) return;
  state.details.blocks = await api(`/api/jobs/${encodeURIComponent(state.jobId)}/blocks`);
}

async function renderBlocks(content) {
  content.innerHTML = '<div class="empty-message"><div><b>加载块检查点</b>正在读取连续序号前缀…</div></div>';
  try {
    await ensureBlocks();
    const blocks = state.details.blocks.blocks || [];
    const stageFilter = state.details.blockStage || "all";
    const filtered = blocks.filter(block => stageFilter === "all" || block.stage === stageFilter);
    content.innerHTML = `
      <div style="display:flex;justify-content:space-between;gap:12px;align-items:center;margin-bottom:14px">
        <div class="muted" style="font-size:11px">仅展示连续、产物摘要校验通过的块。损坏/重复记录默认忽略。</div>
        <select id="blockStageFilter">
          <option value="all">全部阶段</option>
          <option value="activation">activation</option>
          <option value="reorder">reorder</option>
          <option value="reduce">reduce</option>
        </select>
      </div>
      <div class="table-wrap"><table>
        <thead><tr><th>阶段</th><th>序号</th><th>块</th><th>行区间</th><th>列区间</th><th>状态</th><th>产物摘要</th></tr></thead>
        <tbody>${filtered.map((block, index) => `
          <tr class="clickable" data-block-index="${index}">
            <td>${escapeHtml(block.stage)}</td><td class="seq">#${block.sequence}</td>
            <td class="mono">${escapeHtml(block.block.key)}</td>
            <td class="range">${escapeHtml(JSON.stringify(block.block.row_range))}</td>
            <td class="range">${escapeHtml(JSON.stringify(block.block.col_range))}</td>
            <td>${block.status === "ok" ? '<span class="status-chip">ok</span>' : '<span class="status-chip error">E_NUMERIC</span>'}</td>
            <td class="hash" title="${escapeHtml(block.product_digest)}">${escapeHtml(shortHash(block.product_digest, 11, 7))}</td>
          </tr>`).join("") || '<tr><td colspan="7"><div class="empty-message">暂无连续检查点</div></td></tr>'}</tbody>
      </table></div>`;
    const filter = $("#blockStageFilter");
    filter.value = stageFilter;
    filter.addEventListener("change", () => { state.details.blockStage = filter.value; renderTab(); });
    $$("tr.clickable", content).forEach(row => {
      row.addEventListener("click", () => openDiagnostic(filtered[Number(row.dataset.blockIndex)]));
    });
  } catch (error) { showError(error); content.innerHTML = `<div class="empty-message">${escapeHtml(error.message)}</div>`; }
}

async function ensureFingerprints() {
  if (state.details.fingerprints) return;
  state.details.fingerprints = await api(`/api/jobs/${encodeURIComponent(state.jobId)}/fingerprints`);
}

async function renderFingerprints(content) {
  content.innerHTML = '<div class="empty-message"><div><b>计算版本指纹</b>汇总输入、配置与阶段内容…</div></div>';
  try {
    await ensureFingerprints();
    const fp = state.details.fingerprints;
    const stages = ["activation", "reorder", "reduce"];
    content.innerHTML = `
      <div class="fingerprint-grid">
        <div class="fp-card"><h3>全局版本指纹</h3><div class="fp-label">VERSION</div><div class="fp-value">${escapeHtml(fp.version_fingerprint)}</div>
          <div class="fp-label">INPUT CONTENT</div><div class="fp-value">${escapeHtml(fp.input_digest)}</div>
          <div class="fp-label">CONFIG</div><div class="fp-value">${escapeHtml(fp.config_version)}</div></div>
        ${stages.map(stage => `<div class="fp-card"><h3>${escapeHtml(stage)}</h3><div class="fp-label">PLAN FINGERPRINT</div><div class="fp-value">${escapeHtml(fp.stage_plans[stage].fingerprint)}</div>
          <div class="fp-label">CONTENT DIGEST</div><div class="fp-value">${escapeHtml(fp.stage_content[stage] || "未完成 / unavailable")}</div>
          <div class="fp-label">EPOCH</div><div class="fp-value">e${String(fp.stage_epochs[stage]).padStart(6, "0")}</div></div>`).join("")}
      </div>`;
  } catch (error) { showError(error); }
}

async function renderInvalidations(content) {
  content.innerHTML = '<div class="empty-message"><div><b>读取失效事件</b>比较阶段版本计划…</div></div>';
  try {
    if (!state.details.invalidations) state.details.invalidations = await api(`/api/jobs/${encodeURIComponent(state.jobId)}/invalidations`);
    const events = state.details.invalidations.events || [];
    content.innerHTML = events.length ? `<div class="timeline">${events.slice().reverse().map(event => `
      <article class="event">
        <div class="event-head"><div class="event-title">最早失效阶段 <b>${escapeHtml(event.earliest_stage)}</b> · 事件 #${event.event_sequence}</div><div class="event-time">${escapeHtml((event.affected_stages || []).join(" → "))}</div></div>
        <div class="event-tags">${(event.reasons || []).map(reason => `<span class="tag">${escapeHtml(reason)}</span>`).join("")}</div>
        <div class="detail-grid" style="margin-top:12px">
          <div class="detail-item"><span>受影响阶段</span><span>${escapeHtml((event.affected_stages || []).join(", "))}</span></div>
          <div class="detail-item"><span>保留阶段</span><span>${escapeHtml((event.unaffected_stages || []).join(", ") || "—")}</span></div>
          <div class="detail-item"><span>旧输入</span><span title="${escapeHtml(event.old_input_digest)}">${escapeHtml(shortHash(event.old_input_digest))}</span></div>
          <div class="detail-item"><span>新输入</span><span title="${escapeHtml(event.new_input_digest)}">${escapeHtml(shortHash(event.new_input_digest))}</span></div>
          ${Object.entries(event.affected_blocks || {}).map(([stage, info]) => `<div class="detail-item"><span>${escapeHtml(stage)} 块区间</span><span>${escapeHtml(info.interval)} (${info.count})</span></div>`).join("")}
        </div>
      </article>`).join("")}</div>` : '<div class="empty-message"><div><b>暂无失效事件</b>输入与配置未发生变化，或作业刚建立。</div></div>';
  } catch (error) { showError(error); }
}

async function renderDiagnostics(content) {
  content.innerHTML = '<div class="empty-message"><div><b>读取块级诊断</b>检查 NaN / Inf / 全零行与值域…</div></div>';
  try {
    const value = await api(`/api/jobs/${encodeURIComponent(state.jobId)}/diagnostics`);
    const diagnostics = value.diagnostics || [];
    if (!diagnostics.length) {
      content.innerHTML = '<div class="empty-message"><div><b>没有数值异常</b>所有已发布块的有限值与全零行检查均通过。</div></div>';
      return;
    }
    content.innerHTML = `<div class="alert-list">${diagnostics.map(item => `
      <div class="alert-item">
        <div class="alert-item-head"><code>${escapeHtml(item.diagnostics?.code || "E_NUMERIC")}</code><span class="muted mono">${escapeHtml(item.stage)} · #${item.sequence} · ${escapeHtml(item.block.key)}</span></div>
        <p>分片 ${escapeHtml(item.diagnostics?.shard_id || state.status.job.shard_id)} · 行 ${escapeHtml(JSON.stringify(item.diagnostics?.row_range))} · 列 ${escapeHtml(JSON.stringify(item.diagnostics?.col_range))}</p>
        <p>${escapeHtml(diagnosticSummary(item.diagnostics))}</p>
      </div>`).join("")}</div>`;
  } catch (error) { showError(error); }
}

function diagnosticSummary(diag) {
  if (!diag) return "诊断上下文不可用";
  const parts = [];
  if (diag.nonfinite) parts.push(`nonfinite=${diag.nonfinite.count} values=${JSON.stringify(diag.nonfinite.values)}`);
  if (diag.zero_rows) parts.push(`zero_rows=${diag.zero_rows.count} rows=${JSON.stringify(diag.zero_rows.rows)}`);
  if (diag.range_violation) parts.push(`range_violation=${diag.range_violation.count}`);
  if (diag.aggregate) parts.push(`aggregate=${JSON.stringify(diag.aggregate)}`);
  return parts.join(" · ") || "已保留诊断上下文";
}

async function renderOutput(content) {
  content.innerHTML = '<div class="empty-message"><div><b>加载结果摘要</b>校验阶段产物与归约值…</div></div>';
  try {
    if (!state.details.export) {
      state.details.export = await api(`/api/jobs/${encodeURIComponent(state.jobId)}/export`);
    }
    const value = state.details.export;
    const summary = value.summary;
    content.innerHTML = `
      <div class="output-hero">
        <div><div class="eyebrow">DETERMINISTIC EXPORT</div><h2>结果摘要已生成</h2>
          <p class="muted" style="font-size:11px">保存于 <span class="mono">${escapeHtml(value.saved_path)}</span></p></div>
        <div style="display:flex;gap:8px"><button class="btn btn-primary btn-sm" data-action="download">下载 JSON</button><button class="btn btn-ghost btn-sm" data-action="refresh-output">刷新</button></div>
      </div>
      <div class="detail-grid" style="margin-bottom:16px">
        <div class="detail-item"><span>摘要 SHA-256</span><span title="${escapeHtml(value.sha256)}">${escapeHtml(shortHash(value.sha256, 16, 10))}</span></div>
        <div class="detail-item"><span>归约策略 / 值</span><span>${escapeHtml(summary.reduction.strategy)} = ${escapeHtml(String(summary.reduction.value))}</span></div>
        <div class="detail-item"><span>激活输出摘要</span><span title="${escapeHtml(summary.outputs.activation.sha256)}">${escapeHtml(shortHash(summary.outputs.activation.sha256))}</span></div>
        <div class="detail-item"><span>重排输出摘要</span><span title="${escapeHtml(summary.outputs.reordered.sha256)}">${escapeHtml(shortHash(summary.outputs.reordered.sha256))}</span></div>
      </div>
      <pre class="code-block">${escapeHtml(JSON.stringify(summary, null, 2))}</pre>`;
  } catch (error) { showError(error); content.innerHTML = `<div class="empty-message"><div><b>摘要尚不可用</b>${escapeHtml(error.message)}<br>请先完成 reduce 阶段。</div></div>`; }
}

function openDiagnostic(block) {
  $("#drawerTitle").textContent = `${block.stage} · #${block.sequence}`;
  const diag = block.diagnostics;
  $("#drawerBody").innerHTML = `
    <div class="panel"><h3>块坐标</h3><div class="detail-grid">
      <div class="detail-item"><span>块键</span><span>${escapeHtml(block.block.key)}</span></div>
      <div class="detail-item"><span>状态</span><span>${block.status === "ok" ? '<span class="status-chip">ok</span>' : '<span class="status-chip error">E_NUMERIC</span>'}</span></div>
      <div class="detail-item"><span>行区间</span><span>${escapeHtml(JSON.stringify(block.block.row_range))}</span></div>
      <div class="detail-item"><span>列区间</span><span>${escapeHtml(JSON.stringify(block.block.col_range))}</span></div>
      <div class="detail-item"><span>产物摘要</span><span title="${escapeHtml(block.product_digest)}">${escapeHtml(shortHash(block.product_digest, 16, 10))}</span></div>
      <div class="detail-item"><span>纪元</span><span>e${block.epoch}</span></div>
    </div></div>
    <div class="panel"><h3>块元数据</h3><pre class="code-block">${escapeHtml(JSON.stringify(block.metadata || {}, null, 2))}</pre></div>
    <div class="panel"><h3>诊断上下文</h3>${diag ? `<pre class="code-block">${escapeHtml(JSON.stringify(diag, null, 2))}</pre>` : '<div class="muted" style="font-size:11px">该块没有业务异常；块产物已通过摘要校验。</div>'}</div>`;
  $("#diagnosticDrawer").classList.remove("hidden");
}

async function runStages(stage = "all") {
  if (!state.jobId) return;
  const key = `run:${stage}`;
  if (state.busy.has(key)) return;
  state.busy.add(key);
  try {
    const result = await api(`/api/jobs/${encodeURIComponent(state.jobId)}/run`, { method: "POST", body: JSON.stringify({ stage, background: false }) });
    toast(`${stage} · ${result.status} · computed ${result.computed_blocks} / reused ${result.reused_blocks}`, result.business_code ? "" : "success");
    resetDetails();
    await loadStatus();
  } catch (error) { showError(error); await loadStatus(); }
  finally { state.busy.delete(key); }
}

async function stopJob() {
  if (!state.jobId) return;
  try { await api(`/api/jobs/${encodeURIComponent(state.jobId)}/stop`, { method: "POST", body: "{}" }); toast("停止请求已提交：当前块完成后退出", "success"); await loadStatus(); }
  catch (error) { showError(error); }
}

async function resumeJob() {
  if (!state.jobId) return;
  try { await api(`/api/jobs/${encodeURIComponent(state.jobId)}/resume`, { method: "POST", body: JSON.stringify({ stage: "all", background: false }) }); toast("恢复运行完成", "success"); resetDetails(); await loadStatus(); }
  catch (error) { showError(error); }
}

function resetDetails() {
  state.details = {};
}

function openImport() {
  const select = $("#importSample");
  select.innerHTML = '<option value="">自定义路径</option>' + (state.samples?.shards || []).map(sample => `<option value="${escapeHtml(sample.id)}">${escapeHtml(sample.id)}</option>`).join("");
  $("#importModal").classList.remove("hidden");
}

async function importForm(event) {
  event.preventDefault();
  const sampleId = $("#importSample").value;
  const body = {
    sample_id: sampleId || null,
    shard_path: $("#importShard").value.trim() || null,
    config_path: $("#importConfig").value.trim() || null,
    job_id: $("#importJob").value.trim() || null,
    manifest_path: $("#importManifest").value.trim() || null,
  };
  try {
    const result = await api("/api/import", { method: "POST", body: JSON.stringify(body) });
    $("#importModal").classList.add("hidden");
    toast(`已导入 ${result.job_id}`, "success");
    await loadJobs();
    state.jobId = result.job_id;
    $("#jobSelect").value = state.jobId;
    resetDetails();
    await loadStatus();
  } catch (error) { showError(error); }
}

async function importAllSamples() {
  const shards = state.samples?.shards || [];
  for (const sample of shards) {
    try {
      await api("/api/import", { method: "POST", body: JSON.stringify({ sample_id: sample.id, job_id: sample.id }) });
    } catch (error) { showError(error); }
  }
  toast(`已导入 ${shards.length} 个样例作业`, "success");
  await loadJobs();
  resetDetails();
  await loadStatus();
}

function bindEvents() {
  $("#jobSelect").addEventListener("change", async event => { state.jobId = event.target.value; resetDetails(); await loadStatus(); });
  $("#refreshBtn").addEventListener("click", async () => { resetDetails(); await loadJobs(); await loadStatus(); });
  $("#runAllBtn").addEventListener("click", () => runStages("all"));
  $("#stopBtn").addEventListener("click", stopJob);
  $("#resumeBtn").addEventListener("click", resumeJob);
  $("#exportBtn").addEventListener("click", () => { state.tab = "output"; resetDetails(); renderTab(); });
  $("#importBtn").addEventListener("click", openImport);
  $("#importAllBtn").addEventListener("click", importAllSamples);
  $("#closeImport").addEventListener("click", () => $("#importModal").classList.add("hidden"));
  $("#cancelImport").addEventListener("click", () => $("#importModal").classList.add("hidden"));
  $("#importForm").addEventListener("submit", importForm);
  $("#importSample").addEventListener("change", event => {
    const sample = (state.samples?.shards || []).find(item => item.id === event.target.value);
    if (sample) {
      $("#importShard").value = sample.path;
      $("#importConfig").value = sample.config;
      $("#importJob").value = sample.id;
      $("#importManifest").value = "samples/shape_manifest.json";
    }
  });
  $("#closeDrawer").addEventListener("click", () => $("#diagnosticDrawer").classList.add("hidden"));
  $$(".nav-item, .tab", document).forEach(button => button.addEventListener("click", () => {
    state.tab = button.dataset.tab;
    if (state.tab === "fingerprints") delete state.details.fingerprints;
    if (state.tab === "invalidations") delete state.details.invalidations;
    if (state.tab === "output") delete state.details.export;
    renderTab();
  }));
  document.addEventListener("click", event => {
    const action = event.target.closest("[data-action]")?.dataset.action;
    if (action === "run-all") runStages("all");
    if (action === "stop") stopJob();
    if (action === "resume") resumeJob();
    if (action === "download") window.location = `/api/jobs/${encodeURIComponent(state.jobId)}/export?download=1`;
    if (action === "refresh-output") { delete state.details.export; renderTab(); }
    const stage = event.target.closest("[data-run-stage]")?.dataset.runStage;
    if (stage) runStages(stage);
  });
}

async function boot() {
  bindEvents();
  await loadSamples();
  await loadJobs();
  await loadStatus();
  setInterval(async () => {
    if (!state.jobId) return;
    if (state.busy.size) return;
    await loadStatus();
  }, 1800);
}

boot().catch(showError);

