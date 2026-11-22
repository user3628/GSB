const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];

const state = {
  file: null,
  fileBase64: null,
  overview: null,
  lastStats: null,
};

function toast(message, kind = "") {
  const node = $("#toast");
  node.textContent = message;
  node.className = `toast show ${kind}`;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => node.classList.remove("show"), 4200);
}

async function api(path, options = {}) {
  const response = await fetch(path, options);
  let body;
  try {
    body = await response.json();
  } catch (error) {
    throw new Error(`HTTP ${response.status}: 非 JSON 响应`);
  }
  if (!response.ok) {
    const err = new Error(body.message || body.code || `HTTP ${response.status}`);
    err.body = body;
    err.status = response.status;
    throw err;
  }
  return body;
}

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  }[char]));
}

function errorHtml(error) {
  const body = error.body || {};
  const details = body.details ? JSON.stringify(body.details, null, 2) : "";
  return `<div class="error-box"><strong>${esc(body.code || "E_CLIENT")}</strong> · ${esc(body.message || error.message)}${details ? `<pre>${esc(details)}</pre>` : ""}</div>`;
}

function loading(button, enabled = false) {
  if (!button) return;
  button.disabled = !enabled;
}

function readFileBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const bytes = new Uint8Array(reader.result);
      let binary = "";
      const chunk = 0x8000;
      for (let i = 0; i < bytes.length; i += chunk) {
        binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
      }
      resolve(btoa(binary));
    };
    reader.onerror = () => reject(reader.error);
    reader.readAsArrayBuffer(file);
  });
}

function pathOptions(selected = "") {
  return (state.overview?.paths || []).map((p) =>
    `<option value="${esc(p.path_node)}" ${p.path_node === selected ? "selected" : ""}>${esc(p.path_node)} · ${p.total}</option>`
  ).join("");
}

function renderOverview(data) {
  state.overview = data;
  $("#stat-version").textContent = data.snapshot_version;
  $("#stat-records").textContent = data.record_count;
  $("#stat-paths").textContent = data.path_count;
  $("#stat-pending").textContent = data.recovery_pending_intents;
  $("#version-pill").textContent = `version ${data.snapshot_version}`;
  const currentStats = $("#stats-path").value;
  $("#stats-path").innerHTML = pathOptions(currentStats) || `<option value="">暂无路径</option>`;
  $("#export-path").innerHTML = `<option value="">全部路径</option>${pathOptions($("#export-path").value)}`;
  renderRecent(data.recent_batches || []);
  renderServiceMetrics(data.latest_service_metrics || []);
}

function renderRecent(batches) {
  const node = $("#recent-batches");
  if (!batches.length) {
    node.innerHTML = `<p class="placeholder">尚无提交。</p>`;
    return;
  }
  node.innerHTML = batches.map((b) => `
    <div class="mini-row">
      <div><strong>${esc(b.batch_id)}</strong><br><span>${esc(b.kind)} · ${esc(b.status)}</span></div>
      <span>#${esc(b.commit_id ?? "—")}</span>
    </div>`).join("");
}

function renderServiceMetrics(metrics) {
  const node = $("#service-metrics");
  if (!metrics.length) {
    node.innerHTML = `<p class="placeholder">尚无测试服务指标。</p>`;
    return;
  }
  node.innerHTML = metrics.slice(0, 5).map((m) => `
    <div class="mini-row">
      <div><strong>${esc(m.path_node)} · ${esc(m.mode || "?")}</strong><br><span>${esc(m.endpoint)}</span></div>
      <span>${Number(m.throughput_bytes_per_sec || 0).toFixed(2)} B/s · err ${m.errors}</span>
    </div>`).join("");
}

async function refreshOverview() {
  try {
    const [health, data] = await Promise.all([api("/api/health"), api("/api/overview")]);
    const hp = $("#health-pill");
    hp.textContent = `SQLite ${health.sqlite} · OK`;
    hp.className = "pill ok";
    renderOverview(data);
  } catch (error) {
    $("#health-pill").textContent = "SQLite · 异常";
    $("#health-pill").className = "pill bad";
    toast(error.message, "error");
  }
}

async function selectFile(file) {
  if (!file) return;
  if (file.size > 32 * 1024 * 1024) {
    toast("文件超过 32 MiB", "error");
    return;
  }
  state.file = file;
  $("#file-label").textContent = `${file.name} · ${(file.size / 1024).toFixed(1)} KiB`;
  try {
    state.fileBase64 = await readFileBase64(file);
  } catch (error) {
    toast("读取文件失败", "error");
  }
}

function importRequest() {
  if (!state.fileBase64) throw new Error("请先选择诊断包");
  return {
    content_base64: state.fileBase64,
    filename: state.file?.name || null,
    timestamp_policy: $("#timestamp-policy").value,
    batch_id: $("#import-batch-id").value.trim() || null,
  };
}

function renderNegotiation(neg, normalization) {
  const decisions = { accept: "接受", degrade_accept: "降级接受", reject: "拒绝" };
  const degradations = neg.degradations || [];
  const normal = normalization || {};
  const rounding = normal.rounding || {};
  return `
    <div class="success-box">
      <span class="decision ${esc(neg.decision)}">${esc(decisions[neg.decision] || neg.decision)}</span>
      · ${esc(neg.format)} v${neg.version} · ${neg.records_detected} 条记录<br>
      timestamp ${esc(neg.units.timestamp)} · rtt ${esc(neg.units.rtt)} · payload ${esc(neg.units.payload)}
    </div>
    <dl class="kv" style="margin-top:12px">
      <dt>降级明细</dt><dd>${degradations.length ? esc(JSON.stringify(degradations)) : "无"}</dd>
      <dt>舍入规则</dt><dd>${esc(rounding.timestamp_policy || "—")} · ${esc(rounding.timestamp_policy_note || "—")}</dd>
      <dt>亚毫秒记录</dt><dd>${esc(rounding.records_with_sub_ms ?? "—")}</dd>
      <dt>累计舍入误差</dt><dd>${esc(rounding.cumulative_error_us ?? "—")} µs</dd>
    </dl>`;
}

async function doPreflight() {
  const button = $("#btn-preflight");
  loading(button);
  try {
    const data = await api("/api/preflight", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(importRequest()),
    });
    $("#import-result").innerHTML = renderNegotiation(data.negotiation, data.normalization);
    toast("预检完成，未写入数据", "ok");
  } catch (error) {
    $("#import-result").innerHTML = errorHtml(error);
    toast(error.body?.code || error.message, "error");
  } finally { loading(button, true); }
}

async function doImport() {
  const button = $("#btn-import");
  loading(button);
  try {
    const data = await api("/api/imports", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(importRequest()),
    });
    $("#import-result").innerHTML = `
      <div class="success-box"><strong>已提交 v${data.commit.version}</strong> · ${data.records_persisted} 条记录 · batch ${esc(data.batch_id)}${data.reused ? " · 复用幂等结果" : ""}</div>
      ${renderNegotiation(data.negotiation, data.normalization)}`;
    toast(`导入完整提交 v${data.commit.version}`, "ok");
    await refreshOverview();
  } catch (error) {
    $("#import-result").innerHTML = errorHtml(error);
    toast(error.body?.code || error.message, "error");
  } finally { loading(button, true); }
}

async function doProbe() {
  const button = $("#btn-probe");
  loading(button);
  const payload = {
    host: $("#probe-host").value.trim(),
    port: Number($("#probe-port").value),
    path_node: $("#probe-path").value.trim(),
    timeout_ms: Number($("#probe-timeout").value),
    payload_bytes: Number($("#probe-payload").value),
    batch_id: $("#probe-batch-id").value.trim() || null,
  };
  try {
    const response = await fetch("/api/probes", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload),
    });
    const data = await response.json();
    if (data.code) {
      $("#probe-result").innerHTML = `<div class="error-box"><strong>${esc(data.code)}</strong> · ${esc(data.message)}<br>outcome=${esc(data.probe?.outcome)} · socket_closed=${esc(data.probe?.socket_closed)} · elapsed=${esc(data.probe?.elapsed_us)}µs<br>commit v${esc(data.commit?.version)} 已持久化</div>`;
    } else {
      $("#probe-result").innerHTML = `<div class="success-box"><strong>PROBE_OK</strong> · RTT ${esc(data.probe.record.rtt_us)} µs · payload ${esc(data.probe.record.payload_bytes)} B<br>commit v${esc(data.commit.version)} · socket_closed=${esc(data.probe.socket_closed)}</div>`;
    }
    toast(data.code || "探测成功并已提交", data.code ? "error" : "ok");
    await refreshOverview();
  } catch (error) {
    $("#probe-result").innerHTML = errorHtml(error);
    toast(error.message, "error");
  } finally { loading(button, true); }
}

function filterQuery() {
  const params = new URLSearchParams();
  const start = $("#stats-start").value.trim();
  const end = $("#stats-end").value.trim();
  if (start) params.set("start_ms", start);
  if (end) params.set("end_ms", end);
  return params;
}

function renderStats(data) {
  state.lastStats = data;
  const rtt = data.rtt_us;
  const loss = data.loss;
  const jitter = data.jitter_us;
  $("#stats-cards").innerHTML = `
    <div class="metric"><span>丢包率</span><strong>${esc(loss.percent)}%</strong><small>${loss.failed}/${loss.total} · half-up 2dp</small></div>
    <div class="metric"><span>P50 / P95</span><strong>${esc(rtt.p50 ?? "—")}<small> / ${esc(rtt.p95 ?? "—")}</small></strong><small>µs · n=${esc(rtt.sample_count)}</small></div>
    <div class="metric"><span>抖动均值</span><strong>${esc(jitter.mean_abs_successive_us ?? "—")}</strong><small>µs · pairs=${esc(jitter.pair_count)}</small></div>
    <div class="metric"><span>故障等级</span><strong class="level-${esc(data.fault_level)}">${esc(data.fault_level)}</strong><small>${esc(data.sample.quantile_status)}</small></div>`;
  $("#stats-result").innerHTML = `<dl class="kv">
    <dt>快照版本</dt><dd>v${esc(data.snapshot_version)}</dd>
    <dt>有效 RTT 样本</dt><dd>${esc(data.sample.rtt_samples)} / required ${esc(data.sample.required_rtt_samples)}</dd>
    <dt>最近秩分位</dt><dd>P50=${esc(rtt.p50)} · P90=${esc(rtt.p90)} · P95=${esc(rtt.p95)} · P99=${esc(rtt.p99)} µs</dd>
    <dt>累计 RTT</dt><dd>exact=${esc(rtt.cumulative.exact_integer_sum_us)} µs · error≤${Number(rtt.cumulative.error_upper_bound_us).toExponential(3)}</dd>
    <dt>失败类型</dt><dd>${esc(JSON.stringify(data.failure_types))}</dd>
    <dt>稳定性摘要</dt><dd>${esc(data.calculation_digest)}</dd>
  </dl>`;
  drawChart();
}

async function loadStats() {
  const path = $("#stats-path").value;
  if (!path) { toast("暂无路径", "error"); return; }
  const params = filterQuery();
  params.set("path_node", path);
  params.set("min_samples", $("#stats-min").value || "5");
  try {
    const data = await api(`/api/stats?${params}`);
    renderStats(data);
    await drawChart(path);
  } catch (error) {
    $("#stats-cards").innerHTML = errorHtml(error);
    $("#stats-result").innerHTML = "";
    drawEmptyChart();
    toast(error.body?.code || error.message, "error");
  }
}

async function drawChart(path = $("#stats-path").value) {
  const svg = $("#rtt-chart");
  if (!path) { drawEmptyChart(); return; }
  try {
    const data = await api(`/api/records?path_node=${encodeURIComponent(path)}&limit=180`);
    const points = data.records;
    if (!points.length) { drawEmptyChart(); return; }
    const width = 760, height = 190, pad = { left: 42, right: 14, top: 16, bottom: 24 };
    const rtts = points.map((p) => p.rtt_us || 0);
    const min = Math.min(...rtts), max = Math.max(...rtts, min + 1);
    const x = (i) => pad.left + (i / Math.max(points.length - 1, 1)) * (width - pad.left - pad.right);
    const y = (value) => pad.top + (1 - (value - min) / Math.max(max - min, 1)) * (height - pad.top - pad.bottom);
    const pathData = points.map((p, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(p.rtt_us || 0).toFixed(1)}`).join(" ");
    const areaData = `${pathData} L${x(points.length - 1).toFixed(1)},${height - pad.bottom} L${x(0).toFixed(1)},${height - pad.bottom} Z`;
    const grid = [0, .25, .5, .75, 1].map((part) => {
      const gy = pad.top + part * (height - pad.top - pad.bottom);
      const value = Math.round(max - part * (max - min));
      return `<line class="gridline" x1="${pad.left}" y1="${gy}" x2="${width - pad.right}" y2="${gy}"></line><text class="axis" x="4" y="${gy + 3}">${value}</text>`;
    }).join("");
    const dots = points.map((p, i) => p.success
      ? `<circle class="point" cx="${x(i)}" cy="${y(p.rtt_us)}" r="2.4"></circle>`
      : `<rect class="failure" x="${x(i) - 3}" y="${height - pad.bottom - 6}" width="6" height="6"></rect>`).join("");
    svg.innerHTML = `<defs><linearGradient id="chartGradient" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stop-color="#0d786f" stop-opacity=".28"></stop><stop offset="1" stop-color="#0d786f" stop-opacity="0"></stop></linearGradient></defs>
      ${grid}<path class="area" d="${areaData}"></path><path class="line" d="${pathData}"></path>${dots}
      <text class="axis" x="${pad.left}" y="${height - 4}">oldest</text><text class="axis" x="${width - 50}" y="${height - 4}">newest</text>`;
    $("#chart-caption").textContent = `${path} · n=${points.length} · 方块=失败`;
  } catch (error) {
    drawEmptyChart();
  }
}

function drawEmptyChart() {
  $("#rtt-chart").innerHTML = `<text x="380" y="100" text-anchor="middle" fill="#7b878b" font-size="12" font-family="monospace">NO SAMPLE</text>`;
  $("#chart-caption").textContent = "—";
}

async function loadRoots() {
  const params = filterQuery();
  params.set("min_samples", $("#stats-min").value || "5");
  try {
    const data = await api(`/api/root-causes?${params}`);
    const candidates = data.candidates || [];
    if (!candidates.length) {
      $("#root-list").innerHTML = `<p class="placeholder">没有可排序路径。</p>`;
      return;
    }
    $("#root-list").innerHTML = candidates.map((c) => {
      const basis = c.rank_basis;
      const score = Math.max(0, Math.min(100, Number(c.rank_score) * 100));
      return `<div class="root-item">
        <div class="root-top"><span class="root-path">${esc(c.path_node)}</span><span class="root-score level-${esc(c.fault_level)}">${esc(c.rank_score)} · ${esc(c.fault_level)}</span></div>
        <div class="score-track"><div class="score-fill" style="width:${score}%"></div></div>
        <div class="root-meta"><span>loss ${esc(c.loss.percent)}% (${basis.loss_failed}/${basis.loss_total})</span><span>jitter ${basis.jitter_mean_abs_successive_us == null ? "NULL" : `${Number(basis.jitter_mean_abs_successive_us).toFixed(2)}µs`}</span><span>fail ${esc(JSON.stringify(basis.failure_types))}</span><span>pairs ${esc(basis.jitter_pair_count)}</span></div>
      </div>`;
    }).join("") + (data.warnings?.length ? `<div class="error-box">${esc(JSON.stringify(data.warnings))}</div>` : "");
  } catch (error) {
    $("#root-list").innerHTML = errorHtml(error);
  }
}

async function recomputeStats() {
  try {
    const data = await api("/api/stats/recompute", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: "{}",
    });
    toast(`统计快照已重算为 v${data.commit.version}`, "ok");
    await refreshOverview();
    await loadStats();
  } catch (error) { toast(error.body?.code || error.message, "error"); }
}

async function pollMetrics() {
  try {
    const data = await api("/api/service-metrics/poll", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ admin_url: $("#metrics-url").value.trim(), path_node: $("#metrics-path").value.trim() }),
    });
    const m = data.service_metrics.metrics;
    $("#service-metrics").innerHTML = `<div class="mini-row"><div><strong>${esc(m.mode)} · ${esc(data.service_metrics.path_node)}</strong><br><span>accepted ${m.accepted_connections} · completed ${m.completed_probes} · errors ${m.errors}</span></div><span>${Number(m.throughput_bytes_per_sec || 0).toFixed(2)} B/s</span></div>`;
    toast(`测试服务指标已提交 v${data.commit.version}`, "ok");
    await refreshOverview();
  } catch (error) { toast(error.body?.code || error.message, "error"); }
}

async function loadRecovery() {
  try {
    const data = await api("/api/recovery/report");
    $("#recovery-summary").innerHTML = `
      <div class="metric"><span>完整版本</span><strong>${data.latest_complete_version}</strong><small>records ${data.record_count}</small></div>
      <div class="metric"><span>未完成意图</span><strong>${data.pending_intents_now}</strong><small>启动扫描后</small></div>
      <div class="metric"><span>恢复事件</span><strong>${data.recovery_events.length}</strong><small>facts only</small></div>`;
    $("#recovery-result").innerHTML = `<pre>${esc(JSON.stringify({ difference_report: data.difference_report, batch_status_counts: data.batch_status_counts, intent_state_counts: data.intent_state_counts }, null, 2))}</pre>`;
  } catch (error) {
    $("#recovery-summary").innerHTML = errorHtml(error);
  }
}

function updateExportLinks() {
  const path = $("#export-path").value;
  const endian = $("#export-endian").value;
  const query = new URLSearchParams();
  if (path) query.set("path_node", path);
  if ($("#stats-start").value) query.set("start_ms", $("#stats-start").value);
  if ($("#stats-end").value) query.set("end_ms", $("#stats-end").value);
  const base = query.toString();
  $("#export-bin").href = `/api/exports/binary${base ? `?${base}&endian=${endian}` : `?endian=${endian}`}`;
  $("#export-jsonl").href = `/api/exports/jsonl${base ? `?${base}` : ""}`;
}

function bindFileInput() {
  const input = $("#file-input");
  const zone = $("#drop-zone");
  zone.addEventListener("click", (event) => {
    if (event.target !== input) input.click();
  });
  input.addEventListener("change", () => selectFile(input.files[0]));
  ["dragenter", "dragover"].forEach((name) => zone.addEventListener(name, (event) => {
    event.preventDefault(); zone.classList.add("dragging");
  }));
  ["dragleave", "drop"].forEach((name) => zone.addEventListener(name, (event) => {
    event.preventDefault(); zone.classList.remove("dragging");
  }));
  zone.addEventListener("drop", (event) => selectFile(event.dataTransfer.files[0]));
}

function bind() {
  bindFileInput();
  $("#btn-refresh").onclick = refreshOverview;
  $("#btn-preflight").onclick = doPreflight;
  $("#btn-import").onclick = doImport;
  $("#btn-probe").onclick = doProbe;
  $("#btn-stats").onclick = loadStats;
  $("#btn-roots").onclick = loadRoots;
  $("#btn-recompute").onclick = recomputeStats;
  $("#btn-metrics").onclick = pollMetrics;
  $("#btn-recovery").onclick = loadRecovery;
  $("#export-path").onchange = updateExportLinks;
  $("#export-endian").onchange = updateExportLinks;
  $("#stats-path").onchange = () => { updateExportLinks(); drawChart(); };
  ["#stats-start", "#stats-end"].forEach((selector) => $(selector).addEventListener("input", updateExportLinks));
}

async function boot() {
  bind();
  drawEmptyChart();
  await refreshOverview();
  await loadRecovery();
  if ($("#stats-path").value) {
    await loadStats();
    await loadRoots();
  }
}

boot();
