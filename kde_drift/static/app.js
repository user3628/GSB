"use strict";

const $ = (selector) => document.querySelector(selector);
const byId = (id) => document.getElementById(id);

const state = {
  datasets: [],
  config: null,
  status: null,
  results: null,
  selectedColumn: null,
  running: false,
  pollTimer: null,
  logs: [],
};

const STAGE_LABELS = {
  bandwidth: "BANDWIDTH",
  density: "DENSITY",
  metrics: "METRICS",
  drift: "DRIFT",
};

function nowLabel() {
  return new Date().toLocaleTimeString("zh-CN", { hour12: false });
}

function log(message, kind = "") {
  state.logs.push({ at: nowLabel(), message, kind });
  if (state.logs.length > 120) state.logs.shift();
  const consoleNode = byId("log-console");
  consoleNode.innerHTML = state.logs
    .map((item) => `<div class="log-line ${item.kind}"><span class="log-time">${escapeHtml(item.at)}</span>${escapeHtml(item.message)}</div>`)
    .join("");
  consoleNode.scrollTop = consoleNode.scrollHeight;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function shortHash(value, size = 12) {
  if (!value) return "—";
  return String(value).slice(0, size) + "…";
}

function formatNumber(value, digits = 5) {
  if (value === null || value === undefined || value === "") return "—";
  const number = Number(value);
  if (!Number.isFinite(number)) return String(value);
  if (number !== 0 && (Math.abs(number) < 1e-3 || Math.abs(number) >= 1e4)) return number.toExponential(digits - 1);
  return number.toFixed(digits);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const contentType = response.headers.get("Content-Type") || "";
  const payload = contentType.includes("json") ? await response.json() : await response.text();
  if (!response.ok || payload?.ok === false) {
    const error = payload?.error || { code: "E_IO", message: String(payload), stage: "http", location: path };
    throw error;
  }
  return payload;
}

function configFromForm() {
  const rule = byId("bandwidth-rule").value;
  return {
    schema_version: 1,
    kernel: byId("kernel").value,
    bandwidth: {
      rule,
      value: rule === "fixed" ? Number(byId("bandwidth-value").value) : null,
    },
    imputation: { method: byId("imputation").value },
    grid: { points: Number(byId("grid-points").value) },
    chunk_size: Number(byId("chunk-size").value),
    summation: byId("summation").value,
    mmd: {
      kernel: "gaussian",
      bandwidth_scale: 1.0,
      chunk_size: Number(byId("mmd-chunk-size").value),
      max_eval_samples: Number(byId("max-eval-samples").value),
      bootstrap_sample_size: Number(byId("bootstrap-sample-size").value),
    },
    kl: { epsilon: 1e-12 },
    drift: {
      threshold: Number(byId("threshold").value),
      alpha: Number(byId("alpha").value),
      bootstrap_resamples: Number(byId("resamples").value),
      seed: Number(byId("drift-seed").value),
    },
  };
}

function applyConfigToForm(config) {
  if (!config) return;
  byId("kernel").value = config.kernel;
  byId("bandwidth-rule").value = config.bandwidth.rule;
  byId("bandwidth-value").value = config.bandwidth.value ?? 0.25;
  byId("imputation").value = config.imputation.method;
  byId("grid-points").value = config.grid.points;
  byId("chunk-size").value = config.chunk_size;
  byId("summation").value = config.summation;
  byId("mmd-chunk-size").value = config.mmd.chunk_size;
  byId("max-eval-samples").value = config.mmd.max_eval_samples;
  byId("bootstrap-sample-size").value = config.mmd.bootstrap_sample_size;
  byId("threshold").value = config.drift.threshold;
  byId("alpha").value = config.drift.alpha;
  byId("resamples").value = config.drift.bootstrap_resamples;
  byId("drift-seed").value = config.drift.seed;
  toggleFixedBandwidth();
}

function toggleFixedBandwidth() {
  byId("fixed-bandwidth-field").hidden = byId("bandwidth-rule").value !== "fixed";
}

function setRunState(value) {
  const normalized = String(value || "idle").toLowerCase();
  const pill = byId("run-state-pill");
  pill.textContent = normalized.toUpperCase();
  pill.className = `pill ${normalized}`;
  byId("state-value").textContent = normalized.toUpperCase();
  const active = normalized === "running" || normalized === "stopping";
  state.running = active;
  byId("run-button").disabled = active;
  byId("resume-button").disabled = active;
  byId("force-button").disabled = active;
  byId("stop-button").disabled = !active;
}

function showError(error) {
  if (!error) {
    byId("error-banner").hidden = true;
    return;
  }
  byId("error-banner").hidden = false;
  byId("error-code").textContent = error.code || "E_IO";
  byId("error-title").textContent = `${error.code || "E_IO"} · ${error.stage || "runtime"}`;
  byId("error-message").textContent = error.message || "unknown error";
  byId("error-location").textContent = [error.column, error.location].filter(Boolean).join(" @ ");
  log(`${error.code || "E_IO"}: ${error.message || "unknown error"}`, "error");
}

async function initialLoad() {
  try {
    const [datasetResponse, health] = await Promise.all([api("/api/datasets"), api("/api/health")]);
    state.datasets = datasetResponse.datasets || [];
    log(`connected to ${health.service}; numpy ${health.numpy}`);
    const status = await api("/api/status");
    state.status = status;
    state.config = status.config;
    applyConfigToForm(status.config);
    setDatasetButton(status.dataset);
    renderStatus(status);
    if (status.analysis?.valid) await refreshResults();
    startPolling(status.state);
  } catch (error) {
    showError(error);
  }
}

function setDatasetButton(dataset) {
  document.querySelectorAll("#dataset-switch button").forEach((button) => {
    button.classList.toggle("active", button.dataset.dataset === dataset);
  });
}

function renderStatus(status) {
  state.status = status;
  setRunState(status.state);
  byId("config-fingerprint").textContent = shortHash(status.analysis?.config_version, 24);
  byId("workdir-path").textContent = status.workdir || "—";
  byId("sequence-value").textContent = status.analysis?.latest_sequence ?? 0;
  byId("elapsed-value").textContent = Number.isFinite(status.elapsed_seconds)
    ? `${Number(status.elapsed_seconds).toFixed(2)} s · ${status.dataset}`
    : `${status.dataset} · deterministic`;

  const summary = status.last_summary;
  const validDriftColumns = findDriftColumns(status, state.results);
  if (summary) {
    byId("drift-value").textContent = summary.drift_count;
    byId("drift-detail").textContent = summary.drift_columns.join(", ") || "无漂移列";
  } else if (validDriftColumns.length) {
    byId("drift-value").textContent = validDriftColumns.length;
    byId("drift-detail").textContent = validDriftColumns.join(", ");
  } else {
    byId("drift-value").textContent = status.analysis?.valid ? "0" : "—";
    byId("drift-detail").textContent = status.analysis?.valid ? "无漂移列" : "尚无完成结果";
  }

  const bound = maximumBound(state.results);
  byId("bound-value").textContent = bound === null ? "—" : formatNumber(bound, 3);

  const error = status.last_error?.error;
  if (error) showError(error);
  else if (status.state !== "error") showError(null);
  if (status.last_stop) log(`stopped at ${status.last_stop.stage}/${status.last_stop.location}`);
  renderStages(status.analysis);
  renderInvalidation(status.analysis);
  renderProgress(status.progress || []);
  if (status.state === "completed") {
    byId("drift-value").textContent = summary?.drift_count ?? "—";
  }
}

function findDriftColumns(status, results) {
  const drift = results?.payloads?.drift;
  if (!drift) return [];
  return drift.columns.filter((column) => column.drift).map((column) => column.name);
}

function maximumBound(results) {
  const density = results?.payloads?.density;
  if (!density?.columns?.length) return null;
  let maximum = 0;
  for (const column of density.columns) {
    maximum = Math.max(
      maximum,
      column.reference_diagnostics.verified_upper_bound,
      column.current_diagnostics.verified_upper_bound,
    );
  }
  return maximum;
}

function renderStages(analysis) {
  const track = byId("stage-track");
  if (!analysis?.stages) {
    track.innerHTML = '<div class="stage-empty">运行后将在此显示带宽、密度、指标与漂移阶段。</div>';
    return;
  }
  track.innerHTML = Object.entries(STAGE_LABELS)
    .map(([stage, label]) => {
      const value = analysis.stages[stage];
      const checkpoint = value?.checkpoint;
      const stateName = value?.state || "missing";
      const fp = checkpoint?.stage_input_fingerprint || value?.expected_stage_input_fingerprint;
      return `<article class="stage-card ${stateName}">
        <div class="stage-top"><span class="stage-name">${label}</span><span class="stage-state">${stateName}</span></div>
        <div class="stage-meta">
          <span>SEQ <code>${checkpoint?.sequence ?? "∅"}</code></span>
          <span>REV <code>${checkpoint?.stage_revision ?? "∅"}</code></span>
          <span>FP <code title="${escapeHtml(fp || "")}">${shortHash(fp, 10)}</code></span>
        </div>
      </article>`;
    })
    .join("");
}

function renderInvalidation(analysis) {
  if (!analysis) return;
  const reusable = analysis.reusable_stages || [];
  const affected = analysis.affected_stages || [];
  byId("invalidation-pill").textContent = analysis.valid ? "CACHE VALID" : `REBUILD FROM ${String(analysis.earliest_invalid_stage).toUpperCase()}`;
  byId("invalidation-pill").style.color = analysis.valid ? "var(--green)" : "var(--amber)";
  const reasons = [];
  for (const [stage, value] of Object.entries(analysis.stages || {})) {
    for (const reason of value.reasons || []) {
      const paths = reason.changed_paths?.length ? ` (${reason.changed_paths.join(", ")})` : "";
      reasons.push(`<div class="reason"><code>${escapeHtml(stage)}</code> ${escapeHtml(reason.code)}${escapeHtml(paths)}</div>`);
    }
  }
  byId("invalidation-content").innerHTML = `
    <div class="invalidation-block">
      <h3>Reusable upstream</h3>
      <div class="stage-chips">${reusable.length ? reusable.map((s) => `<span class="stage-chip valid">${escapeHtml(s)}</span>`).join("") : '<span class="stage-chip">none</span>'}</div>
      <h3 style="margin-top:12px">Affected chain</h3>
      <div class="stage-chips">${affected.length ? affected.map((s) => `<span class="stage-chip invalid">${escapeHtml(s)}</span>`).join("") : '<span class="stage-chip valid">none</span>'}</div>
    </div>
    <div class="invalidation-block">
      <h3>Fingerprint / reason</h3>
      <div class="reason">pipeline <code>${shortHash(analysis.pipeline_fingerprint, 18)}</code></div>
      <div class="reason">config <code>${shortHash(analysis.config_version, 18)}</code></div>
      <div class="reason-list">${reasons.join("") || '<div class="reason">all stage input fingerprints match</div>'}</div>
    </div>`;
}

function renderProgress(progress) {
  if (!progress.length) return;
  const last = progress[progress.length - 1];
  if (last.event === "stage_complete") log(`stage complete: ${last.stage} · sequence ${last.sequence}`, "done");
  else if (last.event === "run_error") log(`${last.error.code}: ${last.error.message}`, "error");
  else if (last.event === "run_stopped") log(`graceful stop after ${last.completed_units} units at ${last.location}`);
}

async function refreshStatus({ refreshResultsIfValid = false } = {}) {
  try {
    const status = await api("/api/status");
    const wasActive = state.running;
    renderStatus(status);
    if (refreshResultsIfValid && status.analysis?.valid) await refreshResults();
    if (status.analysis?.valid && (!state.results || !wasActive)) await refreshResults();
    startPolling(status.state);
  } catch (error) {
    showError(error);
  }
}

function startPolling(runState) {
  if (state.pollTimer) clearTimeout(state.pollTimer);
  const active = ["running", "stopping"].includes(runState);
  state.pollTimer = setTimeout(() => refreshStatus({ refreshResultsIfValid: !active }), active ? 650 : 2400);
}

async function refreshResults() {
  try {
    const results = await api("/api/results");
    state.results = results;
    renderResults(results);
  } catch (error) {
    showError(error);
  }
}

function renderResults(results) {
  if (!results?.payloads?.metrics) return;
  const densityById = new Map((results.payloads.density?.columns || []).map((c) => [c.name, c]));
  const bandwidthById = new Map((results.payloads.bandwidth?.columns || []).map((c) => [c.name, c]));
  const driftById = new Map((results.payloads.drift?.columns || []).map((c) => [c.name, c]));
  const rows = results.payloads.metrics.columns.map((metric) => {
    const drift = driftById.get(metric.name);
    const bandwidth = bandwidthById.get(metric.name);
    const missing = Number(bandwidth?.reference?.imputation?.missing_count || 0) + Number(bandwidth?.current?.imputation?.missing_count || 0);
    return `<tr data-column="${escapeHtml(metric.name)}" class="${metric.name === state.selectedColumn ? "selected" : ""}">
      <td>${escapeHtml(metric.name)}</td>
      <td>${formatNumber(metric.mmd.score)}</td>
      <td>${formatNumber(metric.kl.value)}</td>
      <td>${formatNumber(drift?.p_value, 4)}</td>
      <td>${formatNumber(bandwidth?.reference?.bandwidth, 3)} / ${formatNumber(bandwidth?.current?.bandwidth, 3)}</td>
      <td>${missing || "—"}</td>
      <td><span class="status-badge ${drift?.drift ? "drift" : "stable"}">${drift?.drift ? "DRIFT" : "STABLE"}</span></td>
    </tr>`;
  });
  byId("metrics-body").innerHTML = rows.join("");
  document.querySelectorAll("#metrics-body tr[data-column]").forEach((row) => {
    row.addEventListener("click", () => selectColumn(row.dataset.column));
  });

  const columns = results.payloads.metrics.columns.map((column) => column.name);
  if (!state.selectedColumn || !columns.includes(state.selectedColumn)) {
    state.selectedColumn = results.payloads.drift?.summary?.drift_columns?.[0] || columns[0];
  }
  const select = byId("column-select");
  select.innerHTML = columns.map((name) => `<option value="${escapeHtml(name)}">${escapeHtml(name)}</option>`).join("");
  select.value = state.selectedColumn;
  select.onchange = () => selectColumn(select.value);
  renderChart(densityById.get(state.selectedColumn));
  renderDiagnostics(densityById.get(state.selectedColumn), bandwidthById.get(state.selectedColumn), driftById.get(state.selectedColumn));
  byId("chart-empty").hidden = Boolean(densityById.get(state.selectedColumn));
  byId("table-summary").textContent = `${results.summary?.drift_count ?? 0} drift / ${columns.length} columns · score > threshold ∧ p ≤ α`;
  byId("drift-value").textContent = results.summary?.drift_count ?? "—";
  byId("drift-detail").textContent = (results.summary?.drift_columns || []).join(", ") || "无漂移列";
  byId("bound-value").textContent = formatNumber(maximumBound(results), 3);
}

function selectColumn(name) {
  state.selectedColumn = name;
  document.querySelectorAll("#metrics-body tr[data-column]").forEach((row) => {
    row.classList.toggle("selected", row.dataset.column === name);
  });
  byId("column-select").value = name;
  if (!state.results?.payloads) return;
  const density = (state.results.payloads.density?.columns || []).find((c) => c.name === name);
  const bandwidth = (state.results.payloads.bandwidth?.columns || []).find((c) => c.name === name);
  const drift = (state.results.payloads.drift?.columns || []).find((c) => c.name === name);
  renderChart(density);
  renderDiagnostics(density, bandwidth, drift);
}

function renderDiagnostics(density, bandwidth, drift) {
  if (!density || !bandwidth) {
    byId("diagnostic-content").innerHTML = '<div class="empty-copy">选择列并完成评估后显示高精度参考误差包络、带宽和填充信息。</div>';
    return;
  }
  const refDiag = density.reference_diagnostics;
  const curDiag = density.current_diagnostics;
  const refMissing = bandwidth.reference.imputation;
  const curMissing = bandwidth.current.imputation;
  const missingTotal = refMissing.missing_count + curMissing.missing_count;
  byId("diagnostic-content").innerHTML = `
    <div class="diag-title"><span>${escapeHtml(density.name)}</span><span class="status-badge ${drift?.drift ? "drift" : "stable"}">${drift?.drift ? "DRIFT" : "STABLE"}</span></div>
    <div class="diag-grid">
      <div class="diag-item"><span>Ref bandwidth</span><strong>${formatNumber(bandwidth.reference.bandwidth, 6)}</strong></div>
      <div class="diag-item"><span>Cur bandwidth</span><strong>${formatNumber(bandwidth.current.bandwidth, 6)}</strong></div>
      <div class="diag-item"><span>Theoretical bound</span><strong>${formatNumber(Math.max(refDiag.theoretical_upper_bound, curDiag.theoretical_upper_bound), 4)}</strong></div>
      <div class="diag-item"><span>High-precision err</span><strong>${formatNumber(Math.max(refDiag.observed_high_precision_error, curDiag.observed_high_precision_error), 4)}</strong></div>
      <div class="diag-item"><span>Verified bound</span><strong>${formatNumber(Math.max(refDiag.verified_upper_bound, curDiag.verified_upper_bound), 4)}</strong></div>
      <div class="diag-item"><span>Chunks · ${escapeHtml(refDiag.summation)}</span><strong>${refDiag.chunks} / ${curDiag.chunks}</strong></div>
    </div>
    ${missingTotal ? `<div class="missing-note">缺失值已重估：ref ${refMissing.missing_count} 个，填充 ${refMissing.fill_method} = ${formatNumber(refMissing.fill_value, 6)}；cur ${curMissing.missing_count} 个，填充 ${curMissing.fill_method} = ${formatNumber(curMissing.fill_value, 6)}。</div>` : `<div class="missing-note" style="border-color:var(--line-strong);color:var(--muted);background:rgba(255,255,255,.015)">该列无缺失值；KDE 使用规范化列序与固定分块归约。</div>`}
    <div class="diag-grid">
      <div class="diag-item"><span>MMD²</span><strong>${formatNumber(drift?.score, 6)}</strong></div>
      <div class="diag-item"><span>p-value / α</span><strong>${formatNumber(drift?.p_value, 4)} / ${formatNumber(drift?.alpha, 4)}</strong></div>
    </div>`;
}

function renderChart(density) {
  const canvas = byId("density-chart");
  const rect = canvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  canvas.width = Math.max(300, Math.floor(rect.width * dpr));
  canvas.height = Math.max(180, Math.floor(rect.height * dpr));
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  const width = rect.width;
  const height = rect.height;
  ctx.clearRect(0, 0, width, height);
  byId("chart-empty").hidden = Boolean(density);
  if (!density) {
    byId("chart-stats").textContent = "—";
    return;
  }
  const padding = { left: 45, right: 17, top: 17, bottom: 31 };
  const plotW = width - padding.left - padding.right;
  const plotH = height - padding.top - padding.bottom;
  const x = density.grid;
  const ref = density.reference_density;
  const cur = density.current_density;
  const xMin = Math.min(...x);
  const xMax = Math.max(...x);
  const yMax = Math.max(...ref, ...cur) * 1.09 || 1;
  const px = (value) => padding.left + ((value - xMin) / (xMax - xMin || 1)) * plotW;
  const py = (value) => padding.top + plotH - (value / yMax) * plotH;

  ctx.strokeStyle = "rgba(134,185,207,.10)";
  ctx.lineWidth = 1;
  ctx.font = "9px SFMono-Regular, Consolas, monospace";
  ctx.fillStyle = "rgba(126,154,170,.8)";
  for (let i = 0; i <= 4; i += 1) {
    const y = padding.top + (plotH * i) / 4;
    ctx.beginPath();
    ctx.moveTo(padding.left, y);
    ctx.lineTo(width - padding.right, y);
    ctx.stroke();
    const value = yMax * (1 - i / 4);
    ctx.fillText(value.toFixed(3), 7, y + 3);
  }
  for (let i = 0; i <= 4; i += 1) {
    const xPos = padding.left + (plotW * i) / 4;
    ctx.beginPath();
    ctx.moveTo(xPos, padding.top);
    ctx.lineTo(xPos, height - padding.bottom);
    ctx.stroke();
  }
  ctx.fillStyle = "rgba(126,154,170,.8)";
  ctx.fillText(xMin.toFixed(2), padding.left, height - 10);
  ctx.fillText(xMax.toFixed(2), width - padding.right - 30, height - 10);

  function drawCurve(values, stroke, fill, lineWidth) {
    ctx.beginPath();
    values.forEach((value, index) => {
      const xPos = px(x[index]);
      const yPos = py(value);
      if (index === 0) ctx.moveTo(xPos, yPos);
      else ctx.lineTo(xPos, yPos);
    });
    ctx.lineTo(px(x[x.length - 1]), height - padding.bottom);
    ctx.lineTo(px(x[0]), height - padding.bottom);
    ctx.closePath();
    ctx.fillStyle = fill;
    ctx.fill();
    ctx.beginPath();
    values.forEach((value, index) => {
      const xPos = px(x[index]);
      const yPos = py(value);
      if (index === 0) ctx.moveTo(xPos, yPos);
      else ctx.lineTo(xPos, yPos);
    });
    ctx.strokeStyle = stroke;
    ctx.lineWidth = lineWidth;
    ctx.stroke();
  }
  drawCurve(ref, "#52e0d0", "rgba(82,224,208,.08)", 1.8);
  drawCurve(cur, "#f5b65a", "rgba(245,182,90,.055)", 1.8);
  byId("chart-stats").textContent = `${density.name} · h=${formatNumber(density.kernel_parameters.reference_bandwidth, 4)}/${formatNumber(density.kernel_parameters.current_bandwidth, 4)} · ${density.grid.length} grid points`;
}

async function runEvaluation({ force = false } = {}) {
  try {
    showError(null);
    const config = configFromForm();
    const batchRaw = byId("batch-size").value;
    const body = {
      dataset: currentDataset(),
      config,
      resume: true,
      force,
      batch_size: batchRaw ? Number(batchRaw) : null,
      batch_order: byId("batch-order").value,
    };
    log(`${force ? "force rebuild" : "evaluation"} requested · ${body.dataset}`);
    await api("/api/run", { method: "POST", body: JSON.stringify(body) });
    state.results = null;
    setTimeout(() => refreshStatus(), 250);
  } catch (error) {
    showError(error);
  }
}

async function resumeEvaluation() {
  try {
    showError(null);
    const body = {
      dataset: currentDataset(),
      config: configFromForm(),
      batch_size: byId("batch-size").value ? Number(byId("batch-size").value) : null,
      batch_order: byId("batch-order").value,
    };
    await api("/api/resume", { method: "POST", body: JSON.stringify(body) });
    log("resume requested");
    setTimeout(() => refreshStatus(), 250);
  } catch (error) {
    showError(error);
  }
}

async function stopEvaluation() {
  try {
    const response = await api("/api/stop", { method: "POST", body: "{}" });
    log(response.accepted ? "cooperative stop requested" : response.message);
    setTimeout(() => refreshStatus(), 250);
  } catch (error) {
    showError(error);
  }
}

function currentDataset() {
  return document.querySelector("#dataset-switch button.active")?.dataset.dataset || state.status?.dataset || "small";
}

async function switchDataset(dataset) {
  if (state.running) return;
  try {
    setDatasetButton(dataset);
    const response = await api("/api/session", {
      method: "POST",
      body: JSON.stringify({ dataset, config: configFromForm() }),
    });
    state.config = response.session.config;
    state.results = null;
    log(`dataset switched to ${dataset}`);
    await refreshStatus();
  } catch (error) {
    showError(error);
  }
}

async function resetConfig() {
  try {
    const response = await api("/api/config/default");
    applyConfigToForm(response.config);
    log("default configuration restored in form");
  } catch (error) {
    showError(error);
  }
}

function bindEvents() {
  byId("bandwidth-rule").addEventListener("change", toggleFixedBandwidth);
  byId("run-button").addEventListener("click", () => runEvaluation());
  byId("force-button").addEventListener("click", () => runEvaluation({ force: true }));
  byId("resume-button").addEventListener("click", resumeEvaluation);
  byId("stop-button").addEventListener("click", stopEvaluation);
  byId("reset-config").addEventListener("click", resetConfig);
  byId("clear-log").addEventListener("click", () => {
    state.logs = [];
    byId("log-console").innerHTML = "";
  });
  document.querySelectorAll("#dataset-switch button").forEach((button) => {
    button.addEventListener("click", () => switchDataset(button.dataset.dataset));
  });
  document.querySelectorAll("input, select").forEach((element) => {
    element.addEventListener("change", () => {
      const fingerprint = byId("config-fingerprint");
      fingerprint.style.color = "var(--amber)";
      fingerprint.textContent = "configuration changed · pending run";
    });
  });
  window.addEventListener("resize", () => {
    if (!state.results?.payloads) return;
    const density = (state.results.payloads.density?.columns || []).find((c) => c.name === state.selectedColumn);
    renderChart(density);
  });
}

document.addEventListener("DOMContentLoaded", () => {
  bindEvents();
  initialLoad();
});
