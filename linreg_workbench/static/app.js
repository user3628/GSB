"use strict";

const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => Array.from(root.querySelectorAll(selector));

const state = {
  datasets: [],
  session: null,
  status: null,
  result: null,
  summary: null,
  trajectory: null,
  checkpoints: null,
  artifactsForRun: null,
  tab: "overview",
  mode: "elasticnet",
  initialized: false,
  polling: false,
  timer: null,
  lastError: null,
};

const stageNames = {
  preprocess: "预处理",
  folds: "折划分",
  solve: "逐折求解",
  aggregate: "汇总",
};
const statusNames = {
  idle: "空闲",
  created: "已创建",
  running: "运行中",
  stopping: "停止中",
  stopped: "已停止",
  complete: "已完成",
  error: "异常",
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function fmt(value, digits = 6) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  const number = Number(value);
  if (Math.abs(number) >= 1e5 || (Math.abs(number) > 0 && Math.abs(number) < 1e-3)) {
    return number.toExponential(3);
  }
  return number.toFixed(digits).replace(/0+$/, "").replace(/\.$/, "");
}

function fmtMetric(value, metric = "log_loss") {
  if (value === null || value === undefined) return "—";
  return metric === "accuracy" ? `${(Number(value) * 100).toFixed(2)}%` : fmt(value, 6);
}

function shortHash(value, head = 12, tail = 6) {
  if (!value) return "—";
  const text = String(value);
  if (text.length <= head + tail + 1) return text;
  return `${text.slice(0, head)}…${text.slice(-tail)}`;
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  let payload;
  try {
    payload = await response.json();
  } catch (_error) {
    throw { code: "E_HTTP", message: `HTTP ${response.status}`, details: {} };
  }
  if (!response.ok || payload.ok === false) {
    throw payload.error || { code: "E_HTTP", message: `HTTP ${response.status}`, details: payload };
  }
  return payload;
}

function toast(message, kind = "info") {
  const node = document.createElement("div");
  node.className = `toast ${kind === "error" ? "err" : ""}`;
  node.textContent = message;
  $("#toastStack").append(node);
  setTimeout(() => node.remove(), 4200);
}

function showError(error, notify = true) {
  const key = `${error.code || "E_UNKNOWN"}\u0000${error.message || String(error)}`;
  state.lastError = error;
  $("#errorBanner").hidden = false;
  $("#errorCode").textContent = error.code || "E_UNKNOWN";
  $("#errorMessage").textContent = error.message || String(error);
  const details = error.details && Object.keys(error.details).length ? error.details : null;
  $("#errorDetailsWrap").hidden = !details;
  $("#errorDetails").textContent = details ? JSON.stringify(details, null, 2) : "";
  if (notify && state.lastErrorToastKey !== key) {
    state.lastErrorToastKey = key;
    toast(`${error.code || "错误"}: ${error.message || error}`, "error");
  }
}

function dismissError() {
  $("#errorBanner").hidden = true;
  state.lastError = null;
  state.lastErrorToastKey = null;
}

function updateDatasetSelect() {
  const select = $("#datasetSelect");
  select.innerHTML = state.datasets
    .map(
      (item) =>
        `<option value="${escapeHtml(item.dataset_id)}">${escapeHtml(item.dataset_id)} · ${item.rows.toLocaleString()} × ${item.columns}</option>`
    )
    .join("");
}

function renderDatasetMeta() {
  const id = $("#datasetSelect").value;
  const dataset = state.datasets.find((item) => item.dataset_id === id);
  if (!dataset) {
    $("#datasetMeta").innerHTML = "";
    return;
  }
  const missing = dataset.missing_columns?.length || 0;
  const redundant = dataset.redundant_columns?.length || 0;
  const positives = dataset.class_counts?.["1"] ?? dataset.class_counts?.[1] ?? "—";
  $("#datasetMeta").innerHTML = `
    <span class="meta-chip"><strong>${dataset.rows.toLocaleString()}</strong> rows</span>
    <span class="meta-chip"><strong>${dataset.columns}</strong> cols</span>
    <span class="meta-chip"><strong>${dataset.feature_count}</strong> feats</span>
    <span class="meta-chip"><strong>${escapeHtml(positives)}</strong> positive</span>
    ${missing ? `<span class="meta-chip warn"><strong>${missing}</strong> missing cols</span>` : ""}
    ${redundant ? `<span class="meta-chip warn"><strong>${redundant}</strong> redundant</span>` : ""}
  `;
}

function setFormValue(key, value) {
  const node = $(`[data-config="${key}"]`);
  if (node) node.value = value;
}

function setMode(mode) {
  state.mode = mode;
  $$("#regularizationMode button").forEach((button) => {
    button.classList.toggle("active", button.dataset.mode === mode);
  });
}

function populateForm(config) {
  if (!config) return;
  $("#datasetSelect").value = config.dataset_id;
  setFormValue("normalization", config.preprocessing.numeric_normalization);
  setFormValue("numericImputation", config.preprocessing.numeric_imputation);
  setFormValue("encoding", config.preprocessing.categorical_encoding);
  setFormValue("categoricalImputation", config.preprocessing.categorical_imputation);
  setFormValue("smoothing", config.preprocessing.target_encoding_smoothing);
  setFormValue("folds", config.cv.folds);
  setFormValue("seed", config.cv.seed);
  setFormValue("metric", config.cv.metric);
  setFormValue("strengths", config.regularization.strengths.join(", "));
  setFormValue("l1Ratios", config.regularization.l1_ratios.join(", "));
  setFormValue("solver", config.solver.name);
  setFormValue("tolerance", config.solver.tolerance);
  setFormValue("maxIter", config.solver.max_iter);
  setFormValue("objectiveLimit", config.solver.objective_limit);
  setMode(config.regularization.mode);
  renderDatasetMeta();
}

function parseNumberList(text, label) {
  const result = String(text)
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)
    .map(Number);
  if (!result.length || result.some((item) => !Number.isFinite(item))) {
    throw { code: "E_INVALID_HYPERPARAM", message: `${label} 必须是逗号分隔的有限数值`, details: { value: text } };
  }
  return result;
}

function numberValue(key, label) {
  const raw = $(`[data-config="${key}"]`).value;
  const number = Number(raw);
  if (!Number.isFinite(number)) {
    throw { code: "E_INVALID_HYPERPARAM", message: `${label} 必须是有效数值`, details: { value: raw } };
  }
  return number;
}

function collectConfigPatch() {
  return {
    dataset_id: $("#datasetSelect").value,
    preprocessing: {
      numeric_normalization: $('[data-config="normalization"]').value,
      numeric_imputation: $('[data-config="numericImputation"]').value,
      categorical_encoding: $('[data-config="encoding"]').value,
      categorical_imputation: $('[data-config="categoricalImputation"]').value,
      target_encoding_smoothing: numberValue("smoothing", "类别平滑"),
    },
    cv: {
      folds: Math.round(numberValue("folds", "折数")),
      seed: Math.round(numberValue("seed", "随机种子")),
      stratify: true,
      metric: $('[data-config="metric"]').value,
    },
    regularization: {
      mode: state.mode,
      strengths: parseNumberList($('[data-config="strengths"]').value, "正则化强度"),
      l1_ratios: parseNumberList($('[data-config="l1Ratios"]').value, "L1 混合比例"),
    },
    solver: {
      name: $('[data-config="solver"]').value,
      tolerance: numberValue("tolerance", "容差"),
      max_iter: Math.round(numberValue("maxIter", "最大轮次")),
      objective_limit: numberValue("objectiveLimit", "目标上界"),
    },
  };
}

async function loadAll(initial = false) {
  const [datasetsPayload, sessionPayload, statusPayload] = await Promise.all([
    api("/api/datasets"),
    api("/api/session"),
    api("/api/status"),
  ]);
  state.datasets = datasetsPayload.datasets;
  state.session = sessionPayload.session;
  state.status = statusPayload.status;
  if (initial || !state.initialized) {
    updateDatasetSelect();
    populateForm(state.session.config);
    state.initialized = true;
  }
  render();
  schedulePoll(3000);
}

function currentStatusClass(status) {
  if (status === "complete") return "complete";
  if (status === "running" || status === "stopping") return "running";
  if (status === "stopped") return "stopped";
  if (status === "error") return "error";
  return "";
}

function render() {
  const status = state.status || {};
  const runStatus = status.status || "idle";
  const statusClass = currentStatusClass(runStatus);
  const global = $("#globalStatus");
  global.className = `status-pill ${statusClass}`;
  global.textContent = statusNames[runStatus] || runStatus;
  $("#sidebarStatusText").textContent = statusNames[runStatus] || runStatus;
  $("#sidebarStatusDot").className = `status-dot ${statusClass}`;
  $("#runId").textContent = status.active_run_id || "—";

  const running = runStatus === "running" || runStatus === "stopping";
  $("#startButton").disabled = running;
  $("#stopButton").disabled = !running;
  $("#resumeButton").disabled = !(runStatus === "stopped" || runStatus === "error");
  $("#startButton").innerHTML = running
    ? '<span class="button-dot"></span>训练进行中'
    : '<span class="button-dot"></span>开始训练';

  if (status.error) showError(status.error, false);
  else if (state.lastError && runStatus !== "error") dismissError();

  renderInvalidationBanner();
  renderStages();
  renderHero();
  renderOverview();
  renderFolds();
  renderFingerprints();
  renderRaw();
  if (state.tab === "checkpoints") loadCheckpoints();
}

function renderInvalidationBanner() {
  const status = state.status || {};
  const invalidation = status.pending_invalidation || status.invalidation || state.session?.pending_invalidation;
  const banner = $("#invalidationBanner");
  if (!invalidation || !invalidation.changed || !invalidation.earliest_stage) {
    banner.hidden = true;
    return;
  }
  const reusable = invalidation.reusable_stages || [];
  const affected = invalidation.affected_stages || [];
  banner.hidden = false;
  banner.innerHTML = `
    <strong>版本失效：从 ${escapeHtml(stageNames[invalidation.earliest_stage] || invalidation.earliest_stage)} 阶段开始重建</strong>
    <div class="inv-flow">
      ${reusable.map((stage) => `<span class="inv-pill reuse">复用 · ${escapeHtml(stageNames[stage] || stage)}</span>`).join("")}
      ${affected.map((stage) => `<span class="inv-pill">失效 · ${escapeHtml(stageNames[stage] || stage)}</span>`).join("")}
    </div>
    <div class="small-note" style="margin-top:7px">${(invalidation.reasons || []).map(escapeHtml).join(" · ")}</div>
  `;
}

function renderStages() {
  const status = state.status || {};
  const stages = status.stages || ["preprocess", "folds", "solve", "aggregate"].map((name) => ({ name, status: "pending" }));
  const progress = status.progress || {};
  $("#stageRail").innerHTML = stages
    .map((stage, index) => {
      const count = stage.name === "solve" && stage.total
        ? `<span class="stage-count">${stage.completed || 0}/${stage.total}</span>`
        : "";
      const stateText = stage.status === "complete" ? "完成" : stage.status === "running" ? "进行中" : "等待";
      return `
        <div class="stage-item ${escapeHtml(stage.status)}">
          <div class="stage-top"><span class="stage-index">${index + 1}</span>${count}</div>
          <div class="stage-name">${escapeHtml(stageNames[stage.name] || stage.name)}</div>
          <div class="stage-state">${stateText}</div>
        </div>`;
    })
    .join("");
  if (progress.solve_total) {
    $("#progressCopy").textContent = `solve ${progress.solve_completed}/${progress.solve_total} · CV ${progress.cv_completed}/${progress.cv_total}`;
  } else {
    $("#progressCopy").textContent = status.active_run_id ? `run ${shortHash(status.active_run_id)}` : "尚未运行";
  }
}

function selectedCandidate() {
  return state.result?.selected_hyperparameters || state.status?.selection || null;
}

function renderHero() {
  const result = state.result;
  const status = state.status || {};
  const summary = state.summary || status.summary;
  const selected = selectedCandidate();
  const metric = result?.cv?.metric || status.config?.cv?.metric || "log_loss";
  const best = result?.cv?.selection?.best || summary?.selected;
  if (best) {
    $("#heroMetric").textContent = fmtMetric(best.mean, metric);
    $("#heroMetricSub").textContent = `${selected?.folds?.length || result?.cv?.folds || status.config?.cv?.folds || "—"} 折 · ${metric}`;
  } else {
    $("#heroMetric").textContent = "—";
    $("#heroMetricSub").textContent = status.status === "running" ? "正在求解交叉验证" : "等待训练完成";
  }
  if (selected) {
    const strength = selected.strength ?? selected.selected_hyperparameters?.strength;
    const ratio = selected.l1_ratio ?? selected.selected_hyperparameters?.l1_ratio;
    $("#heroHyperparams").textContent = `${fmt(strength, 6)} / ${fmt(ratio, 3)}`;
  } else {
    $("#heroHyperparams").textContent = "—";
  }
  if (result) {
    $("#heroObjective").textContent = fmt(result.trajectory_summary.final_objective, 6);
    $("#heroObjectiveSub").textContent = `${result.trajectory_summary.iterations} 轮 · ${result.trajectory_summary.converged ? "已收敛" : "达到上限"}`;
    $("#heroDigest").textContent = shortHash(result.result_digest, 14, 8);
    $("#heroDigest").title = result.result_digest;
  } else if (summary) {
    $("#heroObjective").textContent = fmt(summary.final_trajectory.final_objective, 6);
    $("#heroObjectiveSub").textContent = `${summary.final_trajectory.iterations} 轮`;
    $("#heroDigest").textContent = shortHash(summary.result_digest, 14, 8);
  } else {
    $("#heroObjective").textContent = "—";
    $("#heroObjectiveSub").textContent = "训练目标，含正则项";
    $("#heroDigest").textContent = "—";
    $("#heroDigest").title = "";
  }
}

function renderOverview() {
  const result = state.result;
  const summary = state.summary || state.status?.summary;
  const metric = result?.cv?.metric || state.status?.config?.cv?.metric || "log_loss";
  const best = result?.cv?.selection?.best || summary?.selected;
  if (best) {
    $("#cvMean").textContent = `${fmtMetric(best.mean, metric)} ± ${fmt(best.std, 6)}`;
    $("#cvMeanFoot").textContent = `${best.metric} · ${result?.cv?.folds || summary?.cv_folds?.length || "—"} folds`;
  }
  if (result) {
    const nonzero = result.trajectory_summary.non_zero_coefficients;
    $("#coefCount").textContent = `${result.coefficients.length}`;
    $("#coefFoot").textContent = `${nonzero} 个非零系数`;
    $("#interceptValue").textContent = fmt(result.intercept, 7);
    $("#iterationCount").textContent = String(result.trajectory_summary.iterations);
    $("#convergenceText").textContent = result.trajectory_summary.converged ? "满足容差并收敛" : "达到最大轮次";
    $("#traceDigest").textContent = shortHash(result.trajectory_summary.trajectory_digest, 8, 6);
    $("#traceDigest").title = result.trajectory_summary.trajectory_digest;
    renderCoefficients(result);
    renderTrajectoryChart(state.trajectory?.trajectory || []);
  } else if (summary) {
    $("#coefCount").textContent = String(summary.coefficient_count);
    $("#coefFoot").textContent = shortHash(summary.coefficient_digest, 8, 6);
    $("#interceptValue").textContent = fmt(summary.intercept, 7);
    $("#iterationCount").textContent = String(summary.final_trajectory.iterations);
    $("#convergenceText").textContent = summary.final_trajectory.converged ? "满足容差并收敛" : "达到最大轮次";
  }
  $("#foldMetricLabel").textContent = metric;
  renderFoldChart();
}

function renderTrajectoryChart(rows) {
  const target = $("#trajectoryChart");
  if (!rows || rows.length < 2) {
    target.innerHTML = `<div class="empty-state">训练完成后显示逐轮目标函数。</div>`;
    return;
  }
  const width = 760;
  const height = 220;
  const margin = { left: 55, right: 16, top: 13, bottom: 30 };
  const innerW = width - margin.left - margin.right;
  const innerH = height - margin.top - margin.bottom;
  const values = rows.map((row) => Number(row.objective)).filter(Number.isFinite);
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const span = maxValue - minValue || Math.abs(maxValue) || 1;
  const low = Math.max(0, minValue - span * 0.08);
  const high = maxValue + span * 0.08;
  const x = (index) => margin.left + (index / Math.max(1, rows.length - 1)) * innerW;
  const y = (value) => margin.top + innerH - ((value - low) / (high - low || 1)) * innerH;
  const points = rows.map((row, index) => [x(index), y(Number(row.objective))]);
  const line = points.map((point, index) => `${index ? "L" : "M"}${point[0].toFixed(2)},${point[1].toFixed(2)}`).join(" ");
  const area = `${line} L${points[points.length - 1][0].toFixed(2)},${margin.top + innerH} L${points[0][0].toFixed(2)},${margin.top + innerH} Z`;
  const grid = Array.from({ length: 5 }, (_, index) => {
    const value = low + ((high - low) * index) / 4;
    const yy = y(value);
    return `<line class="chart-gridline" x1="${margin.left}" x2="${width - margin.right}" y1="${yy}" y2="${yy}"/>
      <text class="chart-label" x="${margin.left - 8}" y="${yy + 3}" text-anchor="end">${fmt(value, 4)}</text>`;
  }).join("");
  const xticks = [0, Math.floor((rows.length - 1) / 2), rows.length - 1]
    .filter((value, index, array) => array.indexOf(value) === index)
    .map((index) => `<text class="chart-label" x="${x(index)}" y="${height - 7}" text-anchor="middle">${rows[index].iteration}</text>`)
    .join("");
  target.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Training objective trajectory">
      <defs>
        <linearGradient id="trajectoryGradient" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="#32b7a3" stop-opacity=".20"/>
          <stop offset="100%" stop-color="#32b7a3" stop-opacity="0"/>
        </linearGradient>
      </defs>
      ${grid}
      <line class="chart-axis" x1="${margin.left}" x2="${width - margin.right}" y1="${margin.top + innerH}" y2="${margin.top + innerH}"/>
      <path class="chart-area" d="${area}"/>
      <path class="chart-line" d="${line}"/>
      <circle class="chart-point" cx="${points[points.length - 1][0]}" cy="${points[points.length - 1][1]}" r="3.5"/>
      ${xticks}
      <text class="chart-label" x="${width - margin.right}" y="${height - 7}" text-anchor="end">iteration</text>
    </svg>`;
}

function renderFoldChart() {
  const target = $("#foldChart");
  const status = state.status || {};
  const folds = (status.folds || []).filter((item) => item.role === "solve");
  if (!folds.length) {
    target.innerHTML = `<div class="empty-state">CV 求解后显示折间结果。</div>`;
    return;
  }
  const selected = selectedCandidate();
  let rows = folds;
  if (selected && selected.strength !== undefined) {
    const matched = folds.filter(
      (item) => Number(item.strength) === Number(selected.strength) && Number(item.l1_ratio) === Number(selected.l1_ratio)
    );
    if (matched.length) rows = matched;
  } else {
    const first = folds[0];
    rows = folds.filter(
      (item) => Number(item.strength) === Number(first.strength) && Number(item.l1_ratio) === Number(first.l1_ratio)
    );
  }
  rows = [...rows].sort((a, b) => a.fold - b.fold);
  const metric = state.result?.cv?.metric || status.config?.cv?.metric || "log_loss";
  const values = rows.map((item) => Number(item.validation?.[metric] ?? item.validation?.log_loss ?? 0));
  const maxValue = Math.max(...values, metric === "accuracy" ? 1 : 0.0001);
  const width = 660;
  const height = 220;
  const margin = { left: 48, right: 16, top: 20, bottom: 30 };
  const innerW = width - margin.left - margin.right;
  const innerH = height - margin.top - margin.bottom;
  const band = innerW / rows.length;
  const barW = Math.min(60, band * 0.55);
  const bars = rows.map((row, index) => {
    const value = values[index];
    const scaled = metric === "accuracy" ? value / Math.max(maxValue, 1e-9) : value / Math.max(maxValue, 1e-9);
    const barH = Math.max(2, scaled * innerH);
    const x = margin.left + index * band + (band - barW) / 2;
    const y = margin.top + innerH - barH;
    return `<rect class="fold-bar selected" x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${barW.toFixed(1)}" height="${barH.toFixed(1)}" rx="4"/>
      <text class="fold-bar-value" x="${x + barW / 2}" y="${Math.max(11, y - 6)}">${escapeHtml(fmtMetric(value, metric))}</text>
      <text class="fold-bar-label" x="${x + barW / 2}" y="${height - 8}">F${row.fold}</text>`;
  }).join("");
  const grid = [0, 0.5, 1].map((ratio) => {
    const yy = margin.top + innerH - ratio * innerH;
    const value = maxValue * ratio;
    return `<line class="chart-gridline" x1="${margin.left}" x2="${width - margin.right}" y1="${yy}" y2="${yy}"/>
      <text class="chart-label" x="${margin.left - 8}" y="${yy + 3}" text-anchor="end">${escapeHtml(fmtMetric(value, metric))}</text>`;
  }).join("");
  target.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Cross-validation fold metrics">
      <defs>
        <linearGradient id="foldSelectedGradient" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="#45c6b1"/><stop offset="100%" stop-color="#168a7b"/>
        </linearGradient>
        <linearGradient id="foldGradient" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="#9bd9cf"/><stop offset="100%" stop-color="#64b9ab"/>
        </linearGradient>
      </defs>
      ${grid}
      <line class="chart-axis" x1="${margin.left}" x2="${width - margin.right}" y1="${margin.top + innerH}" y2="${margin.top + innerH}"/>
      ${bars}
    </svg>`;
}

function renderCoefficients(result) {
  const target = $("#coefficientTable");
  if (!result?.feature_names?.length) {
    target.innerHTML = `<div class="empty-state">完成训练后显示系数。</div>`;
    return;
  }
  const rows = result.feature_names
    .map((name, index) => ({ name, value: Number(result.coefficients[index]) }))
    .sort((a, b) => Math.abs(b.value) - Math.abs(a.value));
  const maxAbs = Math.max(...rows.map((row) => Math.abs(row.value)), 1e-12);
  target.innerHTML = `
    <table>
      <thead><tr><th>特征</th><th class="num">系数</th><th class="num">相对幅度</th></tr></thead>
      <tbody>${rows.map((row) => {
        const percent = Math.min(100, (Math.abs(row.value) / maxAbs) * 100);
        return `<tr>
          <td><span class="coef-name">${escapeHtml(row.name)}</span></td>
          <td class="num">${escapeHtml(fmt(row.value, 9))}</td>
          <td><div class="coef-cell"><div class="coef-bar ${row.value < 0 ? "negative" : ""}"><i style="width:${percent.toFixed(1)}%"></i></div><span class="num">${percent.toFixed(0)}%</span></div></td>
        </tr>`;
      }).join("")}</tbody>
    </table>`;
}

function renderFolds() {
  const status = state.status || {};
  const folds = status.folds || [];
  $("#foldsBadge").textContent = `${folds.length} tasks`;
  if (!folds.length) {
    $("#foldsTable").innerHTML = `<div class="empty-state">${status.status === "running" ? "正在等待首个求解任务完成。" : "尚无折任务。"}</div>`;
    return;
  }
  const metric = status.config?.cv?.metric || "log_loss";
  $("#foldsTable").innerHTML = `
    <div class="table-wrap"><table>
      <thead><tr><th>任务</th><th>折</th><th>角色</th><th class="num">λ</th><th class="num">L1 ratio</th><th class="num">Log loss</th><th class="num">Accuracy</th><th class="num">轮次</th><th>状态</th></tr></thead>
      <tbody>${folds.map((item) => {
        const v = item.validation || {};
        return `<tr>
          <td><code>#${item.task_index}</code></td>
          <td>${item.role === "final" ? "全量" : `F${item.fold}`}</td>
          <td>${item.role === "final" ? "最终拟合" : "CV 求解"}</td>
          <td class="num">${escapeHtml(fmt(item.strength, 6))}</td>
          <td class="num">${escapeHtml(fmt(item.l1_ratio, 3))}</td>
          <td class="num">${v.log_loss !== undefined ? escapeHtml(fmt(v.log_loss, 7)) : "—"}</td>
          <td class="num">${v.accuracy !== undefined ? escapeHtml(fmtMetric(v.accuracy, "accuracy")) : "—"}</td>
          <td class="num">${item.trajectory_summary?.iterations ?? "—"}</td>
          <td><span class="badge-code ok">complete</span></td>
        </tr>`;
      }).join("")}</tbody>
    </table></div>
    <div class="small-note" style="margin-top:10px">当前选择指标：${escapeHtml(metric)}。CV 任务按折与候选超参数逐一持久化。</div>`;
}

function invalidationInfo() {
  return state.status?.pending_invalidation || state.status?.invalidation || state.session?.pending_invalidation;
}

function renderFingerprints() {
  const status = state.status || {};
  const fps = status.stage_fingerprints || {};
  $("#versionBadge").textContent = status.version_fingerprint ? shortHash(status.version_fingerprint, 10, 6) : "—";
  $("#versionBadge").title = status.version_fingerprint || "";
  $("#fingerprintGrid").innerHTML = ["preprocess", "folds", "solve", "aggregate"]
    .map((stage) => {
      const value = fps[stage];
      return `<div class="fp-card ${value ? "" : "muted"}">
        <div class="fp-stage"><span>${escapeHtml(stageNames[stage])}</span><span class="badge-code ${value ? "ok" : ""}">${value ? "sealed" : "pending"}</span></div>
        <code>${escapeHtml(value || "not published")}</code>
        <div class="fp-source">${stage === "preprocess" ? "dataset bytes + declaration + preprocessing" : stage === "folds" ? "preprocess fingerprint + fold policy" : stage === "solve" ? "fold partitions + regularization + solver" : "all solves + metric + selection policy"}</div>
      </div>`;
    })
    .join("");
  const invalidation = invalidationInfo();
  const target = $("#invalidationDetail");
  if (!invalidation) {
    target.innerHTML = `<div class="empty-state">修改配置后展示最早失效阶段与原因。</div>`;
    return;
  }
  const reusable = invalidation.reusable_stages || [];
  const affected = invalidation.affected_stages || [];
  target.innerHTML = `
    <div class="fingerprint-meta">
      <div><span>最早失效</span><code>${escapeHtml(invalidation.earliest_stage || "none")}</code></div>
      <div><span>前序运行</span><code>${escapeHtml(invalidation.previous_run_id || "—")}</code></div>
      <div><span>新版本指纹</span><code>${escapeHtml(shortHash(invalidation.new_version_fingerprint, 16, 8))}</code></div>
    </div>
    <div class="reuse-map">
      ${reusable.map((stage) => `<span class="reuse-node">复用 · ${escapeHtml(stage)}</span>`).join("")}
      ${affected.map((stage) => `<span class="reuse-node affected">重建 · ${escapeHtml(stage)}</span>`).join("")}
    </div>
    <ul class="invalidation-list">
      ${(invalidation.reasons || []).map((reason) => `<li>${escapeHtml(reason)}</li>`).join("") || "<li>输入与配置未变化。</li>"}
    </ul>
    ${invalidation.changed_config_paths?.length ? `<div class="small-note" style="margin-top:9px">变更字段：${invalidation.changed_config_paths.map((item) => `<code>${escapeHtml(item)}</code>`).join(" · ")}</div>` : ""}`;
}

async function loadCheckpoints() {
  try {
    const payload = await api("/api/checkpoints");
    state.checkpoints = payload;
    const badge = $("#checkpointBadge");
    badge.textContent = `last #${payload.last_sequence || 0} · next #${payload.next_sequence || 1}`;
    const records = payload.records || [];
    if (!records.length) {
      $("#checkpointTable").innerHTML = `<div class="empty-state">运行开始后显示检查点。</div>`;
      return;
    }
    const ignored = payload.ignored || [];
    $("#checkpointTable").innerHTML = `
      <div class="table-wrap"><table>
        <thead><tr><th>序号</th><th>阶段</th><th>折</th><th>任务位次</th><th>角色</th><th>输入指纹</th><th>复用</th></tr></thead>
        <tbody>${records.map((record, index) => {
          const previous = records[index - 1];
          const contiguous = !previous || record.sequence === previous.sequence + 1;
          return `<tr>
            <td><span class="badge-code ${contiguous ? "ok" : "err"}">${record.sequence}</span></td>
            <td>${escapeHtml(stageNames[record.stage] || record.stage)}</td>
            <td>${record.fold === null ? "—" : record.fold}</td>
            <td>${record.task_index ?? "—"}</td>
            <td><code>${escapeHtml(record.role)}</code></td>
            <td><code title="${escapeHtml(record.input_fingerprint)}">${escapeHtml(shortHash(record.input_fingerprint, 10, 6))}</code></td>
            <td>${record.reused_from ? '<span class="badge-code ok">cache</span>' : '<span class="badge-code">computed</span>'}</td>
          </tr>`;
        }).join("")}</tbody>
      </table></div>
      ${ignored.length ? `<div class="ignored-list"><strong>已忽略 ${ignored.length} 条损坏或跳号记录</strong><ul>${ignored.slice(0, 12).map((item) => `<li>${escapeHtml(item.file)} · ${escapeHtml(item.reason)}</li>`).join("")}</ul></div>` : ""}`;
  } catch (error) {
    $("#checkpointTable").innerHTML = `<div class="empty-state">${escapeHtml(error.message || "无法读取检查点")}</div>`;
  }
}

function renderRaw() {
  const value = {
    session: state.session,
    status: state.status,
    summary: state.summary,
    selected_hyperparameters: state.result?.selected_hyperparameters || null,
  };
  $("#rawJson").textContent = JSON.stringify(value, null, 2);
}

async function loadCompleteArtifacts() {
  const status = state.status || {};
  if (status.status !== "complete") return;
  const runId = status.active_run_id;
  if (state.artifactsForRun === runId) return;
  const [resultPayload, summaryPayload, tracePayload] = await Promise.all([
    api("/api/export/coefficients"),
    api("/api/summary"),
    api("/api/export/trajectory"),
  ]);
  state.result = resultPayload.result;
  state.summary = summaryPayload.summary;
  state.trajectory = tracePayload;
  state.artifactsForRun = runId;
  render();
}

async function poll() {
  if (state.polling) return;
  state.polling = true;
  try {
    const payload = await api("/api/status");
    state.status = payload.status;
    const sessionPayload = await api("/api/session");
    state.session = sessionPayload.session;
    if (state.status.status === "complete") await loadCompleteArtifacts();
    if (state.status.status !== "complete" && state.artifactsForRun !== state.status.active_run_id) {
      state.result = null;
      state.summary = null;
      state.trajectory = null;
    }
    render();
  } catch (error) {
    showError(error);
  } finally {
    state.polling = false;
    const running = ["running", "stopping"].includes(state.status?.status);
    schedulePoll(running ? 700 : 3500);
  }
}

function schedulePoll(delay) {
  if (state.timer) clearTimeout(state.timer);
  state.timer = setTimeout(poll, delay);
}

async function startTraining() {
  dismissError();
  try {
    const patch = collectConfigPatch();
    const stopAfter = Number($("#stopAfterTasks").value || 0);
    const body = { session_id: "default", config_patch: patch, background: true };
    if (Number.isFinite(stopAfter) && stopAfter >= 1) body.stop_after_tasks = Math.floor(stopAfter);
    state.artifactsForRun = null;
    state.result = null;
    state.summary = null;
    state.trajectory = null;
    const payload = await api("/api/train", { method: "POST", body: JSON.stringify(body) });
    toast(`训练已启动 · ${shortHash(payload.run_id)}`);
    state.status = {
      ...(state.status || {}),
      status: "running",
      active_run_id: payload.run_id,
      stages: null,
      folds: [],
      progress: {},
      error: null,
      invalidation: payload.invalidation || null,
    };
    render();
    schedulePoll(250);
  } catch (error) {
    showError(error);
    render();
  }
}

async function stopTraining() {
  try {
    const payload = await api("/api/stop", { method: "POST", body: JSON.stringify({ session_id: "default" }) });
    toast(payload.accepted ? "已请求优雅停止，将在当前求解轮次结束后退出。" : "当前运行已完成。");
    schedulePoll(250);
  } catch (error) {
    showError(error);
  }
}

async function resumeTraining() {
  dismissError();
  try {
    const body = { session_id: "default", background: true };
    await api("/api/resume", { method: "POST", body: JSON.stringify(body) });
    toast("已从最近一致检查点恢复。");
    schedulePoll(250);
  } catch (error) {
    showError(error);
  }
}

function downloadJson(filename, payload) {
  const blob = new Blob([JSON.stringify(payload, null, 2) + "\n"], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.append(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

async function exportCoefficients() {
  try {
    const payload = await api("/api/export/coefficients");
    downloadJson(`coefficients-${payload.run_id || "run"}.json`, payload);
  } catch (error) {
    showError(error);
  }
}

async function exportTrajectory() {
  try {
    const payload = await api("/api/export/trajectory");
    downloadJson(`trajectory-${payload.run_id || "run"}.json`, payload);
  } catch (error) {
    showError(error);
  }
}

async function runErrorDemo(kind) {
  try {
    const patch = collectConfigPatch();
    if (kind === "ratio") {
      patch.regularization.l1_ratios = [1.5];
      const body = { session_id: "default", config_patch: patch, background: true };
      await api("/api/train", { method: "POST", body: JSON.stringify(body) });
    } else if (kind === "folds") {
      patch.cv.folds = 1;
      await api("/api/train", { method: "POST", body: JSON.stringify({ session_id: "default", config_patch: patch, background: true }) });
    } else if (kind === "numeric") {
      patch.solver.objective_limit = 0.01;
      patch.regularization = { mode: "l2", strengths: [0.01], l1_ratios: [0.0] };
      patch.cv.folds = Math.max(2, Math.min(3, patch.cv.folds));
      const payload = await api("/api/train", { method: "POST", body: JSON.stringify({ session_id: "default", config_patch: patch, background: true }) });
      toast(`数值异常演示已启动 · ${shortHash(payload.run_id)}`);
      schedulePoll(250);
    }
  } catch (error) {
    showError(error);
  }
}

function bindEvents() {
  $("#datasetSelect").addEventListener("change", async () => {
    renderDatasetMeta();
    try {
      const patch = { dataset_id: $("#datasetSelect").value };
      const payload = await api("/api/session/config", {
        method: "POST",
        body: JSON.stringify({ session_id: "default", patch }),
      });
      state.session = payload.session;
      state.status = {
        ...(state.status || {}),
        status: "idle",
        active_run_id: null,
        pending_invalidation: payload.invalidation,
        stages: null,
        folds: [],
        progress: {},
        selection: null,
        result_digest: null,
        summary: null,
        error: null,
      };
      state.result = null;
      state.summary = null;
      state.trajectory = null;
      state.artifactsForRun = null;
      toast(`已切换数据集 · 最早失效阶段：${invalidationInfo()?.earliest_stage || "none"}`);
      render();
    } catch (error) {
      showError(error);
    }
  });

  $$("#regularizationMode button").forEach((button) => {
    button.addEventListener("click", () => setMode(button.dataset.mode));
  });

  $("#startButton").addEventListener("click", startTraining);
  $("#stopButton").addEventListener("click", stopTraining);
  $("#resumeButton").addEventListener("click", resumeTraining);
  $("#exportCoefficients").addEventListener("click", exportCoefficients);
  $("#exportTrajectory").addEventListener("click", exportTrajectory);
  $("#dismissError").addEventListener("click", dismissError);
  $("#refreshResult").addEventListener("click", async () => {
    state.artifactsForRun = null;
    try { await loadCompleteArtifacts(); toast("结果已刷新。"); } catch (error) { showError(error); }
  });
  $("#copyRaw").addEventListener("click", async () => {
    try { await navigator.clipboard.writeText($("#rawJson").textContent); toast("状态 JSON 已复制。"); }
    catch (_error) { toast("浏览器未授予剪贴板权限。", "error"); }
  });

  $$(".tab").forEach((tab) => {
    tab.addEventListener("click", () => {
      state.tab = tab.dataset.tab;
      $$(".tab").forEach((item) => item.classList.toggle("active", item === tab));
      $$(".tab-panel").forEach((panel) => panel.classList.toggle("active", panel.dataset.panel === state.tab));
      if (state.tab === "checkpoints") loadCheckpoints();
    });
  });

  const dialog = $("#importDialog");
  $("#openImport").addEventListener("click", () => dialog.showModal());
  $("#closeImport").addEventListener("click", () => dialog.close());
  $("#cancelImport").addEventListener("click", () => dialog.close());
  $("#importForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      const body = {
        csv_path: $("#importPath").value,
        dataset_id: $("#importId").value,
        target: $("#importTarget").value || null,
        override: $("#importOverride").checked,
      };
      const payload = await api("/api/import", { method: "POST", body: JSON.stringify(body) });
      dialog.close();
      const datasets = await api("/api/datasets");
      state.datasets = datasets.datasets;
      updateDatasetSelect();
      $("#datasetSelect").value = payload.dataset.dataset_id;
      renderDatasetMeta();
      toast(`已导入 ${payload.dataset.dataset_id} · ${payload.dataset.rows} 行`);
    } catch (error) {
      showError(error);
    }
  });

  document.addEventListener("click", (event) => {
    const button = event.target.closest("[data-error-demo]");
    if (button) runErrorDemo(button.dataset.errorDemo);
  });
}

bindEvents();
loadAll(true).catch(showError);
