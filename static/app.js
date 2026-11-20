(() => {
  "use strict";

  const STAGES = ["binning", "boosting", "calibration", "diagnostics"];
  const STAGE_LABELS = {
    binning: "分箱",
    boosting: "提升",
    calibration: "校准",
    diagnostics: "诊断",
  };
  const STAGE_DESCRIPTIONS = {
    binning: "确定性分位数边界与缺失值专用桶",
    boosting: "二阶梯度直方图回归树与原始分数",
    calibration: "训练集外折原始分数的单调概率映射",
    diagnostics: "可靠度表、Log loss、Brier 与定位误差界",
  };
  const CONFIG_FIELDS = {
    bins: "int",
    rounds: "int",
    learning_rate: "float",
    max_depth: "int",
    min_leaf: "int",
    l2: "float",
    calibrator: "string",
    calibration_folds: "int",
    calibration_seed: "int",
    diagnostic_bins: "int",
    quantize_decimals: "int",
    platt_max_iter: "int",
    platt_tol: "float",
  };

  let statusState = null;
  let configLoaded = false;
  let reliabilityKey = null;
  let pollTimer = null;
  let toastTimer = null;

  const $ = (id) => document.getElementById(id);

  class ApiError extends Error {
    constructor(code, message, details) {
      super(message);
      this.code = code;
      this.details = details || {};
    }
  }

  async function api(path, options = {}) {
    const init = { ...options, headers: { "Content-Type": "application/json", ...(options.headers || {}) } };
    const response = await fetch(path, init);
    let payload;
    try {
      payload = await response.json();
    } catch (_) {
      payload = { ok: false, error: { code: "E_HTTP", message: `HTTP ${response.status}` } };
    }
    if (!response.ok || payload.ok === false) {
      const error = payload.error || { code: "E_HTTP", message: `HTTP ${response.status}` };
      throw new ApiError(error.code || "E_HTTP", error.message || "request failed", error.details || {});
    }
    return payload;
  }

  function toast(message, kind = "") {
    const element = $("toast");
    element.textContent = message;
    element.className = `toast show ${kind}`.trim();
    window.clearTimeout(toastTimer);
    toastTimer = window.setTimeout(() => {
      element.className = "toast";
    }, kind === "error" ? 7500 : 4800);
  }

  function errorText(error) {
    if (error instanceof ApiError) {
      const detail = Object.keys(error.details || {}).length ? ` · ${JSON.stringify(error.details)}` : "";
      return `${error.code}: ${error.message}${detail}`;
    }
    return String(error && error.message ? error.message : error);
  }

  function esc(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function shortHash(value, length = 12) {
    if (!value) return "—";
    return String(value).slice(0, length);
  }

  function fmt(value, digits = 6) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
    const number = Number(value);
    if (number === 0) return "0";
    if (Math.abs(number) >= 1000 || Math.abs(number) < 0.0001) return number.toExponential(4);
    return number.toFixed(digits).replace(/0+$/, "").replace(/\.$/, "");
  }

  function fixedLabel(q, scale) {
    if (q === null || q === undefined) return "—";
    const value = Number(q);
    if (!Number.isFinite(value)) return "—";
    const decimals = Math.max(0, Math.round(Math.log10(scale)));
    return (value / scale).toFixed(Math.min(decimals, 8));
  }

  function applyConfig(config) {
    for (const [key, kind] of Object.entries(CONFIG_FIELDS)) {
      const input = $(`cfg-${key}`);
      if (!input || config[key] === undefined) continue;
      input.value = kind === "string" ? String(config[key]) : config[key];
    }
    configLoaded = true;
  }

  function collectConfig() {
    const config = {};
    for (const [key, kind] of Object.entries(CONFIG_FIELDS)) {
      const input = $(`cfg-${key}`);
      if (!input) continue;
      if (kind === "string") config[key] = input.value;
      else if (kind === "int") config[key] = Number.parseInt(input.value, 10);
      else config[key] = Number.parseFloat(input.value);
    }
    return config;
  }

  function setBusy(busy) {
    for (const id of ["import-btn", "save-config-btn", "train-btn", "resume-btn", "verify-btn", "batch-btn"]) {
      const element = $(id);
      if (element) element.disabled = busy;
    }
  }

  function overallPill(status) {
    const pill = $("overall-pill");
    const overall = status.overall || "empty";
    const running = status.service && status.service.training;
    pill.className = "pill";
    if (running || overall === "running" || status.last_run.status === "running") {
      pill.classList.add("pill-run");
      pill.textContent = "运行中";
    } else if (overall === "complete") {
      pill.classList.add("pill-ok");
      pill.textContent = "已完成";
    } else if (overall === "stopped" || status.stop_requested) {
      pill.classList.add("pill-warn");
      pill.textContent = "已停止";
    } else if (overall === "invalid" || overall === "partial") {
      pill.classList.add("pill-warn");
      pill.textContent = "待重建";
    } else if (status.last_run.status === "error") {
      pill.classList.add("pill-err");
      pill.textContent = "错误";
    } else {
      pill.classList.add("pill-idle");
      pill.textContent = overall;
    }
  }

  function renderDataset(status) {
    const meta = $("dataset-meta");
    if (!status.dataset) {
      meta.textContent = "尚未导入本地声明样本";
      return;
    }
    const dataset = status.dataset;
    meta.innerHTML = [
      `<strong>${esc(dataset.key)}</strong> · ${dataset.rows} × ${dataset.cols}`,
      `missing columns: ${esc((dataset.missing_columns || []).join(", "))}`,
      `positive rate: ${fmt(dataset.positive_rate, 6)}`,
      `data fp: ${esc(shortHash(dataset.data_fingerprint, 18))}`,
    ].join("<br>");
  }

  function stageExtra(stage, details) {
    if (!details || details.status !== "valid") {
      if (details && details.progress) {
        const progress = details.progress;
        return `round ${progress.rounds_done ?? 0}/${progress.rounds_total ?? "?"} · fold ${progress.fold_index ?? "—"}`;
      }
      return details && details.status === "invalid" ? "等待失效链重建" : "尚未发布检查点";
    }
    const accumulator = details.accumulator || {};
    if (stage === "binning") return `samples ${accumulator.samples ?? "—"} · matrix ${shortHash(accumulator.matrix_sha256, 10)}`;
    if (stage === "boosting") return `samples ${accumulator.samples ?? "—"} · raw ${shortHash(accumulator.raw_score_digest, 10)}`;
    if (stage === "calibration") return `folds ${accumulator.batches ?? "—"} · iterations ${accumulator.fit_iterations ?? "—"}`;
    if (stage === "diagnostics") return `samples ${accumulator.samples ?? "—"} · loss ${shortHash(accumulator.probability_digest, 10)}`;
    return "";
  }

  function renderStages(status) {
    const grid = $("stage-grid");
    grid.innerHTML = STAGES.map((stage, index) => {
      const details = status.stages[stage] || { status: "missing" };
      const cardStatus = details.status || "missing";
      return `
        <article class="stage-card ${esc(cardStatus)}">
          <div class="stage-top">
            <span class="stage-index">0${index + 1}</span>
            <span class="status-dot ${esc(cardStatus)}">${esc(cardStatus)}</span>
          </div>
          <h3>${esc(STAGE_LABELS[stage])}</h3>
          <p>${esc(STAGE_DESCRIPTIONS[stage])}</p>
          <div class="stage-meta">
            <div><span>seq</span><code>${details.seq ?? "—"}</code></div>
            <div><span>version</span><code title="${esc(details.version_fingerprint || "")}">${esc(shortHash(details.version_fingerprint, 14))}</code></div>
            <div><span>detail</span><code title="${esc(stageExtra(stage, details))}">${esc(stageExtra(stage, details))}</code></div>
          </div>
        </article>`;
    }).join("");
  }

  function renderFingerprints(status) {
    const body = $("fingerprint-rows");
    body.innerHTML = STAGES.map((stage) => {
      const details = status.stages[stage] || {};
      return `<tr>
        <td>${esc(stage)}</td>
        <td><span class="status-dot ${esc(details.status || "missing")}">${esc(details.status || "missing")}</span></td>
        <td><code title="${esc(details.input_fingerprint || "")}">${esc(shortHash(details.input_fingerprint, 16))}</code></td>
        <td><code title="${esc(details.version_fingerprint || "")}">${esc(shortHash(details.version_fingerprint, 18))}</code></td>
      </tr>`;
    }).join("");
  }

  function reasonText(reason) {
    if (!reason || typeof reason !== "object") return String(reason);
    const parts = [reason.code || "invalidation"];
    if (reason.field) parts.push(reason.field);
    if (reason.previous !== undefined) parts.push(`${JSON.stringify(reason.previous)} → ${JSON.stringify(reason.current)}`);
    if (reason.detail !== undefined) parts.push(String(reason.detail));
    if (reason.previous_input_fingerprint || reason.current_input_fingerprint) {
      parts.push(`${shortHash(reason.previous_input_fingerprint, 8)} → ${shortHash(reason.current_input_fingerprint, 8)}`);
    }
    return parts.join(" · ");
  }

  function renderInvalidations(status) {
    const container = $("invalidation-list");
    const blocks = [];
    for (const stage of STAGES) {
      const details = status.stages[stage];
      if (!details || details.status !== "invalid") continue;
      const reasons = details.reasons || [];
      blocks.push(`
        <div class="reason">
          <strong>${esc(STAGE_LABELS[stage])} / ${esc(stage)}</strong>
          ${reasons.length ? reasons.map((reason) => `<span>${esc(reasonText(reason))}</span>`).join("") : "<span>上游版本变化</span>"}
        </div>`);
    }
    container.innerHTML = blocks.length ? blocks.join("") : '<div class="empty">当前无失效阶段</div>';
  }

  function renderRunNote(status) {
    const last = status.last_run || {};
    const parts = [`run status: ${last.status || "unknown"}`, `stage seq: ${status.seq ?? 0}`];
    if (last.details && last.details.run_digest) parts.push(`digest: ${shortHash(last.details.run_digest, 20)}`);
    if (last.details && last.details.code) parts.push(`${last.details.code}: ${last.details.message || ""}`);
    if (last.details && last.details.stage) parts.push(`stopped at: ${last.details.stage}`);
    if (status.stop_requested) parts.push("stop request pending");
    if (status.service && status.service.training) parts.push("background training active");
    $("run-note").textContent = parts.join("\n");
    $("run-digest").textContent = last.details && last.details.run_digest ? last.details.run_digest : "—";
  }

  function renderStatus(status) {
    statusState = status;
    if (!configLoaded) applyConfig(status.config || {});
    overallPill(status);
    renderDataset(status);
    renderStages(status);
    renderFingerprints(status);
    renderInvalidations(status);
    renderRunNote(status);
    const training = Boolean(status.service && status.service.training);
    setBusy(training);
    $("stop-btn").disabled = !training && !status.stop_requested;
    $("resume-btn").disabled = training || (!status.stop_requested && status.overall !== "stopped");
    schedulePoll(training);
    maybeLoadReliability(status);
  }

  function schedulePoll(training) {
    window.clearTimeout(pollTimer);
    if (training) {
      pollTimer = window.setTimeout(() => refresh().catch((error) => toast(errorText(error), "error")), 850);
    }
  }

  function clearReliability() {
    reliabilityKey = null;
    for (const id of ["metric-log-loss", "metric-brier", "metric-ece", "metric-max-dev", "calibrator-summary", "calibrator-iterations", "precision-digits", "precision-bound"]) {
      $(id).textContent = "—";
    }
    $("reliability-chart").innerHTML = '<div class="empty">完成诊断阶段后显示</div>';
    $("reliability-rows").innerHTML = "";
  }

  async function maybeLoadReliability(status) {
    const diagnostics = status.stages.diagnostics;
    if (!diagnostics || diagnostics.status !== "valid") {
      clearReliability();
      return;
    }
    const key = diagnostics.version_fingerprint;
    if (key === reliabilityKey) return;
    reliabilityKey = key;
    try {
      const payload = await api("/api/reliability");
      renderReliability(payload.diagnostics);
    } catch (error) {
      reliabilityKey = null;
      clearReliability();
      if (error.code !== "E_INVALID_STAGE") toast(errorText(error), "error");
    }
  }

  function renderReliability(diagnostics) {
    const metrics = diagnostics.metrics || {};
    $("metric-log-loss").textContent = fmt(metrics.log_loss, 6);
    $("metric-brier").textContent = fmt(metrics.brier, 6);
    $("metric-ece").textContent = fmt(metrics.ece, 6);
    $("metric-max-dev").textContent = fmt(metrics.max_bin_deviation, 6);
    const calibrator = diagnostics.calibrator || {};
    const params = calibrator.parameters || {};
    if (calibrator.type === "platt") {
      $("calibrator-summary").textContent = `platt · a=${fmt(params.a, 5)} · b=${fmt(params.b, 5)}`;
    } else {
      $("calibrator-summary").textContent = `isotonic · ${params.knots ?? "—"} knots`;
    }
    $("calibrator-iterations").textContent = String(calibrator.iterations ?? "—");
    const precision = diagnostics.precision || {};
    $("precision-digits").textContent = String(precision.effective_decimals ?? "—");
    const bound = precision.log_loss_error_upper_bound;
    $("precision-bound").textContent = precision.log_loss_bound_unbounded ? "unbounded" : fmt(bound, 4);
    renderReliabilityChart(diagnostics.reliability || { rows: [] });
    renderReliabilityRows(diagnostics.reliability || { rows: [] });
  }

  function renderReliabilityRows(reliability) {
    const rows = reliability.rows || [];
    $("reliability-rows").innerHTML = rows.map((row) => {
      const range = `${fmt(row.lower, 3)} – ${fmt(row.upper, 3)}`;
      return `<tr>
        <td>${row.index}</td>
        <td>${esc(range)}</td>
        <td>${row.count}</td>
        <td>${row.count ? fmt(row.mean_probability, 6) : "—"}</td>
        <td>${row.count ? fmt(row.observed_rate, 6) : "—"}</td>
        <td>${row.count ? fmt(row.deviation, 6) : "—"}</td>
      </tr>`;
    }).join("");
  }

  function renderReliabilityChart(reliability) {
    const rows = (reliability.rows || []).filter((row) => row.count > 0 && row.mean_probability !== null);
    if (!rows.length) {
      $("reliability-chart").innerHTML = '<div class="empty">没有可绘制的可靠度分箱</div>';
      return;
    }
    const width = 760;
    const height = 300;
    const left = 48;
    const right = 22;
    const top = 18;
    const bottom = 42;
    const plotWidth = width - left - right;
    const plotHeight = height - top - bottom;
    const sx = (value) => left + Number(value) * plotWidth;
    const sy = (value) => top + (1 - Number(value)) * plotHeight;
    const total = rows.reduce((sum, row) => sum + row.count, 0) || 1;
    const grid = [];
    for (let index = 0; index <= 5; index += 1) {
      const value = index / 5;
      const x = sx(value);
      const y = sy(value);
      const label = value.toFixed(1);
      grid.push(`<line class="chart-grid" x1="${x}" y1="${top}" x2="${x}" y2="${top + plotHeight}"/>`);
      grid.push(`<line class="chart-grid" x1="${left}" y1="${y}" x2="${left + plotWidth}" y2="${y}"/>`);
      grid.push(`<text class="chart-axis" x="${x}" y="${height - 18}" text-anchor="middle">${label}</text>`);
      grid.push(`<text class="chart-axis" x="${left - 10}" y="${y + 3}" text-anchor="end">${label}</text>`);
    }
    const points = rows
      .slice()
      .sort((a, b) => a.mean_probability - b.mean_probability)
      .map((row) => `${sx(row.mean_probability).toFixed(2)},${sy(row.observed_rate).toFixed(2)}`)
      .join(" ");
    const circles = rows.map((row) => {
      const radius = 2.8 + 6.5 * Math.sqrt(row.count / total);
      return `<circle class="chart-point" cx="${sx(row.mean_probability).toFixed(2)}" cy="${sy(row.observed_rate).toFixed(2)}" r="${radius.toFixed(2)}"><title>bin ${row.index} · n=${row.count} · p=${fmt(row.mean_probability, 5)} · y=${fmt(row.observed_rate, 5)}</title></circle>`;
    }).join("");
    $("reliability-chart").innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="校准可靠性曲线">
        ${grid.join("")}
        <line class="chart-diag" x1="${left}" y1="${top + plotHeight}" x2="${left + plotWidth}" y2="${top}"/>
        <polyline class="chart-line" points="${points}"/>
        ${circles}
        <text class="chart-axis" x="${left + plotWidth / 2}" y="${height - 3}" text-anchor="middle">预测概率</text>
        <text class="chart-axis" x="13" y="${top + plotHeight / 2}" text-anchor="middle" transform="rotate(-90 13 ${top + plotHeight / 2})">真实频率</text>
        <text class="chart-label" x="${left + 8}" y="${top + 14}">ECE ${fmt(reliability.ece, 6)}</text>
      </svg>`;
  }

  async function refresh() {
    const status = await api("/api/status");
    renderStatus(status);
    return status;
  }

  async function loadDatasets() {
    try {
      const payload = await api("/api/datasets");
      const select = $("dataset-select");
      const keys = Object.keys(payload.datasets || {}).sort();
      if (keys.length) {
        select.innerHTML = keys.map((key) => {
          const entry = payload.datasets[key];
          const label = `${key} · ${entry.rows} × ${entry.cols}`;
          return `<option value="${esc(key)}">${esc(label)}</option>`;
        }).join("");
        if (payload.current && keys.includes(payload.current.key)) select.value = payload.current.key;
      }
    } catch (error) {
      toast(errorText(error), "error");
    }
  }

  function bindEvents() {
    $("refresh-btn").addEventListener("click", async () => {
      try {
        await Promise.all([refresh(), loadDatasets()]);
        toast("状态已刷新", "good");
      } catch (error) {
        toast(errorText(error), "error");
      }
    });

    $("import-btn").addEventListener("click", async () => {
      try {
        setBusy(true);
        const key = $("dataset-select").value;
        const result = await api("/api/import", { method: "POST", body: JSON.stringify({ dataset: key }) });
        toast(`已导入 ${key} · ${result.dataset.rows} × ${result.dataset.cols}`, "good");
        configLoaded = false;
        reliabilityKey = null;
        await refresh();
      } catch (error) {
        toast(errorText(error), "error");
      } finally {
        setBusy(false);
      }
    });

    $("save-config-btn").addEventListener("click", async () => {
      try {
        const result = await api("/api/config", { method: "PUT", body: JSON.stringify({ config: collectConfig() }) });
        const earliest = result.earliest_invalid_stage || "无";
        toast(`配置已保存 · 最早失效阶段：${earliest}`, "good");
        await refresh();
      } catch (error) {
        toast(errorText(error), "error");
      }
    });

    $("train-btn").addEventListener("click", async () => {
      try {
        const untilStage = $("until-stage").value;
        const result = await api("/api/train", {
          method: "POST",
          body: JSON.stringify({ until_stage: untilStage, wait: false }),
        });
        toast(`训练已提交 · 目标阶段 ${untilStage}`, "good");
        renderStatus({ ...result.status, service: { training: true } });
        await refresh();
      } catch (error) {
        toast(errorText(error), "error");
      }
    });

    $("stop-btn").addEventListener("click", async () => {
      try {
        const result = await api("/api/stop", { method: "POST", body: "{}" });
        toast("停止请求已登记；将在当前提升轮次完成后退出", "good");
        await refresh();
      } catch (error) {
        toast(errorText(error), "error");
      }
    });

    $("resume-btn").addEventListener("click", async () => {
      try {
        const untilStage = $("until-stage").value;
        await api("/api/resume", { method: "POST", body: JSON.stringify({ until_stage: untilStage, wait: false }) });
        toast("已从持久化进度恢复", "good");
        await refresh();
      } catch (error) {
        toast(errorText(error), "error");
      }
    });

    $("verify-btn").addEventListener("click", async () => {
      try {
        setBusy(true);
        toast("正在执行增量与全量重建比对……");
        const result = await api("/api/verify-incremental", { method: "POST", body: "{}" });
        toast(`比对通过 · ${shortHash(result.incremental_run_digest, 20)}`, "good");
        $("run-note").textContent = `incremental = full\n${result.incremental_run_digest}`;
      } catch (error) {
        toast(errorText(error), "error");
      } finally {
        setBusy(false);
      }
    });

    $("batch-btn").addEventListener("click", async () => {
      try {
        const batchSizes = $("batch-sizes").value.split(",").map((value) => Number.parseInt(value.trim(), 10)).filter((value) => Number.isFinite(value) && value > 0);
        if (!batchSizes.length) throw new ApiError("E_BATCH_STATE", "请输入至少一个正整数批大小", {});
        setBusy(true);
        const result = await api("/api/compare-batches", { method: "POST", body: JSON.stringify({ batch_sizes: batchSizes }) });
        renderBatchResult(result);
        toast(result.all_equal ? "所有批大小与批序结果逐字节一致" : "检测到分批结果差异", result.all_equal ? "good" : "error");
      } catch (error) {
        toast(errorText(error), "error");
      } finally {
        setBusy(false);
      }
    });
  }

  function renderBatchResult(result) {
    const chips = [
      `<span class="chip ${result.all_equal ? "good" : "bad"}">${result.all_equal ? "ALL EQUAL" : "MISMATCH"}</span>`,
      `<span class="chip">probability ${shortHash(result.probability_vector_digest, 16)}</span>`,
      `<span class="chip">result ${shortHash(result.reference_digest, 16)}</span>`,
    ].join("");
    const rows = (result.runs || []).map((run) => `
      <tr>
        <td>${run.batch_size}</td>
        <td>${esc(run.batch_order)}</td>
        <td>${run.batch_count}</td>
        <td>${fmt(run.log_loss, 6)}</td>
        <td>${fmt(run.brier, 6)}</td>
        <td><code>${esc(shortHash(run.result_digest, 16))}</code></td>
      </tr>`).join("");
    $("batch-result").innerHTML = `
      <div class="batch-summary">${chips}</div>
      <div class="table-wrap"><table>
        <thead><tr><th>批大小</th><th>顺序</th><th>批数</th><th>Log loss</th><th>Brier</th><th>结果摘要</th></tr></thead>
        <tbody>${rows}</tbody>
      </table></div>`;
  }

  async function init() {
    bindEvents();
    try {
      await Promise.all([loadDatasets(), refresh()]);
    } catch (error) {
      toast(errorText(error), "error");
    }
  }

  init();
})();
