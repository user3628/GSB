const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

const state = {
  config: localStorage.getItem("telemetry-config") || "v2",
  status: null,
  summary: null,
  conflicts: null,
  gaps: null,
  cleaning: null,
  checkpoints: null,
  verification: null,
  activeTab: "conflicts",
  visibleChannels: new Set(["temperature_c", "humidity_pct"]),
  busy: false,
};

const PALETTE = {
  temperature_c: getComputedStyle(document.documentElement).getPropertyValue("--teal").trim() || "#54d7bd",
  humidity_pct: getComputedStyle(document.documentElement).getPropertyValue("--blue").trim() || "#62a8ff",
  gap: "rgba(255,111,125,.14)",
  gapLine: "rgba(255,111,125,.42)",
  text: "#dbeaf0",
  muted: "#7f9aa8",
  faint: "#4f6b78",
  line: "rgba(121,178,197,.14)",
  amber: "#f0b45a",
  violet: "#b49cff",
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function short(value, length = 10) {
  if (!value) return "—";
  const text = String(value);
  return text.length > length * 2 + 1 ? `${text.slice(0, length)}…${text.slice(-6)}` : text;
}

function fmt(value, digits = 3) {
  if (value === null || value === undefined) return "—";
  if (typeof value === "number") return Number.isFinite(value) ? value.toFixed(digits) : String(value);
  return String(value);
}

function toast(title, message = "", kind = "") {
  const stack = $("#toast-stack");
  const node = document.createElement("div");
  node.className = `toast ${kind}`;
  node.innerHTML = `<strong>${escapeHtml(title)}</strong>${message ? `<div>${escapeHtml(message)}</div>` : ""}`;
  stack.appendChild(node);
  setTimeout(() => node.remove(), kind === "err" ? 7000 : 4500);
}

function setBusy(value, label = "正在执行") {
  state.busy = value;
  $("#busy-overlay").classList.toggle("hidden", !value);
  $("#busy-label").textContent = label;
  $$("button").forEach((button) => {
    if (!button.closest(".tab")) button.disabled = value;
  });
}

function setHealth(kind, text) {
  const dot = $("#health-dot");
  dot.className = `dot ${kind}`;
  $("#health-text").textContent = text;
}

async function api(url, options = {}) {
  const init = { ...options, headers: { "Content-Type": "application/json", ...(options.headers || {}) } };
  if (init.body && typeof init.body !== "string") init.body = JSON.stringify(init.body);
  const response = await fetch(url, init);
  let payload = null;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }
  if (!response.ok || (payload && payload.ok === false)) {
    const error = payload?.error || { code: `HTTP_${response.status}`, message: "请求失败", details: {} };
    const exc = new Error(error.message || "请求失败");
    exc.code = error.code;
    exc.details = error.details || {};
    throw exc;
  }
  return payload;
}

function showApiError(exc) {
  setHealth("err", "API error");
  const details = exc.details && Object.keys(exc.details).length ? JSON.stringify(exc.details) : "";
  toast(`${exc.code || "E_INTERNAL"} · ${exc.message || exc}`, details);
}

function configQuery() {
  return `?config=${encodeURIComponent(state.config)}`;
}

async function refresh({ quiet = false } = {}) {
  try {
    const health = await api("/api/health");
    $("#workspace-path").textContent = health.workspace;
    setHealth("ok", "local service · online");
  } catch (exc) {
    showApiError(exc);
    return;
  }
  try {
    const status = await api(`/api/status${configQuery()}`);
    state.status = status;
    if (!status.initialized) {
      state.summary = state.conflicts = state.gaps = state.cleaning = state.checkpoints = null;
      renderAll();
      return;
    }
    const [summary, conflicts, gaps, cleaning, checkpoints, verification] = await Promise.allSettled([
      api(`/api/summary${configQuery()}`),
      api(`/api/conflicts${configQuery()}`),
      api(`/api/gaps${configQuery()}`),
      api(`/api/cleaning${configQuery()}`),
      api("/api/checkpoints"),
      api("/api/checkpoints/verify"),
    ]);
    state.summary = summary.status === "fulfilled" ? summary.value : null;
    state.conflicts = conflicts.status === "fulfilled" ? conflicts.value : null;
    state.gaps = gaps.status === "fulfilled" ? gaps.value : null;
    state.cleaning = cleaning.status === "fulfilled" ? cleaning.value : null;
    state.checkpoints = checkpoints.status === "fulfilled" ? checkpoints.value : null;
    state.verification = verification.status === "fulfilled" ? verification.value : null;
    renderAll();
  } catch (exc) {
    if (!quiet) showApiError(exc);
    state.status = null;
    renderAll();
  }
}

async function withBusy(label, task) {
  if (state.busy) return;
  setBusy(true, label);
  try {
    await task();
    await refresh({ quiet: true });
  } catch (exc) {
    showApiError(exc);
  } finally {
    setBusy(false);
  }
}

function renderAll() {
  renderTopbar();
  renderMetrics();
  renderStages();
  renderChart();
  renderTabs();
}

function renderTopbar() {
  const status = state.status;
  const chip = $("#run-chip");
  if (!status || !status.initialized) {
    chip.textContent = "未初始化";
    chip.className = "status-chip";
    $("#hero-title").textContent = "本地确定性三源遥测";
    $("#hero-subtitle").textContent = "点击左侧“生成确定性演示并运行”开始 / generate deterministic demo to begin";
    $("#overall-seal").textContent = "—";
    $("#config-meta").textContent = "等待工作区初始化 / waiting for workspace";
    setHealth("ok", "local service · ready");
    return;
  }
  const run = status.run || {};
  const runStatus = run.status || "idle";
  chip.textContent = `${runStatus} · seq ${status.checkpoints?.head_seq ?? "—"}`;
  chip.className = `status-chip ${runStatus === "completed" ? "ok" : runStatus === "error" ? "err" : "warn"}`;
  const config = status.config;
  $("#hero-title").textContent = state.summary?.title || "多源遥测对齐快照";
  $("#hero-subtitle").textContent = `${status.workspace_id} · ${status.complete ? "四阶段已封存 / sealed" : "存在过期阶段 / stale stages"} · ${config.source_schema_version === 1 ? "V1 已显式升级" : "V2 原生配置"}`;
  $("#overall-seal").textContent = status.fingerprints.overall;
  $("#overall-seal").title = status.fingerprints.overall;
  $("#config-meta").innerHTML = `
    <div>${escapeHtml(config.name)} · schema v${config.source_schema_version} → v${config.effective_schema_version}</div>
    <div>${config.degraded ? '<span class="tag gap">DEGRADED / 已降级升级</span>' : '<span class="tag ok">NATIVE V2</span>'}</div>
    <div class="mono" style="margin-top:6px">${escapeHtml(short(config.semantic_fingerprint, 18))}</div>
  `;
  const hasProvenance = Object.keys(config?.provenance || {}).length > 0;
  $("#grid-badge").textContent = hasProvenance ? "compat V1→V2" : (state.summary?.config?.grid ? `${state.summary.config.grid.step_ms} ms grid` : "grid —");
}

function renderMetrics() {
  const counts = state.status?.counts || state.summary?.counts || null;
  const seq = state.status?.checkpoints?.sequence || [];
  const fused = counts?.point_status_counts?.fused ?? "—";
  const gaps = counts?.gap_channel_count ?? "—";
  const conflicts = counts?.conflict_count ?? "—";
  const grid = counts?.grid_points ?? "—";
  $("#metric-grid").innerHTML = [
    ["GRID POINTS", grid, "网格点 / ms aligned"],
    ["FUSED", fused, "多源融合点"],
    ["CONFLICTS", conflicts, "冲突裁决事件"],
    ["GAPS", gaps, "缺口通道事件"],
    ["CHECKPOINTS", seq.length || "—", seq.length ? `seq ${seq[0]}…${seq[seq.length - 1]}` : "连续序号"],
  ].map(([label, value, sub]) => `
    <div class="metric"><span>${label}</span><strong>${escapeHtml(value)}</strong><small>${escapeHtml(sub)}</small></div>
  `).join("");
}

function renderStages() {
  const labels = {
    normalize: ["规范化", "Normalize · grid"],
    resample: ["重采样", "Resample · interpolate"],
    fusion: ["多源融合", "Fusion · arbitrate"],
    summary: ["摘要导出", "Summary · seal"],
  };
  const stages = state.status?.stages || [];
  const strip = $("#stage-strip");
  if (!stages.length) {
    strip.innerHTML = Object.entries(labels).map(([name, [zh, en]], index) => `
      <article class="stage-card pending"><div class="stage-index">0${index + 1}</div><div><h3>${zh}</h3><p>${en}</p></div><span class="stage-state">pending</span><code>—</code></article>
    `).join("");
    return;
  }
  strip.innerHTML = stages.map((stage, index) => {
    const [zh, en] = labels[stage.stage];
    return `
      <article class="stage-card ${escapeHtml(stage.state)}" title="${escapeHtml(stage.current_fingerprint)}">
        <div class="stage-index">0${index + 1}</div>
        <div><h3>${zh}</h3><p>${en}</p></div>
        <span class="stage-state">${escapeHtml(stage.state)}</span>
        <code>${escapeHtml(short(stage.current_fingerprint, 12))} · seq ${stage.checkpoint_seq ?? "—"}</code>
      </article>
    `;
  }).join("");
}

function renderTabs() {
  const conflictCount = state.conflicts?.count ?? 0;
  const gapCount = state.gaps?.point_gaps?.length ?? 0;
  $("#tab-conflicts-count").textContent = conflictCount;
  $("#tab-gaps-count").textContent = gapCount;
  renderConflicts();
  renderGaps();
  renderCleaning();
  renderVersions();
  renderCheckpoints();
}

function renderConflicts() {
  const page = $("#tab-conflicts");
  const conflicts = state.conflicts?.conflicts || [];
  if (!conflicts.length) {
    page.innerHTML = `<div class="empty-state">${state.status?.initialized ? "当前没有多源冲突；或融合结果尚未生成。" : "工作区尚未初始化。"}</div>`;
    return;
  }
  page.innerHTML = `
    <table class="data-table">
      <thead><tr><th>grid / ms</th><th>channel</th><th>winner</th><th>participants · contribution</th><th>fused value</th><th>note</th></tr></thead>
      <tbody>${conflicts.map((item) => `
        <tr>
          <td class="mono">#${item.index} · ${item.timestamp_ms}</td>
          <td><span class="tag">${escapeHtml(item.channel)}</span><div>${escapeHtml(item.channel_label)}</div></td>
          <td class="mono">${escapeHtml(item.winner)}</td>
          <td><div class="contrib-list">${item.participants.map((p) => `
            <div class="contrib-row"><span>${escapeHtml(p.source_id)} · p${p.priority} · ${escapeHtml(p.method)}</span><span>${fmt(p.value)} / ${(p.contribution * 100).toFixed(1)}%</span></div>
          `).join("")}</div></td>
          <td class="mono">${fmt(item.fused_value)}</td>
          <td>${item.priority_tie ? '<span class="tag priority">priority tie</span>' : '<span class="tag">priority ordered</span>'}${item.weight_fallback ? `<div class="tag gap">${escapeHtml(item.weight_fallback)}</div>` : ""}</td>
        </tr>
      `).join("")}</tbody>
    </table>`;
}

function renderGaps() {
  const page = $("#tab-gaps");
  const gaps = state.gaps?.gaps || [];
  if (!gaps.length) {
    page.innerHTML = `<div class="empty-state">${state.status?.initialized ? "当前对齐网格没有缺口；或结果尚未生成。" : "工作区尚未初始化。"}</div>`;
    return;
  }
  page.innerHTML = `
    <table class="data-table">
      <thead><tr><th>grid / ms</th><th>kind</th><th>channel</th><th>source diagnostics</th></tr></thead>
      <tbody>${gaps.map((gap) => `
        <tr>
          <td class="mono">#${gap.index} · ${gap.timestamp_ms}</td>
          <td><span class="tag ${gap.kind === "point_gap" ? "gap" : ""}">${escapeHtml(gap.kind)}</span></td>
          <td><div>${escapeHtml(gap.channel)}</div><small>${escapeHtml(gap.channel_label || "")}</small></td>
          <td><div class="contrib-list">${Object.entries(gap.sources).map(([source, detail]) => `
            <div class="contrib-row"><span>${escapeHtml(source)}</span><span>${escapeHtml(detail.reason || "missing")}</span></div>
          `).join("")}</div></td>
        </tr>
      `).join("")}</tbody>
    </table>`;
}

function renderCleaning() {
  const page = $("#tab-cleaning");
  const reports = state.cleaning?.reports || state.summary?.cleaning_reports || null;
  if (!reports) {
    page.innerHTML = `<div class="empty-state">清洗报告尚不可用 / cleaning report is not available.</div>`;
    return;
  }
  page.innerHTML = `
    <div class="source-stats" style="margin:0 0 14px">
      ${Object.entries(reports).map(([source, report]) => {
        const ratio = report.input_record_count ? Math.min(100, (report.accepted_record_count / report.input_record_count) * 100) : 0;
        return `<div class="source-stat"><span class="mono">${escapeHtml(source)}</span><div><div class="bar"><i style="width:${ratio}%"></i></div><small>${report.accepted_record_count}/${report.input_record_count} accepted · out-of-range ${report.out_of_range_count} · quality reject ${report.quality_rejected_count}</small></div></div>`;
      }).join("")}
    </div>
    <table class="data-table">
      <thead><tr><th>source</th><th>input</th><th>accepted</th><th>quality rejected</th><th>out of range</th><th>non numeric</th><th>duplicates / non-monotonic</th></tr></thead>
      <tbody>${Object.entries(reports).map(([source, report]) => `
        <tr><td class="mono">${escapeHtml(source)}</td><td>${report.input_record_count}</td><td>${report.accepted_record_count}</td><td>${report.quality_rejected_count}</td><td>${report.out_of_range_count}</td><td>${report.non_numeric_count}</td><td>${report.timestamp_duplicates.length} / ${report.non_monotonic.length}</td></tr>
      `).join("")}</tbody>
    </table>
    <div class="subpanel" style="margin-top:14px"><h3>Out-of-range detail · 超界值明细</h3>
      ${Object.values(reports).flatMap((r) => r.out_of_range).length ? Object.values(reports).flatMap((r) => r.out_of_range).map((r) => `<div class="fingerprint-line"><span>${escapeHtml(r.sensor_id)}</span><code>ts=${r.timestamp_ms} ${escapeHtml(r.channel)}=${r.value} ∉ [${r.min}, ${r.max}]</code></div>`).join("") : '<div class="empty-state" style="min-height:80px">无越界值 / no out-of-range values</div>'}
    </div>`;
}

function renderVersions() {
  const page = $("#tab-versions");
  const status = state.status;
  if (!status?.initialized) {
    page.innerHTML = `<div class="empty-state">工作区尚未初始化。</div>`;
    return;
  }
  const invalidations = status.invalidations || [];
  const latest = invalidations[invalidations.length - 1];
  const sourceRows = Object.entries(status.sources).map(([source, info]) => `
    <div class="fingerprint-line"><span>${escapeHtml(source)}</span><code>raw ${escapeHtml(short(info.digest, 16))}<br>norm ${escapeHtml(short(info.normalize_fingerprint, 16))}<br>resample ${escapeHtml(short(info.resample_fingerprint, 16))}</code></div>
  `).join("");
  const stageRows = Object.entries(status.fingerprints).map(([stage, fingerprint]) => `
    <div class="fingerprint-line"><span>${escapeHtml(stage)}</span><code>${escapeHtml(fingerprint)}</code></div>
  `).join("");
  page.innerHTML = `
    <div class="split-grid">
      <div class="subpanel"><h3>Stage fingerprints <span class="tag ok">current</span></h3>${stageRows}<h3 style="margin-top:15px">Source fingerprints</h3>${sourceRows}</div>
      <div class="subpanel"><h3>Latest invalidation <span class="tag ${latest?.earliest_stage ? "gap" : ""}">${escapeHtml(latest?.earliest_stage || "none")}</span></h3>
        ${latest?.reasons?.length ? `<div class="reason-list">${latest.reasons.map((reason) => `
          <div class="reason"><strong>${escapeHtml(reason.code)}</strong>${reason.source_id ? ` · ${escapeHtml(reason.source_id)}` : ""}${reason.fields ? ` · ${reason.fields.map(escapeHtml).join(", ")}` : ""}<br><code>${escapeHtml(reason.message || reason.stage || "stage-level invalidation")}</code></div>
        `).join("")}</div>` : `<div class="empty-state" style="min-height:120px">无失效原因 / no invalidation</div>`}
        <h3 style="margin-top:16px">Config provenance</h3>
        <div class="kv-grid">
          <div class="kv"><span>schema</span><strong>v${status.config.source_schema_version} → v${status.config.effective_schema_version}</strong></div>
          <div class="kv"><span>degraded</span><strong>${status.config.degraded ? "true · explicit upgrade" : "false · native"}</strong></div>
          <div class="kv"><span>semantic fp</span><strong class="mono">${escapeHtml(short(status.config.semantic_fingerprint, 18))}</strong></div>
          <div class="kv"><span>config fp</span><strong class="mono">${escapeHtml(short(status.config.config_fingerprint, 18))}</strong></div>
        </div>
      </div>
    </div>`;
}

function renderCheckpoints() {
  const page = $("#tab-checkpoints");
  const data = state.checkpoints?.checkpoints;
  if (!data) {
    page.innerHTML = `<div class="empty-state">检查点账本尚不可用 / checkpoint ledger is not available.</div>`;
    return;
  }
  const verified = state.verification?.verification?.valid;
  page.innerHTML = `
    <div class="kv-grid" style="margin-bottom:13px">
      <div class="kv"><span>head seq / stage</span><strong>${data.head_seq ?? "—"} · ${escapeHtml(data.head_stage || "—")}</strong></div>
      <div class="kv"><span>records / verify</span><strong>${data.record_count} · ${verified ? "hash-chain ✓" : "not verified"}</strong></div>
    </div>
    <div class="checkpoint-track">
      ${(data.sequence || []).map((seq, index) => `${index ? '<span class="checkpoint-arrow">→</span>' : ""}<span class="checkpoint-node ${seq === data.head_seq ? "head" : ""}">${seq}</span>`).join("") || '<span class="empty-state">no checkpoints</span>'}
    </div>
    <table class="data-table">
      <thead><tr><th>seq</th><th>stage</th><th>config</th><th>input digest</th><th>artifact digest</th><th>stage fingerprint</th></tr></thead>
      <tbody>${(data.records || []).map((record) => `
        <tr><td class="mono">${record.seq}</td><td><span class="tag ok">${escapeHtml(record.stage)}</span></td><td>v${record.config_version}</td><td class="mono">${escapeHtml(short(record.input_digest, 14))}</td><td class="mono">${escapeHtml(short(record.artifact_digest, 14))}</td><td class="mono">${escapeHtml(short(record.fingerprint, 14))}</td></tr>
      `).join("")}</tbody>
    </table>`;
}

function chartPointsFor(channel) {
  if (!state.summary?.alignment) return [];
  return state.summary.alignment
    .map((point) => ({ index: point.index, timestamp_ms: point.timestamp_ms, status: point.status, ...point.channels[channel] }));
}

function renderChart() {
  const canvas = $("#signal-chart");
  const shell = $(".chart-shell");
  const empty = $("#chart-empty");
  const alignment = state.summary?.alignment;
  if (!alignment || !alignment.length) {
    empty.classList.remove("hidden");
    const ctx = canvas.getContext("2d");
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    $("#chart-range").textContent = "—";
    $("#points-digest").textContent = "points —";
    return;
  }
  empty.classList.add("hidden");
  const rect = shell.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  const width = Math.max(320, rect.width);
  const height = 330;
  canvas.width = Math.floor(width * dpr);
  canvas.height = Math.floor(height * dpr);
  canvas.style.height = `${height}px`;
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, width, height);

  const margin = { left: 58, right: 54, top: 26, bottom: 34 };
  const plotW = width - margin.left - margin.right;
  const plotH = height - margin.top - margin.bottom;
  const times = alignment.map((point) => point.timestamp_ms);
  const minTime = Math.min(...times);
  const maxTime = Math.max(...times);
  const x = (time) => margin.left + ((time - minTime) / Math.max(1, maxTime - minTime)) * plotW;

  const channels = ["temperature_c", "humidity_pct"].filter((channel) => state.visibleChannels.has(channel));
  const temperature = chartPointsFor("temperature_c");
  const humidity = chartPointsFor("humidity_pct");
  const tempVals = temperature.filter((p) => p.value !== null && p.value !== undefined).map((p) => p.value);
  const humVals = humidity.filter((p) => p.value !== null && p.value !== undefined).map((p) => p.value);

  const yT = (value) => {
    if (!tempVals.length) return margin.top + plotH / 2;
    const min = Math.min(...tempVals), max = Math.max(...tempVals);
    return margin.top + plotH - ((value - min) / Math.max(1e-9, max - min)) * plotH;
  };
  const yH = (value) => {
    if (!humVals.length) return margin.top + plotH / 2;
    const min = Math.min(...humVals), max = Math.max(...humVals);
    return margin.top + plotH - ((value - min) / Math.max(1e-9, max - min)) * plotH;
  };

  ctx.strokeStyle = PALETTE.line;
  ctx.lineWidth = 1;
  ctx.font = "9px SFMono-Regular, Consolas, monospace";
  ctx.fillStyle = PALETTE.faint;
  for (let i = 0; i <= 5; i++) {
    const yy = margin.top + (plotH * i) / 5;
    ctx.beginPath(); ctx.moveTo(margin.left, yy); ctx.lineTo(width - margin.right, yy); ctx.stroke();
  }
  for (let i = 0; i <= 6; i++) {
    const xx = margin.left + (plotW * i) / 6;
    ctx.beginPath(); ctx.moveTo(xx, margin.top); ctx.lineTo(xx, margin.top + plotH); ctx.stroke();
    const t = Math.round(minTime + ((maxTime - minTime) * i) / 6);
    ctx.fillText(`${t}`, xx - 10, height - 12);
  }

  // Gap bands are explicit, never silently skipped.
  const gaps = state.summary?.gaps || [];
  const gapIndices = [...new Set(gaps.map((gap) => gap.index))];
  if (gapIndices.length) {
    const step = minTime === maxTime ? plotW : plotW / Math.max(1, alignment.length - 1);
    for (const index of gapIndices) {
      const point = alignment[index];
      if (!point) continue;
      const gx = x(point.timestamp_ms) - step / 2;
      ctx.fillStyle = PALETTE.gap;
      ctx.fillRect(gx, margin.top, Math.max(2, step), plotH);
      ctx.strokeStyle = PALETTE.gapLine;
      ctx.beginPath(); ctx.moveTo(gx, margin.top); ctx.lineTo(gx, margin.top + plotH); ctx.stroke();
    }
  }

  function drawSeries(points, yMap, color, label) {
    if (!state.visibleChannels.has(label) || !points.some((point) => point.value !== null && point.value !== undefined)) return;
    ctx.lineWidth = 2;
    ctx.strokeStyle = color;
    ctx.shadowColor = color;
    ctx.shadowBlur = 9;
    let segment = [];
    const strokeSegment = () => {
      if (segment.length < 2) { segment = []; return; }
      ctx.beginPath();
      segment.forEach((point, index) => {
        const xx = x(point.timestamp_ms);
        const yy = yMap(point.value);
        if (index === 0) ctx.moveTo(xx, yy); else ctx.lineTo(xx, yy);
      });
      ctx.stroke();
      segment = [];
    };
    for (const point of points) {
      if (point.value === null || point.value === undefined) {
        strokeSegment();
        continue;
      }
      if (segment.length && point.index !== segment[segment.length - 1].index + 1) strokeSegment();
      segment.push(point);
    }
    strokeSegment();
    ctx.shadowBlur = 0;
    for (const point of points) {
      if (point.value === null || point.value === undefined) continue;
      const xx = x(point.timestamp_ms);
      const yy = yMap(point.value);
      if (point.resolution === "extrapolated" || point.resolution === "single_source") {
        ctx.fillStyle = color;
        ctx.beginPath(); ctx.arc(xx, yy, 2.4, 0, Math.PI * 2); ctx.fill();
      }
    }
  }

  drawSeries(temperature, yT, PALETTE.temperature_c, "temperature_c");
  drawSeries(humidity, yH, PALETTE.humidity_pct, "humidity_pct");

  if (tempVals.length) {
    const min = Math.min(...tempVals), max = Math.max(...tempVals);
    ctx.fillStyle = PALETTE.temperature_c;
    ctx.fillText(`${max.toFixed(1)}`, 9, margin.top + 4);
    ctx.fillText(`${min.toFixed(1)}`, 9, margin.top + plotH);
    ctx.fillText("°C", 9, margin.top - 10);
  }
  if (humVals.length) {
    const min = Math.min(...humVals), max = Math.max(...humVals);
    ctx.fillStyle = PALETTE.humidity_pct;
    ctx.textAlign = "right";
    ctx.fillText(`${max.toFixed(1)}`, width - 9, margin.top + 4);
    ctx.fillText(`${min.toFixed(1)}`, width - 9, margin.top + plotH);
    ctx.fillText("%RH", width - 9, margin.top - 10);
    ctx.textAlign = "left";
  }
  $("#chart-range").textContent = `${minTime}–${maxTime} ms · ${alignment.length} grid points`;
  $("#points-digest").textContent = state.summary?.points_digest ? `points ${short(state.summary.points_digest, 13)}` : "points —";
  canvas.dataset.chartMeta = JSON.stringify({ margin, minTime, maxTime, plotW, plotH });
}

async function handleInit() {
  await withBusy("生成确定性样本并执行完整对齐", async () => {
    await api("/api/demo/init", { method: "POST", body: { reset: true } });
    const result = await api("/api/run", { method: "POST", body: { config: state.config } });
    toast("完整运行完成", `${result.executed_stages.join(" → ")} · seq ${result.checkpoint_sequence.join(",")}`, "ok");
  });
}

async function handleRun() {
  await withBusy("执行四阶段对齐", async () => {
    const result = await api("/api/run", { method: "POST", body: { config: state.config } });
    toast(result.status === "noop" ? "全部阶段已是最新" : "对齐完成", `stage ${result.executed_stages.join(" → ") || "noop"} · seq ${result.checkpoint_sequence.join(",")}`, "ok");
  });
}

async function handleResume() {
  await withBusy("恢复最近一次运行", async () => {
    const result = await api("/api/resume", { method: "POST", body: { config: state.config } });
    toast("恢复执行完成", `executed ${result.executed_stages.join(" → ")}`, "ok");
  });
}

async function handleStep() {
  const until = $("#stage-select").value;
  await withBusy(`运行至 ${until}`, async () => {
    const result = await api("/api/run", { method: "POST", body: { config: state.config, until_stage: until } });
    toast("阶段边界已到达", `${result.status} · ${result.executed_stages.join(" → ")}`, "ok");
  });
}

async function handleStop() {
  const stage = $("#stage-select").value;
  try {
    const result = await api("/api/stop", { method: "POST", body: { after_stage: stage } });
    toast("停止请求已排队", `将在 ${stage} 阶段完成后退出；随后点击“恢复运行”。`, "ok");
    await refresh({ quiet: true });
  } catch (exc) { showApiError(exc); }
}

async function handleIncrement() {
  await withBusy("修改 Source A 并执行增量重建", async () => {
    const result = await api("/api/increment", { method: "POST", body: { sensor_id: "sensor_a", config: state.config } });
    toast("增量重建完成", `earliest=${result.alignment.invalidations.earliest_stage} · reused ${result.alignment.reused.length}`, "ok");
  });
}

async function readFile(input) {
  const file = input.files?.[0];
  if (!file) return null;
  return await file.text();
}

async function handleImport() {
  const inputs = { "sensor_a": $("#file-a"), "sensor_b": $("#file-b"), "sensor_c": $("#file-c") };
  const configInputs = { v2: $("#file-v2"), v1: $("#file-v1") };
  if (Object.values(inputs).some((input) => !input.files?.length) || Object.values(configInputs).some((input) => !input.files?.length)) {
    toast("需要五份本地文件", "导入三源 JSONL 与 V1/V2 YAML。", "err");
    return;
  }
  await withBusy("导入本地分片与配置", async () => {
    const sources = {};
    for (const [source, input] of Object.entries(inputs)) sources[source] = { content: await readFile(input) };
    const configs = {};
    for (const [name, input] of Object.entries(configInputs)) configs[name] = { content: await readFile(input) };
    const result = await api("/api/import", { method: "POST", body: { sources, configs, active_config: state.config } });
    toast("导入成功", `V1 ${result.configs.v1.source_schema_version} → V2 · 运行以重建`, "ok");
  });
}

async function handleUpgrade() {
  await withBusy("执行 V1→V2 显式升级与语义等价校验", async () => {
    const result = await api("/api/upgrade-v1", { method: "POST", body: { config: "v1", output_path: "out/upgraded_config_v2.yaml" } });
    toast("V1 升级验证通过", `equivalent=${result.equivalent} · ${result.path}`, "ok");
  });
}

async function handleExport() {
  await withBusy("导出对齐摘要", async () => {
    const result = await api("/api/export", { method: "POST", body: { config: state.config, output_path: "out/alignment_summary.json" } });
    const blob = new Blob([JSON.stringify(result.summary, null, 2) + "\n"], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "alignment_summary.json";
    link.click();
    URL.revokeObjectURL(url);
    toast("摘要已导出", `${result.path} · sha256 ${short(result.digest, 14)}`, "ok");
  });
}

function bindEvents() {
  $("#btn-init").addEventListener("click", handleInit);
  $("#btn-run").addEventListener("click", handleRun);
  $("#btn-resume").addEventListener("click", handleResume);
  $("#btn-step").addEventListener("click", handleStep);
  $("#btn-stop").addEventListener("click", handleStop);
  $("#btn-increment").addEventListener("click", handleIncrement);
  $("#btn-import").addEventListener("click", handleImport);
  $("#btn-upgrade").addEventListener("click", handleUpgrade);
  $("#btn-export").addEventListener("click", handleExport);
  $("#config-select").value = state.config;
  $("#config-select").addEventListener("change", async (event) => {
    state.config = event.target.value;
    localStorage.setItem("telemetry-config", state.config);
    await refresh({ quiet: true });
  });
  $$(".tab").forEach((tab) => tab.addEventListener("click", () => {
    state.activeTab = tab.dataset.tab;
    $$(".tab").forEach((item) => item.classList.toggle("active", item === tab));
    $$(".tab-page").forEach((page) => page.classList.toggle("active", page.id === `tab-${state.activeTab}`));
  }));
  $$(".legend-toggle").forEach((button) => button.addEventListener("click", () => {
    const channel = button.dataset.channel;
    if (state.visibleChannels.has(channel)) state.visibleChannels.delete(channel);
    else state.visibleChannels.add(channel);
    button.classList.toggle("active", state.visibleChannels.has(channel));
    renderChart();
  }));
  window.addEventListener("resize", () => renderChart());
}

bindEvents();
refresh();
setInterval(() => { if (!state.busy) refresh({ quiet: true }); }, 9000);
