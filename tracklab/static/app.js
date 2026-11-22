"use strict";

const UI = {
  sequences: [],
  sequenceId: null,
  status: null,
  frame: 0,
  payload: null,
  result: null,
  busy: false,
  toastTimer: null,
};

const $ = (id) => document.getElementById(id);
const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const shortHash = (value, n = 16) => value ? `${String(value).slice(0, n)}…` : "—";
const pct = (v) => v === null || v === undefined || Number.isNaN(Number(v)) ? "—" : `${(Number(v) * 100).toFixed(2)}%`;
const num = (v, digits = 4) => v === null || v === undefined || Number.isNaN(Number(v)) ? "—" : Number(v).toFixed(digits);

function showToast(message, isError = false) {
  const el = $("toast");
  clearTimeout(UI.toastTimer);
  el.textContent = message;
  el.classList.toggle("error", isError);
  el.classList.remove("hidden");
  UI.toastTimer = setTimeout(() => el.classList.add("hidden"), isError ? 8000 : 4200);
}

async function api(path, options = {}) {
  const opts = { ...options, headers: { "Content-Type": "application/json", ...(options.headers || {}) } };
  if (opts.body && typeof opts.body !== "string") opts.body = JSON.stringify(opts.body);
  const response = await fetch(path, opts);
  let data;
  try { data = await response.json(); } catch (_) { throw { code: "E_IO", message: "服务端返回了非 JSON 响应", context: { status: response.status } }; }
  if (!response.ok || data.ok === false) throw data.error || { code: "E_IO", message: "请求失败", context: { status: response.status } };
  return data;
}

function errorText(error) {
  const code = error?.code || "E_IO";
  const msg = error?.message || String(error);
  const ctx = error?.context ? JSON.stringify(error.context) : "";
  return `${code}: ${msg}${ctx ? `\n${ctx}` : ""}`;
}

async function withBusy(fn, label) {
  if (UI.busy) return;
  UI.busy = true;
  $("btn-run").disabled = true;
  $("btn-appearance").disabled = true;
  $("btn-candidates").disabled = true;
  $("btn-trajectory").disabled = true;
  $("btn-resume").disabled = true;
  $("btn-report").disabled = true;
  const old = $("btn-run").textContent;
  if (label) $("btn-run").textContent = label;
  try { await fn(); }
  catch (error) { showToast(errorText(error), true); }
  finally {
    UI.busy = false;
    $("btn-run").disabled = false;
    $("btn-appearance").disabled = false;
    $("btn-candidates").disabled = false;
    $("btn-trajectory").disabled = false;
    $("btn-resume").disabled = false;
    $("btn-report").disabled = false;
    $("btn-run").textContent = old;
    if (UI.sequenceId) await refreshSequence(UI.sequenceId, false);
  }
}

async function loadHealth() {
  try {
    await api("/api/health");
    $("health").textContent = "本地服务在线";
    $("health").classList.add("ok");
  } catch (_) {
    $("health").textContent = "服务离线";
  }
}

async function loadSequences(preferred = null) {
  try {
    const data = await api("/api/sequences");
    UI.sequences = data.sequences || [];
    renderSequences();
    const wanted = preferred || UI.sequenceId || UI.sequences[0]?.sequence_id;
    if (wanted) await selectSequence(wanted, false);
    else showEmpty();
  } catch (error) {
    $("sequence-list").innerHTML = `<p class="muted">${esc(errorText(error))}</p>`;
  }
}

function renderSequences() {
  if (!UI.sequences.length) {
    $("sequence-list").innerHTML = '<p class="muted">清单中没有序列</p>';
    return;
  }
  $("sequence-list").innerHTML = UI.sequences.map((seq) => {
    const p = seq.frame_count ? seq.valid_frames / seq.frame_count : 0;
    const dot = seq.complete ? "ok" : seq.valid_frames ? "partial" : "idle";
    return `<button class="sequence-card ${seq.sequence_id === UI.sequenceId ? "active" : ""}" data-seq="${esc(seq.sequence_id)}">
      <div class="row"><span class="name">${esc(seq.sequence_id)}</span><span class="dot ${dot}"></span></div>
      <div class="row muted"><span>${esc(seq.motion_type)}</span><span>${seq.valid_frames}/${seq.frame_count}</span></div>
      <div class="progress"><span style="width:${Math.round(p * 100)}%"></span></div>
    </button>`;
  }).join("");
  document.querySelectorAll("[data-seq]").forEach((button) => button.addEventListener("click", () => selectSequence(button.dataset.seq)));
}

async function selectSequence(sequenceId, resetFrame = true) {
  UI.sequenceId = sequenceId;
  if (resetFrame) UI.frame = 0;
  renderSequences();
  await refreshSequence(sequenceId, true);
}

async function refreshSequence(sequenceId, reloadResult = true) {
  if (!sequenceId) return;
  try {
    const status = await api(`/api/sequences/${encodeURIComponent(sequenceId)}`);
    UI.status = status;
    const index = UI.sequences.findIndex((s) => s.sequence_id === sequenceId);
    if (index >= 0) {
      UI.sequences[index].valid_frames = status.valid_frames;
      UI.sequences[index].complete = status.complete;
      UI.sequences[index].occlusion_intervals = status.occlusion_intervals;
      renderSequences();
    }
    renderSequenceHeader(status);
    renderVersions(status);
    renderOcclusion(status);
    const maxFrame = Math.max(0, status.frame_count - 1);
    UI.frame = clamp(UI.frame, 0, maxFrame);
    $("frame-slider").max = String(maxFrame);
    $("frame-slider").disabled = status.frame_count === 0;
    $("frame-slider").value = String(UI.frame);
    if (reloadResult) {
      UI.result = null;
      if (status.complete) {
        try { UI.result = (await api(`/api/sequences/${encodeURIComponent(sequenceId)}/result`)).bundle; } catch (_) {}
      }
      drawChart();
    }
    await loadFrame(UI.frame);
    await loadInvalidations(sequenceId);
  } catch (error) {
    showToast(errorText(error), true);
  }
}

function renderSequenceHeader(status) {
  $("seq-title").textContent = status.sequence_id;
  const motion = status.motion_model?.type || "unknown";
  $("seq-meta").textContent = `${motion} · ${status.visible_frames} 帧 @ ${status.frame_rate} fps · 搜索扩展 ${status.search_window_expansion} · 更新阈值 ${status.appearance_update_threshold} · 置信下限 ${status.trace_confidence_floor}`;
}

function renderVersions(status) {
  const stages = status.stages || {};
  const rows = [
    ["appearance", "外观建模", stages.appearance],
    ["candidates", "候选打分", stages.candidates],
    ["trajectory", "轨迹演进", stages.trajectory],
  ];
  $("stage-cards").innerHTML = rows.map(([key, label, value]) => {
    const ready = Boolean(value?.ready);
    const frames = value?.frames_ready ?? 0;
    const p = status.frame_count ? frames / status.frame_count : 0;
    return `<div class="stage-card"><div class="row"><span>${esc(label)}</span><span class="pill ${ready ? "ok" : ""}">${ready ? "ready" : `${frames}/${status.frame_count}`}</span></div><div class="bar"><span style="width:${Math.round(p * 100)}%"></span></div></div>`;
  }).join("");
  const versions = status.versions || {};
  const items = [
    ["sequence", versions.sequence_version_fingerprint],
    ["scoring", versions.scoring_config_fingerprint],
    ["trajectory", versions.trajectory_config_fingerprint],
    ["checkpoint chain", versions.checkpoint?.chain_digest],
  ];
  $("version-fingerprints").innerHTML = items.map(([label, value]) => `<div class="fp"><span>${esc(label)}</span><code title="${esc(value || "")}">${esc(shortHash(value, 24))}</code></div>`).join("");
}

function renderOcclusion(status) {
  const intervals = status.occlusion_intervals || [];
  if (!intervals.length) {
    $("occlusion-chips").innerHTML = '<span class="chip">无遮挡区间</span>';
    $("occlusion-panel").innerHTML = '<p class="muted">该序列没有已发布遮挡事件</p>';
    return;
  }
  $("occlusion-chips").innerHTML = intervals.map((item) => `<span class="chip ${item.recovered ? "recovered" : "occluded"}">${item.start_frame}–${item.end_frame}${item.recovered ? ` → ${item.recovery_frame}` : " 未恢复"}</span>`).join("");
  $("occlusion-panel").innerHTML = intervals.map((item) => `<div class="event ${item.recovered ? "" : "warn"}"><strong>遮挡 ${item.start_frame}–${item.end_frame}</strong>锚点帧 ${item.anchor_frame ?? "—"} · ${item.recovered ? `恢复帧 ${item.recovery_frame}` : "序列结束时仍遮挡"}</div>`).join("");
}

async function loadInvalidations(sequenceId) {
  try {
    const data = await api(`/api/invalidations?sequence_id=${encodeURIComponent(sequenceId)}`);
    const events = data.invalidations || [];
    $("invalidation-panel").innerHTML = events.length ? events.slice(-8).reverse().map((event) => `<div class="event warn"><strong>${esc(event.stage)} · ${esc(event.reason)}</strong>帧 ${event.earliest_affected_frame}–${event.affected_frame_interval?.[1]} · 保留至 ${event.retained_through_frame}<br><span class="mono">${esc(shortHash(event.current_fingerprint, 20))}</span></div>`).join("") : '<p class="muted">无失效记录</p>';
    const errors = data.diagnostics || [];
    $("error-panel").innerHTML = errors.length ? errors.slice(-6).reverse().map((item) => `<div class="event warn"><strong>${esc(item.code)}</strong>seq=${esc(item.sequence_id || "—")} frame=${esc(item.frame_index ?? "—")}<br>${esc(item.message)}</div>`).join("") : '<p class="muted">无持久化错误</p>';
  } catch (_) {}
}

function createImageDataFromBase64(base64, width, height) {
  const binary = atob(base64);
  const rgba = new Uint8ClampedArray(width * height * 4);
  for (let i = 0; i < width * height; i += 1) {
    const v = binary.charCodeAt(i);
    rgba[i * 4] = v;
    rgba[i * 4 + 1] = v;
    rgba[i * 4 + 2] = v;
    rgba[i * 4 + 3] = 255;
  }
  return new ImageData(rgba, width, height);
}

function drawBox(ctx, box, color, lineWidth = 1) {
  if (!box) return;
  const [x, y, w, h] = box;
  ctx.save();
  ctx.strokeStyle = color;
  ctx.lineWidth = lineWidth;
  ctx.strokeRect(Math.round(x * ctx.canvas.width) + 0.5, Math.round(y * ctx.canvas.height) + 0.5, Math.round(w * ctx.canvas.width), Math.round(h * ctx.canvas.height));
  ctx.restore();
}

async function loadFrame(frameIndex) {
  if (!UI.sequenceId) return;
  const maxFrame = Math.max(0, (UI.status?.frame_count || 1) - 1);
  UI.frame = clamp(Number(frameIndex), 0, maxFrame);
  $("frame-slider").value = String(UI.frame);
  $("frame-label").textContent = `${UI.frame} / ${maxFrame}`;
  try {
    const payload = await api(`/api/sequences/${encodeURIComponent(UI.sequenceId)}/frame/${UI.frame}`);
    UI.payload = payload;
    const [height, width] = payload.shape;
    const canvas = $("frame-canvas");
    canvas.width = width;
    canvas.height = height;
    const ctx = canvas.getContext("2d", { willReadFrequently: true });
    ctx.putImageData(createImageDataFromBase64(payload.pixels_base64, width, height), 0, 0);
    drawBox(ctx, payload.reference_truth_box_xywh_norm, "rgba(110,231,168,.95)", 1);
    drawBox(ctx, payload.tracked_box_xywh_norm, "rgba(67,217,208,.95)", 1);
    $("frame-empty").classList.add("hidden");
    await loadDiagnostics();
  } catch (error) {
    showToast(errorText(error), true);
  }
}

async function loadDiagnostics() {
  if (!UI.sequenceId) return;
  $("diag-status").textContent = `帧 ${UI.frame}`;
  try {
    const diag = await api(`/api/sequences/${encodeURIComponent(UI.sequenceId)}/diagnostics/${UI.frame}`);
    const tracking = diag.trajectory || {};
    const candidate = diag.candidate || {};
    $("m-action").textContent = tracking.state_action || "—";
    $("m-response").textContent = num(tracking.response_score, 5);
    $("m-iou").textContent = num(tracking.iou, 5);
    $("m-error").textContent = num(tracking.center_error_normalized, 5);
    $("m-update").textContent = tracking.template_updated ? "是" : "否";
    $("m-occluded").textContent = tracking.occluded ? "是" : "否";
    const comps = candidate.score_components || {};
    const grid = candidate.sampling_grid || {};
    const top = candidate.top_candidates || [];
    const rows = top.map((item) => `<tr><td class="num">${esc(item.candidate_id)}</td><td class="num">${item.box_xywh_norm.map((v) => num(v, 4)).join(", ")}</td><td class="num">${num(item.response_score, 5)}</td><td class="num">${num(item.gray_similarity, 4)}</td><td class="num">${num(item.gradient_similarity, 4)}</td><td class="num">${num(item.spatial_similarity, 4)}</td><td class="num">${num(item.center_prior, 4)}</td></tr>`).join("");
    $("diagnostics").innerHTML = `
      <div class="diag-summary">
        <div class="diag-box"><span>外观 / 梯度 / 空间</span><strong>${num(comps.gray_similarity, 3)} / ${num(comps.gradient_similarity, 3)} / ${num(comps.spatial_similarity, 3)}</strong></div>
        <div class="diag-box"><span>中心先验 / 对数分</span><strong>${num(comps.center_prior, 3)} / ${num(comps.log_score, 3)}</strong></div>
        <div class="diag-box"><span>候选 / 有效</span><strong>${candidate.candidate_count ?? "—"} / ${candidate.valid_candidate_count ?? "—"}</strong></div>
        <div class="diag-box"><span>网格 / 搜索扩展</span><strong>${esc((grid.grid_step_pixels || []).join("×"))} px / ${num(grid.search_window_expansion, 2)}</strong></div>
      </div>
      <p class="muted">候选 ID、框 (x,y,w,h)、响应、灰度、梯度、空间签名、中心先验；检查点序号 ${diag.checkpoint_ordinal} · ${esc(shortHash(diag.checkpoint_digest, 20))}</p>
      <table><thead><tr><th>ID</th><th>box</th><th>score</th><th>gray</th><th>grad</th><th>space</th><th>prior</th></tr></thead><tbody>${rows}</tbody></table>`;
  } catch (error) {
    $("m-action").textContent = "—";
    $("m-response").textContent = "—";
    $("m-iou").textContent = "—";
    $("m-error").textContent = "—";
    $("m-update").textContent = "—";
    $("m-occluded").textContent = "—";
    $("diag-status").textContent = error.code || "不可诊断";
    $("diagnostics").innerHTML = `<div class="event warn"><strong>${esc(error.code || "E_INVALID_STAGE")}</strong>${esc(error.message || errorText(error))}<br><span class="muted">诊断需要当前外观阶段与连续前置检查点。</span></div>`;
  }
}

function drawChart() {
  const canvas = $("error-chart");
  const ctx = canvas.getContext("2d");
  const W = canvas.width, H = canvas.height;
  ctx.clearRect(0, 0, W, H);
  ctx.fillStyle = "#101821";
  ctx.fillRect(0, 0, W, H);
  if (!UI.result) {
    $("chart-note").textContent = "完整运行后显示";
    $("chart-note").textContent = "完整运行后显示";
    return;
  }
  const errors = UI.result.frame_errors || [];
  if (!errors.length) return;
  $("chart-note").textContent = `${errors.length} 帧 · mean IoU ${num(UI.result.mean_iou, 4)}`;
  const pad = { l: 38, r: 16, t: 16, b: 24 };
  const plotW = W - pad.l - pad.r, plotH = H - pad.t - pad.b;
  const maxErr = Math.max(0.05, ...errors.map((e) => e.center_error_normalized));
  ctx.strokeStyle = "#253443"; ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i += 1) {
    const y = pad.t + (plotH * i) / 4;
    ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(W - pad.r, y); ctx.stroke();
  }
  ctx.fillStyle = "#8496a8"; ctx.font = "10px ui-monospace, monospace";
  ctx.fillText("IoU 1.0", 2, pad.t + 4);
  ctx.fillText("0.0", 2, pad.t + plotH + 4);
  ctx.fillText("center err", 2, pad.t + plotH / 2 + 4);
  const intervals = UI.result.occlusion_intervals || [];
  intervals.forEach((iv) => {
    const x0 = pad.l + (iv.start_frame / Math.max(1, errors.length - 1)) * plotW;
    const x1 = pad.l + ((iv.end_frame + 1) / Math.max(1, errors.length - 1)) * plotW;
    ctx.fillStyle = "rgba(242,184,75,.10)";
    ctx.fillRect(x0, pad.t, Math.max(2, x1 - x0), plotH);
  });
  const line = (key, color, scale) => {
    ctx.beginPath();
    errors.forEach((item, i) => {
      const x = pad.l + (i / Math.max(1, errors.length - 1)) * plotW;
      const y = pad.t + plotH - clamp(Number(item[key]) / scale, 0, 1) * plotH;
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = color; ctx.lineWidth = 1.6; ctx.stroke();
  };
  line("iou", "#6ee7a8", 1.0);
  line("center_error_normalized", "#f2b84b", maxErr);
  const last = errors[errors.length - 1];
  const x = pad.l + plotW;
  const y1 = pad.t + plotH - clamp(Number(last.iou), 0, 1) * plotH;
  ctx.fillStyle = "#6ee7a8"; ctx.beginPath(); ctx.arc(x, y1, 2.5, 0, Math.PI * 2); ctx.fill();
  ctx.fillStyle = "#8496a8"; ctx.font = "10px system-ui, sans-serif";
  ctx.fillText("IoU", W - 88, pad.t + 10);
  ctx.fillStyle = "#f2b84b"; ctx.fillText("center error", W - 88, pad.t + 24);
}

function showEmpty() {
  $("frame-empty").classList.remove("hidden");
  $("seq-title").textContent = "选择序列";
  $("seq-meta").textContent = "—";
}

async function runStage(stage) {
  if (!UI.sequenceId) return;
  await withBusy(async () => {
    const data = await api("/api/run", { method: "POST", body: { sequence_id: UI.sequenceId, stage } });
    if (data.status === "stopped") showToast(`已在帧 ${data.valid_frames}/${data.total_frames} 优雅停止；点击恢复继续。`);
    else if (data.status === "complete") showToast(`序列完成：${data.valid_frames} 帧。`);
    else showToast(JSON.stringify(data, null, 2));
  }, "执行中…");
}

async function stopRun() {
  if (!UI.sequenceId) return;
  try {
    await api("/api/stop", { method: "POST", body: { sequence_id: UI.sequenceId, reason: "dashboard stop button" } });
    showToast("停止请求已写入；当前逐帧单元完成后退出。");
    await refreshSequence(UI.sequenceId, false);
  } catch (error) { showToast(errorText(error), true); }
}

async function resumeRun() {
  if (!UI.sequenceId) return;
  await withBusy(async () => {
    const data = await api("/api/resume", { method: "POST", body: { sequence_id: UI.sequenceId } });
    showToast(`恢复执行：${data.status} · ${data.valid_frames}/${data.total_frames} 帧`);
  }, "恢复中…");
}

function bindControls() {
  $("btn-refresh").addEventListener("click", () => loadSequences(UI.sequenceId));
  $("btn-run").addEventListener("click", () => runStage("all"));
  $("btn-appearance").addEventListener("click", () => runStage("appearance"));
  $("btn-candidates").addEventListener("click", () => runStage("candidates"));
  $("btn-trajectory").addEventListener("click", () => runStage("trajectory"));
  $("btn-stop").addEventListener("click", stopRun);
  $("btn-resume").addEventListener("click", resumeRun);
  $("btn-report").addEventListener("click", () => withBusy(async () => {
    const data = await api("/api/report", { method: "POST", body: {} });
    showToast(`总报告完成：${data.run_bundle_path}\nsummary_sha256 ${data.summary_sha256}`);
  }, "汇总中…"));
  $("btn-import").addEventListener("click", () => withBusy(async () => {
    const manifestPath = prompt("输入本地 tracking manifest 路径（绝对路径或相对工作区路径）：");
    if (!manifestPath) return;
    const sourceDataDir = prompt("可选：源数据目录（留空则使用清单所在目录）：") || null;
    const data = await api("/api/import", { method: "POST", body: { manifest_path: manifestPath, source_data_dir: sourceDataDir } });
    showToast(`已导入 ${data.manifest_path}`);
    await loadSequences();
  }, "导入中…"));
  $("btn-generate").addEventListener("click", () => withBusy(async () => {
    if (!confirm("重新生成固定种子演示数据并清空序列目录，是否继续？")) return;
    const data = await api("/api/generate", { method: "POST", body: { force: true } });
    showToast(`演示数据已生成：${data.total_visible_frames} 帧\nmanifest ${shortHash(data.manifest_sha256, 20)}`);
    UI.sequenceId = null;
    await loadSequences();
  }, "生成中…"));
  $("frame-slider").addEventListener("input", (event) => loadFrame(Number(event.target.value)));
  $("btn-prev").addEventListener("click", () => loadFrame(UI.frame - 1));
  $("btn-next").addEventListener("click", () => loadFrame(UI.frame + 1));
  document.addEventListener("keydown", (event) => {
    if (event.target.tagName === "INPUT" || event.target.tagName === "TEXTAREA") return;
    if (event.key === "ArrowLeft") loadFrame(UI.frame - 1);
    if (event.key === "ArrowRight") loadFrame(UI.frame + 1);
  });
}

async function boot() {
  bindControls();
  await loadHealth();
  await loadSequences();
  setInterval(async () => {
    if (!UI.busy && UI.sequenceId) {
      try { await refreshSequence(UI.sequenceId, false); } catch (_) {}
    }
  }, 5000);
}

boot();
