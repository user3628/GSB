(() => {
  "use strict";

  const state = {
    health: null,
    workspace: null,
    selected: null,
    diagnostics: new Map(),
    frames: new Map(),
    job: null,
    pollTimer: null,
    chunks: new Map(),
    f0Chart: null,
    frameIndex: 0,
  };

  const $ = (id) => document.getElementById(id);
  const stageNames = ["analysis", "mapping", "reconstruction", "validation"];
  const stageLabels = {
    analysis: "FRAME ANALYSIS",
    mapping: "PARAM MAP",
    reconstruction: "RECONSTRUCT",
    validation: "VALIDATION",
  };
  const injectionPayloads = {
    channels: { channels: 2 },
    sample_rate: { declared_sample_rate: 44100 },
    nan: { nan_index: 321 },
    count: { sample_count_delta: 7 },
    zero_pitch: { pitch_shift: 0 },
    range: { pitch_shift: 0.5 },
    missing_truth: { remove_reference: true },
  };

  function log(message, kind = "") {
    const node = document.createElement("div");
    node.className = `log-line ${kind}`;
    const time = document.createElement("span");
    time.textContent = new Date().toLocaleTimeString("en-GB", { hour12: false }) + "  ";
    node.appendChild(time);
    node.appendChild(document.createTextNode(message));
    const logNode = $("event-log");
    logNode.appendChild(node);
    logNode.scrollTop = logNode.scrollHeight;
    while (logNode.children.length > 180) logNode.removeChild(logNode.firstChild);
  }

  async function api(path, options = {}) {
    const init = { ...options, headers: { ...(options.headers || {}) } };
    if (init.body && typeof init.body !== "string") {
      init.body = JSON.stringify(init.body);
      init.headers["Content-Type"] = "application/json";
    }
    const response = await fetch(path, init);
    let payload = null;
    try {
      payload = await response.json();
    } catch {
      payload = { ok: false, error: { code: "E_BAD_RESPONSE", message: "invalid JSON response" } };
    }
    if (!response.ok || payload.ok === false) {
      const error = payload.error || { code: "E_HTTP", message: `HTTP ${response.status}` };
      const exc = new Error(error.message || error.code);
      exc.payload = payload;
      throw exc;
    }
    return payload;
  }

  function nfmt(value, digits = 2) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
    return Number(value).toFixed(digits);
  }

  function hashShort(value) {
    return value ? `${String(value).slice(0, 10)}…` : "—";
  }

  function setBusy(busy) {
    ["btn-convert", "btn-stream", "btn-resume", "btn-demo", "btn-inject"].forEach((id) => {
      if ($(id)) $(id).disabled = busy;
    });
  }

  async function refreshHealth() {
    try {
      state.health = await api("/api/health");
      $("health-dot").classList.add("online");
      $("health-text").textContent = `numpy ${state.health.numpy} · deterministic`;
      $("workspace-path").textContent = state.health.workspace;
      $("workspace-path").title = state.health.workspace;
    } catch (error) {
      $("health-dot").classList.remove("online");
      $("health-text").textContent = "offline";
      log(error.message, "error");
    }
  }

  async function refreshState() {
    try {
      const payload = await api("/api/state");
      state.workspace = payload;
      $("manifest-digest").textContent = payload.manifest_digest
        ? `sha256:${payload.manifest_digest}`
        : "no manifest";
      const shards = payload.shards || [];
      const complete = shards.filter((s) => s.checkpoints.stage_status.validation === "complete").length;
      $("stat-shards").textContent = String(shards.length);
      $("stat-targets").textContent = String(new Set(shards.map((s) => s.spec.target_speaker)).size);
      $("stat-samples").textContent = shards.reduce((sum, s) => sum + Number(s.spec.sample_count || 0), 0).toLocaleString();
      $("stat-complete").textContent = String(complete);
      const stop = payload.stop || {};
      $("stop-indicator").textContent = stop.requested ? `STOP REQUESTED · ${stop.reason || "operator"}` : "no stop request";
      $("stop-indicator").classList.toggle("active", Boolean(stop.requested));

      if (!state.selected || !shards.some((s) => s.spec.shard_id === state.selected)) {
        state.selected = shards[0]?.spec.shard_id || null;
      }
      renderShardGrid(shards);
      await loadAllDiagnostics(shards);
      renderDetail();
    } catch (error) {
      log(formatError(error), "error");
    }
  }

  async function loadAllDiagnostics(shards) {
    await Promise.all(
      shards.map(async (item) => {
        const id = item.spec.shard_id;
        if (item.checkpoints.stage_status.validation !== "complete") {
          state.diagnostics.delete(id);
          return;
        }
        try {
          state.diagnostics.set(id, await api(`/api/diagnostics/${encodeURIComponent(id)}`));
        } catch (error) {
          state.diagnostics.delete(id);
          log(`${id}: diagnostics unavailable: ${formatError(error)}`, "error");
        }
      })
    );
  }

  function stageStatus(item, stage) {
    return item.checkpoints?.stage_status?.[stage] || "pending";
  }

  function renderShardGrid(shards) {
    const grid = $("shard-grid");
    grid.replaceChildren();
    if (!shards.length) {
      const empty = document.createElement("div");
      empty.className = "empty-state";
      empty.textContent = "工作区尚无清单。点击 Seed demo 生成 3 个确定性中英混排分片。";
      grid.appendChild(empty);
      return;
    }
    shards.forEach((item, index) => {
      const spec = item.spec;
      const card = document.createElement("article");
      card.className = "shard-card" + (state.selected === spec.shard_id ? " selected" : "");
      card.dataset.shardId = spec.shard_id;
      card.addEventListener("click", () => {
        state.selected = spec.shard_id;
        state.frameIndex = 0;
        renderShardGrid(state.workspace.shards);
        renderDetail();
      });

      const top = document.createElement("div");
      top.className = "shard-top";
      const heading = document.createElement("div");
      const title = document.createElement("h3");
      title.className = "shard-id";
      title.textContent = spec.shard_id;
      const route = document.createElement("div");
      route.className = "speaker-route";
      const source = document.createElement("span");
      source.className = "speaker-chip";
      source.textContent = spec.source_speaker;
      const arrow = document.createElement("span");
      arrow.className = "speaker-arrow";
      arrow.textContent = "→";
      const target = document.createElement("span");
      target.className = "speaker-chip target";
      target.textContent = spec.target_speaker;
      route.append(source, arrow, target);
      heading.append(title, route);
      const idx = document.createElement("span");
      idx.className = "shard-index";
      idx.textContent = String(index + 1).padStart(2, "0");
      top.append(heading, idx);

      const transcript = document.createElement("p");
      transcript.className = "transcript-card";
      transcript.textContent = spec.transcript || "";
      transcript.title = spec.transcript || "";

      const params = document.createElement("div");
      params.className = "param-strip";
      [
        ["pitch", `${nfmt(spec.pitch_shift, 2)}×`],
        ["warp", nfmt(spec.mel_warp, 2)],
        ["hop", String(spec.frame_shift)],
        ["samples", Number(spec.expected_output_samples).toLocaleString()],
      ].forEach(([label, value]) => {
        const cell = document.createElement("div");
        cell.className = "param-cell";
        const l = document.createElement("span");
        l.textContent = label;
        const v = document.createElement("strong");
        v.textContent = value;
        cell.append(l, v);
        params.appendChild(cell);
      });

      const canvas = document.createElement("canvas");
      canvas.className = "mini-chart";
      canvas.dataset.miniFor = spec.shard_id;

      const stages = document.createElement("div");
      stages.className = "stage-row";
      stageNames.forEach((stage) => {
        const status = stageStatus(item, stage);
        const pill = document.createElement("div");
        pill.className = `stage-pill ${status}`;
        const name = document.createElement("span");
        name.textContent = stage.slice(0, 7);
        const statusNode = document.createElement("strong");
        statusNode.textContent = status;
        pill.append(name, statusNode);
        stages.appendChild(pill);
      });

      const footer = document.createElement("div");
      footer.className = "shard-footer";
      const shardState = document.createElement("span");
      shardState.className = "shard-state";
      const diag = state.diagnostics.get(spec.shard_id);
      shardState.textContent = diag
        ? `in-range ${(diag.summary.in_range_ratio * 100).toFixed(1)}% · f0 ${nfmt(diag.summary.mean_target_f0_hz, 1)} Hz`
        : "not converted";
      const actions = document.createElement("div");
      actions.className = "mini-actions";
      const convert = document.createElement("button");
      convert.className = "mini-btn";
      convert.textContent = "convert";
      convert.addEventListener("click", (event) => {
        event.stopPropagation();
        startConvert([spec.shard_id]);
      });
      const focus = document.createElement("button");
      focus.className = "mini-btn";
      focus.textContent = "focus";
      focus.addEventListener("click", (event) => {
        event.stopPropagation();
        state.selected = spec.shard_id;
        renderShardGrid(state.workspace.shards);
        renderDetail();
        $("detail-panel").scrollIntoView({ behavior: "smooth", block: "start" });
      });
      actions.append(convert, focus);
      footer.append(shardState, actions);

      card.append(top, transcript, params, canvas, stages, footer);
      grid.appendChild(card);
      requestAnimationFrame(() => drawMiniChart(canvas, state.diagnostics.get(spec.shard_id)));
    });
  }

  function currentShard() {
    if (!state.workspace || !state.selected) return null;
    return (state.workspace.shards || []).find((item) => item.spec.shard_id === state.selected) || null;
  }

  function renderDetail() {
    const item = currentShard();
    if (!item) {
      $("detail-title").textContent = "选择一个分片";
      return;
    }
    const spec = item.spec;
    const diag = state.diagnostics.get(spec.shard_id);
    $("detail-title").textContent = `${spec.shard_id} · ${spec.source_speaker} → ${spec.target_speaker}`;
    $("detail-transcript").textContent = spec.transcript || "";
    const download = $("download-wav");
    if (diag) {
      download.href = `/api/export/${encodeURIComponent(spec.shard_id)}.wav`;
      download.classList.remove("disabled");
      download.setAttribute("download", `${spec.shard_id}.wav`);
    } else {
      download.href = "#";
      download.classList.add("disabled");
      download.removeAttribute("download");
    }
    renderCheckpoints(item, diag);
    renderF0Chart(diag);
    renderChunks(spec.shard_id);

    const slider = $("frame-slider");
    const frames = diag?.frames || [];
    slider.disabled = !diag || frames.length === 0;
    slider.max = String(Math.max(0, frames.length - 1));
    if (state.frameIndex >= frames.length) state.frameIndex = 0;
    slider.value = String(state.frameIndex);
    if (diag) {
      inspectFrame(state.frameIndex, false);
    } else {
      $("frame-index-label").textContent = "frame —";
      $("metric-source-f0").textContent = "—";
      $("metric-target-f0").textContent = "—";
      $("metric-deviation").textContent = "—";
      $("metric-alignment").textContent = "—";
      $("frame-latency").textContent = "—";
      drawMel(null);
    }
  }

  function renderCheckpoints(item, diag) {
    const list = $("checkpoint-list");
    list.replaceChildren();
    stageNames.forEach((stage) => {
      const row = document.createElement("div");
      row.className = "checkpoint-row";
      const status = stageStatus(item, stage);
      const seq = item.checkpoints?.current?.[stage]?.seq;
      const digest = item.checkpoints?.current?.[stage]?.artifact_digest;
      const seqNode = document.createElement("span");
      seqNode.className = "checkpoint-seq";
      seqNode.textContent = seq ? `#${String(seq).padStart(2, "0")}` : "··";
      const name = document.createElement("div");
      name.className = "checkpoint-name";
      const strong = document.createElement("strong");
      strong.textContent = stageLabels[stage];
      const sub = document.createElement("span");
      sub.textContent = digest ? `artifact ${hashShort(digest)}` : "no artifact";
      name.append(strong, sub);
      const statusNode = document.createElement("span");
      statusNode.className = `checkpoint-status ${status}`;
      statusNode.textContent = status;
      row.append(seqNode, name, statusNode);
      list.appendChild(row);
    });
  }

  function chartCanvas(canvas) {
    const rect = canvas.getBoundingClientRect();
    const width = Math.max(220, rect.width || 600);
    const height = Number(canvas.getAttribute("height")) || 260;
    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    canvas.width = Math.round(width * dpr);
    canvas.height = Math.round(height * dpr);
    const ctx = canvas.getContext("2d");
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    return { ctx, width, height };
  }

  function drawMiniChart(canvas, diag) {
    const { ctx, width, height } = chartCanvas(canvas);
    ctx.clearRect(0, 0, width, height);
    const frames = diag?.frames || [];
    const active = frames.filter((f) => f.target_f0_hz !== null);
    if (!active.length) {
      ctx.fillStyle = "rgba(140,165,173,0.45)";
      ctx.font = "9px monospace";
      ctx.fillText("awaiting conversion", 10, height / 2 + 3);
      return;
    }
    const ref = diag.summary.reference_pitch_hz;
    const values = active.map((f) => Number(f.target_f0_hz));
    const minY = Math.min(ref.min_hz, ...values) - 8;
    const maxY = Math.max(ref.max_hz, ...values) + 8;
    const x = (i) => 2 + (i / Math.max(1, frames.length - 1)) * (width - 4);
    const y = (v) => height - 5 - ((v - minY) / Math.max(1, maxY - minY)) * (height - 10);
    ctx.fillStyle = "rgba(98,231,215,0.075)";
    ctx.fillRect(0, y(ref.max_hz), width, Math.max(1, y(ref.min_hz) - y(ref.max_hz)));
    ctx.strokeStyle = "rgba(98,231,215,0.55)";
    ctx.lineWidth = 1.3;
    ctx.beginPath();
    let started = false;
    frames.forEach((frame, index) => {
      if (frame.target_f0_hz === null) {
        started = false;
        return;
      }
      if (!started) {
        ctx.moveTo(x(index), y(frame.target_f0_hz));
        started = true;
      } else {
        ctx.lineTo(x(index), y(frame.target_f0_hz));
      }
    });
    ctx.stroke();
    ctx.fillStyle = "rgba(248,196,92,0.9)";
    frames.forEach((frame, index) => {
      if (frame.alignment === "extrapolation") ctx.fillRect(x(index) - 1, y(frame.target_f0_hz) - 1, 2, 2);
    });
  }

  function renderF0Chart(diag) {
    const canvas = $("f0-canvas");
    const { ctx, width, height } = chartCanvas(canvas);
    ctx.clearRect(0, 0, width, height);
    const margin = { left: 48, right: 18, top: 18, bottom: 27 };
    const plotW = width - margin.left - margin.right;
    const plotH = height - margin.top - margin.bottom;
    ctx.font = "9px monospace";
    ctx.lineWidth = 1;
    ctx.strokeStyle = "rgba(137,189,202,0.11)";
    for (let i = 0; i <= 4; i += 1) {
      const yy = margin.top + (i / 4) * plotH;
      ctx.beginPath();
      ctx.moveTo(margin.left, yy);
      ctx.lineTo(width - margin.right, yy);
      ctx.stroke();
    }
    if (!diag) {
      ctx.fillStyle = "rgba(140,165,173,0.5)";
      ctx.font = "11px monospace";
      ctx.fillText("convert this shard to load F0 truth-band diagnostics", margin.left, height / 2);
      state.f0Chart = null;
      return;
    }
    const frames = diag.frames;
    const ref = diag.summary.reference_pitch_hz;
    const values = frames.filter((f) => f.target_f0_hz !== null).map((f) => Number(f.target_f0_hz));
    const minY = Math.min(ref.min_hz, ...values) - 12;
    const maxY = Math.max(ref.max_hz, ...values) + 12;
    const x = (index) => margin.left + (index / Math.max(1, frames.length - 1)) * plotW;
    const y = (value) => margin.top + (1 - (value - minY) / Math.max(1e-6, maxY - minY)) * plotH;
    ctx.fillStyle = "rgba(98,231,215,0.07)";
    ctx.fillRect(margin.left, y(ref.max_hz), plotW, Math.max(1, y(ref.min_hz) - y(ref.max_hz)));
    ctx.strokeStyle = "rgba(98,231,215,0.35)";
    ctx.setLineDash([4, 4]);
    [ref.min_hz, ref.max_hz].forEach((v) => {
      ctx.beginPath();
      ctx.moveTo(margin.left, y(v));
      ctx.lineTo(width - margin.right, y(v));
      ctx.stroke();
    });
    ctx.setLineDash([]);
    ctx.fillStyle = "rgba(140,165,173,0.75)";
    ctx.textAlign = "right";
    for (let i = 0; i <= 4; i += 1) {
      const value = maxY - (i / 4) * (maxY - minY);
      ctx.fillText(`${value.toFixed(0)}`, margin.left - 8, margin.top + (i / 4) * plotH + 3);
    }
    ctx.textAlign = "left";
    ctx.fillText("0", margin.left, height - 9);
    ctx.textAlign = "right";
    ctx.fillText(`frame ${frames.length - 1}`, width - margin.right, height - 9);

    ctx.strokeStyle = "#62e7d7";
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    let started = false;
    frames.forEach((frame, index) => {
      if (frame.target_f0_hz === null) {
        started = false;
        return;
      }
      if (!started) {
        ctx.moveTo(x(index), y(frame.target_f0_hz));
        started = true;
      } else {
        ctx.lineTo(x(index), y(frame.target_f0_hz));
      }
    });
    ctx.stroke();
    frames.forEach((frame, index) => {
      if (frame.target_f0_hz === null) return;
      ctx.fillStyle = frame.alignment === "extrapolation" ? "#f8c45c" : "rgba(98,231,215,0.72)";
      ctx.beginPath();
      ctx.arc(x(index), y(frame.target_f0_hz), frame.alignment === "extrapolation" ? 2.7 : 1.5, 0, Math.PI * 2);
      ctx.fill();
    });
    const selectedX = x(Math.min(state.frameIndex, frames.length - 1));
    ctx.strokeStyle = "rgba(169,156,255,0.8)";
    ctx.lineWidth = 1;
    ctx.setLineDash([2, 3]);
    ctx.beginPath();
    ctx.moveTo(selectedX, margin.top);
    ctx.lineTo(selectedX, margin.top + plotH);
    ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = "rgba(169,156,255,0.9)";
    ctx.font = "9px monospace";
    ctx.textAlign = selectedX > width / 2 ? "right" : "left";
    ctx.fillText(`frame ${state.frameIndex}`, selectedX + (selectedX > width / 2 ? -5 : 5), margin.top + 9);
    state.f0Chart = { x, frames, margin, width, height, plotW, plotH };
  }

  async function inspectFrame(index, redraw = true) {
    const item = currentShard();
    const diag = item ? state.diagnostics.get(item.spec.shard_id) : null;
    if (!item || !diag || !diag.frames.length) return;
    state.frameIndex = Math.max(0, Math.min(index, diag.frames.length - 1));
    $("frame-slider").value = String(state.frameIndex);
    $("frame-index-label").textContent = `frame ${state.frameIndex}`;
    if (redraw) renderF0Chart(diag);
    const budget = $("latency-budget").value.trim();
    let path = `/api/diagnostics/${encodeURIComponent(item.spec.shard_id)}/frame?frame_index=${state.frameIndex}`;
    if (budget !== "") path += `&latency_budget_ms=${encodeURIComponent(budget)}`;
    try {
      const payload = await api(path);
      state.frames.set(item.spec.shard_id, payload);
      const analysis = payload.analysis_frame;
      const mapping = payload.mapping_frame;
      const diagnostic = payload.diagnostic_frame;
      $("metric-source-f0").textContent = mapping.source_f0_hz === null ? "UNVOICED" : `${nfmt(mapping.source_f0_hz, 2)} Hz`;
      $("metric-target-f0").textContent = mapping.target_f0_hz === null ? "UNVOICED" : `${nfmt(mapping.target_f0_hz, 2)} Hz`;
      $("metric-deviation").textContent = diagnostic.deviation_from_center_hz === null ? "—" : `${nfmt(diagnostic.deviation_from_center_hz, 2)} Hz`;
      $("metric-alignment").textContent = diagnostic.alignment.toUpperCase();
      $("frame-latency").textContent = `${nfmt(payload.latency_ms, 2)} ms`;
      drawMel(analysis.mel_log_energy);
    } catch (error) {
      const code = error.payload?.error?.code || "ERROR";
      $("frame-latency").textContent = code;
      log(`frame ${state.frameIndex}: ${formatError(error)}`, "error");
    }
  }

  function drawMel(mel) {
    const canvas = $("mel-canvas");
    const { ctx, width, height } = chartCanvas(canvas);
    ctx.clearRect(0, 0, width, height);
    if (!mel) {
      ctx.fillStyle = "rgba(140,165,173,0.45)";
      ctx.font = "9px monospace";
      ctx.fillText("select a converted frame", 8, height / 2);
      return;
    }
    const min = Math.min(...mel);
    const max = Math.max(...mel);
    const gap = 2;
    const barW = (width - gap * (mel.length - 1)) / mel.length;
    mel.forEach((value, index) => {
      const ratio = (value - min) / Math.max(1e-6, max - min);
      const barH = Math.max(2, ratio * (height - 18));
      const hue = 174 - ratio * 22;
      ctx.fillStyle = `hsla(${hue}, 72%, ${42 + ratio * 25}%, ${0.44 + ratio * 0.5})`;
      ctx.fillRect(index * (barW + gap), height - barH - 1, barW, barH);
    });
  }

  function renderChunks(shardId) {
    const chunks = state.chunks.get(shardId);
    const table = $("chunk-table");
    table.replaceChildren();
    if (!chunks) {
      table.className = "chunk-table empty";
      table.textContent = "运行 Stream verify 查看逐块 PCM 摘要。";
      $("chunk-summary").textContent = "not run";
      return;
    }
    table.className = "chunk-table";
    $("chunk-summary").textContent = `${chunks.length} chunks · ${Number(chunks.reduce((sum, c) => sum + c.sample_count, 0)).toLocaleString()} samples`;
    const head = document.createElement("div");
    head.className = "chunk-row chunk-head";
    ["#", "frames", "samples", "pcm sha256"].forEach((label) => {
      const cell = document.createElement("span");
      cell.textContent = label;
      head.appendChild(cell);
    });
    table.appendChild(head);
    chunks.forEach((chunk) => {
      const row = document.createElement("div");
      row.className = "chunk-row";
      [String(chunk.chunk_index), `${chunk.frame_start}+${chunk.frame_count}`, String(chunk.sample_count), chunk.pcm_sha256].forEach((value) => {
        const cell = document.createElement("span");
        cell.textContent = value;
        row.appendChild(cell);
      });
      table.appendChild(row);
    });
  }

  function progressFromJob(job) {
    if (!job) return 0;
    if (job.status === "completed") return 100;
    const progress = job.progress || {};
    if (progress.total) return Math.min(99, (Number(progress.index || 0) / Number(progress.total)) * 100);
    return job.status === "running" ? 8 : 0;
  }

  function updateJobUI(job) {
    state.job = job;
    const badge = $("job-state");
    badge.className = `job-state ${job?.status || "idle"}`;
    badge.textContent = (job?.status || "idle").toUpperCase();
    if (!job) {
      $("job-detail").textContent = "等待操作";
      $("job-progress").style.width = "0%";
      return;
    }
    if (job.status === "failed") {
      const error = job.error || {};
      $("job-detail").textContent = `${error.code || "ERROR"} · ${error.message || "job failed"}`;
    } else if (job.status === "stopped") {
      $("job-detail").textContent = `graceful stop · ${job.result?.stage || "block boundary"}`;
    } else if (job.status === "completed") {
      const total = job.result?.results?.length || 0;
      $("job-detail").textContent = `${job.mode} complete · ${total} shard(s)`;
    } else if (job.progress) {
      $("job-detail").textContent = `${job.progress.status || "running"} · ${job.progress.shard_id || ""}`;
    } else {
      $("job-detail").textContent = "job queued";
    }
    $("job-progress").style.width = `${progressFromJob(job)}%`;
  }

  async function pollJob(jobId) {
    clearTimeout(state.pollTimer);
    try {
      const payload = await api(`/api/jobs/${jobId}`);
      updateJobUI(payload.job);
      if (["completed", "failed", "stopped"].includes(payload.job.status)) {
        setBusy(false);
        if (payload.job.status === "failed") {
          log(`${payload.job.error.code}: ${payload.job.error.message}`, "error");
        } else if (payload.job.status === "stopped") {
          log("graceful stop completed at a frame-block boundary", "error");
        } else {
          log(`${payload.job.mode} job completed`, "ok");
          if (payload.job.mode === "stream") await loadChunksForAll();
        }
        await refreshHealth();
        await refreshState();
        return;
      }
      state.pollTimer = setTimeout(() => pollJob(jobId), 420);
    } catch (error) {
      setBusy(false);
      log(formatError(error), "error");
    }
  }

  async function startJob(mode, extra = {}) {
    const blockFrames = Math.max(1, Number($("block-frames").value || 4));
    const stopAfter = $("stop-after-blocks").value.trim();
    const payload = {
      mode,
      block_frames: blockFrames,
      verify_stream: true,
      ...extra,
    };
    if (mode !== "stream" && stopAfter !== "") payload.stop_after_blocks = Number(stopAfter);
    try {
      setBusy(true);
      log(`starting ${mode} · block=${blockFrames}${stopAfter ? ` · stop-after=${stopAfter}` : ""}`);
      const response = await api("/api/jobs", { method: "POST", body: payload });
      updateJobUI(response.job);
      pollJob(response.job.id);
    } catch (error) {
      setBusy(false);
      log(formatError(error), "error");
    }
  }

  function startConvert(shardIds) {
    startJob("convert", { shard_ids: shardIds || null });
  }

  async function loadChunksForAll() {
    const blockFrames = Math.max(1, Number($("block-frames").value || 4));
    const shards = state.workspace?.shards || [];
    await Promise.all(
      shards.map(async (item) => {
        const id = item.spec.shard_id;
        if (item.checkpoints.stage_status.validation !== "complete") return;
        try {
          const payload = await api(`/api/stream/${encodeURIComponent(id)}?block_frames=${blockFrames}`);
          state.chunks.set(id, payload.chunks);
        } catch (error) {
          log(`${id} stream: ${formatError(error)}`, "error");
        }
      })
    );
    renderChunks(state.selected);
  }

  async function runInjection() {
    const kind = $("injection-kind").value;
    if (!kind) {
      log("select an injection first", "error");
      return;
    }
    const shard = state.selected || state.workspace?.shards?.[0]?.spec.shard_id;
    if (!shard) return;
    try {
      setBusy(true);
      log(`injecting ${kind} into ${shard}`);
      const payload = await api("/api/inject", {
        method: "POST",
        body: {
          shard_id: shard,
          injection: injectionPayloads[kind],
          block_frames: Math.max(1, Number($("block-frames").value || 4)),
          verify_stream: false,
        },
      });
      log(`injection unexpectedly succeeded: ${JSON.stringify(payload).slice(0, 120)}`, "error");
    } catch (error) {
      const err = error.payload?.error;
      if (err) {
        log(`${err.code} · stage=${err.stage} · shard=${err.shard_id} · ${err.message}`, "error");
      } else {
        log(formatError(error), "error");
      }
    } finally {
      setBusy(false);
      await refreshState();
    }
  }

  async function stopJob() {
    if (!state.job || !["queued", "running"].includes(state.job.status)) {
      await api("/api/stop", { method: "POST", body: { reason: "operator clicked stop" } });
      log("stop marker written; next conversion will stop at a block boundary");
    } else {
      await api(`/api/jobs/${state.job.id}/stop`, { method: "POST", body: {} });
      log(`stop requested for ${state.job.id}`);
    }
    await refreshState();
  }

  async function downloadReport() {
    if (!state.selected) return;
    try {
      const payload = await api(`/api/report/${encodeURIComponent(state.selected)}`);
      const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = `${state.selected}.report.json`;
      link.click();
      URL.revokeObjectURL(url);
      log(`report downloaded for ${state.selected}`, "ok");
    } catch (error) {
      log(formatError(error), "error");
    }
  }

  function formatError(error) {
    const err = error.payload?.error;
    if (!err) return error.message || String(error);
    return `${err.code} · stage=${err.stage || "—"} · shard=${err.shard_id || "—"} · ${err.message}`;
  }

  function bindEvents() {
    $("btn-demo").addEventListener("click", async () => {
      try {
        setBusy(true);
        const result = await api("/api/demo", { method: "POST", body: {} });
        log(`demo ${result.status} · seed=${result.seed || "existing"} · shards=${result.shard_ids.length}`, "ok");
        await refreshState();
      } catch (error) {
        log(formatError(error), "error");
      } finally {
        setBusy(false);
      }
    });
    $("btn-convert").addEventListener("click", () => startConvert(null));
    $("btn-stream").addEventListener("click", () => startJob("stream", { shard_ids: state.selected ? [state.selected] : null }));
    $("btn-stop").addEventListener("click", () => stopJob().catch((error) => log(formatError(error), "error")));
    $("btn-resume").addEventListener("click", () => startJob("resume", { shard_ids: null }));
    $("btn-report").addEventListener("click", downloadReport);
    $("btn-inject").addEventListener("click", runInjection);
    $("btn-clear-log").addEventListener("click", () => $("event-log").replaceChildren());
    $("btn-inject").addEventListener("click", runInjection);
    $("frame-slider").addEventListener("input", (event) => {
      inspectFrame(Number(event.target.value));
    });
    $("latency-budget").addEventListener("change", () => {
      if (state.selected && state.diagnostics.has(state.selected)) inspectFrame(state.frameIndex, false);
    });
    window.addEventListener("resize", () => {
      if (state.workspace) {
        (state.workspace.shards || []).forEach((item) => {
          const canvas = document.querySelector(`[data-mini-for="${CSS.escape(item.spec.shard_id)}"]`);
          if (canvas) drawMiniChart(canvas, state.diagnostics.get(item.spec.shard_id));
        });
        renderF0Chart(state.selected ? state.diagnostics.get(state.selected) : null);
      }
    });
  }

  async function init() {
    bindEvents();
    await refreshHealth();
    await refreshState();
    log("workbench ready · deterministic DSP profile v1");
  }

  init();
})();
