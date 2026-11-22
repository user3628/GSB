"""Self-contained local dashboard HTML; no CDN, network assets, or build step."""

from __future__ import annotations


INDEX_HTML = r'''<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>SegVal · 实例分割校验工作台</title>
  <style>
    :root {
      --bg: #071018; --panel: #0e1a25; --panel2: #122334; --line: #223a4d;
      --text: #e8f1f7; --muted: #8ca4b6; --blue: #49b7ff; --green: #4bdaa0;
      --amber: #ffbd5c; --red: #ff6b72; --purple: #c49bff; --grid: #173044;
    }
    * { box-sizing: border-box; }
    body { margin: 0; background: radial-gradient(circle at 18% 0%, #102f46 0, var(--bg) 38%);
      color: var(--text); font: 14px/1.5 Inter, "Segoe UI", "Microsoft YaHei", sans-serif; }
    .shell { max-width: 1500px; margin: 0 auto; padding: 22px 24px 48px; }
    header { display: flex; align-items: flex-end; justify-content: space-between; gap: 20px; margin-bottom: 18px; }
    h1 { font-size: 24px; margin: 0; letter-spacing: .3px; }
    h2 { font-size: 15px; margin: 0 0 12px; color: #d8e8f2; }
    .sub { color: var(--muted); margin-top: 4px; }
    .badge { display: inline-flex; align-items: center; gap: 6px; border: 1px solid var(--line);
      background: #0b1721; border-radius: 999px; padding: 4px 10px; color: var(--muted); font-size: 12px; }
    .dot { width: 7px; height: 7px; border-radius: 50%; background: var(--green); box-shadow: 0 0 10px var(--green); }
    .grid { display: grid; gap: 14px; }
    .cards { grid-template-columns: repeat(5, minmax(0, 1fr)); margin-bottom: 14px; }
    .card, .panel { background: linear-gradient(160deg, rgba(20,42,59,.95), rgba(10,24,35,.96));
      border: 1px solid var(--line); border-radius: 14px; box-shadow: 0 12px 28px rgba(0,0,0,.18); }
    .card { padding: 15px 16px; min-height: 104px; }
    .card .label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .7px; }
    .card .value { font-size: 25px; font-weight: 650; margin-top: 8px; }
    .card .hint { color: var(--muted); font-size: 12px; margin-top: 4px; }
    .panel { padding: 16px; }
    .layout { display: grid; grid-template-columns: 1.25fr 1fr; gap: 14px; }
    .toolbar { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
    button, select, input { background: #0c1c29; color: var(--text); border: 1px solid var(--line);
      border-radius: 9px; padding: 8px 11px; font: inherit; outline: none; }
    button { cursor: pointer; transition: border-color .15s, background .15s; }
    button:hover { border-color: #3a688a; background: #11283a; }
    button.primary { background: #12416a; border-color: #2c82bc; color: #eaf8ff; }
    button.danger { border-color: #6b2d34; color: #ffb7bb; }
    button.good { border-color: #24634e; color: #a9f3d4; }
    button:disabled { opacity: .45; cursor: progress; }
    .field { display: inline-flex; gap: 6px; align-items: center; color: var(--muted); }
    .field input { width: 108px; }
    table { width: 100%; border-collapse: collapse; }
    th, td { text-align: left; padding: 8px 9px; border-bottom: 1px solid #1a3041; vertical-align: top; }
    th { color: var(--muted); font-size: 12px; font-weight: 600; }
    tr:last-child td { border-bottom: 0; }
    .mono { font-family: ui-monospace, SFMono-Regular, Consolas, monospace; font-size: 12px; }
    .pill { display: inline-block; border-radius: 999px; padding: 2px 8px; font-size: 11px; border: 1px solid; }
    .ok { color: var(--green); border-color: #286b50; background: #0b2b21; }
    .warn { color: var(--amber); border-color: #73552c; background: #2d2110; }
    .bad { color: var(--red); border-color: #72343a; background: #2c1115; }
    .neutral { color: var(--muted); border-color: var(--line); background: #0b1924; }
    .scroll { max-height: 330px; overflow: auto; }
    .banner { display: none; margin-bottom: 14px; border: 1px solid #74353a; background: #2b1115; color: #ffd6d8;
      padding: 11px 14px; border-radius: 11px; white-space: pre-wrap; }
    .banner.show { display: block; }
    .banner.good { border-color: #286b50; background: #0b2b21; color: #c8ffe9; }
    .two { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
    .stack { display: grid; gap: 14px; }
    canvas { width: 100%; height: 390px; display: block; background: #08121a; border: 1px solid var(--line); border-radius: 12px; }
    .legend { display: flex; flex-wrap: wrap; gap: 12px; margin-top: 10px; color: var(--muted); font-size: 12px; }
    .swatch { display: inline-block; width: 11px; height: 11px; border-radius: 3px; margin-right: 5px; vertical-align: -1px; }
    .kv { display: grid; grid-template-columns: 150px 1fr; gap: 7px 10px; }
    .kv .k { color: var(--muted); }
    .muted { color: var(--muted); }
    .tiny { font-size: 12px; }
    @media (max-width: 1050px) { .cards { grid-template-columns: repeat(2, 1fr); } .layout, .two { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
<div class="shell">
  <header>
    <div>
      <h1>SegVal · 实例分割校验与并行调度工作台</h1>
      <div class="sub">本地确定性分片 · 多边形解码 · 唯一 IoU 匹配 · 预算调度 · 原子检查点</div>
    </div>
    <div class="badge"><span class="dot"></span><span id="endpoint">127.0.0.1</span> · 仅本机回环</div>
  </header>
  <div id="banner" class="banner"></div>

  <section class="grid cards">
    <div class="card"><div class="label">状态</div><div class="value" id="k-status">—</div><div class="hint" id="k-version">config —</div></div>
    <div class="card"><div class="label">分片</div><div class="value" id="k-shards">—</div><div class="hint" id="k-shards-hint">—</div></div>
    <div class="card"><div class="label">命中 / 漏检</div><div class="value" id="k-hits">—</div><div class="hint" id="k-rates">—</div></div>
    <div class="card"><div class="label">重复 / 背景误报</div><div class="value" id="k-noise">—</div><div class="hint">高置信未匹配 / 低置信未匹配</div></div>
    <div class="card"><div class="label">平均 IoU</div><div class="value" id="k-iou">—</div><div class="hint" id="k-budget">预算 —</div></div>
  </section>

  <section class="panel" style="margin-bottom:14px">
    <div class="toolbar">
      <strong>控制</strong>
      <select id="run-stage">
        <option value="all">全部阶段</option><option value="decode">解码</option>
        <option value="match">匹配</option><option value="aggregate">聚合</option><option value="schedule">仅调度</option>
      </select>
      <label class="field">并发 <input id="run-workers" type="number" min="1" max="64" value="2"></label>
      <button class="primary" id="btn-run">运行</button>
      <button class="danger" id="btn-stop">请求停止</button>
      <button class="good" id="btn-resume">恢复</button>
      <button id="btn-verify">增量-全量校验</button>
      <button id="btn-seed">导入内置演示</button>
      <button id="btn-refresh">刷新</button>
      <span style="flex:1"></span>
      <label class="field">IoU <input id="cfg-iou" type="number" step="0.01" min="0" max="1"></label>
      <label class="field">总预算 <input id="cfg-budget" type="number" min="0"></label>
      <label class="field">单进程配额 <input id="cfg-quota" type="number" min="0"></label>
      <label class="field">OOB <select id="cfg-oob"><option value="clamp">clamp</option><option value="reject">reject</option></select></label>
      <button id="btn-config">应用配置</button>
    </div>
  </section>

  <div class="layout" style="margin-bottom:14px">
    <section class="panel">
      <h2>预算裁决与未处理原因</h2>
      <div class="scroll"><table><thead><tr><th>分片</th><th>序号</th><th>预估</th><th>Lane</th><th>状态 / 原因</th></tr></thead><tbody id="budget-body"></tbody></table></div>
    </section>
    <section class="panel">
      <h2>版本指纹与失效链</h2>
      <div class="kv tiny">
        <div class="k">config fingerprint</div><div class="mono" id="fp-config">—</div>
        <div class="k">schedule fingerprint</div><div class="mono" id="fp-schedule">—</div>
        <div class="k">schedule 状态</div><div id="fp-schedule-status">—</div>
        <div class="k">失效原因</div><div id="fp-schedule-reasons" class="muted">—</div>
      </div>
    </section>
  </div>

  <div class="two" style="margin-bottom:14px">
    <section class="panel">
      <h2>分片阶段状态</h2>
      <div class="scroll"><table><thead><tr><th>分片</th><th>密度/样本</th><th>解码</th><th>匹配</th><th>聚合</th><th>最早失效</th></tr></thead><tbody id="shard-body"></tbody></table></div>
    </section>
    <section class="panel">
      <h2>类别质量（聚合）</h2>
      <div class="scroll"><table><thead><tr><th>类别</th><th>命中率</th><th>重复率</th><th>背景误报率</th><th>平均 IoU</th><th>N/A</th></tr></thead><tbody id="class-body"></tbody></table></div>
    </section>
  </div>

  <section class="panel" style="margin-bottom:14px">
    <h2>样本多边形叠加诊断</h2>
    <div class="toolbar" style="margin-bottom:10px">
      <label class="field">分片 <select id="sample-shard"></select></label>
      <label class="field">图像 <select id="sample-image"></select></label>
      <span class="muted tiny" id="sample-meta">—</span>
    </div>
    <canvas id="canvas" width="1000" height="390"></canvas>
    <div class="legend">
      <span><i class="swatch" style="background:#4bdaa0"></i>GT 命中</span>
      <span><i class="swatch" style="background:#ff6b72"></i>GT 漏检</span>
      <span><i class="swatch" style="background:#49b7ff"></i>预测命中</span>
      <span><i class="swatch" style="background:#ffbd5c"></i>重复预测</span>
      <span><i class="swatch" style="background:#c49bff"></i>背景误报</span>
      <span><i class="swatch" style="background:#65798a"></i>无效实例</span>
    </div>
  </section>

  <div class="two">
    <section class="panel">
      <h2>来源算子质量</h2>
      <div class="scroll"><table><thead><tr><th>分片 / 类别 / 算子</th><th>命中</th><th>重复</th><th>背景误报</th><th>平均 IoU</th><th>N/A</th></tr></thead><tbody id="source-body"></tbody></table></div>
    </section>
    <section class="panel">
      <h2>运行与工作进程</h2>
      <div id="run-info" class="kv tiny"></div>
    </section>
  </div>
</div>

<script>
const $ = (id) => document.getElementById(id);
let state = {status: null, fingerprints: null, aggregates: null, samples: null};

function fmt(value, digits = 3) {
  if (value === null || value === undefined) return "N/A";
  if (typeof value === "number") return Number.isInteger(value) ? String(value) : value.toFixed(digits);
  return String(value);
}
function pct(value) { return value === null || value === undefined ? "N/A" : (value * 100).toFixed(1) + "%"; }
function shortHash(value) { return value ? value.slice(0, 12) + "…" : "—"; }
function pill(text, klass) { return `<span class="pill ${klass}">${text}</span>`; }
function stagePill(stage) {
  const s = stage.status;
  const klass = s === "VALID" ? "ok" : (s === "STALE" ? "warn" : "bad");
  return pill(s, klass);
}
function showBanner(text, good = false) {
  const el = $("banner");
  el.textContent = text;
  el.className = "banner show" + (good ? " good" : "");
}
function clearBanner() { $("banner").className = "banner"; }
async function api(path, options = {}) {
  const response = await fetch(path, {headers: {"Content-Type": "application/json"}, ...options});
  const type = response.headers.get("content-type") || "";
  const data = type.includes("json") ? await response.json() : await response.text();
  if (!response.ok) {
    const error = data && data.error ? data.error : {code: "HTTP_" + response.status, message: String(data)};
    const details = error.details ? "\n" + JSON.stringify(error.details, null, 2) : "";
    const exc = new Error(`${error.code}: ${error.message}${details}`);
    exc.code = error.code;
    throw exc;
  }
  return data;
}
async function refresh() {
  try {
    const [status, fingerprints, aggregates] = await Promise.all([
      api("/api/status"), api("/api/fingerprints"), api("/api/export?kind=aggregates")
    ]);
    state.status = status; state.fingerprints = fingerprints; state.aggregates = aggregates;
    render();
    await refreshSamples();
    if (state.samples && state.samples.selected && state.samples.selected.stale) {
      showBanner(state.samples.selected.warning);
    } else {
      clearBanner();
    }
  } catch (err) { showBanner(err.message); }
}
function render() {
  const s = state.status, fp = state.fingerprints, agg = state.aggregates.aggregates;
  const totals = agg.totals;
  $("k-status").textContent = s.status || "new";
  $("k-version").textContent = `config ${s.config_version} · ${shortHash(s.config_fingerprint)}`;
  const complete = s.shards.filter(x => x.all_valid).length;
  $("k-shards").textContent = `${complete}/${s.shards.length}`;
  $("k-shards-hint").textContent = `${s.budget.accepted_count} 已接纳 · ${s.budget.unprocessed_count} 未处理`;
  $("k-hits").textContent = `${totals.hits} / ${totals.misses}`;
  $("k-rates").textContent = `命中率 ${pct(totals.hit_rate)} · 漏检率 ${pct(totals.miss_rate)}`;
  $("k-noise").textContent = `${totals.duplicates} / ${totals.background_false_positives}`;
  $("k-iou").textContent = fmt(totals.average_iou);
  $("k-budget").textContent = `预算 ${s.budget.reserved_evaluations}/${s.budget.max_evaluations} · 实评 ${s.budget.actual_candidate_evaluations}`;
  if (s.config) {
    $("cfg-iou").value = s.config.matching.iou_threshold;
    $("cfg-budget").value = s.config.budget.max_evaluations;
    $("cfg-quota").value = s.config.budget.per_worker_quota;
    $("cfg-oob").value = s.config.decode.out_of_bounds_policy;
  }
  $("fp-config").textContent = s.config_fingerprint;
  $("fp-schedule").textContent = shortHash(s.schedule_fingerprint) + " · " + s.schedule_fingerprint;
  $("fp-schedule-status").innerHTML = stagePill({status: s.schedule_validation.status});
  $("fp-schedule-reasons").textContent = s.schedule_validation.invalid_reasons.join(", ") || "无";

  const budgetRows = [];
  for (const item of s.accepted || []) {
    budgetRows.push(`<tr><td>${item.shard_id}</td><td>${item.shard_index}</td><td>${item.estimated_evaluations}</td><td>${item.lane}</td><td>${pill("ACCEPTED", "ok")}</td></tr>`);
  }
  for (const item of s.unprocessed || []) {
    budgetRows.push(`<tr><td>${item.shard_id}</td><td>${item.shard_index}</td><td>${item.estimated_evaluations}</td><td>—</td><td>${pill(item.code, "bad")}<div class="tiny muted">${item.reason || ""}</div></td></tr>`);
  }
  $("budget-body").innerHTML = budgetRows.join("") || `<tr><td colspan="5" class="muted">没有分片</td></tr>`;

  const shardRows = [];
  for (const shard of s.shards) {
    const by = Object.fromEntries(shard.stages.map(x => [x.stage, x]));
    shardRows.push(`<tr>
      <td><div>${shard.shard_id}</div><div class="tiny muted mono">${shortHash(shard.stages[0].expected_input_fingerprint)}</div></td>
      <td><div>${shard.sample_count} 样本</div><div class="tiny muted">${shard.prediction_count} 预测</div></td>
      <td>${stagePill(by.decode)}</td><td>${stagePill(by.match)}</td><td>${stagePill(by.aggregate)}</td>
      <td>${shard.earliest_invalid_stage ? pill(shard.earliest_invalid_stage, "warn") : pill("none", "ok")}<div class="tiny muted">${JSON.stringify(shard.invalidation.reasons).slice(0, 100)}</div></td>
    </tr>`);
  }
  $("shard-body").innerHTML = shardRows.join("") || `<tr><td colspan="6" class="muted">没有分片</td></tr>`;

  const classRows = (agg.by_class || []).map(row => `<tr>
    <td>${row.class_name}</td><td>${pct(row.hit_rate)}</td><td>${pct(row.duplicate_rate)}</td>
    <td>${pct(row.background_false_positive_rate)}</td><td>${fmt(row.average_iou)}</td><td>${row.na_metric_count}</td>
  </tr>`);
  $("class-body").innerHTML = classRows.join("") || `<tr><td colspan="6" class="muted">尚无匹配结论</td></tr>`;

  const sourceRows = (agg.by_shard_class_source || []).slice(0, 80).map(row => `<tr>
    <td><span class="mono">${row.shard_id}</span><div class="tiny muted">${row.class_name} / ${row.source_operator}</div></td>
    <td>${row.hits}</td><td>${row.duplicates}</td><td>${row.background_false_positives}</td>
    <td>${fmt(row.average_iou)}</td><td>${row.na_metric_count}</td>
  </tr>`);
  $("source-body").innerHTML = sourceRows.join("") || `<tr><td colspan="6" class="muted">尚无来源算子结论</td></tr>`;

  const last = s.last_run || {};
  $("run-info").innerHTML = `
    <div class="k">last target</div><div>${last.target_stage || "—"}</div>
    <div class="k">workers</div><div>${last.execution_workers ?? "—"} (planned ${s.planned_concurrency})</div>
    <div class="k">reserved / actual</div><div>${s.budget.reserved_evaluations} / ${s.budget.actual_candidate_evaluations}</div>
    <div class="k">checkpoint journal</div><div>${s.checkpoint_status.journal_count} records · max seq ${s.checkpoint_status.max_sequence}</div>
    <div class="k">stop</div><div>${s.stop ? JSON.stringify(s.stop) : "not requested"}</div>
    <div class="k">worker errors</div><div>${(last.errors || []).length ? "<span class='bad'>" + (last.errors || []).map(e => e.code).join(", ") + "</span>" : "none"}</div>`;
  if (state.fingerprints) {
    const cfg = state.fingerprints;
    $("endpoint").textContent = new URL(location.href).host;
  }
}
async function refreshSamples() {
  const shardSelect = $("sample-shard");
  const previousShard = shardSelect.value;
  const shardId = previousShard || (state.status.shards[0] && state.status.shards[0].shard_id) || "";
  const data = await api("/api/samples" + (shardId ? "?shard_id=" + encodeURIComponent(shardId) : ""));
  state.samples = data;
  shardSelect.innerHTML = (data.shards || []).map(s => `<option value="${s.shard_id}">${s.shard_id}</option>`).join("");
  if (data.selected) {
    shardSelect.value = data.selected.shard_id;
    const imageSelect = $("sample-image");
    const currentImages = (data.shards.find(s => s.shard_id === data.selected.shard_id) || {}).images || [];
    imageSelect.innerHTML = currentImages.map(img => `<option value="${img}">${img}</option>`).join("");
    imageSelect.value = data.selected.image_id;
    drawSelected();
  } else {
    $("sample-image").innerHTML = "";
    $("sample-meta").textContent = data.message || "尚无样本";
    const canvas = $("canvas"); canvas.getContext("2d").clearRect(0, 0, canvas.width, canvas.height);
  }
}
function drawSelected() {
  const data = state.samples;
  if (!data || !data.selected) return;
  const {sample, matches} = data.selected;
  $("sample-meta").textContent = `${sample.width}×${sample.height} · ${sample.ground_truth.length} GT · ${sample.predictions.length} 预测 · ${matches.length} 类别明细${data.selected.stale ? " · STALE 旧检查点" : ""}`;
  const canvas = $("canvas"); const rect = canvas.getBoundingClientRect();
  canvas.width = Math.max(600, Math.floor(rect.width * devicePixelRatio));
  canvas.height = Math.floor(390 * devicePixelRatio);
  const ctx = canvas.getContext("2d");
  ctx.scale(devicePixelRatio, devicePixelRatio);
  const W = canvas.width / devicePixelRatio, H = canvas.height / devicePixelRatio;
  const scale = Math.min((W - 30) / sample.width, (H - 30) / sample.height);
  const ox = (W - sample.width * scale) / 2, oy = (H - sample.height * scale) / 2;
  ctx.fillStyle = "#08121a"; ctx.fillRect(0, 0, W, H);
  ctx.strokeStyle = "#173044"; ctx.lineWidth = 1;
  for (let x = 0; x <= sample.width; x += 80) { ctx.beginPath(); ctx.moveTo(ox+x*scale, oy); ctx.lineTo(ox+x*scale, oy+sample.height*scale); ctx.stroke(); }
  for (let y = 0; y <= sample.height; y += 80) { ctx.beginPath(); ctx.moveTo(ox, oy+y*scale); ctx.lineTo(ox+sample.width*scale, oy+y*scale); ctx.stroke(); }
  ctx.strokeStyle = "#3c6078"; ctx.strokeRect(ox, oy, sample.width * scale, sample.height * scale);
  const path = (poly) => {
    ctx.beginPath();
    poly.forEach(([x, y], i) => { const px=ox+x*scale, py=oy+y*scale; i ? ctx.lineTo(px,py) : ctx.moveTo(px,py); });
    ctx.closePath();
  };
  for (const gt of sample.ground_truth) {
    const color = gt.match_status === "hit" ? "#4bdaa0" : (gt.match_status === "miss" ? "#ff6b72" : "#65798a");
    path(gt.vertices_px); ctx.fillStyle = color + "12"; ctx.fill();
    ctx.strokeStyle = color; ctx.lineWidth = 2; ctx.setLineDash([5,4]); ctx.stroke(); ctx.setLineDash([]);
    const [x0,y0] = gt.vertices_px[0] || [0,0];
    ctx.fillStyle = color; ctx.font = "11px monospace"; ctx.fillText("GT " + gt.class_name, ox+x0*scale+3, oy+y0*scale-4);
  }
  const colors = {hit:"#49b7ff", duplicate:"#ffbd5c", background_false_positive:"#c49bff", invalid:"#65798a", unmatched:"#ff6b72"};
  for (const pred of sample.predictions) {
    const color = colors[pred.match_status] || "#ff6b72";
    path(pred.vertices_px); ctx.fillStyle = color + "10"; ctx.fill();
    ctx.strokeStyle = color; ctx.lineWidth = pred.match_status === "hit" ? 2 : 1.5;
    ctx.setLineDash(pred.match_status === "hit" ? [] : [2,3]); ctx.stroke(); ctx.setLineDash([]);
    if (pred.vertices_px.length) {
      const [x0,y0] = pred.vertices_px[0];
      ctx.fillStyle = color; ctx.font = "10px monospace";
      ctx.fillText(`${pred.class_name} ${(pred.confidence ?? 0).toFixed(2)} ${pred.match_status}`, ox+x0*scale+3, oy+y0*scale+11);
    }
  }
}
async function clickRun(button, path, body) {
  button.disabled = true; clearBanner();
  try {
    showBanner("运行中… 本地进程池执行，请稍候。", true);
    const result = await api(path, {method: "POST", body: JSON.stringify(body)});
    showBanner("运行完成：" + JSON.stringify({ok: result.ok, stopped: result.stopped, errors: (result.errors || []).map(e => e.code), unprocessed: (result.unprocessed || []).map(x => x.code)}), !result.errors || result.errors.length === 0);
    await refresh();
  } catch (err) { showBanner(err.message); }
  finally { button.disabled = false; }
}
$("btn-run").addEventListener("click", () => clickRun($("btn-run"), "/api/run", {
  stage: $("run-stage").value, workers: Number($("run-workers").value || 2), clear_stop: true
}));
$("btn-stop").addEventListener("click", async () => {
  try { const result = await api("/api/stop", {method: "POST", body: JSON.stringify({reason: "dashboard_stop"})}); showBanner("已发送协作式停止请求；当前样本完成后退出。", true); await refresh(); }
  catch (err) { showBanner(err.message); }
});
$("btn-resume").addEventListener("click", () => clickRun($("btn-resume"), "/api/resume", {workers: Number($("run-workers").value || 2)}));
$("btn-verify").addEventListener("click", () => clickRun($("btn-verify"), "/api/verify-incremental", {}));
$("btn-seed").addEventListener("click", () => clickRun($("btn-seed"), "/api/init-demo", {shard_count: 4}));
$("btn-refresh").addEventListener("click", refresh);
$("btn-config").addEventListener("click", async () => {
  const patch = {
    matching: {iou_threshold: Number($("cfg-iou").value)},
    decode: {out_of_bounds_policy: $("cfg-oob").value},
    budget: {max_evaluations: Number($("cfg-budget").value), per_worker_quota: Number($("cfg-quota").value)}
  };
  try { const result = await api("/api/config", {method: "POST", body: JSON.stringify(patch)}); showBanner("配置已更新；请运行以重建失效链。", true); await refresh(); }
  catch (err) { showBanner(err.message); }
});
$("sample-shard").addEventListener("change", async () => { try { await refreshSamples(); } catch (err) { showBanner(err.message); } });
$("sample-image").addEventListener("change", async () => {
  try {
    const shardId = $("sample-shard").value, imageId = $("sample-image").value;
    state.samples = await api(`/api/samples?shard_id=${encodeURIComponent(shardId)}&image_id=${encodeURIComponent(imageId)}`);
    drawSelected();
  } catch (err) { showBanner(err.message); }
});
window.addEventListener("resize", () => { if (state.samples && state.samples.selected) drawSelected(); });
refresh();
</script>
</body>
</html>'''

