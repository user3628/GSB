const state = {
  status: null,
  diagnostics: null,
  lastReport: null,
  direction: "text_to_image",
  busy: false,
};

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function shortHash(value, length = 12) {
  return value ? String(value).slice(0, length) : "—";
}

function fmt(value, digits = 6) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  const number = Number(value);
  if (number === 0) return "0";
  if (Math.abs(number) >= 1000 || Math.abs(number) < 0.001) return number.toExponential(3);
  return Number(number.toFixed(digits)).toString();
}

function toast(message, kind = "info") {
  const node = document.createElement("div");
  node.className = `toast ${kind}`;
  node.innerHTML = message;
  $("#toast-stack").append(node);
  setTimeout(() => node.remove(), 5200);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  let payload;
  try {
    payload = await response.json();
  } catch {
    throw new Error(`HTTP ${response.status}: invalid JSON response`);
  }
  if (!response.ok || payload.ok === false) {
    const error = payload.error || {};
    throw Object.assign(new Error(error.message || `HTTP ${response.status}`), {
      code: error.code,
      stage: error.stage,
      itemId: error.item_id,
      details: error.details,
    });
  }
  return payload;
}

function setBusy(busy, label = "构建中") {
  state.busy = busy;
  $$("button").forEach((button) => {
    if (button.id !== "btn-stop") button.disabled = busy;
  });
  $("#build-state").textContent = busy ? label : "待机";
  $("#health-text").textContent = busy ? "任务执行中" : "本地服务在线";
}

function showError(error) {
  const pieces = [`<strong>${escapeHtml(error.code || "ERROR")}</strong> · ${escapeHtml(error.message)}`];
  if (error.stage) pieces.push(`<br><code>stage=${escapeHtml(error.stage)}</code>`);
  if (error.itemId) pieces.push(`<code>item_id=${escapeHtml(error.itemId)}</code>`);
  if (error.details && Object.keys(error.details).length) {
    pieces.push(`<br><code>${escapeHtml(JSON.stringify(error.details))}</code>`);
  }
  toast(pieces.join(" "), "error");
  $("#health-dot").className = "pulse error";
}

async function loadHealth() {
  try {
    const health = await api("/api/health");
    $("#health-dot").className = "pulse ok";
    $("#health-text").textContent = "本地服务在线";
    $("#health-text").title = `ShardSeek ${health.version}`;
  } catch (error) {
    $("#health-dot").className = "pulse error";
    $("#health-text").textContent = "服务不可用";
  }
}

async function loadStatus() {
  const status = await api("/api/status");
  state.status = status;
  $("#py-version").textContent = status.python.version;
  $("#np-version").textContent = status.python.numpy;
  $("#pil-version").textContent = status.python.pillow;
  const config = status.config;
  $$("#config-switch button").forEach((button) => {
    button.classList.toggle("active", button.dataset.config === (config.name?.includes("4bit") ? "compact_4bit" : "baseline_8bit"));
  });
  $("#fp-config").textContent = shortHash(config.fingerprint);
  $("#fp-config").title = config.fingerprint;
  $("#fp-projection").textContent = shortHash(config.projection_quantization_fingerprint);
  $("#fp-projection").title = config.projection_quantization_fingerprint;
  $("#fp-representation").textContent = shortHash(status.library.snapshot?.representation_fingerprint);
  $("#fp-representation").title = status.library.snapshot?.representation_fingerprint || "";
  $("#fp-index").textContent = shortHash(status.library.snapshot?.index_fingerprint);
  $("#fp-index").title = status.library.snapshot?.index_fingerprint || "";
  $("#metric-items").textContent = String(status.import.enabled_count);
  $("#metric-rejected").textContent = `${status.import.rejected.length} quarantined`;
  $("#metric-snapshot").textContent = status.library.snapshot?.snapshot_sequence ?? "—";
  $("#metric-bits").textContent = `${config.bits}-bit · ${config.granularity}`;
  $("#metric-ready").textContent = status.library.ready ? "Ready" : "No";
  $("#metric-ready").style.color = status.library.ready ? "var(--accent)" : "var(--warm)";
  const cp = status.checkpoints;
  $("#metric-checkpoints").textContent = `${cp.record_count}/${cp.expected_count || 0}`;
  $("#metric-checkpoint-state").textContent = cp.completed ? "session complete" : cp.session_id ? "session open" : "not started";
  $("#metric-session").textContent = cp.session_id ? shortHash(cp.session_id) : "no active session";
  $("#checkpoint-tag").textContent = `seq ${cp.last_seq || 0}`;
  const progress = cp.expected_count ? (100 * cp.record_count) / cp.expected_count : 0;
  $("#build-fill").style.width = `${progress}%`;
  $("#build-fraction").textContent = `${cp.record_count}/${cp.expected_count || 0}`;
  $("#manifest-tag").textContent = `manifest ${shortHash(status.import.manifest_sha256)}`;
  renderCorpus(status.import.items);
  renderCheckpoints(cp);
  if (status.stop_requested) $("#build-message").textContent = "已收到停止请求；将在当前分片完成后退出。";
  return status;
}

function renderCorpus(items) {
  const enabled = (items || []).filter((item) => item.enabled);
  if (!enabled.length) {
    $("#corpus-grid").innerHTML = '<div class="empty">导入样本后显示分片。</div>';
    $("#query-image").innerHTML = "";
    return;
  }
  $("#corpus-grid").innerHTML = enabled.map((item) => `
    <article class="corpus-card">
      <img src="/api/image/${encodeURIComponent(item.item_id)}" alt="${escapeHtml(item.item_id)}">
      <div class="corpus-meta">
        <strong>${escapeHtml(item.item_id)} <span>${escapeHtml(item.source)}</span></strong>
        <p>${escapeHtml(item.caption)}</p>
      </div>
    </article>
  `).join("");
  const current = $("#query-image").value;
  $("#query-image").innerHTML = enabled.map((item) =>
    `<option value="${escapeHtml(item.item_id)}">${escapeHtml(item.item_id)} · ${escapeHtml(item.source)}</option>`
  ).join("");
  if (enabled.some((item) => item.item_id === current)) $("#query-image").value = current;
}

function renderCheckpoints(cp) {
  const stages = ["feature_extract", "project_quantize", "index_build", "recall_verify"];
  const counts = Object.fromEntries(stages.map((stage) => [stage, 0]));
  if (cp.last_record) counts[cp.last_record.stage] = 1;
  $$("#stage-list .stage-row").forEach((row, index) => {
    const stage = stages[index];
    const complete = cp.completed || (cp.next_expected && cp.next_expected.stage_index > index + 1);
    row.classList.toggle("done", Boolean(complete));
    row.querySelector("b").textContent = complete ? "done" : cp.next_expected?.stage === stage ? "next" : "—";
  });
  if (!cp.session_id) {
    $("#reuse-list").innerHTML = "";
  }
}

async function loadDiagnostics() {
  try {
    const diagnostics = await api("/api/diagnostics");
    state.diagnostics = diagnostics;
    const series = diagnostics.series || [];
    if (!series.length) {
      $("#diagnostics-chart").innerHTML = '<div class="empty">尚无诊断数据。</div>';
      return;
    }
    const max = Math.max(...series.map((row) => row.max_similarity_error_bound), 1e-12);
    $("#diagnostics-chart").innerHTML = series.map((row) => `
      <div class="diag-row">
        <span>n=${row.shard_count}</span>
        <div class="diag-track"><div class="diag-fill" style="width:${Math.max(2, 100 * row.max_similarity_error_bound / max)}%"></div></div>
        <code>${fmt(row.max_similarity_error_bound, 5)}</code>
      </div>
    `).join("");
    const last = series[series.length - 1];
    $("#diagnostic-foot").innerHTML = `${diagnostics.bits}-bit ${escapeHtml(diagnostics.granularity)} · ${last.pair_count} pairwise bounds · min effective digits <strong>${last.min_effective_digits}</strong>`;
  } catch (error) {
    if (error.code === "E_INDEX_NOT_READY") {
      $("#diagnostics-chart").innerHTML = '<div class="empty">构建索引后显示误差曲线。</div>';
      $("#diagnostic-foot").textContent = "等待有效快照。";
    } else {
      showError(error);
    }
  }
}

function renderReport(report) {
  state.lastReport = report;
  const results = report.results || [];
  if (!results.length) {
    $("#results").innerHTML = '<div class="empty">没有结果。</div>';
    return;
  }
  $("#weight-dense").textContent = fmt(report.weights.dense_weight, 2);
  $("#weight-inverted").textContent = fmt(report.weights.inverted_weight, 2);
  $("#results").innerHTML = `
    <div class="search-meta">
      <span>report <code>${shortHash(report.report_sha256)}</code></span>
      <span>index <code>${shortHash(report.index_fingerprint)}</code></span>
      <span>max bound <code>${fmt(report.diagnostics_summary.max_cosine_error_bound, 6)}</code></span>
      <span>min digits <code>${report.diagnostics_summary.min_effective_digits}</code></span>
      <button class="btn ghost small" id="btn-export-report">导出检索报告</button>
    </div>
    ${results.map((result) => {
      const visualWidth = Math.min(100, 100 * result.log_contributions.visual / Math.max(result.log_contributions.visual, result.log_contributions.text, 0.001));
      const textWidth = Math.min(100, 100 * result.log_contributions.text / Math.max(result.log_contributions.visual, result.log_contributions.text, 0.001));
      return `
        <article class="result-card">
          <div class="result-top">
            <div class="rank">${result.rank}</div>
            <div>
              <div class="result-title"><strong>${escapeHtml(result.item_id)}</strong><span>${escapeHtml(result.source_shard)}</span></div>
              <div class="token-list">${(result.inverted.matched_tokens || []).slice(0, 12).map((token) => `<span class="token">${escapeHtml(token)}</span>`).join("")}</div>
            </div>
            <div class="score"><strong>${fmt(result.combined_score, 9)}</strong><small>combined score</small></div>
          </div>
          <div class="result-grid">
            <div class="result-stat"><span>dense cos</span><b>${fmt(result.dense_cosine, 8)}</b></div>
            <div class="result-stat"><span>reference</span><b>${fmt(result.reference_cosine, 8)}</b></div>
            <div class="result-stat"><span>error bound</span><b>${fmt(result.cosine_error_bound, 8)}</b></div>
            <div class="result-stat"><span>effective digits</span><b>${result.effective_digits}</b></div>
            <div class="result-stat"><span>inverted hit</span><b>${fmt(result.inverted.score, 8)}</b></div>
            <div class="result-stat"><span>within bound</span><b style="color:${result.within_declared_bound ? "var(--accent)" : "var(--danger)"}">${result.within_declared_bound ? "yes" : "no"}</b></div>
          </div>
          <div class="contribution-row"><span>vision log</span><div class="contribution-track"><div class="contribution-fill" style="width:${visualWidth}%"></div></div><code>${fmt(result.log_contributions.visual, 6)}</code></div>
          <div class="contribution-row"><span>text log</span><div class="contribution-track"><div class="contribution-fill" style="width:${textWidth}%"></div></div><code>${fmt(result.log_contributions.text, 6)}</code></div>
        </article>`;
    }).join("")}
  `;
}

async function runSearch() {
  if (state.busy) return;
  const topK = Number($("#top-k").value || 5);
  const body = { direction: state.direction, top_k: topK };
  if (state.direction === "text_to_image") body.query_text = $("#query-text").value;
  else body.image_item_id = $("#query-image").value;
  setBusy(true, "检索中");
  try {
    const report = await api("/api/search", { method: "POST", body: JSON.stringify(body) });
    renderReport(report);
    toast(`检索完成 · <code>${shortHash(report.report_sha256)}</code>`, "success");
  } catch (error) {
    showError(error);
  } finally {
    setBusy(false);
    loadStatus().catch(showError);
  }
}

async function runBuild(mode) {
  if (state.busy) return;
  setBusy(true, mode === "full" ? "全量构建" : "增量构建");
  try {
    const result = await api("/api/build", {
      method: "POST",
      body: JSON.stringify({ mode }),
    });
    if (result.stopped) {
      toast(`构建已优雅停止：${result.completed_shards}/${result.total_shards} 分片。`, "info");
    } else if (result.reused) {
      toast("输入与配置未变化，全部阶段复用。", "success");
    } else {
      toast(`构建完成 · earliest <code>${escapeHtml(result.earliest_stage)}</code>`, "success");
    }
    $("#build-message").textContent = result.stopped ? "可从当前检查点恢复。" : "快照已原子发布。";
  } catch (error) {
    showError(error);
  } finally {
    setBusy(false);
    await loadStatus().catch(showError);
    await loadDiagnostics();
  }
}

async function runResume() {
  if (state.busy) return;
  setBusy(true, "恢复构建");
  try {
    const result = await api("/api/resume", { method: "POST", body: "{}" });
    toast(result.stopped ? "仍未完成，已在检查点停止。" : "恢复完成，快照已验证。", result.stopped ? "info" : "success");
  } catch (error) {
    showError(error);
  } finally {
    setBusy(false);
    await loadStatus().catch(showError);
    await loadDiagnostics();
  }
}

async function requestStop() {
  try {
    await api("/api/stop", { method: "POST", body: "{}" });
    $("#build-message").textContent = "停止请求已写入；当前分片完成后退出。";
    toast("停止请求已记录。", "info");
  } catch (error) {
    showError(error);
  }
}

async function resetWorkspace() {
  if (state.busy) return;
  setBusy(true, "重置工作区");
  try {
    await api("/api/reset", {
      method: "POST",
      body: JSON.stringify({ manifest_path: "samples/manifest.jsonl", config: "baseline_8bit" }),
    });
    state.lastReport = null;
    $("#results").innerHTML = '<div class="empty">尚无检索结果。</div>';
    toast("工作区已清空并重新导入 8 个样本分片。", "success");
  } catch (error) {
    showError(error);
  } finally {
    setBusy(false);
    await loadStatus().catch(showError);
    await loadDiagnostics();
  }
}

async function verifyIncremental() {
  if (state.busy) return;
  setBusy(true, "一致性校验");
  try {
    const result = await api("/api/verify-incremental", { method: "POST", body: "{}" });
    toast(`增量与全量逐字节一致 · <code>${shortHash(result.verification_sha256)}</code>`, "success");
  } catch (error) {
    showError(error);
  } finally {
    setBusy(false);
    await loadStatus().catch(showError);
  }
}

function downloadJson(filename, payload) {
  const blob = new Blob([JSON.stringify(payload, null, 2) + "\n"], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

function bindEvents() {
  $("#btn-search").addEventListener("click", runSearch);
  $("#query-text").addEventListener("keydown", (event) => { if (event.key === "Enter") runSearch(); });
  $("#btn-reset").addEventListener("click", resetWorkspace);
  $("#btn-full").addEventListener("click", () => runBuild("full"));
  $("#btn-incremental").addEventListener("click", () => runBuild("incremental"));
  $("#btn-stop").addEventListener("click", requestStop);
  $("#btn-resume").addEventListener("click", runResume);
  $("#btn-verify").addEventListener("click", verifyIncremental);
  $("#btn-diagnostics").addEventListener("click", loadDiagnostics);
  $$("#direction-switch button").forEach((button) => button.addEventListener("click", () => {
    state.direction = button.dataset.direction;
    $$("#direction-switch button").forEach((node) => node.classList.toggle("active", node === button));
    $("#text-query-wrap").classList.toggle("hidden", state.direction !== "text_to_image");
    $("#image-query-wrap").classList.toggle("hidden", state.direction !== "image_to_text");
  }));
  $$("#config-switch button").forEach((button) => button.addEventListener("click", async () => {
    if (state.busy) return;
    try {
      await api("/api/config", { method: "POST", body: JSON.stringify({ config: button.dataset.config }) });
      toast(`已选择 ${button.dataset.config}；请执行全量或增量构建。`, "info");
      await loadStatus();
      await loadDiagnostics();
    } catch (error) {
      showError(error);
    }
  }));
  document.addEventListener("click", (event) => {
    if (event.target.id === "btn-export-report" && state.lastReport) {
      downloadJson(`shardseek-search-${state.lastReport.report_sha256.slice(0, 12)}.json`, state.lastReport);
    }
    if (event.target.id === "btn-export-diag" && state.diagnostics) {
      downloadJson(`shardseek-diagnostics-${state.diagnostics.diagnostics_sha256.slice(0, 12)}.json`, state.diagnostics);
    }
  });
}

async function boot() {
  bindEvents();
  await loadHealth();
  try {
    await loadStatus();
    await loadDiagnostics();
  } catch (error) {
    showError(error);
  }
}

boot();

