const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => Array.from(document.querySelectorAll(selector));

const state = {
  samples: [],
  currentRun: null,
  selectedBatchUid: null,
  invalidPayload: null,
};

function esc(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function statusClass(value) {
  const text = String(value || "UNKNOWN");
  return ["VERIFIED", "COMMITTED"].includes(text) ? "verified"
    : text.includes("FAILED") ? "failed"
    : "aborted";
}

function reasonClass(reason) {
  if (reason.includes("MEMORY") || reason.includes("FRAGMENT")) return "memory";
  if (reason.includes("LATENCY")) return "latency";
  return "";
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  let body;
  try {
    body = await response.json();
  } catch {
    throw { code: `HTTP_${response.status}`, message: "响应不是有效 JSON" };
  }
  if (!response.ok || !body.ok) {
    throw body.error || { code: `HTTP_${response.status}`, message: "请求失败" };
  }
  return body.data;
}

function showError(error) {
  const banner = $("#errorBanner");
  $("#errorCode").textContent = error.code || "E_UNKNOWN";
  const detail = error.details && Object.keys(error.details).length
    ? ` · ${JSON.stringify(error.details).slice(0, 280)}`
    : "";
  $("#errorMessage").textContent = `${error.message || "操作失败"}${detail}`;
  banner.classList.remove("hidden");
}

function clearError() {
  $("#errorBanner").classList.add("hidden");
}

function setHealth(ok, text) {
  $("#healthDot").className = `dot ${ok ? "dot-ok" : "dot-bad"}`;
  $("#healthText").textContent = text;
}

function shortDigest(value) {
  return value ? `${String(value).slice(0, 16)}…` : "—";
}

async function init() {
  $("#dismissError").addEventListener("click", clearError);
  $("#refreshRuns").addEventListener("click", refreshRuns);
  $("#refreshRecovery").addEventListener("click", refreshRecoveryAndIntents);
  $("#submitInvalid").addEventListener("click", submitInvalidPayload);
  setInterval(() => {
    $("#clock").textContent = new Date().toLocaleString("zh-CN", { hour12: false });
  }, 1000);

  try {
    const [health, samples] = await Promise.all([
      api("/api/health"),
      api("/api/samples"),
    ]);
    state.samples = samples.samples;
    setHealth(true, `本地服务正常 · ${health.intent_stats.COMMITTED || 0} committed intents`);
    renderSamples();
  } catch (error) {
    setHealth(false, "本地服务不可用");
    showError(error);
  }
  const runs = await refreshRuns();
  await refreshRecoveryAndIntents();
  if (!state.currentRun && runs.length) {
    const preferred = runs.find((run) => run.state === "COMMITTED")
      || runs.find((run) => run.state === "VALIDATION_FAILED")
      || runs[0];
    try {
      const run = await api(`/api/runs/${encodeURIComponent(preferred.run_id)}`);
      state.currentRun = run;
      renderRun(run);
    } catch (error) {
      showError(error);
    }
  }
}

function renderSamples() {
  const root = $("#sampleButtons");
  root.innerHTML = state.samples.map((sample) => `
    <button class="sample-button" data-sample="${esc(sample.name)}" type="button">
      <strong>${esc(sample.name)}</strong>
      <span>${esc(sample.description)}</span>
      <span class="sample-kind">${esc(sample.kind)} · ${esc(sample.file)}</span>
    </button>
  `).join("");
  $$(".sample-button").forEach((button) => {
    button.addEventListener("click", () => chooseSample(button.dataset.sample));
  });
}

async function chooseSample(name) {
  clearError();
  const sample = state.samples.find((item) => item.name === name);
  if (!sample) return;
  if (sample.kind === "invalid") {
    await loadAnomalySample(name);
    return;
  }
  await runSample(name);
}

async function runSample(name) {
  const button = $(`.sample-button[data-sample="${CSS.escape(name)}"]`);
  if (button) button.disabled = true;
  $("#runTitle").textContent = `正在组织 ${name}…`;
  $("#runSubtitle").textContent = "固定样本正在本地执行确定性调度、分位计算与真值核对。";
  try {
    const data = await api(`/api/demo/run/${encodeURIComponent(name)}`, {
      method: "POST",
      body: "{}",
    });
    state.currentRun = data.run;
    state.selectedBatchUid = null;
    renderRun(data.run);
    setHealth(true, `运行完成 · ${data.run.state}`);
  } catch (error) {
    showError(error);
    if (error.code === "E_PRECISION_DRIFT" && error.details?.run_id) {
      try {
        const run = await api(`/api/runs/${encodeURIComponent(error.details.run_id)}`);
        state.currentRun = run;
        renderRun(run);
      } catch (secondary) {
        showError(secondary);
      }
    }
  } finally {
    if (button) button.disabled = false;
    await Promise.all([refreshRuns(), refreshRecoveryAndIntents()]);
  }
}

async function refreshRuns() {
  try {
    const data = await api("/api/runs?limit=12");
    const root = $("#recentRuns");
    if (!data.runs.length) {
      root.className = "run-list empty";
      root.textContent = "尚无运行";
      return [];
    }
    root.className = "run-list";
    root.innerHTML = data.runs.map((run) => `
      <button type="button" class="run-button ${run.state === "VALIDATION_FAILED" ? "failed" : run.state === "ABORTED" || run.state.includes("PENDING") ? "pending" : ""}" data-run="${esc(run.run_id)}">
        <strong>${esc(run.trace_id)}</strong>
        <span>${esc(run.run_id.slice(0, 20))}… · ${esc(run.created_at || "")}</span>
        <span class="run-state">${esc(run.state)}${run.error_code ? ` · ${esc(run.error_code)}` : ""}</span>
      </button>
    `).join("");
    $$(".run-button").forEach((button) => {
      button.addEventListener("click", async () => {
        clearError();
        try {
          const run = await api(`/api/runs/${encodeURIComponent(button.dataset.run)}`);
          state.currentRun = run;
          state.selectedBatchUid = null;
          renderRun(run);
        } catch (error) {
          showError(error);
        }
      });
    });
    return data.runs;
  } catch (error) {
    showError(error);
    return [];
  }
}

function renderRun(run) {
  $("#runTitle").textContent = run.trace_id;
  $("#runSubtitle").textContent = `${run.algorithm_version} · ${run.state}${run.error_code ? ` · ${run.error_code}` : ""} · 运行 ${run.run_id}`;
  $("#statState").textContent = run.state;
  $("#statBatches").textContent = String(run.batches.length);
  const p99 = (run.quantiles || []).find((item) => item.label === "P99");
  $("#statP99").textContent = p99 ? `${p99.latency_ms}ms` : "N/A";
  const allocated = run.batches.reduce((total, batch) => total + Number(batch.memory_bytes || 0), 0);
  $("#statMemory").textContent = humanBytes(allocated);
  $("#digestLabel").textContent = `digest ${shortDigest(run.deterministic_digest || run.schedule_digest)}`;
  renderTimeline(run);
  renderMemory(run);
  renderBatchTable(run);
  renderFragments(run);
  if (run.batches.length) {
    selectBatch(run.batches[0].batch_uid);
  } else {
    $("#batchDetail").classList.add("hidden");
  }
}

function humanBytes(value) {
  const number = Number(value) || 0;
  const units = [["TiB", 1024 ** 4], ["GiB", 1024 ** 3], ["MiB", 1024 ** 2], ["KiB", 1024]];
  for (const [label, size] of units) {
    if (number >= size) {
      const scaled = number / size;
      return `${scaled >= 100 ? scaled.toFixed(0) : scaled.toFixed(1)}${label}`;
    }
  }
  return `${number}B`;
}

function renderTimeline(run) {
  const root = $("#timeline");
  if (!run.batches.length) {
    root.className = "timeline empty-state";
    root.textContent = "没有持久化批次";
    return;
  }
  root.className = "timeline";
  const starts = run.batches.map((batch) => Number(batch.timeline_start_us ?? (Number(batch.timeline_start_ms) * 1000)));
  const ends = run.batches.map((batch) => Number(batch.timeline_end_us ?? (Number(batch.timeline_end_ms) * 1000)));
  const min = Math.min(...starts);
  const max = Math.max(...ends);
  const span = Math.max(1, max - min);
  const rows = run.batches.map((batch) => {
    const start = Number(batch.timeline_start_us ?? (Number(batch.timeline_start_ms) * 1000));
    const left = ((start - min) / span) * 100;
    const width = Math.max(1.2, (Number(batch.latency_us) / span) * 100);
    const cls = statusClass(batch.state);
    return `
      <div class="timeline-row">
        <div class="timeline-label" title="${esc(batch.batch_id)} · ${esc(batch.model_id)}">${esc(batch.batch_id)} · ${esc(batch.model_id)}</div>
        <div class="timeline-track">
          <button class="timeline-bar ${cls}" type="button" data-batch="${esc(batch.batch_uid)}"
            style="left:${left.toFixed(3)}%;width:${width.toFixed(3)}%"
            title="${esc(batch.batch_id)} · ${esc(batch.latency_ms)}ms · ${esc(batch.memory_display)}"></button>
        </div>
        <div class="timeline-value">${esc(batch.latency_ms)} ms</div>
      </div>
    `;
  }).join("");
  root.innerHTML = `
    <div class="timeline-axis"><span>${(min / 1000).toFixed(2)} ms</span><span>${(max / 1000).toFixed(2)} ms</span></div>
    ${rows}
  `;
  $$(".timeline-bar").forEach((bar) => {
    bar.addEventListener("click", () => selectBatch(bar.dataset.batch));
  });
}

function renderMemory(run) {
  const plan = run.memory_plan;
  const initial = Number(plan.node_initial_free_bytes) || 0;
  const final = Number(plan.node_final_free_bytes) || 0;
  const ratio = initial > 0 ? (final / initial) * 100 : 0;
  $("#initialFree").textContent = plan.node_initial_free_display;
  $("#finalFree").textContent = plan.node_final_free_display;
  $("#memoryFreeLabel").textContent = `${ratio.toFixed(1)}% free`;
  $("#freeBar").style.width = `${Math.max(0, Math.min(100, ratio))}%`;
  const bitmap = plan.final_bitmap || {};
  $("#bitmapMeta").textContent = `${bitmap.block_count || 0} blocks · ${humanBytes(bitmap.block_bytes || 0)}`;
  const text = String(bitmap.text || "");
  const grid = $("#memoryBitmap");
  grid.innerHTML = text.split("").map((char) => {
    let cls = "free";
    if (/[A-Z]/.test(char) || char === "?") cls = "allocated";
    else if (char === "#") cls = "reserved";
    else if (char === "!") cls = "fragment";
    return `<span class="bitmap-cell ${cls}" title="block ${char}">${esc(char)}</span>`;
  }).join("");
}

function renderBatchTable(run) {
  const rows = run.batches.map((batch) => {
    const reasons = (batch.split_reasons || []).map((reason) =>
      `<span class="reason-chip ${reasonClass(reason)}">${esc(reason)}</span>`
    ).join("") || "—";
    return `
      <tr data-batch="${esc(batch.batch_uid)}">
        <td class="mono">${esc(batch.batch_id)}</td>
        <td><strong>${esc(batch.model_id)}</strong><br><span class="subtle">P${esc(batch.priority)}</span></td>
        <td class="mono">${esc(batch.member_count)}</td>
        <td class="mono">${esc(batch.sample_count)}</td>
        <td class="mono">${esc(batch.memory_display)}</td>
        <td class="mono">${esc(batch.latency_ms)} ms</td>
        <td><span class="status-text ${esc(batch.state)}">${esc(batch.state)}</span></td>
        <td>${reasons}</td>
      </tr>
    `;
  }).join("");
  $("#batchRows").innerHTML = rows || '<tr><td colspan="8" class="empty-cell">没有持久化批次</td></tr>';
  $$("#batchRows tr[data-batch]").forEach((row) => {
    row.addEventListener("click", () => selectBatch(row.dataset.batch));
  });
}

function renderFragments(run) {
  const events = run.memory_plan.fragmentation_events || [];
  const section = $("#fragmentSection");
  if (!events.length) {
    section.classList.add("hidden");
    return;
  }
  section.classList.remove("hidden");
  $("#fragmentCount").textContent = `${events.length} events`;
  $("#fragmentContent").innerHTML = events.map((event) => {
    const rejection = event.mixed_rejection;
    return `
      <div class="fragment-block">
        <div class="fragment-head">
          <strong>${esc(event.event_id)} → ${esc(event.batch_id)}</strong>
          <span>${esc(event.allocated_batch_samples)} / ${esc(event.requested_batch_samples)} samples</span>
        </div>
        <div class="subtle" style="margin-bottom:8px">
          largest free ${esc(event.largest_free_display || humanBytes(event.largest_free_bytes))} ·
          requested ${esc(humanBytes(event.requested_batch_bytes))} ·
          constraint within max: ${event.constraint_assertions?.within_model_max ? "PASS" : "FAIL"}
        </div>
        <div class="bitmap-pair">
          <div>
            <h4>降批前 bitmap</h4>
            <div class="bitmap-text">${esc(event.before_bitmap?.text || "")}</div>
          </div>
          <div>
            <h4>降批后 bitmap</h4>
            <div class="bitmap-text">${esc(event.after_bitmap?.text || "")}</div>
          </div>
        </div>
        ${rejection ? `
          <div class="mixed-reject">
            <strong>${esc(rejection.code)}</strong> ·
            ${esc(rejection.reason)}<br>
            combined ${esc(humanBytes(rejection.combined_memory_bytes))} /
            ${esc(rejection.combined_latency_us)} µs vs cap ${esc(rejection.latency_cap_us)} µs
          </div>` : ""}
      </div>
    `;
  }).join("");
}

async function selectBatch(batchUid) {
  state.selectedBatchUid = batchUid;
  $$("#batchRows tr[data-batch]").forEach((row) => {
    row.classList.toggle("selected", row.dataset.batch === batchUid);
  });
  const run = state.currentRun;
  const batch = run?.batches.find((item) => item.batch_uid === batchUid);
  if (!batch) return;
  $("#batchDetail").classList.remove("hidden");
  $("#detailTitle").textContent = `${batch.batch_id} · ${batch.model_id} · priority ${batch.priority}`;
  $("#detailState").textContent = batch.state;
  $("#detailState").className = `status-pill status-text ${esc(batch.state)}`;
  $("#quantileCards").innerHTML = '<div class="empty">加载分位…</div>';
  $("#errorChain").innerHTML = "";
  $("#checkRows").innerHTML = "";
  try {
    const [metrics, errors, validation] = await Promise.all([
      api(`/api/batches/${encodeURIComponent(batchUid)}/metrics`),
      api(`/api/batches/${encodeURIComponent(batchUid)}/errors`),
      api(`/api/batches/${encodeURIComponent(batchUid)}/validation`),
    ]);
    renderQuantiles(metrics);
    renderErrorChain(errors, metrics);
    renderChecks(validation);
    renderMembers(metrics);
    renderRounding(metrics, batch);
  } catch (error) {
    if (error.code === "E_INSUFFICIENT_SAMPLES") {
      renderAbortedDetail(batch, error);
      return;
    }
    showError(error);
  }
}

function renderAbortedDetail(batch, error) {
  $("#quantileCards").innerHTML = `
    <div class="quantile-card" style="grid-column:1/-1">
      <div class="label">E_INSUFFICIENT_SAMPLES</div>
      <div class="number">NO COMMITTED Q</div>
      <div class="meta">${esc(error.message)} · state=${esc(error.details?.state || batch.state)} · 不静默返回 0 或空值。</div>
    </div>`;
  $("#errorChain").innerHTML = `
    <div class="chain-stage"><div class="chain-index">!</div><div>
      <div class="chain-name">INTENT NOT FINALIZED</div>
      <div class="chain-desc">进程在分位最终事务前被终止。恢复扫描已将批次标记为 ABORTED，未生成伪误差链。</div>
    </div></div>`;
  $("#checkRows").innerHTML = '<tr><td colspan="7" class="empty-cell">无可核对的已提交指标</td></tr>';
  $("#memberRows").innerHTML = '<div class="empty">无可读取成员</div>';
  $("#roundingPanel").innerHTML = `<div class="mini-row"><span>恢复事实</span><strong>${esc(error.details?.state || "ABORTED")}</strong></div>`;
}

function renderQuantiles(metrics) {
  const quantiles = metrics.latency.quantiles || [];
  $("#quantileCards").innerHTML = quantiles.map((q) => `
    <div class="quantile-card">
      <div class="label">${esc(q.label)} · n=${esc(q.sample_count)}</div>
      <div class="number">${esc(q.latency_ms)} <small>ms</small></div>
      <div class="meta">
        nearest-rank #${esc(q.rank)} · raw ${esc(q.raw_us)} µs<br>
        output ${esc(q.output_unit)} · rounded ${esc(q.rounded_us)} µs
      </div>
      <div class="rounding">rounding ${q.rounding_error_us >= 0 ? "+" : ""}${esc(q.rounding_error_us)} µs · bit ${q.rounding_bit ? "1" : "0"}</div>
    </div>
  `).join("");
}

function stageValue(stage) {
  if (stage.name === "declared-unit-multiply") return `values = [${(stage.values || []).join(", ")}] µs`;
  if (stage.name === "compensated-accumulation") {
    return `raw ${stage.raw_total_us} + compensation ${stage.compensation_us} = ${stage.result_us} µs · error ${stage.rounding_error_us} µs`;
  }
  if (stage.name === "millisecond-output-quantization") {
    return `fixed ${stage.output_ms_fixed} → ${stage.rounded_us} µs · error ${stage.rounding_error_us} µs · bit ${stage.rounding_bit ? "1" : "0"}`;
  }
  if (stage.name === "quantile-output-quantization") {
    return `cumulative rounding ${stage.cumulative_rounding_error_us} µs`;
  }
  return "";
}

function renderErrorChain(errors, metrics) {
  const chain = errors.error_chain?.chain || [];
  $("#errorChain").innerHTML = chain.map((stage, index) => `
    <div class="chain-stage">
      <div class="chain-index">${index + 1}</div>
      <div>
        <div class="chain-name">${esc(stage.name)}</div>
        <div class="chain-desc">${esc(stage.operation)} · ${esc(stage.domain)}</div>
        <div class="chain-value">${esc(stageValue(stage))}</div>
      </div>
    </div>
  `).join("") || '<div class="empty">无误差链</div>';
  const summary = errors.rounding || metrics.rounding;
  if (summary) {
    const last = document.createElement("div");
    last.className = "mini-panel";
    last.innerHTML = `
      <div class="mini-row"><span>累计舍入误差（signed）</span><strong>${esc(summary.cumulative_rounding_error_us_signed)} µs</strong></div>
      <div class="mini-row"><span>累计舍入误差（abs）</span><strong>${esc(summary.cumulative_rounding_error_us_abs)} µs</strong></div>
    `;
    $("#errorChain").appendChild(last);
  }
}

function renderChecks(validation) {
  const checks = validation.checks || [];
  $("#checkRows").innerHTML = checks.map((check) => `
    <tr>
      <td>${esc(check.scope)}</td>
      <td class="mono">${esc(check.metric)}</td>
      <td class="mono">${esc(check.truth)}</td>
      <td class="mono">${esc(check.estimate)}</td>
      <td class="mono">${esc(check.absolute_deviation)} ${esc(check.unit)}</td>
      <td class="mono">${check.relative_deviation_ppm === null ? "∞" : `${(Number(check.relative_deviation_ppm) / 10000).toFixed(3)}%`}</td>
      <td><span class="${check.passed ? "pass" : "fail"}">${check.passed ? "PASS" : "FAIL"}</span></td>
    </tr>
  `).join("") || '<tr><td colspan="7" class="empty-cell">无核对记录</td></tr>';
}

function renderMembers(metrics) {
  const members = metrics.members || [];
  $("#memberRows").innerHTML = members.map((member) => `
    <div class="member-card">
      <div>
        <strong>${esc(member.request_id)}</strong><br>
        <span>part ${esc(member.part_index)}/${esc(member.part_count)} · samples ${esc(member.samples)} · seq ${esc(member.request_seq)}</span>
      </div>
      <div style="text-align:right">
        <strong>${esc(humanBytes(member.memory_bytes))}</strong><br>
        <span>${esc(member.latency_us)} µs</span>
      </div>
    </div>
  `).join("") || '<div class="empty">无成员</div>';
}

function renderRounding(metrics, batch) {
  const rounding = metrics.rounding || {};
  const budget = metrics.memory_budget || {};
  $("#roundingPanel").innerHTML = `
    <div class="mini-row"><span>输出单位</span><strong>${esc(rounding.output_unit)}</strong></div>
    <div class="mini-row"><span>舍入模式</span><strong>${esc(rounding.rounding_mode)}</strong></div>
    <div class="mini-row"><span>批次舍入位</span><strong>${rounding.batch_latency_rounding_bit ? "1" : "0"} · ${esc(rounding.batch_latency_rounding_direction)}</strong></div>
    <div class="mini-row"><span>累计误差</span><strong>${esc(rounding.cumulative_rounding_error_us_signed)} µs</strong></div>
    <div class="mini-row"><span>显存占用</span><strong>${esc(budget.batch_memory_display || batch.memory_display)}</strong></div>
    <div class="mini-row"><span>区域</span><strong>${esc(budget.region_start_bytes)} → ${esc(budget.region_end_bytes)}</strong></div>
  `;
}

async function refreshRecoveryAndIntents() {
  try {
    const [report, intents] = await Promise.all([
      api("/api/recovery/report"),
      api("/api/recovery/intents?limit=12"),
    ]);
    renderRecovery(report);
    renderIntents(intents.intents);
  } catch (error) {
    showError(error);
  }
}

function renderRecovery(report) {
  const events = report.events || [];
  const body = `
    <div class="recovery-summary">
      <div><strong>${esc(report.pending_intents_found)}</strong><span>未完成意图</span></div>
      <div><strong>${esc(events.length)}</strong><span>恢复事件</span></div>
      <div><strong>${esc((report.recent_runs || []).length)}</strong><span>近期运行</span></div>
    </div>
    ${events.map((event) => `
      <div class="recovery-event">
        <strong>${esc(event.action)}</strong> · ${esc(event.intent_kind)} · checkpoint ${esc(event.last_checkpoint)}<br>
        run ${esc(event.run_id || "—")} · batches aborted ${esc(event.batches_marked_aborted)}
      </div>
    `).join("") || '<div class="empty">启动扫描未发现未完成意图</div>'}
    <div class="recovery-notes">
      <strong>事实边界</strong>
      ${(report.limitations || []).map((item) => `<span>· ${esc(item)}</span>`).join("")}
      <span class="mono">${esc(report.scope)}</span>
    </div>
  `;
  $("#recoveryContent").className = "recovery";
  $("#recoveryContent").innerHTML = body;
}

function renderIntents(intents) {
  $("#intentLog").className = "intent-list";
  $("#intentLog").innerHTML = intents.map((intent) => `
    <div class="intent-row">
      <div class="intent-top">
        <strong>${esc(intent.kind)}</strong>
        <span class="intent-status ${esc(intent.status)}">${esc(intent.status)}</span>
      </div>
      <p>${esc(intent.checkpoint)} · ${esc(intent.intent_id)}</p>
      <p>run=${esc(intent.run_id || "—")} · ${esc(intent.resolution || "unresolved")}</p>
    </div>
  `).join("") || '<div class="empty">没有意图记录</div>';
}

async function loadAnomalySample(name) {
  clearError();
  try {
    const sample = await api(`/api/samples/${encodeURIComponent(name)}`);
    state.invalidPayload = sample.payload;
    $("#anomalyPanel").classList.remove("hidden");
    $("#anomalyContent").innerHTML = (sample.cases || []).map((item) => `
      <div class="anomaly-card">
        <code>${esc(item.field)} = ${esc(item.value)}</code>
        <p>${esc(item.reason)} · ${esc(item.expected_code)}</p>
      </div>
    `).join("");
  } catch (error) {
    showError(error);
  }
}

async function submitInvalidPayload() {
  if (!state.invalidPayload) return;
  clearError();
  try {
    await api("/api/traces", {
      method: "POST",
      body: JSON.stringify(state.invalidPayload),
    });
    showError({ code: "E_ASSERTION_FAILED", message: "异常样本意外被接受" });
  } catch (error) {
    showError(error);
    const notice = document.createElement("div");
    notice.className = "mixed-reject";
    notice.innerHTML = `<strong>${esc(error.code)}</strong> · 整批拒绝，written=${esc(error.details?.written ?? "undefined")}`;
    $("#anomalyContent").prepend(notice);
    await refreshRecoveryAndIntents();
  }
}

init();
