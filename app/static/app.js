const state = {
  fixtures: null,
  layout: null,
  lastRound: null,
  lastTrace: null,
};

const $ = (selector) => document.querySelector(selector);

function esc(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function showToast(message, isError = false) {
  const toast = $("#toast");
  toast.textContent = message;
  toast.className = `toast show${isError ? " error" : ""}`;
  clearTimeout(showToast.timer);
  showToast.timer = setTimeout(() => { toast.className = "toast"; }, 4200);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  let payload = null;
  try { payload = await response.json(); } catch (_) { payload = null; }
  if (!response.ok) {
    const error = payload?.error || { code: `HTTP_${response.status}`, message: response.statusText };
    const wrapped = new Error(`${error.code}: ${error.message}`);
    wrapped.payload = error;
    throw wrapped;
  }
  return payload;
}

function formatBytes(value) {
  if (value === null || value === undefined) return "—";
  if (value >= 1048576) return `${(value / 1048576).toFixed(2)} MiB`;
  if (value >= 1024) return `${(value / 1024).toFixed(2)} KiB`;
  return `${value} B`;
}

function formatUs(value) {
  if (value === null || value === undefined) return "—";
  if (value >= 1000) return `${(value / 1000).toFixed(2)} ms`;
  return `${value} μs`;
}

function strategyLabel(strategy) {
  return {
    EC_REBUILD: "EC 重建",
    REPLICA_FILL: "副本补齐",
    DIRECT_READ: "直接读取",
    DIRECT_FAIL: "快速失败",
  }[strategy] || strategy || "—";
}

async function loadFixtures() {
  state.fixtures = await api("/api/v1/demo/fixtures");
}

async function refreshHealth() {
  try {
    const health = await api("/api/v1/health");
    $("#healthDot").className = "status-dot ok";
    $("#healthText").textContent = `引擎在线 · ${health.layout_count} 个布局`;
  } catch (error) {
    $("#healthDot").className = "status-dot bad";
    $("#healthText").textContent = "服务不可用";
  }
}

function renderLayout(summary) {
  if (!summary) return;
  state.layout = summary;
  $("#layoutPill").textContent = "已加载";
  $("#layoutPill").className = "pill ok";
  $("#layoutVersionPill").textContent = summary.layout_version;
  $("#layoutFacts").innerHTML = [
    ["矩阵", summary.matrix_id],
    ["对象数", summary.object_count],
    ["声明上限 k+m", summary.max_k_plus_m],
    ["文档摘要", summary.document_hash?.slice(0, 26) + "…"],
  ].map(([label, value]) => `<div class="fact-row"><span>${esc(label)}</span><b>${esc(value)}</b></div>`).join("");
  $("#objectStrip").innerHTML = summary.objects.map((obj) =>
    `<span class="object-chip"><b>${esc(obj.object_id)}</b> · ${obj.k}+${obj.m}</span>`
  ).join("");
  const engine = summary.engine;
  if (engine) {
    $("#concurrencyInput").value = engine.rebuild_concurrency;
    $("#queueInput").value = engine.rebuild_queue_capacity;
    $("#bytesInput").value = engine.round_byte_budget;
    $("#timeInput").value = engine.round_time_budget_us;
  }
}

async function loadLayout() {
  if (!state.fixtures) await loadFixtures();
  const summary = await api("/api/v1/layouts", {
    method: "POST",
    body: JSON.stringify({ layout: state.fixtures.layout }),
  });
  renderLayout(summary);
  showToast(summary.created ? "确定性布局已加载" : "布局已存在，幂等复用");
}

async function refreshCurrentLayout() {
  try {
    renderLayout(await api("/api/v1/layouts/current"));
  } catch (error) {
    if (error.payload?.code !== "E_LAYOUT_NOT_FOUND") showToast(error.message, true);
  }
}

async function refreshRounds(selectedRoundId = "") {
  const rounds = (await api("/api/v1/rounds?limit=50")).rounds;
  const select = $("#roundSelect");
  const committed = rounds.filter((round) => round.state === "COMMITTED");
  if (!committed.length) {
    select.innerHTML = `<option value="">暂无已提交轮次</option>`;
    return;
  }
  select.innerHTML = committed.map((round) =>
    `<option value="${esc(round.round_id)}">#${round.round_seq} · ${esc(round.round_id.slice(-8))} · ${esc(round.decision_digest?.slice(0, 12) || "—")}…</option>`
  ).join("");
  select.value = selectedRoundId || committed[0].round_id;
}

async function loadSelectedRound() {
  const roundId = $("#roundSelect").value;
  if (!roundId) return;
  try {
    const result = await api(`/api/v1/rounds/${encodeURIComponent(roundId)}`);
    renderRound(result);
  } catch (error) {
    showToast(error.message, true);
  }
}

function selectedTrace(overrides = false) {
  const scenario = $("#scenarioSelect").value;
  const scenarioData = state.fixtures.scenarios[scenario];
  const trace = JSON.parse(JSON.stringify(scenarioData.trace));
  if (overrides) {
    const options = {
      rebuild_concurrency: Number($("#concurrencyInput").value),
      rebuild_queue_capacity: Number($("#queueInput").value),
      round_byte_budget: Number($("#bytesInput").value),
      round_time_budget_us: Number($("#timeInput").value),
    };
    trace.options = options;
  }
  return trace;
}

function renderMetrics(result) {
  const summary = result.summary;
  const successRate = summary.request_count ? Math.round(summary.success_count * 100 / summary.request_count) : 0;
  const values = [
    [summary.request_count, "本轮裁决"],
    [`${successRate}%`, `${summary.success_count} 成功 / ${summary.failed_count} 失败`],
    [formatBytes(summary.consumed_bytes), `预算 ${formatBytes(result.budget.byte_budget)}`],
    [formatUs(summary.estimated_makespan_us), `时间预算 ${formatUs(result.budget.time_budget_us)}`],
  ];
  $("#metrics").innerHTML = values.map(([value, label], index) => {
    const names = ["请求", "成功率", "字节消耗", "虚拟耗时"];
    return `<article class="metric"><span>${names[index]}</span><strong>${esc(value)}</strong><small>${esc(label)}</small></article>`;
  }).join("");
  $("#digestLabel").textContent = `decision_digest · ${result.decision_digest}`;
  $("#digestLabel").title = result.decision_digest;
}

function renderDecisions(result) {
  const tbody = $("#decisionsTable tbody");
  if (!result) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="7">加载布局并提交轨迹后显示逐请求裁决。</td></tr>`;
    return;
  }
  tbody.innerHTML = result.decisions.map((decision, index) => {
    const latencyOver = decision.estimated_latency_us > decision.tolerable_latency_us;
    return `<tr>
      <td class="bitmap">${decision.seq + 1}</td>
      <td class="request-cell"><strong>${esc(decision.request_id)}</strong><span>${esc(decision.object_id)} · P${decision.priority}</span></td>
      <td><span class="bitmap">${esc(decision.requested_bitmap)}</span><br><span class="bitmap" style="color:var(--faint)">${esc(decision.readable_bitmap)}</span></td>
      <td><span class="strategy strategy-${esc(decision.strategy)}">${esc(strategyLabel(decision.strategy))}</span></td>
      <td><span class="badge ${decision.outcome === "SUCCESS" ? "success" : decision.outcome === "PENDING" ? "pending" : "failed"}">${esc(decision.outcome)}</span>${decision.error ? `<span class="error-code">${esc(decision.error.code)}</span>` : ""}</td>
      <td><span class="latency${latencyOver ? " over" : ""}">${formatUs(decision.estimated_latency_us)}</span><br><span class="bitmap" style="color:var(--faint)">≤ ${formatUs(decision.tolerable_latency_us)}</span></td>
      <td><button class="detail-btn" data-index="${index}" title="查看完整证据">↗</button></td>
    </tr>`;
  }).join("");
  tbody.querySelectorAll(".detail-btn").forEach((button) => {
    button.addEventListener("click", () => {
      const decision = result.decisions[Number(button.dataset.index)];
      $("#dialogJson").textContent = JSON.stringify(decision, null, 2);
      $("#jsonDialog").showModal();
    });
  });
}

function renderBudget(result) {
  if (!result) return;
  const budget = result.budget;
  const ratio = budget.byte_budget ? Math.min(100, budget.consumed_bytes * 100 / budget.byte_budget) : 0;
  $("#budgetPill").textContent = `${formatBytes(budget.consumed_bytes)} / ${formatBytes(budget.byte_budget)}`;
  $("#budgetBar").style.width = `${ratio}%`;
  $("#budgetMeta").textContent = `剩余 ${formatBytes(budget.remaining_bytes)} · 并发 ${budget.concurrency_limit} · 队列 ${budget.queue_capacity} · 任务 ${budget.task_count} · 降级 ${budget.shed_requests.length}`;
  $("#taskList").innerHTML = budget.tasks.length ? budget.tasks.map((task) => `
    <div class="task">
      <span class="task-dot ${task.state === "FAILED" ? "failed" : ""}"></span>
      <div class="task-main"><strong>${esc(task.object_id)} · shard ${task.shard_index}</strong><span>P${task.priority} · wave ${task.wave} · ${formatUs(task.start_us)} → ${formatUs(task.end_us)} · ${esc(task.state)}</span></div>
      <span class="task-bytes">${formatBytes(task.bytes_charged)}</span>
    </div>`).join("") : `<p class="muted">本轮未进入重建队列。</p>`;
}

function renderRound(result) {
  state.lastRound = result;
  renderMetrics(result);
  renderDecisions(result);
  renderBudget(result);
  $("#replayBtn").disabled = false;
}

async function submitTrace() {
  if (!state.layout) await loadLayout();
  const trace = selectedTrace($("#overrideOptions").checked);
  state.lastTrace = trace;
  $("#submitBtn").disabled = true;
  try {
    const result = await api("/api/v1/traces", {
      method: "POST",
      body: JSON.stringify({ trace }),
    });
    renderRound(result);
    await refreshRounds(result.round_id);
    showToast(`轮次 ${result.round_id} 已提交 · ${result.decision_digest.slice(0, 18)}…`);
  } catch (error) {
    showToast(error.message, true);
  } finally {
    $("#submitBtn").disabled = false;
  }
}

async function replayTrace() {
  if (!state.lastTrace) return;
  $("#replayBtn").disabled = true;
  try {
    const replay = await api("/api/v1/traces", {
      method: "POST",
      body: JSON.stringify({ trace: state.lastTrace }),
    });
    const same = replay.decision_digest === state.lastRound.decision_digest;
    showToast(same ? `逐位一致重放通过 · ${replay.decision_digest.slice(0, 22)}…` : "裁决摘要不一致", !same);
    if (!same) {
      console.error("digest mismatch", state.lastRound.decision_digest, replay.decision_digest);
    }
  } catch (error) {
    showToast(error.message, true);
  } finally {
    $("#replayBtn").disabled = false;
  }
}

async function refreshRecovery() {
  try {
    const report = await api("/api/v1/recovery");
    $("#recoveryJson").textContent = JSON.stringify(report, null, 2);
    const summary = report.summary || {};
    $("#recoveryPill").textContent = report.recovery_required ? "检测到恢复事实" : "无未完成意图";
    $("#recoveryPill").className = `pill ${report.recovery_required ? "warn" : "ok"}`;
    const values = [
      summary.pending_intents ?? 0,
      summary.aborted_rounds ?? 0,
      summary.tasks_returned_to_ready ?? 0,
      summary.reusable_committed_shards ?? 0,
    ];
    $("#recoveryGrid").innerHTML = values.map((value, index) => {
      const names = ["待处理意图", "中止轮次", "回到 READY 任务", "可复用分片结果"];
      return `<div><span>${names[index]}</span><strong>${esc(value)}</strong></div>`;
    }).join("");
  } catch (error) {
    showToast(error.message, true);
  }
}

function updateScenarioNote() {
  const scenario = $("#scenarioSelect").value;
  const notes = {
    main: "重建、副本补齐、不可恢复、预算降级与延迟保护同轮演示。",
    queue: "并发 1、队列 0：首个任务运行，后续请求返回 E_REBUILD_QUEUE_FULL。",
    mismatch: "目标分片声明摘要被破坏：重建完成但摘要不一致，返回 E_RECONSTRUCT_MISMATCH。",
    recovery: "正常提交可演练；真正强制终止请运行 scripts/crash_and_recover.py。",
  };
  $("#scenarioNote").textContent = notes[scenario];
}

async function initialize() {
  $("#loadLayoutBtn").addEventListener("click", () => loadLayout().catch((error) => showToast(error.message, true)));
  $("#submitBtn").addEventListener("click", submitTrace);
  $("#replayBtn").addEventListener("click", replayTrace);
  $("#refreshRecoveryBtn").addEventListener("click", refreshRecovery);
  $("#refreshLayoutBtn").addEventListener("click", refreshCurrentLayout);
  $("#scenarioSelect").addEventListener("change", updateScenarioNote);
  $("#roundSelect").addEventListener("change", loadSelectedRound);
  $("#closeDialog").addEventListener("click", () => $("#jsonDialog").close());
  updateScenarioNote();
  await Promise.all([refreshHealth(), loadFixtures()]);
  await Promise.all([refreshCurrentLayout(), refreshRecovery()]);
  await refreshRounds();
  await loadSelectedRound();
}

initialize().catch((error) => showToast(error.message, true));
