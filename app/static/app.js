const state = {
  samples: [],
  selectedBatch: null,
  comparison: null,
  openPods: new Set(),
  selectedNode: null,
};

const $ = (selector) => document.querySelector(selector);
const escapeHtml = (value) => String(value ?? "")
  .replaceAll("&", "&amp;")
  .replaceAll("<", "&lt;")
  .replaceAll(">", "&gt;")
  .replaceAll('"', "&quot;")
  .replaceAll("'", "&#039;");

function formatBytes(bytes) {
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let value = Number(bytes);
  let index = 0;
  while (Math.abs(value) >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }
  return `${Number.isInteger(value) ? value : value.toFixed(1)}${units[index]}`;
}

function formatCpu(millicores) {
  return millicores % 1000 === 0 ? `${millicores / 1000} CPU` : `${millicores}m`;
}

function resourceLine(label, used, total, formatter, kind = "") {
  const percent = total > 0 ? Math.min(100, (used / total) * 100) : 0;
  return `
    <div class="resource-row">
      <div class="resource-meta"><span>${label}</span><span><strong>${formatter(total - used)}</strong> / ${formatter(total)} 可用</span></div>
      <div class="bar ${kind}"><span style="width:${percent.toFixed(3)}%"></span></div>
    </div>`;
}

function setBusy(busy, message = "") {
  document.querySelectorAll("button").forEach((button) => { button.disabled = busy; });
  $("#request-state").textContent = busy ? message : "";
}

function showError(message) {
  const banner = $("#error-banner");
  banner.hidden = false;
  banner.textContent = message;
}

function clearError() {
  $("#error-banner").hidden = true;
  $("#error-banner").textContent = "";
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = payload.error || { code: `HTTP_${response.status}`, message: "请求失败" };
    throw new Error(`${error.code}: ${error.message}`);
  }
  return payload;
}

async function loadSamples() {
  const payload = await api("/api/samples");
  state.samples = payload.samples;
  const select = $("#sample-select");
  select.innerHTML = state.samples.map((sample) => `<option value="${escapeHtml(sample.name)}">${escapeHtml(sample.name)}</option>`).join("");
  if (state.samples.some((sample) => sample.name === "original")) select.value = "original";
}

async function runBatch(sampleName) {
  const batchId = `ui-${sampleName}-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
  return api("/api/batches", {
    method: "POST",
    body: JSON.stringify({ batchId, sample: sampleName }),
  });
}

function summaryMetrics(result) {
  const consistent = result.resourceConsistency.allNodesConsistent;
  return `
    <div class="metric"><div class="label">待调度 Pod</div><div class="value">${result.summary.pendingPods}</div></div>
    <div class="metric good"><div class="label">已放置</div><div class="value">${result.summary.placed}</div></div>
    <div class="metric bad"><div class="label">已否决</div><div class="value">${result.summary.rejected}</div></div>
    <div class="metric warn"><div class="label">被抢占</div><div class="value">${result.summary.preempted}</div></div>
    <div class="metric ${consistent ? "good" : "bad"}"><div class="label">账本一致性</div><div class="value">${consistent ? "OK" : "FAIL"}</div></div>`;
}

function renderSummary(batch) {
  const result = batch.result;
  $("#summary-panel").innerHTML = summaryMetrics(result);
  document.querySelectorAll(".node-card").forEach((card) => card.classList.remove("selected"));
  state.selectedNode = null;
  $("#detail-panel").hidden = true;
}

function renderNodes(batch) {
  const result = batch.result;
  $("#node-grid").innerHTML = result.nodes.map((node) => {
    const cpuTotal = node.allocatable.cpuMillicores;
    const memTotal = node.allocatable.memoryBytes;
    const cpuUsed = node.used.cpuMillicores;
    const memUsed = node.used.memoryBytes;
    const taints = node.taints.length ? node.taints.map((taint) => `${taint.key}:${taint.effect}`).join(" · ") : "无污点";
    return `
      <article class="node-card" data-node="${escapeHtml(node.name)}" tabindex="0" role="button">
        <div class="node-name-row">
          <span class="node-name">${escapeHtml(node.name)}</span>
          <span class="badge ${node.invariant.cpu.consistent && node.invariant.memory.consistent ? "ok" : "fail"}">
            ${node.invariant.cpu.consistent && node.invariant.memory.consistent ? "一致" : "异常"}
          </span>
        </div>
        <div class="labels">${escapeHtml(Object.entries(node.labels).map(([key, value]) => `${key}=${value}`).join(" · "))}<br>污点：${escapeHtml(taints)}</div>
        ${resourceLine("CPU", cpuUsed, cpuTotal, formatCpu)}
        ${resourceLine("内存", memUsed, memTotal, formatBytes, "memory")}
      </article>`;
  }).join("");
  document.querySelectorAll(".node-card").forEach((card) => {
    const open = () => openNodeDetail(batch.batchId, card.dataset.node, card);
    card.addEventListener("click", open);
    card.addEventListener("keydown", (event) => { if (event.key === "Enter" || event.key === " ") open(); });
  });
}

function compactCheck(check) {
  const suffix = check.passed ? "" : ` · ${check.code}`;
  return `${check.plugin}${suffix}`;
}

function traceCard(trace) {
  const checks = trace.checks.map((check) => `
    <div class="check ${check.passed ? "" : "failed"}">
      <span class="check-dot"></span>
      <span class="check-text">${escapeHtml(compactCheck(check))}</span>
    </div>`).join("");
  const rejection = trace.firstRejection ? `
    <div class="reason-box">${escapeHtml(trace.firstRejection.code)}\n${escapeHtml(trace.firstRejection.message)}\n${escapeHtml(JSON.stringify(trace.firstRejection.details, null, 2))}</div>` : "";
  return `
    <div class="trace-card">
      <div class="trace-head">
        <span class="trace-node">${escapeHtml(trace.node)}</span>
        <span class="badge ${trace.passed ? "ok" : "fail"}">${trace.passed ? "通过" : "否决"}</span>
      </div>
      ${checks}${rejection}
    </div>`;
}

function scoreBlock(score) {
  if (!score) return "";
  const components = (score.components || []).map((component) => `
    <div class="component-line"><span>${escapeHtml(component.component)}</span><span>${escapeHtml(component.points)}</span></div>`).join("");
  return `
    <div class="score-box">
      <div>score = <span class="score-total">${escapeHtml(score.score)}</span> · ${escapeHtml(score.scoreDecimals)} 位定点小数</div>
      ${components}
      <div>rounding residual: ${escapeHtml(score.rounding.componentResidualTotalPoints)}</div>
      <div>tie-break: ${escapeHtml(score.tieBreak)}</div>
    </div>`;
}

function renderPods(batch) {
  const result = batch.result;
  const audit = result.scoreContract.roundingAudit;
  const auditLine = `
    <div class="audit-line">
      <strong>批次定点审计</strong>
      ${audit.evaluationCount} 次候选评分 · 累计舍入残差 ${escapeHtml(audit.cumulativeResidualPoints)} 分
      · 精确分数 ${escapeHtml(audit.cumulativeResidualExact)}
    </div>`;
  $("#pod-list").innerHTML = auditLine + result.steps.map((step) => {
    const isOpen = state.openPods.has(step.pod);
    const rejected = step.decision === "rejected";
    const reason = rejected ? `${step.rejection.code} · ${step.rejection.primaryNode}` : `得分 ${step.selectedScore.score}`;
    return `
      <article class="pod-card ${isOpen ? "open" : ""}" data-pod="${escapeHtml(step.pod)}">
        <div class="pod-summary">
          <div><div class="pod-title">${escapeHtml(step.pod)}</div><div class="pod-sub">序位 ${step.sequence} · 样本内优先顺序</div></div>
          <div class="pod-node">优先级<br><strong>${step.priority}</strong></div>
          <div class="pod-node">${rejected ? "否决" : "放置"}<br><strong>${escapeHtml(step.selectedNode || "—")}</strong></div>
          <div class="pod-reason" title="${escapeHtml(reason)}">${escapeHtml(reason)}</div>
          <div class="chevron">›</div>
        </div>
        <div class="pod-detail">
          <div class="trace-grid">${step.filterTrace.map(traceCard).join("")}</div>
          ${scoreBlock(step.selectedScore)}
          ${rejected ? `<div class="reason-box">${escapeHtml(JSON.stringify(step.rejection, null, 2))}</div>` : ""}
          ${step.preemption ? `<div class="reason-box">抢占重算：${escapeHtml(JSON.stringify(step.preemption, null, 2))}</div>` : ""}
        </div>
      </article>`;
  }).join("");
  document.querySelectorAll(".pod-card").forEach((card) => {
    card.querySelector(".pod-summary").addEventListener("click", () => {
      if (state.openPods.has(card.dataset.pod)) state.openPods.delete(card.dataset.pod);
      else state.openPods.add(card.dataset.pod);
      card.classList.toggle("open");
    });
  });
}

async function openNodeDetail(batchId, nodeName, card) {
  state.selectedNode = nodeName;
  document.querySelectorAll(".node-card").forEach((nodeCard) => nodeCard.classList.toggle("selected", nodeCard === card));
  const panel = $("#detail-panel");
  panel.hidden = false;
  panel.innerHTML = `<div class="empty">读取 ${escapeHtml(nodeName)} 约束明细…</div>`;
  try {
    const detail = await api(`/api/batches/${encodeURIComponent(batchId)}/nodes/${encodeURIComponent(nodeName)}`);
    panel.innerHTML = `
      <button class="button ghost detail-close" id="close-detail">关闭</button>
      <p class="eyebrow">NODE DETAIL</p>
      <h2>${escapeHtml(nodeName)}</h2>
      <div class="detail-grid" style="margin-top:18px">
        <div class="detail-block"><h3>资源一致性方程</h3><div class="code">${escapeHtml(JSON.stringify(detail.node.invariant, null, 2))}</div></div>
        <div class="detail-block"><h3>初始分配账本</h3><div class="code">${escapeHtml(JSON.stringify(detail.node.initialLedger, null, 2))}</div></div>
        <div class="detail-block"><h3>本批放置账本</h3><div class="code">${escapeHtml(JSON.stringify(detail.node.placementLedger, null, 2))}</div></div>
        <div class="detail-block"><h3>该节点否决记录</h3><div class="code">${escapeHtml(JSON.stringify(detail.rejections, null, 2))}</div></div>
      </div>`;
    $("#close-detail").addEventListener("click", () => { panel.hidden = true; state.selectedNode = null; document.querySelectorAll(".node-card").forEach((item) => item.classList.remove("selected")); });
  } catch (error) {
    panel.innerHTML = `<div class="empty">${escapeHtml(error.message)}</div>`;
  }
}

function renderComparison(original, modified) {
  const originalPods = new Map(original.result.placed.map((item) => [item.pod, item.node]));
  const modifiedPods = new Map(modified.result.placed.map((item) => [item.pod, item.node]));
  const changed = [...new Set([...originalPods.keys(), ...modifiedPods.keys()])]
    .filter((pod) => originalPods.get(pod) !== modifiedPods.get(pod))
    .map((pod) => `${pod}: ${originalPods.get(pod) || "否决"} → ${modifiedPods.get(pod) || "否决"}`);
  const originalRemaining = new Map(original.result.nodes.map((node) => [node.name, node.remaining]));
  const nodeDiffs = modified.result.nodes.flatMap((node) => {
    const before = originalRemaining.get(node.name);
    if (!before) return [];
    if (before.cpuMillicores === node.remaining.cpuMillicores && before.memoryBytes === node.remaining.memoryBytes) return [];
    return [`${node.name} CPU ${before.cpuMillicores}→${node.remaining.cpuMillicores}m，内存 ${formatBytes(before.memoryBytes)}→${formatBytes(node.remaining.memoryBytes)}`];
  });
  const summary = (batch) => `
    <div class="compare-card">
      <h3>${escapeHtml(batch.specName)}</h3>
      <div class="compare-line"><span>放置</span><strong>${batch.result.summary.placed}</strong></div>
      <div class="compare-line"><span>否决</span><strong>${batch.result.summary.rejected}</strong></div>
      <div class="compare-line"><span>批次</span><code>${escapeHtml(batch.batchId)}</code></div>
      <div class="compare-line"><span>Pod 决策</span><span>${escapeHtml(batch.result.steps.map((step) => `${step.pod}@${step.selectedNode || "×"}`).join(" · "))}</span></div>
    </div>`;
  $("#comparison-panel").hidden = false;
  $("#comparison-panel").innerHTML = `
    <div class="section-heading"><div><p class="eyebrow">A/B DIFF</p><h2>固定样本对比结论</h2></div><p class="hint">唯一输入变化：node-b.accelerator 标签</p></div>
    <div class="comparison-grid">${summary(original)}${summary(modified)}</div>
    <div class="diff-box">
      <div><strong>Pod 放置变化</strong>：${escapeHtml(changed.join("；") || "无")}</div>
      <div><strong>节点余量变化</strong>：${escapeHtml(nodeDiffs.join("；") || "无")}</div>
      <div><strong>结论</strong>：标签变化使 accelerator-job 从 E_UNSATISFIED_AFFINITY 变为可放置；批次内累计请求与既有硬反亲和仍独立生效。</div>
    </div>`;
}

async function renderRecovery() {
  const report = await api("/api/recovery");
  const latest = report.runs.at(-1);
  const events = report.events.length
    ? report.events.slice(-8).reverse().map((event) => `<div class="recovery-event"><code>${escapeHtml(event.action)}</code> · ${escapeHtml(event.batchId)}<br>${escapeHtml(JSON.stringify(event.details))}</div>`).join("")
    : `<div class="empty">没有未完成意图需要恢复。启动扫描已记录为持久化事实。</div>`;
  $("#recovery-content").innerHTML = `
    <div class="recovery-note">
      <strong>仅持久化事实</strong><br>
      最近扫描：${latest ? `${escapeHtml(latest.trigger)} · 扫描 ${latest.scannedCount} · 重算 ${latest.recoveredCount} · 中止 ${latest.abortedCount}` : "无"}<br><br>
      ${report.limitations.map(escapeHtml).join("<br>")}
    </div>
    <div class="recovery-events">${events}</div>`;
}

async function runSelected() {
  clearError();
  setBusy(true, "scheduling selected sample…");
  try {
    const sample = $("#sample-select").value;
    const batch = await runBatch(sample);
    state.selectedBatch = batch;
    state.comparison = null;
    state.openPods.clear();
    $("#comparison-panel").hidden = true;
    renderSummary(batch);
    renderNodes(batch);
    renderPods(batch);
    await renderRecovery();
  } catch (error) {
    showError(error.message);
  } finally {
    setBusy(false);
  }
}

async function runComparison() {
  clearError();
  setBusy(true, "running original and modified batches…");
  try {
    const [original, modified] = await Promise.all([runBatch("original"), runBatch("modified")]);
    state.comparison = { original, modified };
    state.selectedBatch = modified;
    state.openPods.clear();
    renderSummary(modified);
    renderNodes(modified);
    renderPods(modified);
    renderComparison(original, modified);
    await renderRecovery();
  } catch (error) {
    showError(error.message);
  } finally {
    setBusy(false);
  }
}

async function init() {
  try {
    await loadSamples();
    await renderRecovery();
    $("#run-selected").addEventListener("click", runSelected);
    $("#run-comparison").addEventListener("click", runComparison);
    $("#refresh-recovery").addEventListener("click", async () => {
      clearError();
      try { await renderRecovery(); } catch (error) { showError(error.message); }
    });
  } catch (error) {
    showError(error.message);
  }
}

document.addEventListener("DOMContentLoaded", init);
