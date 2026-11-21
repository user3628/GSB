const $ = (id) => document.getElementById(id);
const fmtBytes = (n) => {
  const value = Number(n || 0);
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let i = 0, v = value;
  while (Math.abs(v) >= 1024 && i < units.length - 1) { v /= 1024; i += 1; }
  return `${v.toFixed(i === 0 ? 0 : 2)} ${units[i]}`;
};
const fmtMilli = (n) => `${Number(n || 0)}m`;

let stateCache = null;
let toastTimer = null;

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const text = await response.text();
  let data;
  try { data = text ? JSON.parse(text) : {}; } catch { data = { raw: text }; }
  if (!response.ok) {
    const error = data.error || { code: `HTTP_${response.status}`, message: text || response.statusText, details: {} };
    const exc = new Error(error.message);
    exc.business = error;
    throw exc;
  }
  return data;
}

function toast(message, bad = false) {
  const node = $("toast");
  node.textContent = message;
  node.classList.toggle("bad", bad);
  node.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => node.classList.remove("show"), 2600);
}

function showError(error) {
  const banner = $("errorBanner");
  const business = error.business || { code: "E_CLIENT", message: error.message, details: {} };
  banner.classList.remove("hidden");
  $("errorCode").textContent = business.code;
  $("errorMessage").textContent = business.message || "";
  $("errorDetails").textContent = JSON.stringify(business.details || {}, null, 2);
  toast(`${business.code}: ${business.message}`, true);
}

function clearError() { $("errorBanner").classList.add("hidden"); }

async function refreshHealth() {
  try {
    const data = await api("/api/v1/health");
    $("healthPulse").className = "pulse ok";
    $("healthText").textContent = `服务正常 · 待处理意图 ${data.pending_intents}`;
    $("revisionText").textContent = data.latest_revision || "—";
    $("policyText").textContent = data.policy_version || "—";
    $("roundPolicyVersion").value = data.policy_version || 1;
  } catch (error) {
    $("healthPulse").className = "pulse bad";
    $("healthText").textContent = "服务不可用";
  }
}

async function refreshState() {
  try {
    const data = await api("/api/v1/state");
    stateCache = data;
    renderState(data);
    $("snapshotPill").textContent = `revision ${data.revision} · ${data.kind}`;
  } catch (error) {
    if (error.business?.code !== "E_REVISION_NOT_FOUND") showError(error);
    stateCache = null;
    renderState(null);
  }
}

function renderState(data) {
  const services = data?.services || [];
  const nodes = data?.nodes || [];
  $("metricServices").textContent = services.length;
  $("metricReplicas").textContent = services.reduce((sum, item) => sum + item.current_replicas, 0);
  $("metricNodes").textContent = `${nodes.filter((item) => item.healthy).length}/${nodes.length}`;
  const select = $("transitionService");
  select.innerHTML = services.map((item) => `<option value="${escapeHtml(item.id)}">${escapeHtml(item.id)}</option>`).join("");

  $("services").innerHTML = services.length ? services.map((service) => `
    <article class="service-card">
      <div class="service-main">
        <div class="service-icon">${escapeHtml(service.id.slice(0, 2).toUpperCase())}</div>
        <div class="service-title">
          <strong>${escapeHtml(service.id)}</strong>
          <small>${escapeHtml(service.model)} · min ${service.min_replicas} / max ${service.max_replicas} · p${service.priority}</small>
        </div>
        <div class="replicas">${service.current_replicas}<span> / ${service.desired_replicas}</span></div>
        <span class="state-badge ${service.state}">${service.state}</span>
      </div>
      <div class="service-reason">${escapeHtml(service.reason)}</div>
      ${(service.degraded_domains || []).map((item) => `<div class="domain-conflict">domain ${escapeHtml(item.fault_domain)}: ${item.actual}/${item.max_allowed} · ${escapeHtml((item.node_ids || []).join(", "))}</div>`).join("")}
      <div class="placement-strip">${(service.placements || []).map((item) => `<span class="placement-chip">${escapeHtml(item.replica_id)} → ${escapeHtml(item.node_id)}</span>`).join("")}</div>
    </article>`).join("") : `<div class="empty">尚未提交策略快照</div>`;

  $("nodes").innerHTML = nodes.length ? nodes.map((node) => {
    const memoryPct = Math.min(100, 100 * node.used_memory_bytes / Math.max(1, node.memory_bytes));
    const computePct = Math.min(100, 100 * node.used_compute_milli / Math.max(1, node.compute_milli));
    return `<article class="node-card">
      <div class="node-top">
        <div><div class="node-name">${escapeHtml(node.id)}</div><div class="node-domain">${escapeHtml(node.fault_domain)}</div></div>
        <span class="node-health ${node.healthy ? "" : "bad"}">${node.healthy ? "HEALTHY" : "UNAVAILABLE"}</span>
      </div>
      <div class="bar-row">
        <div class="bar-label"><span>显存</span><span>${fmtBytes(node.used_memory_bytes)} / ${fmtBytes(node.memory_bytes)} · free ${fmtBytes(node.free_memory_bytes)}</span></div>
        <div class="bar"><span style="width:${memoryPct}%"></span></div>
      </div>
      <div class="bar-row">
        <div class="bar-label"><span>算力</span><span>${fmtMilli(node.used_compute_milli)} / ${fmtMilli(node.compute_milli)} · free ${fmtMilli(node.free_compute_milli)}</span></div>
        <div class="bar compute"><span style="width:${computePct}%"></span></div>
      </div>
    </article>`;
  }).join("") : `<div class="empty">尚无节点快照</div>`;
}

function renderRound(data) {
  $("roundStatus").textContent = data?.status || "ABORTED";
  const summary = data?.summary || {};
  $("metricGranted").textContent = summary.scale_up_granted ?? "—";
  $("roundSummary").innerHTML = [
    ["scale-up", summary.scale_up_granted || 0, "good"],
    ["queued", summary.queue_total_replicas || 0, "warn"],
    ["rejected", summary.scale_up_rejected || 0, (summary.scale_up_rejected ? "bad" : "")],
    ["scale-down", summary.scale_down_removed || 0, ""],
    ["migrate", summary.migrations_completed || 0, ""],
    ["slots", `${summary.admission_slots_used || 0}/${summary.admission_slots_available || 0}`, "warn"],
    ["outcome", data?.outcome || data?.status || "—", data?.outcome === "PARTIAL" ? "warn" : "good"],
    ...(data?.business_codes || []).map((code) => ["code", code, "bad"]),
  ].map(([name, value, cls]) => `<span class="chip ${cls || ""}">${name} <strong>${value}</strong></span>`).join("");

  const derivations = data?.derivations || [];
  const actions = data?.actions || [];
  const queue = data?.queue || [];
  const rejected = data?.rejected || [];
  $("roundBody").innerHTML = `
    <details open>
      <summary>推导依据 · ${derivations.length} 个服务</summary>
      ${derivations.length ? `<div class="table-wrap"><table>
        <thead><tr><th>service</th><th>arrivals</th><th>rate(m/s)</th><th>concurrency</th><th>latency</th><th>ratio</th><th>raw</th><th>desired</th><th>clamp</th></tr></thead>
        <tbody>${derivations.map((item) => `<tr>
          <td>${escapeHtml(item.service_id)}</td><td>${item.arrivals}</td><td>${item.arrival_rate_milli_rps}</td>
          <td>${item.concurrency_replicas}</td><td>${item.effective_latency_ms}/${item.latency_budget_ms}ms</td>
          <td>${item.latency_ratio_milli}</td><td>${item.desired_unclamped}</td><td><strong>${item.desired_replicas}</strong></td>
          <td>${item.clamp_min_applied ? "min " : ""}${item.clamp_max_applied ? "max" : ""}${!item.clamp_min_applied && !item.clamp_max_applied ? "—" : ""}</td>
        </tr>`).join("")}</tbody>
      </table></div>` : `<div class="empty">无推导</div>`}
    </details>
    <details open>
      <summary>放置动作 · ${actions.length}</summary>
      <pre class="code-block">${escapeHtml(JSON.stringify(actions, null, 2))}</pre>
    </details>
    <details ${queue.length || rejected.length ? "open" : ""}>
      <summary>排队 / 拒绝 · ${queue.length} / ${rejected.length}</summary>
      <pre class="code-block">${escapeHtml(JSON.stringify({ queue, rejected }, null, 2))}</pre>
    </details>
    <details>
      <summary>预算与完整性</summary>
      <pre class="code-block">${escapeHtml(JSON.stringify({ budget: data.budget, atomic: data.atomic, summary: data.summary, rejected }, null, 2))}</pre>
    </details>`;
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>'"]/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" }[char]));
}

async function loadSample(name, target) {
  try {
    const data = await api(`/api/v1/samples/${encodeURIComponent(name)}`);
    if (target === "policy") $("policyEditor").value = JSON.stringify(data, null, 2);
    else $("observationEditor").value = JSON.stringify(data, null, 2);
    clearError();
    toast(`已载入 ${name}`);
  } catch (error) { showError(error); }
}

async function loadObservationSample() {
  const name = $("observationSample").value;
  await loadSample(name, "observation");
  try {
    const data = JSON.parse($("observationEditor").value);
    if (data.batch_id) $("roundId").value = data.batch_id.replace("obs-", "round-");
  } catch { /* invalid JSON remains editable */ }
}

async function submitPolicy() {
  clearError();
  try {
    const payload = JSON.parse($("policyEditor").value);
    const button = $("submitPolicy");
    button.disabled = true;
    const result = await api("/api/v1/policy", { method: "POST", body: JSON.stringify(payload) });
    toast(`策略 v${result.policy_version} 已提交 · revision ${result.revision}`);
    await refreshAll();
  } catch (error) {
    showError(error);
  } finally { $("submitPolicy").disabled = false; }
}

async function executeRound(deriveOnly = false) {
  clearError();
  const button = deriveOnly ? $("deriveOnly") : $("executeRound");
  button.disabled = true;
  try {
    const observations = JSON.parse($("observationEditor").value);
    if (deriveOnly) {
      const result = await api("/api/v1/derive", {
        method: "POST",
        body: JSON.stringify({ policy_version: Number($("roundPolicyVersion").value), observations }),
      });
      renderRound({ status: "DERIVED", derivations: result.derivations, actions: [], queue: [], rejected: [], summary: {}, budget: { formula_version: result.formula_version, deterministic: result.deterministic } });
      toast("定点推导完成，未修改状态");
    } else {
      const payload = {
        round_id: $("roundId").value.trim(),
        policy_version: Number($("roundPolicyVersion").value),
        observations,
      };
      if ($("faultInjection").checked) payload.fault_injection = { crash_after: "INTENT_COMMITTED" };
      const result = await api("/api/v1/rounds", { method: "POST", body: JSON.stringify(payload) });
      renderRound(result);
      toast(result.replayed ? `轮次 ${result.round_id} 已复用提交结果` : `轮次 ${result.round_id} 已提交`);
      await refreshAll();
    }
  } catch (error) { showError(error); }
  finally { button.disabled = false; }
}

async function loadCurrentPolicy() {
  try {
    const data = await api("/api/v1/policy/current");
    $("policyEditor").value = JSON.stringify(data.snapshot, null, 2);
    toast(`已读取策略 v${data.version}`);
  } catch (error) { showError(error); }
}

async function loadRecovery() {
  try {
    const data = await api("/api/v1/recovery/report");
    $("recoveryPanel").hidden = false;
    $("recoveryBody").textContent = JSON.stringify(data, null, 2);
    $("recoveryPanel").scrollIntoView({ behavior: "smooth", block: "start" });
  } catch (error) { showError(error); }
}

async function transitionState() {
  const service = $("transitionService").value;
  if (!service) return;
  try {
    const result = await api(`/api/v1/services/${encodeURIComponent(service)}/transition`, {
      method: "POST",
      body: JSON.stringify({
        target_state: $("transitionTarget").value,
        expected_revision: stateCache?.revision,
        reason: "operator console transition",
      }),
    });
    toast(`${service}: ${result.from_state} → ${result.to_state} · r${result.revision}`);
    await refreshAll();
  } catch (error) { showError(error); }
}

function nextRound() {
  const match = $("roundId").value.match(/^(.*?)(\d+)$/);
  $("roundId").value = match ? `${match[1]}${String(Number(match[2]) + 1).padStart(match[2].length, "0")}` : `${$("roundId").value}-2`;
}

async function refreshAll() {
  await Promise.all([refreshHealth(), refreshState()]);
}

document.addEventListener("DOMContentLoaded", async () => {
  document.querySelectorAll("[data-sample]").forEach((button) => {
    button.addEventListener("click", () => loadSample(button.dataset.sample, button.dataset.sampleTarget));
  });
  $("submitPolicy").addEventListener("click", submitPolicy);
  $("executeRound").addEventListener("click", () => executeRound(false));
  $("deriveOnly").addEventListener("click", () => executeRound(true));
  $("loadPolicy").addEventListener("click", loadCurrentPolicy);
  $("observationSample").addEventListener("change", loadObservationSample);
  $("nextRound").addEventListener("click", nextRound);
  $("refreshAll").addEventListener("click", refreshAll);
  $("loadRecovery").addEventListener("click", loadRecovery);
  $("transitionState").addEventListener("click", transitionState);
  await refreshHealth();
  await refreshState();
  await loadSample("policy_v1.json", "policy");
  await loadObservationSample();
});
