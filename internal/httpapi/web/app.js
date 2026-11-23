const $ = (id) => document.getElementById(id);
let lastDecision = null;

async function api(path, options = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const body = await res.json().catch(() => ({ ok: false, error: { code: "E_BAD_JSON", message: "non-JSON response" } }));
  if (!res.ok || body.ok === false) {
    const err = body.error || { code: "E_HTTP", message: res.statusText, details: body };
    throw Object.assign(new Error(err.message), { api: err, status: res.status });
  }
  return body.data;
}

function pretty(value) {
  return JSON.stringify(value, null, 2);
}

function text(value) {
  return String(value ?? "");
}

function chips(config) {
  $("statusChips").innerHTML = `
    <span class="chip">config ${escapeHTML(config.version)}</span>
    <span class="chip">fast prefix ${config.fast_precheck_limit}</span>
    <span class="chip">budget ${config.default_query_budget_ms}ms</span>
    <span class="chip">loopback only</span>`;
}

function escapeHTML(value) {
  return text(value).replace(/[&<>"']/g, (ch) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[ch]));
}

function setOutput(id, value) {
  $(id).textContent = typeof value === "string" ? value : pretty(value);
}

function badge(decision) {
  const el = $("decisionBadge");
  el.className = `badge ${decision.outcome}`;
  el.textContent = `${decision.outcome} · v${decision.window_version} · ${decision.code}`;
}

function renderDecision(decision) {
  lastDecision = decision;
  $("modeBatchID").value = decision.batch_id;
  $("incBatchID").value = decision.batch_id;
  badge(decision);
  $("decisionSummary").classList.remove("empty");
  $("decisionSummary").innerHTML = `
    <strong>${escapeHTML(decision.batch_id)}</strong> · ${escapeHTML(decision.mode)} · ${decision.items.length} requests ·
    sample ${Math.round(decision.sample_rate * 1000) / 10}% · confidence <strong>${escapeHTML(decision.confidence)}</strong><br>
    <span class="mono">window=${decision.window_version} config=${escapeHTML(decision.tolerance_config_version)}</span><br>
    ${escapeHTML(decision.confidence_note)}`;
  const cols = ["#", "request", "client", "result", "code", "offset", "boundary", "first occupied", "remaining", "prechecked"];
  const rows = decision.items.map((item) => `
    <tr>
      <td class="mono">${item.ordinal}</td>
      <td class="mono">${escapeHTML(item.request_id)}<br><span class="muted">${escapeHTML(item.route_key)}</span></td>
      <td class="mono">${escapeHTML(item.client_id)}</td>
      <td class="${item.result === "accepted" ? "ok" : item.result === "rejected" ? "bad" : "warn"}">${escapeHTML(item.result)}</td>
      <td class="mono">${escapeHTML(item.code)}</td>
      <td class="mono">${item.offset_ms} ms</td>
      <td class="mono">${escapeHTML(item.triggered_boundary)}</td>
      <td class="mono">${item.first_occupied_at_ms ?? "—"}</td>
      <td class="mono">${item.remaining_window_ms ?? "—"}</td>
      <td class="mono">${item.prechecked ? "yes" : "NO"}</td>
    </tr>`).join("");
  $("decisionTable").innerHTML = `<thead><tr>${cols.map((c) => `<th>${escapeHTML(c)}</th>`).join("")}</tr></thead><tbody>${rows}</tbody>`;
}

async function loadSample() {
  const name = $("sampleSelect").value;
  if (!name) return;
  const res = await fetch(`/api/v1/samples/${encodeURIComponent(name)}`);
  $("batchInput").value = await res.text();
}

async function submitBatch() {
  try {
    const batch = JSON.parse($("batchInput").value);
    const decision = await api("/api/v1/adjudications", { method: "POST", body: JSON.stringify(batch) });
    renderDecision(decision);
  } catch (err) {
    $("decisionBadge").className = "badge rejected";
    $("decisionBadge").textContent = err.api?.code || "error";
    $("decisionSummary").innerHTML = `<span class="bad">${escapeHTML(err.message)}</span>`;
    setOutput("snapshotOutput", err.api || err.message);
  }
}

async function querySnapshot() {
  try {
    const version = $("snapshotVersion").value ? `&version=${encodeURIComponent($("snapshotVersion").value)}` : "";
    const budget = $("snapshotBudget").value || "40";
    const data = await api(`/api/v1/diagnostics/snapshot?budget_ms=${encodeURIComponent(budget)}${version}`);
    setOutput("snapshotOutput", data);
  } catch (err) {
    setOutput("snapshotOutput", err.api || err.message);
  }
}

async function queryMode() {
  try {
    const id = $("modeBatchID").value.trim();
    const data = await api(`/api/v1/diagnostics/mode-differences?batch_id=${encodeURIComponent(id)}&budget_ms=${encodeURIComponent($("modeBudget").value || "40")}`);
    setOutput("modeOutput", data);
  } catch (err) {
    setOutput("modeOutput", err.api || err.message);
  }
}

function parseViews() {
  return $("clientViews").value.split(/\r?\n/).map((line) => line.trim()).filter(Boolean).map((line) => {
    const idx = line.lastIndexOf(":");
    if (idx <= 0) throw new Error(`客户端视图格式错误：${line}`);
    return { client_id: line.slice(0, idx), picked_window_version: Number(line.slice(idx + 1)) };
  });
}

async function queryInconsistencies() {
  try {
    const body = {
      batch_id: $("incBatchID").value.trim(),
      budget_ms: Number($("incBudget").value || "40"),
      views: parseViews(),
    };
    const data = await api("/api/v1/diagnostics/inconsistencies", { method: "POST", body: JSON.stringify(body) });
    setOutput("incOutput", data);
  } catch (err) {
    setOutput("incOutput", err.api || err.message);
  }
}

async function queryRecovery() {
  try {
    const data = await api("/api/v1/diagnostics/recovery/history?limit=20");
    setOutput("recoveryOutput", data);
  } catch (err) {
    setOutput("recoveryOutput", err.api || err.message);
  }
}

async function boot() {
  try {
    const [health, config, samples] = await Promise.all([
      api("/healthz"), api("/api/v1/config"), api("/api/v1/samples"),
    ]);
    $("statusChips").innerHTML = `
      <span class="chip">${escapeHTML(health.status)}</span>
      <span class="chip">config ${escapeHTML(config.version)}</span>
      <span class="chip">fast prefix ${config.fast_precheck_limit}</span>
      <span class="chip">budget ${config.default_query_budget_ms}ms</span>`;
    $("sampleSelect").innerHTML = samples.map((name) => `<option value="${escapeHTML(name)}">${escapeHTML(name)}</option>`).join("");
    if (samples.length) {
      await loadSample();
      await submitBatch();
    }
  } catch (err) {
    $("statusChips").innerHTML = `<span class="chip" style="color:var(--bad)">${escapeHTML(err.api?.code || err.message)}</span>`;
  }
}

$("loadSample").addEventListener("click", loadSample);
$("submitBatch").addEventListener("click", submitBatch);
$("snapshotButton").addEventListener("click", querySnapshot);
$("modeButton").addEventListener("click", queryMode);
$("incButton").addEventListener("click", queryInconsistencies);
$("recoveryButton").addEventListener("click", queryRecovery);
boot();
