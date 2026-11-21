const $ = (id) => document.getElementById(id);
const esc = (value) => String(value ?? "");
let refreshing = false;
let latestState = null;
let toastSeq = 0;

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  let payload;
  try { payload = await response.json(); } catch { payload = null; }
  if (!response.ok || payload?.ok === false) {
    const error = payload?.error || { code: `HTTP_${response.status}`, message: "request failed", context: {} };
    throw Object.assign(new Error(error.message), error);
  }
  return payload;
}

function toast(title, message, kind = "") {
  const node = document.createElement("div");
  node.className = `toast ${kind}`;
  const id = ++toastSeq;
  node.dataset.id = id;
  const b = document.createElement("b");
  b.textContent = title;
  const p = document.createElement("p");
  p.textContent = message;
  node.append(b, p);
  $("toastStack").append(node);
  setTimeout(() => node.remove(), kind === "error" ? 9000 : 4500);
}

function setBusy(button, busy) {
  if (!button) return;
  button.disabled = busy;
  button.dataset.original = button.dataset.original || button.querySelector("span")?.textContent || "";
  const span = button.querySelector("span");
  if (span) span.textContent = busy ? "Working…" : button.dataset.original;
}

function shortDigest(digest) {
  if (!digest) return "—";
  return `${digest.slice(0, 14)}…${digest.slice(-8)}`;
}

function shortTime(value) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.valueOf())) return String(value).slice(11, 19);
  return date.toLocaleTimeString([], { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

async function refresh() {
  if (refreshing) return;
  refreshing = true;
  try {
    const overview = await api("/api/v1/overview");
    latestState = overview.state;
    render(overview);
    $("healthText").textContent = "LOOPBACK READY";
  } catch (error) {
    $("healthText").textContent = "DEGRADED";
    console.error(error);
  } finally {
    refreshing = false;
  }
}

function render(overview) {
  const state = overview.state;
  const snapshot = state.snapshot;
  const channels = state.channels || [];
  const activeCount = channels.filter((item) => item.state === "busy").length;
  const totalAcked = Object.values(state.stats || {}).reduce((sum, item) => sum + (item.frames_acked || 0), 0);
  const activeLeases = (state.leases || []).filter((item) => item.status === "active").length;

  $("topVersion").textContent = state.version ?? "—";
  $("topQueue").textContent = `${state.queue_depth ?? 0}/${state.queue_capacity ?? "—"}`;
  $("metricVersion").textContent = state.version ?? "—";
  $("metricDigest").textContent = shortDigest(state.digest);
  $("snapshotDigest").textContent = state.digest || "—";
  $("metricChannels").textContent = `${activeCount}/${channels.length || "—"}`;
  $("metricChannelMode").textContent = "fixed";
  $("metricQueue").textContent = state.queue_depth ?? 0;
  $("metricQueueCap").textContent = `/ ${state.queue_capacity ?? "—"}`;
  $("metricBudget").textContent = `round budget ${state.scheduler?.round_budget_ms ?? "—"} ms`;
  $("metricAcked").textContent = totalAcked;
  $("metricLeases").textContent = `${activeLeases} active lease${activeLeases === 1 ? "" : "s"}`;
  $("queueRatio").textContent = `${state.queue_depth || 0} / ${state.queue_capacity ?? "—"}`;

  renderTopology(overview);
  renderChannels(channels);
  renderSessions(state);
  renderQueue(state);
  renderRecovery(overview.recovery || state.recovery);
  renderRound(state);
  fillSessionOptions(state);
}

function renderTopology(overview) {
  const line = $("topologyLine");
  line.replaceChildren();
  const gateway = topoNode("gateway", "HTTP encoder", "127.0.0.1 · POST /frames");
  line.append(gateway);
  for (const carrier of overview.topology?.carriers ? Object.entries(overview.topology.carriers) : []) {
    const [name, item] = carrier;
    line.append(topoArrow("→"));
    line.append(topoNode("", `carrier-${name}`, `${item.listen} · relay`));
    line.append(topoArrow("→"));
    line.append(topoNode("egress", item.egress_name, `${item.egress} · decap`));
  }
}

function topoNode(kind, title, sub) {
  const node = document.createElement("div");
  node.className = `topo-node ${kind}`;
  const b = document.createElement("b");
  b.textContent = title;
  const span = document.createElement("span");
  span.textContent = sub;
  node.append(b, span);
  return node;
}

function topoArrow(text) {
  const node = document.createElement("div");
  node.className = "topo-arrow";
  node.textContent = text;
  return node;
}

function renderChannels(channels) {
  const grid = $("channelGrid");
  grid.replaceChildren();
  if (!channels.length) {
    grid.append(emptyState("No snapshot loaded"));
    return;
  }
  for (const channel of channels) {
    const card = document.createElement("div");
    card.className = `channel-card ${channel.state}`;
    const head = document.createElement("div");
    head.className = "channel-head";
    const id = document.createElement("span");
    id.className = "channel-id";
    id.textContent = `CHANNEL ${String(channel.channel_id).padStart(2, "0")}`;
    const dot = document.createElement("span");
    dot.className = "state-dot";
    head.append(id, dot);
    const session = document.createElement("div");
    session.className = "channel-session";
    session.textContent = channel.session_id || "IDLE";
    const meta = document.createElement("div");
    meta.className = "channel-meta";
    meta.textContent = channel.state === "busy"
      ? `${channel.frame_id}\nseq ${channel.sequence} · ${channel.egress}`
      : "available for the bound session";
    card.append(head, session, meta);
    grid.append(card);
  }
}

function renderSessions(state) {
  const body = $("sessionRows");
  body.replaceChildren();
  const sessions = Object.values(state.sessions || {});
  if (!sessions.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 5;
    td.append(emptyState("No active sessions"));
    tr.append(td);
    body.append(tr);
    return;
  }
  for (const session of sessions) {
    const tr = document.createElement("tr");

    const nameCell = document.createElement("td");
    const name = document.createElement("span");
    name.className = "session-name";
    name.textContent = session.session_id;
    const sub = document.createElement("span");
    sub.className = "sub";
    sub.textContent = `priority ${session.priority}`;
    nameCell.append(name, sub);

    const seqCell = document.createElement("td");
    seqCell.className = "seq-cell";
    seqCell.textContent = session.last_acked_seq;
    const next = document.createElement("span");
    next.className = "sub";
    next.textContent = `next ${session.next_expected_seq}`;
    seqCell.append(next);

    const binding = document.createElement("td");
    binding.innerHTML = `<span class="session-name">ch ${esc(session.channel_id)}</span><span class="sub">${esc(session.egress_label)} · ${esc(session.egress)}</span>`;

    const lease = document.createElement("td");
    if (session.lease?.channel_id !== null && session.lease?.channel_id !== undefined) {
      lease.innerHTML = `<span class="tag warn">BUSY ch ${esc(session.lease.channel_id)}</span><span class="sub">unacked ${esc(session.lease.sent_unacked_seq)}</span>`;
    } else {
      lease.innerHTML = `<span class="tag ok">IDLE</span><span class="sub">${session.queued_frames || 0} queued</span>`;
    }

    const stats = document.createElement("td");
    const stat = session.stats;
    if (stat) {
      stats.innerHTML = `<span class="session-name">${esc(stat.frames_acked)} frames</span><span class="sub">${esc(stat.bytes_acked)} bytes · gen ${esc(stat.snapshot_version)} · inv ${esc(stat.invalidations)}</span>`;
    } else {
      stats.innerHTML = `<span class="tag">NO STATS</span>`;
    }
    if (session.blocked_for_rebuild) {
      lease.innerHTML = `<span class="tag warn">REBUILDING</span>`;
    }
    tr.append(nameCell, seqCell, binding, lease, stats);
    body.append(tr);
  }
}

function renderQueue(state) {
  const list = $("queueList");
  list.replaceChildren();
  if (!(state.queue || []).length) {
    list.append(emptyState("Queue empty · no waiting frames"));
    return;
  }
  for (const item of state.queue) {
    const row = document.createElement("div");
    row.className = "queue-item";
    row.innerHTML = `<span class="queue-pos">${String(item.position + 1).padStart(2, "0")}</span><span class="queue-main"><b>${esc(item.frame_id)}</b><span>${esc(item.session_id)} · seq ${esc(item.sequence)} · priority ${esc(item.priority)}</span></span><span class="queue-channel">ch ${esc(item.channel_id)}</span>`;
    list.append(row);
  }
}

function renderRecovery(recovery) {
  const content = $("recoveryContent");
  content.replaceChildren();
  if (!recovery) {
    $("recoveryTag").className = "tag";
    $("recoveryTag").textContent = "NO REPORT";
    content.append(emptyState("No recovery scan persisted"));
    return;
  }
  const found = recovery.found_work;
  $("recoveryTag").className = `tag ${found ? "warn" : "ok"}`;
  $("recoveryTag").textContent = found ? "FACTS RECOVERED" : "CLEAN START";
  const grid = document.createElement("div");
  grid.className = "recovery-grid";
  const stats = [
    ["PENDING INTENTS", recovery.scanned_pending_intents ?? 0],
    ["RETRANSMIT", (recovery.retransmit_required || []).length],
    ["ABORTED", (recovery.aborted || []).length],
  ];
  for (const [label, value] of stats) {
    const item = document.createElement("div");
    item.className = "recovery-stat";
    item.innerHTML = `<span>${label}</span><b>${esc(value)}</b>`;
    grid.append(item);
  }
  content.append(grid);
  const facts = document.createElement("div");
  facts.className = "facts-list";
  for (const [label, value] of [
    ["snapshot", recovery.snapshot_version ?? "—"],
    ["facts only", recovery.facts_only ? "true" : "false"],
    ["exit report", recovery.graceful_exit_report_available ? "available" : "not claimed"],
    ["report", recovery.report_id ? String(recovery.report_id).slice(0, 8) : "—"],
  ]) {
    const row = document.createElement("div");
    row.className = "fact-row";
    row.innerHTML = `<span>${label}</span><b>${esc(value)}</b>`;
    facts.append(row);
  }
  content.append(facts);
  if (recovery.lease_differences?.length) {
    const changed = recovery.lease_differences.filter((item) => JSON.stringify(item.before) !== JSON.stringify(item.after));
    if (changed.length) {
      const row = document.createElement("div");
      row.className = "fact-row";
      row.innerHTML = `<span>lease diffs</span><b>${changed.length}</b>`;
      facts.append(row);
    }
  }
  const statement = document.createElement("div");
  statement.className = "statement";
  statement.textContent = recovery.statement || "Recovery is derived from persisted facts only.";
  content.append(statement);
}

function renderRound(state) {
  const round = state.last_scheduler_round;
  $("roundNote").textContent = round
    ? `round ${round.round} · ${round.dispatched.length} dispatched · ${round.elapsed_ms} ms`
    : "no round completed";
}

function emptyState(text) {
  const div = document.createElement("div");
  div.className = "empty-state";
  div.textContent = text;
  return div;
}

function fillSessionOptions(state) {
  const select = $("sessionSelect");
  const previous = select.value;
  const sessions = Object.values(state.sessions || {});
  select.replaceChildren();
  for (const session of sessions) {
    const option = document.createElement("option");
    option.value = session.session_id;
    option.textContent = `${session.session_id} · next ${session.next_expected_seq}`;
    select.append(option);
  }
  if (sessions.some((item) => item.session_id === previous)) select.value = previous;
  select.dataset.version = String(state.version);
  syncSequenceInput();
}

function syncSequenceInput() {
  const sessionId = $("sessionSelect").value;
  const session = latestState?.sessions?.[sessionId];
  if (session && !$("sequenceInput").dataset.touched) {
    $("sequenceInput").value = session.next_expected_seq;
  }
}

async function loadTraces() {
  try {
    const payload = await api("/api/v1/traces?limit=120");
    renderTraces(payload.traces || []);
  } catch (error) {
    console.error(error);
  }
}

function renderTraces(traces) {
  const log = $("traceLog");
  log.replaceChildren();
  if (!traces.length) {
    log.append(emptyState("No trace events yet"));
    return;
  }
  for (const trace of traces.slice(-80).reverse()) {
    const row = document.createElement("div");
    row.className = `trace-row ${String(trace.kind).includes("fail") || String(trace.kind).includes("abort") ? "error" : ""}`;
    const detail = trace.detail || {};
    const compact = Object.entries(detail)
      .filter(([key]) => !["intent_id", "rebuild_transaction"].includes(key))
      .map(([key, value]) => `${key}=${typeof value === "object" ? JSON.stringify(value) : value}`)
      .join(" ");
    const identity = trace.session_id
      ? `${trace.session_id}${trace.sequence ? `:${trace.sequence}` : ""}${trace.channel_id !== null && trace.channel_id !== undefined ? ` ch${trace.channel_id}` : ""}`
      : trace.frame_id || "system";
    row.innerHTML = `<span class="trace-time">${esc(shortTime(trace.at))}</span><span class="trace-marker">›</span><span class="trace-kind">${esc(trace.kind)}</span><span class="trace-detail">${esc(identity)}${compact ? ` · ${esc(compact)}` : ""}</span>`;
    log.append(row);
  }
}

async function runAction(action, button) {
  setBusy(button, true);
  try {
    if (action === "load-sample") {
      const sample = await api("/api/v1/samples/snapshot_v1.json");
      await api("/api/v1/snapshots", { method: "POST", body: JSON.stringify(sample) });
      toast("Snapshot loaded", "Versioned snapshot accepted after digest validation.");
    } else if (action === "parallel") {
      const result = await api("/api/v1/lab/scenarios/parallel", { method: "POST" });
      const frames = result.rounds.flat();
      toast("Parallel scenario complete", `${frames.filter((item) => item.ok).length}/${frames.length} frames acknowledged.`);
    } else if (action === "rebuild") {
      const result = await api("/api/v1/lab/scenarios/rebuild", { method: "POST" });
      toast("Route rebuilt", `v${result.result.base_version} → v${result.result.version}; scope: ${result.result.affected_sessions.join(", ")}.`);
    } else if (action === "pressure") {
      const result = await api("/api/v1/lab/scenarios/pressure", { method: "POST" });
      const overflow = result.results.find((item) => !item.ok);
      toast("Pressure scenario complete", overflow ? `Overflow rejected with ${overflow.error.code}.` : "No overflow observed.");
    }
    await refresh();
    await loadTraces();
  } catch (error) {
    toast(error.code || "REQUEST FAILED", `${error.message}\n${JSON.stringify(error.context || {}, null, 2)}`, "error");
  } finally {
    setBusy(button, false);
  }
}

document.addEventListener("click", (event) => {
  const button = event.target.closest("[data-action]");
  if (!button) return;
  runAction(button.dataset.action, button);
});

$("refreshBtn").addEventListener("click", () => refresh().then(loadTraces));
$("sessionSelect").addEventListener("change", () => {
  $("sequenceInput").dataset.touched = "";
  syncSequenceInput();
});
$("sequenceInput").addEventListener("input", (event) => {
  event.target.dataset.touched = "1";
});

$("frameForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = event.submitter;
  let payload;
  try {
    payload = JSON.parse($("payloadInput").value);
  } catch (error) {
    toast("INVALID JSON", error.message, "error");
    return;
  }
  const timeoutValue = $("timeoutInput").value.trim();
  const body = {
    frame_id: `manual-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
    session_id: $("sessionSelect").value,
    sequence: Number($("sequenceInput").value),
    payload,
  };
  if (timeoutValue) body.timeout_ms = Number(timeoutValue);
  setBusy(button, true);
  try {
    const result = await api("/api/v1/frames", { method: "POST", body: JSON.stringify(body) });
    toast("Frame acknowledged", `${result.result.frame_id} · ch ${result.result.channel_id} · ${result.result.response.endpoint}`);
    $("sequenceInput").dataset.touched = "";
    await refresh();
    await loadTraces();
  } catch (error) {
    toast(error.code || "FRAME FAILED", `${error.message}\n${JSON.stringify(error.context || {}, null, 2)}`, "error");
    await refresh();
    await loadTraces();
  } finally {
    setBusy(button, false);
  }
});

refresh();
loadTraces();
setInterval(() => { refresh(); loadTraces(); }, 1500);

