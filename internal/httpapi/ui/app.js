const app = {
  state: null,
  samples: null,
  caches: null,
  busy: false,
};

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => Array.from(document.querySelectorAll(selector));

function escapeHTML(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function shortHash(value, length = 12) {
  if (!value) return "—";
  return String(value).slice(0, length);
}

function toast(message, isError = false) {
  const element = $("#toast");
  element.textContent = message;
  element.className = `toast show${isError ? " error" : ""}`;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => { element.className = "toast"; }, 4200);
}

async function api(path, options = {}) {
  const request = { ...options, headers: { ...(options.headers || {}) } };
  if (request.body && typeof request.body !== "string") {
    request.body = JSON.stringify(request.body);
    request.headers["Content-Type"] = "application/json";
  }
  const response = await fetch(path, request);
  let payload;
  try {
    payload = await response.json();
  } catch {
    throw { code: "E_BAD_RESPONSE", message: `HTTP ${response.status}`, context: {} };
  }
  if (!response.ok || !payload.ok) {
    throw payload.error || { code: `E_HTTP_${response.status}`, message: "请求失败", context: {} };
  }
  return payload.data;
}

function setResult(selector, value, isError = false, isSuccess = false) {
  const element = $(selector);
  element.className = `result-box${isError ? " error" : ""}${isSuccess ? " success" : ""}`;
  if (typeof value === "string") {
    element.textContent = value;
  } else {
    element.innerHTML = value;
  }
}

function errorHTML(error) {
  return `<span style="color:var(--red);font-weight:700">${escapeHTML(error.code || "E_UNKNOWN")}</span>\n` +
    `<span style="color:var(--muted)">${escapeHTML(error.message || "")}</span>\n` +
    `${escapeHTML(JSON.stringify(error.context || {}, null, 2))}`;
}

function showError(error, selector = "#action-result") {
  setResult(selector, errorHTML(error), true);
  toast(`${error.code || "ERROR"} · ${error.message || ""}`, true);
}

async function refreshAll() {
  try {
    const [statePayload, cachePayload, intentPayload, commitPayload, recoveryPayload] = await Promise.all([
      api("/api/state"),
      api("/api/caches"),
      api("/api/intents?limit=30"),
      api("/api/commits?limit=30"),
      api("/api/recovery"),
    ]);
    app.state = statePayload.state;
    app.recovery = recoveryPayload;
    app.caches = cachePayload;
    renderMetrics(statePayload.state);
    renderTables(statePayload.state);
    renderCaches(cachePayload);
    renderIntents(intentPayload.intents);
    renderCommits(commitPayload.commits);
    renderRecovery(recoveryPayload);
    $("#health-pill").textContent = "服务在线";
    $("#health-pill").className = "pill";
    $("#header-version").textContent = statePayload.state.current_version || 0;
    $("#query-observed").value = statePayload.state.current_version || 1;
  } catch (error) {
    $("#health-pill").textContent = "服务不可用";
    $("#health-pill").className = "pill down";
    showError(error, "#recovery-box");
  }
}

function renderMetrics(state) {
  const cards = [
    ["当前路由版本", state.current_version || 0, state.current_version ? `checksum ${shortHash(state.snapshot?.checksum)}` : "尚未加载快照"],
    ["逻辑库", state.logical_db_count || 0, "cluster logical databases"],
    ["逻辑表", state.tables?.length || 0, "per-table routing cache"],
    ["物理表", state.physical_table_count || 0, "mapped shard tables"],
    ["缓存一致", state.cache_aligned ? "YES" : "NO", state.cache_aligned ? "atomic rebuild complete" : "inspect cache graph"],
  ];
  $("#metrics").innerHTML = cards.map(([label, value, sub]) => `
    <div class="metric">
      <div class="label">${escapeHTML(label)}</div>
      <div class="value">${escapeHTML(value)}</div>
      <div class="sub">${escapeHTML(sub)}</div>
    </div>`).join("");
}

function renderTables(state) {
  const select = $("#query-table");
  const selected = select.value;
  const tables = state.tables || [];
  select.innerHTML = tables.map((table) => `<option value="${escapeHTML(table.table_id)}">${escapeHTML(table.table_id)} · ${escapeHTML(table.algorithm)} · ${escapeHTML(table.shard_key)}</option>`).join("");
  if (selected && tables.some((table) => table.table_id === selected)) select.value = selected;
}

function renderCaches(payload) {
  const routeCaches = payload.route_caches || [];
  $("#cache-grid").innerHTML = routeCaches.map((cache) => {
    const aligned = cache.cache_version === cache.expected_version;
    const boundaries = (cache.boundary_index || []).map((entry) => {
      const left = entry.lower_inclusive ? "[" : "(";
      const right = entry.upper_inclusive ? "]" : ")";
      return `<div class="boundary"><span>${escapeHTML(entry.range_id)} ${left}${entry.lower},${entry.upper}${right}</span><span class="phys">${escapeHTML(entry.physical_table)}</span></div>`;
    }).join("");
    const physicals = (cache.physical_tables || []).map((table) => `<span class="chip">${escapeHTML(table)}</span>`).join("");
    return `<article class="cache-card">
      <h3><span>${escapeHTML(cache.table_id)}</span><span class="vpill ${aligned ? "ok" : "warn"}">${aligned ? "aligned" : "lagging"}</span></h3>
      <div class="version-stack">
        <span class="vpill">cache v${cache.cache_version}</span>
        <span class="vpill ${aligned ? "ok" : "warn"}">expected v${cache.expected_version}</span>
        <span class="vpill">sha ${shortHash(cache.checksum)}</span>
      </div>
      <div class="chips">${physicals || '<span class="chip">no physical table</span>'}</div>
      <div class="boundaries">${boundaries}</div>
    </article>`;
  }).join("") || `<div class="table-wrap empty">暂无路由缓存；先提交整份快照</div>`;

  const aggregates = payload.aggregate_caches || [];
  $("#aggregate-grid").innerHTML = aggregates.map((cache) => `
    <article class="aggregate-card">
      <h3>${escapeHTML(cache.view_id)} <span class="vpill">view v${cache.cache_version}</span></h3>
      <p>depends on ${escapeHTML((cache.depends_on || []).join(" + "))}</p>
      <p>${(cache.physical_tables || []).map((item) => `<span class="chip">${escapeHTML(item)}</span>`).join(" ")}</p>
    </article>`).join("");
}

function renderIntents(intents) {
  const element = $("#intent-table");
  if (!intents?.length) {
    element.className = "table-wrap empty";
    element.textContent = "暂无意图";
    return;
  }
  element.className = "table-wrap";
  element.innerHTML = `<table><thead><tr><th>意图</th><th>类型</th><th>版本</th><th>状态</th><th>阶段</th></tr></thead><tbody>` +
    intents.map((intent) => `<tr>
      <td title="${escapeHTML(intent.id)}">${escapeHTML(shortHash(intent.id, 14))}<br><span style="color:var(--muted)">${escapeHTML(intent.commit_id)}</span></td>
      <td>${escapeHTML(intent.kind)}</td>
      <td>${intent.base_version} → ${intent.target_version}</td>
      <td><span class="status ${escapeHTML(intent.status)}">${escapeHTML(intent.status)}</span></td>
      <td>${escapeHTML(intent.phase)}</td>
    </tr>`).join("") + `</tbody></table>`;
}

function renderCommits(commits) {
  const element = $("#commit-table");
  if (!commits?.length) {
    element.className = "table-wrap empty";
    element.textContent = "暂无提交";
    return;
  }
  element.className = "table-wrap";
  element.innerHTML = `<table><thead><tr><th>commit_id</th><th>类型</th><th>版本</th><th>内容摘要</th></tr></thead><tbody>` +
    commits.map((commit) => `<tr>
      <td>${escapeHTML(commit.commit_id)}</td>
      <td>${escapeHTML(commit.kind)}</td>
      <td>v${commit.version}</td>
      <td title="${escapeHTML(commit.content_hash)}">${escapeHTML(shortHash(commit.content_hash))}</td>
    </tr>`).join("") + `</tbody></table>`;
}

function renderRecovery(payload) {
  if (!payload.report) {
    setResult("#recovery-box", "尚无恢复记录。正常首次启动不会伪造报告。");
    return;
  }
  setResult("#recovery-box", JSON.stringify(payload.report, null, 2), false, true);
}

async function loadSamples() {
  try {
    app.samples = await api("/api/samples");
    $("#snapshot-editor").value = JSON.stringify(app.samples.snapshot_v1, null, 2);
    $("#increment-editor").value = JSON.stringify(app.samples.increment_v2, null, 2);
  } catch (error) {
    showError(error);
  }
}

function parseJSONEditor(selector) {
  try {
    return JSON.parse($(selector).value);
  } catch (error) {
    throw { code: "E_INVALID_JSON", message: "编辑器 JSON 无法解析", context: { cause: error.message } };
  }
}

async function submitSnapshot(inject = "") {
  try {
    const snapshot = parseJSONEditor("#snapshot-editor");
    const current = app.state?.current_version || 0;
    if ($("#auto-next-version").checked && Number(snapshot.version) <= current) {
      snapshot.version = current + 1;
    }
    const body = {
      snapshot,
      seal_checksum: $("#seal-snapshot").checked,
    };
    const commitID = $("#snapshot-commit-id").value.trim();
    if (commitID) body.commit_id = commitID;
    if (inject) body.inject = inject;
    setResult("#action-result", "提交中…");
    const result = await api("/api/snapshots", { method: "POST", body });
    setResult("#action-result", JSON.stringify(result, null, 2), false, true);
    $("#snapshot-editor").value = JSON.stringify(snapshot, null, 2);
    toast(`快照 v${result.version} 已提交${result.reused ? "（幂等复用）" : ""}`);
    await refreshAll();
  } catch (error) {
    showError(error);
  }
}

async function submitIncrement(inject = "") {
  try {
    const increment = parseJSONEditor("#increment-editor");
    if (inject) increment.inject = inject;
    setResult("#action-result", "提交中…");
    const result = await api("/api/increments", { method: "POST", body: increment });
    setResult("#action-result", JSON.stringify(result, null, 2), false, true);
    toast(`增量 v${result.version} 已提交${result.reused ? "（幂等复用）" : ""}`);
    await refreshAll();
  } catch (error) {
    showError(error);
  }
}

function buildInterruptIncrement() {
  const snapshot = app.state?.snapshot;
  if (!snapshot || !snapshot.logical_tables?.length) {
    throw { code: "E_SNAPSHOT_REQUIRED", message: "需要先提交 v1 快照", context: {} };
  }
  const table = snapshot.logical_tables.find((item) => item.id === "ledger") || snapshot.logical_tables[0];
  const ranges = table.ranges.map((range, index) => ({
    ...range,
    weight: index === 0 ? Number(range.weight || 0) + 1 : Number(range.weight || 0),
  }));
  return {
    commit_id: `ui-interrupt-${Date.now()}`,
    base_version: app.state.current_version,
    generated_at: new Date().toISOString(),
    inject: "rebuild_interrupt",
    changes: [{ op: "replace_ranges", table_id: table.id, ranges }],
  };
}

async function injectInterrupt() {
  try {
    const increment = buildInterruptIncrement();
    $("#increment-editor").value = JSON.stringify(increment, null, 2);
    setResult("#action-result", "正在落盘意图并终止进程…\n收到 E_REBUILD_INTERRUPTED 后刷新页面并查看恢复报告。");
    const result = await api("/api/increments", { method: "POST", body: increment });
    setResult("#action-result", JSON.stringify(result, null, 2), false, true);
  } catch (error) {
    showError(error);
    // The server terminates shortly after the response. Poll health to update UI.
    setTimeout(async () => {
      try { await api("/api/health"); } catch { $("#health-pill").textContent = "进程已终止，请重启服务"; $("#health-pill").className = "pill down"; }
    }, 900);
  }
}

function queryBody(inject = "") {
  const kind = $("#query-kind").value;
  const body = {
    table_id: $("#query-table").value,
    observed_version: Number($("#query-observed").value),
    kind,
  };
  if (inject) body.inject = inject;
  if (kind === "eq") body.value = $("#query-value").value;
  if (kind === "in") body.values = $("#query-values").value.split(",").map((item) => item.trim()).filter(Boolean);
  if (kind === "range") {
    body.lower = $("#query-lower").value;
    body.upper = $("#query-upper").value;
    body.include_lower = $("#query-include-lower").checked;
    body.include_upper = $("#query-include-upper").checked;
  }
  return body;
}

async function runQuery(inject = "") {
  try {
    setResult("#query-result", "计算中…");
    const result = await api("/api/query", { method: "POST", body: queryBody(inject) });
    const chips = (result.physical_tables || []).map((table) => `<span class="chip">${escapeHTML(table)}</span>`).join("") || '<span class="chip">∅ 空区间 / 无映射</span>';
    const hits = (result.hits || []).map((hit) => `<tr><td>${escapeHTML(hit.range_id)}</td><td>${escapeHTML(hit.physical_table)}</td><td>${hit.slot ?? "—"}</td></tr>`).join("");
    const html = `<div class="route-summary">
      <div class="big"><strong>${escapeHTML(result.table_id)}</strong><span class="vpill">observed v${result.observed_version}</span><span class="vpill ok">used v${result.used_version}</span><span class="vpill">cache v${result.cache_version}</span><span class="vpill">current v${result.current_version}</span>${result.conservative ? '<span class="vpill warn">conservative</span>' : ''}</div>
      <div class="chips">${chips}</div>
      ${hits ? `<table style="margin-top:12px"><thead><tr><th>range</th><th>physical</th><th>slot</th></tr></thead><tbody>${hits}</tbody></table>` : ""}
      <details style="margin-top:10px"><summary style="color:var(--muted);cursor:pointer">raw response</summary><pre style="white-space:pre-wrap">${escapeHTML(JSON.stringify(result, null, 2))}</pre></details>
    </div>`;
    setResult("#query-result", html, false, true);
  } catch (error) {
    showError(error, "#query-result");
  }
}

function updateQueryFields() {
  const kind = $("#query-kind").value;
  $("#eq-field").classList.toggle("hidden", kind !== "eq");
  $("#in-field").classList.toggle("hidden", kind !== "in");
  $("#range-fields").classList.toggle("hidden", kind !== "range");
}

function bindEvents() {
  $("#load-snapshot-sample").addEventListener("click", async () => {
    if (!app.samples) await loadSamples();
    $("#snapshot-editor").value = JSON.stringify(app.samples.snapshot_v1, null, 2);
  });
  $("#load-current-snapshot").addEventListener("click", () => {
    if (!app.state?.snapshot) {
      toast("当前还没有已提交快照，请先载入内置 v1", true);
      return;
    }
    $("#snapshot-editor").value = JSON.stringify(app.state.snapshot, null, 2);
    toast(`已载入当前快照 v${app.state.current_version}；提交时会自动提升版本`);
  });
  $("#load-increment-sample").addEventListener("click", async () => {
    if (!app.samples) await loadSamples();
    const increment = structuredClone(app.samples.increment_v2);
    if (app.state?.current_version) increment.base_version = app.state.current_version;
    increment.commit_id = `ui-inc-${Date.now()}`;
    $("#increment-editor").value = JSON.stringify(increment, null, 2);
  });
  $("#commit-snapshot").addEventListener("click", () => submitSnapshot());
  $("#commit-increment").addEventListener("click", () => submitIncrement());
  $$("[data-snapshot-fault]").forEach((button) => button.addEventListener("click", () => submitSnapshot(button.dataset.snapshotFault)));
  $$("[data-increment-fault]").forEach((button) => button.addEventListener("click", () => submitIncrement(button.dataset.incrementFault)));
  $("#inject-interrupt").addEventListener("click", injectInterrupt);
  $("#run-query").addEventListener("click", () => runQuery());
  $("#inject-observed-ahead").addEventListener("click", () => runQuery("observed_ahead"));
  $("#query-kind").addEventListener("change", updateQueryFields);
  $("#refresh-all").addEventListener("click", refreshAll);
  $("#clear-action").addEventListener("click", () => setResult("#action-result", "操作结果与业务码显示在此"));
}

async function boot() {
  bindEvents();
  updateQueryFields();
  await Promise.all([loadSamples(), refreshAll()]);
}

boot();
