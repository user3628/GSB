const state = {
  snapshotVersion: null,
  environments: [],
  selectedEnvironment: null,
  bundle: null,
  diagnostics: null,
  recovery: null,
  cascade: [],
  cycleFailure: null,
  busy: false,
};

const ACTIONS = {
  'full-v1': () => commitSample('snapshot_v1', '/api/commits/full'),
  'full-v2': () => commitSample('snapshot_v2', '/api/commits/full'),
  increment: () => commitSample('increment_image', '/api/commits/incremental'),
  cycle: injectCycle,
  refresh: () => loadAll(state.snapshotVersion),
  'scan-recovery': scanRecovery,
  'close-refs': closeReferences,
};

document.addEventListener('click', (event) => {
  const actionButton = event.target.closest('[data-action]');
  if (actionButton) {
    const action = ACTIONS[actionButton.dataset.action];
    if (action && !state.busy) {
      action();
    }
    return;
  }
  const environmentCard = event.target.closest('[data-environment]');
  if (environmentCard) {
    selectEnvironment(environmentCard.dataset.environment);
    return;
  }
  const referenceButton = event.target.closest('[data-reference-service]');
  if (referenceButton) {
    loadReferences(referenceButton.dataset.referenceService);
  }
});

document.getElementById('service-filter').addEventListener('input', () => renderPlan());

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
    ...options,
  });
  let body;
  try {
    body = await response.json();
  } catch {
    body = { ok: false, error: { code: 'E_NON_JSON', message: `HTTP ${response.status}` } };
  }
  if (!response.ok || !body.ok) {
    const error = body.error || { code: 'E_UNKNOWN', message: '未知错误', details: {} };
    error.status = response.status;
    throw error;
  }
  return body.data;
}

async function commitSample(name, endpoint) {
  await withBusy(async () => {
    const payload = await api(`/api/samples/${name}`);
    const result = await api(endpoint, {
      method: 'POST',
      body: JSON.stringify(payload),
    });
    state.cascade = result.cascade_trace || [];
    state.cycleFailure = null;
    state.selectedEnvironment = result.affected?.environment || state.selectedEnvironment;
    showToast(`${result.idempotent_replay ? '幂等复用' : '提交成功'} · 快照 v${result.snapshot_version}`, 'success');
    await loadAll(result.snapshot_version, { preserveSelection: true });
  });
}

async function injectCycle() {
  await withBusy(async () => {
    const payload = await api('/api/samples/cycle_increment');
    try {
      const result = await api('/api/commits/incremental', {
        method: 'POST',
        body: JSON.stringify(payload),
      });
      state.cascade = result.cascade_trace || [];
      showToast(`意外提交成功 · v${result.snapshot_version}`, 'error');
      await loadAll(result.snapshot_version);
    } catch (error) {
      if (error.code === 'E_DEPENDENCY_CYCLE') {
        state.cycleFailure = error.details;
        state.selectedEnvironment = error.details.environment;
        renderCycleFailure();
        showToast(`依赖环已拒绝：${(error.details.cycle || []).join(' → ')}`, 'error');
      } else {
        throw error;
      }
    }
  });
}

async function scanRecovery() {
  await withBusy(async () => {
    const result = await api('/api/recovery/scan', { method: 'POST' });
    state.recovery = result.report;
    renderRecovery();
    showToast(`扫描完成 · 终止意图 ${result.recovery_scan.recovered.length} 个`, 'success');
  });
}

async function loadAll(version = null, { preserveSelection = false } = {}) {
  const versionQuery = version ? `?version=${version}` : '';
  const [environmentData, diagnostics, recovery] = await Promise.all([
    api(`/api/environments${versionQuery}`),
    api(`/api/diagnostics${versionQuery}`),
    api('/api/recovery'),
  ]);
  state.snapshotVersion = environmentData.snapshot_version;
  state.environments = environmentData.environments;
  state.diagnostics = diagnostics;
  state.recovery = recovery;
  const stillPresent = state.environments.some((environment) => environment.environment === state.selectedEnvironment);
  if (!preserveSelection || !stillPresent) {
    state.selectedEnvironment = state.environments.find((environment) => environment.is_baseline)?.environment
      || state.environments[0]?.environment
      || null;
  }
  if (state.selectedEnvironment) {
    state.bundle = await api(`/api/environments/${state.selectedEnvironment}/bundle${versionQuery}`);
  } else {
    state.bundle = null;
  }
  renderAll();
}

async function selectEnvironment(environment) {
  if (state.selectedEnvironment === environment) return;
  state.selectedEnvironment = environment;
  state.cycleFailure = null;
  await withBusy(async () => {
    const suffix = state.snapshotVersion ? `?version=${state.snapshotVersion}` : '';
    state.bundle = await api(`/api/environments/${environment}/bundle${suffix}`);
    renderAll();
  });
}

async function loadReferences(service) {
  if (!state.selectedEnvironment) return;
  try {
    const versionQuery = state.snapshotVersion ? `?version=${state.snapshotVersion}&subject=${encodeURIComponent(service)}` : `?subject=${encodeURIComponent(service)}`;
    const result = await api(`/api/environments/${state.selectedEnvironment}/references${versionQuery}`);
    document.getElementById('refs-panel').classList.remove('hidden');
    document.getElementById('refs-subject').textContent = `${state.selectedEnvironment} / ${service}`;
    document.getElementById('refs-content').innerHTML = result.references.length
      ? result.references.map((reference) => `<span class="ref-chip"><b>${esc(reference.subject_type)}</b> ${esc(reference.subject_key)} → ${esc(reference.artifact_type)} / ${esc(reference.artifact_key)}</span>`).join('')
      : '<span class="empty-inline">未找到引用</span>';
    document.getElementById('refs-panel').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  } catch (error) {
    showToast(`${error.code}: ${error.message}`, 'error');
  }
}

function closeReferences() {
  document.getElementById('refs-panel').classList.add('hidden');
}

async function withBusy(operation) {
  if (state.busy) return;
  state.busy = true;
  setButtonsDisabled(true);
  try {
    await operation();
    document.getElementById('health-dot').classList.add('ok');
    document.getElementById('health-text').textContent = '本地工作台在线 · 127.0.0.1';
  } catch (error) {
    showToast(`${error.code || 'E_UNKNOWN'}: ${error.message || '执行失败'}`, 'error');
  } finally {
    state.busy = false;
    setButtonsDisabled(false);
  }
}

function setButtonsDisabled(disabled) {
  document.querySelectorAll('button[data-action]').forEach((button) => {
    button.disabled = disabled;
  });
}

function renderAll() {
  document.getElementById('version-pill').textContent = `v${state.snapshotVersion ?? '—'}`;
  renderKpis();
  renderEnvironments();
  renderTopology();
  renderStateSummary();
  renderPlan();
  renderDiagnostics();
  renderCascade();
  renderRecovery();
  document.getElementById('selected-env-label').textContent = state.selectedEnvironment
    ? `当前：${state.selectedEnvironment}`
    : '未选择环境';
}

function renderKpis() {
  const counts = state.diagnostics?.counts || {};
  const severity = (counts.critical || 0) + (counts.high || 0);
  const pending = state.recovery?.pending_intents ?? 0;
  const values = [
    [state.snapshotVersion ?? '—', '全局单调版本'],
    [state.diagnostics?.items?.length ?? 0, '最新只读校验'],
    [severity, 'critical + high'],
    [pending, 'SQLite 已落盘事实'],
  ];
  const cards = document.querySelectorAll('#kpi-grid .kpi strong');
  cards.forEach((card, index) => {
    card.textContent = values[index][0];
  });
}

function renderEnvironments() {
  const container = document.getElementById('environment-grid');
  if (!state.environments.length) {
    container.innerHTML = '<div class="empty-card panel">尚无已提交快照。点击“提交全量 v1”开始。</div>';
    return;
  }
  container.innerHTML = state.environments.map((environment) => {
    const score = environment.consistency || {};
    const lag = Boolean(score.lag_exceeded);
    const selected = environment.environment === state.selectedEnvironment;
    const badges = [
      environment.is_baseline ? '<span class="env-badge base">BASELINE</span>' : '',
      lag ? '<span class="env-badge warn">LAG</span>' : '',
    ].join('');
    return `
      <button class="environment-card ${selected ? 'selected' : ''} ${environment.is_baseline ? 'baseline' : ''} ${lag ? 'lag' : ''}" data-environment="${esc(environment.environment)}">
        <div class="env-top">
          <div class="env-name">${esc(environment.environment)}</div>
          <div>${badges}</div>
        </div>
        <div class="env-score-row"><span class="env-score">${formatScore(score.score)}</span><span class="env-score-label">/ 100 一致度</span></div>
        <div class="score-bar"><i style="width:${Math.max(0, Math.min(100, Number(score.score) || 0))}%"></i></div>
        <div class="env-meta">
          <div>部署代号<b>#${environment.env_version}</b></div>
          <div>单元数<b>${environment.unit_count}</b></div>
          <div>版本级差<b>${score.behind_by ?? 0} / 容忍 ${environment.version_lag_tolerance}</b></div>
        </div>
      </button>`;
  }).join('');
}

function renderTopology() {
  const container = document.getElementById('topology');
  const failure = state.cycleFailure;
  if (failure) {
    document.getElementById('topology-state').textContent = `E_DEPENDENCY_CYCLE · ${failure.environment}`;
    document.getElementById('topology-state').style.color = 'var(--red)';
    const layers = failure.partial_layers || [];
    container.innerHTML = [
      ...layers.map((layer, index) => `
        <div class="topology-layer">
          <div class="layer-label">Layer ${index + 1} · 未污染</div>
          ${layer.map((service) => `<div class="service-chip">${esc(service)}</div>`).join('')}
        </div>`),
      `<div class="topology-layer">
        <div class="layer-label">Blocked · 受阻单元</div>
        ${(failure.blocked_units || []).map((service) => `<div class="service-chip blocked-chip">${esc(service)}</div>`).join('')}
      </div>`,
    ].join('');
    return;
  }
  const topology = state.bundle?.topology;
  if (!topology) {
    container.innerHTML = '<div class="empty-inline">暂无拓扑</div>';
    document.getElementById('topology-state').textContent = '等待快照';
    return;
  }
  document.getElementById('topology-state').textContent = topology.complete ? '完整分层' : '降级分层';
  document.getElementById('topology-state').style.color = '';
  container.innerHTML = `
    ${(topology.layers || []).map((layer, index) => `
      <div class="topology-layer">
        <div class="layer-label">Layer ${index + 1}</div>
        ${layer.map((service) => `<div class="service-chip">${esc(service)}</div>`).join('')}
      </div>`).join('')}
    ${(topology.blocked_units || []).length ? `<div class="topology-layer"><div class="layer-label">Blocked</div>${topology.blocked_units.map((service) => `<div class="service-chip blocked-chip">${esc(service)}</div>`).join('')}</div>` : ''}
  `;
}

function renderStateSummary() {
  const container = document.getElementById('state-summary');
  const bundle = state.bundle;
  if (!bundle?.expected_state) {
    container.innerHTML = '<div class="empty-inline">暂无摘要</div>';
    document.getElementById('score-pill').textContent = '—';
    return;
  }
  const summary = bundle.expected_state;
  const score = bundle.consistency || {};
  document.getElementById('score-pill').textContent = `一致度 ${formatScore(score.score)}`;
  const components = score.component_similarity || {};
  container.innerHTML = `
    <div class="state-row"><span>环境 / 代号</span><code>${esc(summary.environment)} · #${summary.environment_version}</code></div>
    <div class="state-row"><span>服务 / 配置键</span><code>${summary.service_count} / ${summary.config_key_count}</code></div>
    <div class="state-row"><span>状态哈希</span><code title="${esc(summary.state_hash)}">${esc(shorten(summary.state_hash, 24))}</code></div>
    <div class="state-row"><span>相对基线</span><code>${esc(score.baseline_environment || '—')} · lag ${score.version_lag ?? 0}</code></div>
    <div class="component-grid">
      ${componentCard('镜像', components.image)}
      ${componentCard('期望版本', components.expected_version)}
      ${componentCard('配置', components.config)}
      ${componentCard('依赖', components.dependencies)}
    </div>`;
}

function componentCard(label, value) {
  return `<div class="component">${esc(label)}<b>${Math.round((Number(value) || 0) * 100)}%</b></div>`;
}

function renderPlan() {
  const body = document.getElementById('plan-body');
  const filter = document.getElementById('service-filter').value.trim().toLowerCase();
  const plan = state.bundle?.plan || [];
  const filtered = filter ? plan.filter((item) => item.service.toLowerCase().includes(filter)) : plan;
  if (!filtered.length) {
    body.innerHTML = '<tr><td colspan="7" class="empty-row">暂无部署计划</td></tr>';
    return;
  }
  body.innerHTML = filtered.map((item) => `
    <tr>
      <td>${item.order}</td>
      <td>L${(item.layer ?? 0) + 1}</td>
      <td><span class="service-name">${esc(item.service)}</span></td>
      <td><code title="${esc(item.image_digest)}">${esc(shorten(item.image_digest, 22))}</code></td>
      <td><code>${esc(item.expected_version)}</code></td>
      <td class="dep-list">${item.dependencies.length ? item.dependencies.map(esc).join(', ') : '—'}</td>
      <td><button class="tiny-link" data-reference-service="${esc(item.service)}">查看</button></td>
    </tr>`).join('');
}

function renderDiagnostics() {
  const container = document.getElementById('diagnostics-body');
  const items = state.diagnostics?.items || [];
  const counts = state.diagnostics?.counts || {};
  document.getElementById('severity-strip').innerHTML = ['critical', 'high', 'medium', 'low']
    .map((severity) => `<span class="severity-count ${severity}">${severity.toUpperCase()} ${counts[severity] || 0}</span>`)
    .join('');
  const lagMarkers = state.diagnostics?.environment_lag_markers || [];
  const banner = document.getElementById('lag-banner');
  if (lagMarkers.length) {
    banner.classList.remove('hidden');
    banner.innerHTML = lagMarkers.map((marker) => `${esc(marker.environment)} 落后基线 ${marker.behind_by} 个代号（容忍 ${marker.tolerance}）`).join(' · ');
  } else {
    banner.classList.add('hidden');
    banner.textContent = '';
  }
  if (!items.length) {
    container.innerHTML = '<tr><td colspan="6" class="empty-row">未发现差异</td></tr>';
    return;
  }
  container.innerHTML = items.map((item) => `
    <tr>
      <td><span class="severity-badge ${esc(item.severity)}">${esc(item.severity)}</span></td>
      <td>${esc(item.environment)}</td>
      <td>${item.service ? `<span class="service-name">${esc(item.service)}</span>${item.config_key ? `<br><code>${esc(item.config_key)}</code>` : ''}` : '<span class="dep-list">环境级</span>'}</td>
      <td><code>${esc(item.diff_type)}</code></td>
      <td><code>${esc(formatValue(item.left?.value))}</code></td>
      <td><code>${esc(formatValue(item.right?.value))}</code></td>
    </tr>`).join('');
}

function renderCascade() {
  const container = document.getElementById('cascade-trace');
  document.getElementById('cascade-count').textContent = `${state.cascade.length} events`;
  if (!state.cascade.length) {
    container.innerHTML = '<div class="empty-inline">提交增量后显示本次原子重建轨迹。</div>';
    return;
  }
  container.innerHTML = state.cascade.slice(0, 120).map((event) => `
    <div class="cascade-item">
      <span class="cascade-seq">#${event.seq ?? '—'}</span>
      <span class="cascade-subject">${esc(event.environment)} · ${esc(event.subject_type)} / ${esc(event.subject_key)}
        <small>→ ${esc(event.artifact_type)} / ${esc(event.artifact_key)}</small>
      </span>
      <span class="reason-pill">${esc(event.reason)}</span>
    </div>`).join('');
}

function renderRecovery() {
  const container = document.getElementById('recovery-report');
  const report = state.recovery;
  if (!report) {
    container.innerHTML = '<div class="empty-inline">暂无恢复报告</div>';
    return;
  }
  const events = report.recovered_events || [];
  container.innerHTML = `
    <div class="recovery-stat"><span>最后完整快照</span><b>v${report.last_complete_version}</b></div>
    <div class="recovery-stat"><span>已提交完整版本</span><b>${report.complete_revisions}</b></div>
    <div class="recovery-stat"><span>未完成意图</span><b>${report.pending_intents}</b></div>
    <div class="recovery-stat"><span>强制终止退出报告</span><b>不承诺</b></div>
    ${events.length ? events.slice(0, 6).map((event) => `
      <div class="recovery-event">${esc(event.commit_id)} · ${esc(event.status)}
        <small>${esc(event.recovered_at)} · last complete v${event.last_complete_version}</small>
      </div>`).join('') : '<div class="empty-inline">尚无已完成恢复扫描。启动时自动扫描，也可手动触发。</div>'}
    <div class="empty-inline">${esc(report.statement)}</div>`;
}

function renderCycleFailure() {
  const failure = state.cycleFailure;
  if (!failure) return;
  document.getElementById('topology-state').textContent = `E_DEPENDENCY_CYCLE · ${failure.environment}`;
  document.getElementById('topology-state').style.color = 'var(--red)';
  renderTopology();
  document.getElementById('topology').scrollIntoView({ behavior: 'smooth', block: 'center' });
}

function showToast(message, kind = 'success') {
  const toast = document.getElementById('toast');
  toast.textContent = message;
  toast.className = `toast show ${kind}`;
  clearTimeout(showToast.timer);
  showToast.timer = setTimeout(() => {
    toast.className = 'toast';
  }, 4200);
}

function formatScore(value) {
  return Number.isFinite(Number(value)) ? Number(value).toFixed(1) : '—';
}

function formatValue(value) {
  if (value === null || value === undefined) return '∅';
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

function shorten(value, length) {
  const text = String(value ?? '');
  return text.length > length ? `${text.slice(0, length)}…` : text;
}

function esc(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

(async function boot() {
  try {
    const health = await api('/api/health');
    document.getElementById('health-dot').classList.add('ok');
    document.getElementById('health-text').textContent = `本地工作台在线 · 快照 v${health.snapshot_version}`;
    await loadAll();
  } catch (error) {
    if (error.code === 'E_VERSION_RETIRED' || error.code === 'E_ENV_NOT_FOUND') {
      document.getElementById('health-dot').classList.add('ok');
      document.getElementById('health-text').textContent = '本地工作台在线 · 等待首个快照';
      state.environments = [];
      state.selectedEnvironment = null;
      renderAll();
      showToast('工作台为空，提交全量 v1 开始', 'success');
    } else {
      document.getElementById('health-text').textContent = '本地工作台连接失败';
      showToast(`${error.code}: ${error.message}`, 'error');
    }
  }
})();
