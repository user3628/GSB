export const state = {
  snapshot: null,
  intents: [],
  previews: {},
  selected: null,
  mode: 'full',
  filter: 'all',
  query: '',
  loading: true,
  lastLatencyMS: null,
  lastLoadedAt: null,
  busy: false,
};

export async function api(path, options = {}) {
  const started = performance.now();
  const request = { ...options, headers: { ...(options.headers || {}) } };
  if (request.body && typeof request.body !== 'string') {
    request.headers['Content-Type'] = 'application/json';
    request.body = JSON.stringify(request.body);
  }
  const response = await fetch(path, request);
  let payload;
  try {
    payload = await response.json();
  } catch {
    throw Object.assign(new Error('响应不是合法 JSON'), { code: 'E_BAD_RESPONSE' });
  }
  state.lastLatencyMS = Math.round((performance.now() - started) * 10) / 10;
  if (!response.ok || !payload.ok) {
    const error = new Error(payload?.error?.message || `HTTP ${response.status}`);
    error.code = payload?.error?.code || `HTTP_${response.status}`;
    error.context = payload?.error?.context || {};
    throw error;
  }
  return payload.data;
}

export async function loadAll({ preserveSelection = true } = {}) {
  if (!state.snapshot) state.loading = true;
  const [snapshot, intentData] = await Promise.all([
    api('/api/snapshot'),
    api('/api/intents?limit=14').catch(() => ({ intents: [] })),
  ]);
  state.snapshot = snapshot;
  state.intents = intentData.intents || [];
  const batches = snapshot.batches || [];
  const previous = preserveSelection ? state.selected : null;
  if (!batches.some((batch) => batch.id === previous)) {
    state.selected = batches[0]?.id || null;
  }
  const previews = await Promise.all(batches.map(async (batch) => {
    try {
      const preview = await api(`/api/batches/${encodeURIComponent(batch.id)}/risk?mode=compare`);
      return [batch.id, { preview }];
    } catch (error) {
      return [batch.id, { error: serializeError(error) }];
    }
  }));
  state.previews = Object.fromEntries(previews);
  state.loading = false;
  state.lastLoadedAt = Date.now();
}

export async function refreshSelected() {
  if (!state.selected) return;
  try {
    const preview = await api(`/api/batches/${encodeURIComponent(state.selected)}/risk?mode=compare`);
    state.previews[state.selected] = { preview };
  } catch (error) {
    state.previews[state.selected] = { error: serializeError(error) };
  }
}

export async function seedScenario() {
  state.busy = true;
  try {
    const result = await api('/api/demo/seed', { method: 'POST', body: {} });
    state.snapshot = null;
    state.previews = {};
    state.selected = null;
    state.filter = 'all';
    state.query = '';
    await loadAll({ preserveSelection: false });
    return result;
  } finally {
    state.busy = false;
  }
}

export async function evaluateSelected(mode = state.mode) {
  const batchID = state.selected;
  state.busy = true;
  try {
    const result = await api(`/api/batches/${encodeURIComponent(batchID)}/evaluate?mode=${encodeURIComponent(mode)}`, {
      method: 'POST', body: {},
    });
    await loadAll();
    return result;
  } finally {
    state.busy = false;
  }
}

export async function rollbackSelected() {
  const batchID = state.selected;
  state.busy = true;
  try {
    const result = await api(`/api/batches/${encodeURIComponent(batchID)}/rollback`, {
      method: 'POST', body: { reason: 'workbench manual rollback' },
    });
    await loadAll();
    return result;
  } finally {
    state.busy = false;
  }
}

export async function ingestObservation(batchID, observation) {
  const result = await api(`/api/batches/${encodeURIComponent(batchID)}/observations`, {
    method: 'POST', body: { observations: [observation] },
  });
  await loadAll();
  return result;
}

export function selectedBatch() {
  return state.snapshot?.batches?.find((batch) => batch.id === state.selected) || null;
}

export function selectedPreview() {
  return state.previews[state.selected] || null;
}

export function selectedEvaluation() {
  const preview = selectedPreview();
  const comparison = preview?.preview?.comparison;
  if (!comparison) return null;
  return state.mode === 'fast' ? comparison.fast : comparison.full;
}

export function previewFor(batchID) {
  return state.previews[batchID] || null;
}

export function serializeError(error) {
  return { code: error.code || 'E_INTERNAL', message: error.message, context: error.context || {} };
}

export function esc(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;').replaceAll("'", '&#39;');
}

export function num(value) {
  return new Intl.NumberFormat('zh-CN').format(Number(value || 0));
}

export function ppmPercent(value, digits = 3) {
  const n = Number(value || 0) / 10000;
  return `${n.toFixed(digits).replace(/\.0+$/, '').replace(/(\.\d*?)0+$/, '$1')}%`;
}

export function pct(value, digits = 1) {
  return `${(Number(value || 0) * 100).toFixed(digits)}%`;
}

export function shortHash(value, head = 8) {
  const text = String(value || '');
  if (text.length <= head + 4) return text;
  return `${text.slice(0, head)}…${text.slice(-4)}`;
}

export function fmtDate(ms, withSeconds = true) {
  if (!ms) return '—';
  const date = new Date(Number(ms));
  return new Intl.DateTimeFormat('zh-CN', {
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: withSeconds ? '2-digit' : undefined, hour12: false,
  }).format(date);
}

export function fmtTime(ms) {
  if (!ms) return '—';
  return new Intl.DateTimeFormat('zh-CN', {
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  }).format(new Date(Number(ms)));
}

export function duration(ms) {
  const value = Math.max(0, Number(ms || 0));
  if (value < 1000) return `${value}ms`;
  if (value < 60000) return `${(value / 1000).toFixed(value % 1000 === 0 ? 0 : 1)}s`;
  const minutes = Math.floor(value / 60000);
  const seconds = Math.floor((value % 60000) / 1000);
  return `${minutes}m ${seconds}s`;
}

export function statusLabel(status) {
  return {
    OBSERVING: '观察中', PASSED: '已放行', ROLLING_BACK: '回滚中',
    ROLLED_BACK: '已回滚', ESCALATED: '已升级',
  }[status] || status;
}

export function changeTypeLabel(type) {
  return { config_deploy: '配置下发', image_upgrade: '镜像升级', scale: '扩缩容' }[type] || type;
}

export function levelLabel(level) {
  return { LOW: '低风险', MEDIUM: '中风险', HIGH: '高风险', EXTREME: '极高风险' }[level] || level || '—';
}

export function actionLabel(action) {
  return {
    PASS: '放行', ROLLBACK: '自动回滚', ESCALATE: '升级人工', CONTINUE_OBSERVING: '继续观察',
    ROLLBACK_BLOCKED_COOLDOWN: '冷却期拦截',
  }[action] || action || '—';
}

export function metricLabel(metric) {
  return {
    error_rate: '错误率', latency_ms: 'P90 延迟', success_rate: '成功率',
    latency_degradation: 'P90 偏离', success_drop: '成功率下降',
  }[metric] || metric;
}

export function riskColor(level) {
  return { LOW: '#139a72', MEDIUM: '#d38b08', HIGH: '#e3651d', EXTREME: '#d92d45' }[level] || '#64748b';
}

export function riskSoft(level) {
  return { LOW: '#e8f7f1', MEDIUM: '#fff7e4', HIGH: '#fff1e8', EXTREME: '#ffedf0' }[level] || '#f1f5f9';
}

export function scoreOf(batchID, mode = state.mode) {
  const item = state.previews[batchID];
  const evaluation = item?.preview?.comparison?.[mode];
  return evaluation?.score_bp ?? null;
}
