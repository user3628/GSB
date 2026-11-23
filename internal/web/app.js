import {
  state, loadAll, refreshSelected, seedScenario, evaluateSelected, rollbackSelected,
  ingestObservation, selectedBatch, esc, num, duration, fmtDate,
} from './core.js';
import { render, showToast } from './render.js';

const main = () => document.getElementById('main');

async function boot() {
  try {
    await loadAll({ preserveSelection: false });
    render();
  } catch (error) {
    state.loading = false;
    main().innerHTML = `
      <header class="topbar"><div class="crumbs"><span>本地运维域</span><span class="crumb-sep">/</span><strong>风险裁决工作台</strong></div></header>
      <div class="panel" style="margin-top:32px"><div class="empty-state">
        <div class="empty-visual">!</div><h2>无法读取工作台快照</h2>
        <p>${esc(error.message)} · ${esc(error.code || 'E_INTERNAL')}</p>
        <button class="btn btn-primary" data-refresh>重新连接</button>
      </div></div>`;
  }
}

async function refresh(showSuccess = true) {
  if (state.busy) return;
  state.busy = true;
  try {
    await loadAll();
    render();
    if (showSuccess) showToast('success', '快照已刷新', `策略与观测在提交 #${state.snapshot.as_of_commit_seq} 上形成单一视图`);
  } catch (error) {
    showToast('error', '刷新失败', error.message);
  } finally {
    state.busy = false;
  }
}

async function seed() {
  if (state.busy) return;
  const hasData = (state.snapshot?.batches?.length || 0) > 0;
  if (hasData && !window.confirm('这会清空当前本地数据库并重新载入固定演示样本，确认继续？')) return;
  state.busy = true;
  render();
  try {
    const result = await seedScenario();
    render();
    showToast('success', '演示场景已就绪', `策略 v${result.policy_version} · ${result.batches} 个批次 · ${Object.values(result.ingested).reduce((a, b) => a + b, 0)} 份观测`);
  } catch (error) {
    showToast('error', '样本载入失败', error.message);
  } finally {
    state.busy = false;
    render();
  }
}

async function evaluate() {
  if (state.busy) return;
  render();
  try {
    const result = await evaluateSelected(state.mode);
    render();
    const action = result?.evaluation?.decision?.action;
    if (result?.rollback && !result.rollback.success) {
      showToast('error', '回滚触达失败', result.rollback.error || '本地回滚端点返回失败');
    } else {
      showToast('success', `${state.mode === 'fast' ? '快速' : '完整'}裁决已提交`, `风险分 ${result.evaluation.score_bp} bp · 批次状态 ${result.batch.status}`);
    }
  } catch (error) {
    if (error.code === 'E_ROLLBACK_COOLDOWN') {
      const remaining = error.context?.remaining_ms;
      showToast('error', '冷却期拦截 E_ROLLBACK_COOLDOWN', `服务 ${error.context?.service || ''} 仍需冷却 ${duration(remaining)}`);
      await refresh(false);
    } else if (error.code === 'E_INSUFFICIENT_SAMPLES') {
      showToast('error', '样本不足 E_INSUFFICIENT_SAMPLES', '系统没有用零值伪造风险分，请先补充观测');
    } else {
      showToast('error', error.code || '裁决失败', error.message);
    }
  } finally {
    state.busy = false;
    render();
  }
}

async function rollback() {
  if (state.busy) return;
  if (!window.confirm('将通过本地回滚端点触发手动回滚，并在同一事务内占用服务冷却期。确认继续？')) return;
  state.busy = true;
  render();
  try {
    const result = await rollbackSelected();
    render();
    if (result.rollback?.success) showToast('success', '回滚已触达', `批次 ${result.batch.id} 已进入 ${result.batch.status}`);
    else showToast('error', '回滚触达失败', result.rollback?.error || '未知错误');
  } catch (error) {
    if (error.code === 'E_ROLLBACK_COOLDOWN') {
      showToast('error', '冷却期内禁止回滚', `剩余 ${duration(error.context?.remaining_ms)}`);
      await refresh(false);
    } else {
      showToast('error', error.code || '回滚失败', error.message);
    }
  } finally {
    state.busy = false;
    render();
  }
}

function openObservationModal() {
  const batch = selectedBatch();
  if (!batch || batch.status !== 'OBSERVING') return;
  const root = document.getElementById('modal-root');
  const defaultTimestamp = Number(batch.last_observation_ts_ms || batch.baseline.timestamp_ms) + 3000;
  root.innerHTML = `
    <div class="modal-backdrop" data-modal-backdrop>
      <div class="modal" role="dialog" aria-modal="true" aria-labelledby="obs-title">
        <div class="modal-head"><div><h3 id="obs-title">增量摄入观测</h3><p>${esc(batch.id)} · ${esc(batch.service)} · 提交后立即进入新的观测修订</p></div><button class="modal-close" data-close-modal aria-label="关闭">×</button></div>
        <form id="obs-form">
          <div class="modal-body">
            <div class="form-grid">
              <div class="field"><label for="obs-seq">序号 seq</label><input id="obs-seq" name="seq" type="number" min="1" value="${Date.now()}"><small>同一批次同一序号重复上报必须内容一致</small></div>
              <div class="field"><label for="obs-ts">时间戳 ms</label><input id="obs-ts" name="timestamp" type="number" min="1" value="${defaultTimestamp}"><small>不得早于批次最后观测 ${esc(batch.last_observation_ts_ms || '—')}</small></div>
              <div class="field"><label for="obs-metric">指标类型</label><select id="obs-metric" name="metric"><option value="error_rate">错误率 error_rate</option><option value="latency_ms">P90 延迟 latency_ms</option><option value="success_rate">成功率 success_rate</option></select></div>
              <div class="field" id="sample-field"><label for="obs-samples">样本数</label><input id="obs-samples" name="samples" type="number" min="0" value="5"></div>
              <div class="field" id="success-field"><label for="obs-success">成功数</label><input id="obs-success" name="success" type="number" min="0" value="5"><small>必须满足 0 ≤ success ≤ sample</small></div>
              <div class="field full" id="latency-field" style="display:none"><label for="obs-latency">延迟样本（逗号分隔，ms）</label><textarea id="obs-latency" name="latency">121,123,124,126,128</textarea><small>样本数将自动等于延迟值数量，使用最近秩法计算 P90</small></div>
            </div>
            <div class="form-error" id="obs-error"></div>
          </div>
          <div class="modal-foot"><button type="button" class="btn" data-close-modal>取消</button><button type="submit" class="btn btn-teal">提交单份观测</button></div>
        </form>
      </div>
    </div>`;
  const metric = root.querySelector('#obs-metric');
  const latencyField = root.querySelector('#latency-field');
  const successField = root.querySelector('#success-field');
  const sampleField = root.querySelector('#sample-field');
  metric.addEventListener('change', () => {
    const latency = metric.value === 'latency_ms';
    latencyField.style.display = latency ? 'grid' : 'none';
    successField.style.display = latency ? 'none' : 'grid';
    sampleField.style.display = latency ? 'none' : 'grid';
  });
  root.querySelector('#obs-form').addEventListener('submit', submitObservation);
  root.querySelector('#obs-seq').focus();
}

async function submitObservation(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const errorBox = document.getElementById('obs-error');
  errorBox.classList.remove('show');
  const batch = selectedBatch();
  if (!batch) return;
  const metric = form.metric.value;
  const timestamp = Number(form.timestamp.value);
  const seq = Number(form.seq.value);
  if (!Number.isSafeInteger(timestamp) || timestamp < Number(batch.last_observation_ts_ms || 0)) {
    showObservationError('时间戳必须是不小于最后已提交观测的整数毫秒时间戳。');
    return;
  }
  let observation = {
    seq,
    timestamp_ms: timestamp,
    metric_type: metric,
    sample_count: Number(form.samples.value || 0),
    success_count: Number(form.success.value || 0),
  };
  if (metric === 'latency_ms') {
    const values = String(form.latency.value || '').split(',').map((item) => item.trim()).filter(Boolean).map(Number);
    if (!values.length || values.some((value) => !Number.isSafeInteger(value) || value < 0)) {
      showObservationError('延迟样本必须是非负整数，以逗号分隔。');
      return;
    }
    observation.latency_samples_ms = values;
    observation.sample_count = values.length;
    observation.success_count = 0;
  } else {
    if (observation.success_count < 0 || observation.success_count > observation.sample_count) {
      showObservationError('成功数必须位于 0 与样本数之间。');
      return;
    }
  }
  const submit = form.querySelector('button[type="submit"]');
  submit.disabled = true;
  submit.textContent = '提交中…';
  try {
    const result = await ingestObservation(batch.id, observation);
    closeModal();
    render();
    showToast('success', '观测已原子提交', `${result.accepted} 份新增 · revision #${result.observation_revision} · commit #${result.commit_seq}`);
  } catch (error) {
    showObservationError(`${error.code || 'E_INTERNAL'}: ${error.message}`);
    submit.disabled = false;
    submit.textContent = '提交单份观测';
  }
}

function showObservationError(message) {
  const errorBox = document.getElementById('obs-error');
  errorBox.textContent = message;
  errorBox.classList.add('show');
  errorBox.scrollIntoView({ block: 'nearest' });
}

function closeModal() {
  document.getElementById('modal-root').innerHTML = '';
}

document.addEventListener('click', async (event) => {
  const backdrop = event.target.closest('[data-modal-backdrop]');
  if (backdrop && event.target === backdrop) {
    closeModal();
    return;
  }
  if (event.target.closest('[data-close-modal]')) {
    closeModal();
    return;
  }
  const refreshButton = event.target.closest('[data-refresh]');
  if (refreshButton) {
    await refresh();
    return;
  }
  const seedButton = event.target.closest('[data-seed]');
  if (seedButton) {
    await seed();
    return;
  }
  const selectButton = event.target.closest('[data-select]');
  if (selectButton) {
    state.selected = selectButton.dataset.select;
    render();
    return;
  }
  const filterButton = event.target.closest('[data-filter]');
  if (filterButton) {
    state.filter = filterButton.dataset.filter;
    render();
    return;
  }
  const modeButton = event.target.closest('[data-mode]');
  if (modeButton) {
    state.mode = modeButton.dataset.mode;
    render();
    return;
  }
  if (event.target.closest('[data-evaluate]')) {
    await evaluate();
    return;
  }
  if (event.target.closest('[data-rollback]')) {
    await rollback();
    return;
  }
  if (event.target.closest('[data-obs]')) {
    openObservationModal();
  }
});

document.addEventListener('input', (event) => {
  if (!event.target.matches('[data-search]')) return;
  state.query = event.target.value;
  const position = event.target.selectionStart;
  render();
  const search = document.querySelector('[data-search]');
  if (search) {
    search.focus();
    search.setSelectionRange(position, position);
  }
});

document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape') closeModal();
});

window.addEventListener('hashchange', () => {
  document.querySelectorAll('.nav-item').forEach((item) => item.classList.remove('active'));
  const active = document.querySelector(`.nav-item[href="${location.hash}"]`);
  if (active) active.classList.add('active');
});

boot();
