const state = { status: null, selectedSlice: null };

const $ = (id) => document.getElementById(id);
const esc = (value) => String(value ?? '').replace(/[&<>"']/g, (ch) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
const short = (value, n = 14) => value ? `${value.slice(0, n)}…` : '—';
const now = () => new Date().toLocaleTimeString('zh-CN', {hour12: false});

function log(message, kind = 'info') {
  const line = document.createElement('div');
  line.className = `log-line ${kind === 'error' ? 'error' : ''}`;
  line.innerHTML = `<span class="time">${now()}</span><span class="kind">${esc(kind)}</span><span>${esc(message)}</span>`;
  $('log').prepend(line);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: {'Content-Type': 'application/json'},
    ...options,
  });
  let payload;
  try { payload = await response.json(); } catch { payload = {ok: false, error: {message: `HTTP ${response.status}`}}; }
  if (!response.ok || payload.ok === false) {
    const err = payload.error || {};
    throw new Error(`${err.code || 'ERROR'}: ${err.message || response.status}${err.band_index != null ? ` [band ${err.band_index}, block ${err.block_index}]` : ''}`);
  }
  return payload;
}

function setBusy(busy) {
  for (const id of ['importBtn','buildBtn','resumeBtn','refreshBtn']) $(id).disabled = busy;
  $('stopBtn').disabled = false;
}

async function refresh() {
  try {
    const health = await api('/api/health');
    $('health').className = 'health ok';
    $('health').innerHTML = `<span class="dot"></span><span>服务在线 · ${esc(health.port)}</span>`;
    state.status = await api('/api/status');
    render();
  } catch (error) {
    $('health').className = 'health bad';
    $('health').innerHTML = '<span class="dot"></span><span>连接失败</span>';
    log(error.message, 'error');
  }
}

function render() {
  const status = state.status;
  if (!status) return;
  const workspace = status.workspace || {};
  $('metricWorkspace').textContent = workspace.imported ? `${workspace.slice_count} 个分片` : '未导入';
  $('metricDataset').textContent = workspace.imported ? `${workspace.slice_count} / ${workspace.total_samples.toLocaleString()}` : '—';
  const config = status.config || {};
  $('metricConfig').textContent = config.band_count ? `${config.band_count} / ${config.frame_length} / ${config.hop_length}` : '—';
  const cp = status.checkpoints || {};
  $('metricCheckpoint').textContent = cp.latest_sequence != null ? `${cp.recoverable_sequence}/${cp.latest_sequence}` : '0';
  renderSlices(status.slices || []);
  renderCheckpoints(cp);
  renderStages((status.slices || []).find((item) => item.slice_id === state.selectedSlice));
  if (workspace.imported) {
    loadVersionAndInvalidations();
    loadChanges();
  }
}

function renderSlices(slices) {
  const body = $('sliceRows');
  const select = $('incSlice');
  if (!slices.length) {
    body.innerHTML = '<tr><td colspan="6" class="empty">导入工作区后显示分片</td></tr>';
    select.innerHTML = '';
    return;
  }
  body.innerHTML = slices.map((item) => `
    <tr>
      <td><strong>${esc(item.slice_id)}</strong><div class="muted">${Number(item.duration_seconds).toFixed(2)} s</div></td>
      <td><span class="badge ${item.complete ? 'complete' : 'stopped'}">${item.complete ? 'complete' : 'pending'}</span></td>
      <td class="mono">${item.n_samples.toLocaleString()}</td>
      <td>${item.source_count}</td>
      <td><span class="digest mono" title="${esc(item.result_digest)}">${esc(short(item.result_digest, 18))}</span></td>
      <td><button class="ghost" onclick="selectSlice('${esc(item.slice_id)}')">查看</button></td>
    </tr>`).join('');
  select.innerHTML = slices.map((item) => `<option value="${esc(item.slice_id)}">${esc(item.slice_id)}</option>`).join('');
  if (state.selectedSlice && slices.some((item) => item.slice_id === state.selectedSlice)) select.value = state.selectedSlice;
  state.selectedSlice = select.value || slices[0].slice_id;
  renderAudio(slices.find((item) => item.slice_id === state.selectedSlice));
  renderStages(slices.find((item) => item.slice_id === state.selectedSlice));
}

window.selectSlice = (sliceId) => {
  state.selectedSlice = sliceId;
  $('incSlice').value = sliceId;
  renderAudio((state.status?.slices || []).find((item) => item.slice_id === sliceId));
  renderStages((state.status?.slices || []).find((item) => item.slice_id === sliceId));
  loadVersionAndInvalidations();
  loadChanges();
  log(`已选择 ${sliceId}`);
};

function renderCheckpoints(cp) {
  const records = (cp.records || []).slice(-8).reverse();
  $('checkpointPanel').className = 'checkpoint-panel';
  $('checkpointPanel').innerHTML = `
    <div class="checkpoint-line"><span>可恢复 / 最新序号</span><strong>${cp.recoverable_sequence ?? 0} / ${cp.latest_sequence ?? 0}</strong></div>
    <div class="checkpoint-line"><span>损坏或重复记录</span><strong>${(cp.diagnostics || []).length}</strong></div>
    ${records.map((record) => `<div class="checkpoint-line"><span>#${record.sequence} · ${esc(record.stage)} · b${record.block_index}${record.round != null ? ` · r${record.round}` : ''}</span><strong class="mono">${esc(short(record.product_digest, 10))}</strong></div>`).join('') || '<div class="empty">尚无检查点</div>'}`;
}

async function loadVersionAndInvalidations() {
  if (!state.selectedSlice) return;
  try {
    const [version, invalidation] = await Promise.all([
      api(`/api/version?slice_id=${encodeURIComponent(state.selectedSlice)}`),
      api(`/api/invalidations?slice_id=${encodeURIComponent(state.selectedSlice)}`),
    ]);
    $('versionPanel').textContent = JSON.stringify(version.slices?.[0] || {}, null, 2);
    $('invalidationPanel').textContent = JSON.stringify(invalidation.slices?.[0]?.records || [], null, 2);
  } catch (error) { log(error.message, 'error'); }
}

async function loadChanges() {
  if (!state.selectedSlice) return;
  try {
    const changes = await api(`/api/mask-changes?slice_id=${encodeURIComponent(state.selectedSlice)}`);
    const row = changes.slices?.[0];
    const panel = $('changesPanel');
    if (!row?.complete || !row.rounds?.length) {
      panel.className = 'changes empty';
      panel.textContent = '尚无逐轮变化数据';
      return;
    }
    panel.className = 'changes';
    const maxima = row.rounds.map((values) => Math.max(...values.map(Math.abs), 1e-15));
    panel.innerHTML = row.rounds.map((values, round) => `
      <div class="round-row">
        <span class="round-label">ROUND ${round + 1}</span>
        <div class="bars">${values.map((value, band) => `<div class="bar" title="band ${band}: ${value}" style="height:${Math.max(2, (Math.abs(value) / maxima[round]) * 26)}px"></div>`).join('')}</div>
      </div>`).join('') + `<div class="hint mono">${esc(row.round_changes_digest || '')}</div>`;
  } catch (error) { log(error.message, 'error'); }
}

function renderAudio(slice) {
  const panel = $('audioPanel');
  if (!slice?.complete) {
    panel.className = 'audio-panel empty';
    panel.textContent = '该分片尚未完成构建';
    return;
  }
  panel.className = 'audio-panel';
  panel.innerHTML = Array.from({length: slice.source_count}, (_, index) => `
    <div class="audio-item">
      <strong>${esc(slice.slice_id)} · source ${index}</strong>
      <audio controls preload="none" src="/api/audio/${encodeURIComponent(slice.slice_id)}/source-${String(index).padStart(3,'0')}.wav"></audio>
    </div>`).join('') + `
    <div class="checkpoint-line"><span>MASK DIGEST</span><strong class="mono">${esc(short(slice.mask_digest, 20))}</strong></div>
    <div class="checkpoint-line"><span>ROUND CHANGES</span><strong class="mono">${esc(short(slice.round_changes_digest, 20))}</strong></div>`;
}

function renderStages(slice) {
  const panel = $('stagePanel');
  if (!slice?.stage_products) {
    panel.className = 'stage-panel empty';
    panel.textContent = '构建后显示各阶段产物摘要';
    return;
  }
  const names = [
    ['stft', 'STFT 谱矩阵'],
    ['features', '频带特征'],
    ['mask_init', '掩码初始化'],
    ['mask_refine', '迭代掩码精化'],
    ['reconstruct', '重叠相加重建'],
  ];
  panel.className = 'stage-panel';
  const metrics = slice.last_build?.stage_metrics || {};
  panel.innerHTML = names.map(([key, label], index) => `
    <div class="stage">
      <div><div class="round-label">STAGE ${index + 1}</div><div class="stage-name">${label}</div></div>
      <span class="badge ${metrics[key]?.status === 'reused' ? 'reused' : 'complete'}">${esc(metrics[key]?.status || 'ready')}</span>
      <p class="mono" title="${esc(slice.stage_products[key])}">${esc(slice.stage_products[key] || '—')}${metrics[key] ? ` · ${metrics[key].blocks_recomputed?.length || 0} blocks` : ''}</p>
    </div>`).join('');
}

async function runAction(label, task) {
  setBusy(true);
  log(`${label}开始`);
  try {
    const result = await task();
    log(`${label}完成${result.status ? ` · ${result.status}` : ''}`);
    await refresh();
    return result;
  } catch (error) {
    log(error.message, 'error');
    throw error;
  } finally {
    setBusy(false);
  }
}

$('importBtn').onclick = () => runAction('导入', () => api('/api/import', {method: 'POST', body: '{}'}));
$('buildBtn').onclick = () => {
  const stopAfter = $('stopAfter').value.trim();
  const payload = {slice_id: 'all'};
  if (stopAfter !== '') payload.stop_after_units = Number(stopAfter);
  runAction('构建', () => api('/api/build', {method: 'POST', body: JSON.stringify(payload)}));
};
$('resumeBtn').onclick = () => runAction('恢复', () => api('/api/resume', {method: 'POST', body: JSON.stringify({slice_id: 'all'})}));
$('stopBtn').onclick = async () => {
  try { await api('/api/stop', {method: 'POST', body: '{}'}); log('已写入优雅停止请求'); }
  catch (error) { log(error.message, 'error'); }
};
$('refreshBtn').onclick = () => refresh();
$('clearLog').onclick = () => { $('log').innerHTML = ''; };
$('incrementalForm').onsubmit = (event) => {
  event.preventDefault();
  const payload = {
    slice_id: $('incSlice').value,
    source_id: Number($('incSource').value),
    bands: [Number($('incBandStart').value), Number($('incBandEnd').value)],
  };
  if ($('incF0Min').value.trim()) payload.f0_min_hz = Number($('incF0Min').value);
  if ($('incF0Max').value.trim()) payload.f0_max_hz = Number($('incF0Max').value);
  runAction('频带增量更新', async () => {
    const result = await api('/api/incremental', {method: 'POST', body: JSON.stringify(payload)});
    log(`全量等价=${result.full_equivalent} · 频带=${JSON.stringify(result.affected_bands)} · 块=${result.rebuilt_blocks.length}`);
    return result;
  });
};

refresh();
setInterval(() => { if (!document.hidden) refresh(); }, 5000);

