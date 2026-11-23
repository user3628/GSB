import {
  state, selectedBatch, selectedPreview, selectedEvaluation, previewFor,
  esc, num, ppmPercent, pct, shortHash, fmtDate, fmtTime, duration,
  statusLabel, changeTypeLabel, levelLabel, actionLabel, metricLabel,
  riskColor, riskSoft, scoreOf,
} from './core.js';

const ICON = {
  refresh: '<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M20 11a8.1 8.1 0 0 0-15.5-2M4 4v5h5M4 13a8.1 8.1 0 0 0 15.5 2M20 20v-5h-5"/></svg>',
  plus: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 5v14M5 12h14"/></svg>',
  play: '<svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor"><path d="m8 5 11 7-11 7V5Z"/></svg>',
  rollback: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M9 14 4 9l5-5M4 9h9a7 7 0 0 1 7 7v3"/></svg>',
  seed: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 3v18M5.6 6.5a9 9 0 0 0 0 11M18.4 6.5a9 9 0 0 1 0 11"/></svg>',
};

export function render() {
  renderRail();
  const main = document.getElementById('main');
  if (!main) return;
  if (state.loading) {
    main.innerHTML = '<div class="boot-screen"><div class="spinner"></div><span>正在读取持久化快照…</span></div>';
    return;
  }
  const snapshot = state.snapshot;
  const batches = snapshot?.batches || [];
  const body = batches.length
    ? renderWorkspace()
    : renderEmpty();
  main.innerHTML = `${renderTopbar()}${body}`;
}

function renderRail() {
  const rail = document.getElementById('rail-status');
  if (!rail) return;
  const policy = state.snapshot?.policy;
  rail.innerHTML = `
    <div class="rail-kv"><span>策略版本</span><span>${policy ? `v${esc(policy.version)}` : '—'}</span></div>
    <div class="rail-kv"><span>提交序号</span><span>#${esc(state.snapshot?.as_of_commit_seq ?? '—')}</span></div>
    <div class="rail-kv"><span>批次</span><span>${esc(state.snapshot?.batches?.length ?? 0)}</span></div>
    <div class="rail-kv"><span>冷却中</span><span>${esc(state.snapshot?.cooldowns?.length ?? 0)}</span></div>
  `;
}

function renderTopbar() {
  const latency = state.lastLatencyMS === null ? '—' : `${state.lastLatencyMS.toFixed(1)} ms`;
  return `
    <header class="topbar">
      <div class="crumbs"><span>本地运维域</span><span class="crumb-sep">/</span><strong>风险裁决工作台</strong></div>
      <div class="top-actions">
        <span class="live-pill"><i></i>SNAPSHOT CONSISTENT</span>
        <button class="btn btn-icon" data-refresh title="刷新单一提交快照" aria-label="刷新">${ICON.refresh}</button>
      </div>
    </header>
  `;
}

function renderEmpty() {
  return `
    <section class="page-head" id="overview">
      <div>
        <div class="eyebrow">Controlled rollout intelligence</div>
        <h1>变更风险分级与<br>自动回滚裁决</h1>
        <p>所有结论来自同一 SQLite 提交快照。策略、观测、风险重算和回滚决策均留下可恢复意图，不访问任何外部服务。</p>
      </div>
      <div class="head-actions">
        <div class="latency-badge"><strong>${state.lastLatencyMS ?? '—'}</strong><span>ms · local rtt</span></div>
        <button class="btn btn-teal" data-seed>${ICON.seed}载入固定演示样本</button>
      </div>
    </section>
    <div class="panel">
      <div class="empty-state">
        <div class="empty-visual">⌁</div>
        <h2>等待第一份策略快照</h2>
        <p>可载入内置确定性样本：1 个策略快照、4 个变更批次、36 份观测报告。载入后可在界面中执行快速/完整裁决、手动回滚并检查审计数值。</p>
        <button class="btn btn-primary" data-seed>${ICON.seed}初始化本地工作台</button>
      </div>
    </div>
  `;
}

function renderWorkspace() {
  const policy = state.snapshot.policy;
  return `
    ${renderPageHead(policy)}
    ${renderSummary()}
    <div class="layout-grid" id="batches">
      ${renderBatchPanel()}
      ${renderDecisionPanel()}
    </div>
    <div class="section-head" id="explain">
      <div><h2>风险形成与观测证据</h2><p>每一步整数舍入、样本边界与快速/完整差异均可核对。</p></div>
      <div class="section-num">02 / EXPLAINABILITY</div>
    </div>
    ${renderMetricsPanel()}
    <div style="height:15px"></div>
    ${renderComparePanel()}
    <div style="height:15px"></div>
    ${renderAuditPanel()}
    <div class="section-head" id="policy">
      <div><h2>策略、冷却与恢复事实</h2><p>版本化策略快照决定阈值；恢复视图只陈述已落盘事实。</p></div>
      <div class="section-num">03 / DURABILITY</div>
    </div>
    <div class="two-col" id="recovery">
      ${renderPolicyPanel()}
      <div class="stack">${renderCooldownPanel()}${renderRecoveryPanel()}${renderIntentsPanel()}</div>
    </div>
  `;
}

function renderPageHead(policy) {
  return `
    <section class="page-head" id="overview">
      <div>
        <div class="eyebrow">Controlled rollout intelligence</div>
        <h1>变更风险分级与<br>自动回滚裁决</h1>
        <p>快速视图与完整裁决共享同一策略版本、同一观测高水位。任何风险结论都不会混用增量写入的中间状态。</p>
      </div>
      <div class="head-actions">
        <div class="latency-badge"><strong>${state.lastLatencyMS ?? '—'}</strong><span>ms · local rtt</span></div>
        <button class="btn" data-seed>${ICON.seed}重置演示样本</button>
        <button class="btn btn-primary" data-refresh>${ICON.refresh}刷新快照</button>
      </div>
    </section>
  `;
}

function renderSummary() {
  const batches = state.snapshot.batches || [];
  const observing = batches.filter((batch) => batch.status === 'OBSERVING').length;
  const rolledBack = batches.filter((batch) => batch.status === 'ROLLED_BACK').length;
  const cooldowns = state.snapshot.cooldowns || [];
  const scores = batches.map((batch) => scoreOf(batch.id, 'full')).filter((score) => score !== null);
  const highRisk = batches.filter((batch) => (scoreOf(batch.id, 'full') ?? 0) >= (state.snapshot.policy?.risk?.rollback_bp || 7000)).length;
  const avg = scores.length ? Math.round(scores.reduce((sum, value) => sum + value, 0) / scores.length) : 0;
  return `
    <section class="summary-grid" aria-label="运行摘要">
      ${summaryCard('活跃观察批次', observing, '全量批次', '#0f9f8f', '#e6f7f4', '正在观察中的变更批次')}
      ${summaryCard('回滚完成', rolledBack, '自动处置', '#d92d45', '#ffedf0', '已通过本地回滚端点触达')}
      ${summaryCard('服务冷却中', cooldowns.length, '冷却占用', '#d38b08', '#fff5dc', cooldowns[0] ? `${cooldowns[0].service} · ${duration(cooldowns[0].remaining_ms)}` : '当前无服务冷却占用')}
      ${summaryCard('平均风险分', avg, 'bp · 完整窗口', '#2563eb', '#edf3ff', `${highRisk} 个批次处于回滚阈值以上`)}
    </section>
  `;
}

function summaryCard(label, value, suffix, dot, soft, note) {
  return `
    <div class="summary-card" style="--dot:${dot};--soft:${soft}">
      <div class="summary-label"><i></i>${esc(label)}</div>
      <div class="summary-value"><strong>${esc(value)}</strong><span>${esc(suffix)}</span></div>
      <div class="summary-note">${esc(note)}</div>
    </div>`;
}

function renderBatchPanel() {
  const batches = (state.snapshot.batches || []).filter((batch) => {
    if (state.filter === 'observing' && batch.status !== 'OBSERVING') return false;
    if (state.filter === 'terminal' && batch.status === 'OBSERVING') return false;
    const query = state.query.trim().toLowerCase();
    return !query || batch.id.toLowerCase().includes(query) || batch.service.toLowerCase().includes(query);
  });
  const rows = batches.map((batch) => {
    const item = previewFor(batch.id);
    const evaluation = item?.preview?.comparison?.[state.mode];
    const score = evaluation?.score_bp;
    const level = evaluation?.level;
    return `
      <button class="batch-row ${batch.id === state.selected ? 'selected' : ''}" data-select="${esc(batch.id)}">
        <div>
          <div class="batch-id"><code>${esc(batch.id)}</code></div>
          <div class="batch-service">${esc(batch.service)}<i></i>${esc(changeTypeLabel(batch.change_type))}<i></i>${esc(batch.batch_size)} 实例</div>
        </div>
        <div class="batch-meta">
          <span class="status ${esc(batch.status)}">${esc(statusLabel(batch.status))}</span>
          <span class="mini-score" style="color:${level ? riskColor(level) : '#94a3b8'}">${score === undefined ? '--' : num(score)}<small> bp</small></span>
        </div>
      </button>`;
  }).join('');
  return `
    <section class="panel">
      <div class="panel-head">
        <div class="panel-title"><h2>变更批次</h2><span>${state.snapshot.batches.length} 个批次 · 单一提交视图</span></div>
        <button class="link-btn" data-refresh>同步</button>
      </div>
      <div class="filter-bar">
        <button class="filter ${state.filter === 'all' ? 'active' : ''}" data-filter="all">全部</button>
        <button class="filter ${state.filter === 'observing' ? 'active' : ''}" data-filter="observing">观察中</button>
        <button class="filter ${state.filter === 'terminal' ? 'active' : ''}" data-filter="terminal">终态</button>
        <input class="search" data-search placeholder="搜索批次" value="${esc(state.query)}" aria-label="搜索批次">
      </div>
      <div class="batch-list">${rows || '<div class="empty-state" style="padding:34px 12px"><p>没有匹配的批次</p></div>'}</div>
    </section>`;
}

function renderDecisionPanel() {
  const batch = selectedBatch();
  if (!batch) return '<section class="panel"><div class="empty-state"><p>请选择一个批次</p></div></section>';
  const item = selectedPreview();
  const e = selectedEvaluation();
  if (!e) {
    const message = item?.error?.code === 'E_INSUFFICIENT_SAMPLES'
      ? '样本数不足，系统拒绝以零值冒充风险分。请等待候选观测达到策略最小样本量。'
      : item?.error?.message || '正在计算风险结论…';
    return `
      <section class="panel">
        ${renderDecisionHead(batch)}
        <div class="empty-state">
          <div class="empty-visual">◌</div>
          <h2>${batch.status === 'OBSERVING' ? '尚不能形成风险结论' : statusLabel(batch.status)}</h2>
          <p>${esc(message)}</p>
          ${item?.error ? `<span class="tag">${esc(item.error.code)}</span>` : ''}
        </div>
        ${renderDecisionMeta(batch, e)}
      </section>`;
  }
  const color = riskColor(e.level);
  const circumference = 2 * Math.PI * 57;
  const offset = circumference * (1 - Math.min(10000, e.score_bp) / 10000);
  const evaluationStatus = batch.status;
  const canAct = batch.status === 'OBSERVING';
  const guard = e.decision.safety_guard_applied;
  return `
    <section class="panel">
      <div class="panel-head">
        <div class="panel-title"><h2>${esc(batch.id)}</h2><span>${esc(batch.service)} · ${esc(changeTypeLabel(batch.change_type))}</span></div>
        <div class="filter-bar" style="padding:0;border:0;background:transparent">
          <button class="filter ${state.mode === 'fast' ? 'active' : ''}" data-mode="fast">快速</button>
          <button class="filter ${state.mode === 'full' ? 'active' : ''}" data-mode="full">完整</button>
        </div>
      </div>
      <div class="decision-hero">
        <div class="decision-top">
          <div>
            <div class="decision-kicker">Authoritative risk conclusion</div>
            <h2 class="decision-title">${esc(batch.service)}</h2>
            <p class="decision-sub">${esc(changeTypeLabel(batch.change_type))} · 批次规模 ${esc(batch.batch_size)} · 策略 v${esc(e.policy_version)}</p>
          </div>
          <span class="status ${esc(evaluationStatus)}">${esc(statusLabel(evaluationStatus))}</span>
        </div>
        <div class="decision-core">
          <div class="gauge">
            <svg viewBox="0 0 140 140" aria-label="风险分 ${e.score_bp} / 10000">
              <circle class="track" cx="70" cy="70" r="57"></circle>
              <circle class="arc" cx="70" cy="70" r="57" stroke="${color}" stroke-dasharray="${circumference.toFixed(2)}" stroke-dashoffset="${offset.toFixed(2)}"></circle>
            </svg>
            <div class="gauge-center"><strong style="color:${color}">${num(e.score_bp)}</strong><span>risk bp</span><em style="color:${color}">${esc(levelLabel(e.level))}</em></div>
          </div>
          <div class="decision-copy">
            <h3 style="color:${color}">${esc(actionLabel(e.decision.action))}</h3>
            <p>${esc(e.decision.reason)}</p>
            <div class="decision-tags">
              <span class="tag">策略 <b>v${esc(e.policy_version)}</b></span>
              <span class="tag">观测修订 <b>#${esc(e.observation_revision)}</b></span>
              <span class="tag">提交边界 <b>#${esc(e.as_of_commit_seq)}</b></span>
              <span class="tag">${state.mode === 'fast' ? '快速采样' : '完整窗口'}</span>
            </div>
          </div>
        </div>
        <div class="decision-actions">
          <button class="btn btn-teal" data-evaluate ${!canAct || state.busy ? 'disabled' : ''}>${ICON.play}提交${state.mode === 'fast' ? '快速' : '完整'}裁决</button>
          <button class="btn" data-obs ${!canAct || state.busy ? 'disabled' : ''}>${ICON.plus}增量观测</button>
          <button class="btn btn-danger" data-rollback ${!canAct || state.busy ? 'disabled' : ''}>${ICON.rollback}手动回滚</button>
          ${guard ? '<span class="guard-note">快速模式存在未采样观测，安全守护已禁止放行。</span>' : `<span class="guard-note">${state.mode === 'fast' ? `采样率 ${ppmPercent(e.sampling.sample_rate_ppm, 2)} · 仅最近报告` : '完整窗口包含全部已提交观测'}</span>`}
        </div>
      </div>
      ${renderDecisionMeta(batch, e)}
    </section>`;
}

function renderDecisionHead(batch) {
  return `<div class="panel-head"><div class="panel-title"><h2>${esc(batch.id)}</h2><span>${esc(batch.service)}</span></div><span class="status ${esc(batch.status)}">${esc(statusLabel(batch.status))}</span></div>`;
}

function renderDecisionMeta(batch, e) {
  if (!e) {
    return `<div class="meta-strip">
      <div class="meta-cell"><span>批次状态</span><strong>${esc(statusLabel(batch.status))}</strong></div>
      <div class="meta-cell"><span>观测修订</span><strong>#${esc(batch.revision)}</strong></div>
      <div class="meta-cell"><span>最后观测</span><strong>${esc(fmtDate(batch.last_observation_ts_ms))}</strong></div>
      <div class="meta-cell"><span>截止提交</span><strong>#${esc(state.snapshot.as_of_commit_seq)}</strong></div>
    </div>`;
  }
  return `<div class="meta-strip">
    <div class="meta-cell"><span>观察窗口</span><strong>${esc(fmtTime(e.window.start_ms))} – ${esc(fmtTime(e.window.end_ms))}</strong></div>
    <div class="meta-cell"><span>样本 / 报告</span><strong>${esc(e.sampling.included_samples)} / ${esc(e.sampling.included_reports)}</strong></div>
    <div class="meta-cell"><span>策略摘要</span><strong>${esc(shortHash(e.policy_checksum, 10))}</strong></div>
    <div class="meta-cell"><span>截止提交</span><strong>#${esc(e.as_of_commit_seq)} · rev #${esc(e.observation_revision)}</strong></div>
  </div>`;
}

function renderMetricsPanel() {
  const e = selectedEvaluation();
  if (!e) return '<section class="panel"><div class="empty-state"><p>选择批次后显示指标证据。</p></div></section>';
  const policy = state.snapshot.policy;
  const errorRatio = Math.min(1, e.metrics.error_rate_ppm / policy.thresholds.error_rate_ppm);
  const latencyRatio = Math.min(1, e.metrics.latency_degradation_ppm / policy.thresholds.latency_degradation_ppm);
  const successRatio = Math.min(1, e.metrics.success_drop_ppm / policy.thresholds.success_drop_ppm);
  return `
    <section class="panel">
      <div class="panel-head">
        <div class="panel-title"><h3>观测指标与阈值逼近度</h3><span>${state.mode === 'fast' ? '快速抽样' : '完整窗口'} · ${pct(e.sampling.sample_rate_ppm / 1000000, 1)} 样本覆盖</span></div>
        <span class="tag">${esc(e.sampling.method)}</span>
      </div>
      <div class="metric-triptych">
        ${metricBlock('错误率', ppmPercent(e.metrics.error_rate_ppm, 2), `阈值 ${ppmPercent(policy.thresholds.error_rate_ppm, 2)}`, errorRatio, `${e.metrics.error_count} 错误 / ${e.metrics.error_samples} 样本`, 'candidate')}
        ${metricBlock('P90 延迟', `${num(e.metrics.latency_p90_ms)}<small>ms</small>`, `基线 ${num(e.metrics.baseline_latency_p90_ms)}ms`, latencyRatio, `最近秩 #${e.metrics.p90_nearest_rank} · n=${e.metrics.latency_samples}`, 'candidate')}
        ${metricBlock('成功率', ppmPercent(e.metrics.success_rate_ppm, 2), `下降 ${ppmPercent(e.metrics.success_drop_ppm, 2)}`, successRatio, `${e.metrics.success_count} 成功 / ${e.metrics.success_samples} 样本`, 'candidate')}
      </div>
      <div class="panel-body">
        ${renderTimeline()}
      </div>
    </section>`;
}

function metricBlock(name, value, sub, ratio, foot, kind) {
  const color = ratio >= .75 ? '#d92d45' : ratio >= .4 ? '#d38b08' : '#0f9f8f';
  return `<div class="metric-block">
    <div class="metric-top"><span class="metric-name">${esc(name)}</span><span class="tag">${esc(kind)}</span></div>
    <div class="metric-value">${value}</div>
    <div class="metric-bar"><i style="width:${(ratio * 100).toFixed(1)}%;background:${color}"></i></div>
    <div class="metric-foot"><span>${esc(sub)}</span><span>${esc(foot)}</span></div>
  </div>`;
}

function renderTimeline() {
  const item = selectedPreview();
  const buckets = item?.preview?.buckets || [];
  if (!buckets.length) return '<div class="muted" style="font-size:9px">暂无时间桶</div>';
  const groups = {};
  for (const bucket of buckets) (groups[bucket.metric_type] ||= []).push(bucket);
  return `<div class="bucket-timeline">${Object.entries(groups).map(([metric, entries]) => {
    const max = Math.max(...entries.map((entry) => entry.sample_count), 1);
    return `<div class="bucket-lane"><span class="bucket-lane-label">${esc(metricLabel(metric))}</span><div class="bucket-dots">${entries.map((entry) => {
      const height = Math.max(4, Math.round(19 * entry.sample_count / max));
      return `<span class="bucket-dot" style="height:${height}px" data-tip="${esc(fmtTime(entry.bucket_start_ms))} · ${esc(entry.sample_count)} 样本"></span>`;
    }).join('')}</div></div>`;
  }).join('')}</div>`;
}

function renderComparePanel() {
  const item = selectedPreview();
  const comparison = item?.preview?.comparison;
  if (!comparison) return '<section class="panel"><div class="empty-state"><p>当前没有可比较的快速/完整结论。</p></div></section>';
  return `
    <section class="panel">
      <div class="panel-head">
        <div class="panel-title"><h3>快速与完整裁决对照</h3><span>同一策略 v${esc(comparison.policy_version)} · 同一观测修订 #${esc(comparison.observation_revision)}</span></div>
        <span class="tag">提交 #${esc(comparison.as_of_commit_seq)}</span>
      </div>
      <div class="compare-grid">
        ${compareCard('快速', comparison.fast, 'fast')}
        ${compareCard('完整', comparison.full, 'full')}
      </div>
      <div class="audit-table" style="max-height:250px">
        <table>
          <thead><tr><th>对照字段</th><th>快速视图</th><th>完整窗口</th><th>状态</th></tr></thead>
          <tbody>${comparison.differences.map((diff) => `
            <tr class="${diff.equal ? '' : 'diff-row'}">
              <td class="table-strong">${esc(diffLabel(diff.field))}</td>
              <td><code>${esc(compareValue(diff.field, diff.fast))}</code></td>
              <td><code>${esc(compareValue(diff.field, diff.full))}</code></td>
              <td><span class="${diff.equal ? 'same-badge' : 'diff-badge'}">${diff.equal ? '一致' : '差异'}</span></td>
            </tr>`).join('')}</tbody>
        </table>
      </div>
    </section>`;
}

function compareCard(label, e, mode) {
  const color = riskColor(e.level);
  return `<div class="compare-card">
    <div class="compare-head"><span class="mode-label"><i class="mode-dot ${mode}"></i>${label}模式</span><span class="status ${esc(e.status)}">${esc(statusLabel(e.status))}</span></div>
    <div class="compare-score"><strong style="color:${color}">${num(e.score_bp)}</strong><span>bp · ${esc(levelLabel(e.level))}</span></div>
    <div class="compare-action" style="color:${color}">${esc(actionLabel(e.decision.action))}${e.decision.safety_guard_applied ? ' · 安全守护' : ''}</div>
    <div class="compare-facts">
      <div class="fact"><span>采样率</span><b>${ppmPercent(e.sampling.sample_rate_ppm, 1)}</b></div>
      <div class="fact"><span>样本</span><b>${num(e.sampling.included_samples)}</b></div>
      <div class="fact"><span>报告</span><b>${num(e.sampling.included_reports)}/${num(e.sampling.total_reports)}</b></div>
    </div>
  </div>`;
}

function diffLabel(field) {
  return {
    score_bp: '风险分', risk_level: '风险等级', decision: '裁决动作', error_rate_ppm: '错误率',
    latency_p90_ms: 'P90 延迟', success_rate_ppm: '成功率', included_samples: '纳入样本',
  }[field] || field;
}

function compareValue(field, value) {
  if (['error_rate_ppm', 'success_rate_ppm'].includes(field)) return ppmPercent(value, 2);
  if (field === 'latency_p90_ms') return `${value} ms`;
  if (field === 'decision') return actionLabel(value);
  if (field === 'risk_level') return levelLabel(value);
  return String(value);
}

function renderAuditPanel() {
  const e = selectedEvaluation();
  if (!e) return '<section class="panel"><div class="empty-state"><p>暂无舍入审计。</p></div></section>';
  const c = e.calculation;
  return `
    <section class="panel">
      <div class="panel-head">
        <div class="panel-title"><h3>整数数值契约与舍入审计</h3><span>输入 PPM → 分项微单位 → 加权风险基点</span></div>
        <span class="tag">round_half_up</span>
      </div>
      <div class="audit-grid">
        <div class="audit-summary">
          <div class="formula">${esc(c.formula)}</div>
          <div class="audit-kv">
            <div><span>加权分子</span><b>${num(c.weighted_numerator)}</b></div>
            <div><span>加权分母</span><b>${num(c.weighted_denominator)}</b></div>
            <div><span>精确风险分</span><b>${esc(c.score_exact_decimal)} bp</b></div>
            <div><span>整数风险分</span><b>${num(c.score_bp)} bp</b></div>
            <div><span>累计舍入误差</span><b>${esc(c.cumulative_rounding_error_micro_bp)} μbp</b></div>
            <div><span>误差有理数</span><b>${esc(c.rounding_error_rational)}</b></div>
          </div>
          <div class="recovery-banner" style="margin-top:14px">
            <span class="recovery-mark">✓</span>
            <div><strong>${esc(levelLabel(e.level))} · 容差内取下档</strong><p>${esc(e.level_audit.tolerance_rule)}；平局顺序 ${esc(e.level_audit.tie_break_order.join(' → '))}</p></div>
          </div>
        </div>
        <div class="audit-table">
          <table>
            <thead><tr><th>步骤</th><th>精确值</th><th>舍入值</th><th>残差</th></tr></thead>
            <tbody>${e.rounding_steps.map((step) => `<tr>
              <td><span class="table-strong">${esc(step.step)}</span><br><span class="mono">${esc(step.method)}</span></td>
              <td><code>${esc(step.exact)}</code><br><span>${esc(step.expression)}</span></td>
              <td><code>${esc(step.rounded)} ${esc(step.unit)}</code></td>
              <td><code>${esc(step.residual)}</code></td>
            </tr>`).join('')}</tbody>
          </table>
        </div>
      </div>
    </section>`;
}

function renderPolicyPanel() {
  const policy = state.snapshot.policy;
  if (!policy) return '';
  return `
    <section class="panel">
      <div class="panel-head">
        <div class="panel-title"><h2>风险策略快照</h2><span>单调版本号 · SHA-256 整体摘要</span></div>
        <span class="tag">v${esc(policy.version)}</span>
      </div>
      <div class="policy-grid">
        ${policyCell('生成时间', fmtDate(policy.generated_at_ms))}
        ${policyCell('观察窗口', duration(policy.observation_window_ms))}
        ${policyCell('时间桶', duration(policy.bucket_ms))}
        ${policyCell('最小样本', `${policy.min_samples} / 指标`)}
        ${policyCell('错误率阈值', ppmPercent(policy.thresholds.error_rate_ppm, 2))}
        ${policyCell('延迟偏离阈值', ppmPercent(policy.thresholds.latency_degradation_ppm, 2))}
        ${policyCell('成功率下降阈值', ppmPercent(policy.thresholds.success_drop_ppm, 2))}
        ${policyCell('等级容差', `${policy.thresholds.level_tolerance_ppm} ppm`)}
        ${policyCell('放行 / 回滚', `${policy.risk.pass_bp} / ${policy.risk.rollback_bp} bp`)}
        ${policyCell('四档边界', `${policy.risk.medium_bp} · ${policy.risk.high_bp} · ${policy.risk.extreme_bp}`)}
        ${policyCell('自动回滚', policy.auto_rollback ? '开启' : '关闭')}
        ${policyCell('服务冷却期', duration(policy.cooldown_ms))}
      </div>
      <div class="checksum"><span>checksum</span><code>${esc(policy.checksum)}</code></div>
      <div class="panel-body" style="padding-top:14px">
        <div class="component-list">${renderComponents()}</div>
      </div>
    </section>`;
}

function policyCell(label, value) {
  return `<div class="policy-cell"><span>${esc(label)}</span><strong>${esc(value)}</strong></div>`;
}

function renderComponents() {
  const e = selectedEvaluation();
  if (!e) return '<span class="muted" style="font-size:9px">暂无分项</span>';
  return e.components.map((component) => {
    const ratio = component.rounded_micro / 1000000;
    const contribution = Math.round(component.rounded_micro * component.weight_bp / 1000000);
    const cls = ratio >= .7 ? 'hot' : ratio >= .4 ? 'warn' : '';
    return `<div class="component-row ${cls}">
      <span class="component-name">${esc(metricLabel(component.metric))}</span>
      <div class="component-track"><i style="width:${Math.min(100, ratio * 100).toFixed(1)}%"></i></div>
      <span class="component-score">${num(contribution)} bp<small>权重 ${component.weight_bp / 100}%</small></span>
    </div>`;
  }).join('');
}

function renderCooldownPanel() {
  const batch = selectedBatch();
  const cooldown = selectedPreview()?.preview?.cooldown;
  if (!batch) return '';
  const policy = state.snapshot.policy;
  const total = policy?.cooldown_ms || 1;
  const remaining = cooldown?.remaining_ms || 0;
  const active = Boolean(cooldown?.active);
  const pctValue = active ? Math.max(0, Math.min(100, remaining / total * 100)) : 0;
  return `
    <section class="panel">
      <div class="panel-head"><div class="panel-title"><h3>回滚冷却状态</h3><span>按目标服务占用</span></div><span class="status ${active ? 'ROLLING_BACK' : 'PASSED'}">${active ? '冷却中' : '可用'}</span></div>
      <div class="cooldown-card">
        <div class="cooldown-state ${active ? 'active' : ''}">
          <div class="cooldown-icon">◷</div>
          <div><strong>${active ? '服务冷却期生效中' : '当前服务无冷却占用'}</strong><span>${esc(batch.service)}${cooldown?.batch_id ? ` · 占用批次 ${esc(cooldown.batch_id)}` : ''}</span></div>
        </div>
        ${active ? `<div class="cooldown-progress"><i style="width:${pctValue.toFixed(1)}%"></i></div><div class="cooldown-times"><span>剩余 ${esc(duration(remaining))}</span><span>${esc(fmtDate(cooldown.expires_at_ms))}</span></div>` : `<div class="cooldown-times" style="margin-top:14px"><span>策略冷却期 ${esc(duration(total))}</span><span>可触发回滚</span></div>`}
      </div>
    </section>`;
}

function renderRecoveryPanel() {
  const recovery = state.snapshot.recovery;
  const report = recovery?.report;
  const pending = report?.pending_found ?? 0;
  const resumed = report?.resumed_rollbacks?.length ?? 0;
  const clean = report?.clean;
  return `
    <section class="panel">
      <div class="panel-head"><div class="panel-title"><h3>启动恢复报告</h3><span>只呈现已落盘事实</span></div>${report ? `<span class="tag">#${esc(recovery.id)}</span>` : ''}</div>
      <div class="panel-body">
        ${!report ? '<div class="recovery-banner"><span class="recovery-mark">i</span><div><strong>本次会话尚无恢复运行</strong><p>强制终止并重启后，启动扫描会在这里记录未完成意图和批次恢复结果。</p></div></div>' : `
          <div class="recovery-banner ${clean ? '' : 'alert'}"><span class="recovery-mark">${clean ? '✓' : '!'}</span><div><strong>${clean ? '一致状态已恢复' : '检测到未完成意图，已按事务边界处置'}</strong><p>生成于 ${esc(fmtDate(report.generated_at_ms))} · 最新提交 #${esc(report.latest_commit_seq)}</p></div></div>
          <div class="recovery-stats">
            <div class="recovery-stat"><span>未完成意图</span><b>${num(pending)}</b></div>
            <div class="recovery-stat"><span>恢复回滚</span><b>${num(resumed)}</b></div>
            <div class="recovery-stat"><span>回滚中残留</span><b>${num(report.rolling_back_found)}</b></div>
          </div>`}
      </div>
    </section>`;
}

function renderIntentsPanel() {
  const intents = [...(state.intents || [])].reverse().slice(0, 9);
  return `
    <section class="panel">
      <div class="panel-head"><div class="panel-title"><h3>最近意图</h3><span>先持久化意图，再提交业务事务</span></div><span class="tag">${esc(state.intents.length)}</span></div>
      <div class="intent-list">${intents.length ? intents.map((intent) => `
        <div class="intent-row">
          <span class="intent-dot">${esc(intent.id)}</span>
          <div class="intent-main"><strong>${esc(intentLabel(intent.kind))}</strong><span>${esc(intent.batch_id || '全局策略')} · ${esc(intent.note || intent.status)}</span></div>
          <span class="intent-status ${esc(intent.status)}">${esc(intent.status)}</span>
        </div>`).join('') : '<div class="empty-state" style="padding:22px"><p>尚无意图记录</p></div>'}</div>
    </section>`;
}

function intentLabel(kind) {
  return {
    policy_submit: '策略提交', batch_submit: '批次提交', observation_ingest: '观测摄入',
    risk_decision: '风险裁决', manual_rollback: '手动回滚', manual_transition: '状态迁移',
  }[kind] || kind;
}

export function showToast(type, title, message) {
  const stack = document.getElementById('toast-stack');
  if (!stack) return;
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `<span class="toast-icon">${type === 'error' ? '!' : '✓'}</span><div><strong>${esc(title)}</strong><span>${esc(message)}</span></div>`;
  stack.appendChild(toast);
  setTimeout(() => toast.remove(), 5200);
}
