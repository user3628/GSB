// Package engine 编排探测、阶段机、权重快照与调度轮次，
// 保证探测结论、可用集合与归一化权重在同一事务边界内整体提交。
package engine

import (
	"context"
	"sync"

	"gatewayworkbench/internal/model"
	"gatewayworkbench/internal/probe"
	"gatewayworkbench/internal/store"
)

type Engine struct {
	mu       sync.Mutex // 轮次级串行化，配合 DB 单写连接消除中间可见状态
	store    *store.Store
	executor *probe.Executor
	cfg      model.RuntimeConfig
}

func New(st *store.Store, cfg model.RuntimeConfig) *Engine {
	if cfg.ProbeConcurrency <= 0 {
		cfg.ProbeConcurrency = 4
	}
	if cfg.GlobalConcurrency <= 0 {
		cfg.GlobalConcurrency = 4
	}
	if cfg.PerInstanceLimit <= 0 {
		cfg.PerInstanceLimit = 2
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 8
	}
	if cfg.RoundBudgetMillis <= 0 {
		cfg.RoundBudgetMillis = 2000
	}
	return &Engine{store: st, executor: probe.New(), cfg: cfg}
}

func (e *Engine) Config() model.RuntimeConfig { return e.cfg }

// LoadSample 校验并整份替换样本（失败返回 E_INSTANCE_INVALID，旧数据保持不变）。
func (e *Engine) LoadSample(ctx context.Context, sample *model.Sample) error {
	if err := sample.Validate(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.store.ReplaceSample(ctx, sample)
}

// currentPhases 从持久化状态行构造阶段映射与计数器。
func (e *Engine) currentPhases(ctx context.Context) (map[string]*model.StateCounter, map[string]model.Phase, error) {
	rows, err := e.store.ListStates(ctx)
	if err != nil {
		return nil, nil, err
	}
	counters := make(map[string]*model.StateCounter, len(rows))
	phases := make(map[string]model.Phase, len(rows))
	for _, row := range rows {
		counters[row.InstanceID] = &model.StateCounter{
			Phase:            row.Phase,
			SuccessStreak:    row.SuccessStreak,
			FailureStreak:    row.FailureStreak,
			SuccessThreshold: 1, // 由实例探测参数覆盖
			FailureThreshold: 1,
		}
		phases[row.InstanceID] = row.Phase
	}
	return counters, phases, nil
}

// RunProbeRound 并行探测全部实例，并在同一事务边界内提交：
// 探测历史 -> 阶段迁移 -> 可用集合 -> 按区归一化权重 -> 快照版本推进。
func (e *Engine) RunProbeRound(ctx context.Context, roundID string) (map[string]model.ProbeOutcome, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	instances, err := e.store.ListInstances(ctx)
	if err != nil {
		return nil, err
	}
	if len(instances) == 0 {
		return nil, model.NewBizError(model.CodeInstanceInvalid, "no instance loaded", "")
	}

	// 并行探测，信号量控制探测并发；每个探测自带独立超时，返回即释放名额。
	sem := make(chan struct{}, e.cfg.ProbeConcurrency)
	results := make([]model.ProbeOutcome, len(instances))
	var wg sync.WaitGroup
	for idx, ins := range instances {
		wg.Add(1)
		go func(i int, target model.Instance) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[i] = e.executor.Probe(ctx, target)
		}(idx, ins)
	}
	wg.Wait()

	outcomes := make(map[string]model.ProbeOutcome, len(results))
	for _, r := range results {
		outcomes[r.InstanceID] = r
	}

	counters, phases, err := e.currentPhases(ctx)
	if err != nil {
		return nil, err
	}
	stateRows := make([]store.InstanceStateRow, 0, len(instances))
	for _, ins := range instances {
		counter := counters[ins.ID]
		counter.SuccessThreshold = ins.Probe.SuccessThreshold
		counter.FailureThreshold = ins.Probe.FailureThreshold
		outcome := outcomes[ins.ID]
		from := counter.Phase
		var target model.Phase
		var changed bool
		if outcome.Success {
			target, changed = counter.ObserveSuccess()
		} else {
			target, changed = counter.ObserveFailure()
		}
		reason := counterReason(from, target, changed, outcome)
		// 阶段机非法迁移保护：StateCounter 产出的迁移都在合法表内，这里再做显式断言。
		if changed {
			if err := model.AssertTransition(from, target); err != nil {
				return nil, err
			}
		}
		phases[ins.ID] = target
		stateRows = append(stateRows, store.InstanceStateRow{
			InstanceID:    ins.ID,
			Phase:         target,
			SuccessStreak: counter.SuccessStreak,
			FailureStreak: counter.FailureStreak,
			Reason:        reason,
		})
	}

	zoneWeights := model.NormalizeZoneWeights(instances, phases)
	ratios, err := e.store.LoadAffinity(ctx)
	if err != nil {
		return nil, err
	}

	tx, intentID, err := e.store.BeginIntent(ctx, roundID, "probe", map[string]any{"instances": len(instances)})
	if err != nil {
		return nil, err
	}
	if err := e.store.CreateRound(ctx, tx, roundID, "probe", "{}"); err != nil {
		store.AbortIntent(tx)
		return nil, err
	}
	for _, ins := range instances {
		if err := e.store.InsertProbe(ctx, tx, roundID, outcomes[ins.ID]); err != nil {
			store.AbortIntent(tx)
			return nil, err
		}
	}
	for _, row := range stateRows {
		if err := e.store.SetInstanceState(ctx, tx, row); err != nil {
			store.AbortIntent(tx)
			return nil, err
		}
	}
	snap := &model.SnapshotVersion{
		Reason:        "probe_round " + roundID,
		InstanceState: phases,
		ZoneWeights:   zoneWeights,
		ZoneRatios:    ratios,
	}
	version, err := e.store.SaveSnapshot(ctx, tx, snap)
	if err != nil {
		store.AbortIntent(tx)
		return nil, err
	}
	snap.Version = version
	if err := e.store.FinishRound(ctx, tx, roundID, "COMMITTED"); err != nil {
		store.AbortIntent(tx)
		return nil, err
	}
	if err := e.store.CommitIntent(ctx, tx, intentID); err != nil {
		return nil, err
	}
	return outcomes, nil
}

func counterReason(from, target model.Phase, changed bool, outcome model.ProbeOutcome) string {
	if changed {
		base := "transition " + string(from) + "->" + string(target)
		if outcome.Success {
			return base + " after successful probe"
		}
		return base + " after probe " + outcome.ErrCode
	}
	if outcome.Success {
		return "phase unchanged after successful probe"
	}
	return "phase unchanged after probe " + outcome.ErrCode
}

// Drain 人工摘除：校验迁移并在单事务内更新阶段、可用集合、归一化权重与快照版本。
func (e *Engine) Drain(ctx context.Context, instanceID, reason string) (*model.SnapshotVersion, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	instances, err := e.store.ListInstances(ctx)
	if err != nil {
		return nil, err
	}
	row, err := e.store.GetState(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	if err := model.AssertTransition(row.Phase, model.PhaseDraining); err != nil {
		return nil, err
	}

	phases := make(map[string]model.Phase)
	for _, ins := range instances {
		if ins.ID == instanceID {
			phases[ins.ID] = model.PhaseDraining
			continue
		}
		stateRow, err := e.store.GetState(ctx, ins.ID)
		if err != nil {
			return nil, err
		}
		phases[ins.ID] = stateRow.Phase
	}
	zoneWeights := model.NormalizeZoneWeights(instances, phases)
	ratios, err := e.store.LoadAffinity(ctx)
	if err != nil {
		return nil, err
	}

	tx, intentID, err := e.store.BeginIntent(ctx, "", "drain", map[string]string{"instance": instanceID})
	if err != nil {
		return nil, err
	}
	drainReason := reason
	if drainReason == "" {
		drainReason = "manual drain"
	}
	if err := e.store.SetInstanceState(ctx, tx, store.InstanceStateRow{
		InstanceID: instanceID, Phase: model.PhaseDraining,
		SuccessStreak: 0, FailureStreak: 0, Reason: drainReason,
	}); err != nil {
		store.AbortIntent(tx)
		return nil, err
	}
	snap := &model.SnapshotVersion{
		Reason:        "drain " + instanceID + ": " + drainReason,
		InstanceState: phases,
		ZoneWeights:   zoneWeights,
		ZoneRatios:    ratios,
	}
	version, err := e.store.SaveSnapshot(ctx, tx, snap)
	if err != nil {
		store.AbortIntent(tx)
		return nil, err
	}
	snap.Version = version
	if err := e.store.CommitIntent(ctx, tx, intentID); err != nil {
		return nil, err
	}
	return model.CloneSnapshot(snap), nil
}

// Resume 把 DRAINING 实例人工恢复到 UNKNOWN，重新等待探测确认。
func (e *Engine) Resume(ctx context.Context, instanceID string) (*model.SnapshotVersion, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	instances, err := e.store.ListInstances(ctx)
	if err != nil {
		return nil, err
	}
	row, err := e.store.GetState(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	if err := model.AssertTransition(row.Phase, model.PhaseUnknown); err != nil {
		return nil, err
	}
	phases := make(map[string]model.Phase)
	for _, ins := range instances {
		stateRow, err := e.store.GetState(ctx, ins.ID)
		if err != nil {
			return nil, err
		}
		phases[ins.ID] = stateRow.Phase
	}
	phases[instanceID] = model.PhaseUnknown
	zoneWeights := model.NormalizeZoneWeights(instances, phases)
	ratios, err := e.store.LoadAffinity(ctx)
	if err != nil {
		return nil, err
	}
	tx, intentID, err := e.store.BeginIntent(ctx, "", "resume", map[string]string{"instance": instanceID})
	if err != nil {
		return nil, err
	}
	if err := e.store.SetInstanceState(ctx, tx, store.InstanceStateRow{
		InstanceID: instanceID, Phase: model.PhaseUnknown,
		SuccessStreak: 0, FailureStreak: 0, Reason: "manual resume to UNKNOWN",
	}); err != nil {
		store.AbortIntent(tx)
		return nil, err
	}
	snap := &model.SnapshotVersion{
		Reason:        "resume " + instanceID,
		InstanceState: phases,
		ZoneWeights:   zoneWeights,
		ZoneRatios:    ratios,
	}
	version, err := e.store.SaveSnapshot(ctx, tx, snap)
	if err != nil {
		store.AbortIntent(tx)
		return nil, err
	}
	snap.Version = version
	if err := e.store.CommitIntent(ctx, tx, intentID); err != nil {
		return nil, err
	}
	return model.CloneSnapshot(snap), nil
}

// SaveAffinity 更新亲和比例并立即产生新版本快照。
func (e *Engine) SaveAffinity(ctx context.Context, ratios map[string]int) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	instances, err := e.store.ListInstances(ctx)
	if err != nil {
		return err
	}
	phases := map[string]model.Phase{}
	rows, err := e.store.ListStates(ctx)
	if err != nil {
		return err
	}
	for _, r := range rows {
		phases[r.InstanceID] = r.Phase
	}
	tx, intentID, err := e.store.BeginIntent(ctx, "", "affinity", ratios)
	if err != nil {
		return err
	}
	if err := e.store.SaveAffinity(ctx, tx, ratios); err != nil {
		store.AbortIntent(tx)
		return err
	}
	snap := &model.SnapshotVersion{
		Reason:        "affinity update",
		InstanceState: phases,
		ZoneWeights:   model.NormalizeZoneWeights(instances, phases),
		ZoneRatios:    ratios,
	}
	if _, err := e.store.SaveSnapshot(ctx, tx, snap); err != nil {
		store.AbortIntent(tx)
		return err
	}
	return e.store.CommitIntent(ctx, tx, intentID)
}
