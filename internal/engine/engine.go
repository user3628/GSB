package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"motionlab/internal/model"
	"motionlab/internal/motion"
	"motionlab/internal/store"
)

type Options struct {
	AfterCommit func(runID string, seq int64)
}

type Engine struct {
	catalog atomic.Pointer[motion.Catalog]
	store   *store.Store
	opts    Options
	render  motion.Renderer

	mu     sync.Mutex
	active map[string]*activeRun
}

type activeRun struct {
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	err    error
	report *model.RunReport
}

func New(catalog *motion.Catalog, st *store.Store, opts Options) *Engine {
	e := &Engine{store: st, opts: opts, active: make(map[string]*activeRun)}
	e.catalog.Store(catalog)
	return e
}

func (e *Engine) Catalog() *motion.Catalog           { return e.catalog.Load() }
func (e *Engine) SetCatalog(catalog *motion.Catalog) { e.catalog.Store(catalog) }
func (e *Engine) Store() *store.Store                { return e.store }

func RunIDFor(sequenceID string, config model.RunConfig) string {
	clean := strings.NewReplacer("/", "_", "\\", "_", " ", "_").Replace(sequenceID)
	return "run_" + clean + "_" + model.Digest(config)[:16]
}

func (e *Engine) EnsureRun(ctx context.Context, sequenceID string, policy model.PolicySnapshot, config model.RunConfig, fresh bool) (*store.RunRow, error) {
	seq, appErr := e.Catalog().Sequence(sequenceID)
	if appErr != nil {
		return nil, appErr
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	normalized, err := config.Normalize(seq.Spec, policy)
	if err != nil {
		return nil, err
	}
	runID := RunIDFor(sequenceID, normalized)
	if fresh {
		e.mu.Lock()
		_, running := e.active[runID]
		e.mu.Unlock()
		if running {
			return nil, model.NewError(model.CodeRunBusy, "cannot replace a running execution")
		}
		if err := e.store.DeleteRun(ctx, runID); err != nil {
			return nil, err
		}
	}
	if row, err := e.store.GetRun(ctx, runID); err == nil {
		return row, nil
	} else if appErr, ok := err.(*model.AppError); !ok || appErr.Code != model.CodeNotFound {
		return nil, err
	}
	plans, err := BuildPlan(seq.Spec, normalized, policy)
	if err != nil {
		return nil, err
	}
	policyJSON, _ := json.Marshal(policy)
	configJSON, _ := json.Marshal(normalized)
	row := store.RunSeed{
		RunID:           runID,
		SequenceID:      sequenceID,
		Policy:          policy,
		PolicyJSON:      string(policyJSON),
		ConfigJSON:      string(configJSON),
		ConfigHash:      model.Digest(normalized),
		Status:          "pending",
		InitialStateSHA: model.Digest(seq.Spec.InitialState),
		InitialFrameSHA: seq.Shared.Initial.SHA256,
		Branches:        BranchSeeds(plans),
	}
	if err := e.store.CreateRun(ctx, row); err != nil {
		return nil, err
	}
	return e.store.GetRun(ctx, runID)
}

func (e *Engine) Start(runID string) error {
	e.mu.Lock()
	if _, exists := e.active[runID]; exists {
		e.mu.Unlock()
		return model.NewError(model.CodeRunBusy, "run is already executing")
	}
	ctx, cancel := context.WithCancel(context.Background())
	a := &activeRun{cancel: cancel, done: make(chan struct{})}
	e.active[runID] = a
	e.mu.Unlock()
	go func() {
		report, err := e.execute(ctx, runID)
		a.mu.Lock()
		a.report, a.err = report, err
		a.mu.Unlock()
		close(a.done)
		e.mu.Lock()
		delete(e.active, runID)
		e.mu.Unlock()
	}()
	return nil
}

func (e *Engine) Run(ctx context.Context, runID string) (*model.RunReport, error) {
	e.mu.Lock()
	if _, exists := e.active[runID]; exists {
		e.mu.Unlock()
		return nil, model.NewError(model.CodeRunBusy, "run is already executing")
	}
	inner, cancel := context.WithCancel(ctx)
	a := &activeRun{cancel: cancel, done: make(chan struct{})}
	e.active[runID] = a
	e.mu.Unlock()
	report, err := e.execute(inner, runID)
	a.mu.Lock()
	a.report, a.err = report, err
	a.mu.Unlock()
	close(a.done)
	e.mu.Lock()
	delete(e.active, runID)
	e.mu.Unlock()
	return report, err
}

func (e *Engine) Stop(ctx context.Context, runID string) error {
	e.mu.Lock()
	a := e.active[runID]
	e.mu.Unlock()
	if a == nil {
		return model.NewError(model.CodeNotFound, "run is not currently executing")
	}
	a.cancel()
	select {
	case <-a.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Engine) IsActive(runID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.active[runID]
	return ok
}

func (e *Engine) execute(ctx context.Context, runID string) (*model.RunReport, error) {
	run, err := e.store.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	seq, appErr := e.Catalog().Sequence(run.SequenceID)
	if appErr != nil {
		return nil, appErr
	}
	if run.ReportJSON != "" && run.Status == "completed" {
		var report model.RunReport
		if err := json.Unmarshal([]byte(run.ReportJSON), &report); err == nil {
			return &report, nil
		}
	}
	var config model.RunConfig
	if err := json.Unmarshal([]byte(run.ConfigJSON), &config); err != nil {
		return nil, model.NewError(model.CodeInternal, "stored run config is corrupt")
	}
	normalized, err := config.Normalize(seq.Spec, model.PolicySnapshot{
		Version: run.PolicyVersion, PolicyID: "stored", Concurrency: config.Concurrency,
		BranchQuota: config.BranchQuota, RoundBudgetMS: config.RoundBudgetMS, PruneThreshold: config.PruneThreshold,
	})
	if err != nil {
		return nil, err
	}
	config = normalized
	branches, err := e.store.LoadBranches(ctx, runID)
	if err != nil {
		return nil, err
	}
	wasResumed := run.Status != "pending" || hasCommittedWork(branches)
	if wasResumed {
		_ = e.store.IncrementRecovery(context.Background(), runID)
	}
	if err := e.store.SetRunStatus(context.Background(), runID, "running"); err != nil {
		return nil, err
	}
	policy := model.PolicySnapshot{
		Version: run.PolicyVersion, PolicyID: "stored", Concurrency: config.Concurrency,
		BranchQuota: config.BranchQuota, RoundBudgetMS: config.RoundBudgetMS, PruneThreshold: config.PruneThreshold,
	}
	profiles := make(map[int]model.BranchProfile, len(branches))
	for _, b := range branches {
		profiles[b.BranchID] = motion.ProfileFor(seq.Spec, b.BranchID)
	}
	waves := groupWaves(branches)
	var runErr error
	for _, wave := range waves {
		if ctx.Err() != nil {
			break
		}
		var wg sync.WaitGroup
		var errMu sync.Mutex
		for _, b := range wave {
			b := b
			if b.Status.Terminal() {
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := e.runBranch(ctx, run, seq, config, policy, profiles[b.BranchID], b); err != nil {
					errMu.Lock()
					if runErr == nil {
						runErr = err
					}
					errMu.Unlock()
				}
			}()
		}
		wg.Wait()
		if runErr != nil {
			break
		}
		if config.StageMode {
			break
		}
	}
	status := "completed"
	if ctx.Err() != nil {
		status = "paused"
	}
	if status == "completed" && config.StageMode && runErr == nil {
		if remaining, loadErr := e.store.LoadBranches(context.Background(), runID); loadErr == nil {
			for _, b := range remaining {
				if !b.Status.Terminal() {
					status = "paused"
					break
				}
			}
		}
	}
	if runErr != nil {
		status = "failed"
	}
	report, reportErr := e.buildReport(context.Background(), runID, wasResumed)
	if reportErr != nil {
		return nil, reportErr
	}
	report.Status = status
	report.Recovered = wasResumed
	report.DeterministicSHA = report.ComputeFingerprint()
	reportJSON, _ := json.MarshalIndent(report, "", "  ")
	if err := e.store.FinalizeRun(context.Background(), runID, status, string(reportJSON), report.DeterministicSHA); err != nil {
		return report, err
	}
	if latest, err := e.store.GetRun(context.Background(), runID); err == nil {
		report.CreatedAt = latest.CreatedAt
		report.UpdatedAt = latest.UpdatedAt
	}
	if runErr != nil {
		return report, runErr
	}
	return report, nil
}

func hasCommittedWork(branches []store.BranchRow) bool {
	for _, b := range branches {
		if b.CommittedSeq > 0 {
			return true
		}
	}
	return false
}

func groupWaves(branches []store.BranchRow) [][]store.BranchRow {
	byWave := make(map[int][]store.BranchRow)
	var keys []int
	for _, b := range branches {
		if b.Status.Terminal() {
			continue
		}
		if _, ok := byWave[b.Wave]; !ok {
			keys = append(keys, b.Wave)
		}
		byWave[b.Wave] = append(byWave[b.Wave], b)
	}
	sort.Ints(keys)
	out := make([][]store.BranchRow, 0, len(keys))
	for _, k := range keys {
		wave := byWave[k]
		sort.Slice(wave, func(i, j int) bool { return wave[i].BranchID < wave[j].BranchID })
		out = append(out, wave)
	}
	return out
}

func (e *Engine) runBranch(ctx context.Context, run *store.RunRow, seq *motion.SequenceData, config model.RunConfig, policy model.PolicySnapshot, profile model.BranchProfile, branch store.BranchRow) error {
	state := seq.Spec.InitialState
	if branch.StateJSON != "" {
		if err := json.Unmarshal([]byte(branch.StateJSON), &state); err != nil {
			return e.failBranch(ctx, run, branch, fmt.Errorf("branch state is corrupt: %w", err))
		}
	}
	curve := []float64{}
	if branch.ErrorCurveJSON != "" {
		_ = json.Unmarshal([]byte(branch.ErrorCurveJSON), &curve)
	}
	nextStep := branch.NextStep
	if nextStep < 1 {
		nextStep = 1
	}
	if err := e.store.UpdateBranchStatus(context.Background(), run.RunID, branch.BranchID, model.BranchRunning, "", nextStep, mustJSON(curve)); err != nil {
		return err
	}
	for step := nextStep; step <= branch.TargetSteps; step++ {
		if ctx.Err() != nil {
			_ = e.store.UpdateBranchStatus(context.Background(), run.RunID, branch.BranchID, model.BranchStopped,
				"graceful stop requested; committed prefix is recoverable", step, mustJSON(curve))
			return nil
		}
		state = motion.AdvanceBranch(state, seq.Spec, profile, step)
		frame, err := e.render.Render(seq.Shared, seq.Spec, state)
		if err != nil {
			return e.failBranch(ctx, run, branch, err)
		}
		if step < 0 || step >= len(seq.Truth) {
			overflow := model.NewError(model.CodeTimelineOverflow, "prediction step exceeds truth frame timeline")
			overflow.SequenceID = seq.Spec.SequenceID
			overflow.Details = map[string]any{"step": step, "truth_frames": len(seq.Truth)}
			return e.failBranch(ctx, run, branch, overflow)
		}
		mae, err := motion.MAE(frame, seq.Truth[step])
		if err != nil {
			return e.failBranch(ctx, run, branch, err)
		}
		rangeOK := motion.CheckRange(state, seq.Spec.ExpectedStateRange)
		curve = append(curve, mae)
		status := model.BranchRunning
		reason := ""
		decision := "continue"
		reclaimed := 0
		if !config.DisablePrune && config.PruneThreshold > 0 && mae > config.PruneThreshold {
			status = model.BranchPruned
			decision = "pruned"
			reason = fmt.Sprintf("MAE %.6f exceeded prune threshold %.6f at step %d", mae, config.PruneThreshold, step)
			reclaimed = branch.TargetSteps - step
			if reclaimed < 0 {
				reclaimed = 0
			}
		} else if step == branch.TargetSteps {
			if branch.TargetSteps < branch.RequestedSteps {
				status = model.BranchQuotaLimited
				decision = "quota_limited"
				reason = fmt.Sprintf("branch step quota truncated %d requested steps to %d", branch.RequestedSteps, branch.TargetSteps)
			} else {
				status = model.BranchCompleted
				decision = "completed"
			}
		}
		stateJSON, _ := json.Marshal(state)
		stateSHA := model.Digest(state)
		framePNG, err := frame.PNG()
		if err != nil {
			return e.failBranch(ctx, run, branch, err)
		}
		curveJSON := mustJSON(curve)
		next := step + 1
		mutation := store.BranchMutation{
			Status:         status,
			Reason:         reason,
			NextStep:       next,
			QuotaUsed:      step,
			ReclaimedQuota: reclaimed,
			FinalMAE:       mae,
			StateJSON:      string(stateJSON),
			StateSHA:       stateSHA,
			LastFrameSHA:   frame.SHA256,
			ErrorCurveJSON: curveJSON,
		}
		var event *store.Event
		if status == model.BranchPruned || status == model.BranchQuotaLimited || status == model.BranchCompleted {
			detail := map[string]any{
				"step": step, "mae": mae, "status": status, "reason": reason,
				"prune_threshold": config.PruneThreshold, "reclaimed_quota": reclaimed,
			}
			event = &store.Event{Kind: string(status), StepNo: step, DetailJSON: mustJSON(detail)}
		}
		seqNo, err := e.store.CommitStep(context.Background(), store.StepCommit{
			RunID:       run.RunID,
			PolicyHash:  run.PolicyHash,
			ExpectedSeq: 0,
			BranchID:    branch.BranchID,
			StepNo:      step,
			StateJSON:   string(stateJSON),
			StateSHA:    stateSHA,
			FramePNG:    framePNG,
			FrameSHA:    frame.SHA256,
			TruthSHA:    seq.Truth[step].SHA256,
			MAE:         mae,
			RangeOK:     rangeOK,
			Decision:    decision,
			Branch:      mutation,
			Event:       event,
		})
		if err != nil {
			return err
		}
		if e.opts.AfterCommit != nil {
			e.opts.AfterCommit(run.RunID, seqNo)
		}
		if status.Terminal() {
			return nil
		}
	}
	return nil
}

func (e *Engine) failBranch(ctx context.Context, run *store.RunRow, branch store.BranchRow, cause error) error {
	appErr := model.AsAppError(cause)
	reason := appErr.Error()
	_ = e.store.UpdateBranchStatus(context.Background(), run.RunID, branch.BranchID, model.BranchFailed, reason, branch.NextStep, branch.ErrorCurveJSON)
	return cause
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func (e *Engine) buildReport(ctx context.Context, runID string, recovered bool) (*model.RunReport, error) {
	run, err := e.store.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	var config model.RunConfig
	_ = json.Unmarshal([]byte(run.ConfigJSON), &config)
	branches, err := e.store.LoadBranches(ctx, runID)
	if err != nil {
		return nil, err
	}
	report := &model.RunReport{
		RunID:             runID,
		SequenceID:        run.SequenceID,
		Status:            run.Status,
		PolicyVersion:     run.PolicyVersion,
		PolicyHash:        run.PolicyHash,
		PolicyFingerprint: run.PolicyHash,
		ConfigSHA256:      run.ConfigHash,
		Config:            config,
		InitialStateSHA:   run.InitialStateSHA,
		InitialFrameSHA:   run.InitialFrameSHA,
		Recovered:         recovered,
		RecoveryCount:     run.RecoveryCount,
		CreatedAt:         run.CreatedAt,
	}
	summary := model.RunSummary{BestBranch: -1}
	for _, b := range branches {
		steps, err := e.store.LoadSteps(ctx, runID, b.BranchID)
		if err != nil {
			return nil, err
		}
		br := model.BranchReport{
			BranchID:       b.BranchID,
			Status:         b.Status,
			Reason:         b.Reason,
			Wave:           b.Wave,
			RequestedSteps: b.RequestedSteps,
			TargetSteps:    b.TargetSteps,
			StepsCommitted: len(steps),
			QuotaUsed:      b.QuotaUsed,
			ReclaimedQuota: b.ReclaimedQuota,
			FinalMAE:       b.FinalMAE,
			FinalStateSHA:  b.StateSHA,
			FinalFrameSHA:  b.LastFrameSHA,
			ErrorCurve:     []float64{},
		}
		if b.StateJSON != "" {
			var state model.MotionState
			if json.Unmarshal([]byte(b.StateJSON), &state) == nil {
				br.FinalState = &state
			}
		}
		if b.ErrorCurveJSON != "" {
			_ = json.Unmarshal([]byte(b.ErrorCurveJSON), &br.ErrorCurve)
		}
		if len(br.ErrorCurve) == 0 {
			for _, s := range steps {
				br.ErrorCurve = append(br.ErrorCurve, s.MAE)
			}
		}
		br.BestStepMAE = -1
		br.WorstStepMAE = -1
		var sum float64
		for i, s := range steps {
			var state model.MotionState
			_ = json.Unmarshal([]byte(s.StateJSON), &state)
			decision := s.Decision
			br.Steps = append(br.Steps, model.StepRecord{
				Step: s.StepNo, State: state, StateSHA256: s.StateSHA, FrameSHA256: s.FrameSHA,
				TruthSHA256: s.TruthSHA, MAE: s.MAE, RangeOK: s.RangeOK, Decision: decision,
			})
			if !s.RangeOK {
				br.RangeViolations = append(br.RangeViolations, s.StepNo)
			}
			if i == 0 || s.MAE < br.BestStepMAE {
				br.BestStepMAE = s.MAE
			}
			if i == 0 || s.MAE > br.WorstStepMAE {
				br.WorstStepMAE = s.MAE
			}
			sum += s.MAE
			summary.CompletedSteps++
		}
		if len(steps) > 0 {
			br.MeanMAE = sum / float64(len(steps))
		}
		report.Branches = append(report.Branches, br)
		summary.ReclaimedQuota += b.ReclaimedQuota
		switch b.Status {
		case model.BranchCompleted, model.BranchQuotaLimited:
			summary.Completed++
		case model.BranchPruned:
			summary.Pruned++
		case model.BranchNotStarted:
			summary.NotStarted++
		case model.BranchStopped:
			summary.Stopped++
		}
	}
	sort.Slice(report.Branches, func(i, j int) bool { return report.Branches[i].BranchID < report.Branches[j].BranchID })
	best := -1
	bestMean := 0.0
	for _, b := range report.Branches {
		if b.Status == model.BranchPruned || b.Status == model.BranchNotStarted || len(b.Steps) == 0 {
			continue
		}
		if best < 0 || b.MeanMAE < bestMean || (b.MeanMAE == bestMean && b.BranchID < best) {
			best = b.BranchID
			bestMean = b.MeanMAE
		}
	}
	summary.BestBranch = best
	summary.BestMeanMAE = bestMean
	summary.CheckpointSeq = run.CheckpointSeq
	report.Summary = summary
	events, err := e.store.LoadEventsStructured(ctx, runID)
	if err == nil {
		for _, ev := range events {
			kind, _ := ev["kind"].(string)
			if kind == string(model.BranchPruned) {
				report.PruneAudit = append(report.PruneAudit, fmt.Sprintf("branch %v step %v: %v", ev["branch_id"], ev["step"], ev["detail"]))
			}
		}
		sort.Strings(report.PruneAudit)
	}
	report.DeterministicSHA = report.ComputeFingerprint()
	return report, nil
}

// ExecuteSync starts a run if needed and waits for its deterministic outcome.
func (e *Engine) ExecuteSync(ctx context.Context, runID string) (*model.RunReport, error) {
	return e.Run(ctx, runID)
}

// Snapshot returns the stored final report, or a live projection derived only
// from committed facts while a run is still progressing.
func (e *Engine) Snapshot(ctx context.Context, runID string) (*model.RunReport, error) {
	run, err := e.store.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	if run.ReportJSON != "" {
		var report model.RunReport
		if err := json.Unmarshal([]byte(run.ReportJSON), &report); err == nil {
			report.Recovered = run.RecoveryCount > 0
			report.RecoveryCount = run.RecoveryCount
			return &report, nil
		}
	}
	report, err := e.buildReport(ctx, runID, run.RecoveryCount > 0)
	if err != nil {
		return nil, err
	}
	report.Status = run.Status
	return report, nil
}

func (e *Engine) Wait(runID string, timeout time.Duration) (*model.RunReport, error) {
	e.mu.Lock()
	a := e.active[runID]
	e.mu.Unlock()
	if a == nil {
		return nil, model.NewError(model.CodeNotFound, "run is not active")
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-a.done:
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.report, a.err
	case <-timer.C:
		return nil, model.NewError(model.CodeRunBusy, "timed out waiting for run")
	}
}
