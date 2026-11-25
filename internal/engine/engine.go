package engine

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"leasearbiter/internal/core"
	"leasearbiter/internal/loopback"
	"leasearbiter/internal/orchestrator"
	"leasearbiter/internal/store"
)

type HealDispatcher interface {
	DispatchRaw(ctx context.Context, decision core.HealDecision, options loopback.DispatchOptions, requestID string, sentAtMS int64) (core.HealReceipt, *core.BizError)
}

type Config struct {
	Store       *store.Store
	Dispatcher  HealDispatcher
	Coordinator *orchestrator.Coordinator
	Clock       func() time.Time
}

type Engine struct {
	store       *store.Store
	dispatcher  HealDispatcher
	coordinator *orchestrator.Coordinator
	now         func() time.Time
	writeMu     sync.Mutex
}

type BatchReport struct {
	IntentID          string               `json:"intent_id"`
	StateVersion      int64                `json:"state_version"`
	SnapshotVersion   int64                `json:"snapshot_version"`
	Committed         bool                 `json:"committed"`
	Results           []core.RebuildResult `json:"results"`
	Heals             []core.HealExecution `json:"heals"`
	Deferred          []core.DeferredEntry `json:"deferred"`
	RemainingBudgetMS int64                `json:"remaining_budget_ms"`
}

type ProcessOptions struct {
	Source  string
	RunHeal bool
}

type intentPlan struct {
	Source           string                `json:"source"`
	HolderCount      int                   `json:"holder_count"`
	ObservationCount int                   `json:"observation_count"`
	Actions          []plannedIntentAction `json:"actions"`
}

type plannedIntentAction struct {
	HolderID         string          `json:"holder_id"`
	Action           core.HealAction `json:"action"`
	DriftMagnitudeMS int64           `json:"drift_magnitude_ms"`
	Trigger          string          `json:"trigger"`
}

func New(config Config) (*Engine, error) {
	if config.Store == nil {
		return nil, fmt.Errorf("store is required")
	}
	if config.Coordinator == nil {
		return nil, fmt.Errorf("coordinator is required")
	}
	clock := config.Clock
	if clock == nil {
		clock = time.Now
	}
	return &Engine{
		store:       config.Store,
		dispatcher:  config.Dispatcher,
		coordinator: config.Coordinator,
		now:         clock,
	}, nil
}

func (e *Engine) SubmitSnapshot(ctx context.Context, snapshot core.Snapshot) (core.SnapshotSubmission, *core.BizError) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	current, err := e.store.CurrentState(ctx)
	if err != nil {
		return core.SnapshotSubmission{}, storeError(err)
	}
	if snapshot.Version <= current.SnapshotVersion {
		return core.SnapshotSubmission{}, &core.BizError{
			Code:            core.CodeStaleSnapshot,
			Message:         fmt.Sprintf("snapshot version %d is not greater than current snapshot version %d", snapshot.Version, current.SnapshotVersion),
			CurrentVersion:  current.SnapshotVersion,
			ObservedVersion: snapshot.Version,
		}
	}
	if err := core.ValidateSnapshot(snapshot); err != nil {
		return core.SnapshotSubmission{}, err
	}
	observations, mismatch := core.DeduplicateDataset(snapshot.Dataset)
	if mismatch != nil {
		return core.SnapshotSubmission{}, mismatch
	}
	intent, err := e.store.BeginIntent(ctx, "snapshot_switch", map[string]any{
		"snapshot_version": snapshot.Version,
		"generated_at_ms":  snapshot.GeneratedAtMS,
		"digest":           snapshot.Digest,
		"holders":          len(snapshot.Dataset.Holders),
		"observations":     len(observations),
	})
	if err != nil {
		return core.SnapshotSubmission{}, storeError(err)
	}
	next := core.NewState()
	next.Version = intent.TargetVersion
	next.SnapshotVersion = snapshot.Version
	next.SnapshotGeneratedAtMS = snapshot.GeneratedAtMS
	next.SnapshotDigest = snapshot.Digest
	next.Policy = snapshot.Dataset.Policy
	next.Holders = append([]core.Holder(nil), snapshot.Dataset.Holders...)
	next.Observations = observations
	if err := e.store.CommitIntent(ctx, intent.ID, next); err != nil {
		_ = e.store.AbortIntent(ctx, intent.ID, err.Error())
		return core.SnapshotSubmission{}, storeError(err)
	}
	return core.SnapshotSubmission{
		StateVersion:    next.Version,
		SnapshotVersion: next.SnapshotVersion,
		Digest:          next.SnapshotDigest,
	}, nil
}

// Ingest validates the whole batch, writes a durable intent, then performs
// deduplication, deterministic rebuild and bounded healing. A successful commit
// advances one immutable state version. If the queue rejects the batch, the
// prepared intent is aborted and no observation or drift state is changed.
func (e *Engine) Ingest(ctx context.Context, dataset core.Dataset, options ProcessOptions) (BatchReport, *core.BizError) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	if err := core.ValidateDataset(dataset); err != nil {
		return BatchReport{}, err
	}
	current, err := e.store.CurrentState(ctx)
	if err != nil {
		return BatchReport{}, storeError(err)
	}
	holders := mergeHolders(current.Holders, dataset.Holders)
	observations, mismatch := core.MergeStoredObservations(current.Observations, dataset.Observations)
	if mismatch != nil {
		return BatchReport{}, mismatch
	}
	intent, err := e.store.BeginIntent(ctx, "batch_rebuild", intentPlan{
		Source:           options.Source,
		HolderCount:      len(holders),
		ObservationCount: len(observations),
	})
	if err != nil {
		return BatchReport{}, storeError(err)
	}
	report, bizErr := e.runLocked(ctx, current, holders, observations, dataset.Policy, intent, options)
	if report.IntentID == "" {
		report.IntentID = intent.ID
	}
	return report, bizErr
}

// Reprocess rebuilds and heals the currently committed observation set without
// adding observations. This is useful for consuming work deferred by a previous
// round budget.
func (e *Engine) Reprocess(ctx context.Context, options ProcessOptions) (BatchReport, *core.BizError) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	current, err := e.store.CurrentState(ctx)
	if err != nil {
		return BatchReport{}, storeError(err)
	}
	intent, err := e.store.BeginIntent(ctx, "batch_rebuild", intentPlan{
		Source:           options.Source,
		HolderCount:      len(current.Holders),
		ObservationCount: len(current.Observations),
	})
	if err != nil {
		return BatchReport{}, storeError(err)
	}
	return e.runLocked(ctx, current, current.Holders, current.Observations, current.Policy, intent, options)
}

func (e *Engine) runLocked(
	ctx context.Context,
	current core.State,
	holders []core.Holder,
	observations []core.StoredObservation,
	policy core.Policy,
	intent store.Intent,
	options ProcessOptions,
) (BatchReport, *core.BizError) {
	nowMS := e.now().UnixMilli()
	results := core.RebuildAll(holders, observations, policy)
	resultByHolder := make(map[string]core.RebuildResult, len(results))
	holderByID := make(map[string]core.Holder, len(holders))
	observationsByHolder := make(map[string][]core.StoredObservation, len(holders))
	for _, holder := range holders {
		holderByID[holder.ID] = holder
	}
	for _, record := range observations {
		observationsByHolder[record.HolderID] = append(observationsByHolder[record.HolderID], record)
	}
	actions := make([]plannedIntentAction, 0)
	for _, result := range results {
		resultByHolder[result.HolderID] = result
		if options.RunHeal && result.Drift.Drifted {
			if decision := core.DecideHeal(holderByID[result.HolderID], result, nowMS); decision != nil {
				actions = append(actions, plannedIntentAction{
					HolderID:         decision.HolderID,
					Action:           decision.Action,
					DriftMagnitudeMS: decision.DriftMagnitudeMS,
					Trigger:          decision.Trigger,
				})
			}
		}
	}
	// The plan is attached to the already-durable intent before any HTTP heal
	// starts. A crash therefore leaves a reviewable action plan plus PREPARED
	// intent, never an unexplained external side effect.
	plan := intentPlan{
		Source:           options.Source,
		HolderCount:      len(holders),
		ObservationCount: len(observations),
		Actions:          actions,
	}
	if err := e.store.UpdateIntentPayload(ctx, intent.ID, plan); err != nil {
		_ = e.store.AbortIntent(ctx, intent.ID, err.Error())
		return BatchReport{IntentID: intent.ID}, storeError(err)
	}

	tasks := make([]orchestrator.Task, 0, len(results))
	for _, result := range results {
		result := result
		tasks = append(tasks, orchestrator.Task{
			HolderID:         result.HolderID,
			DriftMagnitudeMS: result.Drift.MagnitudeMS,
			Run: func(taskCtx context.Context) orchestrator.TaskResult {
				taskResult := orchestrator.TaskResult{HolderID: result.HolderID}
				if taskCtx.Err() != nil {
					taskResult.Deferred = true
					taskResult.DeferredReason = "ROUND_BUDGET_EXHAUSTED"
					return taskResult
				}
				// The authoritative per-holder reconstruction runs inside the
				// coordinated task, so both reconstruction and heal dispatch are
				// bounded by the global concurrency slot and the round deadline.
				holder := holderByID[result.HolderID]
				liveResult := core.RebuildHolder(holder, observationsByHolder[result.HolderID], policy)
				taskResult.Rebuild = &liveResult
				if taskCtx.Err() != nil {
					taskResult.Deferred = true
					taskResult.DeferredReason = "ROUND_BUDGET_EXHAUSTED"
					return taskResult
				}
				if !options.RunHeal || !liveResult.Drift.Drifted {
					return taskResult
				}
				decision := core.DecideHeal(holder, liveResult, e.now().UnixMilli())
				if decision == nil {
					return taskResult
				}
				execution := e.dispatch(taskCtx, intent.TargetVersion, *decision, nowMS)
				taskResult.Heal = &execution
				if execution.ErrorCode != "" {
					taskResult.Error = &core.BizError{
						Code:     execution.ErrorCode,
						Message:  execution.ErrorMessage,
						HolderID: result.HolderID,
					}
				}
				return taskResult
			},
		})
	}
	outcome := e.coordinator.Execute(ctx, tasks)
	if outcome.Err != nil {
		_ = e.store.AbortIntent(ctx, intent.ID, outcome.Err.Error())
		return BatchReport{IntentID: intent.ID, Committed: false, RemainingBudgetMS: outcome.RemainingBudgetMS}, outcome.Err
	}

	heals := make(map[string]core.HealExecution, len(results))
	for _, deferred := range outcome.Deferred {
		if previous, ok := current.Heals[deferred.HolderID]; ok {
			heals[deferred.HolderID] = previous
		}
	}
	var firstError *core.BizError
	for _, completed := range outcome.Completed {
		if completed.Error != nil && firstError == nil {
			firstError = completed.Error
		}
		if completed.Heal != nil {
			heals[completed.HolderID] = *completed.Heal
		}
		if completed.Rebuild != nil {
			resultByHolder[completed.HolderID] = *completed.Rebuild
		}
		if completed.Heal != nil {
			continue
		}
		// A dedupe-only replay still recomputes drift. Keep the last durable
		// heal verdict for a still-drifted holder instead of erasing it merely
		// because this round did not issue another HTTP action.
		if !options.RunHeal && resultByHolder[completed.HolderID].Drift.Drifted {
			if previous, ok := current.Heals[completed.HolderID]; ok {
				heals[completed.HolderID] = previous
			}
		}
	}

	nextHolders := make([]core.Holder, 0, len(holders))
	for _, holder := range holders {
		result := resultByHolder[holder.ID]
		switch {
		case result.Drift.Drifted && heals[holder.ID].Decision.Action == core.HealActionRevokeLease && heals[holder.ID].Status == core.HealStatusSent:
			holder.Status = core.HolderRevoked
		case result.Drift.Drifted:
			holder.Status = core.HolderDrifted
		default:
			holder.Status = core.HolderActive
		}
		nextHolders = append(nextHolders, holder)
	}
	sort.Slice(nextHolders, func(i, j int) bool { return nextHolders[i].ID < nextHolders[j].ID })

	next := core.State{
		Version:               intent.TargetVersion,
		SnapshotVersion:       current.SnapshotVersion,
		SnapshotGeneratedAtMS: current.SnapshotGeneratedAtMS,
		SnapshotDigest:        current.SnapshotDigest,
		Policy:                policy,
		Holders:               nextHolders,
		Observations:          observations,
		Results:               resultByHolder,
		Heals:                 heals,
		Deferred:              outcome.Deferred,
	}
	finalResults := make([]core.RebuildResult, 0, len(resultByHolder))
	for _, result := range resultByHolder {
		finalResults = append(finalResults, result)
	}
	sort.Slice(finalResults, func(i, j int) bool { return finalResults[i].HolderID < finalResults[j].HolderID })
	if err := e.store.CommitIntent(ctx, intent.ID, next); err != nil {
		_ = e.store.AbortIntent(ctx, intent.ID, err.Error())
		return BatchReport{}, storeError(err)
	}
	report := BatchReport{
		IntentID:          intent.ID,
		StateVersion:      next.Version,
		SnapshotVersion:   next.SnapshotVersion,
		Committed:         true,
		Results:           finalResults,
		Heals:             sortedHeals(heals),
		Deferred:          outcome.Deferred,
		RemainingBudgetMS: outcome.RemainingBudgetMS,
	}
	return report, firstError
}

func (e *Engine) dispatch(ctx context.Context, targetVersion int64, decision core.HealDecision, sentAtMS int64) core.HealExecution {
	execution := core.HealExecution{
		Decision:      decision,
		Status:        core.HealStatusFailed,
		SentAtMS:      sentAtMS,
		RequestBodyID: fmt.Sprintf("heal-%s-v%d", decision.HolderID, targetVersion),
	}
	if e.dispatcher == nil {
		execution.ErrorCode = core.CodeHealFailed
		execution.ErrorMessage = "no heal dispatcher configured"
		execution.CompletedAtMS = e.now().UnixMilli()
		return execution
	}
	receipt, bizErr := e.dispatcher.DispatchRaw(ctx, decision, loopback.DispatchOptions{}, execution.RequestBodyID, sentAtMS)
	execution.CompletedAtMS = e.now().UnixMilli()
	if bizErr != nil {
		execution.ErrorCode = bizErr.Code
		execution.ErrorMessage = bizErr.Message
		switch bizErr.Code {
		case core.CodeHealTimeout:
			execution.Status = core.HealStatusTimeout
		case core.CodeHealRejected:
			execution.Status = core.HealStatusRejected
		default:
			execution.Status = core.HealStatusFailed
		}
		return execution
	}
	execution.Status = core.HealStatusSent
	if !receipt.Applied {
		execution.Status = core.HealStatusRejected
		execution.ErrorCode = core.CodeHealRejected
		execution.ErrorMessage = "loopback endpoint did not apply the heal action"
	}
	return execution
}

func (e *Engine) Read(ctx context.Context, observedVersion int64) (core.ReadResult, *core.BizError) {
	current, err := e.store.CurrentState(ctx)
	if err != nil {
		return core.ReadResult{}, storeError(err)
	}
	if current.Version < observedVersion {
		return core.ReadResult{}, &core.BizError{
			Code:            core.CodeStaleRead,
			Message:         fmt.Sprintf("reconstruction version %d is behind observed version %d", current.Version, observedVersion),
			CurrentVersion:  current.Version,
			ObservedVersion: observedVersion,
		}
	}
	return readResult(current), nil
}

func (e *Engine) Current(ctx context.Context) (core.State, *core.BizError) {
	state, err := e.store.CurrentState(ctx)
	if err != nil {
		return core.State{}, storeError(err)
	}
	return state, nil
}

func (e *Engine) Recover(ctx context.Context) (store.RecoveryReport, *core.BizError) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	report, err := e.store.Recover(ctx)
	if err != nil {
		return store.RecoveryReport{}, storeError(err)
	}
	return report, nil
}

func readResult(state core.State) core.ReadResult {
	results := make([]core.RebuildResult, 0, len(state.Results))
	for _, result := range state.Results {
		results = append(results, result)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].HolderID < results[j].HolderID })
	holders := append([]core.Holder(nil), state.Holders...)
	sort.Slice(holders, func(i, j int) bool { return holders[i].ID < holders[j].ID })
	deferred := make([]core.DeferredEntry, len(state.Deferred))
	copy(deferred, state.Deferred)
	return core.ReadResult{
		StateVersion:          state.Version,
		SnapshotVersion:       state.SnapshotVersion,
		SnapshotGeneratedAtMS: state.SnapshotGeneratedAtMS,
		SnapshotDigest:        state.SnapshotDigest,
		Policy:                state.Policy,
		Holders:               holders,
		Results:               results,
		Heals:                 sortedHeals(state.Heals),
		Deferred:              deferred,
	}
}

func sortedHeals(heals map[string]core.HealExecution) []core.HealExecution {
	out := make([]core.HealExecution, 0, len(heals))
	for _, heal := range heals {
		out = append(out, heal)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Decision.HolderID != out[j].Decision.HolderID {
			return out[i].Decision.HolderID < out[j].Decision.HolderID
		}
		return out[i].SentAtMS < out[j].SentAtMS
	})
	return out
}

func mergeHolders(existing, incoming []core.Holder) []core.Holder {
	byID := make(map[string]core.Holder, len(existing)+len(incoming))
	for _, holder := range existing {
		byID[holder.ID] = holder
	}
	for _, holder := range incoming {
		byID[holder.ID] = holder
	}
	out := make([]core.Holder, 0, len(byID))
	for _, holder := range byID {
		out = append(out, holder)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func storeError(err error) *core.BizError {
	return &core.BizError{Code: core.CodeStoreFailed, Message: err.Error()}
}
