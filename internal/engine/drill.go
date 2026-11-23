package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"recoveryengine/internal/graph"
	"recoveryengine/internal/model"
	"recoveryengine/internal/probe"
	"recoveryengine/internal/store"
)

type DrillResult struct {
	ID                     string             `json:"id"`
	Version                int64              `json:"version"`
	State                  string             `json:"state"`
	StartedAt              string             `json:"started_at"`
	FinishedAt             string             `json:"finished_at"`
	FailedComponents       []string           `json:"failed_components"`
	MaxConcurrency         int                `json:"max_concurrency"`
	RecoveryBatches        []DrillBatchResult `json:"recovery_batches"`
	BlockedComponents      []string           `json:"blocked_components,omitempty"`
	OperationCodes         []string           `json:"operation_codes"`
	CodeCounts             map[string]int     `json:"code_counts"`
	AllOperationsSucceeded bool               `json:"all_operations_succeeded"`
	ReservationsReleased   bool               `json:"reservations_released"`
	IntentID               string             `json:"intent_id"`
	Replayed               bool               `json:"replayed,omitempty"`
}

type DrillBatchResult struct {
	Batch      int                    `json:"batch"`
	Components []string               `json:"components"`
	Results    []ComponentDrillResult `json:"results"`
}

type ComponentDrillResult struct {
	ComponentID   string                 `json:"component_id"`
	Probe         probe.Result           `json:"probe"`
	Recovery      probe.Result           `json:"recovery"`
	Reservation   store.ReservationAudit `json:"reservation"`
	ReservationID string                 `json:"reservation_id"`
	Released      bool                   `json:"released"`
}

func (e *Engine) ExecuteDrill(ctx context.Context, request model.DrillRequest, options CommitOptions) (*DrillResult, *model.BusinessError) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	if request.MaxConcurrency == 0 {
		request.MaxConcurrency = 2
	}
	if request.MaxConcurrency < 1 || request.MaxConcurrency > 16 {
		return nil, model.NewError(model.CodeSpecInvalid, "max_concurrency must be between 1 and 16", 422)
	}
	if len(request.FailedComponents) == 0 {
		return nil, model.NewError(model.CodeSpecInvalid, "failed_components must not be empty", 422)
	}
	request.FailedComponents = uniqueSorted(request.FailedComponents)

	submission, err := e.prepareSubmission(ctx, request.IdempotencyKey, "drill", request)
	if err != nil {
		return nil, err
	}
	if result, replayErr := e.replayDrill(submission); result != nil || replayErr != nil {
		return result, replayErr
	}
	current := e.current()
	intentID, intentErr := e.beginIntent(ctx, request.IdempotencyKey, "drill", current.snapshot.Version, current.snapshot.Version, request, options)
	if intentErr != nil {
		return nil, e.rejectOperation(ctx, request.IdempotencyKey, "", intentErr.Status, intentErr)
	}
	if request.Version != current.snapshot.Version {
		business := model.NewError(model.CodeVersionMismatch, "drill version is not the current committed version", 409).
			WithContext("requested_version", request.Version).
			WithContext("current_version", current.snapshot.Version)
		return nil, e.rejectOperation(ctx, request.IdempotencyKey, intentID, business.Status, business)
	}

	impactResults, missing := current.analysis.ImpactFor(request.FailedComponents)
	if len(missing) > 0 {
		business := model.NewError(model.CodeSpecInvalid, "drill references unknown failed components", 422).
			WithContext("missing_components", missing)
		return nil, e.rejectOperation(ctx, request.IdempotencyKey, intentID, business.Status, business)
	}
	batches, blocked := combineRecoveryBatches(impactResults, current.analysis)
	if len(batches) == 0 {
		business := model.NewError(model.CodeSpecInvalid, "drill has no runnable components", 422).
			WithContext("blocked_components", blocked)
		return nil, e.rejectOperation(ctx, request.IdempotencyKey, intentID, business.Status, business)
	}

	drillID := newDrillID()
	startedAt := store.Now()
	requestJSON, _ := json.Marshal(request)
	if err := e.store.StartDrill(ctx, store.DrillRecord{
		ID: drillID, SubmissionKey: request.IdempotencyKey, Version: request.Version,
		State: "running", RequestJSON: requestJSON, StartedAt: startedAt,
	}); err != nil {
		business := model.NewError(model.CodeInternal, "failed to start persisted drill", 500).WithContext("cause", err.Error())
		return nil, e.rejectOperation(ctx, request.IdempotencyKey, intentID, business.Status, business)
	}

	result, audits, business := e.runDrill(context.WithoutCancel(ctx), drillID, current.snapshot, batches, blocked, request, intentID, startedAt)
	if business != nil {
		_ = e.store.AbortDrill(ctx, drillID, store.Now())
		return nil, e.rejectOperation(ctx, request.IdempotencyKey, intentID, business.Status, business)
	}
	resultJSON, _ := json.Marshal(result)
	now := store.Now()
	result.FinishedAt = now
	resultJSON, _ = json.Marshal(result)
	if err := e.store.CompleteDrill(ctx, store.DrillRecord{
		ID: drillID, SubmissionKey: request.IdempotencyKey, Version: request.Version,
		State: "completed", RequestJSON: requestJSON, ResultJSON: resultJSON, StartedAt: startedAt,
	}, audits, intentID, 200, now); err != nil {
		business := model.NewError(model.CodeInternal, "failed to atomically publish drill result", 500).WithContext("cause", err.Error())
		return nil, e.rejectOperation(ctx, request.IdempotencyKey, intentID, business.Status, business)
	}
	return result, nil
}

func (e *Engine) runDrill(ctx context.Context, drillID string, snapshot model.Snapshot, batches []graph.BatchGroup, blocked []string, request model.DrillRequest, intentID, startedAt string) (*DrillResult, []store.ReservationAudit, *model.BusinessError) {
	components := snapshot.ComponentMap()
	batchResults := make([]DrillBatchResult, 0, len(batches))
	audits := make([]store.ReservationAudit, 0)
	codeCounts := map[string]int{}
	allOK := true
	var resultMu sync.Mutex

	for _, batch := range batches {
		sem := make(chan struct{}, request.MaxConcurrency)
		outcomes := make([]ComponentDrillResult, len(batch.Components))
		var wg sync.WaitGroup
		for index, componentID := range batch.Components {
			component, exists := components[componentID]
			if !exists {
				return nil, nil, model.NewError(model.CodeInternal, "derived batch references unknown component", 500)
			}
			wg.Add(1)
			go func(index int, component model.Component) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				outcome, audit := e.runComponent(ctx, drillID, component)
				outcomes[index] = outcome
				resultMu.Lock()
				audits = append(audits, audit)
				codeCounts[outcome.Probe.Code]++
				codeCounts[outcome.Recovery.Code]++
				if !outcome.Probe.OK || !outcome.Recovery.OK {
					allOK = false
				}
				resultMu.Unlock()
			}(index, component)
		}
		wg.Wait()
		batchResults = append(batchResults, DrillBatchResult{
			Batch: batch.Batch, Components: append([]string(nil), batch.Components...), Results: outcomes,
		})
	}
	codes := make([]string, 0, len(codeCounts))
	for code := range codeCounts {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	sort.Slice(audits, func(i, j int) bool { return audits[i].ComponentID < audits[j].ComponentID })
	result := &DrillResult{
		ID: drillID, Version: snapshot.Version, State: "completed",
		StartedAt: startedAt, FinishedAt: store.Now(), FailedComponents: request.FailedComponents,
		MaxConcurrency: request.MaxConcurrency, RecoveryBatches: batchResults,
		BlockedComponents: blocked, OperationCodes: codes, CodeCounts: codeCounts,
		AllOperationsSucceeded: allOK, ReservationsReleased: true, IntentID: intentID,
	}
	return result, audits, nil
}

func (e *Engine) runComponent(ctx context.Context, drillID string, component model.Component) (ComponentDrillResult, store.ReservationAudit) {
	reservation := e.reservations.Acquire(drillID, component.ID, component.Resources)
	defer e.reservations.Release(reservation.ID)

	probeResult := e.probe.Execute(ctx, probe.Operation{
		Kind: probe.KindProbe, ComponentID: component.ID,
		URL:            replaceComponent(component.HealthProbe.URL, component.ID),
		Method:         component.HealthProbe.Method,
		Timeout:        time.Duration(component.HealthProbe.TimeoutMs) * time.Millisecond,
		ExpectedStatus: component.HealthProbe.ExpectedStatus,
	})
	recoveryResult := e.probe.Execute(ctx, probe.Operation{
		Kind: probe.KindRecover, ComponentID: component.ID,
		URL:     replaceComponent(component.RecoveryRequest.URL, component.ID),
		Method:  component.RecoveryRequest.Method,
		Timeout: time.Duration(component.RecoveryRequest.TimeoutMs) * time.Millisecond,
		Body:    component.RecoveryRequest.Body,
	})
	reason := "completed"
	switch {
	case probeResult.Code == model.CodeProbeTimeout || recoveryResult.Code == model.CodeRecoverTimeout:
		reason = "timeout_released"
	case probeResult.Code == model.CodeProbeConnectionRefused || recoveryResult.Code == model.CodeRecoverConnectionRef:
		reason = "connection_refused_released"
	case probeResult.Code == model.CodeProbeRejected || recoveryResult.Code == model.CodeRecoverRejected:
		reason = "business_rejected_released"
	case !probeResult.OK || !recoveryResult.OK:
		reason = "failed_released"
	}
	audit := store.ReservationAudit{
		ComponentID: component.ID, CPUMillicores: component.Resources.CPUMillicores,
		MemoryBytes: component.Resources.MemoryBytes, State: "released", Reason: reason,
	}
	return ComponentDrillResult{
		ComponentID: component.ID, Probe: probeResult, Recovery: recoveryResult,
		Reservation: audit, ReservationID: reservation.ID, Released: true,
	}, audit
}

func (e *Engine) replayDrill(submission store.Submission) (*DrillResult, *model.BusinessError) {
	switch submission.State {
	case "committed":
		var result DrillResult
		if err := json.Unmarshal(submission.ResponseJSON, &result); err != nil {
			return nil, model.NewError(model.CodeInternal, "stored drill response is unreadable", 500)
		}
		result.Replayed = true
		return &result, nil
	case "rejected":
		return nil, e.replayError(submission)
	default:
		return nil, nil
	}
}

func combineRecoveryBatches(impacts []graph.ImpactResult, analysis *graph.Analysis) ([]graph.BatchGroup, []string) {
	components := map[string]struct{}{}
	blocked := map[string]struct{}{}
	for _, impact := range impacts {
		for _, row := range impact.Affected {
			if row.Blocked {
				blocked[row.Component] = struct{}{}
				continue
			}
			components[row.Component] = struct{}{}
		}
	}
	levels := map[int][]string{}
	for component := range components {
		level, ok := analysis.TopologyBatches[component]
		if !ok {
			blocked[component] = struct{}{}
			continue
		}
		levels[level] = append(levels[level], component)
	}
	levelKeys := make([]int, 0, len(levels))
	for level := range levels {
		levelKeys = append(levelKeys, level)
	}
	sort.Ints(levelKeys)
	batches := make([]graph.BatchGroup, 0, len(levelKeys))
	for batch, level := range levelKeys {
		members := levels[level]
		sort.Strings(members)
		batches = append(batches, graph.BatchGroup{Batch: batch, Components: members})
	}
	blockedList := make([]string, 0, len(blocked))
	for component := range blocked {
		blockedList = append(blockedList, component)
	}
	sort.Strings(blockedList)
	return batches, blockedList
}

func uniqueSorted(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func replaceComponent(raw, componentID string) string {
	return strings.ReplaceAll(raw, "{component}", componentID)
}

func (e *Engine) DrillByID(ctx context.Context, id string) (map[string]any, error) {
	drill, err := e.store.GetDrill(ctx, id)
	if err != nil {
		return nil, err
	}
	result := map[string]any{
		"id": drill.ID, "version": drill.Version, "state": drill.State,
		"request": json.RawMessage(drill.RequestJSON), "started_at": drill.StartedAt,
		"completed_at": drill.CompletedAt,
	}
	if len(drill.ResultJSON) > 0 {
		result["result"] = json.RawMessage(drill.ResultJSON)
	}
	return result, nil
}

func (e *Engine) ReservationSummary() map[string]any {
	return map[string]any{
		"active":         e.reservations.Snapshot(),
		"released_total": e.reservations.ReleasedTotal(),
	}
}

func formatDrillCode(code string) string {
	if code == "" {
		return "E_INTERNAL"
	}
	return fmt.Sprintf("%s", code)
}
