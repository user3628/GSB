package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"driftengine/internal/batch"
	"driftengine/internal/drift"
	"driftengine/internal/errs"
	"driftengine/internal/spec"
	"driftengine/internal/store"
	"driftengine/internal/writeback"
)

type RoundOptions struct {
	Timeout time.Duration
}

type RoundResult struct {
	Version          int64                 `json:"version"`
	StartedAt        time.Time             `json:"started_at"`
	FinishedAt       time.Time             `json:"finished_at"`
	DurationMS       int64                 `json:"duration_ms"`
	BatchResults     []*BatchTriggerResult `json:"batch_results"`
	ConvergedObjects int                   `json:"converged_objects"`
	FailedObjects    int                   `json:"failed_objects"`
}

func (e *Engine) resetScheduler(policy spec.Policy) {
	e.schedMu.Lock()
	defer e.schedMu.Unlock()
	e.sched = NewScheduler(policy.MaxParallelBatches, policy.WriteQueueSize)
}

func (e *Engine) getScheduler() *Scheduler {
	e.schedMu.RLock()
	defer e.schedMu.RUnlock()
	return e.sched
}

func (e *Engine) TriggerBatch(ctx context.Context, batchID, requestHash string) (*BatchTriggerResult, error) {
	run, err := e.startBatch(ctx, batchID, requestHash)
	if err != nil {
		return nil, err
	}
	select {
	case <-run.done:
		return run.result, run.err
	case <-ctx.Done():
		return nil, errs.New(errs.Internal, "batch trigger context ended while batch continues", ctx.Err())
	}
}

func (e *Engine) startBatch(ctx context.Context, batchID, requestHash string) (*batchRun, error) {
	view := e.View()
	if view == nil {
		return nil, errs.New(errs.Internal, "no committed snapshot")
	}
	b := findBatch(view.Plan, batchID)
	if b == nil {
		return nil, errs.New(errs.SpecInvalid, "batch not found in current committed version", batchID)
	}
	state := view.BatchStates[batchID]
	if state == nil {
		return nil, errs.New(errs.Internal, "batch state is missing", batchID)
	}
	effectiveHash := requestHash
	if effectiveHash == "" {
		effectiveHash = state.RequestHash
	}
	if requestHash != "" && state.RequestHash != "" && requestHash != state.RequestHash && state.TriggerCount > 0 {
		return nil, errs.New(errs.ConflictRequest, "same batch id was first triggered with different content", map[string]string{
			"batch_id":       batchID,
			"request_hash":   requestHash,
			"committed_hash": state.RequestHash,
		})
	}

	e.inflightMu.Lock()
	if run, ok := e.inflight[batchID]; ok {
		e.inflightMu.Unlock()
		if requestHash != "" && run.requestHash != requestHash {
			return nil, errs.New(errs.ConflictRequest, "batch is already running with different content", batchID)
		}
		return run, nil
	}
	if state.Status == "completed" || state.Status == "failed" {
		e.inflightMu.Unlock()
		return completedRun(state), nil
	}
	run := &batchRun{requestHash: effectiveHash, done: make(chan struct{})}
	e.inflight[batchID] = run
	e.inflightMu.Unlock()

	organizeIntentID := organizeBatchIntentID(view.Version, batchID)
	objectIDsJSON, err := json.Marshal(b.ObjectIDs)
	if err != nil {
		e.finishInflight(batchID, run, nil, err)
		return nil, errs.New(errs.Internal, "cannot encode batch intent", err.Error())
	}
	budgetJSON, err := json.Marshal(b.Budget)
	if err != nil {
		e.finishInflight(batchID, run, nil, err)
		return nil, errs.New(errs.Internal, "cannot encode batch budget", err.Error())
	}
	if err := e.store.UpsertIntent(ctx, store.Intent{
		ID:          organizeIntentID,
		Type:        "organize_batch",
		Version:     view.Version,
		BatchID:     batchID,
		PayloadJSON: `{"object_ids":` + string(objectIDsJSON) + `,"budget":` + string(budgetJSON) + `}`,
		Status:      "running",
	}); err != nil {
		internalErr := errs.New(errs.Internal, "cannot persist batch organization intent", err.Error())
		e.finishInflight(batchID, run, nil, internalErr)
		return nil, internalErr
	}

	now := time.Now().UTC()
	running := *state
	running.Status = "running"
	running.TriggerCount++
	running.RequestHash = effectiveHash
	running.StartedAt = &now
	running.UpdatedAt = now
	if err := e.persistBatchState(ctx, view.Version, &running); err != nil {
		_ = e.store.SetIntentStatus(context.Background(), organizeIntentID, "aborted", err.Error())
		e.finishInflight(batchID, run, nil, err)
		return nil, err
	}

	err = e.getScheduler().Submit(batchID, func() {
		result, runErr := e.executeBatch(ctx, view, b)
		if result != nil {
			result.TriggerCount = running.TriggerCount
		}
		e.finishInflight(batchID, run, result, runErr)
	})
	if err != nil {
		failed := running
		failed.Status = "planned"
		failed.UpdatedAt = time.Now().UTC()
		_ = e.persistBatchState(context.Background(), view.Version, &failed)
		_ = e.store.SetIntentStatus(context.Background(), organizeIntentID, errs.Code(err), err.Error())
		e.finishInflight(batchID, run, nil, err)
		return nil, err
	}
	return run, nil
}

func (e *Engine) finishInflight(batchID string, run *batchRun, result *BatchTriggerResult, err error) {
	run.result = result
	run.err = err
	close(run.done)
	e.inflightMu.Lock()
	delete(e.inflight, batchID)
	e.inflightMu.Unlock()
}

func completedRun(state *BatchState) *batchRun {
	result := &BatchTriggerResult{
		BatchID:      state.BatchID,
		Status:       state.Status,
		TriggerCount: state.TriggerCount,
		ObjectPhases: state.ObjectPhases,
	}
	done := make(chan struct{})
	close(done)
	return &batchRun{done: done, result: result}
}

func (e *Engine) RunRound(ctx context.Context, opts RoundOptions) (*RoundResult, error) {
	view := e.View()
	if view == nil {
		return nil, errs.New(errs.Internal, "no committed snapshot")
	}
	if view.Plan != nil && view.Plan.DependencyCycle {
		return nil, errs.New(errs.DependencyCycle, "refusing write round while dependency cycle is unresolved", map[string]any{
			"cycle_ids": view.Plan.CycleIDs,
		})
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = time.Duration(view.Snapshot.Policy.RoundTimeBudgetMS) * time.Millisecond
	}
	roundCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	started := time.Now().UTC()
	result := &RoundResult{Version: view.Version, StartedAt: started, BatchResults: []*BatchTriggerResult{}}

	layers := map[int][]*batch.Batch{}
	maxLayer := 0
	for _, b := range view.Plan.Batches {
		layers[b.Layer] = append(layers[b.Layer], b)
		if b.Layer > maxLayer {
			maxLayer = b.Layer
		}
	}
	var queueErr error
	for layer := 0; layer <= maxLayer; layer++ {
		batches := layers[layer]
		runs := make([]*batchRun, 0, len(batches))
		for _, b := range batches {
			run, err := e.startBatch(roundCtx, b.BatchID, "")
			if err != nil {
				if errs.Code(err) == errs.WriteQueueFull && queueErr == nil {
					queueErr = err
				}
				continue
			}
			runs = append(runs, run)
		}
		for _, run := range runs {
			select {
			case <-run.done:
				if run.result != nil {
					result.BatchResults = append(result.BatchResults, run.result)
				}
				if run.err != nil && queueErr == nil {
					queueErr = run.err
				}
			case <-roundCtx.Done():
				queueErr = errs.New(errs.Internal, "round time budget exceeded", roundCtx.Err())
			}
		}
	}

	finalView := e.View()
	for _, state := range finalView.ObjectStates {
		switch state.Phase {
		case PhaseConverged:
			result.ConvergedObjects++
		case PhaseRetryExhausted, PhaseFailed, PhaseWriteTimeout:
			result.FailedObjects++
		}
	}
	result.FinishedAt = time.Now().UTC()
	result.DurationMS = result.FinishedAt.Sub(started).Milliseconds()
	if queueErr != nil {
		return result, queueErr
	}
	return result, nil
}

func (e *Engine) executeBatch(ctx context.Context, view *View, b *batch.Batch) (*BatchTriggerResult, error) {
	policy := view.Snapshot.Policy
	client := writeback.NewClient(e.endpointURL, time.Duration(policy.WriteTimeoutMS)*time.Millisecond)
	defer client.Close()

	out := &BatchTriggerResult{BatchID: b.BatchID, ObjectPhases: map[string]string{}}
	var firstErr error
	for _, objectID := range b.ObjectIDs {
		if err := ctx.Err(); err != nil {
			return out, errs.New(errs.Internal, "round context ended before batch object", err.Error())
		}
		object, ok := spec.ObjectByID(view.Snapshot.Objects)[objectID]
		if !ok {
			continue
		}
		state, err := e.convergeObject(ctx, view, b.BatchID, object, client)
		if state != nil {
			out.ObjectPhases[objectID] = state.Phase
		}
		if err != nil {
			code := errs.Code(err)
			out.Errors = append(out.Errors, BatchObjectError{ObjectID: objectID, Code: code, Message: err.Error()})
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	now := time.Now().UTC()
	finalStatus := "completed"
	if firstErr != nil {
		finalStatus = "failed"
	}
	batchState := &BatchState{
		BatchID:      b.BatchID,
		BatchNumber:  b.BatchNumber,
		RequestHash:  view.BatchStates[b.BatchID].RequestHash,
		Status:       finalStatus,
		TriggerCount: view.BatchStates[b.BatchID].TriggerCount,
		ObjectIDs:    append([]string(nil), b.ObjectIDs...),
		ObjectPhases: out.ObjectPhases,
		StartedAt:    view.BatchStates[b.BatchID].StartedAt,
		FinishedAt:   &now,
		UpdatedAt:    now,
	}
	if firstErr != nil {
		batchState.ErrorCode = errs.Code(firstErr)
	}
	_ = e.persistBatchState(context.Background(), view.Version, batchState)
	intentStatus := "completed"
	intentError := ""
	if firstErr != nil {
		intentStatus = batchState.ErrorCode
		intentError = firstErr.Error()
	}
	_ = e.store.SetIntentStatus(context.Background(), organizeBatchIntentID(view.Version, b.BatchID), intentStatus, intentError)
	out.Status = finalStatus
	return out, firstErr
}

func organizeBatchIntentID(version int64, batchID string) string {
	return fmt.Sprintf("organize-batch:v%d:%s", version, batchID)
}

func (e *Engine) convergeObject(ctx context.Context, view *View, batchID string, object spec.Object, client *writeback.Client) (*ObjectState, error) {
	policy := view.Snapshot.Policy
	totalAttempts := policy.MaxRetries + 1
	intentID := fmt.Sprintf("write-object:v%d:%s:%s", view.Version, batchID, object.ID)
	desiredDigest := digestFields(object.Desired)
	baseState := view.ObjectStates[object.ID]
	if baseState == nil {
		baseState = &ObjectState{ObjectID: object.ID, MaxRetries: policy.MaxRetries}
	}

	var lastDifferences []drift.Difference
	for attempt := 1; attempt <= totalAttempts; attempt++ {
		if err := e.store.UpsertIntent(context.Background(), store.Intent{
			ID: intentID, Type: "write_object", Version: view.Version, BatchID: batchID, ObjectID: object.ID,
			PayloadJSON: `{"desired_digest":` + jsonString(desiredDigest) + `,"attempt":` + strconv.Itoa(attempt) + `}`,
			Status:      "running",
		}); err != nil {
			return nil, errs.New(errs.Internal, "cannot persist write intent", err.Error())
		}

		now := time.Now().UTC()
		state := *baseState
		state.Attempts = attempt
		state.MaxRetries = policy.MaxRetries
		state.IntentID = intentID
		state.FieldDigest = desiredDigest
		state.UpdatedAt = now
		state.Phase = "writing"
		_ = e.saveObjectState(ctx, view.Version, &state, nil)

		writeErr := client.WriteFields(ctx, object.ID, object.Desired)
		if writeErr != nil {
			code := errs.Code(writeErr)
			state.Phase = PhaseFailed
			if code == errs.WriteTimeout {
				state.Phase = PhaseWriteTimeout
			}
			state.LastErrorCode = code
			state.LastMessage = writeErr.Error()
			state.UpdatedAt = time.Now().UTC()
			_ = e.store.InsertWriteAttempt(ctx, intentID, view.Version, batchID, object.ID, attempt, "write_error", code, "")
			_ = e.saveObjectState(context.Background(), view.Version, &state, nil)
			if attempt < totalAttempts {
				continue
			}
			_ = e.store.SetIntentStatus(context.Background(), intentID, code, writeErr.Error())
			return &state, writeErr
		}

		readback, readErr := client.ReadState(ctx, object.ID)
		if readErr != nil {
			code := errs.Code(readErr)
			state.Phase = PhaseFailed
			if code == errs.WriteTimeout {
				state.Phase = PhaseWriteTimeout
			}
			state.LastErrorCode = code
			state.LastMessage = readErr.Error()
			state.UpdatedAt = time.Now().UTC()
			_ = e.store.InsertWriteAttempt(ctx, intentID, view.Version, batchID, object.ID, attempt, "read_error", code, "")
			_ = e.saveObjectState(context.Background(), view.Version, &state, nil)
			if attempt < totalAttempts {
				continue
			}
			_ = e.store.SetIntentStatus(context.Background(), intentID, code, readErr.Error())
			return &state, readErr
		}

		readObject := object
		readObject.Actual = readback.Fields
		if readback.SourceMarkers != nil {
			readObject.SourceMarkers = readback.SourceMarkers
		} else {
			readObject.SourceMarkers = nil
		}
		report := drift.Evaluate(readObject, policy)
		readbackDigest := digestFields(readback.Fields)
		state.ReadbackDigest = readbackDigest
		state.UpdatedAt = time.Now().UTC()

		if report.InSync {
			state.Phase = PhaseConverged
			state.PrimaryCategory = spec.CategoryConverged
			state.HitCategories = nil
			state.Differences = nil
			state.LastErrorCode = ""
			state.LastMessage = ""
			_ = e.store.InsertWriteAttempt(ctx, intentID, view.Version, batchID, object.ID, attempt, "converged", "", "")
			_ = e.saveObjectState(context.Background(), view.Version, &state, nil)
			_ = e.store.SetIntentStatus(context.Background(), intentID, "committed", "")
			return &state, nil
		}

		lastDifferences = report.Differences
		state.Phase = PhaseDriftDetected
		state.PrimaryCategory = report.PrimaryCategory
		state.HitCategories = report.Categories
		state.Differences = report.Differences
		state.LastErrorCode = errs.RetryExhausted
		state.LastMessage = "readback fields do not match desired fields"
		detail, _ := json.Marshal(map[string]any{"differences": report.Differences})
		_ = e.store.InsertWriteAttempt(ctx, intentID, view.Version, batchID, object.ID, attempt, "readback_mismatch", errs.RetryExhausted, string(detail))
		_ = e.saveObjectState(context.Background(), view.Version, &state, nil)
		if attempt < totalAttempts {
			continue
		}
		state.Phase = PhaseRetryExhausted
		state.UpdatedAt = time.Now().UTC()
		_ = e.saveObjectState(context.Background(), view.Version, &state, nil)
		retryErr := errs.New(errs.RetryExhausted, "readback mismatch remained after retries", map[string]any{
			"object_id":   object.ID,
			"attempts":    attempt,
			"differences": report.Differences,
		})
		_ = e.store.SetIntentStatus(context.Background(), intentID, errs.RetryExhausted, retryErr.Error())
		return &state, retryErr
	}

	state := &ObjectState{ObjectID: object.ID, Phase: PhaseFailed, Differences: lastDifferences}
	return state, errs.New(errs.Internal, "convergence loop ended unexpectedly")
}

func (e *Engine) saveObjectState(ctx context.Context, version int64, state *ObjectState, batch *BatchState) error {
	e.progressMu.Lock()
	defer e.progressMu.Unlock()
	current := e.View()
	if current == nil || current.Version != version {
		raw, _ := json.Marshal(state)
		return e.store.PutObjectState(ctx, version, state.ObjectID, string(raw))
	}
	next := *current
	next.ObjectStates = cloneObjectStates(current.ObjectStates)
	next.ObjectStates[state.ObjectID] = state
	var batchRaw *string
	if batch != nil {
		next.BatchStates = cloneBatchStates(current.BatchStates)
		next.BatchStates[batch.BatchID] = batch
		raw, _ := json.Marshal(batch)
		batchRaw = new(string)
		*batchRaw = string(raw)
	}
	next.Summary = buildSummary(&next)
	summaryRaw, _ := json.Marshal(next.Summary)
	objectRaw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	objectJSON := string(objectRaw)
	if err := e.store.SaveWriteProgress(ctx, version, &objectJSON, batchRaw, string(summaryRaw)); err != nil {
		return err
	}
	e.view.Store(&next)
	return nil
}

func (e *Engine) persistBatchState(ctx context.Context, version int64, state *BatchState) error {
	e.progressMu.Lock()
	defer e.progressMu.Unlock()
	current := e.View()
	if current == nil || current.Version != version {
		raw, _ := json.Marshal(state)
		return e.store.PutBatchState(ctx, version, state.BatchID, string(raw))
	}
	next := *current
	next.BatchStates = cloneBatchStates(current.BatchStates)
	next.BatchStates[state.BatchID] = state
	next.Summary = buildSummary(&next)
	summaryRaw, _ := json.Marshal(next.Summary)
	batchRawBytes, _ := json.Marshal(state)
	batchRaw := string(batchRawBytes)
	if err := e.store.SaveWriteProgress(ctx, version, nil, &batchRaw, string(summaryRaw)); err != nil {
		return err
	}
	e.view.Store(&next)
	return nil
}

func findBatch(plan *batch.Plan, id string) *batch.Batch {
	if plan == nil {
		return nil
	}
	for _, b := range plan.Batches {
		if b.BatchID == id {
			return b
		}
	}
	return nil
}

func cloneObjectStates(in map[string]*ObjectState) map[string]*ObjectState {
	out := make(map[string]*ObjectState, len(in))
	for k, v := range in {
		copyValue := *v
		out[k] = &copyValue
	}
	return out
}

func cloneBatchStates(in map[string]*BatchState) map[string]*BatchState {
	out := make(map[string]*BatchState, len(in))
	for k, v := range in {
		copyValue := *v
		out[k] = &copyValue
	}
	return out
}

var _ = errors.Is
