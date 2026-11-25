package convergence

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

type RoundResult struct {
	Status       string                       `json:"status"`
	BatchResults map[string]*BatchApplyResult `json:"batch_results"`
	AppliedOrder []string                     `json:"applied_order"`
	Errors       []*Error                     `json:"errors,omitempty"`
}

func (s *Service) ApplyBatch(ctx context.Context, request ApplyBatchRequest) (*BatchApplyResult, error) {
	view := s.View()
	if view == nil {
		return nil, NewError(CodeNotFound, "no committed snapshot", nil)
	}
	batch := findBatch(view.Plan, request.BatchID)
	if batch == nil {
		return nil, NewError(CodeNotFound, "batch not found in current committed plan", map[string]any{"batch_id": request.BatchID, "version": view.Snapshot.SnapshotVersion})
	}
	requestHash := request.RequestHash
	if requestHash == "" {
		requestHash = batch.ContentHash
	}
	if request.Content != nil {
		contentHash, hashErr := ContentHash(request.Content)
		if hashErr != nil {
			return nil, WrapError(CodeSpecInvalid, "cannot hash batch request content", hashErr, map[string]any{"batch_id": request.BatchID})
		}
		if contentHash != batch.ContentHash {
			return nil, NewError(CodeConflictingRequest, "batch request content does not match committed plan", map[string]any{
				"batch_id":       request.BatchID,
				"planned_hash":   batch.ContentHash,
				"submitted_hash": contentHash,
			})
		}
	}
	if request.ContentHash != "" && request.ContentHash != batch.ContentHash {
		return nil, NewError(CodeConflictingRequest, "batch content hash does not match committed plan", map[string]any{
			"batch_id":       request.BatchID,
			"planned_hash":   batch.ContentHash,
			"submitted_hash": request.ContentHash,
		})
	}

	version := view.Snapshot.SnapshotVersion
	callKey := batchScope(version, request.BatchID)
	prior, err := s.store.FindBatchRun(ctx, version, request.BatchID)
	if err != nil {
		return nil, err
	}

	s.callsMu.Lock()
	if call, ok := s.calls[callKey]; ok {
		if call.requestHash != requestHash {
			s.callsMu.Unlock()
			return nil, NewError(CodeConflictingRequest, "same batch id was submitted with different content", map[string]any{
				"batch_id":         request.BatchID,
				"existing_hash":    call.requestHash,
				"conflicting_hash": requestHash,
			})
		}
		s.callsMu.Unlock()
		select {
		case <-call.done:
			return call.result, call.err
		case <-ctx.Done():
			return nil, WrapError(CodeRoundBudget, "context ended while waiting for idempotent batch execution", ctx.Err(), map[string]any{"batch_id": request.BatchID})
		}
	}
	if prior != nil && prior.RequestHash != "" && prior.RequestHash != requestHash {
		s.callsMu.Unlock()
		return nil, NewError(CodeConflictingRequest, "persisted batch id has different content hash", map[string]any{
			"batch_id":       request.BatchID,
			"existing_hash":  prior.RequestHash,
			"submitted_hash": requestHash,
		})
	}
	call := &batchCall{requestHash: requestHash, done: make(chan struct{})}
	if prior != nil && (prior.Status == "succeeded" || prior.Status == "failed") && prior.ResultJSON != "" {
		var result BatchApplyResult
		if jsonErr := jsonUnmarshalString(prior.ResultJSON, &result); jsonErr == nil {
			var callErr error
			if result.Error != nil {
				callErr = result.Error
			}
			call.result = &result
			call.err = callErr
			close(call.done)
			s.calls[callKey] = call
			s.callsMu.Unlock()
			return &result, errorOrNil(result.Error)
		}
	}
	s.calls[callKey] = call
	s.callsMu.Unlock()

	intent, err := s.store.CreateIntent(ctx, "batch_apply", callKey, map[string]any{
		"batch_id": request.BatchID, "version": version, "request_hash": requestHash,
	})
	if err != nil {
		s.finishCall(callKey, call, nil, err)
		return nil, err
	}

	s.mutateBatchState(version, batch.ContentHash, request.BatchID, func(state *BatchState) {
		state.Status = "applying"
		state.RequestHash = requestHash
		state.Error = nil
	})
	if err := s.store.PrepareBatchRun(ctx, view.Snapshot.SnapshotVersion, &BatchState{
		Batch:       *batch,
		ContentHash: batch.ContentHash,
		RequestHash: requestHash,
	}, batch.ObjectIDs); err != nil {
		persistError := AsError(err)
		if persistError == nil {
			persistError = NewError(CodePersistence, err.Error(), nil)
		}
		s.mutateBatchState(version, batch.ContentHash, request.BatchID, func(state *BatchState) {
			state.Status = "failed"
			state.Error = persistError
		})
		s.finishCall(callKey, call, nil, err)
		return nil, err
	}
	if s.opts.AfterWriteIntent != nil {
		s.opts.AfterWriteIntent(request.BatchID, intent.ID)
	}

	result := s.executeBatch(ctx, view, batch, requestHash)
	persistCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := s.store.CompleteBatchRun(persistCtx, intent.ID, result); err != nil {
		cancel()
		persistenceErr := err
		if result.Error == nil {
			result.Error = AsError(persistenceErr)
			if result.Error == nil {
				result.Error = NewError(CodePersistence, persistenceErr.Error(), nil)
			}
			result.Status = "failed"
		}
	}
	cancel()

	s.mutateBatchState(version, batch.ContentHash, request.BatchID, func(state *BatchState) {
		state.Status = result.Status
		state.Attempts = result.Attempts
		state.ObjectResults = result.ObjectResults
		state.Error = result.Error
		state.UpdatedAt = result.AppliedAt
	})
	applyErr := errorOrNil(result.Error)
	s.finishCall(callKey, call, result, applyErr)
	return result, applyErr
}

func errorOrNil(businessError *Error) error {
	if businessError == nil {
		return nil
	}
	return businessError
}

func (s *Service) finishCall(callKey string, expected *batchCall, result *BatchApplyResult, err error) {
	s.callsMu.Lock()
	call := s.calls[callKey]
	if call != expected {
		s.calls[callKey] = expected
		call = expected
	}
	call.result = result
	call.err = err
	select {
	case <-call.done:
	default:
		close(call.done)
	}
	s.callsMu.Unlock()
}

func (s *Service) mutateBatchState(version int64, contentHash, batchID string, mutate func(*BatchState)) bool {
	s.writerMu.Lock()
	defer s.writerMu.Unlock()
	old := s.state.Load()
	if old == nil || old.Snapshot == nil || old.BatchStates == nil {
		return false
	}
	if old.Snapshot.SnapshotVersion != version {
		return false
	}
	next := *old
	next.BatchStates = make(map[string]*BatchState, len(old.BatchStates))
	for id, state := range old.BatchStates {
		copied := *state
		if state.ObjectResults != nil {
			copied.ObjectResults = append([]ObjectApplyResult(nil), state.ObjectResults...)
		}
		next.BatchStates[id] = &copied
	}
	state := next.BatchStates[batchID]
	if state == nil || state.ContentHash != contentHash {
		return false
	}
	if mutate != nil {
		mutate(state)
	}
	next.Summary = computeSummary(next.Snapshot, next.Plan, next.BatchStates)
	s.state.Store(&next)
	return true
}

func (s *Service) executeBatch(ctx context.Context, view *CommittedVersion, batch *Batch, requestHash string) *BatchApplyResult {
	policy := view.Snapshot.Policy
	result := &BatchApplyResult{
		BatchID:       batch.BatchID,
		Version:       view.Snapshot.SnapshotVersion,
		ContentHash:   batch.ContentHash,
		RequestHash:   requestHash,
		Status:        "applying",
		ObjectResults: []ObjectApplyResult{},
	}
	objectByID := map[string]Object{}
	for _, object := range view.Snapshot.Objects {
		objectByID[object.ID] = object
	}

	allVerified := true
	for index, objectID := range batch.ObjectIDs {
		object, ok := objectByID[objectID]
		if !ok {
			allVerified = false
			objectResult := ObjectApplyResult{ObjectID: objectID, Status: "failed", Error: NewError(CodeNotFound, "batch object missing from snapshot", nil)}
			result.ObjectResults = append(result.ObjectResults, objectResult)
			if result.Error == nil {
				result.Error = objectResult.Error
			}
			continue
		}
		endpoint := s.resolveEndpoint(object, view.Snapshot)
		objectResult := s.applyObjectWithRetry(ctx, endpoint, object, policy, index)
		result.ObjectResults = append(result.ObjectResults, objectResult)
		result.Attempts += objectResult.Attempts
		if objectResult.Error != nil {
			allVerified = false
			if result.Error == nil {
				result.Error = objectResult.Error
			}
		}
	}
	if allVerified {
		result.Status = "succeeded"
	} else {
		result.Status = "failed"
	}
	result.AppliedAt = s.opts.Now().UTC()
	return result
}

func (s *Service) applyObjectWithRetry(ctx context.Context, endpoint string, object Object, policy Policy, requestIndex int) ObjectApplyResult {
	maxAttempts := policy.RetryLimit + 1
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	objectResult := ObjectApplyResult{ObjectID: object.ID, ClusterID: object.ClusterID, Status: "failed"}
	if endpoint == "" {
		businessError := NewError(CodeSpecInvalid, "no loopback endpoint available for object", map[string]any{"object_id": object.ID, "cluster_id": object.ClusterID})
		objectResult.Error = businessError
		return objectResult
	}

	var lastError *Error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		objectResult.Attempts = attempt
		requestID := fmt.Sprintf("%s-%d-%d", object.ID, requestIndex, attempt)
		readback, err := s.client.WriteAndReadback(ctx, endpoint, object, requestID, policy.WriteTimeout.Std())
		if err == nil {
			readObject := object
			readObject.Actual = readback.Fields
			drift, driftErr := DriftObject(readObject, policy)
			if driftErr != nil {
				objectResult.Error = AsError(driftErr)
				if objectResult.Error == nil {
					objectResult.Error = NewError(CodeSpecInvalid, driftErr.Error(), nil)
				}
				return objectResult
			}
			objectResult.Status = "verified"
			objectResult.HTTPStatus = readback.HTTPStatus
			objectResult.ReadbackFields = readback.Fields
			objectResult.ReadbackDrift = drift
			return objectResult
		}
		businessError := AsError(err)
		if businessError == nil {
			businessError = WrapError(CodeWriteFailed, "write attempt failed", err, map[string]any{"object_id": object.ID})
		}
		lastError = businessError
		if businessError.Code == CodeApplyMismatch || businessError.Code == CodeSpecInvalid || businessError.Code == CodeNotFound {
			break
		}
		if ctx.Err() != nil {
			if businessError.Code != CodeRoundBudget {
				lastError = WrapError(CodeRoundBudget, "round context ended", ctx.Err(), map[string]any{"object_id": object.ID})
			}
			break
		}
	}
	objectResult.Error = lastError
	return objectResult
}

func (s *Service) resolveEndpoint(object Object, snapshot *Snapshot) string {
	s.endpointMu.RLock()
	override := s.endpointOverride
	s.endpointMu.RUnlock()
	if override != "" {
		return override
	}
	if object.Endpoint != "" {
		return object.Endpoint
	}
	if snapshot != nil {
		for _, cluster := range snapshot.Clusters {
			if cluster.ID == object.ClusterID {
				return cluster.Endpoint
			}
		}
	}
	return ""
}

func (s *Service) ApplyRound(ctx context.Context) (*RoundResult, error) {
	view := s.View()
	if view == nil {
		return nil, NewError(CodeNotFound, "no committed snapshot", nil)
	}
	policy := view.Snapshot.Policy
	roundCtx, cancel := context.WithTimeout(ctx, policy.RoundBudget.Std())
	defer cancel()

	scheduler := NewScheduler(policy.MaxParallelBatches, policy.WriteQueueSize)
	defer scheduler.Close()

	roundResult := &RoundResult{Status: "running", BatchResults: map[string]*BatchApplyResult{}, AppliedOrder: []string{}}
	for _, layer := range view.Plan.Layers {
		jobs := make(map[string]*scheduledJob)
		var queueError *Error
		for _, batchID := range layer.BatchIDs {
			id := batchID
			job, err := scheduler.Submit(roundCtx, id, func(executionContext context.Context) (*BatchApplyResult, error) {
				return s.ApplyBatch(executionContext, ApplyBatchRequest{BatchID: id})
			})
			if err != nil {
				if businessError := AsError(err); businessError != nil && businessError.Code == CodeWriteQueueFull && queueError == nil {
					queueError = businessError
				}
				roundResult.Errors = append(roundResult.Errors, AsError(err))
				continue
			}
			jobs[id] = job
		}

		ids := make([]string, 0, len(jobs))
		for id := range jobs {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		layerFailed := false
		for _, id := range ids {
			result, err := scheduler.Wait(roundCtx, jobs[id])
			if err != nil {
				// A round-budget timeout cancels in-flight work. Wait for the
				// terminal result before returning so persistence and atomic
				// view updates cannot race with Store.Close.
				result, err = scheduler.Wait(context.Background(), jobs[id])
			}
			if result != nil {
				roundResult.BatchResults[id] = result
				roundResult.AppliedOrder = append(roundResult.AppliedOrder, id)
				if result.Error != nil {
					roundResult.Errors = append(roundResult.Errors, result.Error)
					layerFailed = true
				}
			}
			if err != nil {
				if businessError := AsError(err); businessError != nil {
					roundResult.Errors = append(roundResult.Errors, businessError)
				}
				layerFailed = true
			}
		}
		if queueError != nil {
			roundResult.Status = "queue_full"
			return roundResult, queueError
		}
		if layerFailed {
			roundResult.Status = "failed"
			if len(roundResult.Errors) > 0 {
				return roundResult, roundResult.Errors[0]
			}
			return roundResult, NewError(CodeWriteFailed, "batch layer failed", map[string]any{"layer": layer.Index})
		}
	}
	roundResult.Status = "succeeded"
	return roundResult, nil
}

func jsonUnmarshalString(data string, target any) error {
	return json.Unmarshal([]byte(data), target)
}
