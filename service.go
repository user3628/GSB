package feedbackstream

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

type keyedLocks struct {
	mu    sync.Mutex
	locks map[int]*sync.Mutex
}

func (k *keyedLocks) get(partition int) *sync.Mutex {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.locks == nil {
		k.locks = map[int]*sync.Mutex{}
	}
	if m, ok := k.locks[partition]; ok {
		return m
	}
	m := &sync.Mutex{}
	k.locks[partition] = m
	return m
}

func (k *keyedLocks) Lock(partition int) func() {
	m := k.get(partition)
	m.Lock()
	return m.Unlock
}

type batchCall struct {
	done   chan struct{}
	hash   string
	result *BatchResult
	err    error
}

type Service struct {
	store      *Store
	partitions keyedLocks
	policyMu   sync.RWMutex
	roundMu    sync.Mutex
	calls      sync.Map
}

func NewService(store *Store) *Service {
	return &Service{store: store}
}

func (s *Service) Store() *Store { return s.store }

func (s *Service) InstallInitialPolicy(ctx context.Context, policy *PolicySnapshot) error {
	return s.store.SaveInitialPolicy(ctx, policy)
}

func (s *Service) ActivePlan(ctx context.Context) (*PolicySnapshot, *FusionPlan, error) {
	s.policyMu.RLock()
	defer s.policyMu.RUnlock()
	record, err := s.store.ActivePolicy(ctx)
	if err != nil {
		return nil, nil, err
	}
	if record == nil {
		return nil, nil, NewError(CodeNotFound, "no active policy snapshot")
	}
	plan, err := PlanFusion(record.Snapshot)
	if err != nil {
		return nil, nil, err
	}
	return record.Snapshot, plan, nil
}

type SubmitBatchRequest struct {
	BatchID string  `json:"batch_id"`
	Events  []Event `json:"events"`
}

func conflictRequestError(batchID, expected, actual string) *Error {
	return NewError(CodeConflictRequest, "same batch id was submitted with different content").
		With("batch_id", batchID).
		With("existing_payload_hash", expected).
		With("conflicting_payload_hash", actual)
}

func outcomeFromRecord(rec *BatchRequestRecord) (*BatchResult, error, bool) {
	if rec == nil {
		return nil, nil, false
	}
	switch rec.Status {
	case StatusCompleted, StatusLateRecorded:
		if rec.ResultJSON == "" {
			return &BatchResult{BatchID: rec.BatchID, PartitionID: rec.PartitionID, Status: rec.Status}, nil, true
		}
		var result BatchResult
		if err := json.Unmarshal([]byte(rec.ResultJSON), &result); err != nil {
			return nil, err, true
		}
		result.CompletedOnce = true
		return &result, nil, true
	case StatusFailed:
		if rec.ErrorJSON != "" {
			var coded Error
			if err := json.Unmarshal([]byte(rec.ErrorJSON), &coded); err == nil && coded.Code != "" {
				return nil, &coded, true
			}
		}
		return nil, NewError(rec.ErrorCode, "batch previously failed"), true
	default:
		return nil, nil, false
	}
}

func (s *Service) SubmitBatch(ctx context.Context, request SubmitBatchRequest) (*BatchResult, error) {
	if request.BatchID == "" {
		return nil, SpecInvalid("batch_id is required")
	}
	batch, err := NewBatch(request.BatchID, request.Events)
	if err != nil {
		return nil, err
	}

	existing, err := s.store.GetBatchRequest(ctx, request.BatchID)
	if err != nil {
		return nil, err
	}
	if existing != nil && existing.PayloadHash != batch.PayloadHash {
		return nil, conflictRequestError(request.BatchID, existing.PayloadHash, batch.PayloadHash)
	}
	if result, err, terminal := outcomeFromRecord(existing); terminal {
		return result, err
	}

	callValue, loaded := s.calls.LoadOrStore(request.BatchID, &batchCall{done: make(chan struct{}), hash: batch.PayloadHash})
	call := callValue.(*batchCall)
	if loaded {
		if call.hash != batch.PayloadHash {
			return nil, conflictRequestError(request.BatchID, call.hash, batch.PayloadHash)
		}
		select {
		case <-call.done:
			return call.result, call.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	call.result, call.err = s.executeSubmitOwner(ctx, batch)
	close(call.done)
	s.calls.Delete(request.BatchID)
	return call.result, call.err
}

func (s *Service) executeSubmitOwner(ctx context.Context, batch *Batch) (*BatchResult, error) {
	rec := BatchRequestRecord{
		BatchID: batch.ID, PayloadHash: batch.PayloadHash,
		PartitionID: batch.PartitionID, Status: "RECEIVED",
	}
	created, err := s.store.InsertBatchRequestIfAbsent(ctx, rec)
	if err != nil {
		return nil, err
	}
	existing, err := s.store.GetBatchRequest(ctx, batch.ID)
	if err != nil {
		return nil, err
	}
	if !created && existing != nil && existing.PayloadHash != batch.PayloadHash {
		return nil, conflictRequestError(batch.ID, existing.PayloadHash, batch.PayloadHash)
	}
	if result, err, terminal := outcomeFromRecord(existing); terminal {
		return result, err
	}
	if err := s.store.SaveBatch(ctx, batch); err != nil {
		return nil, err
	}

	result, runErr := s.startAndAdvance(ctx, batch.ID)
	status := StatusCompleted
	resultJSON := ""
	errorCode := ""
	errorPayload := ""
	if runErr != nil {
		status = StatusFailed
		if coded, ok := AsError(runErr); ok {
			errorCode = coded.Code
		} else {
			errorCode = "E_INTERNAL"
		}
		errorPayload = errorJSON(runErr)
		_ = s.store.UpdateBatchStatus(context.Background(), batch.ID, StatusFailed, "", 0)
	} else if result != nil {
		status = result.Status
		if data, err := json.Marshal(result); err == nil {
			resultJSON = string(data)
		}
	}
	_ = s.store.FinishBatchRequest(context.Background(), BatchRequestRecord{
		BatchID: batch.ID, Status: status, ResultJSON: resultJSON,
		ErrorCode: errorCode, ErrorJSON: errorPayload,
	})
	return result, runErr
}

func (s *Service) beginBatchIntent(ctx context.Context, batchID string) (string, error) {
	intentID := "intent-start-" + batchID
	if _, err := s.store.CreateIntent(ctx, IntentRecord{
		ID: intentID, Kind: IntentStartBatch, AggregateID: batchID,
		PayloadJSON: fmt.Sprintf(`{"batch_id":%q}`, batchID),
	}); err != nil {
		return "", err
	}
	if err := s.store.MarkBatchRunning(ctx, batchID); err != nil {
		return "", err
	}
	return intentID, nil
}

func (s *Service) finishBatchIntent(batchID string, runErr error) {
	intentStatus := IntentCommitted
	if runErr != nil {
		intentStatus = IntentAborted
	}
	_, _ = s.store.DB().ExecContext(context.Background(),
		`UPDATE intents SET status=?, updated_at_ms=? WHERE intent_id=? AND status=?`,
		intentStatus, nowMS(), "intent-start-"+batchID, IntentPending)
}

func (s *Service) startAndAdvance(ctx context.Context, batchID string) (*BatchResult, error) {
	if _, err := s.beginBatchIntent(ctx, batchID); err != nil {
		return nil, err
	}

	s.policyMu.RLock()
	policyRecord, err := s.store.ActivePolicy(ctx)
	if err != nil {
		s.policyMu.RUnlock()
		return nil, err
	}
	if policyRecord == nil {
		s.policyMu.RUnlock()
		return nil, NewError(CodeNotFound, "no active policy")
	}
	plan, err := PlanFusion(policyRecord.Snapshot)
	if err != nil {
		s.policyMu.RUnlock()
		return nil, err
	}
	unlockPartition := s.partitions.Lock(partitionForBatch(ctx, s.store, batchID))
	result, err := s.advance(ctx, batchID, policyRecord.Snapshot, plan)
	unlockPartition()
	s.policyMu.RUnlock()
	s.finishBatchIntent(batchID, err)
	return result, err
}

func partitionForBatch(ctx context.Context, store *Store, batchID string) int {
	batch, err := store.GetBatch(ctx, batchID)
	if err != nil || batch == nil {
		return -1
	}
	return batch.PartitionID
}

func (s *Service) AdvanceBatch(ctx context.Context, batchID string) (*BatchResult, error) {
	batch, err := s.store.GetBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	if batch == nil {
		return nil, NewError(CodeNotFound, "batch not found").With("batch_id", batchID)
	}
	s.policyMu.RLock()
	defer s.policyMu.RUnlock()
	record, err := s.store.ActivePolicy(ctx)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, NewError(CodeNotFound, "no active policy")
	}
	plan, err := PlanFusion(record.Snapshot)
	if err != nil {
		return nil, err
	}
	unlock := s.partitions.Lock(batch.PartitionID)
	defer unlock()
	result, err := s.advance(ctx, batchID, record.Snapshot, plan)
	if err == nil && result != nil {
		if data, mErr := json.Marshal(result); mErr == nil {
			_ = s.store.FinishBatchRequest(ctx, BatchRequestRecord{
				BatchID: batchID, Status: result.Status, ResultJSON: string(data),
			})
		}
	}
	return result, err
}

func reportFromPush(push *PushBufferResult, groupID string) *BackpressureReport {
	if push == nil {
		return nil
	}
	return &BackpressureReport{
		Watermark:              push.Watermark,
		TriggerGroupID:         groupID,
		BlockedUpstreamBatches: push.BlockedBatchCount,
		WaitingQueueFull:       push.WaitingQueueCount >= push.Watermark.MaxBatches && false,
	}
}
