package arbiter

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"leasearbiter/internal/store"
)

// IdempotentService wraps Engine operations with durable request_id
// deduplication. The first call executes; concurrent duplicates block on an
// in-process condition and then reuse the stored response. A reused id with a
// different payload fails with E_CONFLICTING_REQUEST.
type IdempotentService struct {
	eng *Engine
	pmu sync.Map // request_id -> *requestGate
}

type requestGate struct {
	leaderMu chan struct{} // capacity 1; holder is the single executor
	done     chan struct{}
}

// NewIdempotentService constructs the wrapper.
func NewIdempotentService(eng *Engine) *IdempotentService {
	return &IdempotentService{eng: eng}
}

// Engine exposes the underlying engine for non-idempotent reads.
func (s *IdempotentService) Engine() *Engine { return s.eng }

type idemRun func() (any, error)

// run executes fn exactly once per (kind, request_id). Leader election is a
// single durable SQL statement (ClaimRunningLeader), so the guarantee holds
// even when separate OS processes race on the same SQLite file. In-process
// duplicates additionally park on a channel and reuse the stored response.
func (s *IdempotentService) run(ctx context.Context, kind, requestID, requestKey string,
	payload any, fn idemRun) (any, error) {
	if requestID == "" {
		return fn()
	}
	payloadHash := hashPayload(payload)

	// Fast path: an already completed call is reused without any write.
	if rec, exists, err := s.eng.st.LookupRequest(requestID); err != nil {
		return nil, err
	} else if exists {
		if rec.Kind != kind || rec.RequestKey != requestKey || rec.PayloadHash != payloadHash {
			return nil, conflictingRequest(requestID, rec, kind, requestKey)
		}
		if rec.Status == store.RequestCompleted {
			return decodeStoredResponse(rec.ResponseJSON)
		}
	}

	gateActual, _ := s.pmu.LoadOrStore(requestID, &requestGate{
		leaderMu: make(chan struct{}, 1),
		done:     make(chan struct{}),
	})
	gate := gateActual.(*requestGate)

	won, conflict, err := s.eng.st.ClaimRunningLeader(store.RequestRecord{
		RequestID:   requestID,
		Kind:        kind,
		RequestKey:  requestKey,
		PayloadHash: payloadHash,
		Status:      store.RequestRunning,
		CreatedAtMs: store.Milli(s.eng.now()),
	})
	if err != nil {
		return nil, err
	}
	if conflict {
		return nil, apiError(CodeConflictingRequest,
			"request_id was already used with a different payload", map[string]any{"request_id": requestID})
	}
	if !won {
		// Another leader (this process or another process) is executing.
		return s.awaitDuplicate(ctx, requestID, fn)
	}

	// This goroutine owns the in-process gate for the request's lifetime.
	gate.leaderMu <- struct{}{}
	out, callErr := fn()
	if callErr == nil {
		if resp, merr := json.Marshal(out); merr == nil {
			_ = s.eng.st.CompleteRequest(requestID, string(resp), store.Milli(s.eng.now()))
		}
	} else {
		// Failed validation must not poison the id: mark aborted so a corrected
		// retry with the same id can win leadership again.
		_, _ = s.eng.st.DB.Exec(`UPDATE inflight_requests SET status=?, completed_at=? WHERE request_id=?`,
			store.RequestAborted, store.Milli(s.eng.now()), requestID)
	}
	close(gate.done)
	<-gate.leaderMu
	s.pmu.Delete(requestID)
	return out, callErr
}

func conflictingRequest(requestID string, rec *store.RequestRecord, kind, key string) *APIError {
	return apiError(CodeConflictingRequest,
		"request_id was already used with a different payload", map[string]any{
			"request_id":    requestID,
			"stored_kind":   rec.Kind,
			"stored_key":    rec.RequestKey,
			"incoming_kind": kind,
			"incoming_key":  key,
		})
}

// awaitDuplicate parks a concurrent duplicate until the leader finishes, then
// reuses the durable response. It uses a channel wait, never a polling loop.
func (s *IdempotentService) awaitDuplicate(ctx context.Context, requestID string, fn idemRun) (any, error) {
	if g, ok := s.pmu.Load(requestID); ok {
		gate := g.(*requestGate)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-gate.done:
		}
	}
	rec, exists, err := s.eng.st.LookupRequest(requestID)
	if err != nil {
		return nil, err
	}
	if !exists || rec.Status != store.RequestCompleted {
		// Leader died before completion (or failed validation): the durable
		// row is aborted/running-aborted; retry the operation once.
		if _, aerr := s.eng.st.AbortRunningRequests(store.Milli(s.eng.now())); aerr != nil {
			return nil, aerr
		}
		return fn()
	}
	return decodeStoredResponse(rec.ResponseJSON)
}

func decodeStoredResponse(raw string) (any, error) {
	var generic any
	if err := json.Unmarshal([]byte(raw), &generic); err != nil {
		return nil, err
	}
	return generic, nil
}

// Idempotent operation wrappers. Each keeps the request key at row granularity
// so the same id cannot be silently reused against another row.

func (s *IdempotentService) Acquire(ctx context.Context, req AcquireRequest) (*AcquireResponse, error) {
	out, err := s.run(ctx, "acquire", req.RequestID, "acquire:"+req.RowID, req,
		func() (any, error) { return s.eng.Acquire(ctx, req) })
	if err != nil {
		return nil, err
	}
	return castResponse[AcquireResponse](out)
}

func (s *IdempotentService) Renew(ctx context.Context, req RenewRequest) (*AcquireResponse, error) {
	out, err := s.run(ctx, "renew", req.RequestID, "renew:"+req.RowID, req,
		func() (any, error) { return s.eng.Renew(ctx, req) })
	if err != nil {
		return nil, err
	}
	return castResponse[AcquireResponse](out)
}

func (s *IdempotentService) Release(ctx context.Context, req ReleaseRequest) error {
	_, err := s.run(ctx, "release", req.RequestID, "release:"+req.RowID, req,
		func() (any, error) { return nil, s.eng.Release(ctx, req) })
	return err
}

func (s *IdempotentService) Commit(ctx context.Context, req CommitRequest) (*CommitResponse, error) {
	out, err := s.run(ctx, "commit", req.RequestID, "commit:"+req.RowID, req,
		func() (any, error) { return s.eng.Commit(ctx, req) })
	if err != nil {
		return nil, err
	}
	return castResponse[CommitResponse](out)
}

func (s *IdempotentService) Recycle(ctx context.Context, req RecycleRequest) (*RecycleResult, error) {
	out, err := s.run(ctx, "recycle", req.RequestID, "recycle:"+req.RowID, req,
		func() (any, error) { return s.eng.Recycle(ctx, req) })
	if err != nil {
		return nil, err
	}
	return castResponse[RecycleResult](out)
}

func castResponse[T any](v any) (*T, error) {
	switch t := v.(type) {
	case *T:
		return t, nil
	case T:
		return &t, nil
	case map[string]any:
		b, err := json.Marshal(t)
		if err != nil {
			return nil, err
		}
		var out T
		if err := json.Unmarshal(b, &out); err != nil {
			return nil, err
		}
		return &out, nil
	default:
		return nil, apiError(CodeBadRequest, "unexpected stored response type", map[string]any{"type": typeName(v)})
	}
}

func typeName(v any) string {
	if v == nil {
		return "nil"
	}
	return fmt.Sprintf("%T", v)
}
