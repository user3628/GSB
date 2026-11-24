package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"time"

	"local/sqlitepool/internal/domain"
	"local/sqlitepool/internal/pool"
	"local/sqlitepool/internal/store"
)

type injectionRequest struct {
	RequestID string         `json:"request_id"`
	Type      string         `json:"type"`
	Params    map[string]any `json:"params"`
}

type injectionOutcome struct {
	code        string
	message     string
	observation map[string]any
	err         error
}

func (s *Server) handleCreateInjection(w http.ResponseWriter, r *http.Request) {
	s.mutation(w, r, "POST /v1/injections", func(ctx context.Context, requestID string, body []byte) apiResult {
		var req injectionRequest
		if err := decodeInto(body, &req); err != nil {
			return resultFromError(domain.WrapError(domain.CodeBadRequest, "invalid injection request", nil, err), requestID)
		}
		if req.Type == "" {
			return resultFromError(domain.NewError(domain.CodeBadRequest, "injection type is required", nil), requestID)
		}
		if req.Params == nil {
			req.Params = map[string]any{}
		}
		s.injMu.Lock()
		defer s.injMu.Unlock()
		rec, err := store.NewInjection(ctx, s.store, req.Type, req.Params)
		if err != nil {
			return resultFromError(domain.WrapError(domain.CodeInternal, "failed to record injection", nil, err), requestID)
		}
		out := s.runInjection(ctx, rec.ID, req.Type, req.Params)
		rec.BusinessCode = out.code
		rec.Observation = out.observation
		rec.Status = "completed"
		if out.err != nil {
			rec.Status = "failed"
			rec.Observation["error"] = out.err.Error()
		}
		if err := s.store.CompleteInjection(ctx, rec); err != nil {
			return resultFromError(domain.WrapError(domain.CodeInternal, "failed to persist injection observation",
				map[string]any{"injection_id": rec.ID}, err), requestID)
		}
		contextMap := map[string]any{
			"injection_id": rec.ID,
			"type":         rec.Type,
			"params":       rec.Params,
			"observation":  rec.Observation,
		}
		if out.err != nil {
			return faultResult(domain.CodeInternal, out.message, contextMap, requestID)
		}
		return faultResult(out.code, out.message, contextMap, requestID)
	})
}

func (s *Server) handleGetInjection(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rec, err := s.store.GetInjection(context.Background(), id)
	if err != nil {
		writeResult(w, resultFromError(domain.WrapError(domain.CodeNotFound, "injection record not found",
			map[string]any{"injection_id": id}, err), ""))
		return
	}
	writeResult(w, successResult(http.StatusOK, "", map[string]any{"injection": rec}))
}

func (s *Server) handleListInjections(w http.ResponseWriter, r *http.Request) {
	limit, _ := parseOptionalInt(r.URL.Query().Get("limit"))
	records, err := s.store.ListInjections(context.Background(), limit)
	if err != nil {
		writeResult(w, resultFromError(domain.WrapError(domain.CodeInternal, "failed to list injections", nil, err), ""))
		return
	}
	writeResult(w, successResult(http.StatusOK, "", map[string]any{"injections": records}))
}

func (s *Server) runInjection(ctx context.Context, injectionID, injectionType string, params map[string]any) injectionOutcome {
	switch injectionType {
	case "dial_failed":
		return s.injectDialFailed(ctx)
	case "query_timeout":
		return s.injectQueryTimeout(ctx, params)
	case "conn_reset":
		return s.injectConnReset(ctx)
	case "wait_queue_overflow":
		return s.injectWaitQueueOverflow(ctx, params)
	case "acquire_timeout":
		return s.injectAcquireTimeout(ctx, params)
	default:
		return injectionOutcome{
			code:        domain.CodeBadRequest,
			message:     "unsupported injection type",
			observation: map[string]any{"supported_types": injectionTypes(), "received": injectionType},
			err:         fmt.Errorf("unsupported injection type %q", injectionType),
		}
	}
}

func injectionTypes() []string {
	return []string{"dial_failed", "query_timeout", "conn_reset", "wait_queue_overflow", "acquire_timeout"}
}

func (s *Server) injectDialFailed(ctx context.Context) injectionOutcome {
	before := s.snapshotStats()
	err := s.pool.ProbeDial(ctx, true, "http_injection_dial_failed")
	code := codeFromError(err, domain.CodeDialFailed)
	after := s.snapshotStats()
	return injectionOutcome{
		code:    code,
		message: "connection establishment failed under deterministic fault injection",
		observation: map[string]any{
			"attempted":            err != nil,
			"simulated":            true,
			"pool_state_unchanged": statsEqual(before, after),
			"before":               before,
			"after":                after,
		},
	}
}

func (s *Server) injectQueryTimeout(ctx context.Context, params map[string]any) injectionOutcome {
	delayMS := intParam(params, "delay_ms", 250)
	timeoutMS := intParam(params, "timeout_ms", 40)
	if timeoutMS <= 0 {
		timeoutMS = 40
	}
	if delayMS <= timeoutMS {
		delayMS = timeoutMS + 120
	}
	before := s.snapshotStats()
	lease, err := s.pool.Acquire(ctx, pool.AcquireOptions{
		RequestID: "injection-query-acquire",
		Timeout:   10 * time.Second,
	})
	if err != nil {
		return injectionOutcome{code: codeFromError(err, domain.CodeInternal),
			message:     "injection setup could not acquire a connection",
			observation: map[string]any{"before": before, "setup_error": err.Error()}, err: err}
	}
	result, queryErr := s.pool.Query(ctx, lease.ConnectionID, "SELECT 1", pool.QueryOptions{
		Timeout:     time.Duration(timeoutMS) * time.Millisecond,
		InjectDelay: time.Duration(delayMS) * time.Millisecond,
		RequestID:   "injection-query-timeout",
	})
	release, releaseErr := s.pool.Release(context.Background(), lease.ConnectionID, "injection-query-release")
	after := s.snapshotStats()
	observation := map[string]any{
		"connection_id":        lease.ConnectionID,
		"delay_ms":             delayMS,
		"timeout_ms":           timeoutMS,
		"query_result_columns": result.Columns,
		"release_disposition":  release.Disposition,
		"release_error":        errorString(releaseErr),
		"before":               before,
		"after":                after,
		"simulated":            true,
	}
	if queryErr == nil {
		return injectionOutcome{code: domain.CodeInternal, message: "query timeout injection did not time out",
			observation: observation, err: errors.New("query unexpectedly succeeded")}
	}
	return injectionOutcome{
		code:        codeFromError(queryErr, domain.CodeQueryTimeout),
		message:     "query execution timed out under deterministic fault injection",
		observation: observation,
	}
}

func (s *Server) injectConnReset(ctx context.Context) injectionOutcome {
	before := s.snapshotStats()
	cfg := s.pool.Config()
	lease, err := s.pool.Acquire(ctx, pool.AcquireOptions{
		RequestID: "injection-reset-acquire",
		Timeout:   10 * time.Second,
	})
	if err != nil {
		return injectionOutcome{code: codeFromError(err, domain.CodeInternal),
			message:     "injection setup could not acquire a connection",
			observation: map[string]any{"before": before, "setup_error": err.Error()}, err: err}
	}
	holds := make([]domain.Lease, 0)
	if cfg.MaxOpen > 1 {
		holdCtx, cancelHold := context.WithTimeout(ctx, 10*time.Second)
		holds, err = s.holdAll(holdCtx, cfg.MaxOpen-1, "injection-reset-hold")
		cancelHold()
		if err != nil {
			s.releaseAll(append(holds, lease))
			return injectionOutcome{code: codeFromError(err, domain.CodeInternal),
				message:     "injection setup could not reserve the remaining pool slots",
				observation: map[string]any{"before": before, "setup_error": err.Error()}, err: err}
		}
	}
	closed, closeErr := s.pool.ForceClose(ctx, lease.ConnectionID, "loopback_test_service_force_reset")
	_, queryErr := s.pool.Query(ctx, lease.ConnectionID, "SELECT 1", pool.QueryOptions{})
	release, releaseErr := s.pool.Release(context.Background(), lease.ConnectionID, "injection-reset-release")
	replacement, acquireErr := s.pool.Acquire(context.Background(), pool.AcquireOptions{
		RequestID: "injection-reset-replacement",
		Timeout:   5 * time.Second,
	})
	replacementRelease := domain.ReleaseResult{}
	if acquireErr == nil {
		replacementRelease, _ = s.pool.Release(context.Background(), replacement.ConnectionID, "injection-reset-replacement-release")
	}
	oldRecord, oldRecordErr := s.store.GetConnection(ctx, lease.ConnectionID)
	newRecord, newRecordErr := s.store.GetConnection(ctx, replacement.ConnectionID)
	s.releaseAll(holds)
	after := s.snapshotStats()
	observation := map[string]any{
		"connection_id":             lease.ConnectionID,
		"held_connections":          connectionIDs(holds),
		"actual_close_performed":    closed.ClosedAt,
		"force_close_error":         errorString(closeErr),
		"query_error":               errorString(queryErr),
		"release_disposition":       release.Disposition,
		"release_error":             errorString(releaseErr),
		"replacement_connection_id": replacement.ConnectionID,
		"replacement_error":         errorString(acquireErr),
		"replacement_release":       replacementRelease.Disposition,
		"old_record":                oldRecord,
		"old_record_error":          errorString(oldRecordErr),
		"replacement_record":        newRecord,
		"replacement_record_error":  errorString(newRecordErr),
		"before":                    before,
		"after":                     after,
		"closed_via":                "127.0.0.1 injection endpoint",
	}
	if closeErr != nil {
		return injectionOutcome{code: domain.CodeInternal, message: "forced close failed",
			observation: observation, err: closeErr}
	}
	return injectionOutcome{
		code:        codeFromError(queryErr, domain.CodeConnReset),
		message:     "underlying SQLite connection was force-closed and rejected for reuse",
		observation: observation,
	}
}

func (s *Server) injectWaitQueueOverflow(ctx context.Context, params map[string]any) injectionOutcome {
	holdTimeoutMS := intParam(params, "hold_timeout_ms", 5000)
	waiterTimeoutMS := intParam(params, "waiter_timeout_ms", 5000)
	before := s.snapshotStats()
	cfg := s.pool.Config()
	holdCtx, cancelHold := context.WithTimeout(ctx, time.Duration(holdTimeoutMS+8000)*time.Millisecond)
	held, err := s.holdAll(holdCtx, cfg.MaxOpen, "injection-overflow-hold")
	cancelHold()
	if err != nil {
		s.releaseAll(held)
		return injectionOutcome{code: codeFromError(err, domain.CodeInternal), message: "injection setup could not hold all connections",
			observation: map[string]any{"before": before, "setup_error": err.Error()}, err: err}
	}
	heldIDs := connectionIDs(held)
	createStarted := time.Now()
	waiters := cfg.MaxWaiters
	waiterCtx, cancelWaiters := context.WithCancel(ctx)
	defer cancelWaiters()
	var wg sync.WaitGroup
	waiterResults := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lease, err := s.pool.Acquire(waiterCtx, pool.AcquireOptions{
				RequestID: fmt.Sprintf("injection-overflow-waiter-%d", i),
				Timeout:   time.Duration(waiterTimeoutMS) * time.Millisecond,
			})
			if err == nil {
				_, _ = s.pool.Release(context.Background(), lease.ConnectionID, "injection-overflow-waiter-release")
			}
			waiterResults <- err
		}(i)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s.pool.Snapshot().Stats.Waiting >= waiters {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	queuedBefore := s.pool.Snapshot().Stats.Waiting
	extraCtx, cancelExtra := context.WithTimeout(ctx, 2*time.Second)
	_, overflowErr := s.pool.Acquire(extraCtx, pool.AcquireOptions{
		RequestID: "injection-overflow-extra",
		Timeout:   500 * time.Millisecond,
	})
	cancelExtra()
	cancelWaiters()
	wg.Wait()
	close(waiterResults)
	var waiterErrors []string
	for err := range waiterResults {
		waiterErrors = append(waiterErrors, codeFromError(err, domain.CodeInternal))
	}
	s.releaseAll(held)
	held = nil
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && s.pool.Snapshot().Stats.Waiting > 0 {
		time.Sleep(5 * time.Millisecond)
	}
	after := s.snapshotStats()
	observation := map[string]any{
		"held_connections":       heldIDs,
		"max_open":               cfg.MaxOpen,
		"queue_capacity":         cfg.MaxWaiters,
		"queued_before_extra":    queuedBefore,
		"queue_wait_ms":          time.Since(createStarted).Milliseconds(),
		"residual_waiters_after": after.Waiting,
		"waiter_outcomes":        waiterErrors,
		"before":                 before,
		"after":                  after,
	}
	if overflowErr == nil {
		return injectionOutcome{code: domain.CodeInternal, message: "overflow request unexpectedly acquired a connection",
			observation: observation, err: errors.New("overflow request succeeded")}
	}
	return injectionOutcome{
		code:        codeFromError(overflowErr, domain.CodePoolExhausted),
		message:     "bounded wait queue rejected the overflowing request without leaving a residual waiter",
		observation: observation,
	}
}

func (s *Server) injectAcquireTimeout(ctx context.Context, params map[string]any) injectionOutcome {
	timeoutMS := intParam(params, "timeout_ms", 80)
	if timeoutMS <= 0 {
		timeoutMS = 80
	}
	holdTimeoutMS := intParam(params, "hold_timeout_ms", 5000)
	before := s.snapshotStats()
	cfg := s.pool.Config()
	holdCtx, cancelHold := context.WithTimeout(ctx, time.Duration(holdTimeoutMS+8000)*time.Millisecond)
	held, err := s.holdAll(holdCtx, cfg.MaxOpen, "injection-timeout-hold")
	cancelHold()
	if err != nil {
		s.releaseAll(held)
		return injectionOutcome{code: codeFromError(err, domain.CodeInternal), message: "injection setup could not hold all connections",
			observation: map[string]any{"before": before, "setup_error": err.Error()}, err: err}
	}
	heldIDs := connectionIDs(held)
	started := time.Now()
	_, acquireErr := s.pool.Acquire(ctx, pool.AcquireOptions{
		RequestID: "injection-acquire-timeout",
		Timeout:   time.Duration(timeoutMS) * time.Millisecond,
	})
	waitElapsed := time.Since(started)
	s.releaseAll(held)
	held = nil
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && s.pool.Snapshot().Stats.Waiting > 0 {
		time.Sleep(5 * time.Millisecond)
	}
	after := s.snapshotStats()
	observation := map[string]any{
		"timeout_ms":       timeoutMS,
		"elapsed_ms":       waitElapsed.Milliseconds(),
		"residual_waiters": after.Waiting,
		"held_connections": heldIDs,
		"before":           before,
		"after":            after,
	}
	if acquireErr == nil {
		return injectionOutcome{code: domain.CodeInternal, message: "timeout injection unexpectedly acquired a connection",
			observation: observation, err: errors.New("acquire unexpectedly succeeded")}
	}
	return injectionOutcome{
		code:        codeFromError(acquireErr, domain.CodeAcquireTimeout),
		message:     "acquire timed out and the FIFO wait entry was removed",
		observation: observation,
	}
}

func (s *Server) holdAll(ctx context.Context, count int, requestPrefix string) ([]domain.Lease, error) {
	leases := make([]domain.Lease, 0, count)
	for i := 0; i < count; i++ {
		lease, err := s.pool.Acquire(ctx, pool.AcquireOptions{
			RequestID: fmt.Sprintf("%s-%d", requestPrefix, i),
			Timeout:   5 * time.Second,
		})
		if err != nil {
			return leases, err
		}
		leases = append(leases, lease)
	}
	return leases, nil
}

func (s *Server) releaseAll(leases []domain.Lease) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, lease := range leases {
		_, _ = s.pool.Release(ctx, lease.ConnectionID, "injection-cleanup-release")
	}
}

func (s *Server) snapshotStats() domain.PoolStats {
	return s.pool.Snapshot().Stats
}

func codeFromError(err error, fallback string) string {
	if err == nil {
		return ""
	}
	var be *domain.BusinessError
	if errors.As(err, &be) {
		return be.Code
	}
	return fallback
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func statsEqual(a, b domain.PoolStats) bool {
	return reflect.DeepEqual(a, b)
}

func connectionIDs(leases []domain.Lease) []string {
	out := make([]string, 0, len(leases))
	for _, lease := range leases {
		out = append(out, lease.ConnectionID)
	}
	return out
}

func intParam(params map[string]any, key string, fallback int) int {
	value, ok := params[key]
	if !ok {
		return fallback
	}
	switch v := value.(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	case string:
		var n int
		_, err := fmt.Sscan(v, &n)
		if err == nil {
			return n
		}
	}
	return fallback
}
