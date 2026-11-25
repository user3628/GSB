package verify

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"time"

	"driftengine/internal/engine"
	"driftengine/internal/errs"
	"driftengine/internal/spec"
	"driftengine/internal/writeback"
)

func checkQueueTimeout(_ context.Context, opts Options) (any, error) {
	var serverCancels atomic.Int64
	slowEntered := make(chan struct{}, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/loopback/v1/objects/q-slow/fields", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case slowEntered <- struct{}{}:
		default:
		}
		<-r.Context().Done()
		serverCancels.Add(1)
	})
	mux.HandleFunc("/loopback/v1/objects/q-fast/fields", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client := writeback.NewClient(server.URL, 80*time.Millisecond)
	defer client.Close()
	scheduler := engine.NewScheduler(1, 1)
	timeoutErr := make(chan error, 1)
	if err := scheduler.Submit("timeout", func() {
		timeoutErr <- client.WriteFields(context.Background(), "q-slow", spec.Fields{"v": 1})
	}); err != nil {
		return nil, err
	}
	select {
	case <-slowEntered:
	case <-time.After(2 * time.Second):
		return nil, fmt.Errorf("slow handler did not start")
	}

	fastDone := make(chan struct{})
	queueErr := scheduler.Submit("fast", func() {
		_ = client.WriteFields(context.Background(), "q-fast", spec.Fields{"v": 1})
		close(fastDone)
	})
	if queueErr != nil {
		return nil, fmt.Errorf("fast job should enter bounded queue: %w", queueErr)
	}
	overflowErr := scheduler.Submit("overflow", func() {})
	if code := errs.Code(overflowErr); code != errs.WriteQueueFull {
		return nil, fmt.Errorf("expected E_WRITE_QUEUE_FULL, got %v", overflowErr)
	}

	var writeTimeoutErr error
	select {
	case writeTimeoutErr = <-timeoutErr:
	case <-time.After(2 * time.Second):
		return nil, fmt.Errorf("timeout job did not return")
	}
	if code := errs.Code(writeTimeoutErr); code != errs.WriteTimeout {
		return nil, fmt.Errorf("expected E_WRITE_TIMEOUT, got %v", writeTimeoutErr)
	}
	select {
	case <-fastDone:
	case <-time.After(2 * time.Second):
		return nil, fmt.Errorf("queued fast job did not run after slot release")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && serverCancels.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if serverCancels.Load() != 1 {
		return nil, fmt.Errorf("server observed %d cancellations", serverCancels.Load())
	}
	return map[string]any{
		"fast_status":            "completed",
		"queue_error":            errs.WriteQueueFull,
		"timeout_error":          errs.WriteTimeout,
		"server_observed_cancel": serverCancels.Load(),
	}, nil
}

func checkRetryExhausted(ctx context.Context, opts Options) (any, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/loopback/v1/objects/retry-bad/fields", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/loopback/v1/objects/retry-bad", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"retry-bad","fields":{"value":"actual"}}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	st, err := openStore(filepath.Join(opts.WorkDir, "retry.db"))
	if err != nil {
		return nil, err
	}
	defer st.Close()
	eng := engine.New(st, server.URL)
	snapshot := &spec.Snapshot{
		Schema:      "drift.snapshot/v1",
		Version:     1,
		GeneratedAt: time.Now().UTC(),
		Policy: spec.Policy{
			NumericRelativeTolerance: "0",
			ToleranceBoundary:        "inclusive",
			MaxBatchSize:             1,
			MaxBatchBudgetUnits:      1,
			RoundTimeBudgetMS:        5000,
			WriteTimeoutMS:           1000,
			MaxRetries:               1,
			MaxParallelBatches:       1,
			WriteQueueSize:           1,
		},
		Objects: []spec.Object{{
			ID: "retry-bad", Category: "Test", Namespace: "default",
			Desired: spec.Fields{"value": "desired"}, Actual: spec.Fields{"value": "desired"},
			ResourceBudget: spec.Budget{Units: 1},
		}},
	}
	if err := spec.NormalizeSnapshot(snapshot); err != nil {
		return nil, err
	}
	digest, _, _ := spec.DigestOf(snapshot)
	snapshot.Digest = digest
	if _, err := eng.SubmitSnapshot(ctx, snapshot); err != nil {
		return nil, err
	}
	_, triggerErr := eng.TriggerBatch(ctx, "v1-b0001", "")
	if code := errs.Code(triggerErr); code != errs.RetryExhausted {
		return nil, fmt.Errorf("expected E_RETRY_EXHAUSTED, got %v", triggerErr)
	}
	state := eng.View().ObjectStates["retry-bad"]
	if state == nil || state.Attempts != 2 || len(state.Differences) != 1 {
		return nil, fmt.Errorf("unexpected retry state: %+v", state)
	}
	return map[string]any{
		"attempts":    state.Attempts,
		"code":        errs.RetryExhausted,
		"differences": len(state.Differences),
	}, nil
}
