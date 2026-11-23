package scheduler

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gatewayworkbench/internal/model"
	"gatewayworkbench/internal/store"
)

func newTestStore(t *testing.T, upstreamPort int) (*store.Store, model.RuntimeConfig) {
	t.Helper()
	cfg := model.RuntimeConfig{
		GlobalConcurrency: 2, PerInstanceLimit: 1, QueueCapacity: 3,
		RoundBudgetMillis: 3000, ProbeConcurrency: 4,
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"))
	if err != nil {
		t.Fatal(err)
	}
	sample := &model.Sample{
		Instances: []model.Instance{{
			ID: "i1", Port: upstreamPort, Weight: 1000, Zone: "z-a",
			Probe: model.ProbeSpec{Mode: "http", Path: "/h", ExpectedStatus: 200,
				BodyMarker: "OK", TimeoutMillis: 100, SuccessThreshold: 1, FailureThreshold: 1},
		}},
		Trace: []model.RequestRecord{
			{TimestampMillis: 1, ClientID: "c1", RequestWeight: 1, RequestBytes: 8},
			{TimestampMillis: 2, ClientID: "c2", RequestWeight: 1, RequestBytes: 8},
			{TimestampMillis: 3, ClientID: "c3", RequestWeight: 1, RequestBytes: 8},
			{TimestampMillis: 4, ClientID: "c4", RequestWeight: 1, RequestBytes: 8},
		},
		Affinity: model.ZoneAffinity{Ratios: map[string]int{"z-a": 1000}},
	}
	if err := st.ReplaceSample(context.Background(), sample); err != nil {
		t.Fatal(err)
	}
	tx, intentID, err := st.BeginIntent(context.Background(), "probe-1", "probe", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetInstanceState(context.Background(), tx, store.InstanceStateRow{
		InstanceID: "i1", Phase: model.PhaseHealthy}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SaveSnapshot(context.Background(), tx, &model.SnapshotVersion{
		Reason: "test", InstanceState: map[string]model.Phase{"i1": model.PhaseHealthy},
		ZoneWeights: map[string]map[string]int{"z-a": {"i1": 1000}},
		ZoneRatios:  map[string]int{"z-a": 1000},
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitIntent(context.Background(), tx, intentID); err != nil {
		t.Fatal(err)
	}
	return st, cfg
}

func TestDispatchRoundRealCallsAndQueueFull(t *testing.T) {
	var mu sync.Mutex
	inflight, maxInflight := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		inflight++
		if inflight > maxInflight {
			maxInflight = inflight
		}
		mu.Unlock()
		time.Sleep(80 * time.Millisecond)
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		inflight--
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))
	defer server.Close()
	port := server.Listener.Addr().(*net.TCPAddr).Port
	_ = port

	st, cfg := newTestStore(t, server.Listener.Addr().(*net.TCPAddr).Port)
	defer st.Close()
	sch := New(st, cfg)
	result, err := sch.RunRound(context.Background(), "round-1")
	if err != nil {
		t.Fatal(err)
	}
	if result.Queued != cfg.QueueCapacity {
		t.Fatalf("expected queue capacity %d accepted, got %d", cfg.QueueCapacity, result.Queued)
	}
	if result.Completed != cfg.QueueCapacity || result.Failed != 0 {
		t.Fatalf("expected all queued requests to complete, got %+v", result)
	}
	if result.QueueFullError == nil || result.QueueFullError.Code != model.CodeDispatchQueueFull {
		t.Fatalf("expected E_DISPATCH_QUEUE_FULL, got %+v", result.QueueFullError)
	}
	if maxInflight > cfg.GlobalConcurrency {
		t.Fatalf("global concurrency exceeded: max=%d limit=%d", maxInflight, cfg.GlobalConcurrency)
	}
	if maxInflight == 0 {
		t.Fatal("upstream never received a real request")
	}
	decision, err := st.RequestDecision(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if decision["status"] != "DISPATCHED" {
		t.Fatalf("request should be marked dispatched, got %+v", decision)
	}
}

func TestDispatchSingleFlightSameRound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(60 * time.Millisecond)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	st, cfg := newTestStore(t, server.Listener.Addr().(*net.TCPAddr).Port)
	defer st.Close()
	sch := New(st, cfg)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	results := make(chan *DispatchResult, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := sch.RunRound(context.Background(), "same-round")
			if err != nil {
				errs <- err
				return
			}
			results <- res
		}()
	}
	wg.Wait()
	close(errs)
	close(results)
	for err := range errs {
		t.Fatal(err)
	}
	count := 0
	for res := range results {
		_ = res
		count++
	}
	if count != 2 {
		t.Fatalf("both concurrent calls must return, got %d", count)
	}
	rows, err := st.ListDispatches(context.Background(), "same-round")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != cfg.QueueCapacity {
		t.Fatalf("same round must not duplicate dispatches, got %d rows", len(rows))
	}
}

func TestRoundBudgetMarksSkippedAndCommits(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(400 * time.Millisecond)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	st, cfg := newTestStore(t, server.Listener.Addr().(*net.TCPAddr).Port)
	defer st.Close()
	cfg.GlobalConcurrency = 1
	cfg.PerInstanceLimit = 1
	cfg.QueueCapacity = 4
	cfg.RoundBudgetMillis = 120
	sch := New(st, cfg)
	result, err := sch.RunRound(context.Background(), "budget-round")
	if err != nil {
		t.Fatal(err)
	}
	if result.SkippedBudget == 0 {
		t.Fatalf("budget exhaustion must skip queued requests, got %+v", result)
	}
	rows, err := st.ListDispatches(context.Background(), "budget-round")
	if err != nil {
		t.Fatal(err)
	}
	skipped := 0
	for _, row := range rows {
		if row["status"] == "BUDGET_SKIPPED" && row["err_code"] == model.CodeBudgetExceeded {
			skipped++
		}
	}
	if skipped == 0 {
		t.Fatalf("expected BUDGET_SKIPPED dispatches, got %+v", rows)
	}
	// 预算耗尽属于正常轮次结束：意图必须到达 COMMITTED，重启不会把该轮回滚。
	var status string
	if err := st.DB().QueryRowContext(context.Background(),
		`SELECT status FROM intents WHERE round_id='budget-round'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "COMMITTED" {
		t.Fatalf("budget round intent must be COMMITTED, got %s", status)
	}
}
