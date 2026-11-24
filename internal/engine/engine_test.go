package engine

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"driftconverge/internal/apperr"
	"driftconverge/internal/model"
	"driftconverge/internal/simulator"
	"driftconverge/internal/store"
)

func TestNormalConvergenceAndRoundReuse(t *testing.T) {
	session, endpoint, cleanup := newTestSession(t, "normal", nil)
	defer cleanup()
	report, err := session.ExecuteRound(context.Background(), ExecuteOptions{RoundID: "normal-round", Endpoint: endpoint})
	if err != nil {
		t.Fatal(err)
	}
	if report.State != "completed" || report.ConvergedObjects != 7 || report.UnresolvedObjects != 0 || report.FailedObjects != 0 {
		t.Fatalf("unexpected execution report: %+v", report)
	}
	before := writeRecordCount(t, session.Store)
	reused, err := session.ExecuteRound(context.Background(), ExecuteOptions{RoundID: "normal-round", Endpoint: endpoint})
	if err != nil {
		t.Fatal(err)
	}
	after := writeRecordCount(t, session.Store)
	if !reused.Reused || before != after || reused.ConvergedObjects != 7 {
		t.Fatalf("round was not reused: reused=%v before=%d after=%d report=%+v", reused.Reused, before, after, reused)
	}
	record, err := session.Store.GetRound(context.Background(), "normal-round")
	if err != nil || record.State != "completed" {
		t.Fatalf("round not durable: record=%+v err=%v", record, err)
	}
}

func TestAnalysisReconcilesBatchPartitionsAcrossPaths(t *testing.T) {
	session, _, cleanup := newTestSession(t, "normal", func(policy *model.Policy) {
		policy.ExactObjectLimit = 1
	})
	defer cleanup()
	analysis := session.Analysis
	if analysis == nil || analysis.DetectionPath != "bucketed" {
		t.Fatalf("expected bucketed analysis, got %+v", analysis)
	}
	if !analysis.BatchPathReconciled || analysis.ExactBatchSignature == "" || analysis.ExactBatchSignature != analysis.BucketedBatchSignature || analysis.BatchSignature != analysis.ExactBatchSignature {
		t.Fatalf("batch path audit failed: exact=%s bucketed=%s selected=%s reconciled=%v",
			analysis.ExactBatchSignature, analysis.BucketedBatchSignature, analysis.BatchSignature, analysis.BatchPathReconciled)
	}
}

func TestConcurrentRoundIdExecutesOnce(t *testing.T) {
	session, endpoint, cleanup := newTestSession(t, "normal", func(policy *model.Policy) {
		policy.WriteQueueCapacity = 8
	})
	defer cleanup()
	const workers = 8
	reports := make(chan *ExecutionReport, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			report, err := session.ExecuteRound(context.Background(), ExecuteOptions{RoundID: "shared-round", Endpoint: endpoint})
			reports <- report
			errs <- err
		}()
	}
	wg.Wait()
	close(reports)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	nonReused := 0
	for report := range reports {
		if !report.Reused {
			nonReused++
		}
	}
	if nonReused != 1 {
		t.Fatalf("round executed %d times, want 1", nonReused)
	}
	var rounds int
	_ = session.Store.DB().QueryRow(`SELECT COUNT(*) FROM rounds WHERE round_id='shared-round'`).Scan(&rounds)
	if rounds != 1 || writeRecordCount(t, session.Store) != 7 {
		t.Fatalf("idempotency violated rounds=%d writes=%d", rounds, writeRecordCount(t, session.Store))
	}
}

func TestUnresolvedReadback(t *testing.T) {
	session, endpoint, cleanup := newTestSession(t, "unresolved", nil)
	defer cleanup()
	report, err := session.ExecuteRound(context.Background(), ExecuteOptions{RoundID: "unresolved-round", Endpoint: endpoint})
	if apperr.CodeOf(err) != apperr.ConvergenceIncomplete {
		t.Fatalf("want E_CONVERGENCE_INCOMPLETE, got %v", err)
	}
	if report.UnresolvedObjects != 7 || !hasResidualCategories(report) {
		t.Fatalf("readback drift was not retained: %+v", report)
	}
}

func TestRefusedEndpoint(t *testing.T) {
	session, _, cleanup := newTestSession(t, "normal", nil)
	defer cleanup()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()
	report, err := session.ExecuteRound(context.Background(), ExecuteOptions{RoundID: "refused-round", Endpoint: "http://" + addr})
	if apperr.CodeOf(err) != apperr.EndpointRefused {
		t.Fatalf("want E_ENDPOINT_REFUSED, got %v", err)
	}
	if report.FailedObjects == 0 {
		t.Fatalf("refused writes not counted: %+v", report)
	}
}

func TestWriteTimeoutReleasesRound(t *testing.T) {
	session, endpoint, cleanup := newTestSession(t, "timeout", func(policy *model.Policy) {
		policy.WriteTimeoutMs = 40
		policy.RetryLimit = 0
	}, 150)
	defer cleanup()
	start := time.Now()
	report, err := session.ExecuteRound(context.Background(), ExecuteOptions{RoundID: "timeout-round", Endpoint: endpoint})
	if apperr.CodeOf(err) != apperr.WriteTimeout {
		t.Fatalf("want E_WRITE_TIMEOUT, got %v", err)
	}
	if report.FailedObjects == 0 || time.Since(start) > 3*time.Second {
		t.Fatalf("timeout did not fail fast/release slots: failed=%d elapsed=%s", report.FailedObjects, time.Since(start))
	}
}

func TestQueueFullKeepsStartedResults(t *testing.T) {
	session, endpoint, cleanup := newTestSession(t, "normal", func(policy *model.Policy) {
		policy.BatchSizeLimit = 1
		policy.MaxParallelBatches = 1
		policy.WriteQueueCapacity = 0
	}, 100)
	defer cleanup()
	report, err := session.ExecuteRound(context.Background(), ExecuteOptions{RoundID: "queue-round", Endpoint: endpoint})
	if apperr.CodeOf(err) != apperr.WriteQueueFull {
		t.Fatalf("want E_WRITE_QUEUE_FULL, got %v", err)
	}
	if len(report.ExecutedBatches) == 0 || len(report.SkippedBatches) == 0 {
		t.Fatalf("queue overflow lost batch facts: %+v", report)
	}
}

func TestStaleSnapshotRejectedWithoutRound(t *testing.T) {
	session, _, cleanup := newTestSession(t, "normal", nil)
	defer cleanup()
	duplicate := NewSession(session.Store, session.Samples)
	_, err := duplicate.Analyze(context.Background())
	if apperr.CodeOf(err) != apperr.StaleSnapshot {
		t.Fatalf("want E_STALE_SNAPSHOT, got %v", err)
	}
	var snapshots int
	_ = session.Store.DB().QueryRow(`SELECT COUNT(*) FROM policy_snapshots`).Scan(&snapshots)
	if snapshots != 1 {
		t.Fatalf("stale analysis added state: snapshots=%d", snapshots)
	}
}

func newTestSession(t *testing.T, mode string, mutate func(*model.Policy), delayMs ...int) (*Session, string, func()) {
	t.Helper()
	samples, err := model.LoadSamples("../../samples/cluster-objects.json", "../../samples/policy-snapshot.json", "../../samples/drift-field-values.json")
	if err != nil {
		t.Fatal(err)
	}
	if mutate != nil {
		mutate(samples.Policy)
		samples.Policy.Checksum = samples.Policy.ComputeChecksum()
	}
	dir := t.TempDir()
	st, err := store.Open(dir + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	writeDelay := 0
	if len(delayMs) > 0 {
		writeDelay = delayMs[0]
	}
	sim, err := simulator.New(samples.Cluster, simulator.Config{ListenAddr: "127.0.0.1:0", Mode: mode, WriteDelayMs: writeDelay})
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	endpoint, err := sim.Start("127.0.0.1:0")
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	session := NewSession(st, samples)
	if _, err := session.Analyze(context.Background()); err != nil {
		sim.Close(context.Background())
		st.Close()
		t.Fatal(err)
	}
	cleanup := func() {
		_ = sim.Close(context.Background())
		_ = st.Close()
	}
	return session, endpoint, cleanup
}

func writeRecordCount(t *testing.T, st *store.Store) int {
	t.Helper()
	var count int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM write_records`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func hasResidualCategories(report *ExecutionReport) bool {
	for _, batch := range report.ExecutedBatches {
		for _, object := range batch.Objects {
			if len(object.ResidualCategories) > 0 {
				return true
			}
		}
	}
	return false
}
