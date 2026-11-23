package store

import (
	"errors"
	"path/filepath"
	"testing"

	"localinfer/workbench/internal/domain"
	"localinfer/workbench/internal/scheduler"
)

func testCatalog(t *testing.T) *domain.ModelCatalog {
	t.Helper()
	catalog, err := domain.ParseModelCatalog([]byte(`{"version":"test","models":[
		{"model_id":"small","vram_per_sample":"256Mi","cost_per_sample":"333us","max_batch_samples":512},
		{"model_id":"large","vram_per_sample":"2Gi","cost_per_sample":"3ms","max_batch_samples":128}
	]}`))
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func testTrace() *domain.Trace {
	return &domain.Trace{TraceID: "trace", Version: "v1", ContentHash: "content-hash", Requests: []domain.TraceRequest{
		{Sequence: 1, RequestID: "r1", ArrivalMS: 0, ModelID: "small", Samples: 4, LatencyLimitUS: 100000, Priority: 2},
		{Sequence: 2, RequestID: "r2", ArrivalMS: 1, ModelID: "large", Samples: 1, LatencyLimitUS: 100000, Priority: 1},
	}}
}

func testOptions() domain.OrganizationOptions {
	opts := domain.DefaultOrganizationOptions()
	opts.Mode = domain.ModeFull
	opts.NodeVRAMBytes = 8 << 30
	opts.LatencyDecimals = 2
	return opts
}

func TestSaveTraceIdempotentAndConflicting(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.InstallCatalog(testCatalog(t)); err != nil {
		t.Fatal(err)
	}
	trace := testTrace()
	created, err := st.SaveTrace(trace)
	if err != nil || !created {
		t.Fatalf("first SaveTrace=%t,%v", created, err)
	}
	created, err = st.SaveTrace(trace)
	if err != nil || created {
		t.Fatalf("idempotent SaveTrace=%t,%v", created, err)
	}
	conflict := *trace
	conflict.ContentHash = "different"
	_, err = st.SaveTrace(&conflict)
	var biz *domain.BizError
	if !errors.As(err, &biz) || biz.Code != domain.CodeConflicting {
		t.Fatalf("expected conflicting request, got %v", err)
	}
	intents, err := st.ListIntents(20)
	if err != nil {
		t.Fatal(err)
	}
	if len(intents) == 0 || intents[len(intents)-1].Kind != "INSTALL_MODELS" {
		t.Fatalf("intent log missing operations: %+v", intents)
	}
	for _, intent := range intents {
		if intent.State != "committed" {
			t.Fatalf("unexpected intent state: %+v", intent)
		}
	}
}

func TestRecoveryPreservesCompleteBatchAndAbortsPendingFacts(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "recovery.db")
	st, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	catalog := testCatalog(t)
	trace := testTrace()
	if err := st.InstallCatalog(catalog); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SaveTrace(trace); err != nil {
		t.Fatal(err)
	}
	plan, err := scheduler.NewPlanner().BuildPlan("run-recovery", trace, catalog, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(plan.Batches))
	}
	if err := st.CreateRun("run-recovery", "options-hash", trace, catalog.Hash, plan); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertBatchBase("run-recovery", &plan.Batches[0]); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertBatchResults("run-recovery", &plan.Batches[0]); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertBatchBase("run-recovery", &plan.Batches[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := st.StartPendingIntent("COMMIT_BATCH_RESULTS", trace.TraceID, "run-recovery", plan.Batches[1].BatchID, map[string]any{"simulated": "kill"}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st, err = Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	report, err := st.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if report.Clean || len(report.AbortedRuns) != 1 || report.AbortedRuns[0] != "run-recovery" {
		t.Fatalf("unexpected recovery report: %+v", report)
	}
	if len(report.StartedIntents) != 1 || report.StartedIntents[0].Kind != "COMMIT_BATCH_RESULTS" {
		t.Fatalf("started intent not reported: %+v", report.StartedIntents)
	}
	if len(report.PreservedBatches) != 1 || report.PreservedBatches[0] != plan.Batches[0].BatchID {
		t.Fatalf("complete batch not preserved: %+v", report.PreservedBatches)
	}
	if len(report.AbortedBatches) != 1 || report.AbortedBatches[0] != plan.Batches[1].BatchID {
		t.Fatalf("pending batch not aborted: %+v", report.AbortedBatches)
	}
	if report.CompletionClaim != "NO_COMPLETION_CLAIM_FOR_INTERRUPTED_RUN" {
		t.Fatalf("unexpected completion claim: %s", report.CompletionClaim)
	}
	run, err := st.GetRun("run-recovery")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "aborted" || len(run.Batches) != 2 {
		t.Fatalf("unexpected recovered run: %+v", run)
	}
	if run.Batches[0].Status != "complete" || run.Batches[1].Status != "aborted" {
		t.Fatalf("unexpected recovered batch states: %s,%s", run.Batches[0].Status, run.Batches[1].Status)
	}
	if run.UsedVRAMBytes != plan.Batches[0].VRAMBytes {
		t.Fatalf("aborted batch VRAM should not be counted: got %d want %d", run.UsedVRAMBytes, plan.Batches[0].VRAMBytes)
	}
	for _, intent := range mustIntents(t, st, 100) {
		if intent.Kind == "COMMIT_BATCH_RESULTS" && intent.BatchID == plan.Batches[1].BatchID && intent.State != "aborted" {
			t.Fatalf("pending intent was not aborted: %+v", intent)
		}
	}
}

func mustIntents(t *testing.T, st *Store, limit int) []domain.IntentFact {
	t.Helper()
	intents, err := st.ListIntents(limit)
	if err != nil {
		t.Fatal(err)
	}
	return intents
}
