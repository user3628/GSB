package service

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"localinfer/workbench/internal/domain"
	"localinfer/workbench/internal/scheduler"
	"localinfer/workbench/internal/store"
)

func serviceCatalog(t *testing.T) *domain.ModelCatalog {
	t.Helper()
	catalog, err := domain.ParseModelCatalog([]byte(`{"version":"test","models":[
		{"model_id":"small","vram_per_sample":"256Mi","cost_per_sample":"333us","max_batch_samples":512},
		{"model_id":"large","vram_per_sample":"2Gi","cost_per_sample":"3ms","max_batch_samples":128},
		{"model_id":"vision","vram_per_sample":"1Gi","cost_per_sample":"7ms","max_batch_samples":64}
	]}`))
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func serviceTrace() *domain.Trace {
	return &domain.Trace{TraceID: "service-trace", Version: "v1", ContentHash: "service-hash", Requests: []domain.TraceRequest{
		{Sequence: 1, RequestID: "r1", ArrivalMS: 0, ModelID: "small", Samples: 10, LatencyLimitUS: 100000, Priority: 10},
		{Sequence: 2, RequestID: "r2", ArrivalMS: 1, ModelID: "large", Samples: 2, LatencyLimitUS: 100000, Priority: 10},
		{Sequence: 3, RequestID: "r3", ArrivalMS: 5, ModelID: "vision", Samples: 1, LatencyLimitUS: 100000, Priority: 5},
	}}
}

func serviceOptions() domain.OrganizationOptions {
	opts := domain.DefaultOrganizationOptions()
	opts.Mode = domain.ModeFull
	opts.NodeVRAMBytes = 128 << 30
	opts.LatencyDecimals = 2
	opts.AllowMixedModels = true
	return opts
}

func setupService(t *testing.T) (*Service, *store.Store, *domain.ModelCatalog, *domain.Trace) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	catalog := serviceCatalog(t)
	if err := st.InstallCatalog(catalog); err != nil {
		t.Fatal(err)
	}
	trace := serviceTrace()
	if _, err := st.SaveTrace(trace); err != nil {
		t.Fatal(err)
	}
	return New(st, catalog, false), st, catalog, trace
}

func TestOrganizeIdempotentFastFullAndCapacity(t *testing.T) {
	svc, _, _, _ := setupService(t)
	full, err := svc.Organize("service-trace", "v1", serviceOptions())
	if err != nil {
		t.Fatal(err)
	}
	fullJSON, _ := json.Marshal(full)
	repeat, err := svc.Organize("service-trace", "v1", serviceOptions())
	if err != nil {
		t.Fatal(err)
	}
	repeatJSON, _ := json.Marshal(repeat)
	if string(fullJSON) != string(repeatJSON) {
		t.Fatalf("repeat organization is not byte-identical\n%s\n%s", fullJSON, repeatJSON)
	}
	if len(full.Batches) != 2 || full.DeferredCount != 0 || full.ResultHash == "" {
		t.Fatalf("unexpected full result: %+v", full)
	}

	fastOpts := serviceOptions()
	fastOpts.Mode = domain.ModeFast
	fastOpts.OrganizationBudgetMS = 2
	fast, err := svc.Organize("service-trace", "v1", fastOpts)
	if err != nil {
		t.Fatal(err)
	}
	if len(fast.Batches) != 1 || fast.DeferredCount != 1 || fast.ModeLabel != "low_latency" {
		t.Fatalf("unexpected fast result: %+v", fast)
	}
	comparison, err := svc.CompareRuns(fast.RunID, full.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(comparison.Pairs) != 2 || comparison.Summary["left_deferred"] != 1 {
		t.Fatalf("unexpected comparison: %+v", comparison)
	}

	capOpts := serviceOptions()
	capOpts.NodeVRAMBytes = 1 << 20
	_, err = svc.Organize("service-trace", "v1", capOpts)
	var biz *domain.BizError
	if !errors.As(err, &biz) || biz.Code != domain.CodeInsufficientCap {
		t.Fatalf("expected capacity error, got %v", err)
	}
}

func TestOrganizationOrderChangesRunIdentityAndPacking(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "order.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	catalog := serviceCatalog(t)
	if err := st.InstallCatalog(catalog); err != nil {
		t.Fatal(err)
	}
	trace := &domain.Trace{TraceID: "order-trace", Version: "v1", ContentHash: "order-content", Requests: []domain.TraceRequest{
		{Sequence: 1, RequestID: "low", ArrivalMS: 0, ModelID: "small", Samples: 10, LatencyLimitUS: 100000, Priority: 1},
		{Sequence: 2, RequestID: "high-1", ArrivalMS: 1, ModelID: "large", Samples: 2, LatencyLimitUS: 100000, Priority: 9},
		{Sequence: 3, RequestID: "high-2", ArrivalMS: 2, ModelID: "small", Samples: 5, LatencyLimitUS: 100000, Priority: 9},
	}}
	if _, err := st.SaveTrace(trace); err != nil {
		t.Fatal(err)
	}
	svc := New(st, catalog, false)

	arrivalOpts := serviceOptions()
	arrivalOpts.OrganizationOrder = "arrival"
	arrival, err := svc.Organize(trace.TraceID, trace.Version, arrivalOpts)
	if err != nil {
		t.Fatal(err)
	}
	priorityOpts := serviceOptions()
	priorityOpts.OrganizationOrder = "priority"
	priority, err := svc.Organize(trace.TraceID, trace.Version, priorityOpts)
	if err != nil {
		t.Fatal(err)
	}
	if arrival.RunID == priority.RunID || arrival.OptionsHash == priority.OptionsHash || arrival.PlanHash == priority.PlanHash {
		t.Fatalf("organization order did not affect run identity: arrival=%s priority=%s", arrival.RunID, priority.RunID)
	}
	if !priority.SampleConservationOK || priority.TotalPlannedSamples != 17 || priority.BatchCount != 2 {
		t.Fatalf("priority packing did not conserve samples: %+v", priority)
	}
	if priority.Batches[0].Priority != 9 || priority.Batches[0].Members[0].RequestID != "high-1" || priority.Batches[1].Priority != 1 || priority.Batches[1].Members[0].RequestID != "low" {
		t.Fatalf("priority order was not reflected in batches: %+v", priority.Batches)
	}
	if arrival.Batches[0].Priority != 1 || arrival.Batches[1].Priority != 9 {
		t.Fatalf("arrival order was not preserved: %+v", arrival.Batches)
	}

	repeat, err := svc.Organize(trace.TraceID, trace.Version, priorityOpts)
	if err != nil {
		t.Fatal(err)
	}
	if repeat.RunID != priority.RunID || repeat.PlanHash != priority.PlanHash || repeat.ResultHash != priority.ResultHash {
		t.Fatalf("priority replay is not deterministic: first=%+v repeat=%+v", priority, repeat)
	}
}

func TestRecoveryThenReorganizeCreatesNewAttempt(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "retry.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	catalog := serviceCatalog(t)
	if err := st.InstallCatalog(catalog); err != nil {
		t.Fatal(err)
	}
	trace := serviceTrace()
	if _, err := st.SaveTrace(trace); err != nil {
		t.Fatal(err)
	}
	opts, err := domain.NormalizeOptions(serviceOptions())
	if err != nil {
		t.Fatal(err)
	}
	optionsHash, baseRunID := runIdentity(trace, catalog, opts)
	plan, err := scheduler.NewPlanner().BuildPlan(baseRunID, trace, catalog, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateRun(baseRunID, optionsHash, trace, catalog.Hash, plan); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertBatchBase(baseRunID, &plan.Batches[0]); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertBatchResults(baseRunID, &plan.Batches[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := st.StartPendingIntent("FINALIZE_RUN", trace.TraceID, baseRunID, "", map[string]any{"simulated": "kill"}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	report, err := reopened.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if report.Clean || len(report.AbortedRuns) != 1 || report.AbortedRuns[0] != baseRunID {
		t.Fatalf("unexpected recovery: %+v", report)
	}
	svc := New(reopened, catalog, false)
	retried, err := svc.Organize("service-trace", "v1", serviceOptions())
	if err != nil {
		t.Fatal(err)
	}
	wantAttemptID := baseRunID + "-a2"
	if retried.RunID != wantAttemptID || retried.Status != "complete" {
		t.Fatalf("expected completed attempt %s, got %s/%s", wantAttemptID, retried.RunID, retried.Status)
	}
	aborted, err := reopened.GetRun(baseRunID)
	if err != nil {
		t.Fatal(err)
	}
	if aborted.Status != "aborted" {
		t.Fatalf("aborted run was overwritten: %s", aborted.Status)
	}
}
