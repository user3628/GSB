package scheduler

import (
	"errors"
	"testing"

	"localinfer/workbench/internal/domain"
)

func testCatalog() *domain.ModelCatalog {
	return domain.MustCatalogFromModels("test", []domain.ModelSpec{
		{ModelID: "small", VRAMPerSample: 256 << 20, CostPerSample: 333, MaxBatchSample: 512},
		{ModelID: "large", VRAMPerSample: 2 << 30, CostPerSample: 3000, MaxBatchSample: 128},
		{ModelID: "vision", VRAMPerSample: 1 << 30, CostPerSample: 7000, MaxBatchSample: 64},
	})
}

func request(seq int, id string, arrival int64, model string, samples, limitUS, priority int64) domain.TraceRequest {
	return domain.TraceRequest{Sequence: seq, RequestID: id, ArrivalMS: arrival, ModelID: model, Samples: samples, LatencyLimitUS: limitUS, Priority: priority}
}

func makeTrace(reqs ...domain.TraceRequest) *domain.Trace {
	return &domain.Trace{TraceID: "trace", Version: "v1", ContentHash: "hash", Requests: reqs}
}

func baseOptions() domain.OrganizationOptions {
	opts := domain.DefaultOrganizationOptions()
	opts.Mode = domain.ModeFull
	opts.NodeVRAMBytes = 128 << 30
	opts.LatencyDecimals = 2
	opts.AllowMixedModels = true
	return opts
}

func TestBuildPlanSplitsByModelSampleLimitAndConservesSamples(t *testing.T) {
	catalog := testCatalog()
	trace := &domain.Trace{
		TraceID: "split", Version: "v1", ContentHash: "hash",
		Requests: []domain.TraceRequest{{Sequence: 1, RequestID: "s1", ArrivalMS: 0, ModelID: "small", Samples: 700, LatencyLimitUS: 500000, Priority: 1}},
	}
	opts := domain.DefaultOrganizationOptions()
	opts.Mode = domain.ModeFull
	opts.NodeVRAMBytes = 256 << 30
	opts.LatencyDecimals = 2
	plan, err := NewPlanner().BuildPlan("run-test", trace, catalog, opts)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if len(plan.Batches) != 2 {
		t.Fatalf("batches=%d want 2", len(plan.Batches))
	}
	if plan.Batches[0].TotalSamples != 512 || plan.Batches[1].TotalSamples != 188 {
		t.Fatalf("split samples=%d,%d", plan.Batches[0].TotalSamples, plan.Batches[1].TotalSamples)
	}
	if !plan.SampleConservationOK || plan.TotalPlannedSamples != 700 {
		t.Fatalf("conservation=%t total=%d", plan.SampleConservationOK, plan.TotalPlannedSamples)
	}
	if plan.Batches[1].RootSequence != plan.Batches[0].RootSequence || plan.Batches[1].SplitIndex != 1 || plan.Batches[1].SplitReason != "MODEL_MAX_SAMPLES" {
		t.Fatalf("split trace fields: %+v", plan.Batches[1].PlannedBatch)
	}
}

func TestFastModeDefersOutsideBudgetAndFullModeUsesAll(t *testing.T) {
	catalog := testCatalog()
	trace := makeTrace(
		request(1, "r1", 0, "small", 10, 100000, 10),
		request(2, "r2", 1, "large", 2, 100000, 10),
		request(3, "r3", 5, "vision", 1, 100000, 5),
	)
	fastOpts := baseOptions()
	fastOpts.Mode = domain.ModeFast
	fastOpts.OrganizationBudgetMS = 2
	fast, err := NewPlanner().BuildPlan("run-fast", trace, catalog, fastOpts)
	if err != nil {
		t.Fatal(err)
	}
	if fast.DeferredCount != 1 || fast.Deferred[0].RequestID != "r3" || fast.Deferred[0].Reason != "ORGANIZATION_BUDGET_EXCEEDED" {
		t.Fatalf("unexpected fast deferral: %+v", fast.Deferred)
	}
	if len(fast.Batches) != 1 || fast.Batches[0].TotalSamples != 12 || fast.SelectedRequests != 2 {
		t.Fatalf("unexpected fast plan: %s", fast.SummaryString())
	}
	full, err := NewPlanner().BuildPlan("run-full", trace, catalog, baseOptions())
	if err != nil {
		t.Fatal(err)
	}
	if full.DeferredCount != 0 || len(full.Batches) != 2 || full.TotalPlannedSamples != 13 {
		t.Fatalf("unexpected full plan: %s", full.SummaryString())
	}
	again, err := NewPlanner().BuildPlan("run-full", trace, catalog, baseOptions())
	if err != nil || again.PlanHash != full.PlanHash {
		t.Fatalf("repeat plan not bitwise deterministic: %v %s %s", err, full.PlanHash, again.PlanHash)
	}
}

func TestPriorityIsolationAndMixedModelLatencyRejection(t *testing.T) {
	catalog := testCatalog()
	trace := makeTrace(
		request(1, "m1", 0, "small", 10, 100000, 7),
		request(2, "m2", 1, "vision", 20, 200000, 7),
		request(3, "m3", 2, "small", 1, 100000, 3),
	)
	plan, err := NewPlanner().BuildPlan("run-mix", trace, catalog, baseOptions())
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Batches) != 3 {
		t.Fatalf("expected 3 separated batches, got %d", len(plan.Batches))
	}
	if len(plan.MixRejections) != 1 || plan.MixRejections[0].Reason != "MIXED_MODEL_LATENCY_LIMIT" {
		t.Fatalf("unexpected mix rejections: %+v", plan.MixRejections)
	}
	if plan.Batches[0].Priority != 7 || plan.Batches[1].Priority != 7 || plan.Batches[2].Priority != 3 {
		t.Fatalf("priority isolation failed: %+v", plan.Batches)
	}
	for _, batch := range plan.Batches {
		if !batch.LatencyOK {
			t.Fatalf("batch unexpectedly failed latency: %s", batch.BatchID)
		}
	}
}

func TestLatePoliciesNeverMutateSealedComposition(t *testing.T) {
	catalog := testCatalog()
	trace := makeTrace(
		request(1, "r1", 0, "small", 10, 100000, 1),
		request(2, "r2", 1, "large", 1, 100000, 1),
	)
	rejectOpts := baseOptions()
	rejectOpts.LatePolicy = domain.LatePolicyReject
	rejectOpts.Faults.LateRequestIDs = []string{"r2"}
	rejected, err := NewPlanner().BuildPlan("run-late-reject", trace, catalog, rejectOpts)
	if err != nil {
		t.Fatal(err)
	}
	if len(rejected.Batches) != 1 || rejected.Batches[0].Members[0].RequestID != "r1" || rejected.DeferredCount != 1 {
		t.Fatalf("late reject mutated sealed batch: %s %+v", rejected.SummaryString(), rejected.Deferred)
	}
	newOpts := rejectOpts
	newOpts.LatePolicy = domain.LatePolicyNewBatch
	isolated, err := NewPlanner().BuildPlan("run-late-new", trace, catalog, newOpts)
	if err != nil {
		t.Fatal(err)
	}
	if len(isolated.Batches) != 2 || isolated.Batches[1].Members[0].RequestID != "r2" || !isolated.Batches[1].LateSealed || isolated.Batches[1].SplitReason != "LATE_AFTER_SEAL_NEW_BATCH" {
		t.Fatalf("late new-batch policy not traceable: %+v", isolated.Batches)
	}
}

func TestNonLateRequestDoesNotJoinLateSealedBatch(t *testing.T) {
	catalog := testCatalog()
	trace := makeTrace(
		request(1, "late", 0, "small", 4, 100000, 9),
		request(2, "normal", 1, "small", 3, 100000, 9),
	)
	opts := baseOptions()
	opts.OrganizationOrder = "priority"
	opts.LatePolicy = domain.LatePolicyNewBatch
	opts.Faults.LateRequestIDs = []string{"late"}
	plan, err := NewPlanner().BuildPlan("run-late-boundary", trace, catalog, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Batches) != 2 {
		t.Fatalf("late batch absorbed a non-late request: %+v", plan.Batches)
	}
	if !plan.Batches[0].LateSealed || plan.Batches[0].Members[0].RequestID != "late" {
		t.Fatalf("late request was not isolated: %+v", plan.Batches[0])
	}
	if plan.Batches[1].LateSealed || plan.Batches[1].Members[0].RequestID != "normal" || plan.Batches[0].SealReason != "LATE_SEAL_BOUNDARY: a non-late request cannot join a late-sealed batch" {
		t.Fatalf("non-late boundary is not traceable: %+v", plan.Batches[1])
	}
}

func TestCapacityAndLatencyFailuresAreNotPassed(t *testing.T) {
	catalog := testCatalog()
	trace := makeTrace(request(1, "c1", 0, "small", 100, 1000000, 1))
	opts := baseOptions()
	opts.NodeVRAMBytes = 100 << 20
	_, err := NewPlanner().BuildPlan("run-cap", trace, catalog, opts)
	var biz *domain.BizError
	if !errors.As(err, &biz) || biz.Code != domain.CodeInsufficientCap {
		t.Fatalf("expected capacity error, got %v", err)
	}

	latencyOpts := baseOptions()
	latencyTrace := makeTrace(request(1, "l1", 0, "small", 1, 100, 1))
	plan, err := NewPlanner().BuildPlan("run-latency", latencyTrace, catalog, latencyOpts)
	if err != nil {
		t.Fatal(err)
	}
	if plan.LatencyOK || plan.Batches[0].LatencyOK || plan.Batches[0].Status != "latency_failed" {
		t.Fatalf("latency breach was marked passed: %+v", plan.Batches[0])
	}
}

func TestQuantilesPrecisionAndDeterministicRounding(t *testing.T) {
	catalog := testCatalog()
	trace := makeTrace(
		request(1, "q1", 0, "small", 100, 300000, 1),
		request(2, "q2", 1, "large", 20, 300000, 1),
		request(3, "q3", 2, "small", 5, 300000, 0),
	)
	plan, err := NewPlanner().BuildPlan("run-quantile", trace, catalog, baseOptions())
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Batches) != 2 {
		t.Fatalf("batches=%d", len(plan.Batches))
	}
	b1 := plan.Batches[0]
	if b1.TotalSamples != 120 || b1.LatencyUS != 93300 || b1.Precision.RoundingErrorUS != 0 || !b1.Precision.PrecisionOK {
		t.Fatalf("unexpected first batch: %+v", b1)
	}
	p99, ok := Quantile(b1.Quantiles, 99)
	if !ok || !p99.Available || p99.LatencyUS != 3000 || p99.SampleCount != 120 {
		t.Fatalf("unexpected P99: %+v", p99)
	}
	p99b2, ok := Quantile(plan.Batches[1].Quantiles, 99)
	if !ok || p99b2.Available || p99b2.ErrorCode != domain.CodeInsufficientSample {
		t.Fatalf("expected insufficient P99 on small batch: %+v", p99b2)
	}

	breachOpts := baseOptions()
	breachOpts.Faults.PrecisionOffsetUS = 2000
	breach, err := NewPlanner().BuildPlan("run-breach", trace, catalog, breachOpts)
	if err != nil {
		t.Fatal(err)
	}
	if breach.PrecisionOK || breach.Batches[0].Status != "precision_failed" || breach.Batches[0].Precision.AbsoluteDeviationUS < 2000 || breach.Batches[0].Precision.RoundingErrorUS != 0 {
		t.Fatalf("precision breach not marked: %+v", breach.Batches[0].Precision)
	}
}
