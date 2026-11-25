package feedbackstream

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func newTestService(t *testing.T, mutate func(*PolicySnapshot)) (*Service, *Store, *PolicySnapshot) {
	t.Helper()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "stream.db")
	store, err := OpenStore(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	policy := DefaultPolicy()
	if mutate != nil {
		mutate(policy)
		policy.ChecksumSHA256 = ""
		sum, err := PolicyChecksum(policy)
		if err != nil {
			t.Fatal(err)
		}
		policy.ChecksumSHA256 = sum
	}
	if err := store.SaveInitialPolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}
	return NewService(store), store, policy
}

func requireCode(t *testing.T, err error, code string) *Error {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s, got nil", code)
	}
	coded, ok := AsError(err)
	if !ok {
		t.Fatalf("expected coded error %s, got %T %v", code, err, err)
	}
	if coded.Code != code {
		t.Fatalf("expected %s, got %s: %v", code, coded.Code, coded)
	}
	return coded
}

func TestSpecValidation(t *testing.T) {
	tests := []struct {
		name string
		line string
	}{
		{"negative partition", `{"timestamp_ms":1,"partition_id":-1,"group_key":"a","metric_fixed":1,"sequence":1}`},
		{"metric outside int64", `{"timestamp_ms":1,"partition_id":0,"group_key":"a","metric_fixed":9223372036854775808,"sequence":1}`},
		{"empty key", `{"timestamp_ms":1,"partition_id":0,"group_key":"","metric_fixed":1,"sequence":1}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseEventsJSONL(strings.NewReader(tc.line + "\n"))
			requireCode(t, err, CodeSpecInvalid)
		})
	}

	lines := []string{
		`{"timestamp_ms":1,"partition_id":7,"group_key":"a","metric_fixed":1,"sequence":2}`,
		`{"timestamp_ms":2,"partition_id":7,"group_key":"b","metric_fixed":2,"sequence":2}`,
	}
	_, err := ParseEventsJSONL(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	coded := requireCode(t, err, CodeSpecInvalid)
	if coded.Details["partition_id"] != 7 {
		t.Fatalf("unexpected details: %#v", coded.Details)
	}
}

func TestPolicyChecksumAndPipelineInvalid(t *testing.T) {
	policy := DefaultPolicy()
	policy.ChecksumSHA256 = "00"
	data, _ := json.Marshal(policy)
	_, err := ParsePolicySnapshot(data)
	requireCode(t, err, CodeSpecInvalid)

	bad := DefaultPolicy()
	bad.Pipeline.Operators[3].InputSignature.Schema = "incompatible.schema"
	_, err = PlanFusion(bad)
	coded := requireCode(t, err, CodePipelineInvalid)
	if coded.Details["operator_id"] != "aggregate" {
		t.Fatalf("expected aggregate as first incompatible operator: %#v", coded.Details)
	}
}

func TestFusionPlanAndRepartitionBoundary(t *testing.T) {
	plan, err := PlanFusion(DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Groups) != 2 || plan.Groups[0].ID != "fg-1" || plan.Groups[1].ID != "fg-2" {
		t.Fatalf("unexpected groups: %#v", plan.Groups)
	}
	if !plan.EdgeDecisions[0].Fused || !plan.EdgeDecisions[1].Fused || plan.EdgeDecisions[2].Fused {
		t.Fatalf("unexpected fusion decisions: %#v", plan.EdgeDecisions)
	}

	repartition := DefaultPolicy()
	repartition.Pipeline.Edges[0].Repartition = true
	plan, err = PlanFusion(repartition)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, edge := range plan.EdgeDecisions {
		if edge.From == "scan" && edge.To == "filter" {
			for _, reason := range edge.Reasons {
				if reason == "CROSS_PARTITION_REDISTRIBUTION" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatalf("repartition reason missing: %#v", plan.EdgeDecisions)
	}
}

func TestWindowBoundaryAndNumericOverflow(t *testing.T) {
	policy := DefaultPolicy()
	start := WindowStart(policy.GeneratedAtMS+999, policy.Window)
	if start != policy.GeneratedAtMS {
		t.Fatalf("unexpected window start %d", start)
	}
	if !WindowContains(policy.GeneratedAtMS+999, start, policy.Window) {
		t.Fatal("end-1 should be included")
	}
	if WindowContains(policy.GeneratedAtMS+1000, start, policy.Window) {
		t.Fatal("exclusive end should not be included")
	}

	rows := []ProjectedEvent{
		{WindowStartMS: start, WindowEndMS: start + 1000, Event: Event{GroupKey: "k", MetricFixed: 1<<63 - 1}},
		{WindowStartMS: start, WindowEndMS: start + 1000, Event: Event{GroupKey: "k", MetricFixed: 1}},
	}
	_, err := AggregateRows(rows, "fg-2", "sig", 3)
	coded := requireCode(t, err, CodeNumericOverflow)
	if coded.Details["group_key"] != "k" || coded.Details["window_start_ms"] != start {
		t.Fatalf("overflow details missing: %#v", coded.Details)
	}
}

func TestBudgetDeterministicOrder(t *testing.T) {
	policy := DefaultPolicy()
	batches, err := BuildWindowBatches(DefaultEvents(), policy.Window)
	if err != nil {
		t.Fatal(err)
	}
	candidates := make([]Candidate, 0, len(batches))
	for _, batch := range batches {
		candidates = append(candidates, CandidateFromBatch(batch, policy))
	}
	plan := PlanRound("r1", candidates, policy, map[int]int{}, 0)
	if len(plan.Shed) != 7 {
		t.Fatalf("expected seven shed decisions, got %d: %#v", len(plan.Shed), plan.Shed)
	}
	if plan.Shed[0].BatchID != "batch-p0-w1" || plan.Shed[0].ReasonCode != "PARTITION_CONCURRENCY_LIMIT" {
		t.Fatalf("unexpected first shed: %#v", plan.Shed[0])
	}
	var budget bool
	for _, decision := range plan.Shed {
		if decision.BatchID == "batch-p2-w0" && decision.ReasonCode == "ROUND_TIME_BUDGET_EXHAUSTED" {
			budget = true
		}
	}
	if !budget {
		t.Fatalf("missing budget shed for p2-w0: %#v", plan.Shed)
	}
}

func TestBackpressureStallIsCodedAndDoesNotDrop(t *testing.T) {
	service, store, policy := newTestService(t, func(p *PolicySnapshot) {
		limit := BufferLimit{MaxInflightBatches: 1, MaxBytes: 1 << 30, HighWatermarkRatio: 1}
		p.OperatorBuffers["fg-1"] = limit
		p.OperatorBuffers["fg-2"] = limit
		p.MaxWaitQueue = 1
	})
	ctx := context.Background()
	plan, err := PlanFusion(policy)
	if err != nil {
		t.Fatal(err)
	}
	for _, req := range []PushBufferRequest{
		{PolicyVersion: 1, GroupID: "fg-1", BatchID: "b1", Bytes: 1, Limit: policy.BufferFor("fg-1")},
		{PolicyVersion: 1, GroupID: "fg-2", BatchID: "b2", Bytes: 1, Limit: policy.BufferFor("fg-2")},
	} {
		result, err := store.PushToBuffer(ctx, req)
		if err != nil || !result.Admitted {
			t.Fatalf("seed admission failed: %v %#v", err, result)
		}
	}
	blocked, err := store.PushToBuffer(ctx, PushBufferRequest{
		PolicyVersion: 1, GroupID: "fg-1", BatchID: "waiting-1", Bytes: 1,
		Limit: policy.BufferFor("fg-1"),
	})
	if err != nil || blocked.Admitted {
		t.Fatalf("expected blocked seed: %v %#v", err, blocked)
	}

	_, err = service.admit(ctx, &Batch{ID: "waiting-2", PartitionID: 9}, *plan.GroupByID("fg-1"), policy)
	requireCode(t, err, CodeBackpressureStall)
	waiting, err := store.CountWaiting(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if waiting != 1 {
		t.Fatalf("queue should remain at capacity one, got %d", waiting)
	}
}

func TestIdempotentSubmitConflictAndConcurrentReaders(t *testing.T) {
	service, store, _ := newTestService(t, nil)
	ctx := context.Background()
	events := append([]Event(nil), DefaultEvents()[:4]...)
	const batchID = "batch-idempotent"

	var wg sync.WaitGroup
	results := make(chan *BatchResult, 16)
	errs := make(chan error, 16)
	stopReaders := make(chan struct{})
	var readerWG sync.WaitGroup
	for i := 0; i < 6; i++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				_, _, _ = service.ActivePlan(ctx)
				_, _ = store.ListWindowResults(ctx, 1, false)
			}
		}()
	}
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := service.SubmitBatch(ctx, SubmitBatchRequest{BatchID: batchID, Events: events})
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}()
	}
	wg.Wait()
	close(stopReaders)
	readerWG.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent submit: %v", err)
	}
	count := 0
	for result := range results {
		count++
		if result.Status != StatusCompleted {
			t.Fatalf("unexpected result status: %s", result.Status)
		}
	}
	if count != 16 {
		t.Fatalf("expected 16 results, got %d", count)
	}
	var attempts int
	if err := store.DB().QueryRow(`SELECT attempts FROM batches WHERE batch_id=?`, batchID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("batch should start once, attempts=%d", attempts)
	}

	changed := append([]Event(nil), events...)
	changed[0].MetricFixed++
	_, err := service.SubmitBatch(ctx, SubmitBatchRequest{BatchID: batchID, Events: changed})
	requireCode(t, err, CodeConflictRequest)
}

func TestRecoveryDiscardsWindowIntent(t *testing.T) {
	_, store, _ := newTestService(t, nil)
	ctx := context.Background()
	_, err := store.CreateIntent(ctx, IntentRecord{
		ID: "intent-window-test", Kind: IntentWindowConclusion, AggregateID: "b",
	})
	if err != nil {
		t.Fatal(err)
	}
	report, err := store.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.ScannedPendingIntents != 1 || len(report.DiscardedWindowIntents) != 1 {
		t.Fatalf("unexpected report: %#v", report)
	}
}

func TestDemoEndToEnd(t *testing.T) {
	report, err := RunDemo(context.Background(), DemoOptions{DataDir: filepath.Join(t.TempDir(), "data")})
	if err != nil {
		t.Fatal(err)
	}
	if report.CompletedBatchCount != 9 {
		t.Fatalf("expected nine completed batches, got %d", report.CompletedBatchCount)
	}
	if !report.BackpressureDrill.AllDrillBatchesCompleted ||
		report.BackpressureDrill.BlockedReport.BlockedUpstreamBatches != 1 {
		t.Fatalf("bad backpressure drill: %#v", report.BackpressureDrill)
	}
	if report.WindowResultsV1 == 0 || report.WindowResultsV2 == 0 {
		t.Fatalf("expected rebuilt window results, v1=%d v2=%d", report.WindowResultsV1, report.WindowResultsV2)
	}
	if report.StaleBaseErrorCode != CodeStaleBase {
		t.Fatalf("expected stale base, got %q", report.StaleBaseErrorCode)
	}
	if report.FinalRecovery.ScannedPendingIntents != 0 {
		t.Fatalf("expected no pending intents: %#v", report.FinalRecovery)
	}
}
