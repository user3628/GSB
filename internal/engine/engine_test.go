package engine

import (
	"context"
	"encoding/json"
	"math"
	"path/filepath"
	"sync"
	"testing"

	"logagg/internal/agg"
	"logagg/internal/errs"
	"logagg/internal/model"
	"logagg/internal/sample"
	"logagg/internal/store"
)

func newTestEngine(t *testing.T) (*Engine, *store.Store, *model.ShardPackage) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "engine.db"))
	if err != nil {
		t.Fatal(err)
	}
	engine := New(st, Config{MemoryLimitBytes: 5120, MaxConcurrency: 2, QueueCapacity: 64})
	t.Cleanup(func() {
		engine.Close()
		if err := st.Close(); err != nil {
			t.Error(err)
		}
	})
	pkg := engine.Generator().Shard(1)
	if _, err := engine.CommitShard(context.Background(), pkg); err != nil {
		t.Fatal(err)
	}
	return engine, st, pkg
}

func TestShardInvalidFailureKeepsCommittedVersion(t *testing.T) {
	ctx := context.Background()
	e, st, _ := newTestEngine(t)

	corrupt := e.Generator().Shard(2)
	corrupt.Blocks[0].MetricPayload[0] ^= 0xff
	_, err := e.CommitShard(ctx, corrupt)
	if errs.Code(err) != errs.EShardInvalid {
		t.Fatalf("expected E_SHARD_INVALID, got %v", err)
	}
	current, ok := e.Registry().Current(sample.ShardID)
	if !ok || current.Package.Manifest.Version != 1 {
		t.Fatalf("committed registry changed after invalid commit: %+v", current)
	}
	version, ok, err := st.CurrentShardVersion(ctx, sample.ShardID)
	if err != nil || !ok || version != 1 {
		t.Fatalf("SQLite current version=%d ok=%v err=%v", version, ok, err)
	}

	stale := e.Generator().Shard(1)
	_, err = e.CommitShard(ctx, stale)
	if errs.Code(err) != errs.EShardInvalid {
		t.Fatalf("expected stale E_SHARD_INVALID, got %v", err)
	}

	validV2 := e.Generator().Shard(2)
	committed, err := e.CommitShard(ctx, validV2)
	if err != nil {
		t.Fatal(err)
	}
	if committed.Package.Manifest.Version != 2 {
		t.Fatalf("version=%d want2", committed.Package.Manifest.Version)
	}
}

func TestBudgetDeferralContinuationIdempotencyAndCrossBatch(t *testing.T) {
	ctx := context.Background()
	e, _, pkg := newTestEngine(t)
	precision := agg.Precision{InputScale: sample.MetricScale, ResultScale: 2, Mode: agg.RoundHalfUp}

	firstItems := []Item{
		{ShardID: sample.ShardID, Version: 1, BlockNo: 0},
		{ShardID: sample.ShardID, Version: 1, BlockNo: 1},
		{ShardID: sample.ShardID, Version: 1, BlockNo: 2},
	}
	req1 := AdvanceRequest{
		SessionID: "acceptance-session", BatchID: "batch-1", Items: firstItems,
		GroupColumn: sample.GroupColumn, MetricColumn: sample.MetricColumn,
		InputScale: sample.MetricScale, Precision: precision, ByteBudget: 1300,
	}
	first, err := e.Advance(ctx, req1)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != "deferred" || len(first.Processed) != 1 || len(first.Deferred) == 0 {
		t.Fatalf("expected one processed and deferrals, got status=%s processed=%d deferred=%d", first.Status, len(first.Processed), len(first.Deferred))
	}
	if first.Processed[0].StartRow != 0 || first.Processed[0].EndRow != 76 {
		t.Fatalf("unexpected first segment: %+v", first.Processed[0])
	}
	if first.Plan.RemainingBudget != 84 {
		t.Fatalf("remaining=%d want84", first.Plan.RemainingBudget)
	}

	duplicate, err := e.Advance(ctx, req1)
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Reused || duplicate.SessionVersion != first.SessionVersion || len(duplicate.Processed) != 1 {
		t.Fatalf("duplicate did not reuse stored result: reused=%v version=%d/%d", duplicate.Reused, duplicate.SessionVersion, first.SessionVersion)
	}

	continueReq := req1
	continueReq.ByteBudget = 1 << 40
	continued, err := e.Continue(ctx, continueReq)
	if err != nil {
		t.Fatal(err)
	}
	if continued.Status != "completed" {
		t.Fatalf("continuation status=%s deferred=%d", continued.Status, len(continued.Deferred))
	}
	if len(continued.ReusedSegments) != 1 {
		t.Fatalf("expected first segment reused, got %+v", continued.ReusedSegments)
	}
	continuedAgain, err := e.Continue(ctx, continueReq)
	if err != nil {
		t.Fatal(err)
	}
	if !continuedAgain.Reused || continuedAgain.Result == nil {
		t.Fatalf("completed continuation should reuse stored result: %+v", continuedAgain)
	}
	firstJSON, err := json.Marshal(continued.Result)
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := json.Marshal(continuedAgain.Result)
	if err != nil {
		t.Fatal(err)
	}
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("continuation results differ:\n%s\n%s", firstJSON, secondJSON)
	}
	if continued.ReusedSegments[0].StartRow != 0 {
		t.Fatalf("wrong reused segment: %+v", continued.ReusedSegments[0])
	}

	req2 := AdvanceRequest{
		SessionID: "acceptance-session", BatchID: "batch-2",
		Items: []Item{
			{ShardID: sample.ShardID, Version: 1, BlockNo: 3},
			{ShardID: sample.ShardID, Version: 1, BlockNo: 4},
			{ShardID: sample.ShardID, Version: 1, BlockNo: 5},
		},
		GroupColumn: sample.GroupColumn, MetricColumn: sample.MetricColumn,
		InputScale: sample.MetricScale, Precision: precision, ByteBudget: 1 << 40,
	}
	second, err := e.Advance(ctx, req2)
	if err != nil {
		t.Fatal(err)
	}
	if second.Status != "completed" || len(second.Processed) == 0 {
		t.Fatalf("bad second batch: %+v", second)
	}
	secondDup, err := e.Advance(ctx, req2)
	if err != nil || !secondDup.Reused {
		t.Fatalf("second batch idempotency err=%v reused=%v", err, secondDup == nil)
	}

	conflict := req1
	conflict.MetricColumn = "different_metric"
	_, err = e.Advance(ctx, conflict)
	if errs.Code(err) != errs.EConflictBatch {
		t.Fatalf("expected E_CONFLICTING_BATCH, got %v", err)
	}

	assertResultMatchesShard(t, ctx, e, pkg, precision)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, _, err := e.Query(ctx, "acceptance-session", precision)
			if err != nil {
				t.Error(err)
				return
			}
			if len(result.Groups) == 0 {
				t.Error("empty concurrent query result")
			}
		}()
	}
	wg.Wait()
}

func assertResultMatchesShard(t *testing.T, ctx context.Context, e *Engine, pkg *model.ShardPackage, precision agg.Precision) {
	t.Helper()
	groups, metrics, err := sample.AllRows(pkg)
	if err != nil {
		t.Fatal(err)
	}
	expectedSegment := agg.NewSegmentSum("expected", sample.ShardID, 1, 0, -1, 0, len(groups), "expected")
	if err := expectedSegment.AddRows(groups, metrics); err != nil {
		t.Fatal(err)
	}
	expectedTotal := agg.NewTotalSum()
	if err := expectedTotal.MergeSegment(expectedSegment); err != nil {
		t.Fatal(err)
	}
	expected, err := agg.Finalize(expectedTotal, precision)
	if err != nil {
		t.Fatal(err)
	}
	actual, version, err := e.Query(ctx, "acceptance-session", precision)
	if err != nil {
		t.Fatal(err)
	}
	if version == 0 || len(actual.Groups) != len(expected.Groups) {
		t.Fatalf("version=%d group count actual=%d expected=%d", version, len(actual.Groups), len(expected.Groups))
	}
	expectedByGroup := map[int64]agg.ResultGroup{}
	for _, group := range expected.Groups {
		expectedByGroup[group.Group] = group
	}
	for _, group := range actual.Groups {
		want := expectedByGroup[group.Group]
		if group.Count != want.Count || group.SumRaw != want.SumRaw || group.MinRaw != want.MinRaw || group.MaxRaw != want.MaxRaw {
			t.Fatalf("group %d mismatch actual=%+v expected=%+v", group.Group, group, want)
		}
		if group.SumRounded != want.SumRounded {
			t.Fatalf("rounding mismatch group %d: %s != %s", group.Group, group.SumRounded, want.SumRounded)
		}
	}
}

func TestFixedSampleNumericDifferenceBound(t *testing.T) {
	ctx := context.Background()
	e, _, pkg := newTestEngine(t)
	groups, metrics, err := sample.AllRows(pkg)
	if err != nil {
		t.Fatal(err)
	}
	valuesByGroup := map[int64][]int64{}
	for i := range groups {
		valuesByGroup[groups[i]] = append(valuesByGroup[groups[i]], metrics[i])
	}
	precision := agg.Precision{InputScale: sample.MetricScale, ResultScale: 2, Mode: agg.RoundHalfUp}
	if _, err := e.Advance(ctx, AdvanceRequest{
		SessionID: "numeric-session", BatchID: "numeric-batch",
		Items: []Item{
			{ShardID: sample.ShardID, Version: 1, BlockNo: 0},
			{ShardID: sample.ShardID, Version: 1, BlockNo: 1},
			{ShardID: sample.ShardID, Version: 1, BlockNo: 2},
			{ShardID: sample.ShardID, Version: 1, BlockNo: 3},
			{ShardID: sample.ShardID, Version: 1, BlockNo: 4},
			{ShardID: sample.ShardID, Version: 1, BlockNo: 5},
		},
		GroupColumn: sample.GroupColumn, MetricColumn: sample.MetricColumn,
		InputScale: sample.MetricScale, Precision: precision, ByteBudget: 1 << 40,
	}); err != nil {
		t.Fatal(err)
	}
	result, _, err := e.Query(ctx, "numeric-session", precision)
	if err != nil {
		t.Fatal(err)
	}
	maxPreRoundBound := 0.0
	maxPreRoundObserved := 0.0
	maxOutputDifference := 0.0
	maxOutputBound := 0.0
	for _, group := range result.Groups {
		values := valuesByGroup[group.Group]
		naive := agg.NaiveFloatSum(values, sample.MetricScale)
		exact := float64(group.SumRaw) / math.Pow10(sample.MetricScale)
		preRoundBound := agg.FloatAccumulationBound(values, sample.MetricScale)
		preRoundObserved := math.Abs(naive - exact)
		if preRoundObserved > preRoundBound {
			t.Fatalf("group %d float difference %.12g exceeds bound %.12g", group.Group, preRoundObserved, preRoundBound)
		}
		ourRounded := float64(group.SumRoundedRaw) / math.Pow10(precision.ResultScale)
		naiveRounded := math.Round(naive*math.Pow10(precision.ResultScale)) / math.Pow10(precision.ResultScale)
		outputDifference := math.Abs(ourRounded - naiveRounded)
		outputBound := preRoundBound + 0.5/math.Pow10(precision.ResultScale) + 1e-12
		if outputDifference > outputBound {
			t.Fatalf("group %d output difference %.6f exceeds bound %.6f", group.Group, outputDifference, outputBound)
		}
		if preRoundBound > maxPreRoundBound {
			maxPreRoundBound = preRoundBound
		}
		if preRoundObserved > maxPreRoundObserved {
			maxPreRoundObserved = preRoundObserved
		}
		if outputDifference > maxOutputDifference {
			maxOutputDifference = outputDifference
		}
		if outputBound > maxOutputBound {
			maxOutputBound = outputBound
		}
	}
	t.Logf("fixed-sample groups=%d max_pre_round_observed=%.3g max_pre_round_bound=%.3g max_output_diff=%.6f max_output_bound=%.6f cumulative_rounding_error=%s",
		len(result.Groups), maxPreRoundObserved, maxPreRoundBound, maxOutputDifference, maxOutputBound, result.CumulativeRoundingError)
}
