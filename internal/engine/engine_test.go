package engine_test

import (
	"context"
	"encoding/json"
	"math"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"ropechunk/internal/engine"
	"ropechunk/internal/model"
	"ropechunk/internal/samples"
	"ropechunk/internal/store"
)

func openTestEngine(t *testing.T) (*engine.Engine, *store.Store) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "rope.db")
	if err := store.Clean(dbPath); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return engine.New(st), st
}

func TestSampleBundleEndToEnd(t *testing.T) {
	eng, _ := openTestEngine(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	bundle, err := samples.Generate()
	if err != nil {
		t.Fatal(err)
	}
	imported, err := eng.Import(ctx, bundle)
	if err != nil {
		t.Fatal(err)
	}
	if imported.Transition.Kind != "initial" {
		t.Fatalf("transition=%s", imported.Transition.Kind)
	}
	report, err := eng.Compute(ctx, engine.ComputeOptions{Stage: "all"})
	if err != nil {
		t.Fatalf("compute: %v\nreport=%+v", err, report)
	}
	if report.Processed == 0 || report.Unprocessed != 0 {
		t.Fatalf("processed=%d unprocessed=%d errors=%v", report.Processed, report.Unprocessed, report.Errors)
	}
	verification, err := eng.VerifyReferences(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !verification.AllMatch {
		t.Fatalf("reference mismatch: %+v", verification)
	}
	status, err := eng.Status(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, shard := range status.Shards {
		for _, stage := range shard.Stages {
			if stage.CompleteRows != stage.ExpectedRows || stage.IncompleteRows != 0 || stage.StaleRows != 0 {
				t.Fatalf("bad status for %s/%s: %+v", shard.ShardID, stage.Stage, stage)
			}
		}
	}
	verifyScoreAgainstReference(t, eng, bundle)
}

func verifyScoreAgainstReference(t *testing.T, eng *engine.Engine, bundle model.Bundle) {
	t.Helper()
	ctx := context.Background()
	var shard model.Shard
	for _, candidate := range bundle.Shards {
		if candidate.ID == "s-short" {
			shard = candidate
			break
		}
	}
	if shard.ReferenceQ == nil || shard.ReferenceK == nil {
		t.Fatal("missing score reference")
	}
	records, err := eng.Store().ListCheckpoints(ctx, shard.ID, model.StageScore)
	if err != nil {
		t.Fatal(err)
	}
	dim := shard.Shape.HeadDim()
	for _, rec := range records {
		if !rec.Complete {
			continue
		}
		var payload engine.ScorePayload
		if err := json.Unmarshal(rec.ResultPayload, &payload); err != nil {
			t.Fatal(err)
		}
		for _, got := range payload.Rows {
			q := make([]float64, dim)
			for d := 0; d < dim; d++ {
				q[d] = shard.ReferenceQ.FloatAt(shard.ReferenceQ.Index(rec.Batch, rec.Head, got.QueryIndex, d))
			}
			maxScore := math.Inf(-1)
			var scores []float64
			for key := 0; key <= got.QueryIndex; key++ {
				dot := 0.0
				for d := 0; d < dim; d++ {
					dot += q[d] * shard.ReferenceK.FloatAt(shard.ReferenceK.Index(rec.Batch, rec.Head, key, d))
				}
				score := dot / math.Sqrt(float64(dim))
				scores = append(scores, score)
				maxScore = math.Max(maxScore, score)
			}
			sum := 0.0
			output := make([]float64, dim)
			for key, score := range scores {
				weight := math.Exp(score - maxScore)
				sum += weight
				for d := 0; d < dim; d++ {
					output[d] += weight * shard.ReferenceK.FloatAt(shard.ReferenceK.Index(rec.Batch, rec.Head, key, d))
				}
			}
			for d := range output {
				output[d] /= sum
			}
			if math.Abs(got.MaxScore-maxScore) > 1e-12 || math.Abs(got.ExpSum-sum) > 1e-11 {
				t.Fatalf("score state mismatch q=%d: got max/sum %v/%v want %v/%v", got.QueryIndex, got.MaxScore, got.ExpSum, maxScore, sum)
			}
			if got.AllowedKeyEnd != got.QueryIndex+1 {
				t.Fatalf("causal range mismatch q=%d end=%d", got.QueryIndex, got.AllowedKeyEnd)
			}
			for d := range output {
				if math.Abs(got.Output[d]-output[d]) > 1e-12 {
					t.Fatalf("output mismatch q=%d d=%d: got %v want %v", got.QueryIndex, d, got.Output[d], output[d])
				}
			}
			if got.WeightDigest == "" || got.OutputDigest == "" {
				t.Fatal("missing score digests")
			}
		}
	}
}

func TestIncrementalReuseAndCausalInvalidation(t *testing.T) {
	eng, st := openTestEngine(t)
	ctx := context.Background()
	bundle, err := samples.Generate()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Import(ctx, bundle); err != nil {
		t.Fatal(err)
	}
	first, err := eng.Compute(ctx, engine.ComputeOptions{Stage: "all"})
	if err != nil {
		t.Fatal(err)
	}
	if first.Processed == 0 {
		t.Fatal("first compute processed nothing")
	}
	second, err := eng.Compute(ctx, engine.ComputeOptions{Stage: "all"})
	if err != nil {
		t.Fatal(err)
	}
	if second.Processed != 0 || second.Reused == 0 || second.Unprocessed != 0 {
		t.Fatalf("second compute processed=%d reused=%d unprocessed=%d", second.Processed, second.Reused, second.Unprocessed)
	}
	rotationBefore, err := st.ListCheckpoints(ctx, "s-short", model.StageRotation)
	if err != nil {
		t.Fatal(err)
	}
	v2 := samples.NextVersion(bundle.Config, 2, func(cfg *model.Config) { cfg.CausalMask = false })
	next, err := samples.GenerateWithConfig(v2)
	if err != nil {
		t.Fatal(err)
	}
	imported, err := eng.Import(ctx, next)
	if err != nil {
		t.Fatal(err)
	}
	if imported.Transition.Kind != "advance" {
		t.Fatalf("transition=%s", imported.Transition.Kind)
	}
	if len(imported.Invalidations) == 0 {
		t.Fatal("causal change did not invalidate score states")
	}
	for _, inv := range imported.Invalidations {
		if inv.Stage == model.StageRotation {
			t.Fatalf("rotation should be reused on causal-only change: %+v", inv)
		}
	}
	rotationAfter, err := st.ListCheckpoints(ctx, "s-short", model.StageRotation)
	if err != nil {
		t.Fatal(err)
	}
	if len(rotationBefore) != len(rotationAfter) {
		t.Fatalf("rotation checkpoints changed: %d -> %d", len(rotationBefore), len(rotationAfter))
	}
	third, err := eng.Compute(ctx, engine.ComputeOptions{Stage: "all"})
	if err != nil {
		t.Fatal(err)
	}
	if third.Processed == 0 {
		t.Fatal("score stage was not rebuilt")
	}
}

func TestGracefulStopThenResume(t *testing.T) {
	eng, _ := openTestEngine(t)
	ctx := context.Background()
	bundle, cfg := tinyBundle(t, 3, 2, 24, 16)
	cfg.MaxConcurrentBlock = 2
	cfg.WorkerBlockQuota = 1
	cfg.RoundTimeBudgetMS = 10_000
	bundle.Config = cfg
	if _, err := eng.Import(ctx, bundle); err != nil {
		t.Fatal(err)
	}
	var publishes atomic.Int32
	report, err := eng.Compute(ctx, engine.ComputeOptions{Stage: "rotation", HookAfterPublish: func(stage, shard string, b, h, rb int, seq int64) {
		if publishes.Add(1) >= 2 {
			eng.Stop()
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !report.Stopped || report.Unprocessed == 0 {
		t.Fatalf("expected stopped report, got stopped=%v unprocessed=%d", report.Stopped, report.Unprocessed)
	}
	eng.ResetStop()
	resumed, err := eng.Compute(ctx, engine.ComputeOptions{Stage: "rotation"})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Stopped || resumed.Reused == 0 {
		t.Fatalf("resume did not reuse committed rows: %+v", resumed)
	}
	status, err := eng.Status(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	rotation := status.Shards[0].Stages[0]
	if rotation.CompleteRows != rotation.ExpectedRows {
		t.Fatalf("rotation incomplete after resume: %+v", rotation)
	}
}

func TestForceCancelRecoveryUsesCommittedFacts(t *testing.T) {
	eng, st := openTestEngine(t)
	ctx, cancel := context.WithCancel(context.Background())
	bundle, cfg := tinyBundle(t, 2, 2, 20, 12)
	cfg.MaxConcurrentBlock = 1
	cfg.WorkerBlockQuota = 1
	if _, err := eng.Import(ctx, bundle); err != nil {
		t.Fatal(err)
	}
	var publishes atomic.Int32
	_, _ = eng.Compute(ctx, engine.ComputeOptions{Stage: "rotation", HookAfterPublish: func(stage, shard string, b, h, rb int, seq int64) {
		if publishes.Add(1) == 2 {
			cancel()
		}
	}})
	if publishes.Load() < 2 {
		t.Fatalf("expected committed publishes, got %d", publishes.Load())
	}
	dbPath := st.Path()
	_ = st.Close()
	reopened, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	eng2 := engine.New(reopened)
	resumed, err := eng2.Compute(context.Background(), engine.ComputeOptions{Stage: "rotation"})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Reused == 0 {
		t.Fatal("resume did not observe committed rows")
	}
	status, err := eng2.Status(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := status.Shards[0].Stages[0]; got.CompleteRows != got.ExpectedRows {
		t.Fatalf("recovered rotation incomplete: %+v", got)
	}
}

func TestBudgetRejectsOversizedRowBlock(t *testing.T) {
	eng, _ := openTestEngine(t)
	ctx := context.Background()
	bundle, cfg := tinyBundle(t, 1, 1, 8, 8192)
	cfg.RoundTimeBudgetMS = 1
	cfg.RowBlockSize = 8
	cfg.MaxConcurrentBlock = 1
	cfg.WorkerBlockQuota = 1
	bundle.Config = cfg
	if _, err := eng.Import(ctx, bundle); err != nil {
		t.Fatal(err)
	}
	report, err := eng.Compute(ctx, engine.ComputeOptions{Stage: "rotation"})
	if err != nil {
		t.Fatal(err)
	}
	if report.Unprocessed == 0 {
		t.Fatalf("expected unprocessed task: %+v", report)
	}
	found := false
	for _, shard := range report.Shards {
		for _, stage := range shard.Stages {
			for _, task := range stage.Tasks {
				if task.Status == "unprocessed" && strings.Contains(task.Reason, "round budget") && task.RemainingUS >= 0 {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatalf("unprocessed task lacks budget reason/remaining: %+v", report)
	}
}

func TestMemoryPlannerSplitsOrRejects(t *testing.T) {
	cfg := samples.DefaultConfig()
	shape := model.Shape{2, 1, 8, 16}
	shard := model.Shard{ID: "m", Shape: shape}
	full := engine.EstimateBreakdown(shape, cfg, false)
	batch := engine.EstimateBreakdown(shape, cfg, true)
	cfg.PeakMemoryBytes = (full.Total + batch.Total) / 2
	plan, err := engine.PlanMemory(shard, cfg, model.StageRotation)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Mode != "batch-split" || len(plan.Steps) != 2 {
		t.Fatalf("expected batch split, got %+v", plan)
	}
	cfg.PeakMemoryBytes = 1024
	_, err = engine.PlanMemory(shard, cfg, model.StageRotation)
	app, ok := model.AsAppError(err)
	if !ok || app.Code != model.CodeMemoryBudget {
		t.Fatalf("expected E_MEMORY_BUDGET, got %v", err)
	}
}

func TestConcurrencyLimit(t *testing.T) {
	eng, _ := openTestEngine(t)
	ctx := context.Background()
	bundle, cfg := tinyBundle(t, 1, 4, 16, 8)
	cfg.MaxConcurrentBlock = 2
	cfg.WorkerBlockQuota = 7
	bundle.Config = cfg
	if _, err := eng.Import(ctx, bundle); err != nil {
		t.Fatal(err)
	}
	var active, maxActive atomic.Int32
	report, err := eng.Compute(ctx, engine.ComputeOptions{Stage: "rotation", HookAfterPublish: func(stage, shard string, b, h, rb int, seq int64) {
		now := active.Add(1)
		for {
			old := maxActive.Load()
			if now <= old || maxActive.CompareAndSwap(old, now) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		active.Add(-1)
	}})
	if err != nil {
		t.Fatal(err)
	}
	if maxActive.Load() > int32(cfg.MaxConcurrentBlock) {
		t.Fatalf("observed %d concurrent workers, cap %d", maxActive.Load(), cfg.MaxConcurrentBlock)
	}
	for _, shard := range report.Shards {
		for _, stage := range shard.Stages {
			for _, round := range stage.Rounds {
				if round.Processed > round.Capacity {
					t.Fatalf("round processed %d blocks above capacity %d", round.Processed, round.Capacity)
				}
			}
		}
	}
}

func TestExecutionPartitionChangeReusesCompleteRows(t *testing.T) {
	eng, _ := openTestEngine(t)
	ctx := context.Background()
	bundle, _ := tinyBundle(t, 1, 2, 12, 8)
	if _, err := eng.Import(ctx, bundle); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Compute(ctx, engine.ComputeOptions{Stage: "all"}); err != nil {
		t.Fatal(err)
	}
	v2 := bundle
	v2.Config.SnapshotVersion = 2
	v2.Config.KeyBlockSize = 3
	imported, err := eng.Import(ctx, v2)
	if err != nil {
		t.Fatal(err)
	}
	if imported.Transition.Kind != "advance" {
		t.Fatalf("transition=%s", imported.Transition.Kind)
	}
	if len(imported.Invalidations) != 0 {
		t.Fatalf("key-block execution partition change invalidated state: %+v", imported.Invalidations)
	}
	report, err := eng.Compute(ctx, engine.ComputeOptions{Stage: "all"})
	if err != nil {
		t.Fatal(err)
	}
	if report.Processed != 0 || report.Reused == 0 || report.Unprocessed != 0 {
		t.Fatalf("expected full reuse, got processed=%d reused=%d unprocessed=%d", report.Processed, report.Reused, report.Unprocessed)
	}
}

func TestEngineReturnsMemoryBudgetError(t *testing.T) {
	eng, _ := openTestEngine(t)
	ctx := context.Background()
	bundle, cfg := tinyBundle(t, 2, 1, 8, 16)
	cfg.PeakMemoryBytes = 1024
	bundle.Config = cfg
	if _, err := eng.Import(ctx, bundle); err != nil {
		t.Fatal(err)
	}
	_, err := eng.Compute(ctx, engine.ComputeOptions{Stage: "rotation"})
	app, ok := model.AsAppError(err)
	if !ok || app.Code != model.CodeMemoryBudget {
		t.Fatalf("expected E_MEMORY_BUDGET, got %v", err)
	}
	if app.Details["full_estimate_bytes"] == nil || app.Details["batch_estimate_bytes"] == nil {
		t.Fatalf("memory error lacks budget details: %+v", app)
	}
}

func tinyBundle(t *testing.T, batch, heads, seq, dim int) (model.Bundle, model.Config) {
	t.Helper()
	cfg := samples.DefaultConfig()
	cfg.RowBlockSize = 2
	cfg.PeakMemoryBytes = 64 << 20
	cfg.RoundTimeBudgetMS = 10_000
	shape := model.Shape{batch, heads, seq, dim}
	data := make([]int64, shape.Len())
	for i := range data {
		data[i] = int64((i*7919)%20001 - 10000)
	}
	kdata := append([]int64(nil), data...)
	for i := range kdata {
		kdata[i] = int64((i*104729)%16001 - 8000)
	}
	shard := model.Shard{ID: "tiny", Shape: shape,
		Q: model.QuantTensor{Shape: shape, Scale: 10_000, Data: data},
		K: model.QuantTensor{Shape: shape, Scale: 10_000, Data: kdata}}
	shard.InputDigest = model.InputDigest(shard)
	return model.Bundle{FormatVersion: 1, Config: cfg, Shards: []model.Shard{shard},
		Declarations: []model.ShapeDeclaration{{ShardID: "tiny", Shape: shape}}}, cfg
}
