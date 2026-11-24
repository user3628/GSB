package plan

import (
	"strings"
	"testing"

	"driftconverge/internal/batch"
	"driftconverge/internal/detect"
	"driftconverge/internal/fixed"
	"driftconverge/internal/model"
)

func TestFixedPointCostContract(t *testing.T) {
	samples, err := model.LoadSamples("../../samples/cluster-objects.json", "../../samples/policy-snapshot.json", "../../samples/drift-field-values.json")
	if err != nil {
		t.Fatal(err)
	}
	drift, err := detect.Analyze(samples.Cluster, samples.Policy, samples.Values)
	if err != nil {
		t.Fatal(err)
	}
	batches, err := batch.Organize(samples.Cluster, drift.Results, samples.Policy)
	if err != nil {
		t.Fatal(err)
	}
	first, err := Build(samples.Cluster, drift, batches, samples.Policy)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Build(samples.Cluster, drift, batches, samples.Policy)
	if err != nil {
		t.Fatal(err)
	}
	if first.StableSignature != second.StableSignature {
		t.Fatal("plan signature is not stable")
	}
	for _, bp := range first.Batches {
		expected := BatchBaseMicros
		for _, item := range bp.Objects {
			expected += item.ObjectCostMicros
		}
		if bp.EstimatedCostMicros != expected {
			t.Fatalf("batch %d cost=%d want %d", bp.Index, bp.EstimatedCostMicros, expected)
		}
		if !strings.Contains(bp.BudgetOccupancy, ".") || len(strings.Split(bp.BudgetOccupancy, ".")[1]) != 6 {
			t.Fatalf("occupancy must keep six decimals: %s", bp.BudgetOccupancy)
		}
		if len(bp.RoundingSteps) == 0 || !strings.Contains(bp.RoundingSteps[0].Mode, "exact") {
			t.Fatalf("rounding audit missing: %+v", bp.RoundingSteps)
		}
	}
	if first.TotalEstimatedCostMicros <= 0 || first.TotalBudgetOccupancy == "" || first.TotalRoundingError == "" {
		t.Fatalf("incomplete fixed-point summary: %+v", first)
	}
}

func TestRoundingErrorIsNonZeroAndBounded(t *testing.T) {
	objects := []model.Object{{
		ID: "a", Kind: "K", Namespace: "n", ResourceBudget: 1001,
		Expected: map[string]model.FieldValue{"x": {Type: model.TypeInteger, Value: "1"}},
		Actual:   map[string]model.FieldValue{"x": {Type: model.TypeInteger, Value: "2"}},
		Sources:  map[string]model.FieldSource{},
	}}
	cluster := &model.ClusterSpec{SnapshotVersion: 1, Objects: objects}
	if err := cluster.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	policy := samplePolicy(1)
	drift := &detect.Report{SnapshotVersion: 1, Results: []detect.ObjectDrift{{
		ObjectID: "a", HasDrift: true, Categories: []detect.Category{detect.ValueOffset}, PrimaryCategory: detect.ValueOffset,
		FieldDrifts: []detect.FieldDrift{{Category: detect.ValueOffset, Field: "x"}},
	}}}
	batches, err := batch.Organize(cluster, drift.Results, policy)
	if err != nil {
		t.Fatal(err)
	}
	cost, err := Build(cluster, drift, batches, policy)
	if err != nil {
		t.Fatal(err)
	}
	if cost.TotalRoundingError == "0.000000" {
		t.Fatalf("expected non-zero budget-weight rounding error, got %s", cost.TotalRoundingError)
	}
	// This fixture's exact budget contribution is 3.003us; half-away rounds to
	// 3us, so the signed fixed-point error is -0.003us.
	if cost.TotalRoundingError != "-0.003000" {
		t.Fatalf("unexpected rounding error: %s", cost.TotalRoundingError)
	}
}

func TestRoundingDirectionMatchesHalfAway(t *testing.T) {
	q, rem, err := fixed.MulDivHalfUp(1001, 3000, 1_000_000)
	if err != nil || q != 3 || rem.String() != "3000" {
		t.Fatalf("unexpected division q=%d rem=%s err=%v", q, rem, err)
	}
}

func samplePolicy(version int64) *model.Policy {
	p := &model.Policy{
		Version: version, GeneratedAt: "2026-11-20T08:00:00Z", ExactObjectLimit: 10, BatchSizeLimit: 10,
		DependencyOrderRule: "dependencies_first", RoundTimeBudgetMs: 10000, TimeBudgetRelativeTolerancePPM: 5000,
		FloatTolerance:     model.FloatTolerance{RelativePPM: 10000, Mode: "relative_decimal_ppm"},
		MaxParallelBatches: 2, WriteQueueCapacity: 1, RetryLimit: 0, WriteTimeoutMs: 100, PreviewSampleSize: 3,
	}
	p.Checksum = p.ComputeChecksum()
	return p
}
