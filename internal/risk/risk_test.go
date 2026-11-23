package risk

import (
	"testing"

	"local/rollout-guard/internal/domain"
)

func basePolicy() domain.Policy {
	return domain.Policy{
		Version: 1, GeneratedAtMS: 1700000000000, ObservationWindowMS: 60000, BucketMS: 5000,
		MinSamples: 5, FastSampleLimit: 1,
		Thresholds:   domain.Thresholds{ErrorRatePPM: 20000, LatencyDegradationPPM: 300000, SuccessDropPPM: 20000, LevelTolerancePPM: 10000},
		WeightsBP:    domain.WeightsBP{ErrorBP: 4000, LatencyBP: 3500, SuccessBP: 2500},
		Risk:         domain.RiskLimits{PassBP: 1800, MediumBP: 2500, HighBP: 5000, ExtremeBP: 7500, RollbackBP: 7000},
		AutoRollback: true, CooldownMS: 120000, Checksum: "test",
	}
}

func TestNearestRankP90AndAuditContract(t *testing.T) {
	policy := basePolicy()
	batch := domain.Batch{
		ID: "p90", Service: "svc", ChangeType: domain.ChangeConfig, BatchSize: 1,
		Baseline: domain.Baseline{TimestampMS: 1700000000000, ErrorRatePPM: 0, P90LatencyMS: 100, SuccessRatePPM: 1000000},
	}
	observations := []StoreObservation{
		{Observation: domain.Observation{Seq: 1, TimestampMS: 1700000100000, MetricType: domain.MetricErrorRate, SampleCount: 10, SuccessCount: 10}},
		{Observation: domain.Observation{Seq: 2, TimestampMS: 1700000100000, MetricType: domain.MetricLatencyMS, SampleCount: 10, LatencySamplesMS: []int64{100, 101, 102, 103, 104, 105, 106, 107, 108, 109}}},
		{Observation: domain.Observation{Seq: 3, TimestampMS: 1700000100000, MetricType: domain.MetricSuccessRate, SampleCount: 10, SuccessCount: 10}},
	}
	evaluation, err := Compute(policy, batch, observations, SnapshotMeta{AsOfCommitSeq: 9, ObservationRevision: 1}, ModeFull)
	if err != nil {
		t.Fatal(err)
	}
	if evaluation.Metrics.LatencySamples != 10 || evaluation.Metrics.P90NearestRank != 9 || evaluation.Metrics.LatencyP90MS != 108 {
		t.Fatalf("nearest rank metadata = n:%d rank:%d p90:%d", evaluation.Metrics.LatencySamples, evaluation.Metrics.P90NearestRank, evaluation.Metrics.LatencyP90MS)
	}
	if evaluation.Calculation.RoundingErrorRational == "" || len(evaluation.RoundingSteps) < 5 {
		t.Fatalf("audit is incomplete: %+v", evaluation.Calculation)
	}
	if evaluation.Calculation.CumulativeRoundingErrorMicroBP != "333333" {
		t.Fatalf("signed cumulative rounding error = %s", evaluation.Calculation.CumulativeRoundingErrorMicroBP)
	}
	if evaluation.AsOfCommitSeq != 9 || evaluation.ObservationRevision != 1 {
		t.Fatal("snapshot metadata lost")
	}
}

func TestLevelBoundaryUsesRelativeToleranceAndStableOrder(t *testing.T) {
	policy := basePolicy()
	level, audit := classify(policy, 2510)
	if level != LevelLow {
		t.Fatalf("2510bp should fall to lower tier inside tolerance, got %s", level)
	}
	if len(audit.Boundaries) == 0 || audit.Boundaries[0].ToleranceBP != 25 || audit.Boundaries[0].EffectiveBoundaryBP != 2525 {
		t.Fatalf("boundary audit = %+v", audit)
	}
	level, _ = classify(policy, 2526)
	if level != LevelMedium {
		t.Fatalf("2526bp should move to MEDIUM, got %s", level)
	}
}

func TestFastModeCannotPassWithExcludedObservations(t *testing.T) {
	policy := basePolicy()
	policy.FastSampleLimit = 1
	batch := domain.Batch{
		ID: "fast", Service: "svc", ChangeType: domain.ChangeConfig, BatchSize: 1,
		Baseline: domain.Baseline{TimestampMS: 1700000000000, ErrorRatePPM: 0, P90LatencyMS: 100, SuccessRatePPM: 1000000},
	}
	observations := make([]StoreObservation, 0)
	for seq := int64(1); seq <= 3; seq++ {
		ts := 1700000100000 + seq*1000
		observations = append(observations,
			StoreObservation{Observation: domain.Observation{Seq: seq * 10, TimestampMS: ts, MetricType: domain.MetricErrorRate, SampleCount: 5, SuccessCount: 5}},
			StoreObservation{Observation: domain.Observation{Seq: seq*10 + 1, TimestampMS: ts, MetricType: domain.MetricLatencyMS, SampleCount: 5, LatencySamplesMS: []int64{100, 101, 102, 103, 104}}},
			StoreObservation{Observation: domain.Observation{Seq: seq*10 + 2, TimestampMS: ts, MetricType: domain.MetricSuccessRate, SampleCount: 5, SuccessCount: 5}},
		)
	}
	evaluation, err := Compute(policy, batch, observations, SnapshotMeta{AsOfCommitSeq: 1, ObservationRevision: 3}, ModeFast)
	if err != nil {
		t.Fatal(err)
	}
	if evaluation.Decision.Action == ActionPass || !evaluation.Decision.SafetyGuardApplied {
		t.Fatalf("fast pass guard failed: %+v", evaluation.Decision)
	}
	if evaluation.Sampling.ExcludedReports == 0 || evaluation.Sampling.PassAllowed {
		t.Fatalf("sampling guard metadata missing: %+v", evaluation.Sampling)
	}
}
