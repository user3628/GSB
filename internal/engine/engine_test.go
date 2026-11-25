package engine

import (
	"testing"
	"time"

	"profilehotspot/internal/model"
)

func testPolicy() model.Policy {
	policy := model.Policy{
		Version:     1,
		GeneratedAt: time.Date(2026, 11, 26, 0, 0, 0, 0, time.UTC),
		Channels: map[string]model.ChannelPolicy{
			"cpu":  {TargetPermille: 250, BiasCorrection: 4},
			"wall": {TargetPermille: 1000, BiasCorrection: 1},
		},
		Services: map[string]model.ServicePolicy{
			"svc": {ReservoirCapacity: 2, ExactLimit: 0, BiasCorrection: 1},
		},
		Stratification:           model.Stratification{DepthPrefixes: []int{1}, Separator: ">"},
		DefaultReservoirCapacity: 2,
		DefaultExactLimit:        0,
		RoundTimeBudgetMS:        100,
		RankingRelativeTolerance: 0.005,
		ReplacementRule:          model.ReplacementRule,
	}
	_ = policy.Normalize()
	return policy
}

func sample(id string, order int64, channel string, weight int64, frames ...string) SampleRecord {
	framesOut := make([]model.Frame, 0, len(frames))
	for _, frame := range frames {
		framesOut = append(framesOut, model.Frame{ID: frame, Name: frame})
	}
	return SampleRecord{
		ID:           id,
		ContentHash:  "hash-" + id,
		TimestampMS:  order,
		Service:      "svc",
		FirstChannel: channel,
		Frames:       framesOut,
		Weight:       weight,
		SeenOrder:    order,
	}
}

func TestReservoirDeterministicBoundedAndOrderIndependent(t *testing.T) {
	policy := testPolicy()
	samples := []SampleRecord{
		sample("a", 1, "wall", 3, "root", "leaf"),
		sample("b", 2, "wall", 5, "root", "leaf"),
		sample("c", 3, "wall", 2, "root", "leaf"),
		sample("d", 4, "wall", 7, "root", "leaf"),
		sample("e", 5, "wall", 1, "root", "leaf"),
	}
	first := BuildReservoirs(policy, samples)
	shuffled := []SampleRecord{samples[3], samples[0], samples[4], samples[2], samples[1]}
	second := BuildReservoirs(policy, shuffled)
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("expected one reservoir, got %d and %d", len(first), len(second))
	}
	if first[0].SelectedCount != 2 || len(first[0].Members) != 2 {
		t.Fatalf("reservoir grew beyond capacity: %+v", first[0])
	}
	if first[0].SeenCount != 5 {
		t.Fatalf("seen count = %d, want 5", first[0].SeenCount)
	}
	if len(first[0].ReplacedSampleIDs) > first[0].Capacity {
		t.Fatalf("replacement trail is unbounded: %d", len(first[0].ReplacedSampleIDs))
	}
	if first[0].ReplacedTotal+first[0].RejectedTotal != 3 {
		t.Fatalf("replacement accounting = %d, want 3", first[0].ReplacedTotal+first[0].RejectedTotal)
	}
	for i := range first[0].Members {
		if first[0].Members[i].SampleID != second[0].Members[i].SampleID {
			t.Fatalf("selected set depends on arrival order: %v vs %v", first[0].Members, second[0].Members)
		}
	}
}

func TestHotspotUsesCorrectionBiasBoundAndAuditableDifferences(t *testing.T) {
	policy := testPolicy()
	samples := []SampleRecord{
		sample("a", 1, "cpu", 1, "root", "hot"),
		sample("b", 2, "wall", 10, "root", "cold"),
		sample("c", 3, "wall", 2, "root", "cold"),
		sample("d", 4, "wall", 3, "root", "hot"),
	}
	reservoirs := BuildReservoirs(policy, samples)
	report := BuildHotspots(policy, 9, time.Now().UTC(), samples, reservoirs)
	if len(report.Services) != 1 {
		t.Fatalf("services = %d", len(report.Services))
	}
	service := report.Services[0]
	if service.Path.Mode != ModeApproximate || !service.Path.UsedReservoir {
		t.Fatalf("expected explicit approximate mode: %+v", service.Path)
	}
	if service.BiasUpperBound <= 0 || service.BiasUpperBound > 1 {
		t.Fatalf("invalid bias bound: %v", service.BiasUpperBound)
	}
	if len(service.ApproximationBasis.StratificationKeys) == 0 {
		t.Fatal("approximation basis is missing stratification keys")
	}
	for _, frame := range service.Frames {
		if frame.AbsoluteError > frame.BiasUpperBound+1e-12 {
			t.Fatalf("frame %s error %v exceeds bound %v", frame.FrameID, frame.AbsoluteError, frame.BiasUpperBound)
		}
		if frame.ExactShare < frame.ConfidenceInterval.Lower-1e-12 || frame.ExactShare > frame.ConfidenceInterval.Upper+1e-12 {
			t.Fatalf("frame %s exact share %v outside CI [%v,%v]", frame.FrameID, frame.ExactShare, frame.ConfidenceInterval.Lower, frame.ConfidenceInterval.Upper)
		}
	}
}

func TestRankingToleranceAndFrameIDTieBreakAreStable(t *testing.T) {
	build := func(a, b int64) []FrameHotspot {
		return []FrameHotspot{
			{FrameID: "z-frame", FixedEstimate: a, EstimatedShare: float64(a) / float64(fixedScale)},
			{FrameID: "a-frame", FixedEstimate: b, EstimatedShare: float64(b) / float64(fixedScale)},
			{FrameID: "m-frame", FixedEstimate: 900_000_000_000, EstimatedShare: 0.9},
		}
	}
	first := build(100_000_000_000, 100_000_100_000)
	second := build(100_000_100_000, 100_000_000_000)
	rankFrames(first, 5000)
	rankFrames(second, 5000)
	if first[1].FrameID != "a-frame" || first[2].FrameID != "z-frame" {
		t.Fatalf("tie did not break by frame id: %v %v", first[1].FrameID, first[2].FrameID)
	}
	for i := range first {
		if first[i].FrameID != second[i].FrameID {
			t.Fatalf("relative-tolerance ranking changed under in-tolerance perturbation: %v vs %v", first, second)
		}
	}
	conclusion := buildConclusion(first, RankingPolicy{RelativeTolerance: 0.005, TolerancePPM: 5000})
	if conclusion.LeaderFrameID != "m-frame" || len(conclusion.TopTieFrameIDs) != 1 {
		t.Fatalf("unexpected conclusion: %+v", conclusion)
	}
}

func TestWithinToleranceIntegerComparison(t *testing.T) {
	if !withinTolerance(1000, 995, 5000) {
		t.Fatal("0.5% relative tolerance should include 995 vs 1000")
	}
	if withinTolerance(1000, 994, 5000) {
		t.Fatal("0.5% relative tolerance should exclude 994 vs 1000")
	}
}
