package model

import (
	"encoding/json"
	"testing"
	"time"
)

func validPolicy() Policy {
	return Policy{
		Version:     1,
		GeneratedAt: time.Date(2026, 11, 26, 0, 0, 0, 0, time.UTC),
		Channels: map[string]ChannelPolicy{
			"cpu": {TargetPermille: 1, BiasCorrection: 2},
		},
		Services: map[string]ServicePolicy{
			"svc": {ReservoirCapacity: 3, ExactLimit: 1, BiasCorrection: 1},
		},
		Stratification:           Stratification{DepthPrefixes: []int{3, 1, 3}, Separator: ">"},
		DefaultReservoirCapacity: 4,
		DefaultExactLimit:        2,
		RoundTimeBudgetMS:        10,
		RankingRelativeTolerance: 0.01,
		ReplacementRule:          ReplacementRule,
	}
}

func TestPolicyNormalizationAndRateBounds(t *testing.T) {
	policy := validPolicy()
	if err := policy.Normalize(); err != nil {
		t.Fatal(err)
	}
	if len(policy.Stratification.DepthPrefixes) != 2 || policy.Stratification.DepthPrefixes[0] != 1 || policy.Stratification.DepthPrefixes[1] != 3 {
		t.Fatalf("depth prefixes not normalized: %+v", policy.Stratification.DepthPrefixes)
	}
	if policy.Version != 1 || policy.GeneratedAt.Location() == nil {
		t.Fatalf("policy identity lost: %+v", policy)
	}
	for _, rate := range []int{0, 1001, -1} {
		bad := validPolicy()
		bad.Channels["cpu"] = ChannelPolicy{TargetPermille: rate, BiasCorrection: 1}
		if err := bad.Normalize(); err == nil {
			t.Fatalf("target rate %d should fail validation", rate)
		}
	}
	for _, rate := range []int{1, 1000} {
		ok := validPolicy()
		ok.Channels["cpu"] = ChannelPolicy{TargetPermille: rate, BiasCorrection: 1}
		if err := ok.Normalize(); err != nil {
			t.Fatalf("target rate %d should be valid: %v", rate, err)
		}
	}
}

func TestSampleValidationAndChannelIndependentContentHash(t *testing.T) {
	chunk := SampleChunk{
		BatchID: "b",
		Samples: []SampleInput{{
			ID:          "s",
			TimestampMS: 1,
			Service:     "svc",
			Frames:      []Frame{{ID: "root", Name: "root"}},
			Weight:      2,
			Channel:     "cpu",
		}},
	}
	if err := chunk.Normalize(); err != nil {
		t.Fatal(err)
	}
	firstHash := chunk.Samples[0].ContentHash()
	chunk.Samples[0].Channel = "wall"
	if got := chunk.Samples[0].ContentHash(); got != firstHash {
		t.Fatal("delivery channel must not change sample content identity")
	}
	chunk.Samples[0].Weight = 0
	if err := chunk.Normalize(); err == nil {
		t.Fatal("zero weight should be rejected")
	}
	chunk.Samples[0].Weight = 1
	chunk.Samples[0].Frames = nil
	if err := chunk.Normalize(); err == nil {
		t.Fatal("empty stack should be rejected")
	}

	var frame Frame
	if err := json.Unmarshal([]byte(`{"id":"leaf","file":"x.go","line":7}`), &frame); err != nil {
		t.Fatal(err)
	}
	if err := frame.Normalize(); err != nil || frame.ID != "leaf" || frame.Name != "leaf" {
		t.Fatalf("frame normalization failed: %+v %v", frame, err)
	}
}

func TestIncrementMergeProducesNextVersionAndAffectedChanges(t *testing.T) {
	base := validPolicy()
	if err := base.Normalize(); err != nil {
		t.Fatal(err)
	}
	capacity := 9
	next, err := (PolicyIncrement{
		BaseVersion: 1,
		Services: map[string]ServicePatch{
			"svc": {ReservoirCapacity: &capacity},
		},
	}).Merge(base, time.Date(2026, 11, 26, 1, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if next.Version != 2 || next.Services["svc"].ReservoirCapacity != 9 {
		t.Fatalf("increment merge failed: %+v", next)
	}
	changes := DiffPolicies(base, next)
	if len(changes.ChangedServices) != 1 || changes.ChangedServices[0] != "svc" {
		t.Fatalf("unexpected changes: %+v", changes)
	}
}
