package fixed_test

import (
	"encoding/json"
	"testing"

	"recoveryengine/internal/fixed"
	"recoveryengine/internal/spec"
	"recoveryengine/samples"
)

func TestFixedPointDeterminismAndTolerance(t *testing.T) {
	t.Helper()
	data, err := samples.Read("full-snapshot.json")
	if err != nil {
		t.Fatal(err)
	}
	raw, business := spec.DecodeSnapshot(data)
	if business != nil {
		t.Fatal(business)
	}
	normalized, business := spec.NormalizeSnapshot(raw, true)
	if business != nil {
		t.Fatal(business)
	}
	first := fixed.Compute(normalized.Snapshot)
	second := fixed.Compute(normalized.Snapshot)
	if first.Digest != second.Digest {
		t.Fatalf("digest changed across identical calculations: %s != %s", first.Digest, second.Digest)
	}
	firstJSON, _ := json.Marshal(first)
	secondJSON, _ := json.Marshal(second)
	if string(firstJSON) != string(secondJSON) {
		t.Fatal("fixed-point result is not byte-stable")
	}
	if first.ScoreMicro != 663333 {
		t.Fatalf("unexpected score: %d", first.ScoreMicro)
	}
	if first.Decision != fixed.DecisionWithinTolerance || !first.ThresholdMet {
		t.Fatalf("expected tolerance decision, got %s met=%t", first.Decision, first.ThresholdMet)
	}
	if first.ToleranceMicro != 3325 || first.DeltaToThresholdMicro != -1667 {
		t.Fatalf("unexpected tolerance comparison: tolerance=%d delta=%d", first.ToleranceMicro, first.DeltaToThresholdMicro)
	}
}

func TestTieBreakByComponentID(t *testing.T) {
	data, err := samples.Read("full-snapshot.json")
	if err != nil {
		t.Fatal(err)
	}
	raw, business := spec.DecodeSnapshot(data)
	if business != nil {
		t.Fatal(business)
	}
	raw.ConsistencyConfig.ThresholdPermille = 800
	raw.ConsistencyConfig.RelativeTolerancePPM = 0
	normalized, business := spec.NormalizeSnapshot(raw, false)
	if business != nil {
		t.Fatal(business)
	}
	result := fixed.Compute(normalized.Snapshot)
	want := []string{"api", "auth", "database"}
	if len(result.TieComponents) != len(want) {
		t.Fatalf("tie components = %v, want %v", result.TieComponents, want)
	}
	for i := range want {
		if result.TieComponents[i] != want[i] {
			t.Fatalf("tie components = %v, want stable lexicographic %v", result.TieComponents, want)
		}
	}
}
