package core

import "testing"

func TestSnapshotDigestIsCanonical(t *testing.T) {
	snap := Snapshot{
		Version: 1, GeneratedAt: "2026-11-24T00:00:00Z",
		Config: map[string]float64{"weight_y": 0.25, "weight_x": 0.75, "bias": 0.05},
	}
	first := SnapshotDigest(snap)
	snap.Config = map[string]float64{"bias": 0.05, "weight_x": 0.75, "weight_y": 0.25}
	second := SnapshotDigest(snap)
	if first != second {
		t.Fatalf("map iteration order changed digest: %s != %s", first, second)
	}
	snap.Digest = first
	if err := ValidateSnapshot(snap); err != nil {
		t.Fatalf("valid snapshot rejected: %v", err)
	}
	snap.Digest = "sha256:bad"
	if err := ValidateSnapshot(snap); err == nil || BizErrorOf(err).Code != "E_SNAPSHOT_INVALID" {
		t.Fatalf("bad digest error = %v", err)
	}
}

func TestLegalTransitions(t *testing.T) {
	legal := [][2]string{
		{"", StatusPending},
		{StatusPending, StatusPushed},
		{StatusPending, StatusAborted},
		{StatusPushed, StatusAcked},
		{StatusPushed, StatusAborted},
		{StatusAcked, StatusApplied},
		{StatusAcked, StatusAborted},
	}
	for _, pair := range legal {
		if !CanTransition(pair[0], pair[1]) {
			t.Errorf("expected legal transition %s -> %s", pair[0], pair[1])
		}
	}
	illegal := [][2]string{
		{"", StatusAcked},
		{StatusPending, StatusAcked},
		{StatusApplied, StatusAborted},
		{StatusAborted, StatusPushed},
	}
	for _, pair := range illegal {
		if CanTransition(pair[0], pair[1]) {
			t.Errorf("expected illegal transition %s -> %s", pair[0], pair[1])
		}
	}
}
