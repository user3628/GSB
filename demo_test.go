package prefixcache

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestRunDemoDeterministicAndCoversRequiredScenarios(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	first, err := RunDemo(ctx, filepath.Join(t.TempDir(), "one.db"), DefaultSeed)
	if err != nil {
		t.Fatalf("first demo: %v", err)
	}
	second, err := RunDemo(ctx, filepath.Join(t.TempDir(), "two.db"), DefaultSeed)
	if err != nil {
		t.Fatalf("second demo: %v", err)
	}
	firstJSON, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("demo is not deterministic\nfirst=%s\nsecond=%s", firstJSON, secondJSON)
	}

	if first.Outcomes.BaseDecision != DecisionExact {
		t.Fatalf("base decision=%s", first.Outcomes.BaseDecision)
	}
	if first.Outcomes.NearPrefixDecision != DecisionMerged || first.Outcomes.NearPrefixMargin < 0 {
		t.Fatalf("near prefix outcome=%s margin=%f", first.Outcomes.NearPrefixDecision, first.Outcomes.NearPrefixMargin)
	}
	if first.Outcomes.UnrelatedDecision != DecisionNotReusable {
		t.Fatalf("unrelated decision=%s", first.Outcomes.UnrelatedDecision)
	}
	if first.Outcomes.InitialBudgetEvictions == 0 {
		t.Fatal("demo did not exercise occupancy-budget eviction")
	}
	if first.Recovery.InterruptedCode != CodeInterrupted || first.Recovery.VersionAfterInterrupt != 1 || first.Recovery.RetriedVersion != 2 {
		t.Fatalf("unexpected recovery record: %+v", first.Recovery)
	}
	if first.Recovery.HTTPHealth != "healthy" || first.Recovery.HTTPShutdown != "stopping" {
		t.Fatalf("HTTP graceful stop not exercised: %+v", first.Recovery)
	}
	if !first.Recovery.FingerprintStable || first.Recovery.ReopenVersion != first.FinalVersion {
		t.Fatalf("reopen consistency failed: %+v final=%d", first.Recovery, first.FinalVersion)
	}
}
