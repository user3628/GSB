package verify_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"supplyaudit/internal/report"
	"supplyaudit/internal/verify"
)

func TestFaultInjectionChainPassesAndIsDeterministic(t *testing.T) {
	ctx := context.Background()
	firstDir := t.TempDir()
	secondDir := t.TempDir()
	callerState := filepath.Join(firstDir, "state")
	if err := os.MkdirAll(callerState, 0o755); err != nil {
		t.Fatal(err)
	}
	callerDB := filepath.Join(callerState, "audit.db")
	if err := os.WriteFile(callerDB, []byte("caller-owned"), 0o644); err != nil {
		t.Fatal(err)
	}
	first, err := verify.Run(ctx, firstDir)
	if err != nil {
		t.Fatal(err)
	}
	second, err := verify.Run(ctx, secondDir)
	if err != nil {
		t.Fatal(err)
	}
	if !first.AllPassed {
		t.Fatalf("verification failed: %#v", first)
	}
	preserved, err := os.ReadFile(callerDB)
	if err != nil || string(preserved) != "caller-owned" {
		t.Fatalf("verify removed caller-owned audit state: data=%q err=%v", preserved, err)
	}
	if len(first.Scenarios) != 6 {
		t.Fatalf("expected six injected scenarios, got %d", len(first.Scenarios))
	}
	for _, scenario := range first.Scenarios {
		if scenario.SnapshotBefore != 1 || scenario.SnapshotAfter != 1 {
			t.Fatalf("scenario advanced canonical snapshot: %#v", scenario)
		}
	}
	firstBytes, err := report.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := report.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstBytes, secondBytes) {
		t.Fatal("verification report is not byte deterministic")
	}
	firstBaseline, err := os.ReadFile(filepath.Join(firstDir, "baseline-report.json"))
	if err != nil {
		t.Fatal(err)
	}
	secondBaseline, err := os.ReadFile(filepath.Join(secondDir, "baseline-report.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstBaseline, secondBaseline) {
		t.Fatal("baseline report is not byte deterministic")
	}
}
