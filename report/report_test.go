package report_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sandboxpolicy/engine"
	"sandboxpolicy/policy"
	"sandboxpolicy/report"
	"sandboxpolicy/snapshot"
)

func TestReportIsByteStableAndSorted(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	store, err := snapshot.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	baseline, err := policy.ParsePolicyFile(filepath.Join("..", "samples", "policies", "baseline.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Initialize(ctx, baseline); err != nil {
		t.Fatal(err)
	}
	manager := engine.NewManager(store, engine.Config{MaxInflight: 2, QueueTimeout: 2 * time.Second})
	change, err := policy.ParseChangeSetFile(filepath.Join("..", "samples", "changes", "01-add-cache.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Apply(ctx, change); err != nil {
		t.Fatal(err)
	}
	opts := report.Options{Probe: true}
	first, err := report.Build(ctx, store, baseline.Namespace, 2, opts)
	if err != nil {
		t.Fatal(err)
	}
	second, err := report.Build(ctx, store, baseline.Namespace, 2, opts)
	if err != nil {
		t.Fatal(err)
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
		t.Fatal("report bytes changed across identical runs")
	}
	if first.Probe == nil || first.Probe.Summary.Matched != 10 {
		t.Fatalf("probe report = %+v", first.Probe)
	}
	for i := 1; i < len(first.Probe.Results); i++ {
		if first.Probe.Results[i-1].RuleID > first.Probe.Results[i].RuleID {
			t.Fatalf("probe results are not sorted")
		}
	}
	for i := 1; i < len(first.AffectedRules); i++ {
		if first.AffectedRules[i-1].RuleID > first.AffectedRules[i].RuleID {
			t.Fatalf("affected rules are not sorted")
		}
	}
	if len(first.Chain) != 2 {
		t.Fatalf("chain = %+v", first.Chain)
	}
}

func TestReportBytesAreStableAcrossIndependentStores(t *testing.T) {
	firstBytes, _ := buildDemoReport(t)
	secondBytes, _ := buildDemoReport(t)
	if !bytes.Equal(firstBytes, secondBytes) {
		t.Fatalf("independent identical stores produced different report bytes\nfirst=%x\nsecond=%x", firstBytes, secondBytes)
	}
}

func TestWriteFileReplacesExistingReport(t *testing.T) {
	_, result := buildDemoReport(t)
	path := filepath.Join(t.TempDir(), "report.json")
	if err := report.WriteFile(path, result); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := report.WriteFile(path, result); err != nil {
		t.Fatalf("replace existing report: %v", err)
	}
	second, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("rewritten report changed bytes")
	}
}

func buildDemoReport(t *testing.T) ([]byte, *report.Report) {
	t.Helper()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	store, err := snapshot.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	baseline, err := policy.ParsePolicyFile(filepath.Join("..", "samples", "policies", "baseline.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Initialize(ctx, baseline); err != nil {
		t.Fatal(err)
	}
	manager := engine.NewManager(store, engine.Config{MaxInflight: 2, QueueTimeout: 2 * time.Second})
	change, err := policy.ParseChangeSetFile(filepath.Join("..", "samples", "changes", "01-add-cache.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Apply(ctx, change); err != nil {
		t.Fatal(err)
	}
	result, err := report.Build(ctx, store, baseline.Namespace, 2, report.Options{Probe: true})
	if err != nil {
		t.Fatal(err)
	}
	data, err := report.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return data, result
}
