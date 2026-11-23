package app

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"vpnbench/internal/config"
	"vpnbench/internal/model"
	"vpnbench/internal/samplegen"
)

func generatedSamples(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "samples")
	if _, err := samplegen.Generate(dir); err != nil {
		t.Fatal(err)
	}
	return dir
}

func configSpec(t *testing.T, samplesDir string) RunSpec {
	t.Helper()
	spec, err := LoadRunConfig(filepath.Join(samplesDir, "configs", "workbench.json"))
	if err != nil {
		t.Fatal(err)
	}
	return spec
}

func TestRunIsByteDeterministicAndReportRoundTrips(t *testing.T) {
	ctx := context.Background()
	samplesDir := generatedSamples(t)
	spec := configSpec(t, samplesDir)
	first, err := Run(ctx, spec, filepath.Join(t.TempDir(), "first.db"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := Run(ctx, spec, filepath.Join(t.TempDir(), "second.db"))
	if err != nil {
		t.Fatal(err)
	}
	firstBytes, err := first.Report.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := second.Report.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstBytes, secondBytes) {
		t.Fatal("repeated runs are not byte-identical")
	}
	if first.Report.Clock.Marker != "T_MONO_FIXED_250US_V1" {
		t.Fatalf("clock marker = %+v", first.Report.Clock)
	}
	if len(first.Report.Sessions) != 2 || first.Report.Sessions[0].SessionID != "session-a" || first.Report.Sessions[1].SessionID != "session-b" {
		t.Fatalf("sessions not stable sorted: %+v", first.Report.Sessions)
	}
	for _, session := range first.Report.Sessions {
		if session.Consistency.Total != session.Consistency.Consistent || len(session.Consistency.Mismatches) != 0 {
			t.Fatalf("inconsistent session: %+v", session.Consistency)
		}
		if session.LatencyMarker != "T_MONO_FIXED_250US_V1" {
			t.Fatalf("session latency marker = %s", session.LatencyMarker)
		}
	}
	if first.Report.Concurrency.PeakInFlight != 2 || first.Report.Concurrency.PeakQueued != 0 {
		t.Fatalf("concurrency metrics = %+v", first.Report.Concurrency)
	}

	databasePath := filepath.Join(t.TempDir(), "roundtrip.db")
	run, err := Run(ctx, spec, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	exported, err := ReportFromDB(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	runBytes, _ := run.Report.Bytes()
	exportedBytes, _ := exported.Bytes()
	if !bytes.Equal(runBytes, exportedBytes) {
		t.Fatal("report --db export differs from live report")
	}
}

func TestRunQueuesSessionsAndKeepsThemIsolated(t *testing.T) {
	samplesDir := generatedSamples(t)
	single := filepath.Join(samplesDir, "requests", "single.txt")
	base := filepath.Join(samplesDir, "requests", "base.txt")
	spec := RunSpec{
		PolicyPath:       filepath.Join(samplesDir, "policies", "base.json"),
		ConcurrencyLimit: 2,
		LatencyClock:     "fixed",
		DefaultBudget:    64,
		Sessions: []config.SessionSpec{
			{SessionID: "budgeted", RequestsFile: base, RequestBudget: 2},
			{SessionID: "healthy-1", RequestsFile: single, RequestBudget: 64},
			{SessionID: "healthy-2", RequestsFile: single, RequestBudget: 64},
		},
	}
	run, err := Run(context.Background(), spec, filepath.Join(t.TempDir(), "queued.db"))
	if err != nil {
		t.Fatal(err)
	}
	if run.Report.Concurrency.PeakInFlight != 2 || run.Report.Concurrency.PeakQueued != 1 || run.Report.Concurrency.Admitted != 3 {
		t.Fatalf("concurrency metrics = %+v", run.Report.Concurrency)
	}
	if len(run.Report.Sessions) != 3 {
		t.Fatalf("sessions = %d", len(run.Report.Sessions))
	}
	if run.Report.Sessions[0].SessionID != "budgeted" || run.Report.Sessions[0].Status != "TERMINATED_REQUEST_BUDGET" {
		t.Fatalf("budgeted session = %+v", run.Report.Sessions[0])
	}
	if len(run.Report.Sessions[0].Requests) != 2 {
		t.Fatalf("processed requests were not preserved: %+v", run.Report.Sessions[0])
	}
	foundBudget := false
	for _, occurrence := range run.Report.Sessions[0].BusinessCodes {
		if occurrence.Code == model.CodeRequestBudget {
			foundBudget = true
		}
	}
	if !foundBudget {
		t.Fatalf("missing request budget code: %+v", run.Report.Sessions[0].BusinessCodes)
	}
	for _, session := range run.Report.Sessions[1:] {
		if session.Status != "FINALIZED" || session.Consistency.Total != session.Consistency.Consistent {
			t.Fatalf("healthy session affected: %+v", session)
		}
	}
	for _, outcome := range run.Outcomes {
		for _, entry := range outcome.ErrorLedger {
			if entry.SessionID != outcome.SessionID {
				t.Fatalf("ledger leaked across sessions: %+v in %s", entry, outcome.SessionID)
			}
		}
	}
}
