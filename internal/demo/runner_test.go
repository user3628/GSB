package demo

import (
	"bytes"
	"context"
	"testing"
)

func TestDeterministicReport(t *testing.T) {
	injections, err := ParseInjections("all")
	if err != nil {
		t.Fatal(err)
	}
	run := func() ([]byte, Report) {
		t.Helper()
		report, err := Run(context.Background(), Options{WorkDir: t.TempDir(), Threshold: 4, Injections: injections})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		data, err := EncodeReport(report)
		if err != nil {
			t.Fatalf("EncodeReport: %v", err)
		}
		return data, report
	}
	first, report := run()
	second, _ := run()
	if !bytes.Equal(first, second) {
		t.Fatalf("report bytes differ across identical runs")
	}
	if !report.PathParity.ConclusionBytesIdentical || !report.PathParity.CodesEqual || !report.PathParity.QuotaDeductionsEqual {
		t.Fatalf("path parity failed: %+v", report.PathParity)
	}
	hasContention := false
	for _, code := range report.Contention.CodesSorted {
		if code == "E_QUOTA_CONTENTION" {
			hasContention = true
		}
	}
	if !report.Contention.ContendedObserved || !hasContention {
		t.Fatalf("contention scenario failed: %+v", report.Contention)
	}
	if report.SnapshotGap == nil || report.SnapshotGap.DetectedCode != "E_SNAPSHOT_VERSION" || report.SnapshotGap.FallbackVersion != 3 {
		t.Fatalf("snapshot gap scenario failed: %+v", report.SnapshotGap)
	}
	expected := map[string]string{
		"signature": "E_TOKEN_INVALID", "replay": "E_TOKEN_REUSE",
		"quota_overflow": "E_QUOTA_OVERFLOW", "snapshot_gap": "E_SNAPSHOT_VERSION",
		"contention": "E_QUOTA_CONTENTION",
	}
	for _, result := range report.InjectionResults {
		if !result.Observed || string(result.Code) != expected[result.Name] {
			t.Fatalf("unexpected injection result: %+v", result)
		}
	}
}
