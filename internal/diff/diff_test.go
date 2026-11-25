package diff_test

import (
	"testing"

	"deployverify/internal/demo"
	"deployverify/internal/diff"
	"deployverify/internal/model"
)

func TestDiffStableOrderAndDrift(t *testing.T) {
	report, err := diff.Compare(demo.SnapshotV1(), diff.Options{Baseline: "prod", ToleranceUnits: 1})
	if err != nil {
		t.Fatal(err)
	}
	if report.Code != model.CodeEnvDrift || !report.Drift {
		t.Fatalf("code=%s drift=%v, want E_ENV_DRIFT", report.Code, report.Drift)
	}
	if len(report.BehindUnits) < 2 {
		t.Fatalf("behind units = %+v", report.BehindUnits)
	}
	for i := 1; i < len(report.Items); i++ {
		if severityRank(report.Items[i-1].Severity) > severityRank(report.Items[i].Severity) {
			t.Fatalf("items not severity sorted: %+v", report.Items)
		}
	}

	okReport, err := diff.Compare(demo.SnapshotV1(), diff.Options{Baseline: "prod", ToleranceUnits: 10})
	if err != nil {
		t.Fatal(err)
	}
	if okReport.Code != "OK" || okReport.Drift {
		t.Fatalf("expected tolerance to absorb drift, got %+v", okReport)
	}
}

func severityRank(severity string) int {
	switch severity {
	case "critical":
		return 0
	case "high":
		return 1
	case "medium":
		return 2
	default:
		return 3
	}
}
