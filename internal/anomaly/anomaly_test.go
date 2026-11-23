package anomaly

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"vpnbench/internal/model"
	"vpnbench/internal/samplegen"
)

func TestAllFiveAnomalyCasesPass(t *testing.T) {
	samplesDir := filepath.Join(t.TempDir(), "samples")
	if _, err := samplegen.Generate(samplesDir); err != nil {
		t.Fatal(err)
	}
	cases := []string{"no_match", "fixed_overflow", "snapshot_gap", "whole_replacement", "session_race"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			result, err := Run(context.Background(), filepath.Join(samplesDir, "anomalies", name+".json"), filepath.Join(t.TempDir(), "anomaly.db"))
			if err != nil {
				t.Fatal(err)
			}
			if !result.Passed {
				t.Fatalf("case did not pass: %+v", result.Assertions)
			}
			found := false
			for _, code := range result.ExpectedCodes {
				if code == model.CodeNoTunnel || code == model.CodeFixedOverflow || code == model.CodeSnapshotVersion ||
					code == model.CodePolicyReplaced || code == model.CodeSessionConflict || code == model.CodeRequestBudget {
					found = true
				}
			}
			if !found {
				t.Fatalf("unexpected expected codes: %+v", result.ExpectedCodes)
			}
		})
	}
}

func TestAnomalyReportIsByteDeterministic(t *testing.T) {
	samplesDir := filepath.Join(t.TempDir(), "samples")
	if _, err := samplegen.Generate(samplesDir); err != nil {
		t.Fatal(err)
	}
	params := filepath.Join(samplesDir, "anomalies", "session_race.json")
	first, err := Run(context.Background(), params, filepath.Join(t.TempDir(), "one.db"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := Run(context.Background(), params, filepath.Join(t.TempDir(), "two.db"))
	if err != nil {
		t.Fatal(err)
	}
	firstBytes, _ := first.Bytes()
	secondBytes, _ := second.Bytes()
	if !bytes.Equal(firstBytes, secondBytes) {
		t.Fatal("anomaly reports differ across identical runs")
	}
}
