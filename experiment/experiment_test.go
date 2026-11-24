package experiment_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"example.com/authlab/experiment"
	"example.com/authlab/sample"
)

func runReport(t *testing.T, injection string) *experiment.Report {
	t.Helper()
	report, err := experiment.Run(context.Background(), experiment.Options{
		Injection: injection, WorkDir: t.TempDir(), Concurrency: 6, ContentionWait: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("run experiment %s: %v", injection, err)
	}
	if !report.Summary.Passed {
		t.Fatalf("experiment %s did not pass: %+v", injection, report.Summary)
	}
	return report
}

func TestExperimentReportIsByteDeterministic(t *testing.T) {
	first := runReport(t, "all")
	second := runReport(t, "all")
	firstBytes, err := experiment.Marshal(first)
	if err != nil {
		t.Fatalf("marshal first: %v", err)
	}
	secondBytes, err := experiment.Marshal(second)
	if err != nil {
		t.Fatalf("marshal second: %v", err)
	}
	if !bytes.Equal(firstBytes, secondBytes) {
		t.Fatalf("reports are not byte-identical: first=%d bytes second=%d bytes", len(firstBytes), len(secondBytes))
	}
	if len(first.Cases) != len(experiment.InjectionOrder) {
		t.Fatalf("case count = %d, want %d", len(first.Cases), len(experiment.InjectionOrder))
	}
	if first.BaseTime != sample.BaseTime.Format(time.RFC3339) {
		t.Fatalf("base time = %q", first.BaseTime)
	}
}

func TestEachInjectionIsIndependentlyRunnable(t *testing.T) {
	expected := map[string]string{
		"expired":              "E_TOKEN_INVALID",
		"cross-tenant":         "E_CROSS_TENANT",
		"session-fixation":     "E_SESSION_FIXATION",
		"privilege-escalation": "E_PRIVILEGE_ESCALATION",
		"refresh-race":         "E_TOKEN_REUSE",
	}
	for _, injection := range experiment.InjectionOrder {
		t.Run(injection, func(t *testing.T) {
			report := runReport(t, injection)
			if len(report.Cases) != 1 || report.Cases[0].Injection != injection {
				t.Fatalf("unexpected selected cases: %+v", report.Cases)
			}
			found := false
			for _, step := range report.Cases[0].Steps {
				if step.ActualCode == expected[injection] {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("case %s did not observe %s", injection, expected[injection])
			}
			if injection == "refresh-race" {
				observed := strings.Join(report.Cases[0].ObservedCodes, ",")
				if !strings.Contains(observed, "E_AUTH_CONTENTION") || !strings.Contains(observed, "E_TOKEN_REUSE") {
					t.Fatalf("refresh race observed codes = %s", observed)
				}
			}
			if report.Cases[0].Tenants[0].Version == 0 || report.Cases[0].Tenants[0].Digest == "" {
				t.Fatalf("tenant report did not use actual snapshot metadata")
			}
		})
	}
}
