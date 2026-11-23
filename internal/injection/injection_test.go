package injection

import (
	"context"
	"testing"

	"internalpki/workbench/internal/model"
)

func TestFourInjectedAnomaliesAreIsolatedAndAccounted(t *testing.T) {
	result, err := Verify(context.Background(), t.TempDir(), "")
	if err != nil {
		t.Fatalf("verify injections: %v", err)
	}
	if err := RequirePassed(result); err != nil {
		t.Fatalf("injection verification failed: %v", err)
	}
	if !result.Report.Summary.FullyAccounted {
		t.Fatal("report is not fully accounted")
	}
	if result.Report.Summary.Completed+result.Report.Summary.Failed+result.Report.Summary.Degraded != result.Report.Summary.Total {
		t.Fatalf("summary counts do not add up: %+v", result.Report.Summary)
	}

	byPath := make(map[string]model.SampleResult)
	for _, sample := range result.Report.Samples {
		byPath[sample.Path] = sample
	}
	expected := map[string]string{
		"anomaly/missing-intermediate": model.CodeChainBroken,
		"anomaly/sni-mismatch":         model.CodeHostnameMismatch,
		"anomaly/expired":              model.CodeExpired,
		"anomaly/untrusted-root":       model.CodeChainUntrusted,
	}
	for path, code := range expected {
		if got := byPath[path].BusinessCode; got != code {
			t.Errorf("%s: got %s, want %s", path, got, code)
		}
	}
	for path, sample := range byPath {
		if len(path) >= 7 && path[:7] == "normal/" && sample.Outcome != model.OutcomePassed {
			t.Errorf("normal control %s was polluted: %s", path, sample.BusinessCode)
		}
	}
}
