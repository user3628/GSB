package verify_test

import (
	"context"
	"testing"

	"protocolfuzzlab/internal/verify"
)

func TestFiveInjectionScenariosPass(t *testing.T) {
	report, err := verify.Run(context.Background(), "", []string{
		"missing-seed", "operator-cycle", "bad-checksum", "snapshot-gap", "concurrent-race",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Scenarios) != 5 {
		t.Fatalf("scenarios=%d", len(report.Scenarios))
	}
	for _, scenario := range report.Scenarios {
		if !scenario.Passed {
			t.Fatalf("scenario %s failed: %#v", scenario.Scenario, scenario)
		}
	}
}
