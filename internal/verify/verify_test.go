package verify

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestRunWithFreeEndpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	reportPath := filepath.Join(t.TempDir(), "report.json")
	report, err := Run(ctx, Options{
		SamplesDir:    filepath.Join("..", "..", "samples"),
		DBPath:        filepath.Join(t.TempDir(), "verify.db"),
		OutputPath:    reportPath,
		ResetDB:       true,
		FreeEndpoints: true,
		ForceKill:     false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report == nil || !report.OK {
		for _, check := range report.Checks {
			if !check.OK {
				t.Errorf("check %s failed: %s", check.Name, check.Error)
			}
		}
		t.Fatal("verification report was not successful")
	}
}
