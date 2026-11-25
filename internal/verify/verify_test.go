package verify

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestFullAcceptanceReport(t *testing.T) {
	if testing.Short() {
		t.Skip("full acceptance is skipped in -short mode")
	}
	temp := t.TempDir()
	exeName := "drift-engine"
	if runtime.GOOS == "windows" {
		exeName += ".exe"
	}
	exe := filepath.Join(temp, exeName)
	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelBuild()
	cmd := exec.CommandContext(buildCtx, "go", "build", "-o", exe, "../../cmd/drift-engine")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("build CLI: %v", err)
	}

	workDir := filepath.Join(temp, "work")
	reportPath := filepath.Join(temp, "report.json")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	report, err := Run(ctx, Options{Seed: 245245, WorkDir: workDir, ExePath: exe, ReportPath: reportPath})
	if err != nil {
		t.Fatal(err)
	}
	if !report.OK {
		for _, check := range report.Checks {
			if !check.OK {
				t.Errorf("check %s failed: %s", check.Name, check.Error)
			}
		}
		t.Fatal("acceptance report was not ok")
	}
	if len(report.Checks) != 9 {
		t.Fatalf("expected 9 checks, got %d", len(report.Checks))
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(reportPath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(reportPath); err != nil {
		t.Fatalf("report path was not written by test run: %v", err)
	}
}
