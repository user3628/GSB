package abeval

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

var (
	cliBinaryOnce sync.Once
	cliBinaryPath string
	cliBinaryErr  error
)

func buildCLITestBinary(t *testing.T) string {
	t.Helper()
	cliBinaryOnce.Do(func() {
		root, err := os.Getwd()
		if err != nil {
			cliBinaryErr = err
			return
		}
		directory, err := os.MkdirTemp("", "abeval-cli-test-*")
		if err != nil {
			cliBinaryErr = err
			return
		}
		binaryPath := filepath.Join(directory, "abeval")
		build := exec.Command("go", "build", "-o", binaryPath, "./cmd/abeval")
		build.Dir = root
		output, err := build.CombinedOutput()
		if err != nil {
			cliBinaryErr = fmt.Errorf("build CLI: %w\n%s", err, output)
			return
		}
		cliBinaryPath = binaryPath
	})
	if cliBinaryErr != nil {
		t.Fatal(cliBinaryErr)
	}
	return cliBinaryPath
}

func TestCLIForceSIGKILLRecoveryByteIdentical(t *testing.T) {
	binaryPath := buildCLITestBinary(t)

	workDir := t.TempDir()
	baselineDB := filepath.Join(workDir, "baseline.db")
	killedDB := filepath.Join(workDir, "killed.db")
	baselineReport := filepath.Join(workDir, "baseline-report.json")
	recoveredReport := filepath.Join(workDir, "recovered-report.json")

	runCLI(t, binaryPath, "import", "--db", baselineDB, "--scenario", ScenarioRollout, "--batch", "batch-cli")
	runCLI(t, binaryPath, "import", "--db", killedDB, "--scenario", ScenarioRollout, "--batch", "batch-cli")
	runCLI(t, binaryPath, "run", "--db", baselineDB, "--batch", "batch-cli", "--writer", "baseline")
	runCLI(t, binaryPath, "report", "--db", baselineDB, "--batch", "batch-cli", "--out", baselineReport)

	crashCommand := exec.Command(binaryPath, "run", "--db", killedDB, "--batch", "batch-cli",
		"--writer", "crash", "--crash-after-stage", "1")
	crashOutput, crashErr := crashCommand.CombinedOutput()
	if crashErr == nil {
		t.Fatalf("crash command unexpectedly exited cleanly\n%s", crashOutput)
	}

	statusOutput := runCLI(t, binaryPath, "status", "--db", killedDB, "--batch", "batch-cli")
	if !bytes.Contains(statusOutput, []byte(`"current_stage": 1`)) {
		t.Fatalf("status after SIGKILL does not show committed stage 1:\n%s", statusOutput)
	}

	runCLI(t, binaryPath, "run", "--db", killedDB, "--batch", "batch-cli", "--writer", "recovery")
	runCLI(t, binaryPath, "report", "--db", killedDB, "--batch", "batch-cli", "--out", recoveredReport)

	baselineBytes, err := os.ReadFile(baselineReport)
	if err != nil {
		t.Fatal(err)
	}
	recoveredBytes, err := os.ReadFile(recoveredReport)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(baselineBytes, recoveredBytes) {
		t.Fatalf("CLI baseline and SIGKILL recovery reports differ:\nbaseline:\n%s\nrecovered:\n%s",
			baselineBytes, recoveredBytes)
	}
}

func TestCLIGracefulStopFlag(t *testing.T) {
	binaryPath := buildCLITestBinary(t)

	databasePath := filepath.Join(t.TempDir(), "graceful.db")
	runCLI(t, binaryPath, "import", "--db", databasePath, "--scenario", ScenarioRecovery, "--batch", "graceful-cli")
	output := runCLI(t, binaryPath, "run", "--db", databasePath, "--batch", "graceful-cli", "--stop-after-stage", "1")
	if !bytes.Contains(output, []byte(`"graceful_stopped": true`)) {
		t.Fatalf("run output = %s, want graceful stop", output)
	}
	status := runCLI(t, binaryPath, "status", "--db", databasePath, "--batch", "graceful-cli")
	if !bytes.Contains(status, []byte(`"status": "graceful_stopped"`)) {
		t.Fatalf("status = %s, want graceful_stopped", status)
	}
}

func runCLI(t *testing.T, binaryPath string, args ...string) []byte {
	t.Helper()
	command := exec.Command(binaryPath, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("abeval %v: %v\n%s", args, err, output)
	}
	return output
}
