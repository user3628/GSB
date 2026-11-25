package acceptance

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"voiceline/internal/checkpoint"
	"voiceline/internal/sample"
)

var (
	buildCLIBinaryOnce sync.Once
	cliBinaryPath      string
	cliBuildErr        error
)

func repositoryRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(file))
}

func buildCLIBinary(t *testing.T) string {
	t.Helper()
	buildCLIBinaryOnce.Do(func() {
		dir, err := os.MkdirTemp("", "voiceline-cli-bin")
		if err != nil {
			cliBuildErr = err
			return
		}
		cliBinaryPath = filepath.Join(dir, "voiceline")
		cmd := exec.Command("go", "build", "-o", cliBinaryPath, "./cmd/voiceline")
		cmd.Dir = repositoryRoot()
		cmd.Env = append(os.Environ(), "GOPROXY=off", "GOTOOLCHAIN=local")
		output, err := cmd.CombinedOutput()
		if err != nil {
			cliBuildErr = fmt.Errorf("%w: %s", err, output)
		}
	})
	if cliBuildErr != nil {
		t.Fatal(cliBuildErr)
	}
	return cliBinaryPath
}

func TestForceKillRestartProducesContinuousWaveform(t *testing.T) {
	bin := buildCLIBinary(t)
	work := t.TempDir()
	if _, err := sample.Generate(sample.Options{OutDir: filepath.Join(work, "samples")}); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(work, "samples", "manifest.json")

	continuousOut := filepath.Join(work, "continuous-out")
	continuousDB := filepath.Join(work, "continuous", "cp.db")
	runCLIForTest(t, bin, manifest, continuousOut, continuousDB, "continuous")
	continuousWAV, err := os.ReadFile(filepath.Join(continuousOut, "seg-000.wav"))
	if err != nil {
		t.Fatal(err)
	}

	crashOut := filepath.Join(work, "crash-out")
	crashDB := filepath.Join(work, "crash", "cp.db")
	args := convertArgs(manifest, crashOut, crashDB, "killed", 80*time.Millisecond)
	crashCmd := exec.Command(bin, args...)
	var crashStdout, crashStderr bytes.Buffer
	crashCmd.Stdout = &crashStdout
	crashCmd.Stderr = &crashStderr
	if err := crashCmd.Start(); err != nil {
		t.Fatal(err)
	}

	waitForCommittedBlock(t, crashDB, 15*time.Second)
	if err := crashCmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = crashCmd.Wait()

	recoveryOut := filepath.Join(work, "recovery-out")
	recoveryDB := crashDB
	runCLIForTest(t, bin, manifest, recoveryOut, recoveryDB, "recovery")
	recoveredWAV, err := os.ReadFile(filepath.Join(recoveryOut, "seg-000.wav"))
	if err != nil {
		t.Fatalf("read recovered wav: %v; crash stdout=%s stderr=%s", err, crashStdout.String(), crashStderr.String())
	}
	if !bytes.Equal(recoveredWAV, continuousWAV) {
		t.Fatal("force-kill recovery WAV is not byte-identical to continuous run")
	}
}

func convertArgs(manifest, outDir, dbPath, worker string, delay time.Duration) []string {
	args := []string{
		"convert", "--manifest", manifest, "--segment", "0",
		"--out", outDir, "--db", dbPath, "--parallel=true", "--worker-id", worker,
	}
	if delay > 0 {
		args = append(args, "--block-delay", delay.String())
	}
	return args
}

func runCLIForTest(t *testing.T, bin, manifest, outDir, dbPath, worker string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	args := convertArgs(manifest, outDir, dbPath, worker, 0)
	cmd := exec.CommandContext(ctx, bin, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("cli failed: %v\nstdout=%s\nstderr=%s", err, stdout.String(), stderr.String())
	}
}

func waitForCommittedBlock(t *testing.T, dbPath string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	ctx := context.Background()
	for time.Now().Before(deadline) {
		if _, err := os.Stat(dbPath); err == nil {
			store, err := checkpoint.Open(ctx, dbPath)
			if err == nil {
				next, err := store.NextBlockIndex(ctx, "seg-000")
				_ = store.Close()
				if err == nil && next >= 1 {
					return
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for a committed block before force kill")
}
