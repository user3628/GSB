package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestCLIForceKillWriteIntentAndRecover(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	temp := t.TempDir()
	exeName := "converge"
	if runtime.GOOS == "windows" {
		exeName = "converge.exe"
	}
	binPath := filepath.Join(temp, exeName)

	build := exec.Command("go", "build", "-o", binPath, "./cmd/converge")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build CLI: %v\n%s", err, output)
	}

	addrFile := filepath.Join(temp, "target.addr")
	target := exec.Command(binPath, "target", "--addr", "127.0.0.1:0", "--addr-file", addrFile)
	targetOutput := &bytes.Buffer{}
	target.Stdout = targetOutput
	target.Stderr = targetOutput
	if err := target.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = target.Process.Kill()
		_, _ = target.Process.Wait()
	})

	endpoint := waitFileContent(t, addrFile, 10*time.Second)
	endpoint = strings.TrimSpace(endpoint)
	if !strings.HasPrefix(endpoint, "http://") {
		t.Fatalf("bad endpoint %q target output: %s", endpoint, targetOutput.String())
	}

	dbPath := filepath.Join(temp, "crash.db")
	snapshot := filepath.Join(root, "samples", "snapshot-v1.json")
	runCLI(t, binPath, root, "commit-full", "--db", dbPath, "--file", snapshot)

	readyFile := filepath.Join(temp, "ready.json")
	crash := exec.Command(binPath, "apply", "--db", dbPath, "--endpoint", endpoint,
		"--batch", "B001-S1", "--crash-after-write-intent", "--ready-file", readyFile)
	crashOutput := &bytes.Buffer{}
	crash.Stdout = crashOutput
	crash.Stderr = crashOutput
	if err := crash.Start(); err != nil {
		t.Fatal(err)
	}
	waitFileContent(t, readyFile, 10*time.Second)
	if err := crash.Process.Kill(); err != nil {
		t.Fatalf("force kill: %v", err)
	}
	_ = crash.Wait()

	recoveryPath := filepath.Join(temp, "recovery.json")
	runCLI(t, binPath, root, "recover", "--db", dbPath, "--out", recoveryPath)
	var recoveryEnvelope struct {
		Recovery struct {
			RolledBackWriteBatches []map[string]any `json:"rolled_back_write_batches"`
		} `json:"recovery"`
	}
	decodeJSON(t, recoveryPath, &recoveryEnvelope)
	if len(recoveryEnvelope.Recovery.RolledBackWriteBatches) != 1 {
		t.Fatalf("expected one rolled back batch, got %+v", recoveryEnvelope.Recovery)
	}

	reapplyPath := filepath.Join(temp, "reapply.json")
	runCLI(t, binPath, root, "apply", "--db", dbPath, "--endpoint", endpoint,
		"--batch", "B001-S1", "--out", reapplyPath)
	var reapplyEnvelope struct {
		OK bool `json:"ok"`
	}
	decodeJSON(t, reapplyPath, &reapplyEnvelope)
	if !reapplyEnvelope.OK {
		t.Fatal("reapply envelope was not ok")
	}
}

func runCLI(t *testing.T, binPath, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command(binPath, args...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v: %v\n%s", binPath, args, err, output)
	}
}

func waitFileContent(t *testing.T, path string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil && len(bytes.TrimSpace(data)) > 0 {
			return string(data)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
	return ""
}

func decodeJSON(t *testing.T, path string, target any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
}
