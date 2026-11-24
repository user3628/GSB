package ledger_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	. "ledgerworkbench"
)

type subprocessServer struct {
	cmd     *exec.Cmd
	baseURL string
	logs    *bytes.Buffer
}

func startSubprocessServer(t *testing.T, binary, dbPath, portFile string) subprocessServer {
	t.Helper()
	logs := &bytes.Buffer{}
	cmd := exec.Command(binary, "--db", dbPath, "--addr", "127.0.0.1:0", "--port-file", portFile)
	cmd.Stdout = logs
	cmd.Stderr = logs
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v\n%s", err, logs.String())
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})

	deadline := time.Now().Add(10 * time.Second)
	var baseURL string
	for time.Now().Before(deadline) {
		if content, err := readFileString(portFile); err == nil && strings.TrimSpace(content) != "" {
			baseURL = strings.TrimSpace(content)
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if baseURL == "" {
		_ = cmd.Process.Kill()
		t.Fatalf("server did not write port file\n%s", logs.String())
	}

	healthy := false
	for time.Now().Before(deadline) {
		response, err := http.Get(baseURL + "/healthz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				healthy = true
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !healthy {
		_ = cmd.Process.Kill()
		t.Fatalf("server did not become healthy\n%s", logs.String())
	}
	return subprocessServer{cmd: cmd, baseURL: baseURL, logs: logs}
}

func readFileString(path string) (string, error) {
	content, err := os.ReadFile(path)
	return string(content), err
}

func TestForcedKillRecoveryDependsOnlyOnSQLiteFacts(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not available for subprocess build")
	}
	outputDir := t.TempDir()
	binary := filepath.Join(outputDir, "ledger-workbench")
	build := exec.Command("go", "build", "-o", binary, "./cmd/ledger-workbench")
	build.Dir = "."
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build server: %v\n%s", err, string(output))
	}

	stateDir := t.TempDir()
	dbPath := filepath.Join(stateDir, "crash.db")
	firstPortFile := filepath.Join(stateDir, "port-1")
	first := startSubprocessServer(t, binary, dbPath, firstPortFile)

	payload := `{"commit_id":"e2e-crash","staged_entry_count":2,"kill_delay_ms":100}`
	response, err := http.Post(first.baseURL+"/v1/admin/crash", "application/json", strings.NewReader(payload))
	if response != nil {
		_ = response.Body.Close()
	}
	_ = err

	exited := make(chan error, 1)
	go func() { exited <- first.cmd.Wait() }()
	select {
	case <-exited:
	case <-time.After(8 * time.Second):
		_ = first.cmd.Process.Kill()
		t.Fatal("server did not terminate after SIGKILL crash request")
	}

	secondPortFile := filepath.Join(stateDir, "port-2")
	second := startSubprocessServer(t, binary, dbPath, secondPortFile)
	response, err = http.Get(second.baseURL + "/v1/recovery")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var envelope struct {
		OK   bool           `json:"ok"`
		Data RecoveryReport `json:"data"`
	}
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	report := envelope.Data
	if report.DiscardedStagedEntries != 2 {
		t.Fatalf("discarded staged entries=%d report=%+v logs=%s",
			report.DiscardedStagedEntries, report, second.logs.String())
	}
	if len(report.UnfinishedIntents) != 2 {
		t.Fatalf("unfinished intents=%d report=%+v", len(report.UnfinishedIntents), report)
	}
	foundMigration := false
	for _, id := range report.RecoveredMigrationIDs {
		if id == "mig-crash-e2e-crash" {
			foundMigration = true
		}
	}
	if !foundMigration {
		t.Fatalf("recovery report lacks durable migration fact: %+v", report)
	}
}
