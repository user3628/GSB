package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"vpnbench/internal/model"
	"vpnbench/internal/requests"
)

func TestCLIEndToEndCommands(t *testing.T) {
	root := t.TempDir()
	samples := filepath.Join(root, "samples")
	var stdout, stderr bytes.Buffer
	if code := Main([]string{"generate", "--out", samples}, &stdout, &stderr); code != 0 {
		t.Fatalf("generate exit=%d stderr=%s", code, stderr.String())
	}
	dbPath := filepath.Join(root, "workbench.db")
	reportPath := filepath.Join(root, "report.json")
	stdout.Reset()
	stderr.Reset()
	code := Main([]string{
		"run",
		"--config", filepath.Join(samples, "configs", "workbench.json"),
		"--db", dbPath,
		"--report", reportPath,
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run exit=%d stderr=%s", code, stderr.String())
	}
	if _, err := os.Stat(reportPath); err != nil {
		t.Fatal(err)
	}

	exportedPath := filepath.Join(root, "exported.json")
	stdout.Reset()
	stderr.Reset()
	if code := Main([]string{"report", "--db", dbPath, "--out", exportedPath}, &stdout, &stderr); code != 0 {
		t.Fatalf("report exit=%d stderr=%s", code, stderr.String())
	}
	original, _ := os.ReadFile(reportPath)
	exported, _ := os.ReadFile(exportedPath)
	if !bytes.Equal(original, exported) {
		t.Fatal("report export mismatch")
	}

	stdout.Reset()
	stderr.Reset()
	if code := Main([]string{"correction", "--db", dbPath, "--session", "session-a", "--stream", "flow-a", "--seq", "1"}, &stdout, &stderr); code != 0 {
		t.Fatalf("correction exit=%d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "C_ORDER_NORMALIZED") {
		t.Fatalf("correction output = %s", stdout.String())
	}

	injectPath := filepath.Join(root, "inject.json")
	stdout.Reset()
	stderr.Reset()
	if code := Main([]string{
		"inject",
		"--params", filepath.Join(samples, "anomalies", "no_match.json"),
		"--db", filepath.Join(root, "inject.db"),
		"--out", injectPath,
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("inject exit=%d stderr=%s", code, stderr.String())
	}
	injected, _ := os.ReadFile(injectPath)
	if !bytes.Contains(injected, []byte("E_NO_TUNNEL")) {
		t.Fatal("inject report does not contain E_NO_TUNNEL")
	}

	recoverPath := filepath.Join(root, "recover.json")
	stdout.Reset()
	stderr.Reset()
	if code := Main([]string{"recover", "--db", dbPath, "--out", recoverPath}, &stdout, &stderr); code != 0 {
		t.Fatalf("recover exit=%d stderr=%s", code, stderr.String())
	}
	if _, err := os.Stat(recoverPath); err != nil {
		t.Fatal(err)
	}
}

func TestCLIReportsCodedParseError(t *testing.T) {
	root := t.TempDir()
	policyPath := filepath.Join(root, "bad.json")
	requestPath := filepath.Join(root, "requests.txt")
	_ = os.WriteFile(policyPath, []byte(`[{"tunnel_id":"x","destination_cidr":"not-a-cidr","priority":1,"bandwidth":1,"forwarding_cost":1}]`), 0o644)
	_ = os.WriteFile(requestPath, []byte(requests.FormatBlock(model.Request{
		StreamID: "f", RequestSeq: 1, Source: "10.0.0.1", Destination: "10.0.0.2", PacketBytes: 1,
	})), 0o644)
	var stdout, stderr bytes.Buffer
	code := Main([]string{"run", "--policy", policyPath, "--requests", requestPath, "--db", filepath.Join(root, "x.db"), "--report", filepath.Join(root, "x.json")}, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("exit = %d", code)
	}
	if !strings.Contains(stderr.String(), "E_POLICY_INVALID") || !strings.Contains(stderr.String(), "offset") {
		t.Fatalf("stderr = %s", stderr.String())
	}
}
