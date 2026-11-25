package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

type Step struct {
	Name       string `json:"name"`
	OK         bool   `json:"ok"`
	HTTPStatus int    `json:"httpStatus,omitempty"`
	Code       string `json:"code,omitempty"`
	Expected   string `json:"expected,omitempty"`
	Message    string `json:"message,omitempty"`
	Observed   any    `json:"observed,omitempty"`
}

type Report struct {
	OK             bool           `json:"ok"`
	StartedAt      string         `json:"startedAt"`
	FinishedAt     string         `json:"finishedAt,omitempty"`
	Workdir        string         `json:"workdir"`
	Database       string         `json:"database"`
	PlatformBinary string         `json:"platformBinary"`
	BaseURL        string         `json:"baseUrl,omitempty"`
	Steps          []Step         `json:"steps"`
	Crash          map[string]any `json:"crash,omitempty"`
	Recovery       map[string]any `json:"recovery,omitempty"`
	Fatal          string         `json:"fatal,omitempty"`
}

type client struct {
	base string
	hc   *http.Client
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func main() {
	platformBin := flag.String("platform-bin", "", "path to built deployverify platform binary")
	workdir := flag.String("workdir", "artifacts/e2e-runtime", "fresh E2E runtime directory")
	out := flag.String("out", "artifacts/results/e2e-result.json", "JSON report output path")
	flag.Parse()

	report, err := execute(*platformBin, *workdir)
	if err != nil {
		report.Fatal = err.Error()
	}
	report.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if *out != "" {
		if mkErr := os.MkdirAll(filepath.Dir(*out), 0o755); mkErr == nil {
			data, _ := json.MarshalIndent(report, "", "  ")
			_ = os.WriteFile(*out, data, 0o644)
		}
	}
	data, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(data))
	if err != nil || !report.OK {
		os.Exit(1)
	}
}

func execute(platformBin, workdir string) (Report, error) {
	report := Report{StartedAt: time.Now().UTC().Format(time.RFC3339Nano), Workdir: workdir, Steps: []Step{}}
	if err := os.RemoveAll(workdir); err != nil {
		return report, err
	}
	if err := os.MkdirAll(workdir, 0o755); err != nil {
		return report, err
	}
	dbPath := filepath.Join(workdir, "data", "e2e.db")
	report.Database = dbPath

	binPath, err := ensurePlatformBinary(platformBin, workdir)
	if err != nil {
		return report, err
	}
	report.PlatformBinary = binPath

	addr, err := freeAddr()
	if err != nil {
		return report, err
	}
	cmd, logs, err := startPlatform(binPath, addr, dbPath, workdir)
	if err != nil {
		return report, err
	}
	defer killQuiet(cmd)
	baseURL := "http://" + addr
	report.BaseURL = baseURL
	c := &client{base: baseURL, hc: &http.Client{Timeout: 20 * time.Second}}
	if err := waitHealthy(c); err != nil {
		return report, fmt.Errorf("platform did not become healthy: %w; logs:\n%s", err, logs.String())
	}

	runFunctionalChecks(&report, c, workdir)
	restartCmd, restartLogs, crashInfo, err := runCrashCheck(binPath, dbPath, workdir, cmd, logs, &report)
	if err != nil {
		report.Steps = append(report.Steps, Step{Name: "force_kill_and_restart", OK: false, Message: err.Error()})
	} else {
		defer killQuiet(restartCmd)
		report.Crash = crashInfo
		restartedAddr, _ := crashInfo["restartAddr"].(string)
		restartClient := &client{base: "http://" + restartedAddr, hc: &http.Client{Timeout: 10 * time.Second}}
		if err := waitHealthy(restartClient); err != nil {
			report.Steps = append(report.Steps, Step{Name: "restart_health", OK: false, Message: err.Error()})
		} else {
			crashInfo["startupRecoveryLog"] = tail(restartLogs.String(), 2000)
			recovery := verifyRecovery(&report, restartClient, crashInfo)
			report.Recovery = recovery
		}
	}

	report.OK = true
	for _, step := range report.Steps {
		if !step.OK {
			report.OK = false
			break
		}
	}
	return report, nil
}

func ensurePlatformBinary(platformBin, workdir string) (string, error) {
	if platformBin != "" {
		abs, err := filepath.Abs(platformBin)
		return abs, err
	}
	exeName := "platform"
	if runtime.GOOS == "windows" {
		exeName += ".exe"
	}
	output := filepath.Join(workdir, "bin", exeName)
	if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
		return "", err
	}
	cmd := exec.Command("go", "build", "-o", output, "./cmd/platform")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOSUMDB=off")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("build platform: %w", err)
	}
	abs, err := filepath.Abs(output)
	return abs, err
}

func freeAddr() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	return addr, nil
}

func startPlatform(bin, addr, dbPath, workdir string) (*exec.Cmd, *syncBuffer, error) {
	cmd := exec.Command(bin, "-addr", addr, "-db", dbPath, "-workdir", workdir)
	logs := &syncBuffer{}
	cmd.Stdout = logs
	cmd.Stderr = logs
	if err := cmd.Start(); err != nil {
		return nil, nil, err
	}
	return cmd, logs, nil
}

func killQuiet(cmd *exec.Cmd) {
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}
}

func waitHealthy(c *client) error {
	deadline := time.Now().Add(15 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		status, body, err := c.do(http.MethodGet, "/healthz", nil)
		if err == nil && status == 200 && body != nil {
			return nil
		}
		last = fmt.Errorf("status=%d err=%v body=%v", status, err, body)
		time.Sleep(100 * time.Millisecond)
	}
	return last
}

func runFunctionalChecks(report *Report, c *client, workdir string) {
	sampleDir := filepath.Join(workdir, "samples")
	_, _ = os.Stat(filepath.Join(sampleDir, "snapshot-v1.json"))

	status, body := postFile(c, "/api/snapshots", filepath.Join(sampleDir, "snapshot-invalid.json"))
	report.Steps = append(report.Steps, expectCode("invalid_spec", status, body, 422, "E_SPEC_INVALID"))

	status, body = postFile(c, "/api/snapshots", filepath.Join(sampleDir, "snapshot-v1.json"))
	report.Steps = append(report.Steps, expectCode("snapshot_v1", status, body, 201, "OK"))

	var wg sync.WaitGroup
	var mu sync.Mutex
	codes := map[string]int{}
	for _, name := range []string{"increment-concurrent-a.json", "increment-concurrent-b.json"} {
		name := name
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, body := postFile(c, "/api/increments", filepath.Join(sampleDir, name))
			code := businessCode(body)
			if status == 201 {
				code = "OK"
			}
			mu.Lock()
			codes[code]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	step := Step{Name: "concurrent_increments_same_base", OK: codes["OK"] == 1 && codes["E_STALE_BASE"] == 1, HTTPStatus: 200, Code: fmt.Sprintf("OK=%d,E_STALE_BASE=%d", codes["OK"], codes["E_STALE_BASE"]), Expected: "one OK and one E_STALE_BASE", Observed: codes}
	report.Steps = append(report.Steps, step)

	status, body = postFile(c, "/api/increments", filepath.Join(sampleDir, "increment-cascade.json"))
	report.Steps = append(report.Steps, expectCode("cascade_increment", status, body, 201, "OK"))

	status, body, _ = c.do(http.MethodGet, "/api/cascade/latest", nil)
	cascade, _ := body["cascade"].(map[string]any)
	affected, _ := cascade["affected"].([]any)
	report.Steps = append(report.Steps, Step{Name: "cascade_downstream_identity", OK: status == 200 && len(affected) >= 3, HTTPStatus: status, Code: "OK", Expected: "identity,gateway,billing affected", Observed: map[string]any{"affectedCount": len(affected)}})

	status, body, _ = c.do(http.MethodGet, "/api/refs?environment=prod&service=identity&configKey=SESSION_TTL", nil)
	refs, _ := body["refs"].([]any)
	report.Steps = append(report.Steps, Step{Name: "config_key_reference_index", OK: status == 200 && len(refs) > 0, HTTPStatus: status, Code: "OK", Expected: "SESSION_TTL references rebuilt artifacts", Observed: map[string]any{"count": len(refs)}})

	status, body = postFile(c, "/api/increments", filepath.Join(sampleDir, "increment-cycle.json"))
	report.Steps = append(report.Steps, expectCode("dependency_cycle_commit", status, body, 201, "OK"))
	status, body, _ = c.do(http.MethodGet, "/api/diagnostics/dependency-cycle?environment=staging", nil)
	cycleStep := expectCode("dependency_cycle_diagnostic", status, body, 422, "E_DEPENDENCY_CYCLE")
	if details, ok := body["error"].(map[string]any)["details"].(map[string]any); ok {
		blocked, _ := details["blocked"].([]any)
		cycleStep.Observed = map[string]any{"blockedCount": len(blocked), "status": details["status"]}
	}
	report.Steps = append(report.Steps, cycleStep)

	status, body, _ = c.do(http.MethodGet, "/api/diffs?baseline=prod&toleranceUnits=0", nil)
	report.Steps = append(report.Steps, expectCode("environment_drift", status, body, 409, "E_ENV_DRIFT"))

	status, body = postJSON(c, "/api/probe-rounds", map[string]any{"allServices": false})
	roundReport, _ := body["report"].(map[string]any)
	report.Steps = append(report.Steps, Step{Name: "empty_round_explicit", OK: status == 200 && roundReport["empty"] == true, HTTPStatus: status, Code: "OK", Expected: "empty=true and QUEUE_EMPTY conclusion", Observed: roundReport["conclusion"]})

	status, body = postJSON(c, "/api/probe-rounds", map[string]any{
		"environment": "dev", "services": []string{"slowprobe"}, "globalConcurrency": 1, "envConcurrency": 1,
		"queueCapacity": 1, "budgetMs": 1000,
	})
	roundReport, _ = body["report"].(map[string]any)
	report.Steps = append(report.Steps, Step{Name: "probe_timeout", OK: status == 200 && number(roundReport["timeoutCount"]) == 1, HTTPStatus: status, Code: "E_PROBE_TIMEOUT", Expected: "timeoutCount=1", Observed: conciseReport(roundReport)})

	status, body = postJSON(c, "/api/probe-rounds", map[string]any{
		"environment": "prod", "globalConcurrency": 1, "envConcurrency": 1, "queueCapacity": 1,
		"budgetMs": 2000, "probePath": "/probe/slow?delay=2s", "probeTimeoutMs": 300,
	})
	report.Steps = append(report.Steps, expectCode("probe_queue_full", status, body, 429, "E_PROBE_QUEUE_FULL"))

	status, body = postJSON(c, "/api/probe-rounds", map[string]any{
		"environment": "prod", "globalConcurrency": 4, "envConcurrency": 4, "queueCapacity": 10,
		"budgetMs": 120, "probeTimeoutMs": 800, "probePath": "/probe/slow?delay=2s",
	})
	roundReport, _ = body["report"].(map[string]any)
	elapsed := number(roundReport["elapsedMs"])
	report.Steps = append(report.Steps, Step{Name: "probe_budget_cutoff", OK: status == 200 && number(roundReport["skippedCount"]) >= 4 && elapsed < 1000, HTTPStatus: status, Code: "E_PROBE_BUDGET_EXCEEDED", Expected: "all in-flight tasks cut off at ~budget", Observed: conciseReport(roundReport)})

	status, body = postJSON(c, "/api/probe-rounds", map[string]any{
		"environment": "prod", "services": []string{"identity"}, "globalConcurrency": 1, "envConcurrency": 1,
		"queueCapacity": 1, "budgetMs": 1000,
	})
	probeOK := status == 200
	_, plansBody, _ := c.do(http.MethodGet, "/api/plans?environment=prod", nil)
	plans, _ := plansBody["plans"].([]any)
	atomic := false
	for _, item := range plans {
		plan, _ := item.(map[string]any)
		if plan["service"] == "identity" && plan["status"] == "success" {
			atomic = true
		}
	}
	report.Steps = append(report.Steps, Step{Name: "probe_plan_atomic_status", OK: probeOK && atomic, HTTPStatus: status, Code: "OK", Expected: "identity plan status=success in same transaction", Observed: map[string]any{"planSuccess": atomic}})
}

func runCrashCheck(binPath, dbPath, workdir string, oldCmd *exec.Cmd, oldLogs *syncBuffer, report *Report) (*exec.Cmd, *syncBuffer, map[string]any, error) {
	crashClient := &client{base: "http://" + strings.TrimPrefix(report.BaseURL, "http://"), hc: &http.Client{Timeout: 45 * time.Second}}
	crashDone := make(chan error, 1)
	go func() {
		_, _, err := crashClient.do(http.MethodPost, "/api/probe-rounds", map[string]any{
			"id": "run-crash-e2e", "environment": "prod", "services": []string{"identity"},
			"globalConcurrency": 1, "envConcurrency": 1, "queueCapacity": 1,
			"budgetMs": 30000, "probeTimeoutMs": 20000, "probePath": "/probe/slow?delay=30s",
		})
		crashDone <- err
	}()

	running := false
	for i := 0; i < 80; i++ {
		status, body, err := crashClient.do(http.MethodGet, "/api/probe-runs/run-crash-e2e", nil)
		if err == nil && status == 200 {
			if run, ok := body["run"].(map[string]any); ok && run["status"] == "running" {
				running = true
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !running {
		return nil, nil, nil, fmt.Errorf("crash probe run did not reach running state")
	}
	if oldCmd.Process == nil {
		return nil, nil, nil, fmt.Errorf("old platform process missing")
	}
	killErr := oldCmd.Process.Kill()
	waitErr := oldCmd.Wait()
	var clientErr error
	select {
	case clientErr = <-crashDone:
	case <-time.After(3 * time.Second):
		clientErr = fmt.Errorf("crash HTTP request did not return after kill")
	}

	restartAddr, err := freeAddr()
	if err != nil {
		return nil, nil, nil, err
	}
	newCmd, newLogs, err := startPlatform(binPath, restartAddr, dbPath, workdir)
	if err != nil {
		return nil, nil, nil, err
	}

	return newCmd, newLogs, map[string]any{
		"runId":           "run-crash-e2e",
		"runningObserved": running,
		"killError":       errString(killErr),
		"waitError":       errString(waitErr),
		"clientError":     errString(clientErr),
		"restartAddr":     restartAddr,
		"oldLogTail":      tail(oldLogs.String(), 1200),
		"restartedCmdPid": newCmd.Process.Pid,
	}, nil
}

func verifyRecovery(report *Report, c *client, crash map[string]any) map[string]any {
	recovery := map[string]any{}
	status, body, err := c.do(http.MethodPost, "/api/admin/recover", map[string]any{})
	if err == nil {
		recovery["adminRecoverStatus"] = status
		recovery["adminRecover"] = body["recovery"]
	}
	_, intentsBody, _ := c.do(http.MethodGet, "/api/intents?status=pending", nil)
	intents, _ := intentsBody["intents"].([]any)
	recovery["pendingIntentCount"] = len(intents)

	_, runBody, _ := c.do(http.MethodGet, "/api/probe-runs/run-crash-e2e", nil)
	run, _ := runBody["run"].(map[string]any)
	recovery["crashedRun"] = run
	runInterrupted := run != nil && run["status"] == "interrupted"
	recovery["runInterrupted"] = runInterrupted
	recovery["startupRecoveryLog"] = crash["startupRecoveryLog"]

	healthStatus, stateBody, healthErr := c.do(http.MethodGet, "/healthz", nil)
	recovery["health"] = map[string]any{"status": healthStatus, "body": stateBody, "error": errString(healthErr)}
	noPending := len(intents) == 0
	report.Steps = append(report.Steps, Step{
		Name: "force_kill_recovery", OK: noPending && runInterrupted,
		HTTPStatus: 200, Code: "RECOVERED", Expected: "zero pending intents and run-crash-e2e=interrupted",
		Observed: map[string]any{"pendingIntentCount": len(intents), "runStatus": run["status"]},
	})
	return recovery
}

func (c *client) do(method, path string, body any) (int, map[string]any, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(data)
	}
	request, err := http.NewRequest(method, c.base+path, reader)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.hc.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return response.StatusCode, nil, err
	}
	decoded := map[string]any{}
	if len(data) > 0 {
		_ = json.Unmarshal(data, &decoded)
	}
	return response.StatusCode, decoded, nil
}

func postJSON(c *client, path string, body any) (int, map[string]any) {
	status, decoded, err := c.do(http.MethodPost, path, body)
	if err != nil {
		return 0, map[string]any{"clientError": err.Error()}
	}
	return status, decoded
}

func postFile(c *client, path, filename string) (int, map[string]any) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return 0, map[string]any{"fileError": err.Error(), "filename": filename}
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return 0, map[string]any{"jsonError": err.Error()}
	}
	return postJSON(c, path, value)
}

func businessCode(body map[string]any) string {
	if body == nil {
		return ""
	}
	if errBody, ok := body["error"].(map[string]any); ok {
		if code, ok := errBody["code"].(string); ok {
			return code
		}
	}
	if code, ok := body["code"].(string); ok {
		return code
	}
	return ""
}

func expectCode(name string, status int, body map[string]any, expectedStatus int, expectedCode string) Step {
	code := businessCode(body)
	ok := status == expectedStatus && code == expectedCode
	message := ""
	if !ok {
		message = fmt.Sprintf("expected HTTP %d/%s, got HTTP %d/%s", expectedStatus, expectedCode, status, code)
	}
	return Step{Name: name, OK: ok, HTTPStatus: status, Code: code, Expected: fmt.Sprintf("HTTP %d %s", expectedStatus, expectedCode), Message: message}
}

func number(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case int:
		return float64(typed)
	default:
		return 0
	}
}

func conciseReport(report map[string]any) map[string]any {
	if report == nil {
		return nil
	}
	return map[string]any{
		"status": report["status"], "code": report["code"], "elapsedMs": report["elapsedMs"],
		"succeededCount": report["succeededCount"], "failedCount": report["failedCount"],
		"timeoutCount": report["timeoutCount"], "skippedCount": report["skippedCount"],
		"peakGlobalActive": report["peakGlobalActive"], "peakEnvActive": report["peakEnvActive"],
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func tail(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return "..." + value[len(value)-max:]
}

var _ = context.Background
