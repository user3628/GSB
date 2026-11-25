package verify

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"mpckpt/internal/format"
	"mpckpt/internal/model"
	"mpckpt/internal/sample"
)

type Result struct {
	Status          string            `json:"status"`
	StartedAt       string            `json:"started_at"`
	RunDirectory    string            `json:"run_directory"`
	Imports         []map[string]any  `json:"imports"`
	FullCheckpoint  map[string]any    `json:"full_checkpoint"`
	Incremental     map[string]any    `json:"incremental"`
	Eviction        map[string]any    `json:"eviction"`
	Precision       map[string]any    `json:"precision"`
	PrecisionRepeat string            `json:"precision_repeat_checksum"`
	Idempotency     map[string]any    `json:"idempotency"`
	Conflict        map[string]any    `json:"conflicting_request"`
	CrashRecovery   map[string]any    `json:"crash_recovery"`
	FinalState      map[string]any    `json:"final_state"`
	Artifacts       map[string]string `json:"artifacts"`
}

type childServer struct {
	cmd     *exec.Cmd
	logFile *os.File
	port    int
}

func Run(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	dir := fs.String("dir", "./run", "runtime directory (cleaned before verification)")
	port := fs.Int("port", 18081, "loopback HTTP port")
	queueCap := fs.Int("queue-cap", 32, "bounded write queue capacity")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *port <= 0 || *port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}
	if err := os.RemoveAll(*dir); err != nil {
		return fmt.Errorf("clean runtime directory: %w", err)
	}
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		return fmt.Errorf("create runtime directory: %w", err)
	}
	result := Result{
		StartedAt:    time.Now().UTC().Format(time.RFC3339Nano),
		RunDirectory: *dir,
		Artifacts:    map[string]string{},
		Imports:      []map[string]any{},
	}

	binaryManifest := sample.Manifest(2)
	binaryManifest.Format = format.FormatBinary
	binaryBytes, berr := format.Encode(binaryManifest)
	if berr != nil {
		return fmt.Errorf("encode binary sample: %w", berr)
	}
	jsonlManifest := sample.Manifest(1)
	jsonlManifest.Format = format.FormatJSONL
	jsonlBytes, berr := format.Encode(jsonlManifest)
	if berr != nil {
		return fmt.Errorf("encode JSONL sample: %w", berr)
	}
	binaryPath := filepath.Join(*dir, "sample-v2.mpck")
	jsonlPath := filepath.Join(*dir, "sample-v1.jsonl")
	if err := os.WriteFile(binaryPath, binaryBytes, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(jsonlPath, jsonlBytes, 0o644); err != nil {
		return err
	}
	result.Artifacts["binary_sample"] = binaryPath
	result.Artifacts["jsonl_sample"] = jsonlPath

	server, err := startChild(*dir, *port, *queueCap)
	if err != nil {
		return err
	}
	defer stopChild(server)
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", *port)
	client := &http.Client{Timeout: 10 * time.Second}

	strategyBody := map[string]any{
		"commit_id": "strategy-1",
		"strategy":  sample.StrategyV1(),
	}
	if err := postJSON(client, baseURL+"/v1/strategy", strategyBody, nil); err != nil {
		return fmt.Errorf("submit strategy: %w%s", err, childLogTail(*dir))
	}
	batch := sample.Build()

	importBin := map[string]any{}
	if err := postJSON(client, baseURL+"/v1/import", map[string]any{
		"commit_id": "import-binary-v2", "format": "binary", "data_base64": base64.StdEncoding.EncodeToString(binaryBytes),
	}, &importBin); err != nil {
		return fmt.Errorf("import binary manifest: %w%s", err, childLogTail(*dir))
	}
	appendImportResult(&result, importBin, "binary")

	importJSONL := map[string]any{}
	if err := postJSON(client, baseURL+"/v1/import", map[string]any{
		"commit_id": "import-jsonl-v1", "format": "jsonl", "data_base64": base64.StdEncoding.EncodeToString(jsonlBytes),
	}, &importJSONL); err != nil {
		return fmt.Errorf("import JSONL manifest: %w%s", err, childLogTail(*dir))
	}
	appendImportResult(&result, importJSONL, "jsonl")

	fullCheckpoint := map[string]any{}
	if err := postJSON(client, baseURL+"/v1/checkpoints", map[string]any{
		"commit_id": "checkpoint-full-1", "shards": batch.Shards, "baseline": batch.Baseline,
	}, &fullCheckpoint); err != nil {
		return fmt.Errorf("commit full checkpoint: %w%s", err, childLogTail(*dir))
	}
	result.FullCheckpoint = fullCheckpoint

	replay := map[string]any{}
	if err := postJSON(client, baseURL+"/v1/checkpoints", map[string]any{
		"commit_id": "checkpoint-full-1", "shards": batch.Shards, "baseline": batch.Baseline,
	}, &replay); err != nil {
		return fmt.Errorf("replay checkpoint: %w", err)
	}
	result.Idempotency = map[string]any{
		"same_commit_id_reused": jsonEqual(fullCheckpoint, replay),
		"response":              replay,
	}
	altered := append([]model.Shard(nil), batch.Shards...)
	altered[0].SizeBytes++
	var conflictBody map[string]any
	err = postJSON(client, baseURL+"/v1/checkpoints", map[string]any{
		"commit_id": "checkpoint-full-1", "shards": altered, "baseline": batch.Baseline,
	}, &conflictBody)
	if err == nil {
		return fmt.Errorf("conflicting request was unexpectedly accepted")
	}
	result.Conflict = map[string]any{"error": err.Error()}

	incremental := map[string]any{}
	if err := postJSON(client, baseURL+"/v1/incremental", map[string]any{
		"commit_id":    "incremental-precision-1",
		"base_version": 1,
		"generated_at": "2026-01-02T04:05:06Z",
		"changes":      []model.IncrementalPatch{batch.IncrementPatch},
	}, &incremental); err != nil {
		return fmt.Errorf("submit incremental precision change: %w%s", err, childLogTail(*dir))
	}
	result.Incremental = incremental

	eviction := map[string]any{}
	if err := postJSON(client, baseURL+"/v1/pools/reside", map[string]any{
		"commit_id": "reside-eviction-1", "shard": batch.ExtraResident,
	}, &eviction); err != nil {
		return fmt.Errorf("reside shard and evict: %w%s", err, childLogTail(*dir))
	}
	result.Eviction = eviction

	precision := map[string]any{}
	if err := getJSON(client, baseURL+"/v1/precision", &precision); err != nil {
		return fmt.Errorf("query precision report: %w", err)
	}
	result.Precision = precision
	precisionAgain := map[string]any{}
	if err := getJSON(client, baseURL+"/v1/precision", &precisionAgain); err != nil {
		return err
	}
	firstReport, _ := precision["report"].(map[string]any)
	secondReport, _ := precisionAgain["report"].(map[string]any)
	result.PrecisionRepeat = fmt.Sprint(secondReport["report_checksum"])
	repeatIdentical := fmt.Sprint(firstReport["report_checksum"]) == result.PrecisionRepeat && jsonEqual(precision, precisionAgain)
	imports := map[string]any{}
	if err := getJSON(client, baseURL+"/v1/imports", &imports); err != nil {
		return err
	}
	stateBeforeCrash := map[string]any{}
	if err := getJSON(client, baseURL+"/v1/state", &stateBeforeCrash); err != nil {
		return err
	}
	poolsBeforeCrash := map[string]any{}
	if err := getJSON(client, baseURL+"/v1/pools", &poolsBeforeCrash); err != nil {
		return err
	}
	derivedBeforeCrash := map[string]any{}
	if err := getJSON(client, baseURL+"/v1/derived", &derivedBeforeCrash); err != nil {
		return err
	}

	crashCommitID := fmt.Sprintf("crash-fault-%d", time.Now().UnixNano())
	if err := triggerCrashAndKill(server, baseURL, crashCommitID); err != nil {
		return err
	}
	server = nil
	restarted, err := startChild(*dir, *port, *queueCap)
	if err != nil {
		return fmt.Errorf("restart after forced termination: %w", err)
	}
	defer stopChild(restarted)
	client = &http.Client{Timeout: 10 * time.Second}
	recovery := map[string]any{}
	if err := getJSON(client, baseURL+"/v1/recovery", &recovery); err != nil {
		return fmt.Errorf("query recovery report: %w", err)
	}
	stateAfterCrash := map[string]any{}
	if err := getJSON(client, baseURL+"/v1/state", &stateAfterCrash); err != nil {
		return err
	}
	result.CrashRecovery = map[string]any{
		"recovery":                  recovery,
		"dataset_version_preserved": stateBeforeCrash["dataset_version"] == stateAfterCrash["dataset_version"],
		"state":                     stateAfterCrash,
	}
	result.FinalState = map[string]any{
		"state":                    stateAfterCrash,
		"pools_before_crash":       poolsBeforeCrash,
		"derived_before_crash":     derivedBeforeCrash,
		"import_persistence":       imports,
		"precision_repeat_bitwise": repeatIdentical,
	}

	if err := validateResult(result); err != nil {
		return err
	}
	resultPath := filepath.Join(*dir, "verification-results.json")
	result.Status = "PASS"
	result.Artifacts["verification_results"] = resultPath
	raw, _ := json.MarshalIndent(result, "", "  ")
	if err := os.WriteFile(resultPath, append(raw, '\n'), 0o644); err != nil {
		return err
	}
	printSummary(result)
	return nil
}

func appendImportResult(result *Result, response map[string]any, formatName string) {
	entry := map[string]any{
		"format":          formatName,
		"verdict":         response["verdict"],
		"format_version":  response["format_version"],
		"details":         response["details"],
		"dataset_version": response["dataset_version"],
	}
	result.Imports = append(result.Imports, entry)
}

func validateResult(result Result) error {
	if len(result.Imports) != 2 {
		return fmt.Errorf("expected two import results, got %d", len(result.Imports))
	}
	if fmt.Sprint(result.Imports[0]["verdict"]) != "ACCEPT" {
		return fmt.Errorf("binary v2 import verdict = %v, want ACCEPT", result.Imports[0]["verdict"])
	}
	if fmt.Sprint(result.Imports[1]["verdict"]) != "DEGRADE_ACCEPT" {
		return fmt.Errorf("JSONL v1 import verdict = %v, want DEGRADE_ACCEPT", result.Imports[1]["verdict"])
	}
	if !hasEviction(result.Eviction) {
		return fmt.Errorf("residency operation did not report a real eviction: %v", result.Eviction)
	}
	if fmt.Sprint(result.Idempotency["same_commit_id_reused"]) != "true" {
		return fmt.Errorf("same commit id was executed more than once: %v", result.Idempotency)
	}
	if !strings.Contains(fmt.Sprint(result.Conflict["error"]), model.CodeConflictingRequest) {
		return fmt.Errorf("conflicting request did not return %s: %v", model.CodeConflictingRequest, result.Conflict)
	}
	affected, _ := result.Incremental["affected_shards"].([]any)
	if len(affected) != 1 || affected[0] != "r2-attn-fp16-b" {
		return fmt.Errorf("incremental fragment did not affect exactly one shard: %v", result.Incremental)
	}
	report, _ := result.Precision["report"].(map[string]any)
	if report == nil || fmt.Sprint(report["within_policy"]) != "true" {
		return fmt.Errorf("precision report is missing or outside policy: %v", result.Precision)
	}
	if result.PrecisionRepeat == "" || result.PrecisionRepeat == "<nil>" {
		return fmt.Errorf("precision report checksum is missing")
	}
	if fmt.Sprint(result.FinalState["precision_repeat_bitwise"]) != "true" {
		return fmt.Errorf("repeated precision report was not bit-identical")
	}
	if fmt.Sprint(result.CrashRecovery["dataset_version_preserved"]) != "true" {
		return fmt.Errorf("recovery advanced or changed the committed dataset version")
	}
	recovery, _ := result.CrashRecovery["recovery"].(map[string]any)
	events, _ := recovery["events"].([]any)
	if len(events) == 0 {
		return fmt.Errorf("restart did not record a recovery event")
	}
	firstEvent, _ := events[0].(map[string]any)
	details, _ := firstEvent["details"].(map[string]any)
	pendingReset, _ := details["pending_reset"].([]any)
	evictionsRestored, _ := details["evictions_restored"].([]any)
	if len(pendingReset) == 0 || len(evictionsRestored) == 0 {
		return fmt.Errorf("recovery event did not restore both pending and evicted states: %v", details)
	}
	importPersistence, _ := result.FinalState["import_persistence"].(map[string]any)
	reports, _ := importPersistence["reports"].([]any)
	if len(reports) != 2 {
		return fmt.Errorf("compatibility decisions/import reports were not persisted: %v", importPersistence)
	}
	return nil
}

func hasEviction(value map[string]any) bool {
	evictions, ok := value["evictions"].([]any)
	return ok && len(evictions) > 0
}

func printSummary(result Result) {
	fmt.Println("MPCKPT VERIFY: PASS")
	fmt.Printf("run_directory=%s\n", result.RunDirectory)
	for _, item := range result.Imports {
		details, _ := json.Marshal(item["details"])
		fmt.Printf("import format=%v version=%v verdict=%v dataset_version=%v details=%s\n",
			item["format"], item["format_version"], item["verdict"], item["dataset_version"], details)
	}
	report, _ := result.Precision["report"].(map[string]any)
	fmt.Printf("precision within_policy=%v max_abs_error_fixed=%v max_rel_error_ppm=%v cumulative_rounding_error=%v checksum=%v\n",
		report["within_policy"], report["max_abs_error_fixed"], report["max_rel_error_ppm"],
		report["cumulative_rounding_error_fixed"], report["report_checksum"])
	evictions, _ := result.Eviction["evictions"].([]any)
	fmt.Printf("evictions=%d gpu_usage_after=%v\n", len(evictions), result.Eviction["gpu_usage_bytes"])
	recovery, _ := result.CrashRecovery["recovery"].(map[string]any)
	events, _ := recovery["events"].([]any)
	fmt.Printf("crash_recovery_events=%d dataset_version_preserved=%v\n", len(events), result.CrashRecovery["dataset_version_preserved"])
	fmt.Printf("artifacts=%s\n", result.Artifacts["verification_results"])
}

func startChild(dir string, port, queueCap int) (*childServer, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	logPath := filepath.Join(dir, "serve.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(exe, "serve", "-dir", dir, "-addr", fmt.Sprintf("127.0.0.1:%d", port), "-queue-cap", fmt.Sprint(queueCap))
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, err
	}
	server := &childServer{cmd: cmd, logFile: logFile, port: port}
	if err := waitHealthy(&http.Client{Timeout: time.Second}, port, 15*time.Second); err != nil {
		stopChild(server)
		return nil, fmt.Errorf("child server did not become healthy: %w%s", err, childLogTail(dir))
	}
	return server, nil
}

func waitHealthy(client *http.Client, port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	url := fmt.Sprintf("http://127.0.0.1:%d/healthz", port)
	for time.Now().Before(deadline) {
		response, err := client.Get(url)
		if err == nil {
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return errors.New("health check timed out")
}

func stopChild(server *childServer) {
	if server == nil || server.cmd == nil || server.cmd.Process == nil {
		return
	}
	if server.cmd.ProcessState == nil {
		_ = server.cmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() {
			_ = server.cmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			_ = server.cmd.Process.Kill()
			<-done
		}
	}
	if server.logFile != nil {
		_ = server.logFile.Close()
	}
}

func triggerCrashAndKill(server *childServer, baseURL, commitID string) error {
	body, _ := json.Marshal(map[string]any{"commit_id": commitID, "hold_ms": 120000})
	request, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/debug/crash-fault", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("prepare crash fault: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(response.Body)
		response.Body.Close()
		return fmt.Errorf("prepare crash fault status=%d body=%s", response.StatusCode, raw)
	}
	time.Sleep(150 * time.Millisecond)
	if err := server.cmd.Process.Kill(); err != nil {
		return fmt.Errorf("kill child process: %w", err)
	}
	_ = server.cmd.Wait()
	_ = response.Body.Close()
	_ = server.logFile.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		connection, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", server.port), 100*time.Millisecond)
		if err != nil {
			return nil
		}
		connection.Close()
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

func postJSON(client *http.Client, url string, body any, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	request, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(raw))
	request.Header.Set("Content-Type", "application/json")
	return doJSON(client, request, out)
}

func getJSON(client *http.Client, url string, out any) error {
	request, _ := http.NewRequest(http.MethodGet, url, nil)
	return doJSON(client, request, out)
}

func doJSON(client *http.Client, request *http.Request, out any) error {
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var envelope struct {
			Code    string         `json:"code"`
			Message string         `json:"message"`
			Context map[string]any `json:"context"`
		}
		if json.Unmarshal(raw, &envelope) == nil && envelope.Code != "" {
			return fmt.Errorf("%s: %s context=%v", envelope.Code, envelope.Message, envelope.Context)
		}
		return fmt.Errorf("HTTP %d: %s", response.StatusCode, raw)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode response: %w body=%s", err, raw)
	}
	return nil
}

func jsonEqual(left, right any) bool {
	leftJSON, _ := json.Marshal(left)
	rightJSON, _ := json.Marshal(right)
	var leftBuf, rightBuf bytes.Buffer
	if json.Compact(&leftBuf, leftJSON) != nil || json.Compact(&rightBuf, rightJSON) != nil {
		return false
	}
	return bytes.Equal(leftBuf.Bytes(), rightBuf.Bytes())
}

func childLogTail(dir string) string {
	raw, err := os.ReadFile(filepath.Join(dir, "serve.log"))
	if err != nil {
		return ""
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) > 12 {
		lines = lines[len(lines)-12:]
	}
	return "\nchild log tail:\n" + strings.Join(lines, "\n")
}
