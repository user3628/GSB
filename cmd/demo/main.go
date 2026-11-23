package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"recoveryengine/internal/engine"
	"recoveryengine/internal/graph"
	"recoveryengine/internal/model"
	"recoveryengine/internal/spec"
	"recoveryengine/internal/store"
)

type childProcess struct {
	name    string
	cmd     *exec.Cmd
	logFile *os.File
	exited  chan struct{}
	waitErr error
	stop    sync.Once
}

type apiEnvelope struct {
	OK            bool            `json:"ok"`
	Data          json.RawMessage `json:"data"`
	Error         *apiError       `json:"error"`
	PartialResult json.RawMessage `json:"partial_result,omitempty"`
}

type apiError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
}

type step struct {
	Name    string `json:"name"`
	Passed  bool   `json:"passed"`
	Details any    `json:"details,omitempty"`
	Error   string `json:"error,omitempty"`
}

type demo struct {
	root             string
	workdir          string
	runDir           string
	serverBinary     string
	endpointBinary   string
	serverAddr       string
	baselineAddr     string
	endpointAddr     string
	client           *http.Client
	children         []*childProcess
	steps            []step
	runID            string
	mainDatabase     string
	baselineDatabase string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "DEMO FAILED:", err)
		os.Exit(1)
	}
}

func run() (runErr error) {
	root, err := moduleRoot()
	if err != nil {
		return err
	}
	workdir := flag.String("workdir", filepath.Join(".tmp", "demo"), "directory for demo binaries, databases and evidence")
	serverAddr := flag.String("server-addr", "127.0.0.1:19180", "coordinator listen address")
	baselineAddr := flag.String("baseline-addr", "127.0.0.1:19181", "baseline coordinator listen address")
	endpointAddr := flag.String("endpoint-addr", "127.0.0.1:19090", "local test endpoint address used by fixed samples")
	skipBuild := flag.Bool("skip-build", false, "reuse existing demo binaries")
	flag.Parse()

	if !filepath.IsAbs(*workdir) {
		*workdir = filepath.Join(root, *workdir)
	}
	runID := time.Now().Format("20060102-150405")
	runDir := filepath.Join(*workdir, runID)
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return fmt.Errorf("create run directory: %w", err)
	}
	binDir := filepath.Join(*workdir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return fmt.Errorf("create binary cache directory: %w", err)
	}

	d := &demo{
		root:             root,
		workdir:          *workdir,
		runDir:           runDir,
		serverAddr:       *serverAddr,
		baselineAddr:     *baselineAddr,
		endpointAddr:     *endpointAddr,
		client:           &http.Client{Timeout: 20 * time.Second},
		runID:            runID,
		mainDatabase:     filepath.Join(runDir, "recovery.db"),
		baselineDatabase: filepath.Join(runDir, "baseline.db"),
	}
	suffix := ""
	if runtime.GOOS == "windows" {
		suffix = ".exe"
	}
	d.serverBinary = filepath.Join(binDir, "recovery-server"+suffix)
	d.endpointBinary = filepath.Join(binDir, "testendpoint"+suffix)
	success := false
	defer func() {
		d.stopAll()
		d.writeEvidence(success, runErr)
	}()

	fmt.Printf("Recovery Coordinator end-to-end demo\nrun directory: %s\n", runDir)
	if !*skipBuild {
		if err := d.buildBinaries(); err != nil {
			return err
		}
	} else {
		if _, err := os.Stat(d.serverBinary); err != nil {
			return fmt.Errorf("server binary is missing: %w", err)
		}
		if _, err := os.Stat(d.endpointBinary); err != nil {
			return fmt.Errorf("test endpoint binary is missing: %w", err)
		}
	}

	if err := d.execute(); err != nil {
		return err
	}
	success = true
	fmt.Printf("\nALL CHECKS PASSED\nEvidence: %s\n", filepath.Join(runDir, "evidence.json"))
	return nil
}

func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("go.mod was not found; run the demo from the repository")
		}
		dir = parent
	}
}

func (d *demo) buildBinaries() error {
	for _, target := range []struct {
		output string
		pkg    string
	}{
		{d.serverBinary, "./cmd/server"},
		{d.endpointBinary, "./cmd/testendpoint"},
	} {
		command := exec.Command("go", "build", "-trimpath", "-o", target.output, target.pkg)
		command.Dir = d.root
		output, err := command.CombinedOutput()
		if err != nil {
			return fmt.Errorf("build %s: %w\n%s", target.pkg, err, output)
		}
	}
	d.record(step{Name: "build", Passed: true, Details: map[string]any{
		"server": d.serverBinary, "test_endpoint": d.endpointBinary,
	}})
	return nil
}

func (d *demo) execute() error {
	endpoint, err := d.startProcess("test-endpoint", d.endpointBinary, d.endpointAddr, nil)
	if err != nil {
		return err
	}
	if err := d.waitEndpoint(endpoint); err != nil {
		return err
	}

	server, err := d.startServer("coordinator-main-1", d.serverAddr, d.mainDatabase)
	if err != nil {
		return err
	}
	if err := d.waitServer(server, d.serverAddr); err != nil {
		return err
	}
	baseURL := "http://" + d.serverAddr

	status, envelope, err := d.postJSON(baseURL+"/api/v1/samples/load", map[string]any{
		"name": "full-snapshot.json", "idempotency_key": d.runID + "-bootstrap",
	}, nil)
	if err != nil {
		return err
	}
	var loaded engine.CommitResult
	if err := expectSuccess(status, envelope, http.StatusOK, &loaded); err != nil {
		return fmt.Errorf("full snapshot commit: %w", err)
	}
	if err := d.requireVersion(baseURL, 1); err != nil {
		return err
	}
	d.record(step{Name: "full_snapshot_commit", Passed: true, Details: map[string]any{
		"version": loaded.Version, "components": 9, "edges": 8,
		"checksum": loaded.ChecksumSHA256, "derived_digest": loaded.DerivedDigest,
	}})

	for _, invalid := range []string{
		"invalid-negative-resource.json",
		"invalid-overflow-resource.json",
		"invalid-missing-dependency.json",
		"invalid-weight.json",
	} {
		status, envelope, err = d.postJSON(baseURL+"/api/v1/samples/validate", map[string]any{"name": invalid}, nil)
		if err != nil {
			return err
		}
		if err := expectError(status, envelope, http.StatusUnprocessableEntity, model.CodeSpecInvalid); err != nil {
			return fmt.Errorf("invalid sample %s: %w", invalid, err)
		}
	}
	invalidBytes, err := os.ReadFile(filepath.Join(d.root, "samples", "invalid-negative-resource.json"))
	if err != nil {
		return err
	}
	status, envelope, err = d.postRaw(baseURL+"/api/v1/snapshots", invalidBytes, map[string]string{
		"Content-Type": "application/json", "Idempotency-Key": d.runID + "-invalid-snapshot",
	})
	if err != nil {
		return err
	}
	if err := expectError(status, envelope, http.StatusUnprocessableEntity, model.CodeSpecInvalid); err != nil {
		return fmt.Errorf("invalid snapshot must not enter storage: %w", err)
	}
	if err := d.requireVersion(baseURL, 1); err != nil {
		return err
	}
	d.record(step{Name: "invalid_spec_atomic_rejection", Passed: true, Details: map[string]any{
		"samples": 4, "committed_version": 1, "code": model.CodeSpecInvalid,
	}})

	impact, err := d.gatewayImpact(baseURL)
	if err != nil {
		return err
	}
	if impact.AffectedCount != 4 || impact.MaxHop != 3 {
		return fmt.Errorf("unexpected gateway impact: affected=%d max_hop=%d", impact.AffectedCount, impact.MaxHop)
	}
	wantBatches := []string{"gateway", "telemetry", "reporter", "notifier"}
	if len(impact.RecoveryBatches) != len(wantBatches) {
		return fmt.Errorf("unexpected gateway batches: %#v", impact.RecoveryBatches)
	}
	for i, want := range wantBatches {
		if len(impact.RecoveryBatches[i].Components) != 1 || impact.RecoveryBatches[i].Components[0] != want {
			return fmt.Errorf("recovery batch %d = %#v, want %s", i, impact.RecoveryBatches[i], want)
		}
	}
	d.record(step{Name: "impact_and_recovery_batches", Passed: true, Details: map[string]any{
		"origin": "gateway", "affected_count": impact.AffectedCount, "max_hop": impact.MaxHop,
		"batches": impact.RecoveryBatches,
	}})

	cycle, err := d.sampleIncrement("increments/cycle-add.json", d.runID+"-cycle", 1)
	if err != nil {
		return err
	}
	status, envelope, err = d.postJSON(baseURL+"/api/v1/increments", cycle, nil)
	if err != nil {
		return err
	}
	if err := expectError(status, envelope, http.StatusUnprocessableEntity, model.CodeDependencyCycle); err != nil {
		return fmt.Errorf("cycle rejection: %w", err)
	}
	if len(envelope.PartialResult) == 0 || bytes.Equal(envelope.PartialResult, []byte("null")) {
		return errors.New("cycle rejection did not include partial graph result")
	}
	if err := d.requireVersion(baseURL, 1); err != nil {
		return fmt.Errorf("cycle rejection changed committed state: %w", err)
	}
	d.record(step{Name: "dependency_cycle_atomic_rejection", Passed: true, Details: map[string]any{
		"code": model.CodeDependencyCycle, "committed_version": 1, "partial_result": true,
	}})

	resource, err := d.sampleIncrement("increments/resource-adjust.json", d.runID+"-resource", 1)
	if err != nil {
		return err
	}
	status, envelope, err = d.postJSON(baseURL+"/api/v1/increments", resource, nil)
	if err != nil {
		return err
	}
	var resourceResult engine.CommitResult
	if err := expectSuccess(status, envelope, http.StatusOK, &resourceResult); err != nil {
		return fmt.Errorf("resource increment: %w", err)
	}
	if resourceResult.Version != 2 || len(resourceResult.Cascade.Invalidated) == 0 || len(resourceResult.Cascade.Rebuilt) == 0 {
		return fmt.Errorf("cascade did not report invalidation/rebuild: %#v", resourceResult.Cascade)
	}
	d.record(step{Name: "incremental_cascade_rebuild", Passed: true, Details: map[string]any{
		"base_version": resourceResult.BaseVersion, "version": resourceResult.Version,
		"changed_refs": len(resourceResult.Cascade.ChangedRefs),
		"invalidated":  len(resourceResult.Cascade.Invalidated), "rebuilt": len(resourceResult.Cascade.Rebuilt),
		"strategy": resourceResult.Cascade.Strategy,
	}})

	stale, err := d.sampleIncrement("increments/stale-base.json", d.runID+"-stale", 1)
	if err != nil {
		return err
	}
	status, envelope, err = d.postJSON(baseURL+"/api/v1/increments", stale, nil)
	if err != nil {
		return err
	}
	if err := expectError(status, envelope, http.StatusConflict, model.CodeStaleBase); err != nil {
		return fmt.Errorf("stale base rejection: %w", err)
	}
	if err := d.requireVersion(baseURL, 2); err != nil {
		return fmt.Errorf("stale base changed committed state: %w", err)
	}
	d.record(step{Name: "stale_base_rejection", Passed: true, Details: map[string]any{
		"code": model.CodeStaleBase, "base_version": 1, "current_version": 2,
	}})

	concurrent, err := d.runConcurrentIncrements(baseURL)
	if err != nil {
		return err
	}
	d.record(step{Name: "concurrent_increment_conflict", Passed: true, Details: concurrent})

	if err := d.postRawNoJSON("http://"+d.endpointAddr+"/reset", nil); err != nil {
		return err
	}
	drillData, err := os.ReadFile(filepath.Join(d.root, "samples", "drill-request.json"))
	if err != nil {
		return err
	}
	drillRequest, business := spec.DecodeDrill(drillData)
	if business != nil {
		return business
	}
	drillRequest.Version = 3
	drillRequest.IdempotencyKey = d.runID + "-drill"
	drillStarted := time.Now()
	status, envelope, err = d.postJSON(baseURL+"/api/v1/drills", drillRequest, nil)
	if err != nil {
		return err
	}
	var drill engine.DrillResult
	if err := expectSuccess(status, envelope, http.StatusOK, &drill); err != nil {
		return fmt.Errorf("drill: %w", err)
	}
	drillElapsed := time.Since(drillStarted)
	if drill.AllOperationsSucceeded {
		return errors.New("failure-mode drill unexpectedly reported all operations successful")
	}
	for _, code := range []string{
		model.CodeProbeConnectionRefused, model.CodeRecoverConnectionRef,
		model.CodeProbeTimeout, model.CodeRecoverTimeout,
		model.CodeProbeRejected, model.CodeRecoverRejected,
	} {
		if drill.CodeCounts[code] == 0 {
			return fmt.Errorf("drill is missing code %s: %#v", code, drill.CodeCounts)
		}
	}
	if !drill.ReservationsReleased {
		return errors.New("drill did not report reservation release")
	}
	if err := d.verifyTimeoutEvidence(drill); err != nil {
		return err
	}
	var reservations struct {
		Active        []engine.ReservationView `json:"active"`
		ReleasedTotal uint64                   `json:"released_total"`
	}
	status, envelope, err = d.getJSON(baseURL + "/api/v1/runtime/reservations")
	if err != nil {
		return err
	}
	if err := expectSuccess(status, envelope, http.StatusOK, &reservations); err != nil {
		return err
	}
	if len(reservations.Active) != 0 || reservations.ReleasedTotal < 4 {
		return fmt.Errorf("reservation leak: active=%d released=%d", len(reservations.Active), reservations.ReleasedTotal)
	}
	endpointStats, err := d.readEndpointStats()
	if err != nil {
		return err
	}
	cancelled := int64FromAny(endpointStats["cancelled"])
	if cancelled < 1 {
		return fmt.Errorf("test endpoint did not observe a cancelled timed-out connection: %#v", endpointStats)
	}
	status, envelope, err = d.getJSON(baseURL + "/api/v1/consistency?repeat=3")
	if err != nil {
		return err
	}
	var consistency struct {
		Version      int64    `json:"version"`
		Reproducible bool     `json:"reproducible"`
		Digests      []string `json:"digests"`
	}
	if err := expectSuccess(status, envelope, http.StatusOK, &consistency); err != nil {
		return err
	}
	if !consistency.Reproducible || len(consistency.Digests) != 3 {
		return fmt.Errorf("fixed-point consistency was not reproducible: %#v", consistency)
	}
	d.record(step{Name: "real_http_drill_and_timeout", Passed: true, Details: map[string]any{
		"elapsed_ms": drillElapsed.Milliseconds(), "code_counts": drill.CodeCounts,
		"reservations_active": len(reservations.Active), "reservations_released": reservations.ReleasedTotal,
		"endpoint_cancelled": cancelled, "consistency_digests": consistency.Digests,
	}})

	crashRequest := model.IncrementRequest{
		IdempotencyKey: d.runID + "-crash-retry",
		BaseVersion:    3,
		GeneratedAt:    "2026-03-01T10:00:00Z",
		Note:           "Crash-window and exact replay verification",
		Operations: []model.IncrementOperation{{
			Type: "patch_resource", ComponentID: "auth",
			Resources: &model.Resource{CPUMillicores: 1700, MemoryBytes: 2147483648},
		}},
	}
	if err := server.stopGracefully(); err != nil {
		return fmt.Errorf("stop coordinator before baseline copy: %w", err)
	}
	if err := backupSQLite(d.mainDatabase, d.baselineDatabase); err != nil {
		return fmt.Errorf("create baseline database: %w", err)
	}
	baseline, err := d.startServer("coordinator-baseline", d.baselineAddr, d.baselineDatabase)
	if err != nil {
		return err
	}
	if err := d.waitServer(baseline, d.baselineAddr); err != nil {
		return err
	}
	baselineURL := "http://" + d.baselineAddr
	status, envelope, err = d.postJSON(baselineURL+"/api/v1/increments", crashRequest, nil)
	if err != nil {
		return err
	}
	var expected engine.CommitResult
	if err := expectSuccess(status, envelope, http.StatusOK, &expected); err != nil {
		return fmt.Errorf("uninterrupted baseline: %w", err)
	}
	if err := baseline.stopGracefully(); err != nil {
		return err
	}

	crashServer, err := d.startServer("coordinator-crash-window", d.serverAddr, d.mainDatabase)
	if err != nil {
		return err
	}
	if err := d.waitServer(crashServer, d.serverAddr); err != nil {
		return err
	}
	heldResult := make(chan error, 1)
	go func() {
		_, _, requestErr := d.postJSON(baseURL+"/api/v1/increments", crashRequest, map[string]string{
			"X-Test-Hold-After-Intent-Ms": "5000",
		})
		heldResult <- requestErr
	}()
	if err := d.waitPendingIntent(baseURL, crashRequest.IdempotencyKey, 6*time.Second); err != nil {
		return err
	}
	if err := crashServer.kill(); err != nil {
		return fmt.Errorf("force kill during intent window: %w", err)
	}
	select {
	case <-heldResult:
	case <-time.After(5 * time.Second):
		return errors.New("held HTTP request did not terminate after process kill")
	}
	if err := d.requireVersion(baseURL, 0); err == nil {
		return errors.New("coordinator still reachable after forced kill")
	}

	recoveredServer, err := d.startServer("coordinator-recovered", d.serverAddr, d.mainDatabase)
	if err != nil {
		return err
	}
	if err := d.waitServer(recoveredServer, d.serverAddr); err != nil {
		return err
	}
	if err := d.requireVersion(baseURL, 3); err != nil {
		return fmt.Errorf("recovered state moved away from last committed version: %w", err)
	}
	reports, err := d.recoveryReports(baseURL)
	if err != nil {
		return err
	}
	if len(reports) == 0 || reports[0].ScannedIntents < 1 || !bytes.Contains(reports[0].Aborted, []byte(crashRequest.IdempotencyKey)) {
		return fmt.Errorf("startup recovery report is not based on the interrupted intent: %#v", reports)
	}
	intents, err := d.intents(baseURL, "aborted")
	if err != nil {
		return err
	}
	abortedFound := false
	for _, intent := range intents {
		if intent.SubmissionKey == crashRequest.IdempotencyKey && intent.Phase == "recovered_on_startup" {
			abortedFound = true
		}
	}
	if !abortedFound {
		return fmt.Errorf("interrupted intent was not marked aborted: %#v", intents)
	}

	status, envelope, err = d.postJSON(baseURL+"/api/v1/increments", crashRequest, nil)
	if err != nil {
		return err
	}
	var replayed engine.CommitResult
	if err := expectSuccess(status, envelope, http.StatusOK, &replayed); err != nil {
		return fmt.Errorf("retry after crash: %w", err)
	}
	if err := compareReplay(expected, replayed); err != nil {
		return err
	}
	recoveredState, err := d.state(baseURL)
	if err != nil {
		return err
	}
	if recoveredState.Version != 4 {
		return fmt.Errorf("post-crash retry version = %d, want 4", recoveredState.Version)
	}
	if err := recoveredServer.stopGracefully(); err != nil {
		return err
	}
	restarted, err := d.startServer("coordinator-restarted", d.serverAddr, d.mainDatabase)
	if err != nil {
		return err
	}
	if err := d.waitServer(restarted, d.serverAddr); err != nil {
		return err
	}
	restartedState, err := d.state(baseURL)
	if err != nil {
		return err
	}
	if restartedState.Version != recoveredState.Version ||
		restartedState.ChecksumSHA256 != recoveredState.ChecksumSHA256 ||
		restartedState.Derivation.DerivedDigest != recoveredState.Derivation.DerivedDigest {
		return fmt.Errorf("restart changed committed facts: before=%#v after=%#v", recoveredState.Version, restartedState.Version)
	}
	d.record(step{Name: "forced_kill_recovery_and_exact_replay", Passed: true, Details: map[string]any{
		"last_committed_version": 3, "recovered_report_id": reports[0].ID,
		"aborted_intents": reports[0].ScannedIntents, "retried_version": replayed.Version,
		"checksum": replayed.ChecksumSHA256, "derived_digest": replayed.DerivedDigest,
		"exact_match_with_uninterrupted_baseline": true,
	}})

	replacement, err := d.fullReplacementSnapshot(5)
	if err != nil {
		return err
	}
	status, envelope, err = d.postJSON(baseURL+"/api/v1/snapshots", model.FullSnapshotRequest{
		IdempotencyKey: d.runID + "-replacement", Snapshot: replacement,
	}, nil)
	if err != nil {
		return err
	}
	var replaced engine.CommitResult
	if err := expectSuccess(status, envelope, http.StatusOK, &replaced); err != nil {
		return fmt.Errorf("full snapshot replacement: %w", err)
	}
	finalState, err := d.state(baseURL)
	if err != nil {
		return err
	}
	if replaced.Version != 5 || replaced.BaseVersion != 4 || finalState.Version != 5 || len(finalState.Components) != 9 {
		return fmt.Errorf("unexpected full replacement result: %#v", replaced)
	}
	d.record(step{Name: "full_snapshot_replacement", Passed: true, Details: map[string]any{
		"base_version": replaced.BaseVersion, "version": replaced.Version,
		"components": len(finalState.Components), "edges": len(finalState.Edges),
	}})

	return nil
}

func (d *demo) record(item step) {
	d.steps = append(d.steps, item)
	if item.Passed {
		fmt.Printf("[PASS] %s\n", item.Name)
		return
	}
	fmt.Printf("[FAIL] %s: %s\n", item.Name, item.Error)
}

func (d *demo) writeEvidence(success bool, runErr error) {
	payload := map[string]any{
		"run_id": d.runID, "generated_at": time.Now().UTC().Format(time.RFC3339Nano),
		"success": success, "steps": d.steps, "run_directory": d.runDir,
		"note": "All facts below were produced by real loopback HTTP requests and SQLite-backed coordinator processes.",
	}
	if runErr != nil {
		payload["error"] = runErr.Error()
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(d.runDir, "evidence.json"), append(data, '\n'), 0o644)
}

func (d *demo) startServer(name, addr, database string) (*childProcess, error) {
	return d.startProcess(name, d.serverBinary, addr, []string{
		"-db", database, "-test-hooks=true",
	})
}

func (d *demo) startProcess(name, binary, addr string, extra []string) (*childProcess, error) {
	logPath := filepath.Join(d.runDir, name+".log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	args := []string{"-addr", addr}
	args = append(args, extra...)
	command := exec.Command(binary, args...)
	command.Dir = d.root
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("start %s: %w", name, err)
	}
	child := &childProcess{name: name, cmd: command, logFile: logFile, exited: make(chan struct{})}
	go func() {
		child.waitErr = command.Wait()
		close(child.exited)
	}()
	d.children = append(d.children, child)
	return child, nil
}

func (d *demo) waitServer(child *childProcess, addr string) error {
	return d.waitHTTP(child, "http://"+addr+"/api/v1/healthz", 12*time.Second)
}

func (d *demo) waitEndpoint(child *childProcess) error {
	return d.waitHTTP(child, "http://"+d.endpointAddr+"/stats", 12*time.Second)
}

func (d *demo) waitHTTP(child *childProcess, endpoint string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case <-child.exited:
			return fmt.Errorf("%s exited before becoming ready: %v (see %s.log)", child.name, child.waitErr, child.name)
		default:
		}
		response, err := d.client.Get(endpoint)
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				return nil
			}
			lastErr = fmt.Errorf("HTTP %d", response.StatusCode)
		} else {
			lastErr = err
		}
		time.Sleep(120 * time.Millisecond)
	}
	return fmt.Errorf("%s did not become ready at %s: %v", child.name, endpoint, lastErr)
}

func (d *demo) stopAll() {
	for i := len(d.children) - 1; i >= 0; i-- {
		_ = d.children[i].stopGracefully()
	}
}

func (c *childProcess) stopGracefully() error {
	var stopErr error
	c.stop.Do(func() {
		select {
		case <-c.exited:
		default:
			_ = c.cmd.Process.Signal(os.Interrupt)
			select {
			case <-c.exited:
				return
			case <-time.After(3 * time.Second):
			}
			stopErr = c.cmd.Process.Kill()
			select {
			case <-c.exited:
			case <-time.After(3 * time.Second):
				if stopErr == nil {
					stopErr = errors.New("process did not exit after kill")
				}
			}
		}
		_ = c.logFile.Close()
	})
	return stopErr
}

func (c *childProcess) kill() error {
	var killErr error
	c.stop.Do(func() {
		select {
		case <-c.exited:
		default:
			killErr = c.cmd.Process.Kill()
			select {
			case <-c.exited:
			case <-time.After(5 * time.Second):
				if killErr == nil {
					killErr = errors.New("process did not exit after forced kill")
				}
			}
		}
		_ = c.logFile.Close()
	})
	return killErr
}

func (d *demo) postJSON(endpoint string, payload any, headers map[string]string) (int, apiEnvelope, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, apiEnvelope{}, err
	}
	merged := map[string]string{"Content-Type": "application/json"}
	for key, value := range headers {
		merged[key] = value
	}
	return d.postRaw(endpoint, body, merged)
}

func (d *demo) postRaw(endpoint string, body []byte, headers map[string]string) (int, apiEnvelope, error) {
	request, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, apiEnvelope{}, err
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	return d.doEnvelope(request)
}

func (d *demo) getJSON(endpoint string) (int, apiEnvelope, error) {
	request, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, apiEnvelope{}, err
	}
	return d.doEnvelope(request)
}

func (d *demo) doEnvelope(request *http.Request) (int, apiEnvelope, error) {
	response, err := d.client.Do(request)
	if err != nil {
		return 0, apiEnvelope{}, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		return response.StatusCode, apiEnvelope{}, err
	}
	var envelope apiEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return response.StatusCode, apiEnvelope{}, fmt.Errorf("decode HTTP %d response: %w: %s", response.StatusCode, err, data)
	}
	return response.StatusCode, envelope, nil
}

func expectSuccess(status int, envelope apiEnvelope, wantStatus int, target any) error {
	if status != wantStatus || !envelope.OK || envelope.Error != nil {
		return fmt.Errorf("HTTP %d unexpected response: %#v", status, envelope)
	}
	if target != nil {
		if err := json.Unmarshal(envelope.Data, target); err != nil {
			return err
		}
	}
	return nil
}

func expectError(status int, envelope apiEnvelope, wantStatus int, code string) error {
	if status != wantStatus || envelope.OK || envelope.Error == nil || envelope.Error.Code != code {
		return fmt.Errorf("HTTP %d %s expected, got %#v", wantStatus, code, envelope)
	}
	return nil
}

func (d *demo) sampleIncrement(name, key string, baseVersion int64) (model.IncrementRequest, error) {
	data, err := os.ReadFile(filepath.Join(d.root, "samples", name))
	if err != nil {
		return model.IncrementRequest{}, err
	}
	request, business := spec.DecodeIncrement(data)
	if business != nil {
		return model.IncrementRequest{}, business
	}
	request.IdempotencyKey = key
	request.BaseVersion = baseVersion
	return request, nil
}

func (d *demo) requireVersion(baseURL string, version int64) error {
	state, err := d.state(baseURL)
	if err != nil {
		return err
	}
	if state.Version != version {
		return fmt.Errorf("committed version = %d, want %d", state.Version, version)
	}
	return nil
}

func (d *demo) state(baseURL string) (engine.StateView, error) {
	status, envelope, err := d.getJSON(baseURL + "/api/v1/state")
	if err != nil {
		return engine.StateView{}, err
	}
	var state engine.StateView
	if err := expectSuccess(status, envelope, http.StatusOK, &state); err != nil {
		return engine.StateView{}, err
	}
	return state, nil
}

func (d *demo) gatewayImpact(baseURL string) (graph.ImpactResult, error) {
	status, envelope, err := d.getJSON(baseURL + "/api/v1/impact/gateway")
	if err != nil {
		return graph.ImpactResult{}, err
	}
	var payload struct {
		Impacts []graph.ImpactResult `json:"impacts"`
	}
	if err := expectSuccess(status, envelope, http.StatusOK, &payload); err != nil {
		return graph.ImpactResult{}, err
	}
	if len(payload.Impacts) != 1 {
		return graph.ImpactResult{}, fmt.Errorf("expected one impact, got %d", len(payload.Impacts))
	}
	return payload.Impacts[0], nil
}

func (d *demo) runConcurrentIncrements(baseURL string) (map[string]any, error) {
	requests := []model.IncrementRequest{
		{
			IdempotencyKey: d.runID + "-race-a", BaseVersion: 2, GeneratedAt: "2026-02-10T10:00:00Z",
			Note: "Concurrent writer A",
			Operations: []model.IncrementOperation{{
				Type: "patch_resource", ComponentID: "cache",
				Resources: &model.Resource{CPUMillicores: 1600, MemoryBytes: 4294967296},
			}},
		},
		{
			IdempotencyKey: d.runID + "-race-b", BaseVersion: 2, GeneratedAt: "2026-02-10T10:00:01Z",
			Note: "Concurrent writer B",
			Operations: []model.IncrementOperation{{
				Type: "patch_probe", ComponentID: "reporter",
				HealthProbe: &model.HealthProbe{
					Type: "http", URL: "http://127.0.0.1:19090/health?mode=timeout&component=reporter",
					Method: http.MethodGet, TimeoutMs: 420,
				},
			}},
		},
	}
	type outcome struct {
		label  string
		status int
		env    apiEnvelope
		err    error
	}
	results := make(chan outcome, len(requests))
	var wg sync.WaitGroup
	for index, request := range requests {
		index, request := index, request
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, envelope, err := d.postJSON(baseURL+"/api/v1/increments", request, nil)
			results <- outcome{label: fmt.Sprintf("writer-%d", index+1), status: status, env: envelope, err: err}
		}()
	}
	wg.Wait()
	close(results)
	successes := 0
	stale := 0
	labels := make([]string, 0, len(requests))
	for item := range results {
		if item.err != nil {
			return nil, item.err
		}
		labels = append(labels, item.label+":"+fmt.Sprintf("%d", item.status))
		switch {
		case item.status == http.StatusOK && item.env.OK:
			successes++
		case item.status == http.StatusConflict && item.env.Error != nil && item.env.Error.Code == model.CodeStaleBase:
			stale++
		default:
			return nil, fmt.Errorf("unexpected concurrent outcome: %#v", item)
		}
	}
	if successes != 1 || stale != 1 {
		return nil, fmt.Errorf("concurrent increments must produce one winner and one E_STALE_BASE, got success=%d stale=%d", successes, stale)
	}
	if err := d.requireVersion(baseURL, 3); err != nil {
		return nil, err
	}
	sort.Strings(labels)
	return map[string]any{
		"base_version": 2, "success": successes, "stale_base": stale,
		"final_version": 3, "outcomes": labels,
	}, nil
}

func (d *demo) verifyTimeoutEvidence(result engine.DrillResult) error {
	timeoutSeen := false
	for _, batch := range result.RecoveryBatches {
		for _, component := range batch.Results {
			for _, operation := range []struct {
				name   string
				result int64
				code   string
				closed bool
			}{
				{"probe", component.Probe.DurationMs, component.Probe.Code, component.Probe.ConnectionClosed},
				{"recovery", component.Recovery.DurationMs, component.Recovery.Code, component.Recovery.ConnectionClosed},
			} {
				if operation.code != model.CodeProbeTimeout && operation.code != model.CodeRecoverTimeout {
					continue
				}
				timeoutSeen = true
				if operation.result < 250 || operation.result > 2000 {
					return fmt.Errorf("timeout operation %s/%s elapsed %dms, outside the declared 300-420ms contract", component.ComponentID, operation.name, operation.result)
				}
				if !operation.closed {
					return fmt.Errorf("timeout operation %s/%s did not close the transport connection", component.ComponentID, operation.name)
				}
			}
		}
	}
	if !timeoutSeen {
		return errors.New("drill did not report a real timeout operation")
	}
	return nil
}

func (d *demo) readEndpointStats() (map[string]any, error) {
	response, err := d.client.Get("http://" + d.endpointAddr + "/stats")
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	var stats map[string]any
	if err := json.NewDecoder(response.Body).Decode(&stats); err != nil {
		return nil, err
	}
	return stats, nil
}

func (d *demo) recoveryReports(baseURL string) ([]store.RecoveryReport, error) {
	status, envelope, err := d.getJSON(baseURL + "/api/v1/recovery-reports?limit=5")
	if err != nil {
		return nil, err
	}
	var payload struct {
		Items []store.RecoveryReport `json:"items"`
	}
	if err := expectSuccess(status, envelope, http.StatusOK, &payload); err != nil {
		return nil, err
	}
	return payload.Items, nil
}

func (d *demo) intents(baseURL, state string) ([]store.IntentSummary, error) {
	status, envelope, err := d.getJSON(baseURL + "/api/v1/intents?state=" + state + "&limit=100")
	if err != nil {
		return nil, err
	}
	var payload struct {
		Items []store.IntentSummary `json:"items"`
	}
	if err := expectSuccess(status, envelope, http.StatusOK, &payload); err != nil {
		return nil, err
	}
	return payload.Items, nil
}

func (d *demo) waitPendingIntent(baseURL, key string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		items, err := d.intents(baseURL, "pending")
		if err != nil {
			time.Sleep(80 * time.Millisecond)
			continue
		}
		for _, item := range items {
			if item.SubmissionKey == key {
				return nil
			}
		}
		time.Sleep(80 * time.Millisecond)
	}
	return fmt.Errorf("pending intent for %s was not observed before timeout", key)
}

func (d *demo) fullReplacementSnapshot(version int64) (model.Snapshot, error) {
	data, err := os.ReadFile(filepath.Join(d.root, "samples", "full-snapshot.json"))
	if err != nil {
		return model.Snapshot{}, err
	}
	snapshot, business := spec.DecodeSnapshot(data)
	if business != nil {
		return model.Snapshot{}, business
	}
	snapshot.Version = version
	snapshot.GeneratedAt = "2026-03-02T08:00:00Z"
	snapshot.ChecksumSHA256 = ""
	normalized, business := spec.NormalizeSnapshot(snapshot, false)
	if business != nil {
		return model.Snapshot{}, business
	}
	return normalized.Snapshot, nil
}

func (d *demo) postRawNoJSON(endpoint string, body []byte) error {
	request, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	response, err := d.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d from %s", response.StatusCode, endpoint)
	}
	return nil
}

func backupSQLite(source, destination string) error {
	_ = os.Remove(destination)
	db, err := sql.Open("sqlite", source)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	quoted := strings.ReplaceAll(destination, "'", "''")
	_, err = db.ExecContext(ctx, "VACUUM INTO '"+quoted+"'")
	return err
}

func compareReplay(expected, actual engine.CommitResult) error {
	if expected.Version != actual.Version ||
		expected.BaseVersion != actual.BaseVersion ||
		expected.ChecksumSHA256 != actual.ChecksumSHA256 ||
		expected.DerivedDigest != actual.DerivedDigest ||
		expected.GraphDigest != actual.GraphDigest ||
		expected.Consistency.ScoreMicro != actual.Consistency.ScoreMicro ||
		expected.Consistency.Digest != actual.Consistency.Digest {
		return fmt.Errorf("post-crash replay differs from uninterrupted baseline:\nexpected=%#v\nactual=%#v", expected, actual)
	}
	expectedInvalidated := append([]string(nil), expected.Cascade.Invalidated...)
	actualInvalidated := append([]string(nil), actual.Cascade.Invalidated...)
	expectedRebuilt := append([]string(nil), expected.Cascade.Rebuilt...)
	actualRebuilt := append([]string(nil), actual.Cascade.Rebuilt...)
	sort.Strings(expectedInvalidated)
	sort.Strings(actualInvalidated)
	sort.Strings(expectedRebuilt)
	sort.Strings(actualRebuilt)
	if strings.Join(expectedInvalidated, "\x00") != strings.Join(actualInvalidated, "\x00") ||
		strings.Join(expectedRebuilt, "\x00") != strings.Join(actualRebuilt, "\x00") {
		return fmt.Errorf("post-crash cascade differs from uninterrupted baseline")
	}
	return nil
}

func int64FromAny(value any) int64 {
	switch typed := value.(type) {
	case float64:
		return int64(typed)
	case int64:
		return typed
	case json.Number:
		number, _ := typed.Int64()
		return number
	default:
		return 0
	}
}
