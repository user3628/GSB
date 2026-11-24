package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"driftconverge/internal/apperr"
	"driftconverge/internal/batch"
	"driftconverge/internal/detect"
	"driftconverge/internal/engine"
	"driftconverge/internal/model"
	"driftconverge/internal/simulator"
	"driftconverge/internal/store"
	"driftconverge/internal/web"
)

func runWeb(args []string) error {
	fs := flag.NewFlagSet("web", flag.ContinueOnError)
	var paths samplePaths
	addSampleFlags(fs, &paths)
	dbPath := fs.String("db", "", "optional persistent SQLite path; default is an isolated temp database")
	addr := fs.String("addr", "127.0.0.1:8787", "dashboard loopback listen address")
	mode := fs.String("simulator-mode", "normal", "normal|unresolved|timeout|fail")
	delay := fs.Int("write-delay-ms", 0, "simulator write delay")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !simulator.IsLoopbackListenAddr(*addr) {
		return apperr.New(apperr.InvalidArgument, "dashboard address must be loopback")
	}
	samples, err := model.LoadSamples(paths.objects, paths.policy, paths.values)
	if err != nil {
		return err
	}
	cleanup := func() {}
	if *dbPath == "" {
		dir, err := os.MkdirTemp("", "driftconverge-web-*")
		if err != nil {
			return err
		}
		*dbPath = filepath.Join(dir, "web.db")
		cleanup = func() { _ = os.RemoveAll(dir) }
	}
	defer cleanup()
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	startupRecovery, err := scanPendingIntents("web", st)
	if err != nil {
		return err
	}
	cfg := simulator.Config{ListenAddr: "127.0.0.1:0", Mode: *mode, WriteDelayMs: *delay}
	sim, err := simulator.New(samples.Cluster, cfg)
	if err != nil {
		return err
	}
	endpoint, err := sim.Start(cfg.ListenAddr)
	if err != nil {
		return err
	}
	defer sim.Close(context.Background())
	session := engine.NewSession(st, samples)
	analysis, analyzeErr := session.Analyze(context.Background())
	if analysis == nil {
		return analyzeErr
	}
	if analyzeErr != nil {
		fmt.Printf("analysis warning: %s\n", analyzeErr)
	}
	app := web.New(session, sim, endpoint, *mode)
	app.SetRecovery(startupRecovery)
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		return apperr.New(apperr.InvalidArgument, "listen dashboard: %v", err)
	}
	server := &http.Server{Handler: app.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "dashboard server error: %v\n", serveErr)
		}
	}()
	fmt.Printf("DriftLoom dashboard: http://%s\n", listener.Addr().String())
	fmt.Printf("embedded simulator: %s mode=%s\n", endpoint, *mode)
	fmt.Println("press Ctrl+C to stop")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}

type verificationCheck struct {
	Name   string `json:"name"`
	Pass   bool   `json:"pass"`
	Detail string `json:"detail"`
}

type verificationScenario struct {
	Name    string              `json:"name"`
	Pass    bool                `json:"pass"`
	Checks  []verificationCheck `json:"checks"`
	Error   string              `json:"error,omitempty"`
	Elapsed string              `json:"elapsed"`
}

type verificationReport struct {
	GeneratedAt string                 `json:"generatedAt"`
	Pass        bool                   `json:"pass"`
	Scenarios   []verificationScenario `json:"scenarios"`
}

func runVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	var paths samplePaths
	addSampleFlags(fs, &paths)
	cyclePath := fs.String("cycle-objects", "samples/cluster-cycle.json", "cycle fixture path")
	outPath := fs.String("out", "artifacts/verification.json", "verification JSON output path")
	largeCount := fs.Int("large-count", 12000, "scale-adaptive object count")
	exactLimit := fs.Int("large-exact-limit", 2500, "scale-adaptive exact-path limit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	report := verificationReport{GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), Pass: true}
	scenarios := []func() verificationScenario{
		func() verificationScenario { return verifyDriftAndConvergence(paths) },
		func() verificationScenario { return verifyCycle(paths, *cyclePath) },
		func() verificationScenario { return verifyRefused(paths) },
		func() verificationScenario { return verifyUnresolved(paths) },
		func() verificationScenario { return verifyTimeout(paths) },
		func() verificationScenario { return verifyQueueFull(paths) },
		func() verificationScenario { return verifyBudgetSkip(paths) },
		func() verificationScenario { return verifyCrashRecovery(paths) },
		func() verificationScenario { return verifyScale(*largeCount, *exactLimit) },
	}
	for _, run := range scenarios {
		start := time.Now()
		scenario := run()
		scenario.Elapsed = time.Since(start).String()
		if !scenario.Pass {
			report.Pass = false
		}
		report.Scenarios = append(report.Scenarios, scenario)
		printVerificationScenario(scenario)
	}
	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		return apperr.New(apperr.PersistenceFailed, "create verification output directory: %v", err)
	}
	encoded, _ := json.MarshalIndent(report, "", "  ")
	if err := os.WriteFile(*outPath, append(encoded, '\n'), 0o644); err != nil {
		return apperr.New(apperr.PersistenceFailed, "write verification report: %v", err)
	}
	fmt.Printf("verification %s; report=%s\n", passLabel(report.Pass), *outPath)
	if !report.Pass {
		return apperr.New(apperr.Internal, "one or more verification scenarios failed")
	}
	return nil
}

func verifyDriftAndConvergence(paths samplePaths) verificationScenario {
	scenario := newScenario("drift-batching-adaptive-convergence")
	defer finishScenario(&scenario)
	samples, st, cleanup, err := freshSession(paths)
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	defer cleanup()
	cfg := simulator.Config{ListenAddr: "127.0.0.1:0", Mode: "normal"}
	sim, err := simulator.New(samples.Cluster, cfg)
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	endpoint, err := sim.Start(cfg.ListenAddr)
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	defer sim.Close(context.Background())
	session := engine.NewSession(st, samples)
	analysis, err := session.Analyze(context.Background())
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	scenario.Checks = append(scenario.Checks,
		check("both paths reconciled all objects", analysis.Drift.Reconciled && analysis.Drift.ObjectsCompared == len(samples.Cluster.Objects), fmt.Sprintf("compared=%d reconciled=%v", analysis.Drift.ObjectsCompared, analysis.Drift.Reconciled)),
		check("both paths produced the same batch partition", analysis.BatchPathReconciled && analysis.ExactBatchSignature == analysis.BucketedBatchSignature && analysis.BatchSignature == analysis.ExactBatchSignature, fmt.Sprintf("exact=%s bucketed=%s selected=%s", analysis.ExactBatchSignature, analysis.BucketedBatchSignature, analysis.BatchSignature)),
		check("all four drift categories observed", observedCategories(analysis.Drift.Results), fmt.Sprintf("categories=%v", categoryCounts(analysis.Drift.Results))),
		check("source conflict is primary for web deployment", primaryFor(analysis.Drift.Results, "prod/deployment/web") == detect.SourceConflict, string(primaryFor(analysis.Drift.Results, "prod/deployment/web"))),
		check("float inside tolerance is not drift", !hasFieldDrift(analysis.Drift.Results, "prod/deployment/web", "ratio"), "ratio=0.7502 versus 0.75 at 1% relative tolerance"),
		check("clean objects excluded from write batches", cleanObjectsExcluded(analysis.Drift.Results, analysis.Batches.Batches), fmt.Sprintf("clean=%d batched=%d", analysis.Batches.Stats.CleanObjects, analysis.Batches.Stats.BatchedObjects)),
		check("batch size and dependency constraints", batchConstraintsHold(analysis.Batches.Batches, samples.Policy.BatchSizeLimit), fmt.Sprintf("batches=%d limit=%d", len(analysis.Batches.Batches), samples.Policy.BatchSizeLimit)),
		check("fixed-point plan exposes non-zero rounding propagation", analysis.Plan.TotalEstimatedCost != "" && analysis.Plan.TotalRoundingError != "" && analysis.Plan.TotalRoundingError != "0.000000", fmt.Sprintf("cost=%s rounding=%s", analysis.Plan.TotalEstimatedCost, analysis.Plan.TotalRoundingError)),
	)
	execution, runErr := session.ExecuteRound(context.Background(), engine.ExecuteOptions{RoundID: "verify-round-7", Endpoint: endpoint})
	scenario.Checks = append(scenario.Checks,
		check("normal writeback converges", runErr == nil && execution.State == "completed" && execution.ConvergedObjects > 0 && execution.UnresolvedObjects == 0 && execution.FailedObjects == 0, fmt.Sprintf("state=%s converged=%d error=%v", safeState(execution), safeConverged(execution), runErr)),
	)
	before := countWriteRecords(st)
	reused, reusedErr := session.ExecuteRound(context.Background(), engine.ExecuteOptions{RoundID: "verify-round-7", Endpoint: endpoint})
	after := countWriteRecords(st)
	scenario.Checks = append(scenario.Checks,
		check("round id is single-execution and reused", reusedErr == nil && reused != nil && reused.Reused && before == after, fmt.Sprintf("beforeRecords=%d afterRecords=%d reused=%v", before, after, reused != nil && reused.Reused)),
	)
	scenario.Pass = allChecksPass(scenario.Checks)
	return scenario
}

func verifyCycle(paths samplePaths, cyclePath string) verificationScenario {
	scenario := newScenario("dependency-cycle-partial-batches")
	defer finishScenario(&scenario)
	samples, err := model.LoadSamples(cyclePath, paths.policy, paths.values)
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	report, verifyErr := detect.Analyze(samples.Cluster, samples.Policy, samples.Values)
	if verifyErr != nil {
		scenario.Error = verifyErr.Error()
		return scenario
	}
	batchPkgOrganize(samples, report, func(batches []string, cycles []string, errCode string) {
		scenario.Checks = append(scenario.Checks,
			check("cycle returned E_DEPENDENCY_CYCLE", errCode == string(apperr.DependencyCycle), errCode),
			check("cycle members identified", strings.Join(cycles, ",") == "cycle/a,cycle/b,cycle/c", strings.Join(cycles, ",")),
			check("outside batches retained", strings.Contains(strings.Join(batches, ","), "outside/d") && strings.Contains(strings.Join(batches, ","), "outside/e"), strings.Join(batches, ",")),
		)
	})
	scenario.Pass = allChecksPass(scenario.Checks)
	return scenario
}

func verifyRefused(paths samplePaths) verificationScenario {
	scenario := newScenario("connection-refused")
	defer finishScenario(&scenario)
	samples, st, cleanup, err := freshSession(paths)
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	defer cleanup()
	session := engine.NewSession(st, samples)
	if _, err := session.Analyze(context.Background()); err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	deadAddr := reserveClosedAddress()
	execution, runErr := session.ExecuteRound(context.Background(), engine.ExecuteOptions{RoundID: "verify-refused-7", Endpoint: "http://" + deadAddr})
	scenario.Checks = append(scenario.Checks,
		check("refused endpoint classified", apperr.CodeOf(runErr) == apperr.EndpointRefused, fmt.Sprintf("code=%s error=%v", apperr.CodeOf(runErr), runErr)),
		check("failed writes are recorded", execution != nil && execution.FailedObjects > 0, fmt.Sprintf("failed=%d", safeFailed(execution))),
	)
	scenario.Pass = allChecksPass(scenario.Checks)
	return scenario
}

func verifyUnresolved(paths samplePaths) verificationScenario {
	scenario := newScenario("readback-unresolved")
	defer finishScenario(&scenario)
	samples, st, cleanup, err := freshSession(paths)
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	defer cleanup()
	cfg := simulator.Config{ListenAddr: "127.0.0.1:0", Mode: "unresolved"}
	sim, err := simulator.New(samples.Cluster, cfg)
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	endpoint, err := sim.Start(cfg.ListenAddr)
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	defer sim.Close(context.Background())
	session := engine.NewSession(st, samples)
	if _, err := session.Analyze(context.Background()); err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	execution, runErr := session.ExecuteRound(context.Background(), engine.ExecuteOptions{RoundID: "verify-unresolved-7", Endpoint: endpoint})
	scenario.Checks = append(scenario.Checks,
		check("HTTP success is not treated as convergence", execution != nil && execution.UnresolvedObjects > 0, fmt.Sprintf("unresolved=%d", safeUnresolved(execution))),
		check("residual categories are retained", residualCategoriesPresent(execution), residualSummary(execution)),
		check("partial round reports incomplete convergence", apperr.CodeOf(runErr) == apperr.ConvergenceIncomplete, string(apperr.CodeOf(runErr))),
	)
	scenario.Pass = allChecksPass(scenario.Checks)
	return scenario
}

func verifyTimeout(paths samplePaths) verificationScenario {
	scenario := newScenario("write-timeout")
	defer finishScenario(&scenario)
	samples, st, cleanup, err := freshSession(paths)
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	defer cleanup()
	samples.Policy.WriteTimeoutMs = 80
	samples.Policy.RetryLimit = 0
	samples.Policy.Checksum = samples.Policy.ComputeChecksum()
	cfg := simulator.Config{ListenAddr: "127.0.0.1:0", Mode: "timeout", WriteDelayMs: 220}
	sim, err := simulator.New(samples.Cluster, cfg)
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	endpoint, err := sim.Start(cfg.ListenAddr)
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	defer sim.Close(context.Background())
	session := engine.NewSession(st, samples)
	if _, err := session.Analyze(context.Background()); err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	execution, runErr := session.ExecuteRound(context.Background(), engine.ExecuteOptions{RoundID: "verify-timeout-7", Endpoint: endpoint})
	scenario.Checks = append(scenario.Checks,
		check("write timeout classified", apperr.CodeOf(runErr) == apperr.WriteTimeout, fmt.Sprintf("code=%s error=%v", apperr.CodeOf(runErr), runErr)),
		check("timed-out objects counted as failures", execution != nil && execution.FailedObjects > 0, fmt.Sprintf("failed=%d", safeFailed(execution))),
	)
	scenario.Pass = allChecksPass(scenario.Checks)
	return scenario
}

func verifyQueueFull(paths samplePaths) verificationScenario {
	scenario := newScenario("bounded-write-queue")
	defer finishScenario(&scenario)
	samples, st, cleanup, err := freshSession(paths)
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	defer cleanup()
	samples.Policy.BatchSizeLimit = 1
	samples.Policy.MaxParallelBatches = 1
	samples.Policy.WriteQueueCapacity = 0
	samples.Policy.Checksum = samples.Policy.ComputeChecksum()
	cfg := simulator.Config{ListenAddr: "127.0.0.1:0", Mode: "normal", WriteDelayMs: 100}
	sim, err := simulator.New(samples.Cluster, cfg)
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	endpoint, err := sim.Start(cfg.ListenAddr)
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	defer sim.Close(context.Background())
	session := engine.NewSession(st, samples)
	if _, err := session.Analyze(context.Background()); err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	execution, runErr := session.ExecuteRound(context.Background(), engine.ExecuteOptions{RoundID: "verify-queue-7", Endpoint: endpoint})
	scenario.Checks = append(scenario.Checks,
		check("bounded queue overflow classified", apperr.CodeOf(runErr) == apperr.WriteQueueFull, fmt.Sprintf("code=%s error=%v", apperr.CodeOf(runErr), runErr)),
		check("already started batches retain results", execution != nil && len(execution.ExecutedBatches) > 0, fmt.Sprintf("executed=%d skipped=%d", safeExecuted(execution), safeSkipped(execution))),
	)
	scenario.Pass = allChecksPass(scenario.Checks)
	return scenario
}

func verifyBudgetSkip(paths samplePaths) verificationScenario {
	scenario := newScenario("round-time-budget-skip")
	defer finishScenario(&scenario)
	samples, st, cleanup, err := freshSession(paths)
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	defer cleanup()
	samples.Policy.RoundTimeBudgetMs = 1
	samples.Policy.TimeBudgetRelativeTolerancePPM = 0
	samples.Policy.Checksum = samples.Policy.ComputeChecksum()
	session := engine.NewSession(st, samples)
	if _, err := session.Analyze(context.Background()); err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	execution, runErr := session.ExecuteRound(context.Background(), engine.ExecuteOptions{RoundID: "verify-budget-7", Endpoint: "http://127.0.0.1:1"})
	scenario.Checks = append(scenario.Checks,
		check("budget skip list is deterministic", execution != nil && len(execution.SkippedBatches) > 0 && len(execution.SkippedOrder) == len(execution.SkippedBatches), fmt.Sprintf("skipped=%d order=%v", safeSkipped(execution), safeSkippedOrder(execution))),
		check("budget exhaustion yields controlled incomplete round", apperr.CodeOf(runErr) == apperr.ConvergenceIncomplete, string(apperr.CodeOf(runErr))),
	)
	scenario.Pass = allChecksPass(scenario.Checks)
	return scenario
}

func verifyCrashRecovery(paths samplePaths) verificationScenario {
	scenario := newScenario("forced-process-crash-recovery")
	defer finishScenario(&scenario)
	samples, st, cleanup, err := freshSession(paths)
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	defer cleanup()
	session := engine.NewSession(st, samples)
	analysis, err := session.Analyze(context.Background())
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	dbPath := stDatabasePath(st)
	_ = st.Close()
	executable, err := os.Executable()
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	cmd := exec.Command(executable, "__crash-fixture", "--db", dbPath, "--round-id", "verify-crash-7", "--batch-version", fmt.Sprintf("%d", analysis.BatchVersionID))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	childErr := cmd.Run()
	var exitErr *exec.ExitError
	crashObserved := errors.As(childErr, &exitErr) && exitErr.ExitCode() == 91
	reopened, err := store.Open(dbPath)
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	defer reopened.Close()
	recovery, err := reopened.Recover(context.Background())
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	var pendingWriteState, batchState string
	_ = reopened.DB().QueryRow(`SELECT COALESCE((SELECT state FROM write_records ORDER BY record_id DESC LIMIT 1),'')`).Scan(&pendingWriteState)
	_ = reopened.DB().QueryRow(`SELECT COALESCE((SELECT state FROM batch_versions WHERE state='aborted' ORDER BY id DESC LIMIT 1),'')`).Scan(&batchState)
	scenario.Checks = append(scenario.Checks,
		check("child process exited abruptly with code 91", crashObserved, fmt.Sprintf("childErr=%v", childErr)),
		check("pending write rolled back to pending_writeback", recovery.PendingWrites >= 1 && pendingWriteState == "pending_writeback", fmt.Sprintf("pendingWrites=%d state=%s", recovery.PendingWrites, pendingWriteState)),
		check("staging batch marked aborted", recovery.AbortedBatchVersions >= 1 && batchState == "aborted", fmt.Sprintf("aborted=%d state=%s", recovery.AbortedBatchVersions, batchState)),
		check("interrupted round aborted", recovery.AbortedRounds >= 1, fmt.Sprintf("abortedRounds=%d", recovery.AbortedRounds)),
		check("last complete batch version retained", recovery.LastCompleteBatchVersion > 0, fmt.Sprintf("lastComplete=%d", recovery.LastCompleteBatchVersion)),
	)
	scenario.Pass = allChecksPass(scenario.Checks)
	return scenario
}

func verifyScale(count, exactLimit int) verificationScenario {
	scenario := newScenario("scale-adaptive-detection")
	defer finishScenario(&scenario)
	if count < 2 {
		count = 2
	}
	cluster, policy, err := generateStressCluster(count, 4)
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	policy.ExactObjectLimit = exactLimit
	values := &model.DriftValuesSample{Version: 1, Cases: []model.FieldValueCase{}}
	report, err := detect.Analyze(cluster, policy, values)
	if err != nil {
		scenario.Error = err.Error()
		return scenario
	}
	batches, batchPathsReconciled, batchErr := organizeStress(cluster, report, policy)
	scenario.Checks = append(scenario.Checks,
		check("large input selects bucketed path", report.SelectedPath == "bucketed", report.SelectedPath),
		check("all objects reconcile across paths", report.Reconciled && report.ObjectsCompared == count, fmt.Sprintf("compared=%d reconciled=%v", report.ObjectsCompared, report.Reconciled)),
		check("large input batch partitions reconcile", batchPathsReconciled, fmt.Sprintf("batchPathsReconciled=%v", batchPathsReconciled)),
		check("batch plan generated for drifted objects", batchErr == nil && batches > 0, fmt.Sprintf("batches=%d error=%v", batches, batchErr)),
	)
	scenario.Pass = allChecksPass(scenario.Checks)
	return scenario
}

func runCrashFixture(args []string) error {
	fs := flag.NewFlagSet("__crash-fixture", flag.ContinueOnError)
	dbPath := fs.String("db", "", "database path")
	roundID := fs.String("round-id", "crash-round", "round id")
	batchVersion := fs.Int64("batch-version", 0, "parent batch version id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	version, err := st.CurrentPolicyVersion(context.Background())
	if err != nil {
		return err
	}
	record, _, err := st.CreateRound(context.Background(), *roundID, version, *batchVersion)
	if err != nil {
		return err
	}
	if record == nil {
		return fmt.Errorf("cannot create crash round")
	}
	var objectID string
	if err := st.DB().QueryRow(`SELECT object_id FROM cluster_objects WHERE snapshot_version=? ORDER BY object_id LIMIT 1`, version).Scan(&objectID); err != nil {
		return err
	}
	_, err = st.BeginWriteAttempt(context.Background(), *roundID, "crash-batch", objectID, 1, map[string]any{"crash": true})
	if err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := st.DB().Exec(`INSERT INTO write_intents(intent_id,kind,round_id,state,payload,created_at,updated_at) VALUES(?,?,?,?,?,?,?)`,
		"crash-batch-intent", "BATCH_COMMIT", *roundID, "pending", `{"kind":"BATCH_COMMIT"}`, now, now); err != nil {
		return err
	}
	if _, err := st.DB().Exec(`INSERT INTO batch_versions(snapshot_version,round_id,intent_id,state,signature,payload,error_code,created_at) VALUES(?,?,?,?,?,?,?,?)`,
		version, *roundID, "crash-batch-intent", "staging", "crash-staging", `{}`, "", now); err != nil {
		return err
	}
	// Simulate SIGKILL: no defer, no close, no completion. The parent process
	// observes this non-zero exit and performs factual recovery.
	os.Exit(91)
	return nil
}

// Helpers shared by the verification scenarios.

func freshSession(paths samplePaths) (*model.Samples, *store.Store, func(), error) {
	samples, err := model.LoadSamples(paths.objects, paths.policy, paths.values)
	if err != nil {
		return nil, nil, nil, err
	}
	dir, err := os.MkdirTemp("", "driftconverge-verify-*")
	if err != nil {
		return nil, nil, nil, err
	}
	st, err := store.Open(filepath.Join(dir, "verify.db"))
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, nil, nil, err
	}
	return samples, st, func() { _ = st.Close(); _ = os.RemoveAll(dir) }, nil
}

func newScenario(name string) verificationScenario {
	return verificationScenario{Name: name, Pass: true, Checks: []verificationCheck{}}
}

func finishScenario(scenario *verificationScenario) {
	if scenario.Error != "" {
		scenario.Pass = false
	}
	if !allChecksPass(scenario.Checks) {
		scenario.Pass = false
	}
}

func check(name string, pass bool, detail string) verificationCheck {
	return verificationCheck{Name: name, Pass: pass, Detail: detail}
}

func allChecksPass(checks []verificationCheck) bool {
	for _, item := range checks {
		if !item.Pass {
			return false
		}
	}
	return true
}

func printVerificationScenario(scenario verificationScenario) {
	fmt.Printf("[%s] %s\n", passLabel(scenario.Pass), scenario.Name)
	for _, item := range scenario.Checks {
		fmt.Printf("  [%s] %s — %s\n", passLabel(item.Pass), item.Name, item.Detail)
	}
	if scenario.Error != "" {
		fmt.Printf("  error: %s\n", scenario.Error)
	}
}

func passLabel(pass bool) string {
	if pass {
		return "PASS"
	}
	return "FAIL"
}

func observedCategories(results []detect.ObjectDrift) bool {
	counts := categoryCounts(results)
	return counts[detect.SourceConflict] > 0 && counts[detect.ValueOffset] > 0 && counts[detect.FieldMissing] > 0 && counts[detect.FieldExtra] > 0
}

func categoryCounts(results []detect.ObjectDrift) map[detect.Category]int {
	counts := map[detect.Category]int{}
	for _, result := range results {
		for _, category := range result.Categories {
			counts[category]++
		}
	}
	return counts
}

func primaryFor(results []detect.ObjectDrift, id string) detect.Category {
	for _, result := range results {
		if result.ObjectID == id {
			return result.PrimaryCategory
		}
	}
	return ""
}

func hasFieldDrift(results []detect.ObjectDrift, id, field string) bool {
	for _, result := range results {
		if result.ObjectID != id {
			continue
		}
		for _, drift := range result.FieldDrifts {
			if drift.Field == field {
				return true
			}
		}
	}
	return false
}

func cleanObjectsExcluded(results []detect.ObjectDrift, batches []batch.Batch) bool {
	batched := map[string]bool{}
	for _, batch := range batches {
		for _, id := range batch.ObjectIDs {
			batched[id] = true
		}
	}
	for _, result := range results {
		if !result.HasDrift && batched[result.ObjectID] {
			return false
		}
	}
	return true
}

func batchConstraintsHold(batches []batch.Batch, limit int) bool {
	objectBatch := map[string]int{}
	for _, batch := range batches {
		if len(batch.ObjectIDs) > limit {
			return false
		}
		for _, id := range batch.ObjectIDs {
			objectBatch[id] = batch.Index
		}
	}
	for _, batch := range batches {
		for _, dep := range batch.DependsOnBatchIndexes {
			if dep >= batch.Index {
				return false
			}
		}
	}
	return true
}

func safeState(report *engine.ExecutionReport) string {
	if report == nil {
		return ""
	}
	return report.State
}

func safeConverged(report *engine.ExecutionReport) int {
	if report == nil {
		return 0
	}
	return report.ConvergedObjects
}

func safeUnresolved(report *engine.ExecutionReport) int {
	if report == nil {
		return 0
	}
	return report.UnresolvedObjects
}

func safeFailed(report *engine.ExecutionReport) int {
	if report == nil {
		return 0
	}
	return report.FailedObjects
}

func safeExecuted(report *engine.ExecutionReport) int {
	if report == nil {
		return 0
	}
	return len(report.ExecutedBatches)
}

func safeSkipped(report *engine.ExecutionReport) int {
	if report == nil {
		return 0
	}
	return len(report.SkippedBatches)
}

func safeSkippedOrder(report *engine.ExecutionReport) []string {
	if report == nil {
		return nil
	}
	return report.SkippedOrder
}

func residualCategoriesPresent(report *engine.ExecutionReport) bool {
	if report == nil {
		return false
	}
	for _, batch := range report.ExecutedBatches {
		for _, object := range batch.Objects {
			if len(object.ResidualCategories) > 0 {
				return true
			}
		}
	}
	return false
}

func residualSummary(report *engine.ExecutionReport) string {
	if report == nil {
		return "no report"
	}
	for _, batch := range report.ExecutedBatches {
		for _, object := range batch.Objects {
			if len(object.ResidualCategories) > 0 {
				return fmt.Sprintf("%s=%v", object.ObjectID, object.ResidualCategories)
			}
		}
	}
	return "none"
}

func countWriteRecords(st *store.Store) int {
	var count int
	_ = st.DB().QueryRow(`SELECT COUNT(*) FROM write_records`).Scan(&count)
	return count
}

func stDatabasePath(st *store.Store) string {
	return st.Path()
}

func batchPkgOrganize(samples *model.Samples, report *detect.Report, consume func([]string, []string, string)) {
	organized, err := batch.Organize(samples.Cluster, report.Results, samples.Policy)
	if organized == nil {
		consume(nil, nil, string(apperr.CodeOf(err)))
		return
	}
	batches := []string{}
	for _, b := range organized.Batches {
		batches = append(batches, b.ObjectIDs...)
	}
	cycles := []string{}
	for _, cycle := range organized.Cycles {
		cycles = append(cycles, cycle.ObjectIDs...)
	}
	consume(batches, cycles, organized.ErrorCode)
}
