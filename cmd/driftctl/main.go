package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"driftconverge/internal/apperr"
	"driftconverge/internal/detect"
	"driftconverge/internal/engine"
	"driftconverge/internal/model"
	"driftconverge/internal/simulator"
	"driftconverge/internal/store"
)

const version = "1.0.0"

type samplePaths struct {
	objects string
	policy  string
	values  string
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	command := os.Args[1]
	var err error
	switch command {
	case "version", "--version", "-v":
		fmt.Printf("driftctl %s\n", version)
	case "checksum":
		err = runChecksum(os.Args[2:])
	case "validate":
		err = runValidate(os.Args[2:])
	case "analyze":
		err = runAnalyzeCommand(os.Args[2:])
	case "run":
		err = runRoundCommand(os.Args[2:])
	case "simulate":
		err = runSimulatorCommand(os.Args[2:])
	case "recover":
		err = runRecoverCommand(os.Args[2:])
	case "demo":
		err = runDemo(os.Args[2:])
	case "verify":
		err = runVerify(os.Args[2:])
	case "stress":
		err = runStress(os.Args[2:])
	case "web":
		err = runWeb(os.Args[2:])
	case "__crash-fixture":
		err = runCrashFixture(os.Args[2:])
	default:
		err = apperr.New(apperr.InvalidArgument, "unknown command %q", command)
	}
	if err != nil {
		printError(err)
		os.Exit(exitCode(err))
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `driftctl - deterministic drift convergence adjudicator

Commands:
  validate   validate samples and run both detection paths
  analyze    validate, persist snapshot/drift/batches, print the adjudication
  run        analyze then execute writeback against a local loopback endpoint
  simulate   run the deterministic local writeback/readback endpoint
  recover    scan durable intents and produce a recovery report
  demo       one-command normal convergence and refused-endpoint demonstration
  verify     run reproducible end-to-end acceptance scenarios
  stress     generate a large deterministic in-memory DAG and exercise adaptive detection
  web        start the interactive local dashboard
  checksum   compute a policy snapshot checksum

Every command accepts --help for its flags.`)
}

func addSampleFlags(fs *flag.FlagSet, paths *samplePaths) {
	fs.StringVar(&paths.objects, "objects", "samples/cluster-objects.json", "cluster object JSON path")
	fs.StringVar(&paths.policy, "policy", "samples/policy-snapshot.json", "policy snapshot JSON path")
	fs.StringVar(&paths.values, "values", "samples/drift-field-values.json", "drift field values sample path")
}

func runChecksum(args []string) error {
	fs := flag.NewFlagSet("checksum", flag.ContinueOnError)
	policyPath := fs.String("policy", "samples/policy-snapshot.json", "policy snapshot JSON or output path")
	writeBack := fs.Bool("write", false, "write the computed checksum into the policy file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	data, err := os.ReadFile(*policyPath)
	if err != nil {
		return apperr.New(apperr.SpecInvalid, "read policy: %v", err)
	}
	var policy model.Policy
	if err := json.Unmarshal(data, &policy); err != nil {
		return apperr.New(apperr.SpecInvalid, "decode policy: %v", err)
	}
	policy.Checksum = ""
	checksum := policy.ComputeChecksum()
	if *writeBack {
		policy.Checksum = checksum
		encoded, _ := json.MarshalIndent(policy, "", "  ")
		encoded = append(encoded, '\n')
		if err := os.WriteFile(*policyPath, encoded, 0o644); err != nil {
			return apperr.New(apperr.PersistenceFailed, "write policy: %v", err)
		}
		fmt.Println(checksum)
		return nil
	}
	fmt.Println(checksum)
	return nil
}

func runValidate(args []string) error {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	var paths samplePaths
	addSampleFlags(fs, &paths)
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	samples, err := model.LoadSamples(paths.objects, paths.policy, paths.values)
	if err != nil {
		return err
	}
	report, err := detect.Analyze(samples.Cluster, samples.Policy, samples.Values)
	if err != nil {
		return err
	}
	summary := analysisSummary(samples, report)
	if *jsonOut {
		return printJSON(map[string]any{"ok": true, "summary": summary, "drift": report})
	}
	printHumanAnalysis(samples, report)
	return nil
}

func runAnalyzeCommand(args []string) error {
	fs := flag.NewFlagSet("analyze", flag.ContinueOnError)
	var paths samplePaths
	addSampleFlags(fs, &paths)
	dbPath := fs.String("db", "artifacts/analyze.db", "SQLite database path")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	if _, err := scanPendingIntents("analyze", st); err != nil {
		return err
	}
	samples, err := model.LoadSamples(paths.objects, paths.policy, paths.values)
	if err != nil {
		return err
	}
	session := engine.NewSession(st, samples)
	report, analyzeErr := session.Analyze(context.Background())
	if *jsonOut {
		if analyzeErr != nil {
			_ = printJSON(map[string]any{"ok": false, "error": errorValue(analyzeErr), "analysis": report})
		} else if err := printJSON(report); err != nil {
			return err
		}
		return analyzeErr
	}
	if report != nil {
		printAnalysisReport(report)
	}
	return analyzeErr
}

func runRoundCommand(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	var paths samplePaths
	addSampleFlags(fs, &paths)
	dbPath := fs.String("db", "artifacts/round.db", "SQLite database path")
	endpoint := fs.String("endpoint", "http://127.0.0.1:19090", "loopback simulator endpoint")
	roundID := fs.String("round-id", "", "stable round identifier")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *roundID == "" {
		*roundID = fmt.Sprintf("round-%d", time.Now().UnixNano())
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	if _, err := scanPendingIntents("run", st); err != nil {
		return err
	}
	samples, err := model.LoadSamples(paths.objects, paths.policy, paths.values)
	if err != nil {
		return err
	}
	session := engine.NewSession(st, samples)
	analysis, analyzeErr := session.Analyze(context.Background())
	if analyzeErr != nil {
		return analyzeErr
	}
	report, runErr := session.ExecuteRound(context.Background(), engine.ExecuteOptions{RoundID: *roundID, Endpoint: *endpoint})
	if *jsonOut {
		if runErr != nil {
			_ = printJSON(map[string]any{"ok": false, "error": errorValue(runErr), "analysis": analysis, "execution": report})
		} else if err := printJSON(map[string]any{"ok": true, "analysis": analysis, "execution": report}); err != nil {
			return err
		}
		return runErr
	}
	printAnalysisReport(analysis)
	printExecutionReport(report)
	return runErr
}

func runSimulatorCommand(args []string) error {
	fs := flag.NewFlagSet("simulate", flag.ContinueOnError)
	var paths samplePaths
	addSampleFlags(fs, &paths)
	configPath := fs.String("config", "samples/simulator-config.json", "simulator config JSON path (optional)")
	addr := fs.String("addr", "", "override loopback listen address")
	mode := fs.String("mode", "", "override mode: normal|unresolved|timeout|fail")
	delay := fs.Int("write-delay-ms", -1, "override write delay in milliseconds")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg := simulator.Config{ListenAddr: "127.0.0.1:19090", Mode: "normal"}
	if *configPath != "" {
		if loaded, err := simulator.LoadConfig(*configPath); err == nil {
			cfg = loaded
		} else if !errors.Is(err, os.ErrNotExist) {
			return apperr.New(apperr.InvalidArgument, "load simulator config: %v", err)
		}
	}
	if *addr != "" {
		cfg.ListenAddr = *addr
	}
	if *mode != "" {
		cfg.Mode = *mode
	}
	if *delay >= 0 {
		cfg.WriteDelayMs = *delay
	}
	samples, err := model.LoadSamples(paths.objects, paths.policy, paths.values)
	if err != nil {
		return err
	}
	sim, err := simulator.New(samples.Cluster, cfg)
	if err != nil {
		return apperr.New(apperr.InvalidArgument, "create simulator: %v", err)
	}
	endpoint, err := sim.Start(cfg.ListenAddr)
	if err != nil {
		return apperr.New(apperr.InvalidArgument, "start simulator: %v", err)
	}
	defer sim.Close(context.Background())
	fmt.Printf("simulator listening on %s (mode=%s)\n", endpoint, cfg.Mode)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	fmt.Println("simulator stopping")
	return nil
}

func runRecoverCommand(args []string) error {
	fs := flag.NewFlagSet("recover", flag.ContinueOnError)
	dbPath := fs.String("db", "artifacts/demo.db", "SQLite database path")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	report, err := st.Recover(context.Background())
	if err != nil {
		return err
	}
	if *jsonOut {
		return printJSON(report)
	}
	fmt.Printf("Recovery scanned=%d committed=%d rolledBack=%d pendingWrites=%d abortedBatches=%d abortedRounds=%d lastCompleteBatch=%d\n",
		report.ScannedIntents, report.CommittedIntents, report.RolledBackIntents, report.PendingWrites,
		report.AbortedBatchVersions, report.AbortedRounds, report.LastCompleteBatchVersion)
	for _, action := range report.Actions {
		fmt.Printf("  %s %s observed=%s action=%s result=%s\n", action.Kind, action.IntentID, action.Observed, action.Action, action.Result)
	}
	return nil
}

func runDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	var paths samplePaths
	addSampleFlags(fs, &paths)
	dbPath := fs.String("db", "", "optional persistent SQLite path; default is an isolated temp database")
	outDir := fs.String("out", "artifacts/demo", "directory for JSON demonstration artifacts")
	mode := fs.String("simulator-mode", "normal", "normal|unresolved|timeout|fail")
	delay := fs.Int("write-delay-ms", 0, "simulator write delay")
	skipRefused := fs.Bool("skip-refused", false, "skip the connection-refused scenario")
	if err := fs.Parse(args); err != nil {
		return err
	}
	samples, err := model.LoadSamples(paths.objects, paths.policy, paths.values)
	if err != nil {
		return err
	}
	cleanup := func() {}
	if *dbPath == "" {
		dir, err := os.MkdirTemp("", "driftconverge-demo-*")
		if err != nil {
			return err
		}
		*dbPath = filepath.Join(dir, "demo.db")
		cleanup = func() { _ = os.RemoveAll(dir) }
	}
	defer cleanup()
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	startupRecovery, err := scanPendingIntents("demo", st)
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
	analysis, err := session.Analyze(context.Background())
	if err != nil {
		return err
	}
	roundID := fmt.Sprintf("demo-round-%d", samples.Policy.Version)
	execution, runErr := session.ExecuteRound(context.Background(), engine.ExecuteOptions{RoundID: roundID, Endpoint: endpoint})
	if runErr != nil && execution == nil {
		return runErr
	}
	reused, reusedErr := session.ExecuteRound(context.Background(), engine.ExecuteOptions{RoundID: roundID, Endpoint: endpoint})
	var refused *engine.ExecutionReport
	var refusedErr error
	if !*skipRefused {
		deadAddr := reserveClosedAddress()
		refused, refusedErr = session.ExecuteRound(context.Background(), engine.ExecuteOptions{
			RoundID: fmt.Sprintf("demo-refused-%d", samples.Policy.Version), Endpoint: "http://" + deadAddr,
		})
	}
	recovery, recoverErr := st.Recover(context.Background())
	if recoverErr != nil {
		return recoverErr
	}
	artifacts := map[string]any{
		"generatedAt":      time.Now().UTC().Format(time.RFC3339Nano),
		"database":         *dbPath,
		"simulator":        map[string]any{"endpoint": endpoint, "mode": *mode, "writeDelayMs": *delay},
		"analysis":         analysis,
		"execution":        execution,
		"executionError":   optionalError(runErr),
		"reusedExecution":  reused,
		"reusedError":      optionalError(reusedErr),
		"refusedExecution": refused,
		"refusedError":     optionalError(refusedErr),
		"startupRecovery":  startupRecovery,
		"recovery":         recovery,
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		return apperr.New(apperr.PersistenceFailed, "create demo output directory: %v", err)
	}
	for name, value := range map[string]any{"analysis": analysis, "execution": execution, "reused": reused, "refused": refused, "startup-recovery": startupRecovery, "recovery": recovery, "summary": artifacts} {
		encoded, _ := json.MarshalIndent(value, "", "  ")
		if err := os.WriteFile(filepath.Join(*outDir, name+".json"), append(encoded, '\n'), 0o644); err != nil {
			return apperr.New(apperr.PersistenceFailed, "write %s artifact: %v", name, err)
		}
	}
	printAnalysisReport(analysis)
	printExecutionReport(execution)
	if runErr != nil {
		fmt.Printf("normal scenario outcome: %s\n", errorLine(runErr))
	}
	if reused != nil {
		fmt.Printf("idempotency replay: reused=%v state=%s convergedObjects=%d\n", reused.Reused, reused.State, reused.ConvergedObjects)
	}
	if refused != nil {
		fmt.Printf("refused-endpoint scenario: state=%s errorCode=%s failedObjects=%d error=%s\n", refused.State, refused.ErrorCode, refused.FailedObjects, errorLine(refusedErr))
	}
	fmt.Printf("recovery: scanned=%d rolledBack=%d pendingWrites=%d abortedRounds=%d\n", recovery.ScannedIntents, recovery.RolledBackIntents, recovery.PendingWrites, recovery.AbortedRounds)
	fmt.Printf("artifacts written to %s\n", *outDir)
	if runErr != nil {
		return runErr
	}
	return nil
}

func scanPendingIntents(label string, st *store.Store) (*store.RecoveryReport, error) {
	report, err := st.Recover(context.Background())
	if err != nil {
		return nil, err
	}
	if report.ScannedIntents > 0 {
		fmt.Fprintf(os.Stderr, "%s startup recovery: scanned=%d committed=%d rolledBack=%d pendingWrites=%d abortedBatches=%d abortedRounds=%d\n",
			label, report.ScannedIntents, report.CommittedIntents, report.RolledBackIntents, report.PendingWrites,
			report.AbortedBatchVersions, report.AbortedRounds)
	}
	return report, nil
}

func reserveClosedAddress() string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "127.0.0.1:1"
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	return addr
}

func analysisSummary(samples *model.Samples, report *detect.Report) map[string]any {
	driftCount := 0
	for _, result := range report.Results {
		if result.HasDrift {
			driftCount++
		}
	}
	return map[string]any{
		"snapshotVersion":  samples.Cluster.SnapshotVersion,
		"checksum":         samples.Policy.Checksum,
		"objects":          len(samples.Cluster.Objects),
		"driftObjects":     driftCount,
		"cleanObjects":     len(samples.Cluster.Objects) - driftCount,
		"path":             report.SelectedPath,
		"pathReconciled":   report.Reconciled,
		"valueCasesPassed": len(report.ValueCaseChecks),
	}
}

func printHumanAnalysis(samples *model.Samples, report *detect.Report) {
	summary := analysisSummary(samples, report)
	fmt.Printf("snapshot v%d checksum=%s\n", summary["snapshotVersion"], summary["checksum"])
	fmt.Printf("objects=%d drift=%d clean=%d selectedPath=%s reconciled=%v\n", summary["objects"], summary["driftObjects"], summary["cleanObjects"], summary["path"], summary["pathReconciled"])
	fmt.Printf("detection durations: exact=%dus bucket=%dus\n", report.ExactDurationUs, report.BucketDurationUs)
	for _, bucket := range report.Buckets {
		fmt.Printf("  bucket %-16s objects=%d sample=%v\n", bucket.Category, bucket.ObjectCnt, bucket.SampleIDs)
	}
	for _, result := range report.Results {
		if !result.HasDrift {
			continue
		}
		fields := make([]string, 0, len(result.FieldDrifts))
		for _, drift := range result.FieldDrifts {
			fields = append(fields, fmt.Sprintf("%s:%s", drift.Category, drift.Field))
		}
		fmt.Printf("  drift %-28s primary=%-15s duration=%dus %s\n", result.ObjectID, result.PrimaryCategory, result.DurationMicros, strings.Join(fields, ","))
	}
}

func printAnalysisReport(report *engine.AnalysisReport) {
	if report == nil {
		return
	}
	fmt.Printf("analysis snapshot v%d objects=%d drift=%d path=%s reconciled=%v batchVersion=%d\n",
		report.SnapshotVersion, report.ObjectCount, report.DriftObjectCount, report.DetectionPath, report.Drift.Reconciled, report.BatchVersionID)
	fmt.Printf("path batch signatures: exact=%s bucketed=%s reconciled=%v\n", report.ExactBatchSignature, report.BucketedBatchSignature, report.BatchPathReconciled)
	fmt.Printf("batch signature=%s batches=%d clean=%d cyclic=%d\n", report.Batches.StableSignature, len(report.Batches.Batches), report.Batches.Stats.CleanObjects, report.Batches.Stats.CyclicObjects)
	if report.Plan != nil {
		fmt.Printf("cost estimate=%s budgetOccupancy=%s roundingError=%s\n", report.Plan.TotalEstimatedCost, report.Plan.TotalBudgetOccupancy, report.Plan.TotalRoundingError)
		for _, bp := range report.Plan.Batches {
			fmt.Printf("  batch-%04d objects=%v deps=%v cost=%s occupancy=%s\n", bp.Index, bp.ObjectIDs, bp.DependsOnBatchIndexes, bp.EstimatedCost, bp.BudgetOccupancy)
		}
	}
}

func printExecutionReport(report *engine.ExecutionReport) {
	if report == nil {
		return
	}
	fmt.Printf("round %s state=%s endpoint=%s reused=%v converged=%d unresolved=%d failed=%d\n",
		report.RoundID, report.State, report.Endpoint, report.Reused, report.ConvergedObjects, report.UnresolvedObjects, report.FailedObjects)
	fmt.Printf("budget acceptedEstimated=%s remaining=%s\n", report.Budget.EstimatedAcceptedCost, report.Budget.EstimatedRemaining)
	for _, batch := range report.ExecutedBatches {
		fmt.Printf("  executed %s state=%s severity=%d duration=%dus objects=%d error=%s\n", batch.BatchID, batch.State, batch.MaxSeverityRank, batch.DurationMicros, len(batch.Objects), batch.ErrorCode)
	}
	for _, skipped := range report.SkippedBatches {
		fmt.Printf("  skipped %s order=%d reason=%s detail=%s\n", skipped.BatchID, skipped.Order, skipped.Reason, skipped.Detail)
	}
}

func optionalError(err error) any {
	if err == nil {
		return nil
	}
	return errorValue(err)
}

func errorValue(err error) any {
	if appErr, ok := apperr.As(err); ok {
		return appErr
	}
	return apperr.Error{Code: apperr.Internal, Message: err.Error()}
}

func printError(err error) {
	if err == nil {
		return
	}
	encoded, _ := json.Marshal(errorValue(err))
	fmt.Fprintln(os.Stderr, string(encoded))
}

func errorLine(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func printJSON(value any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func exitCode(err error) int {
	code := apperr.CodeOf(err)
	switch code {
	case apperr.SpecInvalid, apperr.InvalidArgument:
		return 2
	case apperr.StaleSnapshot, apperr.DependencyCycle, apperr.PathDivergence:
		return 3
	case apperr.WriteQueueFull, apperr.WriteTimeout, apperr.EndpointRefused, apperr.ConvergenceIncomplete, apperr.RoundBudgetExhausted:
		return 4
	default:
		return 1
	}
}
