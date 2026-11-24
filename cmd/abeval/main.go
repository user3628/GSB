package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"abeval"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		code := exitCode(err)
		writeCommandError(os.Stderr, err)
		os.Exit(code)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		printUsage(stdout)
		return nil
	}

	switch args[0] {
	case "generate":
		return runGenerate(args[1:], stdout)
	case "import":
		return runImport(ctx, args[1:], stdout)
	case "advance":
		return runAdvance(ctx, args[1:], stdout)
	case "run":
		return runRun(ctx, args[1:], stdout, stderr)
	case "status":
		return runStatus(ctx, args[1:], stdout)
	case "version":
		return runVersion(ctx, args[1:], stdout)
	case "report":
		return runReport(ctx, args[1:], stdout)
	case "demo":
		return runDemo(ctx, args[1:], stdout)
	case "help", "-h", "--help":
		printUsage(stdout)
		return nil
	default:
		return abeval.NewError("E_UNKNOWN_COMMAND", "unknown command").
			WithStage("cli").WithDetail("command", args[0])
	}
}

func printUsage(writer io.Writer) {
	fmt.Fprintln(writer, `abeval - deterministic staged A/B experiment evaluator

Usage:
  abeval generate --scenario rollout [--seed N] [--version N] [--out sample.json]
  abeval import --db abeval.db [--batch ID] (--input sample.json | --scenario rollout)
  abeval advance --db abeval.db --batch ID [--writer ID]
  abeval run --db abeval.db --batch ID [--writer ID] [--stop-after-stage N] [--crash-after-stage N]
  abeval status --db abeval.db --batch ID
  abeval version --db abeval.db --batch ID
  abeval report --db abeval.db --batch ID [--out report.json]
  abeval demo [--out-dir artifacts/demo]

Scenarios: rollout, insufficient, rollback, recovery, rollback_ctr, rollback_latency`)
}

func runGenerate(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("generate", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	scenario := flags.String("scenario", abeval.ScenarioRollout, "built-in scenario")
	seed := flags.Int64("seed", abeval.DefaultSeed, "fixed generator seed")
	version := flags.Int64("version", 1, "dataset version")
	batchID := flags.String("batch-id", "", "override batch id")
	out := flags.String("out", "", "output file; empty means stdout")
	if err := flags.Parse(args); err != nil {
		return err
	}

	dataset, err := abeval.GenerateDataset(abeval.GenerateOptions{
		Scenario: *scenario, Seed: *seed, Version: *version, BatchID: *batchID,
	})
	if err != nil {
		return err
	}
	return writeJSONOrFile(stdout, *out, dataset)
}

func runImport(ctx context.Context, args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("import", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	database := flags.String("db", "abeval.db", "sqlite database path")
	batchID := flags.String("batch", "", "batch id")
	input := flags.String("input", "", "dataset JSON file")
	scenario := flags.String("scenario", "", "built-in scenario when input is absent")
	version := flags.Int64("version", 0, "override dataset version")
	if err := flags.Parse(args); err != nil {
		return err
	}

	dataset, err := loadDataset(*input, *scenario, *version)
	if err != nil {
		return err
	}
	store, err := abeval.OpenStore(*database)
	if err != nil {
		return err
	}
	defer store.Close()

	result, err := store.ImportDataset(ctx, *batchID, dataset)
	if err != nil {
		return err
	}
	return printJSON(stdout, result)
}

func loadDataset(input, scenario string, version int64) (abeval.Dataset, error) {
	var dataset abeval.Dataset
	if input != "" {
		data, err := os.ReadFile(input)
		if err != nil {
			return abeval.Dataset{}, err
		}
		if err := json.Unmarshal(data, &dataset); err != nil {
			return abeval.Dataset{}, abeval.NewError(abeval.CodeInvalidConfig, "invalid dataset JSON").
				WithStage("load_dataset").WithField("input").WithDetail("error", err.Error())
		}
	} else {
		generated, err := abeval.GenerateDataset(abeval.GenerateOptions{Scenario: scenario, Version: version})
		if err != nil {
			return abeval.Dataset{}, err
		}
		dataset = generated
	}
	if version > 0 {
		dataset.Version = version
	}
	return dataset, nil
}

func runAdvance(ctx context.Context, args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("advance", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	database := flags.String("db", "abeval.db", "sqlite database path")
	batchID := flags.String("batch", "", "batch id")
	writer := flags.String("writer", "cli-advance", "writer id for diagnostics")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *batchID == "" {
		return missingBatch()
	}
	store, err := abeval.OpenStore(*database)
	if err != nil {
		return err
	}
	defer store.Close()

	checkpoint, err := store.AdvanceStage(ctx, *batchID, *writer)
	if errors.Is(err, abeval.ErrCompleted) {
		status, statusErr := store.GetStatus(ctx, *batchID)
		if statusErr != nil {
			return statusErr
		}
		return printJSON(stdout, map[string]any{"already_completed": true, "status": status})
	}
	if err != nil {
		return err
	}
	return printJSON(stdout, checkpoint)
}

func runRun(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	database := flags.String("db", "abeval.db", "sqlite database path")
	batchID := flags.String("batch", "", "batch id")
	writer := flags.String("writer", "cli-run", "writer id for diagnostics")
	stopAfterStage := flags.Int("stop-after-stage", 0, "gracefully stop after this stage commits")
	crashAfterStage := flags.Int("crash-after-stage", 0, "fault injection: SIGKILL self immediately after this stage commits")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *batchID == "" {
		return missingBatch()
	}

	store, err := abeval.OpenStore(*database)
	if err != nil {
		return err
	}
	defer store.Close()

	if *crashAfterStage > 0 {
		return runWithForcedCrash(ctx, store, *batchID, *writer, *crashAfterStage, stdout)
	}

	runner := abeval.NewRunnerWithStopAfterStage(store, *batchID, *writer, *stopAfterStage)
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		received := <-signals
		fmt.Fprintf(stderr, "received %s; completing and checkpointing current stage\n", received)
		runner.RequestStop()
		second := <-signals
		fmt.Fprintf(stderr, "received second signal %s; exiting immediately\n", second)
		os.Exit(130)
	}()

	report, err := runner.Run(ctx)
	if errors.Is(err, abeval.ErrGracefulStop) {
		status, statusErr := store.GetStatus(ctx, *batchID)
		if statusErr != nil {
			return statusErr
		}
		if jsonErr := printJSON(stdout, map[string]any{"graceful_stopped": true, "status": status}); jsonErr != nil {
			return jsonErr
		}
		return nil
	}
	if err != nil {
		return err
	}
	return printJSON(stdout, map[string]any{
		"completed": true, "batch_id": report.BatchID, "decision": report.Decision,
		"decision_code": report.DecisionCode, "stage_count": len(report.Stages),
	})
}

func runWithForcedCrash(ctx context.Context, store *abeval.Store, batchID, writer string, crashAfterStage int, stdout io.Writer) error {
	for {
		checkpoint, err := store.AdvanceStage(ctx, batchID, writer)
		if errors.Is(err, abeval.ErrCompleted) {
			status, statusErr := store.GetStatus(ctx, batchID)
			if statusErr != nil {
				return statusErr
			}
			return printJSON(stdout, map[string]any{"already_completed": true, "status": status})
		}
		if err != nil {
			return err
		}
		if checkpoint.StageIndex == crashAfterStage {
			killSelfForFaultInjection()
			return nil
		}
	}
}

func runStatus(ctx context.Context, args []string, stdout io.Writer) error {
	store, batchID, err := openBatchQuery(args, "status")
	if err != nil {
		return err
	}
	defer store.Close()
	status, err := store.GetStatus(ctx, batchID)
	if err != nil {
		return err
	}
	return printJSON(stdout, status)
}

func runVersion(ctx context.Context, args []string, stdout io.Writer) error {
	store, batchID, err := openBatchQuery(args, "version")
	if err != nil {
		return err
	}
	defer store.Close()
	version, err := store.GetRevision(ctx, batchID)
	if err != nil {
		return err
	}
	return printJSON(stdout, version)
}

func runReport(ctx context.Context, args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("report", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	database := flags.String("db", "abeval.db", "sqlite database path")
	batchID := flags.String("batch", "", "batch id")
	out := flags.String("out", "", "report output file; empty means stdout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *batchID == "" {
		return missingBatch()
	}
	store, err := abeval.OpenStore(*database)
	if err != nil {
		return err
	}
	defer store.Close()
	report, err := store.ExportReport(ctx, *batchID)
	if err != nil {
		return err
	}
	if *out == "" {
		_, err = stdout.Write(report)
		return err
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		return err
	}
	return os.WriteFile(*out, report, 0o644)
}

func openBatchQuery(args []string, command string) (*abeval.Store, string, error) {
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	database := flags.String("db", "abeval.db", "sqlite database path")
	batchID := flags.String("batch", "", "batch id")
	if err := flags.Parse(args); err != nil {
		return nil, "", err
	}
	if *batchID == "" {
		return nil, "", missingBatch()
	}
	store, err := abeval.OpenStore(*database)
	if err != nil {
		return nil, "", err
	}
	return store, *batchID, nil
}

func missingBatch() error {
	return abeval.NewError(abeval.CodeInvalidConfig, "--batch is required").
		WithStage("cli").WithField("batch")
}

func printJSON(writer io.Writer, value any) error {
	data, err := abeval.PrettyJSON(value)
	if err != nil {
		return err
	}
	_, err = writer.Write(data)
	return err
}

func writeJSONOrFile(stdout io.Writer, path string, value any) error {
	data, err := abeval.PrettyJSON(value)
	if err != nil {
		return err
	}
	if path == "" {
		_, err = stdout.Write(data)
		return err
	}
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, data, 0o644)
}

func exitCode(err error) int {
	switch abeval.ErrorCode(err) {
	case abeval.CodeVersionRollback:
		return 10
	case abeval.CodeStaleCheckpoint:
		return 11
	case abeval.CodeNotFound:
		return 12
	case abeval.CodeBusy:
		return 13
	case abeval.CodeInvalidConfig, abeval.CodeInvalidEvent, abeval.CodeStageCoverage:
		return 2
	default:
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 1
	}
}

func writeCommandError(writer io.Writer, err error) {
	payload := map[string]any{"ok": false}
	if businessError, ok := abeval.AsError(err); ok {
		payload["error"] = businessError
	} else {
		payload["error"] = map[string]string{"code": "E_INTERNAL", "message": err.Error()}
	}
	data, marshalErr := json.MarshalIndent(payload, "", "  ")
	if marshalErr != nil {
		fmt.Fprintf(writer, `{"ok":false,"error":{"code":"E_INTERNAL","message":%q}}`+"\n", err.Error())
		return
	}
	_, _ = fmt.Fprintln(writer, string(data))
}
