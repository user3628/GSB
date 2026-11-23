package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"internalpki/workbench/internal/injection"
	"internalpki/workbench/internal/model"
	"internalpki/workbench/internal/samplegen"
	"internalpki/workbench/internal/workbench"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	if args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		usage()
		return
	}
	if strings.HasPrefix(args[0], "-") {
		args = append([]string{"audit"}, args...)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch args[0] {
	case "audit":
		err = runAudit(ctx, args[1:])
	case "generate":
		err = runGenerate(args[1:])
	case "verify-injections":
		err = runVerifyInjections(ctx, args[1:])
	default:
		usage()
		err = fmt.Errorf("unknown command %q", args[0])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "pkiaudit:", err)
		os.Exit(1)
	}
}

func runAudit(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("audit", flag.ContinueOnError)
	samplesDir := flags.String("samples", "samples", "sample suite directory")
	reportPath := flags.String("report", "audit-report.json", "JSON report path, or - for stdout")
	checkpointPath := flags.String("checkpoint", "audit-checkpoint.json", "checkpoint path; empty disables persistence")
	concurrency := flags.Int("concurrency", 4, "maximum concurrent audit workers")
	sampleCPU := flags.Duration("sample-cpu", 5*time.Millisecond, "deterministic per-sample CPU budget")
	globalCPU := flags.Duration("global-cpu", 500*time.Millisecond, "deterministic total CPU budget")
	memoryBudget := flags.Int64("memory", 128*1024*1024, "total memory budget in bytes")
	validationAt := flags.String("at", model.DefaultValidationAt, "fixed RFC3339 validation time")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected audit arguments: %s", strings.Join(flags.Args(), " "))
	}
	at, err := time.Parse(time.RFC3339, *validationAt)
	if err != nil {
		return fmt.Errorf("parse -at: %w", err)
	}
	options := workbench.Options{
		SamplesDir:      *samplesDir,
		CheckpointPath:  *checkpointPath,
		Concurrency:     *concurrency,
		SampleCPUBudget: *sampleCPU,
		GlobalCPUBudget: *globalCPU,
		MemoryBudget:    *memoryBudget,
		ValidationTime:  at,
	}
	report, err := workbench.Run(ctx, options)
	if err != nil {
		return err
	}
	data, err := workbench.MarshalReport(report)
	if err != nil {
		return err
	}
	if err := writeOutput(*reportPath, data); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "audit: total=%d completed=%d failed=%d degraded=%d findings=%d accounted=%t\n",
		report.Summary.Total, report.Summary.Completed, report.Summary.Failed,
		report.Summary.Degraded, report.Summary.Findings, report.Summary.FullyAccounted)
	return nil
}

func runGenerate(args []string) error {
	flags := flag.NewFlagSet("generate", flag.ContinueOnError)
	outDir := flags.String("out", "samples", "sample suite output directory")
	anomalies := flags.Bool("anomalies", true, "include malformed anomaly samples")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	paths, err := samplegen.Generate(*outDir, samplegen.Options{IncludeAnomalies: *anomalies})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "generated %d deterministic samples under %s\n", len(paths), *outDir)
	return nil
}

func runVerifyInjections(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("verify-injections", flag.ContinueOnError)
	outDir := flags.String("out", "injection-samples", "generated anomaly sample directory")
	reportPath := flags.String("report", "injection-report.json", "JSON report path, or - for stdout")
	checkpointPath := flags.String("checkpoint", "", "optional checkpoint path")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	result, err := injection.Verify(ctx, *outDir, *checkpointPath)
	if err != nil {
		return err
	}
	data, err := workbench.MarshalReport(result.Report)
	if err != nil {
		return err
	}
	if err := writeOutput(*reportPath, data); err != nil {
		return err
	}
	checksJSON, err := json.MarshalIndent(result.Checks, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "injection checks: passed=%t\n%s\n", result.Passed, checksJSON)
	return injection.RequirePassed(result)
}

func writeOutput(path string, data []byte) error {
	if path == "-" {
		_, err := os.Stdout.Write(data)
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create report directory: %w", err)
	}
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0o644); err != nil {
		return fmt.Errorf("write report temp file: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		if runtime.GOOS != "windows" {
			return fmt.Errorf("replace report: %w", err)
		}
		if removeErr := os.Remove(path); removeErr != nil && !os.IsNotExist(removeErr) {
			return fmt.Errorf("replace report on Windows: %w", err)
		}
		if renameErr := os.Rename(tempPath, path); renameErr != nil {
			return fmt.Errorf("replace report on Windows: %w", renameErr)
		}
	}
	return nil
}

func usage() {
	fmt.Fprintln(os.Stderr, `Usage:
  pkiaudit audit --samples DIR --report FILE [--checkpoint FILE] [--concurrency N]
                 [--sample-cpu DURATION] [--global-cpu DURATION] [--memory BYTES] [--at RFC3339]
  pkiaudit generate --out DIR [--anomalies=true]
  pkiaudit verify-injections --out DIR --report FILE

The audit is fully offline. roots.pem is the only trust anchor source.`)
}
