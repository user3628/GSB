package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"svcworkbench/internal/domain"
	"svcworkbench/internal/samplegen"
	"svcworkbench/internal/workbench"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = runCommand(os.Args[2:])
	case "demo":
		err = demoCommand(os.Args[2:])
	case "replay":
		err = replayCommand(os.Args[2:])
	case "trace":
		err = traceCommand(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		err = fmt.Errorf("unknown subcommand %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `service control adjudication workbench

Usage:
  svcworkbench run [options]
  svcworkbench demo [--target-format v1|v2] [--compact]
  svcworkbench replay --state-dir DIR [--version N] [options]
  svcworkbench trace --state-dir DIR [--version N] --service NAME [options]

Run options:
  --state-dir DIR          persistent state directory; omitted means a fresh temporary run
  --target-format v1|v2    binary policy target generation format (default v2)
  --priorities LIST        comma-separated source priority (default binary_policy,json_manifest,legacy_text)
  --max-concurrency N      maximum in-flight source sessions (default 3)
  --service-budget N       maximum records per source (default 128)
  --fault NAME             none|unknown-version|text-prefix-oob|unterminated-quote|unknown-account|declaration-conflict|snapshot-gap
  --crash-point merge|commit
  --crash-exit-code N      process exit code used by crash injection (default 86)
  --compact                emit compact JSON instead of indented JSON`)
}

type commonFlags struct {
	stateDir       string
	targetFormat   string
	priorities     string
	maxConcurrency int
	serviceBudget  int
	compact        bool
}

func addCommonFlags(fs *flag.FlagSet) *commonFlags {
	values := &commonFlags{}
	fs.StringVar(&values.stateDir, "state-dir", "", "state directory")
	fs.StringVar(&values.targetFormat, "target-format", "v2", "target binary format")
	fs.StringVar(&values.priorities, "priorities", "binary_policy,json_manifest,legacy_text", "source priorities")
	fs.IntVar(&values.maxConcurrency, "max-concurrency", 3, "source concurrency")
	fs.IntVar(&values.serviceBudget, "service-budget", 128, "per-source service budget")
	fs.BoolVar(&values.compact, "compact", false, "compact JSON")
	return values
}

func runCommand(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	common := addCommonFlags(fs)
	fault := fs.String("fault", samplegen.FaultNone, "fault injection")
	crashPoint := fs.String("crash-point", "", "crash point")
	crashExitCode := fs.Int("crash-exit-code", 86, "crash exit code")
	if err := fs.Parse(args); err != nil {
		return err
	}
	priorities, err := parsePriorities(common.priorities)
	if err != nil {
		return err
	}
	options := workbench.Options{
		StateDir:       common.stateDir,
		TargetFormat:   common.targetFormat,
		Priorities:     priorities,
		MaxConcurrency: common.maxConcurrency,
		ServiceBudget:  common.serviceBudget,
		Fault:          *fault,
		CrashPoint:     *crashPoint,
	}
	if *crashPoint != "" {
		options.Crash = func(point string) {
			os.Exit(*crashExitCode)
		}
	}
	outcome, err := workbench.Run(context.Background(), options)
	if err != nil {
		return err
	}
	if err := writeJSON(outcome.Report, common.compact); err != nil {
		return err
	}
	if outcome.Ephemeral {
		_ = os.RemoveAll(outcome.StateDir)
	}
	return nil
}

func demoCommand(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	targetFormat := fs.String("target-format", "v2", "target binary format")
	compact := fs.Bool("compact", false, "compact JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	output, err := workbench.Demo(context.Background(), *targetFormat)
	if err != nil {
		return err
	}
	return writeJSON(output, *compact)
}

func replayCommand(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	common := addCommonFlags(fs)
	version := fs.Int("version", 0, "snapshot version; 0 means active")
	if err := fs.Parse(args); err != nil {
		return err
	}
	priorities, err := parsePriorities(common.priorities)
	if err != nil {
		return err
	}
	output, err := workbench.Replay(context.Background(), common.stateDir, *version, common.targetFormat, priorities)
	if err != nil {
		return err
	}
	return writeJSON(output, common.compact)
}

func traceCommand(args []string) error {
	fs := flag.NewFlagSet("trace", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	common := addCommonFlags(fs)
	version := fs.Int("version", 0, "snapshot version; 0 means active")
	service := fs.String("service", "", "service name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*service) == "" {
		return fmt.Errorf("--service is required")
	}
	priorities, err := parsePriorities(common.priorities)
	if err != nil {
		return err
	}
	output, err := workbench.Trace(context.Background(), common.stateDir, *version, *service, common.targetFormat, priorities)
	if err != nil {
		return err
	}
	return writeJSON(output, common.compact)
}

func parsePriorities(value string) ([]domain.SourceKind, error) {
	if strings.TrimSpace(value) == "" {
		return append([]domain.SourceKind(nil), domain.DefaultSourcePriorities...), nil
	}
	parts := strings.Split(value, ",")
	out := make([]domain.SourceKind, 0, len(parts))
	seen := make(map[domain.SourceKind]bool)
	for _, part := range parts {
		kind, ok := domain.ParseSourceKind(part)
		if !ok {
			return nil, fmt.Errorf("unknown source priority %q", strings.TrimSpace(part))
		}
		if seen[kind] {
			return nil, fmt.Errorf("duplicate source priority %q", kind)
		}
		seen[kind] = true
		out = append(out, kind)
	}
	return out, nil
}

func writeJSON(value any, compact bool) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	if !compact {
		encoder.SetIndent("", "  ")
	}
	return encoder.Encode(value)
}
