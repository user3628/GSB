// Command pathcourt is the local interactive path-resolution adjudication
// workbench. All subcommands work against an isolated temporary directory and
// never touch system directories or external services.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	"pathcourt/internal/inject"
	"pathcourt/internal/report"
	"pathcourt/internal/run"
	"pathcourt/internal/sample"
	"pathcourt/internal/snapshot"
)

func main() {
	if err := dispatch(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "pathcourt:", err)
		os.Exit(1)
	}
}

func dispatch(args []string) error {
	if len(args) == 0 {
		usage()
		return errors.New("missing subcommand")
	}
	switch args[0] {
	case "doctor":
		return cmdDoctor(args[1:])
	case "generate":
		return cmdGenerate(args[1:])
	case "run":
		return cmdRun(args[1:])
	case "report":
		return cmdReport(args[1:])
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `pathcourt - local path resolution adjudication workbench

Usage:
  pathcourt doctor   [-workdir DIR]
  pathcourt generate [-workdir DIR] [-scenario S]
  pathcourt run      [-workdir DIR] [-scenario S] [-config FILE]
                     [-max-jumps N] [-max-concurrent N] [-max-paths N]
                     [-force-gap-version V] [-db PATH] [-report PATH]
  pathcourt report   [-db PATH] [-workdir DIR] [-report PATH]

Scenarios: baseline, escape, cycle, depth, dangling, snapshot-gap
`)
}

func cmdDoctor(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	workdir := fs.String("workdir", "", "temporary working directory (created under the OS temp dir when empty)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	dir, cleanup, err := resolveWorkdir(*workdir)
	if err != nil {
		return err
	}
	defer cleanup()
	caps, capErr := run.PrintCapabilities(dir)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(caps); err != nil {
		return err
	}
	if capErr != nil {
		fmt.Fprintln(os.Stderr, "notice:", capErr)
	}
	return nil
}

func cmdGenerate(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	workdir := fs.String("workdir", "", "temporary working directory")
	scenario := fs.String("scenario", "baseline", "fixture scenario")
	if err := fs.Parse(args); err != nil {
		return err
	}
	sc, err := sample.ParseScenario(*scenario)
	if err != nil {
		return err
	}
	dir, cleanup, err := resolveWorkdir(*workdir)
	if err != nil {
		return err
	}
	defer cleanup()
	out, err := run.Execute(context.Background(), run.Options{
		WorkDir: dir,
		Config:  inject.Config{Scenario: sc},
	})
	if err != nil {
		return err
	}
	fmt.Printf("workdir: %s\nroot:    %s\noutside: %s\nlinks:   %d\nreport:  %s\n",
		out.Workspace.Dir, out.Workspace.Root, out.Workspace.Outside, len(out.Workspace.Links), out.ReportPath)
	for _, note := range out.Workspace.Notes {
		fmt.Println("note:  ", note)
	}
	return out.Store.Close()
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	workdir := fs.String("workdir", "", "temporary working directory")
	scenario := fs.String("scenario", "baseline", "fixture scenario")
	configPath := fs.String("config", "", "JSON injection config file")
	maxJumps := fs.Int("max-jumps", 0, "reparse jump cap (0 = default 16)")
	maxConcurrent := fs.Int("max-concurrent", 0, "concurrent session cap")
	maxPaths := fs.Int("max-paths", 0, "per-session path budget")
	forceGap := fs.Int64("force-gap-version", 0, "force the post-run snapshot to this version (gap injection)")
	dbPath := fs.String("db", "", "SQLite database path")
	reportPath := fs.String("report", "", "write the JSON report to this path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := inject.Defaults()
	configProvided := false
	if *configPath != "" {
		loaded, err := inject.LoadFile(*configPath)
		if err != nil {
			return err
		}
		cfg = loaded
		configProvided = true
	}
	sc, err := sample.ParseScenario(*scenario)
	if err != nil {
		return err
	}
	// Explicit CLI scenario wins; otherwise keep the config-file scenario.
	if !configProvided || isFlagPassed(fs, "scenario") {
		cfg.Scenario = sc
	}
	if *maxJumps > 0 {
		cfg.MaxJumps = *maxJumps
	}
	if *maxConcurrent > 0 {
		cfg.MaxConcurrent = *maxConcurrent
	}
	if *maxPaths > 0 {
		cfg.MaxPathsPerSession = *maxPaths
	}
	if *forceGap > 0 {
		cfg.ForceSnapshotGap = *forceGap
		cfg.Scenario = sample.ScenarioSnapshot
	}

	dir, cleanup, err := resolveWorkdir(*workdir)
	if err != nil {
		return err
	}
	defer cleanup()

	out, err := run.Execute(context.Background(), run.Options{WorkDir: dir, Config: cfg, DBPath: *dbPath})
	if err != nil {
		return err
	}
	defer out.Store.Close()

	if *reportPath != "" {
		if err := os.WriteFile(*reportPath, append(out.ReportBytes, '\n'), 0o644); err != nil {
			return err
		}
	}

	fmt.Printf("scenario: %s\nworkdir:  %s\nreport:   %s\nsessions: %d (inflight=%d waiting=%d budget-hits=%d)\n",
		cfg.Scenario, out.Workspace.Dir, out.ReportPath,
		len(out.Outcomes), out.SessionStats.Inflight, out.SessionStats.Waiting, out.SessionStats.BudgetHits)
	for _, so := range out.Outcomes {
		fmt.Printf("  - %s: code=%s paths=%d committed=%d", so.ManifestID, so.Code, so.PathCount, so.Committed)
		if so.Version != nil {
			fmt.Printf(" version=%d", so.Version.Version)
		}
		fmt.Println()
	}
	if out.LoadErr != nil {
		if errors.Is(out.LoadErr, snapshot.ErrVersionGap) {
			fmt.Println("snapshot: rolled back to most recent contiguous version ->", out.LoadErr)
		} else {
			return out.LoadErr
		}
	}
	return nil
}

func cmdReport(args []string) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	dbPath := fs.String("db", "", "SQLite database path (required)")
	workdir := fs.String("workdir", "", "workspace root used for path aliasing")
	reportPath := fs.String("report", "", "output report path (stdout when empty)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" {
		return errors.New("-db is required")
	}
	ctx := context.Background()
	store, err := snapshot.Open(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	versions, loadErr := store.LoadChain(ctx)
	if loadErr != nil && !errors.Is(loadErr, snapshot.ErrVersionGap) {
		return loadErr
	}
	builder := newReportBuilder(*workdir)
	data, err := builder.Build(versions, loadErr)
	if err != nil {
		return err
	}
	if *reportPath == "" {
		_, err = os.Stdout.Write(data)
		return err
	}
	return os.WriteFile(*reportPath, append(data, '\n'), 0o644)
}

// resolveWorkdir creates a fresh isolated temp directory when the caller did
// not pin one. The cleanup is a no-op for pinned directories so generated
// fixtures remain inspectable.
func resolveWorkdir(pinned string) (string, func(), error) {
	if pinned != "" {
		if err := os.MkdirAll(pinned, 0o755); err != nil {
			return "", nil, err
		}
		return pinned, func() {}, nil
	}
	dir, err := os.MkdirTemp("", "pathcourt-")
	if err != nil {
		return "", nil, err
	}
	return dir, func() {}, nil
}

func newReportBuilder(workdir string) *report.Builder {
	return report.NewBuilder(workdir, report.DefaultWorkspaceAlias)
}

// isFlagPassed reports whether name was explicitly provided on the command
// line, distinguishing a real flag from its default value.
func isFlagPassed(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}
