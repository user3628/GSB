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
	"strings"
	"syscall"

	"recordlab/internal/workbench"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := execute(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func execute(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		printUsage(stderr)
		return errors.New("a subcommand is required")
	}
	switch args[0] {
	case "run":
		return runCommand(ctx, args[1:], stdout, stderr)
	case "versions":
		return versionsCommand(args[1:], stdout)
	case "edit":
		return editCommand(args[1:], stdout)
	case "replay":
		return replayCommand(args[1:], stdout)
	case "rejected":
		return rejectedCommand(args[1:], stdout)
	case "export":
		return exportCommand(args[1:], stdout)
	case "inject":
		return injectCommand(args[1:], stdout)
	case "sample":
		return sampleCommand(args[1:], stdout)
	case "help", "-h", "--help":
		printUsage(stdout)
		return nil
	default:
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

type configFlagValues struct {
	seed       uint64
	records    int
	payload    int
	maxDepth   int
	nested     int64
	global     int64
	task       int64
	scenario   string
	inject     bool
	configFile string
}

func addConfigFlags(fs *flag.FlagSet, values *configFlagValues) {
	fs.Uint64Var(&values.seed, "seed", 1, "deterministic sample seed")
	fs.IntVar(&values.records, "records", 3, "records per normal sample")
	fs.IntVar(&values.payload, "payload", 64, "base payload bytes")
	fs.IntVar(&values.maxDepth, "max-depth", 4, "maximum nested record depth")
	fs.Int64Var(&values.nested, "nested-budget", 4096, "maximum cumulative nested declaration bytes")
	fs.Int64Var(&values.global, "global-budget", 262144, "global declared-memory budget")
	fs.Int64Var(&values.task, "task-budget", 131072, "single-task memory budget")
	fs.StringVar(&values.scenario, "scenario", workbench.BaselineScenario, "task scenario")
	fs.BoolVar(&values.inject, "inject", false, "enable the selected injection scenario")
	fs.StringVar(&values.configFile, "config", "", "optional JSON configuration patch")
}

func resolveConfig(fs *flag.FlagSet, values configFlagValues, base workbench.Config) (workbench.Config, bool, error) {
	set := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	changed := false
	if values.configFile != "" {
		if err := applyConfigFile(&base, values.configFile); err != nil {
			return workbench.Config{}, false, err
		}
		changed = true
	}
	if set["seed"] {
		base.Seed = values.seed
		changed = true
	}
	if set["records"] {
		base.RecordCount = values.records
		changed = true
	}
	if set["payload"] {
		base.PayloadSize = values.payload
		changed = true
	}
	if set["max-depth"] {
		base.MaxDepth = values.maxDepth
		changed = true
	}
	if set["nested-budget"] {
		base.NestedBudget = values.nested
		changed = true
	}
	if set["global-budget"] {
		base.GlobalMemoryBudget = values.global
		changed = true
	}
	if set["task-budget"] {
		base.TaskMemoryBudget = values.task
		changed = true
	}
	if set["scenario"] {
		base.Scenario = values.scenario
		changed = true
	}
	if set["inject"] {
		base.Injection = values.inject
		changed = true
	}
	if err := base.Validate(); err != nil {
		return workbench.Config{}, false, err
	}
	return base, changed, nil
}

type configPatch struct {
	Seed               *uint64 `json:"seed"`
	RecordCount        *int    `json:"record_count"`
	PayloadSize        *int    `json:"payload_size"`
	MaxDepth           *int    `json:"max_depth"`
	NestedBudget       *int64  `json:"nested_budget"`
	GlobalMemoryBudget *int64  `json:"global_memory_budget"`
	TaskMemoryBudget   *int64  `json:"task_memory_budget"`
	Scenario           *string `json:"scenario"`
	Injection          *bool   `json:"injection"`
}

func applyConfigFile(cfg *workbench.Config, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var patch configPatch
	if err := json.Unmarshal(data, &patch); err != nil {
		return fmt.Errorf("decode config %s: %w", path, err)
	}
	if patch.Seed != nil {
		cfg.Seed = *patch.Seed
	}
	if patch.RecordCount != nil {
		cfg.RecordCount = *patch.RecordCount
	}
	if patch.PayloadSize != nil {
		cfg.PayloadSize = *patch.PayloadSize
	}
	if patch.MaxDepth != nil {
		cfg.MaxDepth = *patch.MaxDepth
	}
	if patch.NestedBudget != nil {
		cfg.NestedBudget = *patch.NestedBudget
	}
	if patch.GlobalMemoryBudget != nil {
		cfg.GlobalMemoryBudget = *patch.GlobalMemoryBudget
	}
	if patch.TaskMemoryBudget != nil {
		cfg.TaskMemoryBudget = *patch.TaskMemoryBudget
	}
	if patch.Scenario != nil {
		cfg.Scenario = *patch.Scenario
	}
	if patch.Injection != nil {
		cfg.Injection = *patch.Injection
	}
	return nil
}

func runCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", "recordlab.db", "SQLite state database")
	outPath := fs.String("out", "report.json", "JSON report path, or - for stdout")
	concurrency := fs.Int("concurrency", 4, "worker concurrency")
	stopAfter := fs.Int("stop-after", 0, "stop after N newly committed task snapshots")
	reset := fs.Bool("reset", false, "discard the existing snapshot chain before running")
	var values configFlagValues
	addConfigFlags(fs, &values)
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, cfgSet, err := resolveConfig(fs, values, workbench.DefaultConfig())
	if err != nil {
		return err
	}
	outcome, err := workbench.RunWorkbench(ctx, *dbPath, workbench.RunOptions{
		Concurrency: *concurrency,
		StopAfter:   *stopAfter,
		Reset:       *reset,
		Config:      cfg,
		ConfigSet:   cfgSet,
	})
	if err != nil {
		if outcome.Report.Format != "" {
			if data, marshalErr := workbench.MarshalReport(outcome.Report); marshalErr == nil && writeOutput(*outPath, data, stdout) == nil {
				fmt.Fprintf(stderr, "run: tasks=%d snapshots=%d new_tasks=%d interrupted=true report=%s\n",
					outcome.Report.Summary.TaskCount, outcome.Report.Summary.SnapshotCount, outcome.NewTasks, *outPath)
			}
		}
		return err
	}
	data, err := workbench.MarshalReport(outcome.Report)
	if err != nil {
		return err
	}
	if err := writeOutput(*outPath, data, stdout); err != nil {
		return err
	}
	summary := fmt.Sprintf("run: tasks=%d snapshots=%d new_tasks=%d interrupted=%t report=%s\n",
		outcome.Report.Summary.TaskCount, outcome.Report.Summary.SnapshotCount, outcome.NewTasks, outcome.Interrupted, *outPath)
	if *outPath == "-" {
		fmt.Fprint(stderr, summary)
	} else {
		fmt.Fprint(stdout, summary)
	}
	if outcome.WarningCode != "" {
		if *outPath == "-" {
			fmt.Fprintf(stderr, "snapshot_warning=%s\n", outcome.WarningCode)
		} else {
			fmt.Fprintf(stdout, "snapshot_warning=%s\n", outcome.WarningCode)
		}
	}
	return nil
}

func versionsCommand(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("versions", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", "recordlab.db", "SQLite state database")
	asJSON := fs.Bool("json", false, "emit JSON")
	outPath := fs.String("out", "-", "output path, or - for stdout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	load, err := workbench.LoadWorkspace(*dbPath)
	if err != nil {
		return err
	}
	if *asJSON {
		view := struct {
			Format        string                 `json:"format"`
			WarningCode   string                 `json:"warning_code,omitempty"`
			WarningDetail string                 `json:"warning_detail,omitempty"`
			Snapshots     []workbench.ChainEntry `json:"snapshots"`
		}{Format: "recordlab-chain-v1", WarningCode: load.WarningCode, WarningDetail: load.WarningDetail, Snapshots: []workbench.ChainEntry{}}
		for _, snapshot := range load.Valid {
			view.Snapshots = append(view.Snapshots, workbench.ChainEntry{
				Version: snapshot.Version, Kind: snapshot.Kind, ParentDigest: snapshot.ParentDigest,
				StateSHA256: snapshot.StateSHA256, ChainDigest: snapshot.ChainDigest,
			})
		}
		data, err := marshalIndented(view)
		if err != nil {
			return err
		}
		return writeOutput(*outPath, data, stdout)
	}
	var text strings.Builder
	for _, snapshot := range load.Valid {
		fmt.Fprintf(&text, "version=%d kind=%s state_sha256=%s chain_digest=%s\n", snapshot.Version, snapshot.Kind, snapshot.StateSHA256, snapshot.ChainDigest)
	}
	if load.WarningCode != "" {
		fmt.Fprintf(&text, "warning=%s detail=%s fallback_version=%d\n", load.WarningCode, load.WarningDetail, load.LatestVersion())
	}
	if text.Len() == 0 {
		text.WriteString("no snapshots\n")
	}
	return writeOutput(*outPath, []byte(text.String()), stdout)
}

func editCommand(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("edit", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", "recordlab.db", "SQLite state database")
	var values configFlagValues
	addConfigFlags(fs, &values)
	if err := fs.Parse(args); err != nil {
		return err
	}
	load, err := workbench.LoadWorkspace(*dbPath)
	if err != nil {
		return err
	}
	if !load.HasState {
		return errors.New("workspace has no committed state to edit")
	}
	next, changed, err := resolveConfig(fs, values, load.State.Config)
	if err != nil {
		return err
	}
	if !changed {
		return errors.New("edit requires at least one configuration flag or --config")
	}
	_, snapshot, err := workbench.EditWorkspace(*dbPath, next)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "edit: version=%d nested_budget=%d global_budget=%d task_budget=%d max_depth=%d scenario=%s\n",
		snapshot.Version, next.NestedBudget, next.GlobalMemoryBudget, next.TaskMemoryBudget, next.MaxDepth, next.Scenario)
	return nil
}

func replayCommand(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", "recordlab.db", "SQLite state database")
	taskID := fs.String("task", "", "task identifier")
	step := fs.Int("step", 0, "show a single 1-based trajectory event")
	asJSON := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *taskID == "" {
		return errors.New("replay requires --task")
	}
	report, _, err := workbench.WorkspaceReport(*dbPath)
	if err != nil {
		return err
	}
	task, ok := workbench.FindTaskReport(report, *taskID)
	if !ok {
		return fmt.Errorf("task %q is not present in the report", *taskID)
	}
	if *asJSON {
		if *step > 0 {
			if *step > len(task.Trajectory) {
				return fmt.Errorf("step %d is outside trajectory length %d", *step, len(task.Trajectory))
			}
			view := struct {
				TaskID string               `json:"task_id"`
				Step   int                  `json:"step"`
				Event  workbench.TraceEvent `json:"event"`
			}{TaskID: task.TaskID, Step: *step, Event: task.Trajectory[*step-1]}
			data, err := marshalIndented(view)
			if err != nil {
				return err
			}
			fmt.Fprint(stdout, string(data))
			return nil
		}
		data, err := marshalIndented(task)
		if err != nil {
			return err
		}
		fmt.Fprint(stdout, string(data))
		return nil
	}
	fmt.Fprintf(stdout, "task=%s final=%s admission=%s decode=%s decision=%s snapshot_version=%d\n", task.TaskID, task.FinalState, task.AdmissionCode, task.DecodeCode, task.DecisionCode, task.SnapshotVersion)
	fmt.Fprintf(stdout, "budget=declared:%d peak:%d allocations:%d global_reserved_after:%d\n", task.DeclaredBudget, task.PeakMemoryBytes, task.Allocations, task.GlobalReservedAfter)
	for _, record := range task.Records {
		fmt.Fprintf(stdout, "record=%d id=%d code=%s fields=%d children=%d depth=%d payload=%d peak=%d allocations=%d digest=%s\n",
			record.Index, record.RecordID, record.Code, record.FieldCount, record.ChildRecords, record.MaxDepth, record.PayloadBytes, record.PeakBytes, record.Allocations, record.Digest)
	}
	if *step > 0 {
		if *step > len(task.Trajectory) {
			return fmt.Errorf("step %d is outside trajectory length %d", *step, len(task.Trajectory))
		}
		event := task.Trajectory[*step-1]
		fmt.Fprintf(stdout, "step=%d seq=%d action=%s record=%d depth=%d offset=%d end=%d code=%s detail=%s\n",
			*step, event.Seq, event.Action, event.RecordIndex, event.Depth, event.Offset, event.EndOffset, event.Code, event.Detail)
		return nil
	}
	for index, event := range task.Trajectory {
		fmt.Fprintf(stdout, "step=%d seq=%d action=%s record=%d depth=%d offset=%d end=%d code=%s detail=%s\n",
			index+1, event.Seq, event.Action, event.RecordIndex, event.Depth, event.Offset, event.EndOffset, event.Code, event.Detail)
	}
	return nil
}

func rejectedCommand(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("rejected", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", "recordlab.db", "SQLite state database")
	asJSON := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	report, _, err := workbench.WorkspaceReport(*dbPath)
	if err != nil {
		return err
	}
	if *asJSON {
		view := struct {
			Tasks []workbench.TaskReport `json:"tasks"`
		}{Tasks: report.RejectedTasks}
		data, err := marshalIndented(view)
		if err != nil {
			return err
		}
		fmt.Fprint(stdout, string(data))
		return nil
	}
	if len(report.RejectedTasks) == 0 {
		fmt.Fprintln(stdout, "no rejected tasks")
		return nil
	}
	for _, task := range report.RejectedTasks {
		fmt.Fprintf(stdout, "task=%s code=%s state=%s declared_budget=%d peak=%d reason=%s\n", task.TaskID, task.DecisionCode, task.FinalState, task.DeclaredBudget, task.PeakMemoryBytes, task.RejectionReason)
	}
	return nil
}

func exportCommand(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", "recordlab.db", "SQLite state database")
	outPath := fs.String("out", "report.json", "JSON report path, or - for stdout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	report, _, err := workbench.WorkspaceReport(*dbPath)
	if err != nil {
		return err
	}
	data, err := workbench.MarshalReport(report)
	if err != nil {
		return err
	}
	if err := writeOutput(*outPath, data, stdout); err != nil {
		return err
	}
	if *outPath != "-" {
		fmt.Fprintf(stdout, "export: tasks=%d snapshots=%d report=%s\n", report.Summary.TaskCount, report.Summary.SnapshotCount, *outPath)
	}
	return nil
}

func injectCommand(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("inject", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dir := fs.String("dir", "", "writable temporary parent directory")
	outPath := fs.String("out", "injections.json", "injection verification JSON path, or - for stdout")
	kind := fs.String("kind", "all", "length_wrap, deep_nesting, task_budget, global_budget, snapshot_gap, or all")
	if err := fs.Parse(args); err != nil {
		return err
	}
	report, err := workbench.RunInjection(*dir, *kind)
	if err != nil {
		return err
	}
	data, err := marshalIndented(report)
	if err != nil {
		return err
	}
	if err := writeOutput(*outPath, data, stdout); err != nil {
		return err
	}
	if *outPath != "-" {
		fmt.Fprintf(stdout, "inject: cases=%d all_passed=%t report=%s\n", len(report.Cases), report.AllPassed, *outPath)
	}
	return nil
}

func sampleCommand(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("sample", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	kind := fs.String("kind", workbench.SampleValid, "sample kind")
	outPath := fs.String("out", "sample.bin", "binary sample path")
	var values configFlagValues
	addConfigFlags(fs, &values)
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, _, err := resolveConfig(fs, values, workbench.DefaultConfig())
	if err != nil {
		return err
	}
	spec := workbench.TaskSpec{TaskID: "sample-" + *kind, SampleKind: *kind}
	data := workbench.BuildSample(spec, cfg)
	if err := writeOutput(*outPath, data, stdout); err != nil {
		return err
	}
	if *outPath != "-" {
		fmt.Fprintf(stdout, "sample: kind=%s bytes=%d sha256=%s path=%s\n", *kind, len(data), workbench.ReportDigest(data), *outPath)
	}
	return nil
}

func writeOutput(path string, data []byte, stdout io.Writer) error {
	if path == "-" {
		_, err := stdout.Write(data)
		return err
	}
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, data, 0o644)
}

func marshalIndented(value any) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func printUsage(w io.Writer) {
	fmt.Fprint(w, `recordlab: deterministic untrusted-binary analysis workbench

usage:
  recordlab run      --db state.db --out report.json [--concurrency N] [--stop-after N] [config flags]
  recordlab versions --db state.db [--json]
  recordlab edit     --db state.db [config flags]
  recordlab replay   --db state.db --task ID [--step N] [--json]
  recordlab rejected --db state.db [--json]
  recordlab export   --db state.db --out report.json
  recordlab inject   [--dir DIR] [--kind all] --out injections.json
  recordlab sample   --kind valid --out sample.bin [config flags]

config flags:
  --seed --records --payload --max-depth --nested-budget
  --global-budget --task-budget --scenario --inject --config FILE
`)
}
