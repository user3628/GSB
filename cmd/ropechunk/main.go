package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"ropechunk/internal/engine"
	"ropechunk/internal/httpapi"
	"ropechunk/internal/model"
	"ropechunk/internal/samples"
	"ropechunk/internal/store"
)

const defaultDB = "ropechunk.db"

func main() {
	if err := run(os.Args[1:]); err != nil {
		var app *model.AppError
		if errors.As(err, &app) {
			encoded, _ := json.MarshalIndent(map[string]any{"error": app}, "", "  ")
			fmt.Fprintln(os.Stderr, string(encoded))
		} else {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		printUsage()
		return nil
	}
	command, args := args[0], args[1:]
	switch command {
	case "demo":
		return runDemo(args)
	case "import":
		return runImport(args)
	case "compute":
		return runCompute(args, false)
	case "resume":
		return runCompute(args, true)
	case "status":
		return runStatus(args)
	case "verify":
		return runVerify(args)
	case "blocks":
		return runBlocks(args)
	case "budget":
		return runBudget(args)
	case "invalidations":
		return runInvalidations(args)
	case "versions":
		return runVersions(args)
	case "serve":
		return runServe(args)
	case "clean":
		return runClean(args)
	default:
		return fmt.Errorf("unknown command %q; run ropechunk help", command)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stdout, `ropechunk - streaming RoPE chunk operator with SQLite checkpoints

Usage:
  ropechunk demo         --db PATH [--bundle PATH] [--report PATH] [--clean]
  ropechunk import       --db PATH --file BUNDLE.json
  ropechunk compute      --db PATH [--stage all|rotation|score] [--shards a,b] [--note TEXT]
  ropechunk resume       same flags as compute
  ropechunk status       --db PATH [--shards a,b]
  ropechunk verify       --db PATH [--shards a,b]
  ropechunk blocks       --db PATH --shard ID [--stage rotation|score] [--batch N] [--head N] [--row-block N]
  ropechunk budget       --db PATH [--run-id ID] [--shard ID]
  ropechunk invalidations --db PATH [--shard ID]
  ropechunk versions     --db PATH
  ropechunk serve        --db PATH [--addr 127.0.0.1:8080]
  ropechunk clean        --db PATH

The demo is deterministic, generates four built-in shards, computes rotation and
score stages, verifies fixed-point references, exercises causal-only invalidation,
and then demonstrates a classified version rollback.`)
}

func dbFlags(fs *flag.FlagSet) *string {
	return fs.String("db", defaultDB, "SQLite database path")
}

func prettyFlag(fs *flag.FlagSet) *bool {
	return fs.Bool("pretty", true, "pretty-print JSON output")
}

func openEngine(dbPath string) (*engine.Engine, *store.Store, error) {
	st, err := store.Open(dbPath)
	if err != nil {
		return nil, nil, err
	}
	return engine.New(st), st, nil
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if path == "" {
		_, err = os.Stdout.Write(append(data, '\n'))
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

func runDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	dbPath := dbFlags(fs)
	bundlePath := fs.String("bundle", filepath.Join("artifacts", "demo-bundle.json"), "write the deterministic v1 bundle here")
	reportPath := fs.String("report", filepath.Join("artifacts", "demo-report.json"), "write the demo report here")
	clean := fs.Bool("clean", true, "remove old DB/WAL/checkpoint files before running")
	pretty := prettyFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *clean {
		if err := store.Clean(*dbPath); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	bundle, err := samples.Generate()
	if err != nil {
		return err
	}
	if *bundlePath != "" {
		if err := writeJSON(*bundlePath, bundle); err != nil {
			return err
		}
	}
	eng, st, err := openEngine(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	phase1Import, err := eng.Import(ctx, bundle)
	if err != nil {
		return err
	}
	verification, err := eng.VerifyReferences(ctx, nil)
	if err != nil {
		return err
	}
	if !verification.AllMatch {
		return fmt.Errorf("deterministic references do not match the core rotation")
	}
	phase1Compute, err := eng.Compute(ctx, engine.ComputeOptions{Stage: "all", RunID: "demo-v1", Note: "demo initial full run"})
	if err != nil {
		return err
	}
	phase1Status, err := eng.Status(ctx, nil)
	if err != nil {
		return err
	}

	v2Cfg := samples.NextVersion(bundle.Config, 2, func(cfg *model.Config) { cfg.CausalMask = false })
	v2Bundle, err := samples.GenerateWithConfig(v2Cfg)
	if err != nil {
		return err
	}
	phase2Import, err := eng.Import(ctx, v2Bundle)
	if err != nil {
		return err
	}
	phase2Compute, err := eng.Compute(ctx, engine.ComputeOptions{Stage: "all", RunID: "demo-v2", Note: "causal mask changed; rotation should be reused"})
	if err != nil {
		return err
	}
	phase3Import, err := eng.Import(ctx, bundle)
	if err != nil {
		return err
	}
	phase3Compute, err := eng.Compute(ctx, engine.ComputeOptions{Stage: "all", RunID: "demo-rollback", Note: "rollback to v1"})
	if err != nil {
		return err
	}
	phase3Status, err := eng.Status(ctx, nil)
	if err != nil {
		return err
	}
	report := map[string]any{
		"format_version": 1,
		"seed":           samples.DefaultSeed,
		"shard_count":    len(bundle.Shards),
		"verification":   verification,
		"phase_v1": map[string]any{
			"import":  phase1Import,
			"compute": phase1Compute,
			"status":  phase1Status,
		},
		"phase_v2_causal_change": map[string]any{
			"import":  phase2Import,
			"compute": phase2Compute,
		},
		"phase_v3_rollback": map[string]any{
			"import":  phase3Import,
			"compute": phase3Compute,
			"status":  phase3Status,
		},
	}
	if err := writeJSON(*reportPath, report); err != nil {
		return err
	}
	if *pretty {
		finalCounts := make(map[string]int)
		for _, shard := range phase3Status.Shards {
			for _, stage := range shard.Stages {
				finalCounts[stage.Stage+"/complete"] += stage.CompleteRows
				finalCounts[stage.Stage+"/expected"] += stage.ExpectedRows
				finalCounts[stage.Stage+"/incomplete"] += stage.IncompleteRows
				finalCounts[stage.Stage+"/stale"] += stage.StaleRows
			}
		}
		summary := map[string]any{
			"shards":                  len(bundle.Shards),
			"verification_all_match":  verification.AllMatch,
			"v1_processed":            phase1Compute.Processed,
			"v1_reused":               phase1Compute.Reused,
			"v2_invalidations":        len(phase2Import.Invalidations),
			"v2_invalidation_stages":  invalidationStages(phase2Import.Invalidations),
			"v2_processed":            phase2Compute.Processed,
			"v2_reused":               phase2Compute.Reused,
			"rollback_transition":     phase3Import.Transition.Kind,
			"rollback_processed":      phase3Compute.Processed,
			"rollback_reused":         phase3Compute.Reused,
			"final_status_row_counts": finalCounts,
			"bundle":                  *bundlePath,
			"report":                  *reportPath,
			"database":                *dbPath,
		}
		return writeJSON("", summary)
	}
	return nil
}

func invalidationStages(events []model.InvalidationEvent) map[string]int {
	out := make(map[string]int)
	for _, event := range events {
		out[event.Stage]++
	}
	return out
}

func runImport(args []string) error {
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	dbPath := dbFlags(fs)
	file := fs.String("file", "", "bundle JSON path")
	pretty := prettyFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *file == "" {
		return fmt.Errorf("--file is required")
	}
	data, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	var bundle model.Bundle
	if err := json.Unmarshal(data, &bundle); err != nil {
		return err
	}
	eng, st, err := openEngine(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	report, err := eng.Import(context.Background(), bundle)
	if err != nil {
		return err
	}
	if *pretty {
		return writeJSON("", report)
	}
	data, _ = json.Marshal(report)
	fmt.Println(string(data))
	return nil
}

func runCompute(args []string, resume bool) error {
	name := "compute"
	if resume {
		name = "resume"
	}
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	dbPath := dbFlags(fs)
	stage := fs.String("stage", "all", "all, rotation, or score")
	shards := fs.String("shards", "", "comma-separated shard IDs")
	runID := fs.String("run-id", "", "run identifier")
	note := fs.String("note", "", "run note")
	pretty := prettyFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	eng, st, err := openEngine(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	if resume {
		eng.ResetStop()
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sig)
	done := make(chan struct{})
	go func() {
		select {
		case <-sig:
			eng.Stop()
			fmt.Fprintln(os.Stderr, "stop requested: current block will finish")
		case <-done:
		}
	}()
	report, computeErr := eng.Compute(context.Background(), engine.ComputeOptions{Stage: *stage, ShardIDs: splitCSV(*shards), RunID: *runID, Note: *note})
	close(done)
	if computeErr != nil {
		if *pretty {
			_ = writeJSON("", map[string]any{"error": computeErr, "report": report})
		}
		return computeErr
	}
	if *pretty {
		return writeJSON("", report)
	}
	data, _ := json.Marshal(report)
	fmt.Println(string(data))
	return nil
}

func runStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	dbPath := dbFlags(fs)
	shards := fs.String("shards", "", "comma-separated shard IDs")
	pretty := prettyFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	eng, st, err := openEngine(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	report, err := eng.Status(context.Background(), splitCSV(*shards))
	if err != nil {
		return err
	}
	if *pretty {
		return writeJSON("", report)
	}
	data, _ := json.Marshal(report)
	fmt.Println(string(data))
	return nil
}

func runVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	dbPath := dbFlags(fs)
	shards := fs.String("shards", "", "comma-separated shard IDs")
	pretty := prettyFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	eng, st, err := openEngine(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	report, err := eng.VerifyReferences(context.Background(), splitCSV(*shards))
	if err != nil {
		return err
	}
	if *pretty {
		return writeJSON("", report)
	}
	data, _ := json.Marshal(report)
	fmt.Println(string(data))
	return nil
}

func runBlocks(args []string) error {
	fs := flag.NewFlagSet("blocks", flag.ContinueOnError)
	dbPath := dbFlags(fs)
	shardID := fs.String("shard", "", "shard ID")
	stage := fs.String("stage", "", "rotation or score")
	batch := fs.Int("batch", -1, "batch filter")
	head := fs.Int("head", -1, "head filter")
	rowBlock := fs.Int("row-block", -1, "row block filter")
	pretty := prettyFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *shardID == "" {
		return fmt.Errorf("--shard is required")
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	var records []store.CheckpointRecord
	if *stage == "" {
		for _, candidate := range []string{model.StageRotation, model.StageScore} {
			part, err := st.ListCheckpoints(context.Background(), *shardID, candidate)
			if err != nil {
				return err
			}
			records = append(records, part...)
		}
	} else {
		records, err = st.ListCheckpoints(context.Background(), *shardID, *stage)
		if err != nil {
			return err
		}
	}
	type view struct {
		model.Checkpoint
		Result json.RawMessage `json:"result,omitempty"`
	}
	views := make([]view, 0, len(records))
	for _, rec := range records {
		if *batch >= 0 && rec.Batch != *batch || *head >= 0 && rec.Head != *head || *rowBlock >= 0 && rec.RowBlock != *rowBlock {
			continue
		}
		v := view{Checkpoint: rec.Checkpoint}
		if len(rec.ResultPayload) > 0 {
			v.Result = json.RawMessage(rec.ResultPayload)
		}
		views = append(views, v)
	}
	if *pretty {
		return writeJSON("", views)
	}
	data, _ := json.Marshal(views)
	fmt.Println(string(data))
	return nil
}

func runBudget(args []string) error {
	fs := flag.NewFlagSet("budget", flag.ContinueOnError)
	dbPath := dbFlags(fs)
	runID := fs.String("run-id", "", "run filter")
	shardID := fs.String("shard", "", "shard filter")
	pretty := prettyFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	events, err := st.ListBudgetEvents(context.Background(), *runID)
	if err != nil {
		return err
	}
	if *shardID != "" {
		filtered := events[:0]
		for _, event := range events {
			if event.ShardID == *shardID {
				filtered = append(filtered, event)
			}
		}
		events = filtered
	}
	if *pretty {
		return writeJSON("", events)
	}
	data, _ := json.Marshal(events)
	fmt.Println(string(data))
	return nil
}

func runInvalidations(args []string) error {
	fs := flag.NewFlagSet("invalidations", flag.ContinueOnError)
	dbPath := dbFlags(fs)
	shardID := fs.String("shard", "", "shard filter")
	pretty := prettyFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	events, err := st.ListInvalidations(context.Background(), *shardID)
	if err != nil {
		return err
	}
	if *pretty {
		return writeJSON("", events)
	}
	data, _ := json.Marshal(events)
	fmt.Println(string(data))
	return nil
}

func runVersions(args []string) error {
	fs := flag.NewFlagSet("versions", flag.ContinueOnError)
	dbPath := dbFlags(fs)
	pretty := prettyFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	events, err := st.ListVersionEvents(context.Background())
	if err != nil {
		return err
	}
	if *pretty {
		return writeJSON("", events)
	}
	data, _ := json.Marshal(events)
	fmt.Println(string(data))
	return nil
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	dbPath := dbFlags(fs)
	addr := fs.String("addr", "127.0.0.1:8080", "HTTP listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	eng, st, err := openEngine(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	server := &http.Server{Addr: *addr, Handler: httpapi.New(eng).Handler(), ReadHeaderTimeout: 10 * time.Second}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)
	go func() {
		<-stop
		eng.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
	fmt.Fprintf(os.Stderr, "ropechunk HTTP listening on http://%s\n", *addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func runClean(args []string) error {
	fs := flag.NewFlagSet("clean", flag.ContinueOnError)
	dbPath := dbFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := store.Clean(*dbPath); err != nil {
		return err
	}
	fmt.Printf("removed database, WAL/SHM, and checkpoint lock for %s\n", *dbPath)
	return nil
}

func splitCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
