// Command quantops generates deterministic samples, executes the four-stage
// checkpointed quantization pipeline, and provides inspection utilities.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"local/quantops/internal/force"
	"local/quantops/pkg/canon"
	"local/quantops/pkg/dataset"
	"local/quantops/pkg/engine"
	"local/quantops/pkg/qerr"
	"local/quantops/pkg/quant"
	"local/quantops/pkg/scheduler"
	"local/quantops/pkg/store"
)

const version = "1.0.0"

func main() { os.Exit(runCLI(os.Args[1:])) }

func runCLI(args []string) int {
	if len(args) == 0 {
		printUsage(os.Stderr)
		return 2
	}
	switch args[0] {
	case "generate":
		return cmdGenerate(args[1:])
	case "run", "recover":
		return cmdRun(args[1:], args[0])
	case "schedule":
		return cmdSchedule(args[1:])
	case "status":
		return cmdStatus(args[1:])
	case "fingerprint":
		return cmdFingerprint(args[1:])
	case "checkpoint":
		return cmdCheckpoint(args[1:])
	case "diagnose":
		return cmdDiagnose(args[1:])
	case "compare":
		return cmdCompare(args[1:])
	case "quantize":
		return cmdQuantize(args[1:])
	case "version", "--version":
		fmt.Printf("quantops %s\n", version)
		return 0
	case "help", "-h", "--help":
		printUsage(os.Stdout)
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", args[0])
		printUsage(os.Stderr)
		return 2
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, `quantops - deterministic low-precision quantization and checkpoint tool

Usage:
  quantops generate  [--dir data] [--seed 20261125]
  quantops run       [--data data] [--db quantops.db] [--summary result-summary.json]
                     [--total-elements N] [--per-worker-elements N]
                     [--peak-memory BYTES] [--workers N] [--crash-after-commits N]
  quantops recover   (same flags as run; committed stages are reused)
  quantops schedule  [--data data] [--total-elements N] [--per-worker-elements N]
                     [--peak-memory BYTES] [--workers N]
  quantops status    [--db quantops.db] [--shard ID]
  quantops fingerprint [--db quantops.db] [--data data] [--shard ID]
  quantops checkpoint [--db quantops.db] [--data data] [--shard ID]
  quantops diagnose  [--db quantops.db] [--shard ID]
  quantops compare   --left FILE --right FILE
  quantops quantize  --file TENSOR.json [--bits 4|8] [--mode symmetric|asymmetric]
                     [--granularity per_tensor|per_channel] [--scale F] [--zero-point N] [--full]

Stable business codes:
  E_INVALID_INPUT, E_INVALID_TRANSITION, E_MISSING_ARTIFACT,
  E_STALE_CHECKPOINT, E_BUDGET_EXCEEDED, E_NUMERIC_MISMATCH`)
}

func cmdGenerate(args []string) int {
	fs := newFlagSet("generate")
	dir := fs.String("dir", "data", "sample directory")
	seed := fs.Int64("seed", dataset.DefaultSeed, "fixed deterministic seed")
	if err := parseFlags(fs, args); err != nil {
		return 2
	}
	loaded, err := dataset.Generate(*dir, *seed)
	if err != nil {
		return fail(err)
	}
	printJSON(map[string]any{
		"manifest":    filepath.Join(*dir, "manifest.json"),
		"shards":      len(loaded.Manifest.Shards),
		"fingerprint": loaded.Fingerprint,
	})
	return 0
}

type limitsFlags struct {
	total   int64
	worker  int64
	memory  int64
	workers int
}

func addLimitFlags(fs *flag.FlagSet, def scheduler.Limits) *limitsFlags {
	l := &limitsFlags{}
	fs.Int64Var(&l.total, "total-elements", def.TotalElements, "total element budget")
	fs.Int64Var(&l.worker, "per-worker-elements", def.PerWorkerElements, "per-worker element quota")
	fs.Int64Var(&l.memory, "peak-memory", def.PeakMemoryBytes, "peak aggregate working-set bytes")
	fs.IntVar(&l.workers, "workers", def.MaxWorkers, "maximum worker processes/slots")
	return l
}

func (l *limitsFlags) limits() scheduler.Limits {
	return scheduler.Limits{TotalElements: l.total, PerWorkerElements: l.worker, PeakMemoryBytes: l.memory, MaxWorkers: l.workers}
}

func cmdRun(args []string, command string) int {
	def := scheduler.DefaultLimits()
	fs := newFlagSet(command)
	dataDir := fs.String("data", "data", "sample directory")
	manifestPath := fs.String("manifest", "", "manifest path (overrides --data)")
	dbPath := fs.String("db", "quantops.db", "SQLite checkpoint database")
	summaryPath := fs.String("summary", "result-summary.json", "deterministic result summary")
	limits := addLimitFlags(fs, def)
	shards := fs.String("shards", "", "comma-separated shard ids (default: all)")
	crashAfter := fs.Int("crash-after-commits", 0, "force terminate after N stage commits (validation aid)")
	if err := parseFlags(fs, args); err != nil {
		return 2
	}
	mp := *manifestPath
	if mp == "" {
		mp = filepath.Join(*dataDir, "manifest.json")
	}
	loaded, err := dataset.Load(mp)
	if err != nil {
		return fail(qerr.New(qerr.CodeInvalidInput, "", "", "", "load dataset", err))
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return fail(qerr.New(qerr.CodeInternal, "", "", "", "open checkpoint database", err))
	}
	defer st.Close()
	runner, err := engine.New(loaded, st)
	if err != nil {
		return fail(err)
	}
	opts := engine.RunOptions{Limits: limits.limits()}
	if *shards != "" {
		for _, id := range strings.Split(*shards, ",") {
			id = strings.TrimSpace(id)
			if id != "" {
				opts.ShardIDs = append(opts.ShardIDs, id)
			}
		}
	}
	if *crashAfter > 0 {
		opts.CommitHook = func(committed int) {
			if committed >= *crashAfter {
				force.Terminate()
			}
		}
	}
	result, runErr := runner.Run(context.Background(), opts)
	if result != nil && len(result.SummaryBytes) > 0 {
		if err := canon.WriteFileAtomic(*summaryPath, result.SummaryBytes, 0o644); err != nil {
			return fail(qerr.New(qerr.CodeInternal, "", "", "", "write summary", err))
		}
	}
	if runErr != nil {
		printBusinessError(runErr)
		return qerr.ExitCode(runErr)
	}
	if result == nil {
		fmt.Fprintln(os.Stderr, "no result returned")
		return 1
	}
	printJSON(map[string]any{
		"status":         result.Summary.Status,
		"summary":        *summaryPath,
		"summary_digest": result.Summary.AggregateDigest,
		"shards":         len(result.Summary.Shards),
		"untreated":      len(result.Summary.Untreated),
		"concurrency":    result.Concurrency,
		"numeric_errors": len(result.NumericErrors),
	})
	if len(result.NumericErrors) > 0 {
		for _, err := range result.NumericErrors {
			printBusinessError(err)
		}
		return 3
	}
	if len(result.BudgetErrors) > 0 {
		for _, err := range result.BudgetErrors {
			printBusinessError(err)
		}
		return 5
	}
	return 0
}

func cmdSchedule(args []string) int {
	def := scheduler.DefaultLimits()
	fs := newFlagSet("schedule")
	dataDir := fs.String("data", "data", "sample directory")
	manifestPath := fs.String("manifest", "", "manifest path")
	limits := addLimitFlags(fs, def)
	if err := parseFlags(fs, args); err != nil {
		return 2
	}
	mp := *manifestPath
	if mp == "" {
		mp = filepath.Join(*dataDir, "manifest.json")
	}
	loaded, err := dataset.Load(mp)
	if err != nil {
		return fail(qerr.New(qerr.CodeInvalidInput, "", "", "", "load dataset", err))
	}
	plan, err := scheduler.PlanBatches(loaded.Manifest.Shards, limits.limits())
	if err != nil {
		return fail(err)
	}
	printJSON(plan)
	if len(plan.Rejected) > 0 {
		return 5
	}
	return 0
}

func cmdStatus(args []string) int {
	fs := newFlagSet("status")
	dbPath := fs.String("db", "quantops.db", "SQLite checkpoint database")
	shard := fs.String("shard", "", "shard id")
	if err := parseFlags(fs, args); err != nil {
		return 2
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return fail(err)
	}
	defer st.Close()
	ctx := context.Background()
	if *shard != "" {
		p, err := st.Progress(ctx, *shard)
		if err != nil {
			return fail(err)
		}
		states, err := st.States(ctx, *shard)
		if err != nil {
			return fail(err)
		}
		invs, err := st.Invalidations(ctx, *shard)
		if err != nil {
			return fail(err)
		}
		printJSON(map[string]any{"progress": p, "stages": stateViews(states), "invalidations": invs})
		return 0
	}
	progress, err := st.ListProgress(ctx)
	if err != nil {
		return fail(err)
	}
	out := make([]map[string]any, 0, len(progress))
	for _, p := range progress {
		states, err := st.States(ctx, p.ShardID)
		if err != nil {
			return fail(err)
		}
		invs, err := st.Invalidations(ctx, p.ShardID)
		if err != nil {
			return fail(err)
		}
		out = append(out, map[string]any{"progress": p, "stages": stateViews(states), "invalidations": invs})
	}
	printJSON(map[string]any{"shards": out})
	return 0
}

func cmdFingerprint(args []string) int {
	fs := newFlagSet("fingerprint")
	dbPath := fs.String("db", "quantops.db", "SQLite checkpoint database")
	dataDir := fs.String("data", "data", "sample directory")
	manifestPath := fs.String("manifest", "", "manifest path")
	shard := fs.String("shard", "", "shard id")
	if err := parseFlags(fs, args); err != nil {
		return 2
	}
	mp := *manifestPath
	if mp == "" {
		mp = filepath.Join(*dataDir, "manifest.json")
	}
	loaded, err := dataset.Load(mp)
	if err != nil {
		return fail(qerr.New(qerr.CodeInvalidInput, "", "", "", "load dataset", err))
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return fail(err)
	}
	defer st.Close()
	ctx := context.Background()
	if *shard != "" {
		p, err := st.Progress(ctx, *shard)
		if err != nil {
			return fail(err)
		}
		states, err := st.States(ctx, *shard)
		if err != nil {
			return fail(err)
		}
		printJSON(map[string]any{
			"shard_id": *shard, "manifest_fingerprint": loaded.Fingerprint,
			"version_fingerprint": versionOf(p), "stages": stageFingerprints(states),
		})
		return 0
	}
	progress, err := st.ListProgress(ctx)
	if err != nil {
		return fail(err)
	}
	var versions []map[string]any
	for _, p := range progress {
		versions = append(versions, map[string]any{
			"shard_id": p.ShardID, "version_fingerprint": p.VersionFingerprint, "next_stage_seq": p.NextStageSeq,
		})
	}
	printJSON(map[string]any{"manifest_fingerprint": loaded.Fingerprint, "shards": versions})
	return 0
}

func cmdCheckpoint(args []string) int {
	fs := newFlagSet("checkpoint")
	dbPath := fs.String("db", "quantops.db", "SQLite checkpoint database")
	dataDir := fs.String("data", "data", "sample directory")
	manifestPath := fs.String("manifest", "", "manifest path")
	shard := fs.String("shard", "", "shard id (default: all)")
	if err := parseFlags(fs, args); err != nil {
		return 2
	}
	mp := *manifestPath
	if mp == "" {
		mp = filepath.Join(*dataDir, "manifest.json")
	}
	loaded, err := dataset.Load(mp)
	if err != nil {
		return fail(qerr.New(qerr.CodeInvalidInput, "", "", "", "load dataset", err))
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return fail(err)
	}
	defer st.Close()
	ids := []string{*shard}
	if *shard == "" {
		ids = ids[:0]
		for _, sh := range loaded.Manifest.Shards {
			ids = append(ids, sh.ID)
		}
	}
	var out []map[string]any
	for _, id := range ids {
		res, err := engine.ClassifyCheckpoint(context.Background(), loaded, st, id)
		if err != nil {
			printBusinessError(err)
			return qerr.ExitCode(err)
		}
		out = append(out, map[string]any{
			"shard_id": id, "classification": res.Classification,
			"invalidated_from": res.InvalidatedFrom, "reason": res.Reason, "progress": res.Progress,
		})
	}
	if *shard != "" && len(out) == 1 {
		printJSON(out[0])
	} else {
		printJSON(map[string]any{"checkpoints": out})
	}
	return 0
}

func cmdDiagnose(args []string) int {
	fs := newFlagSet("diagnose")
	dbPath := fs.String("db", "quantops.db", "SQLite checkpoint database")
	shard := fs.String("shard", "", "shard id (default: all with verification)")
	if err := parseFlags(fs, args); err != nil {
		return 2
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return fail(err)
	}
	defer st.Close()
	ctx := context.Background()
	ids := []string{*shard}
	if *shard == "" {
		progress, err := st.ListProgress(ctx)
		if err != nil {
			return fail(err)
		}
		ids = ids[:0]
		for _, p := range progress {
			ids = append(ids, p.ShardID)
		}
	}
	var out []map[string]any
	for _, id := range ids {
		state, err := st.State(ctx, id, store.Verification)
		if err != nil {
			return fail(err)
		}
		if state == nil {
			out = append(out, map[string]any{"shard_id": id, "status": "verification_not_committed"})
			continue
		}
		var ver engine.VerifyArtifact
		if err := json.Unmarshal(state.Payload, &ver); err != nil {
			return fail(qerr.New(qerr.CodeStaleCheckpoint, id, "verification", "", "decode verification artifact", err))
		}
		out = append(out, map[string]any{
			"shard_id": id, "stage_digest": state.StageDigest,
			"report": ver.Report,
		})
	}
	if *shard != "" && len(out) == 1 {
		printJSON(out[0])
	} else {
		printJSON(map[string]any{"diagnostics": out})
	}
	return 0
}

func cmdCompare(args []string) int {
	fs := newFlagSet("compare")
	left := fs.String("left", "", "left summary file")
	right := fs.String("right", "", "right summary file")
	if err := parseFlags(fs, args); err != nil {
		return 2
	}
	if *left == "" || *right == "" {
		fmt.Fprintln(os.Stderr, "--left and --right are required")
		return 2
	}
	lb, err := os.ReadFile(*left)
	if err != nil {
		return fail(qerr.New(qerr.CodeInvalidInput, "", "", "", "read left summary", err))
	}
	rb, err := os.ReadFile(*right)
	if err != nil {
		return fail(qerr.New(qerr.CodeInvalidInput, "", "", "", "read right summary", err))
	}
	equal := string(lb) == string(rb)
	out := map[string]any{
		"byte_identical": equal,
		"left":           filepath.Base(*left), "right": filepath.Base(*right),
		"left_size": len(lb), "right_size": len(rb),
		"left_sha256": canon.HashBytes(lb), "right_sha256": canon.HashBytes(rb),
	}
	printJSON(out)
	if !equal {
		return 1
	}
	return 0
}

func cmdQuantize(args []string) int {
	fs := newFlagSet("quantize")
	file := fs.String("file", "", "tensor JSON file")
	bits := fs.Int("bits", 8, "bit width (4 or 8)")
	mode := fs.String("mode", "symmetric", "symmetric or asymmetric")
	gran := fs.String("granularity", "per_tensor", "per_tensor or per_channel")
	scale := fs.Float64("scale", 0, "fixed scale override (requires per_tensor or per-channel fixed zero point)")
	zero := fs.Int("zero-point", 0, "fixed zero point")
	full := fs.Bool("full", false, "include raw quantized and dequantized values")
	if err := parseFlags(fs, args); err != nil {
		return 2
	}
	if *file == "" {
		fmt.Fprintln(os.Stderr, "--file is required")
		return 2
	}
	cfg := quant.Config{Bits: *bits, Mode: quant.Mode(*mode), Granularity: quant.Granularity(*gran)}
	if err := cfg.Validate(); err != nil {
		return fail(qerr.New(qerr.CodeInvalidInput, "", "", "", "invalid quantization config", err))
	}
	tf, t, err := dataset.LoadTensorFile(*file)
	if err != nil {
		return fail(qerr.New(qerr.CodeInvalidInput, "", "", "", "load tensor", err))
	}
	t, nonFinite := quant.Sanitize(t)
	var q quant.QuantizedTensor
	if *scale > 0 {
		channels := 1
		if cfg.Granularity == quant.PerChannel {
			channels = t.Shape[0]
		}
		params := make([]quant.Params, channels)
		for i := range params {
			params[i] = quant.Params{Scale: *scale, ZeroPoint: int32(*zero)}
		}
		q, err = quant.QuantizeWithParams(t, cfg, params)
	} else {
		q, err = quant.Quantize(t, cfg)
	}
	if err != nil {
		return fail(qerr.New(qerr.CodeInvalidInput, "", "", "", "quantize tensor", err))
	}
	deq, err := quant.Dequantize(q)
	if err != nil {
		return fail(qerr.New(qerr.CodeInternal, "", "", "", "dequantize tensor", err))
	}
	digest, _ := canon.Hash(deq)
	out := map[string]any{
		"tensor": tf.Name, "shape": q.Shape, "config": cfg,
		"params": q.Params, "clamp_count": q.ClampCount, "clamps": q.Clamps,
		"non_finite_inputs": nonFinite, "dequantized_digest": digest,
	}
	if *full {
		out["quantized_values"] = q.Values
		out["dequantized_values"] = deq.Values
	}
	printJSON(out)
	return 0
}

func stateViews(states []store.State) []map[string]any {
	out := make([]map[string]any, 0, len(states))
	for _, st := range states {
		out = append(out, map[string]any{
			"stage": st.Stage, "stage_seq": st.StageSeq, "global_sequence": st.GlobalSequence,
			"config_version": st.ConfigVersion, "input_fingerprint": st.InputFingerprint,
			"stage_digest": st.StageDigest,
		})
	}
	return out
}

func stageFingerprints(states []store.State) []map[string]any {
	out := make([]map[string]any, 0, len(states))
	for _, st := range states {
		out = append(out, map[string]any{
			"stage": st.Stage, "stage_seq": st.StageSeq,
			"config_version": st.ConfigVersion, "stage_digest": st.StageDigest,
		})
	}
	return out
}

func versionOf(p *store.Progress) any {
	if p == nil {
		return nil
	}
	return p.VersionFingerprint
}

func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

func parseFlags(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			printUsage(os.Stdout)
			return err
		}
		fmt.Fprintf(os.Stderr, "%s: %v\n", fs.Name(), err)
		return err
	}
	if fs.NArg() != 0 {
		err := fmt.Errorf("unexpected positional arguments: %v", fs.Args())
		fmt.Fprintf(os.Stderr, "%s: %v\n", fs.Name(), err)
		return err
	}
	return nil
}

func printJSON(v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "encode output: %v\n", err)
		return
	}
	fmt.Println(string(b))
}

func printBusinessError(err error) {
	var agg *qerr.Aggregate
	if errors.As(err, &agg) {
		printJSON(agg)
		return
	}
	var be *qerr.Error
	if errors.As(err, &be) {
		printJSON(be)
		return
	}
	printJSON(map[string]any{"code": qerr.CodeInternal, "message": err.Error()})
}

func fail(err error) int {
	printBusinessError(err)
	return qerr.ExitCode(err)
}
