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
	"strings"
	"syscall"

	"flashattention/internal/audit"
	"flashattention/internal/dataset"
	"flashattention/internal/errs"
	"flashattention/internal/service"
	"flashattention/internal/tensor"
)

func main() {
	cmd, args, err := resolveArgs(os.Args[1:])
	if err != nil {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch cmd {
	case "demo":
		err = runDemo(ctx, args)
	case "generate":
		err = runGenerate(ctx, args)
	case "import":
		err = runImport(args)
	case "run", "recover":
		err = runRun(ctx, cmd == "recover", args)
	case "incremental":
		err = runIncremental(ctx, args)
	case "audit", "report":
		err = runAudit(args)
	case "status":
		err = runStatus(ctx, args)
	case "versions":
		err = runVersions(ctx, args)
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", cmd)
		usage()
		os.Exit(2)
	}
	if err != nil {
		printError(err)
		os.Exit(exitCode(err))
	}
}

func resolveArgs(argv []string) (string, []string, error) {
	if len(argv) == 0 {
		return "", nil, errors.New("missing command")
	}
	if !strings.HasPrefix(argv[0], "-") || argv[0] == "-h" || argv[0] == "--help" {
		return argv[0], argv[1:], nil
	}

	fs := flag.NewFlagSet("flashattn", flag.ContinueOnError)
	dataDir := fs.String("data", "data", "data directory")
	reportDir := fs.String("reports", "reports", "report directory")
	if err := fs.Parse(argv); err != nil {
		return "", nil, err
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return "", nil, errors.New("missing command")
	}

	args := append([]string(nil), rest[1:]...)
	seen := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { seen[f.Name] = true })
	if seen["data"] && !argsHaveFlag(args, "data") {
		args = append(args, "--data", *dataDir)
	}
	if seen["reports"] && !argsHaveFlag(args, "reports") {
		args = append(args, "--reports", *reportDir)
	}
	return rest[0], args, nil
}

func argsHaveFlag(args []string, name string) bool {
	prefix := "--" + name
	for _, arg := range args {
		if arg == prefix || strings.HasPrefix(arg, prefix+"=") {
			return true
		}
	}
	return false
}

func usage() {
	fmt.Println(`flashattn - deterministic tiled online-softmax attention

Usage:
  flashattn demo [--data data] [--reports reports] [--budget-mib 64]
  flashattn generate [--data data] [--sample all|ID] [--force]
  flashattn import --file shard.json [--id ID] [--data data]
  flashattn run --shard ID [--data data] [--budget-mib 64] [--stop-after N] [--no-resume] [--strict-numeric]
  flashattn recover --shard ID [--data data] [--budget-mib 64]
  flashattn incremental --shard ID --mode correct|append --start N --end N [--seed N] [--data data]
  flashattn audit --shard ID [--data data]
  flashattn status [--shard ID] [--data data]
  flashattn versions [--shard ID] [--data data]

Common --data/--reports flags may appear before or after <command>.
Exit codes: 0 success/graceful stop, 2 usage/config/IO, 3 E_MEMORY_BUDGET,
4 E_NUMERIC, 5 E_ERROR_BOUND, 6 E_INCREMENTAL_MISMATCH, 7 E_STALE_CHECKPOINT.

Run 'flashattn <command> -h' for details. SIGINT/SIGTERM completes the current
durable checkpoint and exits gracefully.`)
}

type commonFlags struct {
	data    string
	reports string
}

func bindCommon(fs *flag.FlagSet) *commonFlags {
	c := &commonFlags{}
	fs.StringVar(&c.data, "data", "data", "data directory")
	fs.StringVar(&c.reports, "reports", "reports", "report directory")
	return c
}

func openService(c *commonFlags) (*service.Service, error) {
	return service.NewWithReports(c.data, c.reports)
}

func runDemo(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	c := bindCommon(fs)
	budgetMiB := fs.Int64("budget-mib", 64, "peak memory budget in MiB")
	if err := fs.Parse(args); err != nil {
		return err
	}
	svc, err := openService(c)
	if err != nil {
		return err
	}
	defer svc.Close()
	rep, err := svc.Demo(ctx, *budgetMiB<<20)
	printJSON(rep)
	return err
}

func runGenerate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	c := bindCommon(fs)
	sample := fs.String("sample", "all", "sample ID or all")
	force := fs.Bool("force", false, "overwrite generated shard")
	if err := fs.Parse(args); err != nil {
		return err
	}
	svc, err := openService(c)
	if err != nil {
		return err
	}
	defer svc.Close()
	if *sample == "all" {
		bs, err := svc.GenerateAll(ctx, *force)
		if err != nil {
			return err
		}
		printJSON(map[string]any{"generated": len(bs), "data_dir": c.data})
		return nil
	}
	spec, ok := findSpec(*sample)
	if !ok {
		return errs.New(errs.EConfig, "unknown sample %q", *sample)
	}
	b, err := svc.Generate(spec, *force)
	if err != nil {
		return err
	}
	printJSON(map[string]any{"id": b.Spec.ID, "shape": b.Shape, "path": svc.ShardPath(b.Spec.ID)})
	return nil
}

func findSpec(id string) (dataset.Spec, bool) {
	for _, sp := range dataset.Catalog() {
		if sp.ID == id {
			return sp, true
		}
	}
	return dataset.Spec{}, false
}

func runImport(args []string) error {
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	c := bindCommon(fs)
	file := fs.String("file", "", "shard JSON file")
	id := fs.String("id", "", "override shard ID")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *file == "" {
		return errs.New(errs.EConfig, "--file is required")
	}
	svc, err := openService(c)
	if err != nil {
		return err
	}
	defer svc.Close()
	b, err := svc.ImportBundle(*file, *id)
	if err != nil {
		return err
	}
	printJSON(map[string]any{"imported": b.Spec.ID, "shape": b.Shape})
	return nil
}

func runRun(ctx context.Context, recover bool, args []string) error {
	name := "run"
	if recover {
		name = "recover"
	}
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	c := bindCommon(fs)
	shard := fs.String("shard", "", "shard ID")
	budgetMiB := fs.Int64("budget-mib", 64, "peak memory budget MiB; 0 uses 64 MiB")
	budgetBytes := fs.Int64("budget-bytes", 0, "exact peak budget in bytes (overrides MiB)")
	stopAfter := fs.Int("stop-after", 0, "gracefully stop after N durable checkpoints")
	noResume := fs.Bool("no-resume", false, "do not load SQLite recovery state")
	strictNumeric := fs.Bool("strict-numeric", false, "treat E_NUMERIC diagnostics as an error")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *shard == "" {
		return errs.New(errs.EConfig, "--shard is required")
	}
	budget := *budgetBytes
	if budget == 0 {
		budget = *budgetMiB << 20
	}
	svc, err := openService(c)
	if err != nil {
		return err
	}
	defer svc.Close()
	rep, err := svc.Run(ctx, *shard, service.RunOptions{
		MemoryBudgetBytes: budget, Resume: !*noResume || recover,
		StopAfterCheckpoint: *stopAfter, StrictNumeric: *strictNumeric,
	})
	if rep != nil {
		printJSON(rep)
	}
	if errors.Is(err, service.ErrGracefulStop) {
		return nil
	}
	return err
}

func runIncremental(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("incremental", flag.ContinueOnError)
	c := bindCommon(fs)
	shard := fs.String("shard", "", "shard ID")
	mode := fs.String("mode", "correct", "correct or append")
	start := fs.Int("start", 0, "affected key start")
	end := fs.Int("end", 0, "affected key end")
	seed := fs.Uint64("seed", 0, "deterministic patch seed")
	budgetMiB := fs.Int64("budget-mib", 64, "memory budget MiB")
	if err := fs.Parse(args); err != nil {
		return err
	}
	svc, err := openService(c)
	if err != nil {
		return err
	}
	defer svc.Close()
	rep, err := svc.Incremental(ctx, service.UpdateRequest{
		ShardID: *shard, Mode: *mode, Start: *start, End: *end, Seed: *seed,
	}, *budgetMiB<<20)
	if rep != nil {
		printJSON(rep)
	}
	return err
}

func runAudit(args []string) error {
	fs := flag.NewFlagSet("audit", flag.ContinueOnError)
	c := bindCommon(fs)
	shard := fs.String("shard", "", "shard ID")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *shard == "" {
		return errs.New(errs.EConfig, "--shard is required")
	}
	svc, err := openService(c)
	if err != nil {
		return err
	}
	defer svc.Close()
	b, err := svc.LoadShard(*shard)
	if err != nil {
		return err
	}
	var out tensor.Tensor
	if err := tensor.LoadJSON(filepath.Join(svc.ShardPath(*shard), "..", "output.json"), &out); err != nil {
		runRep := svc.ReportDir + "/" + *shard + "-run.json"
		if err2 := tensor.LoadJSON(runRep, &struct{}{}); err2 != nil {
			return errs.New(errs.EIO, "no saved output; run `flashattn run --shard %s` first: %v", *shard, err)
		}
		return errs.New(errs.EIO, "output.json missing; rerun the shard")
	}
	set, auditErr := audit.Audit(*shard, b.Config, b.Shape, &out, b.Reference, audit.Envelope{
		QMax: audit.TensorMaxAbs(b.Q), KMax: audit.TensorMaxAbs(b.K), VMax: audit.TensorMaxAbs(b.V),
	})
	printJSON(set)
	return auditErr
}

func runStatus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	c := bindCommon(fs)
	shard := fs.String("shard", "", "shard ID; empty shows all")
	if err := fs.Parse(args); err != nil {
		return err
	}
	svc, err := openService(c)
	if err != nil {
		return err
	}
	defer svc.Close()
	if *shard == "" {
		rows, err := svc.Store.ListAllCheckpoints(ctx)
		if err != nil {
			return err
		}
		printJSON(map[string]any{"checkpoints": rows})
		return nil
	}
	rows, err := svc.Store.ListCheckpoints(ctx, *shard)
	if err != nil {
		return err
	}
	heads, err := svc.Store.HeadSequences(ctx, *shard)
	if err != nil {
		return err
	}
	printJSON(map[string]any{"shard": *shard, "heads": heads, "checkpoints": rows})
	return nil
}

func runVersions(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("versions", flag.ContinueOnError)
	c := bindCommon(fs)
	shard := fs.String("shard", "", "shard ID")
	if err := fs.Parse(args); err != nil {
		return err
	}
	svc, err := openService(c)
	if err != nil {
		return err
	}
	defer svc.Close()
	if *shard == "" {
		printJSON(map[string]string{"hint": "pass --shard ID for fingerprint and invalidation ranges"})
		return nil
	}
	fp, err := svc.Store.GetFingerprint(ctx, *shard)
	if err != nil {
		return err
	}
	invs, err := svc.Store.ListInvalidations(ctx, *shard)
	if err != nil {
		return err
	}
	printJSON(map[string]any{"fingerprint": fp, "invalidations": invs})
	return nil
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func printError(err error) {
	ce := errs.As(err)
	if ce != nil {
		printJSON(map[string]any{"ok": false, "error": ce})
		return
	}
	printJSON(map[string]any{"ok": false, "error": map[string]string{"message": err.Error()}})
}

func exitCode(err error) int {
	switch errs.Code(err) {
	case errs.EConfig, errs.EIO:
		return 2
	case errs.EMemoryBudget:
		return 3
	case errs.ENumeric:
		return 4
	case errs.EErrorBound:
		return 5
	case errs.EIncrementalMismatch:
		return 6
	case errs.EStaleCheckpoint:
		return 7
	default:
		if errors.Is(err, context.Canceled) {
			return 130
		}
		return 1
	}
}
