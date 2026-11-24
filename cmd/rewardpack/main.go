package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"rewardpack"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		var be *rewardpack.BusinessError
		if errors.As(err, &be) && be != nil {
			_ = json.NewEncoder(os.Stderr).Encode(be)
			if be.Code == rewardpack.CodeStopped {
				os.Exit(130)
			}
		} else {
			_ = json.NewEncoder(os.Stderr).Encode(map[string]string{"code": "E_INTERNAL", "message": err.Error()})
		}
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return nil
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch args[0] {
	case "generate":
		return commandGenerate(args[1:])
	case "validate":
		return commandValidate(args[1:])
	case "config":
		return commandConfig(ctx, args[1:])
	case "build":
		return commandBuild(ctx, args[1:])
	case "edit":
		return commandEdit(ctx, args[1:])
	case "replace-source":
		return commandReplaceSource(ctx, args[1:])
	case "verify":
		return commandVerify(ctx, args[1:])
	case "status":
		return commandStatus(ctx, args[1:])
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		usage()
		return &rewardpack.BusinessError{Code: rewardpack.CodeInvalidInput, Context: args[0], Message: "unknown command"}
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `rewardpack - deterministic preference feature packs

  rewardpack generate --out DIR [--seed 42]
  rewardpack validate --config FILE --input JSONL
  rewardpack config --db FILE --config FILE
  rewardpack build --db FILE [--config FILE] [--shards DIR] [--out DIR]
  rewardpack edit --db FILE --file ANNOTATION.json
  rewardpack replace-source --db FILE --source NAME --file SHARD.jsonl
  rewardpack verify --db FILE [--out DIR]
  rewardpack status --db FILE
`)
}

func commandGenerate(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	out := fs.String("out", "sample", "output directory")
	seed := fs.Int64("seed", 42, "deterministic seed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := rewardpack.GenerateSample(*out, *seed); err != nil {
		return err
	}
	return printJSON(map[string]any{"ok": true, "out": *out, "seed": *seed})
}

func commandValidate(args []string) error {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	configPath := fs.String("config", "", "config YAML")
	inputPath := fs.String("input", "", "annotation JSONL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *configPath == "" || *inputPath == "" {
		return &rewardpack.BusinessError{Code: rewardpack.CodeInvalidInput, Message: "--config and --input are required"}
	}
	cfg, err := rewardpack.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	annotations, err := rewardpack.ReadJSONL(*inputPath)
	if err != nil {
		return err
	}
	sort.Slice(annotations, func(i, j int) bool { return annotations[i].PairID < annotations[j].PairID })
	for _, annotation := range annotations {
		if _, err := rewardpack.NormalizePair(annotation, cfg); err != nil {
			return err
		}
	}
	return printJSON(map[string]any{"ok": true, "valid_pairs": len(annotations), "config_version": cfg.Version})
}

func commandConfig(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("config", flag.ContinueOnError)
	dbPath := fs.String("db", "", "SQLite database")
	configPath := fs.String("config", "", "config YAML")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" || *configPath == "" {
		return &rewardpack.BusinessError{Code: rewardpack.CodeInvalidInput, Message: "--db and --config are required"}
	}
	cfg, err := rewardpack.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	store, err := rewardpack.OpenStoreContext(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.WithProcessLock(ctx, "cli", func() error { return store.ApplyConfig(ctx, cfg) }); err != nil {
		return err
	}
	return printJSON(map[string]any{"ok": true, "config_version": cfg.Version, "config_digest": cfg.Digest()})
}

func commandBuild(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("build", flag.ContinueOnError)
	dbPath := fs.String("db", "", "SQLite database")
	configPath := fs.String("config", "", "optional config YAML")
	shardsDir := fs.String("shards", "", "optional directory of JSONL shards")
	outDir := fs.String("out", "", "optional artifact export directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" {
		return &rewardpack.BusinessError{Code: rewardpack.CodeInvalidInput, Message: "--db is required"}
	}
	store, err := rewardpack.OpenStoreContext(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	var result *rewardpack.BuildResult
	err = store.WithProcessLock(ctx, "cli", func() error {
		if *configPath != "" {
			cfg, err := rewardpack.LoadConfig(*configPath)
			if err != nil {
				return err
			}
			if err := store.ApplyConfig(ctx, cfg); err != nil {
				return err
			}
		}
		if *shardsDir != "" {
			if err := rewardpack.SyncShardDirectory(ctx, store, *shardsDir); err != nil {
				return err
			}
		}
		var buildErr error
		result, buildErr = rewardpack.NewEngine(store).Build(ctx, rewardpack.BuildOptions{})
		return buildErr
	})
	if err != nil {
		return err
	}
	if *outDir != "" {
		if err := rewardpack.ExportBundleFiles(*outDir, result.Bundle); err != nil {
			return err
		}
	}
	return printJSON(map[string]any{
		"ok": true, "change_kind": result.ChangeKind,
		"retained":      len(result.Bundle.Budget.Retained),
		"eliminated":    len(result.Bundle.Budget.Eliminated),
		"report_digest": result.Bundle.Report.Digest,
		"invalidated":   result.Invalidation,
		"rebuilt_pairs": result.RebuiltPairs,
	})
}

func commandEdit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("edit", flag.ContinueOnError)
	dbPath := fs.String("db", "", "SQLite database")
	filePath := fs.String("file", "", "single annotation JSON file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" || *filePath == "" {
		return &rewardpack.BusinessError{Code: rewardpack.CodeInvalidInput, Message: "--db and --file are required"}
	}
	payload, err := os.ReadFile(*filePath)
	if err != nil {
		return &rewardpack.BusinessError{Code: rewardpack.CodeInvalidInput, Context: *filePath, Message: err.Error()}
	}
	var annotation rewardpack.Annotation
	if err := json.Unmarshal(payload, &annotation); err != nil {
		return &rewardpack.BusinessError{Code: rewardpack.CodeInvalidInput, Context: *filePath, Message: err.Error()}
	}
	store, err := rewardpack.OpenStoreContext(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.WithProcessLock(ctx, "cli", func() error { return store.UpsertAnnotation(ctx, annotation) }); err != nil {
		return err
	}
	return printJSON(map[string]any{"ok": true, "pair_id": annotation.PairID, "changed": "annotation_edit_or_append"})
}

func commandReplaceSource(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("replace-source", flag.ContinueOnError)
	dbPath := fs.String("db", "", "SQLite database")
	source := fs.String("source", "", "source/shard name")
	filePath := fs.String("file", "", "replacement JSONL shard")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" || *source == "" || *filePath == "" {
		return &rewardpack.BusinessError{Code: rewardpack.CodeInvalidInput, Message: "--db, --source and --file are required"}
	}
	annotations, err := rewardpack.ReadJSONL(*filePath)
	if err != nil {
		return err
	}
	store, err := rewardpack.OpenStoreContext(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.WithProcessLock(ctx, "cli", func() error { return store.ReplaceSource(ctx, *source, annotations) }); err != nil {
		return err
	}
	return printJSON(map[string]any{"ok": true, "source": *source, "pairs": len(annotations), "changed": "source_replace"})
}

func commandVerify(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	dbPath := fs.String("db", "", "SQLite database")
	outDir := fs.String("out", "", "optional full-reference export directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" {
		return &rewardpack.BusinessError{Code: rewardpack.CodeInvalidInput, Message: "--db is required"}
	}
	store, err := rewardpack.OpenStoreContext(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	var result *rewardpack.VerifyResult
	err = store.WithProcessLock(ctx, "cli", func() error {
		var verifyErr error
		result, verifyErr = rewardpack.NewEngine(store).VerifyIncremental(ctx)
		return verifyErr
	})
	if err != nil {
		return err
	}
	if *outDir != "" {
		if err := rewardpack.ExportBundleFiles(*outDir, result.Incremental); err != nil {
			return err
		}
	}
	return printJSON(map[string]any{
		"ok": true, "byte_identical": true,
		"incremental_report": result.Incremental.Report.Digest,
		"full_report":        result.Full.Report.Digest,
		"incremental_budget": result.Incremental.Budget.Digest,
		"full_budget":        result.Full.Budget.Digest,
	})
}

func commandStatus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	dbPath := fs.String("db", "", "SQLite database")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" {
		return &rewardpack.BusinessError{Code: rewardpack.CodeInvalidInput, Message: "--db is required"}
	}
	store, err := rewardpack.OpenStoreContext(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	states, err := store.AllStageStates(ctx)
	if err != nil {
		return err
	}
	checkpoints, err := store.Checkpoints(ctx)
	if err != nil {
		return err
	}
	invalidations, err := store.Invalidations(ctx)
	if err != nil {
		return err
	}
	sources, err := store.LoadSources(ctx)
	if err != nil {
		return err
	}
	manifest, hasManifest, err := store.CurrentManifest(ctx)
	if err != nil {
		return err
	}
	summary := map[string]any{
		"stages": states, "checkpoints": checkpoints,
		"invalidations": invalidations, "sources": sources,
	}
	if hasManifest {
		summary["manifest_digest"] = manifest.Digest()
		summary["config_version"] = manifest.ConfigVersion
	}
	return printJSON(summary)
}

func printJSON(value any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(value)
}
