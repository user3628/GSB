package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"configimpact"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "load":
		err = runLoad(ctx, os.Args[2:])
	case "patch":
		err = runPatch(ctx, os.Args[2:])
	case "read":
		err = runRead(ctx, os.Args[2:])
	case "impact":
		err = runImpact(ctx, os.Args[2:])
	case "refs":
		err = runRefs(ctx, os.Args[2:])
	case "recover":
		err = runRecover(ctx, os.Args[2:])
	case "demo":
		err = runDemo(ctx, os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		writeJSON(os.Stderr, map[string]any{"ok": false, "error": errorPayload(err)})
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `configimpact commands:
  load    -db PATH -doc FILE
  patch   -db PATH -patch FILE
  read    -db PATH -component ID -observe VERSION
  impact  -db PATH -version VERSION -component ID
  refs    -db PATH -version VERSION -component ID
  recover -db PATH
  demo    [-db PATH] [-sample FILE] [-patch FILE] [-fresh=true]

All command results are JSON on stdout. Business errors are JSON on stderr.`)
}

func openEngine(ctx context.Context, dbPath string, threshold float64) (*configimpact.Engine, error) {
	return configimpact.Open(ctx, dbPath, configimpact.Options{IncrementalThreshold: threshold})
}

func runLoad(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("load", flag.ContinueOnError)
	dbPath := flags.String("db", "", "SQLite database path")
	docPath := flags.String("doc", "", "document JSON path")
	threshold := flags.Float64("threshold", configimpact.DefaultIncrementalThreshold, "incremental threshold")
	if err := flags.Parse(args); err != nil {
		return err
	}
	payload, err := os.ReadFile(*docPath)
	if err != nil {
		return err
	}
	engine, err := openEngine(ctx, *dbPath, *threshold)
	if err != nil {
		return err
	}
	defer engine.Close()
	result, err := engine.LoadJSON(ctx, payload)
	if err != nil {
		return err
	}
	writeJSON(os.Stdout, map[string]any{"ok": true, "load": result, "recovery": engine.RecoveryReport()})
	return nil
}

func runPatch(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("patch", flag.ContinueOnError)
	dbPath := flags.String("db", "", "SQLite database path")
	patchPath := flags.String("patch", "", "patch JSON path")
	threshold := flags.Float64("threshold", configimpact.DefaultIncrementalThreshold, "incremental threshold")
	if err := flags.Parse(args); err != nil {
		return err
	}
	payload, err := os.ReadFile(*patchPath)
	if err != nil {
		return err
	}
	engine, err := openEngine(ctx, *dbPath, *threshold)
	if err != nil {
		return err
	}
	defer engine.Close()
	result, err := engine.SubmitPatchJSON(ctx, payload)
	if err != nil {
		return err
	}
	writeJSON(os.Stdout, map[string]any{"ok": true, "patch": result})
	return nil
}

func runRead(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("read", flag.ContinueOnError)
	dbPath := flags.String("db", "", "SQLite database path")
	component := flags.String("component", "", "component id")
	observed := flags.Int64("observe", 0, "observed document version; 0 means current")
	threshold := flags.Float64("threshold", configimpact.DefaultIncrementalThreshold, "incremental threshold")
	if err := flags.Parse(args); err != nil {
		return err
	}
	engine, err := openEngine(ctx, *dbPath, *threshold)
	if err != nil {
		return err
	}
	defer engine.Close()
	result, err := engine.Read(ctx, configimpact.ReadRequest{ComponentID: *component, ObservedVersion: *observed})
	if err != nil {
		return err
	}
	writeJSON(os.Stdout, map[string]any{"ok": true, "read": result})
	return nil
}

func runImpact(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("impact", flag.ContinueOnError)
	dbPath := flags.String("db", "", "SQLite database path")
	version := flags.Int64("version", 0, "snapshot version")
	component := flags.String("component", "", "component id")
	if err := flags.Parse(args); err != nil {
		return err
	}
	engine, err := configimpact.Open(ctx, *dbPath, configimpact.Options{})
	if err != nil {
		return err
	}
	defer engine.Close()
	impact, err := engine.ImpactAt(ctx, *version, *component)
	if err != nil {
		return err
	}
	writeJSON(os.Stdout, map[string]any{"ok": true, "impact": impact})
	return nil
}

func runRefs(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("refs", flag.ContinueOnError)
	dbPath := flags.String("db", "", "SQLite database path")
	version := flags.Int64("version", 0, "snapshot version")
	component := flags.String("component", "", "component id")
	if err := flags.Parse(args); err != nil {
		return err
	}
	engine, err := configimpact.Open(ctx, *dbPath, configimpact.Options{})
	if err != nil {
		return err
	}
	defer engine.Close()
	refs, err := engine.ReferencesAt(ctx, *version, *component)
	if err != nil {
		return err
	}
	writeJSON(os.Stdout, map[string]any{"ok": true, "references": refs})
	return nil
}

func runRecover(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("recover", flag.ContinueOnError)
	dbPath := flags.String("db", "", "SQLite database path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	engine, err := configimpact.Open(ctx, *dbPath, configimpact.Options{})
	if err != nil {
		return err
	}
	defer engine.Close()
	version, err := engine.CurrentVersion(ctx)
	if err != nil {
		return err
	}
	writeJSON(os.Stdout, map[string]any{
		"ok":              true,
		"current_version": version,
		"recovery":        engine.RecoveryReport(),
	})
	return nil
}

func runDemo(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("demo", flag.ContinueOnError)
	dbPath := flags.String("db", filepath.Join("artifacts", "configimpact-demo.db"), "SQLite database path")
	samplePath := flags.String("sample", filepath.Join("testdata", "sample.json"), "sample document")
	patchPath := flags.String("patch", filepath.Join("testdata", "patch.json"), "sample patch")
	fresh := flags.Bool("fresh", true, "remove the demo database before starting")
	threshold := flags.Float64("threshold", configimpact.DefaultIncrementalThreshold, "incremental threshold")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *fresh {
		for _, path := range []string{*dbPath, *dbPath + "-wal", *dbPath + "-shm"} {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	documentPayload, err := os.ReadFile(*samplePath)
	if err != nil {
		return err
	}
	patchPayload, err := os.ReadFile(*patchPath)
	if err != nil {
		return err
	}
	engine, err := openEngine(ctx, *dbPath, *threshold)
	if err != nil {
		return err
	}
	defer engine.Close()
	loadResult, err := engine.LoadJSON(ctx, documentPayload)
	if err != nil {
		return err
	}
	patchResult, err := engine.SubmitPatchJSON(ctx, patchPayload)
	if err != nil {
		return err
	}
	componentIDs := []string{"db", "auth", "api", "worker", "audit", "metrics"}
	oldReads := make(map[string]any, len(componentIDs))
	newReads := make(map[string]any, len(componentIDs))
	for _, componentID := range componentIDs {
		oldReads[componentID] = readOutcome(ctx, engine, componentID, 1)
		newReads[componentID] = readOutcome(ctx, engine, componentID, 2)
	}
	stale := readOutcome(ctx, engine, "db", 3)
	writeJSON(os.Stdout, map[string]any{
		"ok": true,
		"demo": map[string]any{
			"load":                loadResult,
			"patch":               patchResult,
			"reads_observed_v1":   oldReads,
			"reads_observed_v2":   newReads,
			"future_read_v3":      stale,
			"database":            *dbPath,
			"recovery_at_startup": engine.RecoveryReport(),
		},
	})
	return nil
}

func readOutcome(ctx context.Context, engine *configimpact.Engine, componentID string, version int64) any {
	result, err := engine.Read(ctx, configimpact.ReadRequest{ComponentID: componentID, ObservedVersion: version})
	if err != nil {
		return map[string]any{"ok": false, "error": errorPayload(err)}
	}
	return map[string]any{
		"ok":                   true,
		"observed_version":     result.ObservedVersion,
		"materialized_version": result.MaterializedVersion,
		"values":               result.Config.Values,
		"undefined_keys":       result.Config.Undefined,
	}
}

func errorPayload(err error) any {
	if code := configimpact.CodeOf(err); code != "" {
		return err
	}
	return map[string]any{"code": configimpact.ErrInternal, "message": err.Error()}
}

func writeJSON(writer *os.File, value any) {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(value)
}
