package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"driftengine/internal/api"
	"driftengine/internal/engine"
	"driftengine/internal/loopendpoint"
	"driftengine/internal/sample"
	"driftengine/internal/store"
	"driftengine/internal/verify"
)

func Run(args []string) int {
	if len(args) == 0 {
		usage(os.Stdout)
		return 0
	}
	var err error
	switch args[0] {
	case "gen-samples":
		err = genSamples(args[1:])
	case "serve":
		err = serve(args[1:])
	case "test-endpoint":
		err = testEndpoint(args[1:])
	case "recover":
		err = recoverCmd(args[1:])
	case "verify":
		err = verifyCmd(args[1:])
	case "crash-worker":
		err = crashWorker(args[1:])
	case "-h", "--help", "help":
		usage(os.Stdout)
		return 0
	default:
		usage(os.Stderr)
		return 2
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}
	return 0
}

func usage(w *os.File) {
	fmt.Fprintln(w, `usage: drift-engine <command> [flags]

commands:
  gen-samples   generate fixed-seed JSON samples
  serve         run the declarative drift convergence HTTP service
  test-endpoint run a loopback-only write/readback test endpoint
  recover       scan SQLite intents and print a durable recovery report
  verify        run repeatable local acceptance checks
  crash-worker  internal helper used by forced-kill verification`)
}

func genSamples(args []string) error {
	fs := flag.NewFlagSet("gen-samples", flag.ContinueOnError)
	dir := fs.String("dir", "samples", "output directory")
	seed := fs.Int64("seed", sample.FixedSeed, "fixed random seed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	written, err := sample.GenerateTo(*dir, *seed)
	if err != nil {
		return err
	}
	fmt.Printf("wrote fixed-seed samples to %s (seed=%d): %d files\n", *dir, *seed, len(written))
	return nil
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:18080", "engine API listen address")
	dbPath := fs.String("db", filepath.Join("data", "drift-engine.db"), "SQLite database path")
	endpoint := fs.String("endpoint", "http://127.0.0.1:18081", "loopback write/readback endpoint base URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return fmt.Errorf("open sqlite: %w", err)
	}
	defer st.Close()
	eng := engine.New(st, *endpoint)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := eng.Bootstrap(ctx); err != nil {
		return err
	}
	server := api.NewServer(eng)
	fmt.Printf("drift-engine API listening on http://%s (endpoint=%s db=%s version=%d)\n", *addr, *endpoint, *dbPath, eng.CurrentVersion())
	return server.ListenAndServe(*addr)
}

func testEndpoint(args []string) error {
	fs := flag.NewFlagSet("test-endpoint", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:18081", "loopback listen address")
	statePath := fs.String("state", "", "optional endpoint-state.json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	states, err := loopendpoint.LoadStateFile(*statePath)
	if err != nil {
		return fmt.Errorf("load state: %w", err)
	}
	server, err := loopendpoint.New(*addr, states)
	if err != nil {
		return err
	}
	fmt.Println("loopback test endpoint listening on", server.URL())
	return server.ListenAndServe()
}

func recoverCmd(args []string) error {
	fs := flag.NewFlagSet("recover", flag.ContinueOnError)
	dbPath := fs.String("db", "", "SQLite database path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" {
		return errors.New("-db is required")
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	report, err := engine.Recover(context.Background(), st)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}

func verifyCmd(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	exe := fs.String("exe", "", "drift-engine executable path for forced-kill check")
	reportPath := fs.String("report", filepath.Join("artifacts", "verification-report.json"), "JSON report output path")
	seed := fs.Int64("seed", sample.FixedSeed, "fixed sample seed")
	workDir := fs.String("workdir", filepath.Join(".verify-work", fmt.Sprint(time.Now().UnixNano())), "verification work directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	report, err := verify.Run(context.Background(), verify.Options{
		Seed: *seed, WorkDir: *workDir, ExePath: *exe, ReportPath: *reportPath,
	})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(*reportPath), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(*reportPath, data, 0o644); err != nil {
		return err
	}
	for _, check := range report.Checks {
		status := "PASS"
		if !check.OK {
			status = "FAIL"
		}
		fmt.Printf("%s %s (%dms)\n", status, check.Name, check.DurationMS)
	}
	fmt.Println("report:", *reportPath)
	if !report.OK {
		return errors.New("one or more verification checks failed")
	}
	return nil
}

func crashWorker(args []string) error {
	fs := flag.NewFlagSet("crash-worker", flag.ContinueOnError)
	dbPath := fs.String("db", "", "sqlite path")
	endpoint := fs.String("endpoint", "http://127.0.0.1:0", "endpoint")
	scenario := fs.String("scenario", "write", "write or snapshot-intent")
	if err := fs.Parse(args); err != nil {
		return err
	}
	_ = endpoint
	if *dbPath == "" {
		return errors.New("-db is required")
	}
	return verify.CrashWorker(*dbPath, *scenario)
}
