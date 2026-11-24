package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"hostacl-workbench/internal/acceptance"
	"hostacl-workbench/internal/engine"
	"hostacl-workbench/internal/httpapi"
	"hostacl-workbench/internal/model"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = runServe(os.Args[2:])
	case "recover":
		err = runRecover(os.Args[2:])
	case "cleanup":
		err = runCleanup(os.Args[2:])
	case "sample":
		err = runSample(os.Args[2:])
	case "acceptance":
		err = runAcceptance(os.Args[2:])
	case "digest":
		err = runDigest(os.Args[2:])
	case "version":
		fmt.Println("hostacl-workbench dev")
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `hostacl-workbench commands:
  serve      Run the HTTP workbench
  recover    Scan and resolve pending intents
  cleanup    Remove managed trees created by the workbench
  sample     Print the deterministic sample policies and increments
  digest     Compute and stamp a canonical snapshot or increment digest
  acceptance Run the end-to-end local acceptance suite
  version    Print the build version`)
}

type commonFlags struct {
	root     string
	db       string
	maxConc  int
	queue    int
	budgetMS int64
}

func addCommon(fs *flag.FlagSet) *commonFlags {
	cfg := &commonFlags{}
	fs.StringVar(&cfg.root, "root", "", "empty managed root directory (required)")
	fs.StringVar(&cfg.db, "db", "", "SQLite database path outside the managed root (required)")
	fs.IntVar(&cfg.maxConc, "max-concurrency", 4, "global verification concurrency limit")
	fs.IntVar(&cfg.queue, "queue-size", 128, "bounded verification queue size")
	fs.Int64Var(&cfg.budgetMS, "budget-ms", 60000, "verification round time budget in milliseconds")
	return cfg
}

func (c *commonFlags) config() engine.Config {
	return engine.Config{
		Root: c.root, DBPath: c.db, MaxConcurrency: c.maxConc, QueueSize: c.queue,
		VerifyBudget: time.Duration(c.budgetMS) * time.Millisecond,
	}
}

func (c *commonFlags) validate() error {
	if c.root == "" || c.db == "" {
		return fmt.Errorf("--root and --db are required")
	}
	rootAbs, err := filepath.Abs(c.root)
	if err != nil {
		return err
	}
	dbAbs, err := filepath.Abs(c.db)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(rootAbs, dbAbs)
	if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel) {
		return fmt.Errorf("--db must be outside --root")
	}
	return nil
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	common := addCommon(fs)
	addr := fs.String("addr", "127.0.0.1:8080", "HTTP listen address")
	crash := fs.Bool("crash-after-materialize", false, "test hook: terminate after ACL materialization and before DB commit")
	recoveryOut := fs.String("recovery-out", "", "write the startup recovery report to this path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := common.validate(); err != nil {
		return err
	}
	cfg := common.config()
	cfg.CrashAfterMaterialize = *crash
	eng, err := engine.New(cfg)
	if err != nil {
		return err
	}
	defer eng.Close()
	recovery, err := eng.Recover(context.Background())
	if err != nil {
		return err
	}
	if *recoveryOut != "" {
		if err := writeJSONFile(*recoveryOut, recovery); err != nil {
			return err
		}
	}
	raw, _ := json.Marshal(recovery)
	fmt.Fprintln(os.Stderr, "startup recovery:", string(raw))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	handler := httpapi.New(eng)
	fmt.Fprintf(os.Stderr, "listening on http://%s root=%s db=%s\n", httpapi.NormalizeAddr(*addr), eng.Root(), common.db)
	return httpapi.Serve(ctx, *addr, handler)
}

func runRecover(args []string) error {
	fs := flag.NewFlagSet("recover", flag.ContinueOnError)
	common := addCommon(fs)
	out := fs.String("out", "", "write recovery report JSON to this path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := common.validate(); err != nil {
		return err
	}
	eng, err := engine.New(common.config())
	if err != nil {
		return err
	}
	defer eng.Close()
	report, err := eng.Recover(context.Background())
	raw, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(raw))
	if *out != "" {
		if writeErr := writeJSONFile(*out, report); writeErr != nil && err == nil {
			err = writeErr
		}
	}
	return err
}

func runCleanup(args []string) error {
	fs := flag.NewFlagSet("cleanup", flag.ContinueOnError)
	root := fs.String("root", "", "workbench managed root to clean")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *root == "" {
		return fmt.Errorf("--root is required")
	}
	return engine.CleanupRoot(*root)
}

func runSample(args []string) error {
	fs := flag.NewFlagSet("sample", flag.ContinueOnError)
	out := fs.String("out", "", "write the deterministic sample to this path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	raw, _ := json.MarshalIndent(model.DeterministicSample(), "", "  ")
	if *out == "" {
		fmt.Println(string(raw))
		return nil
	}
	return os.WriteFile(*out, append(raw, '\n'), 0o644)
}

func runAcceptance(args []string) error {
	fs := flag.NewFlagSet("acceptance", flag.ContinueOnError)
	workDir := fs.String("workdir", "", "work directory (default: new system temp directory)")
	keep := fs.Bool("keep", false, "keep the work directory and managed content")
	reportPath := fs.String("report", "artifacts/acceptance-report.json", "acceptance report output path")
	recoveryPath := fs.String("recovery-report", "artifacts/recovery-report.json", "recovery report output path")
	maxConc := fs.Int("max-concurrency", 6, "verification concurrency used by acceptance")
	queue := fs.Int("queue-size", 256, "verification queue used by acceptance")
	budget := fs.Int64("budget-ms", 120000, "verification budget used by acceptance")
	if err := fs.Parse(args); err != nil {
		return err
	}
	result, err := acceptance.Run(context.Background(), acceptance.Options{
		WorkDir: *workDir, Keep: *keep, ReportPath: *reportPath,
		RecoveryReportPath: *recoveryPath, MaxConcurrency: *maxConc,
		QueueSize: *queue, VerifyBudget: time.Duration(*budget) * time.Millisecond,
	})
	if result.ReportPath != "" {
		fmt.Printf("acceptance report: %s\n", result.ReportPath)
	}
	if result.RecoveryReportPath != "" {
		fmt.Printf("recovery report: %s\n", result.RecoveryReportPath)
	}
	if err != nil {
		return err
	}
	fmt.Printf("acceptance passed: %d checks, final version %d\n", result.Checks, result.FinalVersion)
	return nil
}

func runDigest(args []string) error {
	fs := flag.NewFlagSet("digest", flag.ContinueOnError)
	in := fs.String("in", "", "input JSON file (required)")
	out := fs.String("out", "", "write the digest-stamped JSON to this path (default: stdout)")
	kind := fs.String("kind", "", "snapshot or increment (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *in == "" || (*kind != "snapshot" && *kind != "increment") {
		return fmt.Errorf("--in and --kind=snapshot|increment are required")
	}
	raw, err := os.ReadFile(*in)
	if err != nil {
		return err
	}
	var value any
	switch *kind {
	case "snapshot":
		var snapshot model.Policy
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			return err
		}
		snapshot.Digest = model.SnapshotDigest(&snapshot)
		value = snapshot
	case "increment":
		var increment model.Increment
		if err := json.Unmarshal(raw, &increment); err != nil {
			return err
		}
		increment.Digest = model.IncrementDigest(&increment)
		value = increment
	}
	stamped, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if *out == "" {
		fmt.Println(string(stamped))
		return nil
	}
	return os.WriteFile(*out, append(stamped, '\n'), 0o644)
}

func writeJSONFile(path string, value any) error {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}
