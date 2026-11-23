package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"baselineworkbench/internal/app"
	"baselineworkbench/internal/config"
	"baselineworkbench/internal/engine"
	"baselineworkbench/internal/samples"
	"baselineworkbench/internal/verify"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		printUsage()
		return errors.New("command is required")
	}
	switch args[0] {
	case "generate":
		return runGenerate(args[1:])
	case "demo":
		return runDemo(args[1:])
	case "verify":
		return runVerify(args[1:])
	case "help", "-h", "--help":
		printUsage()
		return nil
	default:
		printUsage()
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `baselineworkbench - Windows security baseline consistency workbench

Commands:
  generate  Generate the deterministic local sample set
  demo      Generate samples, validate all packages, and write JSON+terminal reports
  verify    Inject five anomaly classes and assert codes, ledger, and snapshot stability

Examples:
  go run ./cmd/baselinebench demo --workdir work/demo
  go run ./cmd/baselinebench demo --mode tolerant --concurrency 4
  go run ./cmd/baselinebench verify --inject all
  go run ./cmd/baselinebench generate --out work/samples`)
}

func runGenerate(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	out := fs.String("out", filepath.Join("work", "samples"), "sample set output directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if _, err := samples.Generate(*out); err != nil {
		return err
	}
	digest, err := samples.Digest(*out)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*out, "manifest.sha256"), []byte(digest+"\n"), 0o644); err != nil {
		return err
	}
	fmt.Printf("generated=%s\nsha256=%s\n", *out, digest)
	return nil
}

func runDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	workdir := fs.String("workdir", filepath.Join("work", "demo"), "demo work directory")
	samplesDirFlag := fs.String("samples", "", "sample directory (default <workdir>/samples)")
	dbFlag := fs.String("db", "", "SQLite database path (default <workdir>/state/snapshots.db)")
	reportFlag := fs.String("report", "", "JSON report path (default <workdir>/report.json)")
	summaryFlag := fs.String("summary", "", "terminal summary path (default <workdir>/summary.txt)")
	modeFlag := fs.String("mode", string(engine.ModeStrict), "error handling mode: strict or tolerant")
	concurrencyFlag := fs.Int("concurrency", 2, "maximum concurrent validation sessions")
	itemBudgetFlag := fs.Int("item-budget", 64, "maximum policy items per session")
	maxErrorFlag := fs.Int64("max-error-bound", 100, "package error upper bound in score_1e-6")
	configFlag := fs.String("config", "", "optional JSON configuration file")
	keepFlag := fs.Bool("keep", false, "keep previous database and outputs")
	if err := fs.Parse(args); err != nil {
		return err
	}
	fileConfig, err := config.Load(*configFlag)
	if err != nil {
		return err
	}
	cfg := engine.Config{
		Mode:                engine.Mode(*modeFlag),
		MaxConcurrency:      *concurrencyFlag,
		MaxItems:            *itemBudgetFlag,
		MaxErrorBoundMicros: *maxErrorFlag,
	}
	applyFileConfig(&cfg, fileConfig)
	applyDemoOverrides(fs, &cfg)
	if cfg.Mode != engine.ModeStrict && cfg.Mode != engine.ModeTolerant {
		return fmt.Errorf("invalid mode %q (want strict or tolerant)", cfg.Mode)
	}
	workdirPath, err := filepath.Abs(*workdir)
	if err != nil {
		return err
	}
	samplesDir := *samplesDirFlag
	if samplesDir == "" {
		samplesDir = filepath.Join(workdirPath, "samples")
	}
	dbPath := *dbFlag
	if dbPath == "" {
		dbPath = filepath.Join(workdirPath, "state", "snapshots.db")
	}
	reportPath := *reportFlag
	if reportPath == "" {
		reportPath = filepath.Join(workdirPath, "report.json")
	}
	summaryPath := *summaryFlag
	if summaryPath == "" {
		summaryPath = filepath.Join(workdirPath, "summary.txt")
	}
	if !*keepFlag {
		if err := cleanDemoOutputs(workdirPath, samplesDir, dbPath, reportPath, summaryPath); err != nil {
			return err
		}
	}
	if _, err := samples.Generate(samplesDir); err != nil {
		return err
	}
	sampleDigest, err := samples.Digest(samplesDir)
	if err != nil {
		return err
	}
	output, err := app.Run(context.Background(), app.RunOptions{
		SamplesRoot:  samplesDir,
		DatabasePath: dbPath,
		Config:       cfg,
	})
	if err != nil {
		return err
	}
	if err := writeFile(reportPath, output.ReportJSON); err != nil {
		return err
	}
	if err := writeFile(summaryPath, []byte(output.SummaryText)); err != nil {
		return err
	}
	reportDigest := sha256Hex(output.ReportJSON)
	fmt.Print(output.SummaryText)
	fmt.Printf("\nsample_sha256=%s\nreport_sha256=%s\nreport=%s\nsummary=%s\ndb=%s\n",
		sampleDigest, reportDigest, reportPath, summaryPath, dbPath)
	return nil
}

func runVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	workdir := fs.String("workdir", filepath.Join("work", "verify"), "verification work directory")
	samplesDirFlag := fs.String("samples", "", "sample directory (default <workdir>/samples)")
	injectFlag := fs.String("inject", "all", "comma-separated: all,unknown-unit,mul-overflow,filetime-overflow,version-gap,error-bound")
	configFlag := fs.String("config", "", "optional JSON configuration file")
	modeFlag := fs.String("mode", string(engine.ModeStrict), "verification mode (strict is required for assertions)")
	concurrencyFlag := fs.Int("concurrency", 2, "maximum concurrent sessions used outside the isolated checks")
	itemBudgetFlag := fs.Int("item-budget", 64, "maximum policy items per session")
	maxErrorFlag := fs.Int64("max-error-bound", 100, "strict package error bound in score_1e-6")
	keepFlag := fs.Bool("keep", false, "keep previous verification database")
	if err := fs.Parse(args); err != nil {
		return err
	}
	fileConfig, err := config.Load(*configFlag)
	if err != nil {
		return err
	}
	cfg := engine.Config{
		Mode:                engine.Mode(*modeFlag),
		MaxConcurrency:      *concurrencyFlag,
		MaxItems:            *itemBudgetFlag,
		MaxErrorBoundMicros: *maxErrorFlag,
	}
	applyFileConfig(&cfg, fileConfig)
	applyVerifyOverrides(fs, &cfg)
	if cfg.Mode != engine.ModeStrict {
		return errors.New("verify requires strict mode for stable anomaly assertions")
	}
	workdirPath, err := filepath.Abs(*workdir)
	if err != nil {
		return err
	}
	samplesDir := *samplesDirFlag
	if samplesDir == "" {
		samplesDir = filepath.Join(workdirPath, "samples")
	}
	injections := splitCSV(*injectFlag)
	if !flagWasSet(fs, "inject") && len(fileConfig.Inject) > 0 {
		injections = fileConfig.Inject
	}
	if !*keepFlag {
		if err := os.RemoveAll(filepath.Join(workdirPath, "cases")); err != nil {
			return err
		}
	}
	if _, err := samples.Generate(samplesDir); err != nil {
		return err
	}
	verification, err := verify.Run(context.Background(), verify.Options{
		Workdir:     workdirPath,
		SamplesRoot: samplesDir,
		Injections:  injections,
		Config:      cfg,
	})
	if err != nil {
		return err
	}
	encoded, err := verify.JSONBytes(verification)
	if err != nil {
		return err
	}
	reportPath := filepath.Join(workdirPath, "verification.json")
	summaryPath := filepath.Join(workdirPath, "verification-summary.txt")
	if err := writeFile(reportPath, encoded); err != nil {
		return err
	}
	summary := verify.Summary(verification)
	if err := writeFile(summaryPath, []byte(summary)); err != nil {
		return err
	}
	fmt.Print(summary)
	fmt.Printf("\nverification_sha256=%s\nreport=%s\nsummary=%s\n", sha256Hex(encoded), reportPath, summaryPath)
	if !verification.AllPassed {
		return errors.New("one or more anomaly assertions failed")
	}
	return nil
}

func applyFileConfig(cfg *engine.Config, file config.File) {
	if file.Mode != "" {
		cfg.Mode = engine.Mode(file.Mode)
	}
	if file.MaxConcurrency > 0 {
		cfg.MaxConcurrency = file.MaxConcurrency
	}
	if file.MaxItems > 0 {
		cfg.MaxItems = file.MaxItems
	}
	if file.MaxErrorBoundMicros > 0 {
		cfg.MaxErrorBoundMicros = file.MaxErrorBoundMicros
	}
}

func applyDemoOverrides(fs *flag.FlagSet, cfg *engine.Config) {
	if flagWasSet(fs, "mode") {
		cfg.Mode = engine.Mode(fs.Lookup("mode").Value.String())
	}
	if flagWasSet(fs, "concurrency") {
		fmt.Sscanf(fs.Lookup("concurrency").Value.String(), "%d", &cfg.MaxConcurrency)
	}
	if flagWasSet(fs, "item-budget") {
		fmt.Sscanf(fs.Lookup("item-budget").Value.String(), "%d", &cfg.MaxItems)
	}
	if flagWasSet(fs, "max-error-bound") {
		fmt.Sscanf(fs.Lookup("max-error-bound").Value.String(), "%d", &cfg.MaxErrorBoundMicros)
	}
}

func applyVerifyOverrides(fs *flag.FlagSet, cfg *engine.Config) {
	if flagWasSet(fs, "mode") {
		cfg.Mode = engine.Mode(fs.Lookup("mode").Value.String())
	}
	if flagWasSet(fs, "concurrency") {
		fmt.Sscanf(fs.Lookup("concurrency").Value.String(), "%d", &cfg.MaxConcurrency)
	}
	if flagWasSet(fs, "item-budget") {
		fmt.Sscanf(fs.Lookup("item-budget").Value.String(), "%d", &cfg.MaxItems)
	}
	if flagWasSet(fs, "max-error-bound") {
		fmt.Sscanf(fs.Lookup("max-error-bound").Value.String(), "%d", &cfg.MaxErrorBoundMicros)
	}
}

func flagWasSet(fs *flag.FlagSet, name string) bool {
	wasSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			wasSet = true
		}
	})
	return wasSet
}

func splitCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.Split(value, ",")
}

func cleanDemoOutputs(workdir, samplesDir, dbPath, reportPath, summaryPath string) error {
	targets := []string{samplesDir, filepath.Dir(dbPath), reportPath, summaryPath}
	for _, target := range targets {
		if target == "" {
			continue
		}
		abs, err := filepath.Abs(target)
		if err != nil {
			return err
		}
		if filepath.Dir(abs) == abs {
			return fmt.Errorf("refusing to remove filesystem root %s", abs)
		}
		if abs == workdir && target != samplesDir {
			return fmt.Errorf("refusing to remove work directory %s", abs)
		}
		if err := os.RemoveAll(abs); err != nil {
			return err
		}
	}
	return os.MkdirAll(workdir, 0o755)
}

func writeFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
