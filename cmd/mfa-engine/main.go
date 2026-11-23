package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	mfaengine "mfaengine"
	"mfaengine/internal/scenario"
)

type errorDocument struct {
	Code   mfaengine.Code `json:"code"`
	Reason string         `json:"reason"`
	Detail any            `json:"detail,omitempty"`
}

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	command := "suite"
	commandArgs := args
	if len(args) > 0 && args[0] != "" && args[0][0] != '-' {
		command = args[0]
		commandArgs = args[1:]
	}
	switch command {
	case "suite":
		return runSuite(commandArgs)
	case "validate":
		return runValidate(commandArgs)
	case "register":
		return runRegister(commandArgs)
	case "recover":
		return runRecover(commandArgs)
	case "auth":
		return runAuth(commandArgs)
	case "rebind":
		return runRebind(commandArgs)
	case "expire":
		return runExpire(commandArgs)
	case "inject":
		return runInject(commandArgs)
	case "example-config":
		return runExampleConfig(commandArgs)
	case "crash-auth":
		return runCrashAuth(commandArgs)
	case "crash-rebind":
		return runCrashRebind(commandArgs)
	case "help", "-h", "--help":
		printUsage()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", command)
		printUsage()
		return 64
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `mfa-engine - deterministic multi-tenant MFA engine

Usage:
  mfa-engine [suite] [--workdir DIR] [--out FILE|-]
  mfa-engine validate --config FILE
  mfa-engine register --db FILE --config FILE
  mfa-engine recover --db FILE
  mfa-engine auth --db FILE --tenant ID --user ID --attempt ID [--offset N] [--mode valid|invalid] [--recovery-index N] [--hints ID,ID]
  mfa-engine rebind --db FILE --tenant ID --user ID --type totp --new-id ID --attempt ID [--sensitivity high] [--hold-ms N]
  mfa-engine expire --db FILE --tenant ID --user ID --attempt ID
  mfa-engine inject --db FILE --kind snapshot_version_gap|snapshot_wholesale_replacement
  mfa-engine example-config [--out FILE|-]
  mfa-engine crash-auth --db FILE --point before-commit|after-commit --tenant ID --user ID --attempt ID
  mfa-engine crash-rebind --db FILE --point before-commit|after-commit --tenant ID --user ID --new-id ID --attempt ID

The default suite creates deterministic SQLite databases, runs acceptance scenarios,
and writes a stable JSON diagnostic report.`)
}

func runRegister(args []string) int {
	flags := flag.NewFlagSet("register", flag.ContinueOnError)
	dbPath := flags.String("db", "", "SQLite database path")
	configPath := flags.String("config", "", "configuration JSON path")
	if err := flags.Parse(args); err != nil {
		return 64
	}
	if *dbPath == "" || *configPath == "" {
		fmt.Fprintln(os.Stderr, "--db and --config are required")
		return 64
	}
	cfg, err := mfaengine.LoadConfigFile(*configPath)
	if err != nil {
		return writeError(err)
	}
	engine, err := mfaengine.NewEngine(context.Background(), *dbPath, mfaengine.EngineOptions{})
	if err != nil {
		return writeError(err)
	}
	defer engine.Close()
	if err := engine.Register(context.Background(), cfg); err != nil {
		return writeError(err)
	}
	status := engine.SnapshotStatus(context.Background())
	return writeJSONResult(map[string]any{
		"code": string(mfaengine.CodeOK), "snapshot_version": status.LatestValid,
		"factors": len(mustFactors(engine)),
	})
}

func mustFactors(engine *mfaengine.Engine) []mfaengine.Factor {
	factors, _ := engine.ListFactors(context.Background())
	return factors
}

func runSuite(args []string) int {
	flags := flag.NewFlagSet("suite", flag.ContinueOnError)
	workDir := flags.String("workdir", filepath.Join("artifacts", "run"), "deterministic SQLite work directory")
	outPath := flags.String("out", filepath.Join("artifacts", "report.json"), "report path, or - for stdout")
	if err := flags.Parse(args); err != nil {
		return 64
	}
	executable, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve executable: %v\n", err)
		return 1
	}
	report, err := scenario.Run(context.Background(), scenario.Options{WorkDir: *workDir, Executable: executable})
	if err != nil {
		fmt.Fprintf(os.Stderr, "run suite: %v\n", err)
		return 1
	}
	if *outPath == "-" {
		if err := mfaengine.WriteReport(os.Stdout, report); err != nil {
			fmt.Fprintf(os.Stderr, "write report: %v\n", err)
			return 1
		}
		return 0
	}
	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "create report directory: %v\n", err)
		return 1
	}
	file, err := os.Create(*outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create report: %v\n", err)
		return 1
	}
	if err := mfaengine.WriteReport(file, report); err != nil {
		_ = file.Close()
		fmt.Fprintf(os.Stderr, "write report: %v\n", err)
		return 1
	}
	if err := file.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close report: %v\n", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "report=%s scenarios=%d/%d faults=%d/%d\n", *outPath, report.Summary.Passed, report.Summary.Total, countPassedFaults(report.Faults), len(report.Faults))
	if report.Summary.Failed != 0 || countPassedFaults(report.Faults) != len(report.Faults) || (report.Recovery != nil && !report.Recovery.Passed) {
		return 2
	}
	return 0
}

func countPassedFaults(faults []mfaengine.FaultScenarioResult) int {
	passed := 0
	for _, fault := range faults {
		if fault.Passed {
			passed++
		}
	}
	return passed
}

func runValidate(args []string) int {
	flags := flag.NewFlagSet("validate", flag.ContinueOnError)
	configPath := flags.String("config", "", "configuration JSON path")
	if err := flags.Parse(args); err != nil {
		return 64
	}
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "--config is required")
		return 64
	}
	cfg, err := mfaengine.LoadConfigFile(*configPath)
	if err != nil {
		return writeError(err)
	}
	normalized, err := mfaengine.ValidateConfig(cfg)
	if err != nil {
		return writeError(err)
	}
	return writeJSONResult(normalized)
}

func runRecover(args []string) int {
	flags := flag.NewFlagSet("recover", flag.ContinueOnError)
	dbPath := flags.String("db", "", "SQLite database path")
	if err := flags.Parse(args); err != nil {
		return 64
	}
	if *dbPath == "" {
		fmt.Fprintln(os.Stderr, "--db is required")
		return 64
	}
	engine, err := mfaengine.NewEngine(context.Background(), *dbPath, mfaengine.EngineOptions{})
	if err != nil {
		return writeError(err)
	}
	defer engine.Close()
	result, recoverErr := engine.Recover(context.Background())
	if code := writeJSONResult(result); code != 0 {
		return code
	}
	if recoverErr != nil {
		return 2
	}
	return 0
}

func runAuth(args []string) int {
	flags := flag.NewFlagSet("auth", flag.ContinueOnError)
	dbPath := flags.String("db", "", "SQLite database path")
	tenantID := flags.String("tenant", "", "tenant identifier")
	userID := flags.String("user", "", "user identifier")
	attemptID := flags.String("attempt", "", "attempt identifier")
	offset := flags.Int("offset", 0, "TOTP step offset")
	mode := flags.String("mode", "valid", "pseudo-response mode: valid or invalid")
	recoveryIndex := flags.Int("recovery-index", -1, "recovery code index, or -1 for deterministic first available")
	hints := flags.String("hints", "", "comma-separated factor identifiers that must satisfy the policy")
	if err := flags.Parse(args); err != nil {
		return 64
	}
	if *dbPath == "" || *tenantID == "" || *userID == "" || *attemptID == "" {
		fmt.Fprintln(os.Stderr, "--db, --tenant, --user and --attempt are required")
		return 64
	}
	engine, err := mfaengine.NewEngine(context.Background(), *dbPath, mfaengine.EngineOptions{})
	if err != nil {
		return writeError(err)
	}
	defer engine.Close()
	request := mfaengine.AuthRequest{
		TenantID: *tenantID, UserID: *userID, AttemptID: *attemptID,
		SessionID: "session-" + *attemptID, TOTPStepOffset: *offset, AnswerMode: *mode,
	}
	if *recoveryIndex >= 0 {
		value := *recoveryIndex
		request.RecoveryCodeIndex = &value
	}
	if strings.TrimSpace(*hints) != "" {
		for _, hint := range strings.Split(*hints, ",") {
			if hint = strings.TrimSpace(hint); hint != "" {
				request.FactorHints = append(request.FactorHints, hint)
			}
		}
	}
	result, authErr := engine.Authenticate(context.Background(), request)
	if code := writeJSONResult(result); code != 0 {
		return code
	}
	if authErr != nil {
		return 2
	}
	return 0
}

func runRebind(args []string) int {
	flags := flag.NewFlagSet("rebind", flag.ContinueOnError)
	dbPath := flags.String("db", "", "SQLite database path")
	tenantID := flags.String("tenant", "", "tenant identifier")
	userID := flags.String("user", "", "user identifier")
	factorType := flags.String("type", "", "factor type")
	newFactorID := flags.String("new-id", "", "replacement factor identifier")
	attemptID := flags.String("attempt", "", "attempt identifier")
	sensitivity := flags.String("sensitivity", "high", "new factor sensitivity label")
	holdMillis := flags.Int("hold-ms", 0, "keep the rebind lock held before the transaction")
	expires := flags.Int64("expires-at", 0, "new factor expiry Unix timestamp")
	if err := flags.Parse(args); err != nil {
		return 64
	}
	if *dbPath == "" || *tenantID == "" || *userID == "" || *factorType == "" || *newFactorID == "" || *attemptID == "" {
		fmt.Fprintln(os.Stderr, "--db, --tenant, --user, --type, --new-id and --attempt are required")
		return 64
	}
	engine, err := mfaengine.NewEngine(context.Background(), *dbPath, mfaengine.EngineOptions{})
	if err != nil {
		return writeError(err)
	}
	defer engine.Close()
	result, rebindErr := engine.Rebind(context.Background(), mfaengine.RebindRequest{
		TenantID: *tenantID, UserID: *userID, FactorType: mfaengine.FactorType(*factorType),
		NewFactorID: *newFactorID, AttemptID: *attemptID,
		Sensitivity: mfaengine.Sensitivity(*sensitivity), HoldMillis: *holdMillis, ExpiresAtUnix: *expires,
	})
	if code := writeJSONResult(result); code != 0 {
		return code
	}
	if rebindErr != nil {
		return 2
	}
	return 0
}

func runExpire(args []string) int {
	flags := flag.NewFlagSet("expire", flag.ContinueOnError)
	dbPath := flags.String("db", "", "SQLite database path")
	tenantID := flags.String("tenant", "", "tenant identifier")
	userID := flags.String("user", "", "user identifier")
	attemptID := flags.String("attempt", "", "attempt identifier")
	if err := flags.Parse(args); err != nil {
		return 64
	}
	if *dbPath == "" || *tenantID == "" || *userID == "" || *attemptID == "" {
		fmt.Fprintln(os.Stderr, "--db, --tenant, --user and --attempt are required")
		return 64
	}
	engine, err := mfaengine.NewEngine(context.Background(), *dbPath, mfaengine.EngineOptions{})
	if err != nil {
		return writeError(err)
	}
	defer engine.Close()
	result, expireErr := engine.ExpireFactors(context.Background(), *tenantID, *userID, *attemptID)
	if code := writeJSONResult(result); code != 0 {
		return code
	}
	if expireErr != nil {
		return 2
	}
	return 0
}

func runInject(args []string) int {
	flags := flag.NewFlagSet("inject", flag.ContinueOnError)
	dbPath := flags.String("db", "", "SQLite database path")
	kind := flags.String("kind", "", "snapshot_version_gap or snapshot_wholesale_replacement")
	if err := flags.Parse(args); err != nil {
		return 64
	}
	if *dbPath == "" || *kind == "" {
		fmt.Fprintln(os.Stderr, "--db and --kind are required")
		return 64
	}
	engine, err := mfaengine.NewEngine(context.Background(), *dbPath, mfaengine.EngineOptions{})
	if err != nil {
		return writeError(err)
	}
	defer engine.Close()
	fault := mfaengine.FaultInjection{Kind: mfaengine.FaultKind(*kind)}
	if err := engine.ApplyFault(context.Background(), fault); err != nil {
		return writeError(err)
	}
	return writeJSONResult(map[string]string{"code": string(mfaengine.CodeOK), "kind": *kind})
}

func runExampleConfig(args []string) int {
	flags := flag.NewFlagSet("example-config", flag.ContinueOnError)
	outPath := flags.String("out", "-", "output path, or - for stdout")
	if err := flags.Parse(args); err != nil {
		return 64
	}
	cfg := scenario.DemoConfig()
	if *outPath == "-" {
		return writeJSONResult(cfg)
	}
	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "create config directory: %v\n", err)
		return 1
	}
	file, err := os.Create(*outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create config: %v\n", err)
		return 1
	}
	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(cfg); err != nil {
		_ = file.Close()
		fmt.Fprintf(os.Stderr, "write config: %v\n", err)
		return 1
	}
	return writeFileClose(file)
}

func writeFileClose(file *os.File) int {
	if err := file.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close file: %v\n", err)
		return 1
	}
	return 0
}

func runCrashAuth(args []string) int {
	flags := flag.NewFlagSet("crash-auth", flag.ContinueOnError)
	dbPath := flags.String("db", "", "SQLite database path")
	point := flags.String("point", "", "before-commit or after-commit")
	tenantID := flags.String("tenant", "", "tenant identifier")
	userID := flags.String("user", "", "user identifier")
	attemptID := flags.String("attempt", "", "attempt identifier")
	if err := flags.Parse(args); err != nil {
		return 64
	}
	if *dbPath == "" || *tenantID == "" || *userID == "" || *attemptID == "" {
		fmt.Fprintln(os.Stderr, "--db, --tenant, --user and --attempt are required")
		return 64
	}
	options := mfaengine.EngineOptions{}
	switch *point {
	case "before-commit":
		options.BeforeCommit = func(operation string) {
			if operation == "authenticate" {
				os.Exit(86)
			}
		}
	case "after-commit":
		options.AfterCommit = func(operation string) {
			if operation == "authenticate" {
				os.Exit(86)
			}
		}
	default:
		fmt.Fprintln(os.Stderr, "--point must be before-commit or after-commit")
		return 64
	}
	engine, err := mfaengine.NewEngine(context.Background(), *dbPath, options)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open engine: %v\n", err)
		return 1
	}
	defer engine.Close()
	_, authErr := engine.Authenticate(context.Background(), mfaengine.AuthRequest{
		TenantID: *tenantID, UserID: *userID, AttemptID: *attemptID,
		SessionID: "session-" + *attemptID,
	})
	if authErr != nil {
		fmt.Fprintf(os.Stderr, "auth failed before crash point: %v\n", authErr)
		return 2
	}
	fmt.Fprintln(os.Stderr, "crash point was not reached")
	return 1
}

func runCrashRebind(args []string) int {
	flags := flag.NewFlagSet("crash-rebind", flag.ContinueOnError)
	dbPath := flags.String("db", "", "SQLite database path")
	point := flags.String("point", "", "before-commit or after-commit")
	tenantID := flags.String("tenant", "", "tenant identifier")
	userID := flags.String("user", "", "user identifier")
	newFactorID := flags.String("new-id", "", "replacement factor identifier")
	attemptID := flags.String("attempt", "", "attempt identifier")
	factorType := flags.String("type", "totp", "factor type")
	sensitivity := flags.String("sensitivity", "high", "new factor sensitivity label")
	if err := flags.Parse(args); err != nil {
		return 64
	}
	if *dbPath == "" || *tenantID == "" || *userID == "" || *newFactorID == "" || *attemptID == "" {
		fmt.Fprintln(os.Stderr, "--db, --tenant, --user, --new-id and --attempt are required")
		return 64
	}
	options := mfaengine.EngineOptions{}
	switch *point {
	case "before-commit":
		options.BeforeCommit = func(operation string) {
			if operation == "rebind" {
				os.Exit(86)
			}
		}
	case "after-commit":
		options.AfterCommit = func(operation string) {
			if operation == "rebind" {
				os.Exit(86)
			}
		}
	default:
		fmt.Fprintln(os.Stderr, "--point must be before-commit or after-commit")
		return 64
	}
	engine, err := mfaengine.NewEngine(context.Background(), *dbPath, options)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open engine: %v\n", err)
		return 1
	}
	defer engine.Close()
	_, rebindErr := engine.Rebind(context.Background(), mfaengine.RebindRequest{
		TenantID: *tenantID, UserID: *userID, FactorType: mfaengine.FactorType(*factorType),
		NewFactorID: *newFactorID, AttemptID: *attemptID, Sensitivity: mfaengine.Sensitivity(*sensitivity),
	})
	if rebindErr != nil {
		fmt.Fprintf(os.Stderr, "rebind failed before crash point: %v\n", rebindErr)
		return 2
	}
	fmt.Fprintln(os.Stderr, "crash point was not reached")
	return 1
}

func writeError(err error) int {
	document := errorDocument{Code: mfaengine.ErrorCode(err), Reason: mfaengine.ErrorReason(err)}
	_ = writeJSON(document)
	return 2
}

func writeJSON(value any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func writeJSONResult(value any) int {
	if err := writeJSON(value); err != nil {
		fmt.Fprintf(os.Stderr, "write JSON: %v\n", err)
		return 1
	}
	return 0
}
