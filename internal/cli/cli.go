package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"

	"vpnbench/internal/anomaly"
	"vpnbench/internal/app"
	"vpnbench/internal/config"
	"vpnbench/internal/model"
	"vpnbench/internal/report"
	"vpnbench/internal/samplegen"
	"vpnbench/internal/store"
	"vpnbench/internal/verify"
)

const usage = `vpnbench - deterministic VPN tunnel adjudication workbench

Commands:
  generate  --out DIR
  run       --config FILE --db FILE --report FILE
            or --policy FILE --requests FILE --session ID --db FILE --report FILE
  inject    --params FILE --db FILE --out FILE
  report    --db FILE --out FILE
  recover   --db FILE [--out FILE]
  correction --db FILE --session ID --stream ID --seq N
  verify    --samples DIR [--work DIR] --out FILE
`

type encodedError struct {
	OK    bool              `json:"ok"`
	Error *model.CodedError `json:"error"`
}

type summary struct {
	OK      bool     `json:"ok"`
	Command string   `json:"command"`
	Detail  string   `json:"detail,omitempty"`
	Codes   []string `json:"codes,omitempty"`
}

func Main(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "generate":
		return runGenerate(args[1:], stdout, stderr)
	case "run":
		return runWorkbench(args[1:], stdout, stderr)
	case "inject":
		return runInject(args[1:], stdout, stderr)
	case "report":
		return runReport(args[1:], stdout, stderr)
	case "recover":
		return runRecover(args[1:], stdout, stderr)
	case "correction", "query-correction":
		return runCorrection(args[1:], stdout, stderr)
	case "verify":
		return runVerify(args[1:], stdout, stderr)
	case "help", "--help", "-h":
		fmt.Fprint(stdout, usage)
		return 0
	default:
		return fail(stderr, model.NewError(model.CodeInternal, "unknown command "+args[0], 0))
	}
}

func runGenerate(args []string, stdout, stderr io.Writer) int {
	flags := newFlags("generate")
	out := flags.String("out", "samples", "output sample directory")
	if code := parseFlags(flags, args, stderr); code != 0 {
		return code
	}
	manifest, err := samplegen.Generate(*out)
	if err != nil {
		return fail(stderr, err)
	}
	return printSummary(stdout, summary{OK: true, Command: "generate", Detail: fmt.Sprintf("%d files", len(manifest.Files))})
}

func runWorkbench(args []string, stdout, stderr io.Writer) int {
	flags := newFlags("run")
	configPath := flags.String("config", "", "run config JSON")
	policyPath := flags.String("policy", "", "policy JSON")
	requestsPath := flags.String("requests", "", "length-prefixed requests")
	sessionID := flags.String("session", "session-1", "single-session ID")
	dbPath := flags.String("db", "", "SQLite database path")
	reportPath := flags.String("report", "", "output report path")
	concurrency := flags.Int("concurrency", 1, "concurrent session limit")
	requestBudget := flags.Int("request-budget", 10000, "per-session request budget")
	latencyClock := flags.String("latency-clock", "fixed", "fixed or real monotonic clock")
	if code := parseFlags(flags, args, stderr); code != 0 {
		return code
	}
	if *dbPath == "" || *reportPath == "" {
		return fail(stderr, model.NewError(model.CodeInternal, "--db and --report are required", 0))
	}
	var spec app.RunSpec
	if *configPath != "" {
		loaded, err := app.LoadRunConfig(*configPath)
		if err != nil {
			return fail(stderr, model.NewError(model.CodeInternal, err.Error(), 0))
		}
		spec = loaded
	} else {
		if *policyPath == "" || *requestsPath == "" {
			return fail(stderr, model.NewError(model.CodeInternal, "--policy and --requests are required without --config", 0))
		}
		spec = app.RunSpec{
			PolicyPath:       *policyPath,
			ConcurrencyLimit: *concurrency,
			LatencyClock:     *latencyClock,
			DefaultBudget:    *requestBudget,
			Sessions: []config.SessionSpec{{
				SessionID:     *sessionID,
				RequestsFile:  *requestsPath,
				RequestBudget: *requestBudget,
			}},
		}
	}
	run, err := app.Run(context.Background(), spec, *dbPath)
	if err != nil {
		return fail(stderr, err)
	}
	if err := run.Report.Write(*reportPath); err != nil {
		return fail(stderr, model.NewError(model.CodeInternal, err.Error(), 0))
	}
	codes := reportCodes(run.Report)
	exit := 0
	for _, code := range codes {
		if code == string(model.CodeResultMismatch) {
			exit = 1
		}
	}
	detail := fmt.Sprintf("%d sessions, %d requests", len(run.Report.Sessions), requestCount(run.Report))
	if code := printSummary(stdout, summary{OK: exit == 0, Command: "run", Detail: detail, Codes: codes}); code != 0 {
		return code
	}
	return exit
}

func runInject(args []string, stdout, stderr io.Writer) int {
	flags := newFlags("inject")
	paramsPath := flags.String("params", "", "anomaly parameters JSON")
	dbPath := flags.String("db", "", "SQLite database path")
	outPath := flags.String("out", "", "anomaly report path")
	if code := parseFlags(flags, args, stderr); code != 0 {
		return code
	}
	if *paramsPath == "" || *dbPath == "" || *outPath == "" {
		return fail(stderr, model.NewError(model.CodeInternal, "--params, --db and --out are required", 0))
	}
	result, err := anomaly.Run(context.Background(), *paramsPath, *dbPath)
	if err != nil {
		return fail(stderr, err)
	}
	if err := result.Write(*outPath); err != nil {
		return fail(stderr, model.NewError(model.CodeInternal, err.Error(), 0))
	}
	observed := make([]string, 0, len(result.ObservedCodes))
	for _, code := range result.ObservedCodes {
		observed = append(observed, string(code))
	}
	if code := printSummary(stdout, summary{OK: result.Passed, Command: "inject", Detail: result.Case, Codes: observed}); code != 0 {
		return code
	}
	if !result.Passed {
		return 1
	}
	return 0
}

func runReport(args []string, stdout, stderr io.Writer) int {
	flags := newFlags("report")
	dbPath := flags.String("db", "", "SQLite database path")
	outPath := flags.String("out", "", "output report path")
	if code := parseFlags(flags, args, stderr); code != 0 {
		return code
	}
	if *dbPath == "" || *outPath == "" {
		return fail(stderr, model.NewError(model.CodeInternal, "--db and --out are required", 0))
	}
	built, err := app.ReportFromDB(context.Background(), *dbPath)
	if err != nil {
		return fail(stderr, err)
	}
	if err := built.Write(*outPath); err != nil {
		return fail(stderr, model.NewError(model.CodeInternal, err.Error(), 0))
	}
	return printSummary(stdout, summary{OK: true, Command: "report", Detail: fmt.Sprintf("%d sessions", len(built.Sessions))})
}

func runRecover(args []string, stdout, stderr io.Writer) int {
	flags := newFlags("recover")
	dbPath := flags.String("db", "", "SQLite database path")
	outPath := flags.String("out", "", "optional output path")
	if code := parseFlags(flags, args, stderr); code != 0 {
		return code
	}
	if *dbPath == "" {
		return fail(stderr, model.NewError(model.CodeInternal, "--db is required", 0))
	}
	recovery, err := app.Recover(context.Background(), *dbPath)
	if err != nil {
		return fail(stderr, err)
	}
	if *outPath != "" {
		if err := recovery.Write(*outPath); err != nil {
			return fail(stderr, model.NewError(model.CodeInternal, err.Error(), 0))
		}
		return printSummary(stdout, summary{OK: true, Command: "recover", Detail: fmt.Sprintf("%d windows", len(recovery.Windows))})
	}
	encoded, err := recovery.Bytes()
	if err != nil {
		return fail(stderr, model.NewError(model.CodeInternal, err.Error(), 0))
	}
	_, _ = stdout.Write(encoded)
	return 0
}

func runCorrection(args []string, stdout, stderr io.Writer) int {
	flags := newFlags("correction")
	dbPath := flags.String("db", "", "SQLite database path")
	sessionID := flags.String("session", "", "session ID")
	streamID := flags.String("stream", "", "stream ID")
	seq := flags.Int64("seq", 0, "request sequence")
	if code := parseFlags(flags, args, stderr); code != 0 {
		return code
	}
	if *dbPath == "" || *sessionID == "" || *streamID == "" || *seq <= 0 {
		return fail(stderr, model.NewError(model.CodeInternal, "--db, --session, --stream and positive --seq are required", 0))
	}
	database, err := store.Open(*dbPath)
	if err != nil {
		return fail(stderr, err)
	}
	defer database.Close()
	corrections, err := database.QueryCorrections(context.Background(), *sessionID, *streamID, *seq)
	if err != nil {
		return fail(stderr, err)
	}
	encoded, err := json.MarshalIndent(corrections, "", "  ")
	if err != nil {
		return fail(stderr, model.NewError(model.CodeInternal, err.Error(), 0))
	}
	_, _ = stdout.Write(append(encoded, '\n'))
	return 0
}

func runVerify(args []string, stdout, stderr io.Writer) int {
	flags := newFlags("verify")
	samplesDir := flags.String("samples", "samples", "sample directory")
	workDir := flags.String("work", "", "optional work directory")
	outPath := flags.String("out", "acceptance-report.json", "acceptance report path")
	if code := parseFlags(flags, args, stderr); code != 0 {
		return code
	}
	result, err := verify.Run(context.Background(), *samplesDir, *workDir)
	if err != nil {
		return fail(stderr, model.NewError(model.CodeInternal, err.Error(), 0))
	}
	if err := result.Write(*outPath); err != nil {
		return fail(stderr, model.NewError(model.CodeInternal, err.Error(), 0))
	}
	if code := printSummary(stdout, summary{OK: result.Passed, Command: "verify", Detail: fmt.Sprintf("%d anomalies", len(result.Anomalies))}); code != 0 {
		return code
	}
	if !result.Passed {
		return 1
	}
	return 0
}

func newFlags(name string) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	return flags
}

func parseFlags(flags *flag.FlagSet, args []string, stderr io.Writer) int {
	if err := flags.Parse(args); err != nil {
		return fail(stderr, model.NewError(model.CodeInternal, err.Error(), 0))
	}
	if flags.NArg() != 0 {
		return fail(stderr, model.NewError(model.CodeInternal, "unexpected positional arguments", 0))
	}
	return 0
}

func printSummary(writer io.Writer, value summary) int {
	encoded, err := json.Marshal(value)
	if err != nil {
		return 1
	}
	_, _ = writer.Write(append(encoded, '\n'))
	return 0
}

func fail(writer io.Writer, err error) int {
	var coded *model.CodedError
	if !errors.As(err, &coded) {
		coded = model.NewError(model.CodeInternal, err.Error(), 0)
	}
	encoded, marshalErr := json.Marshal(encodedError{OK: false, Error: coded})
	if marshalErr != nil {
		fmt.Fprintln(writer, err)
		return 2
	}
	_, _ = writer.Write(append(encoded, '\n'))
	return 2
}

func reportCodes(input report.Report) []string {
	set := make(map[string]struct{})
	for _, occurrence := range input.BusinessCodes {
		set[string(occurrence.Code)] = struct{}{}
	}
	codes := make([]string, 0, len(set))
	for code := range set {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	return codes
}

func requestCount(input report.Report) int {
	count := 0
	for _, session := range input.Sessions {
		count += len(session.Requests)
	}
	return count
}
