package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"dns-cache-workbench/internal/sample"
	"dns-cache-workbench/internal/verify"
	"dns-cache-workbench/internal/workbench"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		writeError(err)
		os.Exit(2)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		printUsage()
		return fmt.Errorf("missing subcommand")
	}
	switch args[0] {
	case "generate":
		return runGenerate(args[1:])
	case "scan":
		return runScan(ctx, args[1:])
	case "query":
		return runQuery(args[1:])
	case "replay":
		return runReplay(args[1:])
	case "switch-version":
		return runSwitchVersion(args[1:])
	case "snapshots":
		return runSnapshots(args[1:])
	case "verify-injections":
		return runVerifyInjections(ctx, args[1:])
	case "demo":
		return runDemo(ctx, args[1:])
	case "help", "-h", "--help":
		printUsage()
		return nil
	default:
		printUsage()
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func runGenerate(args []string) error {
	flags := flag.NewFlagSet("generate", flag.ContinueOnError)
	out := flags.String("out", "artifacts/samples", "deterministic sample output directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return fmt.Errorf("--out is required")
	}
	if err := sample.Generate(*out); err != nil {
		return err
	}
	payload := struct {
		Out        string   `json:"out"`
		Clean      string   `json:"clean"`
		Injections []string `json:"injections"`
	}{
		Out:        filepath.ToSlash(*out),
		Clean:      filepath.ToSlash(filepath.Join(*out, "clean")),
		Injections: append([]string(nil), sample.InjectionKinds...),
	}
	return writePretty(payload)
}

func runScan(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("scan", flag.ContinueOnError)
	input := flags.String("input", "artifacts/samples/clean", "observation window source directory")
	db := flags.String("db", "artifacts/dnswatch.db", "SQLite snapshot database")
	reportPath := flags.String("report", "", "JSON report output path")
	target := flags.String("target-parser", "v2", "target parser version: v1 or v2")
	concurrency := flags.Int("max-concurrency", 3, "maximum concurrently parsed sources")
	maxEvents := flags.Int("max-events", 10000, "maximum events per source (0 means unlimited)")
	window := flags.Uint64("window-ticks", 10, "response-to-cache association window")
	flip := flags.Uint64("flip-window-ticks", 3, "rapid address flip window")
	jump := flags.Int("snapshot-jump", 0, "inject a snapshot version gap for validation")
	noCommit := flags.Bool("no-commit", false, "parse and report without committing a snapshot")
	fresh := flags.Bool("fresh-db", false, "remove the snapshot database before scanning")
	stdoutReport := flags.Bool("stdout", false, "print the complete report to stdout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *fresh && !*noCommit {
		if err := os.Remove(*db); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	report, err := workbench.Scan(ctx, workbench.ScanOptions{
		InputDir:        *input,
		DBPath:          *db,
		ReportPath:      *reportPath,
		TargetParser:    *target,
		MaxConcurrency:  *concurrency,
		MaxEvents:       *maxEvents,
		WindowTicks:     *window,
		FlipWindowTicks: *flip,
		SnapshotJump:    *jump,
		NoCommit:        *noCommit,
	})
	if err != nil {
		return err
	}
	if *stdoutReport {
		return writePretty(report)
	}
	return writePretty(scanSummary(report))
}

type codeCount struct {
	Code  string `json:"code"`
	Count int    `json:"count"`
}

type scanOutput struct {
	Tool                string      `json:"tool"`
	SnapshotVersion     int         `json:"snapshot_version,omitempty"`
	TargetParserVersion string      `json:"target_parser_version"`
	Compatible          bool        `json:"compatible"`
	CompatibilityCode   string      `json:"compatibility_code,omitempty"`
	SourceCount         int         `json:"source_count"`
	TransactionCount    int         `json:"transaction_count"`
	FindingCount        int         `json:"finding_count"`
	FindingCodes        []codeCount `json:"finding_codes"`
	ReportPath          string      `json:"report_path,omitempty"`
}

func scanSummary(report workbench.Report) scanOutput {
	version := 0
	if report.Snapshot != nil {
		version = report.Snapshot.Version
	}
	counts := make(map[string]int)
	for _, finding := range report.Findings {
		counts[finding.Code]++
	}
	codes := make([]codeCount, 0, len(counts))
	for code, count := range counts {
		codes = append(codes, codeCount{Code: code, Count: count})
	}
	sort.Slice(codes, func(i, j int) bool { return codes[i].Code < codes[j].Code })
	return scanOutput{
		Tool:                report.Tool,
		SnapshotVersion:     version,
		TargetParserVersion: report.TargetParserVersion,
		Compatible:          report.Compatibility.Compatible,
		CompatibilityCode:   report.Compatibility.Code,
		SourceCount:         len(report.Sources),
		TransactionCount:    len(report.Transactions),
		FindingCount:        len(report.Findings),
		FindingCodes:        codes,
	}
}

func runQuery(args []string) error {
	flags := flag.NewFlagSet("query", flag.ContinueOnError)
	db := flags.String("db", "artifacts/dnswatch.db", "SQLite snapshot database")
	version := flags.Int("version", 0, "snapshot version (0 means latest)")
	txidText := flags.String("txid", "", "32-bit transaction id in decimal or 0x-hex form")
	parser := flags.String("parser", "v2", "current parser version: v1 or v2")
	if err := flags.Parse(args); err != nil {
		return err
	}
	txid, err := workbench.ParseTxID(*txidText)
	if err != nil {
		return err
	}
	store, err := workbench.OpenStore(*db)
	if err != nil {
		return err
	}
	defer store.Close()
	var report workbench.Report
	if *version == 0 {
		report, err = store.LoadLatest(*parser)
	} else {
		report, err = store.LoadSnapshot(*version, *parser)
	}
	if err != nil {
		return err
	}
	found := make([]workbench.TransactionReport, 0)
	for _, transaction := range report.Transactions {
		if transaction.TxID == txid {
			found = append(found, transaction)
		}
	}
	if len(found) == 0 {
		return workbench.NewBusinessError(workbench.CodeParse, "", 0, fmt.Sprintf("txid %d has no transaction in the selected snapshot", txid))
	}
	return writePretty(struct {
		SnapshotVersion int                           `json:"snapshot_version"`
		Transactions    []workbench.TransactionReport `json:"transactions"`
	}{SnapshotVersion: report.Snapshot.Version, Transactions: found})
}

func runReplay(args []string) error {
	flags := flag.NewFlagSet("replay", flag.ContinueOnError)
	db := flags.String("db", "artifacts/dnswatch.db", "SQLite snapshot database")
	version := flags.Int("version", 0, "snapshot version (0 means latest)")
	parser := flags.String("parser", "v2", "current parser version: v1 or v2")
	reportPath := flags.String("report", "", "optional replay report output path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	store, err := workbench.OpenStore(*db)
	if err != nil {
		return err
	}
	defer store.Close()
	var report workbench.Report
	if *version == 0 {
		report, err = store.LoadLatest(*parser)
	} else {
		report, err = store.LoadSnapshot(*version, *parser)
	}
	if err != nil {
		return err
	}
	encoded, err := workbench.PrettyReportBytes(report)
	if err != nil {
		return err
	}
	if *reportPath != "" {
		if err := os.MkdirAll(filepath.Dir(*reportPath), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(*reportPath, encoded, 0o644); err != nil {
			return err
		}
	}
	fmt.Print(string(encoded))
	return nil
}

func runSnapshots(args []string) error {
	flags := flag.NewFlagSet("snapshots", flag.ContinueOnError)
	db := flags.String("db", "artifacts/dnswatch.db", "SQLite snapshot database")
	parser := flags.String("parser", "v2", "current parser version: v1 or v2")
	if err := flags.Parse(args); err != nil {
		return err
	}
	store, err := workbench.OpenStore(*db)
	if err != nil {
		return err
	}
	defer store.Close()
	list, err := store.ListSnapshots(*parser)
	if err != nil {
		return err
	}
	return writePretty(struct {
		CurrentParserVersion string                         `json:"current_parser_version"`
		Snapshots            []workbench.SnapshotDescriptor `json:"snapshots"`
	}{CurrentParserVersion: *parser, Snapshots: list})
}

func runSwitchVersion(args []string) error {
	flags := flag.NewFlagSet("switch-version", flag.ContinueOnError)
	db := flags.String("db", "artifacts/dnswatch.db", "SQLite snapshot database")
	parser := flags.String("parser", "v2", "target parser version: v1 or v2")
	version := flags.Int("version", 0, "snapshot version (0 means latest)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *parser != "v1" && *parser != "v2" {
		return fmt.Errorf("--parser must be v1 or v2")
	}
	store, err := workbench.OpenStore(*db)
	if err != nil {
		return err
	}
	defer store.Close()
	list, err := store.ListSnapshots(*parser)
	if err != nil {
		return err
	}
	if len(list) == 0 {
		return workbench.NewBusinessError(workbench.CodeSnapshotVersion, "", 0, "no committed snapshots")
	}
	selected := list[len(list)-1]
	if *version != 0 {
		selected = workbench.SnapshotDescriptor{}
		for _, snapshot := range list {
			if snapshot.Version == *version {
				selected = snapshot
				break
			}
		}
		if selected.Version == 0 {
			return workbench.NewBusinessError(workbench.CodeSnapshotVersion, "", 0, fmt.Sprintf("snapshot version %d does not exist", *version))
		}
	}
	output := struct {
		TargetParserVersion   string `json:"target_parser_version"`
		SnapshotVersion       int    `json:"snapshot_version"`
		RequiredParserVersion string `json:"required_parser_version"`
		Compatible            bool   `json:"compatible"`
		Code                  string `json:"code,omitempty"`
		Detail                string `json:"detail"`
	}{
		TargetParserVersion:   *parser,
		SnapshotVersion:       selected.Version,
		RequiredParserVersion: selected.RequiredParserVersion,
		Compatible:            selected.Compatible,
		Code:                  selected.Code,
		Detail:                "snapshot can be replayed with the target parser",
	}
	if !selected.Compatible {
		return workbench.NewBusinessError(workbench.CodeSnapshotVersion, "", 0, fmt.Sprintf("snapshot %d requires parser %s; target parser %s is incompatible and would be rejected as a whole", selected.Version, selected.RequiredParserVersion, *parser))
	}
	return writePretty(output)
}

func runVerifyInjections(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("verify-injections", flag.ContinueOnError)
	input := flags.String("input", "artifacts/samples", "generated sample root")
	work := flags.String("work", "artifacts/verification", "verification output directory")
	target := flags.String("target-parser", "v2", "target parser version: v1 or v2")
	window := flags.Uint64("window-ticks", 10, "association window")
	flip := flags.Uint64("flip-window-ticks", 3, "flip detection window")
	if err := flags.Parse(args); err != nil {
		return err
	}
	result, err := verify.Run(ctx, verify.Options{
		InputRoot: *input, WorkDir: *work, Target: *target, Window: *window, Flip: *flip,
	})
	if err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*work, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*work, "result.json"), append(encoded, '\n'), 0o644); err != nil {
		return err
	}
	if !result.AllPassed {
		return fmt.Errorf("one or more injection assertions failed")
	}
	return writePretty(result)
}

func runDemo(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("demo", flag.ContinueOnError)
	out := flags.String("out", "artifacts/demo", "demo output directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := os.RemoveAll(*out); err != nil {
		return err
	}
	samplesDir := filepath.Join(*out, "samples")
	if err := sample.Generate(samplesDir); err != nil {
		return err
	}
	cleanDir := filepath.Join(samplesDir, "clean")
	run1Dir := filepath.Join(*out, "run1")
	run2Dir := filepath.Join(*out, "run2")
	var reports [2]workbench.Report
	var reportBytes [2][]byte
	for index, dir := range []string{run1Dir, run2Dir} {
		report, err := workbench.Scan(ctx, workbench.ScanOptions{
			InputDir:        cleanDir,
			DBPath:          filepath.Join(dir, "dnswatch.db"),
			ReportPath:      filepath.Join(dir, "report.json"),
			TargetParser:    "v2",
			MaxConcurrency:  3,
			MaxEvents:       10000,
			WindowTicks:     10,
			FlipWindowTicks: 3,
		})
		if err != nil {
			return err
		}
		reports[index] = report
		encoded, err := workbench.PrettyReportBytes(report)
		if err != nil {
			return err
		}
		reportBytes[index] = encoded
	}
	if !bytes.Equal(reportBytes[0], reportBytes[1]) {
		return fmt.Errorf("determinism check failed: run reports differ")
	}
	store, err := workbench.OpenStore(filepath.Join(run1Dir, "dnswatch.db"))
	if err != nil {
		return err
	}
	defer store.Close()
	tx, err := workbench.QuerySnapshotTransaction(store, reports[0].Snapshot.Version, "v2", 0x3003)
	if err != nil {
		return err
	}
	transactionBytes, err := json.MarshalIndent(tx, "", "  ")
	if err != nil {
		return err
	}
	transactionBytes = append(transactionBytes, '\n')
	if err := os.MkdirAll(run1Dir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(run1Dir, "query-0x3003.json"), transactionBytes, 0o644); err != nil {
		return err
	}
	replayed, err := store.LoadSnapshot(reports[0].Snapshot.Version, "v2")
	if err != nil {
		return err
	}
	replayBytes, err := workbench.PrettyReportBytes(replayed)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(run1Dir, "replay.json"), replayBytes, 0o644); err != nil {
		return err
	}
	if _, incompatibleErr := store.LoadSnapshot(reports[0].Snapshot.Version, "v1"); incompatibleErr == nil {
		return fmt.Errorf("v1 replay of a v2-dependent snapshot unexpectedly succeeded")
	}
	verifyResult, err := verify.Run(ctx, verify.Options{
		InputRoot: samplesDir, WorkDir: filepath.Join(*out, "verification"), Target: "v2", Window: 10, Flip: 3,
	})
	if err != nil {
		return err
	}
	manifest := struct {
		Out                   string              `json:"out"`
		Deterministic         bool                `json:"deterministic_bytes"`
		Report1               string              `json:"report_1"`
		Report2               string              `json:"report_2"`
		SnapshotVersion       int                 `json:"snapshot_version"`
		FindingCodes          []string            `json:"finding_codes"`
		QueryTxID             string              `json:"query_txid"`
		Replay                string              `json:"replay"`
		V1ReplayCode          string              `json:"v1_replay_code"`
		InjectionVerification bool                `json:"injection_verification_passed"`
		InjectionCases        []verify.CaseResult `json:"injection_cases"`
	}{
		Out:                   filepath.ToSlash(*out),
		Deterministic:         true,
		Report1:               filepath.ToSlash(filepath.Join(run1Dir, "report.json")),
		Report2:               filepath.ToSlash(filepath.Join(run2Dir, "report.json")),
		SnapshotVersion:       reports[0].Snapshot.Version,
		FindingCodes:          uniqueFindingCodes(reports[0]),
		QueryTxID:             "0x00003003",
		Replay:                filepath.ToSlash(filepath.Join(run1Dir, "replay.json")),
		V1ReplayCode:          workbench.CodeSnapshotVersion,
		InjectionVerification: verifyResult.AllPassed,
		InjectionCases:        verifyResult.Cases,
	}
	return writePretty(manifest)
}

func uniqueFindingCodes(report workbench.Report) []string {
	seen := make(map[string]struct{})
	for _, finding := range report.Findings {
		seen[finding.Code] = struct{}{}
	}
	codes := make([]string, 0, len(seen))
	for code := range seen {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	return codes
}

func writePretty(value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))
	return nil
}

func writeError(err error) {
	payload := struct {
		Error any `json:"error"`
	}{Error: err.Error()}
	var businessErr *workbench.BusinessError
	if errors.As(err, &businessErr) {
		payload.Error = businessErr
	}
	encoded, _ := json.Marshal(payload)
	fmt.Println(string(encoded))
}

func printUsage() {
	fmt.Println(`dnswatch - deterministic DNS cache-poisoning detection workbench

Subcommands:
  generate           generate deterministic clean and injected samples
  scan               parse three formats, correlate, and commit a snapshot
  query              query a transaction by txid in a snapshot
  replay             replay a snapshot as JSON
  switch-version     check snapshot compatibility for parser v1/v2
  snapshots          list the verified snapshot chain
  verify-injections  execute five external anomaly-injection assertions
  demo               repeatable end-to-end demonstration and determinism check`)
}
