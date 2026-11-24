package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"mitmpipeline/internal/codec"
	"mitmpipeline/internal/domain"
	"mitmpipeline/internal/httpapi"
	"mitmpipeline/internal/sample"
	"mitmpipeline/internal/service"
	"mitmpipeline/internal/store"
	"mitmpipeline/internal/verify"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(os.Args[2:])
	case "verify":
		err = runVerify(os.Args[2:])
	case "crash-import":
		err = crashImport(os.Args[2:])
	case "recover":
		err = recoverCmd(os.Args[2:])
	case "gen-samples":
		err = genSamples(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		var de *domain.Error
		if errors.As(err, &de) {
			data, _ := json.MarshalIndent(map[string]any{"ok": false, "error": de}, "", "  ")
			fmt.Fprintln(os.Stderr, string(data))
		} else {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
		}
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `pipeline commands:
  serve [--addr 127.0.0.1:18080] [--db pipeline.db]
  verify [--addr 127.0.0.1:0] [--db-dir DIR] [--report artifacts/verification-report.json] [--json]
  crash-import --db pipeline.db
  recover --db pipeline.db
  gen-samples --dir artifacts/samples`)
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:18080", "listen address")
	dbPath := fs.String("db", "artifacts/pipeline.db", "SQLite database path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	st, err := store.Open(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	if report, err := st.Recover(ctx); err == nil {
		fmt.Printf("recovery scan complete: last_complete_commit=%d aborted_intents=%d\n", report.LastCompleteCommit, len(report.AbortedIntents))
	}
	svc := service.New(st)
	server := &http.Server{Addr: *addr, Handler: httpapi.New(svc).Handler(), ReadHeaderTimeout: 10 * time.Second}
	fmt.Printf("MTLP pipeline listening on http://%s (db=%s)\n", *addr, *dbPath)
	return server.ListenAndServe()
}

func runVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:0", "loopback listen address; :0 selects a free port")
	dbDir := fs.String("db-dir", "", "directory for deterministic verification database and samples")
	reportPath := fs.String("report", "artifacts/verification-report.json", "full JSON report output path")
	asJSON := fs.Bool("json", false, "print full JSON report to stdout")
	cliPath := fs.String("cli-path", "", "CLI binary used for SIGKILL subprocess; defaults to current executable")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cli := *cliPath
	if cli == "" {
		executable, err := os.Executable()
		if err != nil {
			return err
		}
		cli = executable
	}
	report, err := verify.Run(verify.Config{Addr: *addr, DBDir: *dbDir, CLIPath: cli, ReportPath: *reportPath})
	if report != nil && *asJSON {
		data, _ := json.MarshalIndent(report, "", "  ")
		fmt.Println(string(data))
	} else if report != nil {
		printVerifySummary(report)
	}
	if err != nil {
		return err
	}
	if report != nil && !report.OK {
		return fmt.Errorf("verification completed with failed checks; see %s", *reportPath)
	}
	if report != nil && *reportPath != "" && !*asJSON {
		fmt.Printf("\nFull JSON report: %s\n", *reportPath)
	}
	return nil
}

func crashImport(args []string) error {
	fs := flag.NewFlagSet("crash-import", flag.ContinueOnError)
	dbPath := fs.String("db", "", "SQLite database path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" {
		return fmt.Errorf("--db is required")
	}
	ctx := context.Background()
	st, err := store.Open(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	svc := service.New(st)
	fx := sample.NewFixture()
	_, err = svc.ImportRecords(ctx, []domain.Record{fx.Accepted}, service.ImportOptions{
		BatchID: "baseline-complete-batch", RoundingRule: "truncate",
	}, domain.Negotiation{Format: domain.FormatBinary, RequestedVersion: 2, EffectiveVersion: 2, Action: domain.ActionAccept}, domain.FormatBinary)
	if err != nil {
		return err
	}
	fmt.Println("baseline commit durable; writing crash intent")
	st.EnableCrashAfterIntent()
	_, err = svc.ImportRecords(ctx, []domain.Record{fx.CrashCandidate}, service.ImportOptions{
		BatchID: "force-killed-batch", RoundingRule: "truncate",
	}, domain.Negotiation{Format: domain.FormatBinary, RequestedVersion: 2, EffectiveVersion: 2, Action: domain.ActionAccept}, domain.FormatBinary)
	if err != nil {
		return err
	}
	return fmt.Errorf("crash point did not terminate process")
}

func recoverCmd(args []string) error {
	fs := flag.NewFlagSet("recover", flag.ContinueOnError)
	dbPath := fs.String("db", "", "SQLite database path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" {
		return fmt.Errorf("--db is required")
	}
	ctx := context.Background()
	st, err := store.Open(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	report, err := st.Recover(ctx)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func genSamples(args []string) error {
	fs := flag.NewFlagSet("gen-samples", flag.ContinueOnError)
	dir := fs.String("dir", "artifacts/samples", "output directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		return err
	}
	fx := sample.NewFixture()
	acceptedBin, err := codec.EncodeBinary([]domain.Record{fx.Accepted, fx.HotUpdate, fx.MITM, fx.Aborted}, 2)
	if err != nil {
		return err
	}
	jsonl, err := codec.EncodeJSONL([]domain.Record{fx.JSONAccepted, fx.Degraded})
	if err != nil {
		return err
	}
	legacyBin, err := codec.EncodeBinary([]domain.Record{fx.Degraded}, 1)
	if err != nil {
		return err
	}
	files := map[string][]byte{
		"current.bin":           acceptedBin,
		"current.jsonl":         jsonl,
		"legacy-v1.bin":         legacyBin,
		"unsupported-v99.jsonl": fx.UnsupportedJSON,
		"corrupt.bin":           fx.CorruptBinary,
		"good-chain.pem":        []byte(fx.Good.ChainPEM),
		"attacker-chain.pem":    []byte(fx.Attacker.ChainPEM),
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(*dir, name), data, 0o644); err != nil {
			return err
		}
	}
	fmt.Printf("wrote deterministic samples to %s\n", *dir)
	return nil
}

func printVerifySummary(report *verify.Report) {
	status := "PASS"
	if !report.OK {
		status = "FAIL"
	}
	fmt.Printf("MTLP pipeline verification: %s\n", status)
	fmt.Printf("workdir: %s\nhttp: %s\ndb: %s\n", report.WorkDir, report.HTTPBaseURL, report.DatabasePath)
	fmt.Println("\nScenarios:")
	for _, scenario := range report.Scenarios {
		mark := "✓"
		if !scenario.OK {
			mark = "✗"
		}
		line := fmt.Sprintf("  %s %-42s http=%d expected=%s", mark, scenario.Name, scenario.HTTPStatus, scenario.Expected)
		if scenario.Conclusion != "" {
			line += " got=" + scenario.Conclusion
		}
		if scenario.Code != "" {
			line += " code=" + scenario.Code
		}
		fmt.Println(line)
	}
	fmt.Println("\nChecks:")
	for _, check := range report.Checks {
		mark := "✓"
		if !check.OK {
			mark = "✗"
		}
		fmt.Printf("  %s %s — %s\n", mark, check.Name, check.Detail)
	}
	if report.Recovery.Report != nil {
		fmt.Printf("\nRecovery: pending_before=%d aborted=%d last_complete_commit=%d visible=%d\n",
			len(report.Recovery.PendingBefore), len(report.Recovery.Report.AbortedIntents),
			report.Recovery.LastCompleteCommit, len(report.Recovery.VisibleAfter))
		for _, fact := range report.Recovery.Report.Facts {
			fmt.Printf("  fact: %s\n", fact)
		}
	}
	fmt.Printf("Degradations: %d; rounding detail connections: %d\n", len(report.DegradationDetails), len(report.RoundingDetails))
	for _, degradation := range report.DegradationDetails {
		record := "-"
		if degradation.RecordIndex > 0 {
			record = fmt.Sprintf("%d", degradation.RecordIndex)
		}
		fmt.Printf("  degrade rec=%s conn=%s field=%s rule=%s filled=%s\n",
			record, degradation.ConnectionID, degradation.Field, degradation.Rule, degradation.Filled)
	}
	if rounding, ok := report.RoundingDetails["HOT-UPDATE-0001"]; ok {
		fmt.Println("Rounding HOT-UPDATE-0001:")
		for _, item := range rounding {
			fmt.Printf("  seq=%d raw_ns=%d ms=%d remainder_ns=%d bit=%d delta_ns=%d cumulative_ns=%d\n",
				item.EventSeq, item.RawUnixNS, item.TimestampMS, item.RemainderNS,
				item.RoundingBit, item.AppliedDeltaNS, item.CumulativeRoundingErrorNS)
		}
	}
}
