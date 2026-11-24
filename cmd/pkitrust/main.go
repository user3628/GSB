package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"pkitrust/truststore"
)

const usageText = `pkitrust - deterministic local PKI trust-management example

Usage:
  pkitrust run [-scenario name] [-workdir path] [-out file]
  pkitrust recovery-child -db file -mode uncommitted|committed -marker file

Scenarios:
  all, baseline, missing-ca, revoke-intermediate, revoke-root,
  cross-replace, whole-replace, version-gap, contention, recovery
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usageText)
		os.Exit(2)
	}
	switch os.Args[1] {
	case "run":
		os.Exit(runCommand(os.Args[2:]))
	case "recovery-child":
		os.Exit(recoveryChildCommand(os.Args[2:]))
	case "help", "-h", "--help":
		fmt.Print(usageText)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", os.Args[1], usageText)
		os.Exit(2)
	}
}

func runCommand(args []string) int {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	scenario := flags.String("scenario", "all", "scenario name or all")
	workdir := flags.String("workdir", filepath.Join(".", "_pkitrust_run"), "work directory without spaces or non-ASCII characters")
	out := flags.String("out", "", "report path for one scenario, or summary path for all")
	maxInflight := flags.Int("max-inflight", 2, "maximum in-flight trust transactions")
	waitMillis := flags.Int("wait-ms", 500, "default concurrency wait threshold in milliseconds")
	holdMillis := flags.Int("hold-ms", 400, "diagnostic transaction hold in milliseconds")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(os.Stderr, "unexpected arguments: %v\n", flags.Args())
		return 2
	}
	root, err := filepath.Abs(*workdir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve workdir: %v\n", err)
		return 1
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "create workdir: %v\n", err)
		return 1
	}
	options := scenarioOptions{
		Root:        root,
		MaxInflight: *maxInflight,
		WaitTimeout: time.Duration(*waitMillis) * time.Millisecond,
		Hold:        time.Duration(*holdMillis) * time.Millisecond,
	}
	names, err := scenarioNames(*scenario)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 2
	}
	all := *scenario == "all"
	if !all {
		options.Out = *out
	}
	outcomes := make([]scenarioOutcome, 0, len(names))
	for _, name := range names {
		dir := filepath.Join(root, sanitizeName(name))
		if err := resetDirectory(dir); err != nil {
			fmt.Fprintf(os.Stderr, "prepare %s: %v\n", name, err)
			return 1
		}
		outcome := runScenario(context.Background(), name, dir, options)
		outcomes = append(outcomes, outcome)
		status := "PASS"
		if !outcome.Passed {
			status = "FAIL"
		}
		fmt.Printf("[%s] %-20s code=%-20s version=%d report=%s\n", status, outcome.Name, outcome.Code, outcome.Version, outcome.ReportPath)
		for _, detail := range outcome.Details {
			fmt.Printf("       %s\n", detail)
		}
	}
	summary := runSummary{
		SchemaVersion: "1",
		Scenarios:     outcomes,
		Passed:        true,
	}
	for _, outcome := range outcomes {
		if !outcome.Passed {
			summary.Passed = false
		}
	}
	summaryPath := *out
	if summaryPath == "" {
		summaryPath = filepath.Join(root, "summary.json")
	} else {
		if !all {
			summaryPath = outcomes[0].ReportPath
		}
	}
	if all {
		if err := writeJSON(summaryPath, summary); err != nil {
			fmt.Fprintf(os.Stderr, "write summary: %v\n", err)
			return 1
		}
		fmt.Printf("summary=%s passed=%t\n", summaryPath, summary.Passed)
	}
	if !summary.Passed {
		return 1
	}
	return 0
}

func recoveryChildCommand(args []string) int {
	flags := flag.NewFlagSet("recovery-child", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	dbPath := flags.String("db", "", "SQLite database path")
	namespace := flags.String("namespace", "recovery", "namespace")
	mode := flags.String("mode", "uncommitted", "uncommitted or committed")
	marker := flags.String("marker", "", "marker path")
	hold := flags.Duration("hold", 30*time.Second, "hold duration after the diagnostic point")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if strings.TrimSpace(*dbPath) == "" || strings.TrimSpace(*marker) == "" {
		fmt.Fprintln(os.Stderr, "-db and -marker are required")
		return 2
	}
	if *mode != "uncommitted" && *mode != "committed" {
		fmt.Fprintln(os.Stderr, "-mode must be uncommitted or committed")
		return 2
	}
	store, err := truststore.Open(context.Background(), truststore.StoreOptions{DBPath: *dbPath, WaitTimeout: 5 * time.Second})
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		return 1
	}
	defer store.Close()
	diagnostics := &truststore.ApplyDiagnostics{}
	if *mode == "uncommitted" {
		diagnostics.MarkerBeforeCommit = *marker
		diagnostics.HoldBeforeCommit = *hold
	} else {
		diagnostics.MarkerAfterCommit = *marker
		diagnostics.HoldAfterCommit = *hold
	}
	_, err = store.Apply(context.Background(), *namespace, truststore.Changeset{Operations: []truststore.Operation{{Type: truststore.OperationAddRoot, CertID: "root-gamma"}}}, truststore.ApplyOptions{RequestID: "recovery-child", Diagnostics: diagnostics})
	if err != nil {
		fmt.Fprintf(os.Stderr, "apply: %v\n", err)
		return 1
	}
	return 0
}
