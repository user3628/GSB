// fwbench is the local forensic workbench command line. It runs the
// deterministic multi-format demo, inspects reports and validates snapshot
// chains. All I/O stays inside the chosen working directory; no containers or
// network targets are used.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"forensicworkbench/internal/pipeline"
	"forensicworkbench/internal/report"
	"forensicworkbench/internal/snapshot"
	"forensicworkbench/internal/view"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "demo":
		runDemo(os.Args[2:])
	case "view":
		runView(os.Args[2:])
	case "snapshot":
		runSnapshot(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `fwbench - local audit correlation & forensics workbench

usage:
  fwbench demo [-workdir DIR] [-concurrency N] [-budget N] [-version v1|v2|all] [-strategy continue|abort]
  fwbench view -report FILE [-session ID] [-event ID]
  fwbench snapshot -db FILE`)
}

func runDemo(args []string) {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)
	workdir := fs.String("workdir", filepath.Join(".", "fwbench-run"), "working directory for samples, sqlite and report")
	concurrency := fs.Int("concurrency", 3, "max concurrent parser sessions")
	budget := fs.Int64("budget", 0, "per-session event budget (0 = unlimited)")
	version := fs.String("version", "all", "binary format version: v1, v2 or all")
	strategy := fs.String("strategy", "continue", "malformed handling: continue or abort")
	_ = fs.Parse(args)

	var st report.Strategy
	switch *strategy {
	case "continue":
		st = report.StrategyContinue
	case "abort":
		st = report.StrategyAbort
	default:
		fatal("invalid strategy %q (want continue|abort)", *strategy)
	}
	abs, err := filepath.Abs(*workdir)
	if err != nil {
		fatal("%v", err)
	}
	res, err := pipeline.Run(pipeline.Options{
		WorkDir: abs, Concurrency: *concurrency, EventBudget: *budget,
		Strategy: st, TargetVersion: normalizeVersion(*version),
	})
	if err != nil {
		fatal("pipeline failed: %v", err)
	}
	reportPath := filepath.Join(abs, "report.json")
	if err := os.WriteFile(reportPath, res.ReportBytes, 0o644); err != nil {
		fatal("write report: %v", err)
	}
	fmt.Printf("snapshot version : %d\n", res.Snapshot.Version)
	fmt.Printf("snapshot hash    : %s\n", res.Snapshot.ContentHash)
	fmt.Printf("report           : %s (%d bytes)\n", reportPath, len(res.ReportBytes))
	fmt.Printf("sources          : %d\n", len(res.Outcome.Results))
	fmt.Printf("timeline events  : %d (normal %d)\n", len(res.Report.Timeline), res.Report.NormalEventCount)
	fmt.Printf("lineage edges    : %d\n", len(res.Report.LineageEdges))
	fmt.Printf("error records    : %d\n", len(res.Report.Errors))
	fmt.Printf("strategy         : %s\n", st)
}

func runView(args []string) {
	fs := flag.NewFlagSet("view", flag.ExitOnError)
	reportPath := fs.String("report", "", "path to report.json (required)")
	session := fs.String("session", "", "filter by session id")
	event := fs.String("event", "", "show multi-format sources for an event id")
	_ = fs.Parse(args)
	if *reportPath == "" {
		fatal("view requires -report")
	}
	raw, err := os.ReadFile(*reportPath)
	if err != nil {
		fatal("%v", err)
	}
	r, err := report.Decode(raw)
	if err != nil {
		fatal("decode report: %v", err)
	}
	switch {
	case *event != "":
		m, err := view.MultiFormatSources(r, *event)
		if err != nil {
			fatal("%v", err)
		}
		fmt.Printf("correlation key %s (%d sources):\n", m.Key, len(m.Sources))
		for _, ev := range m.Sources {
			fmt.Printf("  %-6s %-10s off=%-4d subj=%s obj=%s\n", ev.Source, ev.FormatVersion, ev.Offset, ev.Subject, ev.Object)
		}
	case *session != "":
		for _, ev := range view.FilterBySession(r, *session) {
			t := "nil"
			if ev.TimeUnixNano != nil {
				t = fmt.Sprintf("%d", *ev.TimeUnixNano)
			}
			fmt.Printf("%s t=%s %s pid=%d %s off=%d [%s]\n", ev.EventID, t, ev.Type, ev.PID, ev.Source, ev.Offset, ev.Code)
		}
	default:
		if err := view.RunREPL(r, os.Stdin, os.Stdout); err != nil {
			fatal("%v", err)
		}
	}
}

func runSnapshot(args []string) {
	fs := flag.NewFlagSet("snapshot", flag.ExitOnError)
	db := fs.String("db", "", "path to forensic.sqlite (required)")
	_ = fs.Parse(args)
	if *db == "" {
		fatal("snapshot requires -db")
	}
	store, err := snapshot.Open(*db)
	if err != nil {
		fatal("%v", err)
	}
	defer store.Close()
	chain, err := store.LoadChain()
	if err != nil {
		fatal("snapshot load rejected: %v", err)
	}
	for _, sn := range chain {
		fmt.Printf("v%d hash=%s events=%d formats=%v\n", sn.Version, sn.ContentHash, sn.EventCount, sn.FormatVersions)
	}
}

func normalizeVersion(v string) string {
	switch v {
	case "v1", "v2":
		return v
	default:
		return ""
	}
}

func fatal(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "fwbench: "+format+"\n", a...)
	os.Exit(1)
}
