// flowtrace is the sensitive data-flow tracing pipeline CLI.
//
// Usage:
//
//	flowtrace gen  -dir samples
//	flowtrace run  -dir samples -db trace.db -report report.json [flags]
//	flowtrace verify -report report.json
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"flowtrace/internal/pipeline"
	"flowtrace/internal/report"
	"flowtrace/internal/samplegen"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(64)
	}
	var err error
	switch os.Args[1] {
	case "gen":
		err = cmdGen(os.Args[2:])
	case "run":
		err = cmdRun(os.Args[2:])
	case "verify":
		err = cmdVerify(os.Args[2:])
	default:
		usage()
		os.Exit(64)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  flowtrace gen    -dir samples
  flowtrace run    -dir samples -db trace.db -report report.json
                   [-inject reorder,cycle,escape,snapshot-gap,contention]
                   [-max-concurrent N] [-max-events N] [-mem-budget N]
                   [-crash-after N] [-resume]
  flowtrace verify -report report.json`)
}

func cmdGen(args []string) error {
	fs := flag.NewFlagSet("gen", flag.ContinueOnError)
	dir := fs.String("dir", "samples", "output sample directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := samplegen.WriteDir(*dir); err != nil {
		return err
	}
	fmt.Println("samples written to", *dir)
	return nil
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	dir := fs.String("dir", "samples", "sample directory")
	db := fs.String("db", "trace.db", "SQLite snapshot database")
	reportPath := fs.String("report", "report.json", "output report path")
	inject := fs.String("inject", "", "comma-separated anomaly injections")
	maxConcurrent := fs.Int("max-concurrent", 4, "max concurrent windows")
	maxEvents := fs.Int("max-events", 0, "per-window event budget (0=unlimited)")
	memBudget := fs.Int64("mem-budget", 0, "per-session memory budget bytes (0=unlimited)")
	crashAfter := fs.Int("crash-after", 0, "commit N windows then simulate kill")
	resume := fs.Bool("resume", false, "recover committed windows from the DB")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var flags pipeline.InjectFlags
	for _, name := range strings.Split(*inject, ",") {
		switch strings.TrimSpace(name) {
		case "":
		case "reorder":
			flags.Reorder = true
		case "cycle":
			flags.Cycle = true
		case "escape":
			flags.Escape = true
		case "snapshot-gap":
			flags.SnapshotGap = true
		case "contention":
			flags.Contention = true
		default:
			return fmt.Errorf("unknown injection %q", name)
		}
	}

	out, err := pipeline.Run(pipeline.Options{
		Dir: *dir, DBPath: *db, Inject: flags,
		MaxConcurrent: *maxConcurrent, MaxEventsPerWindow: *maxEvents,
		SessionMemoryBudget: *memBudget, CrashAfter: *crashAfter, Resume: *resume,
	})
	if err != nil {
		return err
	}
	if out.Crashed {
		fmt.Println("simulated kill: process terminated before remaining windows committed")
		os.Exit(2)
	}
	if err := os.WriteFile(*reportPath, report.Marshal(out.Report), 0o644); err != nil {
		return err
	}
	fmt.Println("report written to", *reportPath, "| overall_consistent =", out.Report.OverallConsistent)
	return nil
}

func cmdVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	reportPath := fs.String("report", "report.json", "report to verify")
	if err := fs.Parse(args); err != nil {
		return err
	}
	raw, err := os.ReadFile(*reportPath)
	if err != nil {
		return err
	}
	var rep report.Report
	if err := jsonUnmarshal(raw, &rep); err != nil {
		return err
	}
	escapes := 0
	for _, w := range rep.Windows {
		for _, j := range w.Judgments {
			if j.Code == "E_FLOW_ESCAPED" {
				escapes++
			}
		}
	}
	fmt.Printf("windows=%d escapes=%d overall_consistent=%v\n", len(rep.Windows), escapes, rep.OverallConsistent)
	if !rep.OverallConsistent {
		return fmt.Errorf("consistency check failed")
	}
	return nil
}
