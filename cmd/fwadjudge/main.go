// fwadjudge is the rule-set consistency adjudication engine CLI.
//
// Usage:
//
//	fwadjudge gen -dir <samples-dir>
//	fwadjudge run -db <sqlite-path> -samples <dir> -report <out.json>
//	     [-scenario all|normal|diverge|conflict|bad-cidr|port-invert|overflow|version-gap|replace]
//	     [-max-concurrency N] [-rule-budget N] [-wait-threshold 2s]
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"fwadjudge/internal/app"
	"fwadjudge/internal/samples"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "gen":
		fs := flag.NewFlagSet("gen", flag.ExitOnError)
		dir := fs.String("dir", "samples", "output directory for deterministic samples")
		fs.Parse(os.Args[2:])
		if err := samples.Generate(*dir); err != nil {
			fmt.Fprintln(os.Stderr, "gen:", err)
			os.Exit(1)
		}
		fmt.Println("SAMPLES", *dir)
	case "run":
		fs := flag.NewFlagSet("run", flag.ExitOnError)
		db := fs.String("db", "fwadjudge.db", "SQLite database path")
		dir := fs.String("samples", "samples", "samples directory")
		rep := fs.String("report", "", "report output path (written only on normal exit)")
		scenario := fs.String("scenario", "all", "scenario to run")
		maxConc := fs.Int("max-concurrency", 4, "max concurrent compare sessions")
		budget := fs.Int("rule-budget", 1000, "max rules per snapshot session")
		wait := fs.Duration("wait-threshold", 2*time.Second, "max queue wait before E_COMPARE_CONTENTION")
		fs.Parse(os.Args[2:])
		out, err := app.Run(app.Options{
			DBPath:        *db,
			SamplesDir:    *dir,
			ReportPath:    *rep,
			Scenario:      *scenario,
			MaxConcurrent: *maxConc,
			RuleBudget:    *budget,
			WaitThreshold: *wait,
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, "run:", err)
			os.Exit(1)
		}
		for _, name := range scenarioOrder(out) {
			for _, c := range out.Scenario[name] {
				fmt.Printf("SCENARIO %s CODE %s\n", name, c)
			}
		}
		if *rep != "" {
			fmt.Println("REPORT", *rep)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func scenarioOrder(out *app.Outcome) []string {
	names := make([]string, 0, len(out.Scenario))
	for n := range out.Scenario {
		names = append(names, n)
	}
	for i := 0; i < len(names); i++ {
		for j := i + 1; j < len(names); j++ {
			if names[j] < names[i] {
				names[i], names[j] = names[j], names[i]
			}
		}
	}
	return names
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: fwadjudge gen -dir <dir> | fwadjudge run -db <db> -samples <dir> [-report <out>] [-scenario <name>]")
}
