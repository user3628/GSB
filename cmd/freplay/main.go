package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"freplay/internal/engine"
)

func main() {
	var (
		seed         = flag.Int64("seed", 37, "deterministic sample seed")
		numWindows   = flag.Int("windows", 12, "number of base sample windows")
		concurrency  = flag.Int("concurrency", 4, "scheduler concurrency limit")
		globalMem    = flag.Int64("global-mem", 2600, "global memory budget")
		globalCPU    = flag.Int64("global-cpu", 1000, "global correlation cpu budget")
		windowCPUMax = flag.Int64("window-cpu-max", 100, "per-window cpu cap")
		dbPath       = flag.String("db", "freplay.db", "sqlite database path")
		outPath      = flag.String("out", "", "report output path (default: stdout)")
		inject       = flag.String("inject", "", "comma separated injections: dup-event-id, orphan-ref, tiny-window-budget, global-budget-shortage, conflict-merge, snapshot-version-skip")
		crashAfter   = flag.Int("crash-after", -1, "commit N window verdicts then simulate a hard kill (no snapshot commit)")
	)
	flag.Parse()

	var injections []string
	gap := false
	if *inject != "" {
		for _, name := range strings.Split(*inject, ",") {
			name = strings.TrimSpace(name)
			if name == engine.InjectSnapshotVersionSkip {
				gap = true
				continue
			}
			injections = append(injections, name)
		}
	}

	outcome, err := engine.Run(engine.Options{
		Seed:        *seed,
		NumWindows:  *numWindows,
		Concurrency: *concurrency,
		Budget: engine.BudgetParams{
			GlobalMemory: *globalMem,
			GlobalCPU:    *globalCPU,
			WindowCPUMax: *windowCPUMax,
		},
		Inject:            injections,
		DBPath:            *dbPath,
		CrashAfter:        *crashAfter,
		InjectSnapshotGap: gap,
	})
	if errors.Is(err, engine.ErrSimulatedCrash) {
		fmt.Fprintln(os.Stderr, "process killed (simulated) before snapshot commit; committed verdicts persist in", *dbPath)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "engine error:", err)
		os.Exit(1)
	}

	if *outPath == "" {
		os.Stdout.Write(outcome.ReportJSON)
		return
	}
	if err := os.WriteFile(*outPath, outcome.ReportJSON, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "write report:", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "report written to %s (snapshot v%d, %d windows)\n",
		*outPath, outcome.Snapshot.Version, len(outcome.Report.Windows))
}
