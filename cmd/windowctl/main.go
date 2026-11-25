package main

import (
	"flag"
	"fmt"
	"os"

	"local/windowengine/internal/domain"
	"local/windowengine/internal/sample"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: windowctl <generate|validate> [flags]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "generate":
		flags := flag.NewFlagSet("generate", flag.ExitOnError)
		seed := flags.Int64("seed", 242, "deterministic generator seed")
		output := flags.String("out", "samples/deterministic.json", "output JSON path")
		flags.Parse(os.Args[2:])
		data, err := sample.Generate(*seed)
		if err != nil {
			fatal(err)
		}
		if err := os.WriteFile(*output, append(data, '\n'), 0o644); err != nil {
			fatal(err)
		}
		snapshot, business := domain.ParseSnapshot(data)
		if business != nil {
			fatal(business)
		}
		windows, planErr := domain.PlanWindows(snapshot)
		if planErr != nil {
			fatal(planErr)
		}
		fmt.Printf("wrote %s: tenants=%d specs=%d events=%d planned_windows=%d\n",
			*output, len(snapshot.Tenants), len(snapshot.WindowSpecs), len(snapshot.Events), len(windows))
	case "validate":
		flags := flag.NewFlagSet("validate", flag.ExitOnError)
		input := flags.String("file", "samples/deterministic.json", "snapshot JSON path")
		flags.Parse(os.Args[2:])
		data, err := os.ReadFile(*input)
		if err != nil {
			fatal(err)
		}
		snapshot, business := domain.ParseSnapshot(data)
		if business != nil {
			fatal(business)
		}
		windows, planErr := domain.PlanWindows(snapshot)
		if planErr != nil {
			fatal(planErr)
		}
		fmt.Printf("valid %s: hash=%s events=%d windows=%d\n",
			snapshot.SnapshotID, snapshot.ContentHash, len(snapshot.Events), len(windows))
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		os.Exit(2)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
