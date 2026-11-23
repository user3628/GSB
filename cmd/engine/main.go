package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"iotrust/internal/app"
	"iotrust/internal/samples"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: engine <command> [flags]

commands:
  gen      --dir <dir>                          generate deterministic samples and key material
  run      --scenario <name> [--dir] [--workers] [--report]
  run-all  [--dir] [--workers]                  run every scenario and export reports`)
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "gen":
		fs := flag.NewFlagSet("gen", flag.ExitOnError)
		dir := fs.String("dir", "", "work dir (default: local temp dir)")
		fs.Parse(os.Args[2:])
		d, err := app.GenSamples(*dir)
		if err != nil {
			fmt.Fprintln(os.Stderr, "gen:", err)
			os.Exit(1)
		}
		fmt.Println("samples generated at", d)
	case "run":
		fs := flag.NewFlagSet("run", flag.ExitOnError)
		dir := fs.String("dir", "", "work dir (default: local temp dir)")
		scenario := fs.String("scenario", "normal", "scenario name")
		workers := fs.Int("workers", 0, "runtime worker parallelism")
		report := fs.String("report", "", "report output path")
		fs.Parse(os.Args[2:])
		rep, d, err := app.RunScenario(*dir, *scenario, *workers)
		if err != nil {
			fmt.Fprintln(os.Stderr, "run:", err)
			os.Exit(1)
		}
		out := *report
		if out == "" {
			out = filepath.Join(d, "reports", *scenario+".json")
		}
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			fmt.Fprintln(os.Stderr, "run:", err)
			os.Exit(1)
		}
		if err := os.WriteFile(out, rep.Bytes(), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "run:", err)
			os.Exit(1)
		}
		fmt.Printf("scenario=%s snapshot_version=%d codes=%s report=%s\n",
			*scenario, rep.SnapshotVersion, rep.CodeSummary(), out)
	case "run-all":
		fs := flag.NewFlagSet("run-all", flag.ExitOnError)
		dir := fs.String("dir", "", "work dir (default: local temp dir)")
		workers := fs.Int("workers", 0, "runtime worker parallelism")
		fs.Parse(os.Args[2:])
		d := *dir
		for _, name := range samples.Scenarios {
			rep, used, err := app.RunScenario(d, name, *workers)
			if err != nil {
				fmt.Fprintln(os.Stderr, "run-all:", name, err)
				os.Exit(1)
			}
			d = used
			out := filepath.Join(d, "reports", name+".json")
			if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
				fmt.Fprintln(os.Stderr, "run-all:", err)
				os.Exit(1)
			}
			if err := os.WriteFile(out, rep.Bytes(), 0o644); err != nil {
				fmt.Fprintln(os.Stderr, "run-all:", err)
				os.Exit(1)
			}
			fmt.Printf("scenario=%-18s snapshot_version=%d codes=%s\n", name, rep.SnapshotVersion, rep.CodeSummary())
		}
		fmt.Println("reports at", filepath.Join(d, "reports"))
	default:
		usage()
	}
}
