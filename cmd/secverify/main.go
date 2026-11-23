package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "__apply-crash":
			if err := runApplyCrash(os.Args[2:]); err != nil {
				fmt.Fprintln(os.Stderr, "apply-crash:", err)
				os.Exit(2)
			}
			return
		case "demo":
			if err := runDemo(os.Args[2:]); err != nil {
				fmt.Fprintln(os.Stderr, "demo:", err)
				os.Exit(1)
			}
			return
		case "inject":
			if err := runInject(os.Args[2:]); err != nil {
				fmt.Fprintln(os.Stderr, "inject:", err)
				os.Exit(1)
			}
			return
		case "crash-demo":
			if err := runCrashDemo(os.Args[2:]); err != nil {
				fmt.Fprintln(os.Stderr, "crash-demo:", err)
				os.Exit(1)
			}
			return
		case "run":
			if err := runExternal(os.Args[2:]); err != nil {
				fmt.Fprintln(os.Stderr, "run:", err)
				os.Exit(1)
			}
			return
		case "help", "-h", "--help":
			usage()
			return
		}
	}
	usage()
}

func usage() {
	fmt.Fprint(os.Stderr, `secverify - deterministic Windows security configuration verification lab

Commands:
  demo         Generate local samples, apply four rounds, probe, and emit a deterministic report.
  inject       Exercise missing dependency, cycle, probe escape, version gap, and contention.
  crash-demo   Force-kill worker processes at probe and pre-commit points, then verify recovery.
  run          Apply an external baseline JSON and one or more changeset files.

Common flags:
  --workdir DIR        Empty/new working directory (default: fresh OS temp directory)
  --report FILE        Report path, or - for stdout (default: stdout)
  --keep               Keep an automatically-created temporary working directory
  --max-tx N           Maximum in-flight change transactions (default 4)
  --max-probes N       Maximum in-flight probe processes (default 2)
  --wait-timeout DUR   Contention wait threshold (default 2s)
  --probe-timeout DUR  Probe subprocess timeout (default 10s)
  --probe-delay DUR    Optional deterministic delay inside each probe slot

Examples:
  go run ./cmd/secverify demo --report report-a.json
  go run ./cmd/secverify inject --report injections.json
  go run ./cmd/secverify crash-demo --report crash.json
  go run ./cmd/secverify run --baseline baseline.json --changeset round-01.changes --report run.json
`)
}
