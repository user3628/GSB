package main

import (
	"fmt"
	"os"
)

// crashExit simulates a hard process kill between write and commit.
// It is a variable so tests can intercept it.
var crashExit = func() { os.Exit(2) }

func usage() {
	fmt.Fprintln(os.Stderr, `bire - backup integrity & tamper-evident recovery engine

Usage:
  bire generate --samples <dir> [--config <plan.json>]
  bire inspect --repo <name> --samples <dir> --state <dir> [--budget <n>] [--crash-before-commit]
  bire batch --samples <dir> --state <dir> [--concurrency <n>] [--budget <n>] [--report <file>] [--inject-snapshot-gap <repo>]
  bire replay --repo <name> --state <dir> --version <n>
  bire restore --repo <name> --state <dir>
  bire report --state <dir> [--out <file>]
  bire stats --state <dir>`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "generate":
		err = cmdGenerate(os.Args[2:])
	case "inspect":
		err = cmdInspect(os.Args[2:])
	case "batch":
		err = cmdBatch(os.Args[2:])
	case "replay":
		err = cmdReplay(os.Args[2:])
	case "restore":
		err = cmdRestore(os.Args[2:])
	case "report":
		err = cmdReport(os.Args[2:])
	case "stats":
		err = cmdStats(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
