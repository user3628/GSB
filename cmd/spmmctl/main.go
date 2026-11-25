package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"spmmopt/spmm"
)

const usageText = `spmmctl - deterministic CSR SpMM operator and diagnostic lab

Usage:
  spmmctl generate       Generate the fixed-seed local sample corpus
  spmmctl plan           Select and publish a versioned block plan
  spmmctl run            Execute settlement with stop/crash injection support
  spmmctl query          Execute the interactive path
  spmmctl verify         Verify dual-path agreement over the local corpus
  spmmctl reconcile      Inspect plan rollback/replacement and invalidate caches
  spmmctl serve          Start the interactive diagnostic HTTP application
  spmmctl demo           Run the complete acceptance demonstration
  spmmctl publish-once   Publish one plan version (used by concurrency demo)

Run "spmmctl <command> -h" for command flags.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usageText)
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "generate":
		err = cmdGenerate(os.Args[2:])
	case "plan":
		err = cmdPlan(os.Args[2:])
	case "run":
		err = cmdRun(os.Args[2:])
	case "query":
		err = cmdQuery(os.Args[2:])
	case "verify":
		err = cmdVerify(os.Args[2:])
	case "reconcile":
		err = cmdReconcile(os.Args[2:])
	case "serve":
		err = cmdServe(os.Args[2:])
	case "demo":
		err = cmdDemo(os.Args[2:])
	case "publish-once":
		err = cmdPublishOnce(os.Args[2:])
	case "help", "-h", "--help":
		fmt.Print(usageText)
		return
	default:
		err = fmt.Errorf("unknown command %q\n\n%s", os.Args[1], usageText)
	}
	if err != nil {
		printError(err)
		os.Exit(1)
	}
}

func printError(err error) {
	if e, ok := spmm.AsError(err); ok {
		b, _ := json.MarshalIndent(e, "", "  ")
		fmt.Fprintln(os.Stderr, string(b))
		return
	}
	fmt.Fprintln(os.Stderr, err)
}

func failUsage(fs *flag.FlagSet, format string, args ...any) error {
	fs.Usage()
	return fmt.Errorf(format, args...)
}

func writeJSON(path string, v any) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func writeBytes(path string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func resetDB(path string) error {
	for _, suffix := range []string{"", "-wal", "-shm", ".lock"} {
		err := os.Remove(path + suffix)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return os.MkdirAll(filepath.Dir(path), 0o755)
}

func isStateMissing(err error) bool {
	e, ok := spmm.AsError(err)
	return ok && e.Code == spmm.CodeState && strings.Contains(e.Message, "not found")
}
