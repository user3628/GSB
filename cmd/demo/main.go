package main

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "server" {
		if err := runServerSubcommand(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	report, runErr := runDemo()
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if report != nil {
		_ = encoder.Encode(report)
	}
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "DEMO_FAILED: %v\n", runErr)
		os.Exit(1)
	}
	if report == nil || !report.OK {
		fmt.Fprintln(os.Stderr, "DEMO_FAILED: one or more checks failed")
		os.Exit(1)
	}
	fmt.Println("DEMO_OK: all local MVCC scenarios passed")
}

func runtimeVersion() string { return runtime.Version() }
