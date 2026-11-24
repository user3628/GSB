// Command casengine is the interactive demo entry point for the CAS one-time
// ticket validation engine.
package main

import (
	"os"

	"casengine/internal/cli"
)

func main() {
	args := os.Args[1:]
	if len(args) > 0 && (args[0] == cli.CrashChildPrepare || args[0] == cli.CrashChildKill) {
		os.Exit(cli.RunCrashChild(args[0], args[1:]))
	}
	os.Exit(cli.Run(args))
}
