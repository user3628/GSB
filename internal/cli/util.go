package cli

import (
	"context"
	"flag"
	"time"
)

func nilBg() context.Context { return context.Background() }

func newFlagSet(name string, workDir *string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.StringVar(workDir, "workdir", *workDir, "working directory")
	return fs
}

func sleep10ms() { time.Sleep(10 * time.Millisecond) }
