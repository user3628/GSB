// Command injectctl runs the local injection detection and context-encoding
// adjudication engine.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"injectctl/internal/app"
	"injectctl/internal/report"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = cmdRun(os.Args[2:])
	case "selfcheck":
		err = cmdSelfCheck(os.Args[2:])
	case "recover":
		err = cmdRecover(os.Args[2:])
	case "crash":
		err = cmdCrash(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: injectctl <command> [flags]
  run       -workdir DIR [-concurrency N] [-queue N] [-maxsamples N] [-maxencoded N]
  selfcheck -workdir DIR
  recover   -workdir DIR
  crash     -workdir DIR   (hard-exits after the first snapshot commit)`)
}

func flags(args []string) map[string]string {
	out := map[string]string{}
	for i := 0; i < len(args); i++ {
		if a := args[i]; len(a) > 1 && a[0] == '-' {
			if i+1 < len(args) && (len(args[i+1]) == 0 || args[i+1][0] != '-') {
				out[a] = args[i+1]
				i++
			} else {
				out[a] = ""
			}
		}
	}
	return out
}

func atoi(s string, def int) int {
	if s == "" {
		return def
	}
	var v int
	if _, err := fmt.Sscanf(s, "%d", &v); err != nil {
		return def
	}
	return v
}

func cmdRun(args []string) error {
	f := flags(args)
	workDir := f["-workdir"]
	if workDir == "" {
		workDir = filepath.Join(os.TempDir(), "injectctl-work")
	}
	cfg := app.RunConfig{
		WorkDir:       workDir,
		MaxConcurrent: atoi(f["-concurrency"], 2),
		MaxQueueDepth: atoi(f["-queue"], 16),
		MaxSamples:    atoi(f["-maxsamples"], 0),
		MaxEncoded:    atoi(f["-maxencoded"], 0),
	}
	res, err := app.Run(cfg)
	if err != nil {
		return err
	}
	return writeReport(workDir, res)
}

func cmdSelfCheck(args []string) error {
	f := flags(args)
	workDir := f["-workdir"]
	if workDir == "" {
		workDir = filepath.Join(os.TempDir(), "injectctl-selfcheck")
	}
	rep, err := app.RunSelfCheck(workDir)
	if err != nil {
		return err
	}
	raw, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(raw))
	if !rep.AllPassed {
		os.Exit(3)
	}
	return nil
}

func cmdRecover(args []string) error {
	f := flags(args)
	workDir := f["-workdir"]
	if workDir == "" {
		return fmt.Errorf("-workdir is required")
	}
	res, err := app.Run(app.RunConfig{WorkDir: workDir, MaxConcurrent: 2, MaxQueueDepth: 16, RecoveryMode: true})
	if err != nil {
		return err
	}
	return writeReport(workDir, res)
}

func cmdCrash(args []string) error {
	f := flags(args)
	workDir := f["-workdir"]
	if workDir == "" {
		return fmt.Errorf("-workdir is required")
	}
	_, err := app.Run(app.RunConfig{
		WorkDir: workDir, MaxConcurrent: 1, MaxQueueDepth: 16,
		CrashAfterSnap: true,
	})
	return err
}

func writeReport(workDir string, res app.PipelineResult) error {
	doc := report.Build(res.ContextVer, res.Results, res.ParseErrors, res.Chain)
	raw, err := report.Render(doc)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(workDir, "report.json")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		return err
	}
	fmt.Println(path)
	return nil
}
