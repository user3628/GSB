package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"speakerdiar"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		var de *speakerdiar.Error
		code := "E_INTERNAL"
		if errors.As(err, &de) {
			code = de.Code
		}
		fmt.Fprintf(os.Stderr, "%s: %v\n", code, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		args = []string{"run"}
	}
	switch args[0] {
	case "generate":
		return generateCmd(args[1:])
	case "run":
		return runCmd(args[1:])
	case "serve":
		return serveCmd(args[1:])
	case "verify":
		return verifyCmd(args[1:])
	case "version":
		fmt.Println("speakerdiar 1.0.0")
		return nil
	default:
		return fmt.Errorf("unknown command %q (expected generate, run, verify, version)", args[0])
	}
}

func serveCmd(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	workDir := fs.String("workdir", speakerdiar.DefaultWorkDir, "workspace directory")
	addr := fs.String("addr", "127.0.0.1:8088", "local listen address")
	producer := fs.String("producer", "http-local", "producer identifier")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if _, err := speakerdiar.EnsureWorkspace(*workDir, speakerdiar.DefaultConfig()); err != nil {
		return err
	}
	fmt.Printf("speakerdiar HTTP API listening on http://%s (workdir=%s)\n", *addr, *workDir)
	return http.ListenAndServe(*addr, speakerdiar.NewHTTPHandler(*workDir, *producer))
}

func generateCmd(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	workDir := fs.String("workdir", speakerdiar.DefaultWorkDir, "deterministic workspace directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	m, err := speakerdiar.GenerateSession(*workDir, speakerdiar.DefaultConfig())
	if err != nil {
		return err
	}
	fmt.Printf("generated session %s with %d chunks in %s\n", m.SessionID, len(m.Chunks), *workDir)
	return nil
}

func runCmd(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	workDir := fs.String("workdir", speakerdiar.DefaultWorkDir, "workspace containing manifest.json")
	producer := fs.String("producer", "cli-local", "stable producer identifier")
	inject := fs.String("inject", "", "optional deterministic injection case")
	verify := fs.Bool("verify-incremental", false, "compare result with a zero-state full recomputation")
	jsonReport := fs.Bool("json", false, "print JSON run report")
	stageDelayMS := fs.Int("stage-delay-ms", 0, "test-only delay before stages")
	if err := fs.Parse(args); err != nil {
		return err
	}

	svc, err := speakerdiar.NewService(*workDir, *producer, speakerdiar.Injection(*inject))
	if err != nil {
		return err
	}
	defer svc.Close()
	svc.StageDelay = time.Duration(*stageDelayMS) * time.Millisecond

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		svc.RequestStop()
		fmt.Fprintln(os.Stderr, "stop requested; finishing current chunk")
	}()

	report, err := svc.Run()
	if err != nil {
		return err
	}
	if report.GracefulStop {
		if *jsonReport {
			b, err := json.MarshalIndent(report, "", "  ")
			if err != nil {
				return err
			}
			fmt.Println(string(b))
		} else {
			fmt.Printf("graceful stop after %d completed chunk(s); checkpoint is consistent\n", len(report.Chunks))
			for _, c := range report.Chunks {
				fmt.Printf("  %s chunk_seq=%d->%d\n", c.ChunkID, c.BaseChunkSequence, c.FinalChunkSequence)
			}
			fmt.Println("rerun the same command to recover")
		}
		return nil
	}
	if *verify {
		if err := speakerdiar.VerifyIncrementalEqualsFull(*workDir, svc.Manifest, report); err != nil {
			return err
		}
	}
	if *jsonReport {
		b, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(b))
	} else {
		fmt.Printf("chunks=%d segments=%d overlaps=%d voiceprints=%d graceful_stop=%v\n",
			len(report.Chunks), len(report.Final.Segments), len(report.Final.Overlaps),
			len(report.Final.Voiceprints), report.GracefulStop)
		for _, c := range report.Chunks {
			fmt.Printf("  %s recompute=%v reuse=%v chunk_seq=%d->%d\n",
				c.ChunkID, c.RecomputedStages, c.ReusedStages, c.BaseChunkSequence, c.FinalChunkSequence)
		}
	}
	return nil
}

func verifyCmd(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	workDir := fs.String("workdir", speakerdiar.DefaultWorkDir, "workspace containing outputs/final.json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	m, err := speakerdiar.LoadManifest(*workDir)
	if err != nil {
		return err
	}
	products, files, err := speakerdiar.ComputeFullArtifacts(*workDir, m, speakerdiar.InjectNone)
	if err != nil {
		return err
	}
	_ = products
	onDisk, err := speakerdiar.ReadOutputFile(*workDir, "final.json")
	if err != nil {
		return err
	}
	if !equalBytes(onDisk, files.Final) {
		de := speakerdiar.NewIncrementalMismatch("", speakerdiar.StageSummary, "on-disk final.json differs from full computation")
		return de
	}
	fmt.Println("full verification matched outputs/final.json")
	return nil
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
