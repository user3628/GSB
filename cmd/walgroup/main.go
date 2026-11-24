package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"walgroup"
	"walgroup/demo"
	walsample "walgroup/sample"
)

const version = "1.0.0"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = runServe(os.Args[2:])
	case "generate":
		err = runGenerate(os.Args[2:])
	case "demo":
		err = runDemo(os.Args[2:])
	case "crash-demo":
		err = runCrashDemo(os.Args[2:])
	case "crash-worker":
		err = runCrashWorker(os.Args[2:])
	case "version":
		fmt.Println(version)
	case "help", "-h", "--help":
		usage()
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `walgroup - local WAL segment group-commit and checkpoint coordinator

Commands:
  serve       -data DIR [-addr 127.0.0.1:8080]
  generate    -out FILE [-seed 42]
  demo        -data DIR [-seed 42] [-workload FILE] [-clean] [-report FILE]
  crash-demo  -data DIR [-report FILE]
  version
`)
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	data := fs.String("data", "", "data directory")
	addr := fs.String("addr", "127.0.0.1:8080", "local listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *data == "" {
		return errors.New("-data is required")
	}
	e, err := walgroup.Open(*data, walgroup.Options{})
	if err != nil {
		return err
	}
	defer e.Close()
	srv := &http.Server{
		Addr:              *addr,
		Handler:           walgroup.NewHTTPHandler(e),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	fmt.Fprintf(os.Stderr, "walgroup %s listening on http://%s (data=%s, recovery=%s)\n",
		version, *addr, e.Dir(), e.RecoveryReport().Status)
	err = srv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func runGenerate(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	out := fs.String("out", "", "output workload JSON")
	seed := fs.Int64("seed", 42, "deterministic seed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("-out is required")
	}
	w := walsample.Generate(*seed)
	b, err := w.JSON()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil && filepath.Dir(*out) != "." {
		return err
	}
	tmp := *out + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, *out); err != nil {
		return err
	}
	fmt.Printf("wrote deterministic workload seed=%d actions=%d to %s\n", *seed, len(w.Actions), *out)
	return nil
}

type demoOutput struct {
	Version       string                  `json:"version"`
	Transport     string                  `json:"transport"`
	Seed          int64                   `json:"seed"`
	Run           demo.Report             `json:"run"`
	Recovery      walgroup.RecoveryReport `json:"recovery_after_reopen"`
	Verified      bool                    `json:"verified"`
	FailureReason string                  `json:"failure_reason,omitempty"`
}

func runDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	data := fs.String("data", "", "data directory")
	seed := fs.Int64("seed", 42, "deterministic seed when -workload is omitted")
	workloadPath := fs.String("workload", "", "optional workload JSON")
	clean := fs.Bool("clean", false, "remove and recreate the data directory first")
	reportPath := fs.String("report", "", "optional JSON report path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *data == "" {
		return errors.New("-data is required")
	}
	if *clean {
		if err := os.RemoveAll(*data); err != nil {
			return err
		}
	}
	w := walsample.Generate(*seed)
	if *workloadPath != "" {
		b, err := os.ReadFile(*workloadPath)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(b, &w); err != nil {
			return fmt.Errorf("decode workload: %w", err)
		}
	}
	ctx := context.Background()
	e, err := walgroup.Open(*data, walgroup.Options{
		SegmentMaxBytes: w.SegmentMaxBytes, MaxTxnsPerSegment: w.MaxTxnsPerSegment,
	})
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = e.Close()
		return err
	}
	localServer := &http.Server{Handler: walgroup.NewHTTPHandler(e), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = localServer.Serve(ln) }()
	runReport, runErr := demo.RunHTTP(ctx, "http://"+ln.Addr().String(), w)
	_ = localServer.Close()
	if closeErr := e.Close(); closeErr != nil && runErr == nil {
		runErr = closeErr
	}
	e, err = walgroup.Open(*data, walgroup.Options{
		SegmentMaxBytes: w.SegmentMaxBytes, MaxTxnsPerSegment: w.MaxTxnsPerSegment,
	})
	if err != nil {
		return err
	}
	recovery := e.RecoveryReport()
	if closeErr := e.Close(); closeErr != nil && runErr == nil {
		runErr = closeErr
	}
	out := demoOutput{Version: version, Transport: "local-http", Seed: w.Seed, Run: runReport, Recovery: recovery}
	out.Verified = runErr == nil && runReport.AllOK && recovery.Status == "CLEAN"
	if runErr != nil {
		out.FailureReason = runErr.Error()
	} else if !runReport.AllOK {
		out.FailureReason = "one or more workload expectations failed"
	} else if recovery.Status != "CLEAN" {
		out.FailureReason = "clean reopen produced non-CLEAN recovery status"
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
	if *reportPath != "" {
		if err := writeReport(*reportPath, b); err != nil {
			return err
		}
	}
	if !out.Verified {
		return errors.New(out.FailureReason)
	}
	return nil
}

func writeReport(path string, b []byte) error {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

type crashScenario struct {
	Name     string                  `json:"name"`
	Verified bool                    `json:"verified"`
	Details  []string                `json:"details"`
	Recovery walgroup.RecoveryReport `json:"recovery"`
}

type crashOutput struct {
	Version   string          `json:"version"`
	Scenarios []crashScenario `json:"scenarios"`
	Verified  bool            `json:"verified"`
}

func runCrashDemo(args []string) error {
	fs := flag.NewFlagSet("crash-demo", flag.ContinueOnError)
	data := fs.String("data", "", "data directory")
	reportPath := fs.String("report", "", "optional JSON report path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *data == "" {
		return errors.New("-data is required")
	}
	out := crashOutput{Version: version, Verified: true}

	groupDir := filepath.Join(*data, "group")
	if err := os.RemoveAll(groupDir); err != nil {
		return err
	}
	if err := killWorker(groupDir, "group"); err != nil {
		return err
	}
	e, err := walgroup.Open(groupDir, walgroup.Options{SegmentMaxBytes: 4096, MaxTxnsPerSegment: 16})
	if err != nil {
		return fmt.Errorf("group scenario reopen: %w", err)
	}
	grec := e.RecoveryReport()
	details := []string{}
	ok := contains(grec.AbortedTxnIDs, "crash-a") && contains(grec.AbortedTxnIDs, "crash-b")
	if ok {
		details = append(details, "crash-a and crash-b were ABORTED from a PENDING group intent")
	}
	base, berr := e.Transaction(context.Background(), "baseline")
	if berr == nil && base.State == walgroup.TxnDurable {
		details = append(details, "baseline remained DURABLE")
	} else {
		ok = false
	}
	for _, id := range []string{"crash-a", "crash-b"} {
		tx, err := e.Transaction(context.Background(), id)
		if err != nil || tx.State != walgroup.TxnAborted {
			ok = false
		}
	}
	if ok {
		details = append(details, "no transaction from the interrupted group entered DURABLE")
	}
	_ = e.Close()
	out.Scenarios = append(out.Scenarios, crashScenario{Name: "strong-kill-after-group-fsync", Verified: ok, Details: details, Recovery: grec})
	out.Verified = out.Verified && ok

	checkpointDir := filepath.Join(*data, "checkpoint")
	if err := os.RemoveAll(checkpointDir); err != nil {
		return err
	}
	e, err = walgroup.Open(checkpointDir, walgroup.Options{})
	if err != nil {
		return err
	}
	if _, err := e.Commit(context.Background(), walgroup.TransactionInput{ID: "cp-a", Records: []walgroup.PageUpdate{{PageNo: 1, Value: "one"}}}); err != nil {
		return err
	}
	cp1, err := e.AdvanceCheckpoint(context.Background(), walgroup.CheckpointRequest{Sequence: 1, ReplayStartLSN: 1})
	if err != nil {
		return err
	}
	if _, err := e.Commit(context.Background(), walgroup.TransactionInput{ID: "cp-b", Records: []walgroup.PageUpdate{{PageNo: 1, Value: "two"}}}); err != nil {
		return err
	}
	if err := e.Close(); err != nil {
		return err
	}
	if err := killWorker(checkpointDir, "checkpoint"); err != nil {
		return err
	}
	e, err = walgroup.Open(checkpointDir, walgroup.Options{})
	if err != nil {
		return fmt.Errorf("checkpoint scenario reopen: %w", err)
	}
	crec := e.RecoveryReport()
	cp, err := e.CurrentCheckpoint(context.Background())
	cok := err == nil && cp != nil && cp.Sequence == cp1.Sequence && cp.Digest == cp1.Digest
	cdetails := []string{}
	if cok {
		cdetails = append(cdetails, "interrupted checkpoint sequence 2 was not installed")
		cdetails = append(cdetails, "verified checkpoint sequence 1 and digest were preserved")
	}
	_ = e.Close()
	out.Scenarios = append(out.Scenarios, crashScenario{Name: "strong-kill-before-checkpoint-metadata", Verified: cok, Details: cdetails, Recovery: crec})
	out.Verified = out.Verified && cok

	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
	if *reportPath != "" {
		if err := writeReport(*reportPath, b); err != nil {
			return err
		}
	}
	if !out.Verified {
		return errors.New("crash recovery verification failed")
	}
	return nil
}

func killWorker(dir, mode string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	marker := filepath.Join(dir, "crash-marker")
	_ = os.Remove(marker)
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(exe, "crash-worker", "-data", dir, "-marker", marker, "-mode", mode)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(marker); err == nil {
			if err := cmd.Process.Kill(); err != nil {
				return fmt.Errorf("kill crash worker: %w", err)
			}
			_ = cmd.Wait()
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	return errors.New("crash worker did not reach failpoint")
}

func runCrashWorker(args []string) error {
	fs := flag.NewFlagSet("crash-worker", flag.ContinueOnError)
	data := fs.String("data", "", "data directory")
	marker := fs.String("marker", "", "ready marker")
	mode := fs.String("mode", "", "group or checkpoint")
	if err := fs.Parse(args); err != nil {
		return err
	}
	block := func() {
		if err := os.WriteFile(*marker, []byte("ready"), 0o644); err != nil {
			os.Exit(2)
		}
		select {}
	}
	opts := walgroup.Options{SegmentMaxBytes: 4096, MaxTxnsPerSegment: 16}
	switch *mode {
	case "group":
		opts.CrashPoint = func(point string, details map[string]any) {
			if point != walgroup.CrashAfterGroupSync {
				return
			}
			ids, _ := details["txn_ids"].([]string)
			for _, id := range ids {
				if id == "crash-a" {
					block()
				}
			}
		}
	case "checkpoint":
		opts.CrashPoint = func(point string, details map[string]any) {
			if point != walgroup.CrashBeforeCheckpointMeta {
				return
			}
			if seq, ok := details["sequence"].(uint64); ok && seq == 2 {
				block()
			}
		}
	default:
		return errors.New("invalid -mode")
	}
	e, err := walgroup.Open(*data, opts)
	if err != nil {
		return err
	}
	defer e.Close()
	ctx := context.Background()
	switch *mode {
	case "group":
		if _, err := e.Commit(ctx, walgroup.TransactionInput{ID: "baseline", Records: []walgroup.PageUpdate{{PageNo: 1, Value: "base"}}}); err != nil {
			return err
		}
		_, _ = e.CommitGroup(ctx, []walgroup.TransactionInput{
			{ID: "crash-a", Records: []walgroup.PageUpdate{{PageNo: 2, Value: "a"}}},
			{ID: "crash-b", Records: []walgroup.PageUpdate{{PageNo: 3, Value: "b"}}},
		})
	case "checkpoint":
		_, err = e.AdvanceCheckpoint(ctx, walgroup.CheckpointRequest{Sequence: 2, ReplayStartLSN: 2})
		return err
	}
	return nil
}

func contains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}
