package cli

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"

	"multirecall/internal/model"
	"multirecall/internal/orchestrator"
	"multirecall/internal/recall"
	"multirecall/internal/sample"
	"multirecall/internal/server"
	"multirecall/internal/store"
)

func Main() {
	if len(os.Args) < 2 {
		if err := runServe(nil); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		return
	}
	var err error
	switch os.Args[1] {
	case "init":
		err = runInit(os.Args[2:])
	case "run":
		err = runRecall(os.Args[2:])
	case "serve", "server", "http":
		err = runServe(os.Args[2:])
	case "budget":
		err = runBudget(os.Args[2:])
	case "help", "-h", "--help":
		usage()
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
	fmt.Fprintln(os.Stderr, `multirecall commands:
  (default) serve [--data data] [--addr 127.0.0.1:18080]
  init   [--data data]
  run    [--data data] [--out report.json] [--mode sequential|parallel] [--workers N] [--db checkpoints.db] [--kill-after-commits N]
  serve  [--data data] [--addr 127.0.0.1:18080]
  budget [--data data]`)
}

func runInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	data := fs.String("data", "data", "deterministic sample directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ds, cfg, err := sample.Ensure(*data)
	if err != nil {
		return err
	}
	out := map[string]any{
		"data_dir": *data, "dataset": sample.DatasetPath(*data), "config": sample.ConfigPath(*data),
		"dataset_digest": model.Digest(ds), "config_digest": cfg.ContentDigest,
		"items": len(ds.Items), "queries": len(ds.Queries), "config_version": cfg.ConfigVersion,
	}
	fmt.Println(string(model.CanonicalJSON(out)))
	return nil
}

func runRecall(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	data := fs.String("data", "data", "deterministic sample directory")
	out := fs.String("out", "recall-report.json", "canonical JSON report output")
	mode := fs.String("mode", "parallel", "sequential or parallel")
	workers := fs.Int("workers", 0, "query worker count; 0 uses config")
	dbPath := fs.String("db", "", "SQLite checkpoint path; empty disables checkpoints")
	killAfter := fs.Int("kill-after-commits", 0, "send SIGKILL to self after N durable checkpoint commits")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ds, cfg, err := sample.Ensure(*data)
	if err != nil {
		return err
	}
	eng := recall.NewEngine(ds, cfg)
	workerCount := *workers
	if *mode == "sequential" {
		workerCount = 1
	}

	var report *recall.Report
	if *dbPath != "" {
		st, err := store.Open(*dbPath)
		if err != nil {
			return err
		}
		if *killAfter > 0 {
			var commits int32
			st.AfterCommit = func(seq int64, queryID, stage string) {
				n := atomic.AddInt32(&commits, 1)
				if int(n) == *killAfter {
					_ = syscall.Kill(syscall.Getpid(), syscall.SIGKILL)
				}
			}
		}
		report, err = orchestrator.New(eng, st).RunAll(context.Background(), workerCount)
		closeErr := st.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	} else {
		report, err = eng.RunAll(recall.RunOptions{Workers: workerCount})
	}
	if err != nil {
		return err
	}
	if dir := filepath.Dir(*out); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	raw := append(recall.CanonicalReport(report), '\n')
	if err := os.WriteFile(*out, raw, 0o644); err != nil {
		return err
	}
	summary := map[string]any{
		"out": *out, "bytes": len(raw), "result_digest": report.ResultDigest,
		"queries": report.QueryCount, "mode": *mode, "checkpoint_db": *dbPath,
	}
	fmt.Println(string(model.CanonicalJSON(summary)))
	return nil
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	data := fs.String("data", "data", "deterministic sample directory")
	addr := fs.String("addr", "127.0.0.1:18080", "loopback listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	app, err := server.NewApp(*data, true)
	if err != nil {
		return err
	}
	defer app.Close()
	fmt.Fprintf(os.Stderr, "multirecall listening on http://%s (data=%s)\n", *addr, *data)
	return app.ListenAndServe(*addr)
}

func runBudget(args []string) error {
	fs := flag.NewFlagSet("budget", flag.ContinueOnError)
	data := fs.String("data", "data", "deterministic sample directory")
	queryCount := fs.Int("queries", 0, "query count; 0 uses generated query count")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ds, cfg, err := sample.Ensure(*data)
	if err != nil {
		return err
	}
	n := *queryCount
	if n == 0 {
		n = len(ds.Queries)
	}
	fmt.Println(string(model.CanonicalJSON(recall.AdjudicateBudget(cfg, n))))
	return nil
}
