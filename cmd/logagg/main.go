package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"logagg/internal/agg"
	"logagg/internal/api"
	"logagg/internal/codec"
	"logagg/internal/engine"
	"logagg/internal/sample"
	"logagg/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = runServe(os.Args[2:])
	case "prepare-crash":
		err = runPrepareCrash(os.Args[2:])
	case "recover":
		err = runRecover(os.Args[2:])
	case "resume-crash":
		err = runResumeCrash(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		log.Fatal(err)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `logagg commands:
  serve [--addr 127.0.0.1:18080] [--db artifacts/service/logagg.db]
  prepare-crash [--db artifacts/recovery/logagg.db] [--reset]
  recover [--db artifacts/recovery/logagg.db]
  resume-crash [--db artifacts/recovery/logagg.db]`)
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:18080", "loopback listen address")
	dbPath := fs.String("db", "artifacts/service/logagg.db", "SQLite database path")
	memoryMB := fs.Int64("memory-mb", 16, "per-block memory limit in MiB")
	concurrency := fs.Int("concurrency", 2, "global decode concurrency")
	queueCapacity := fs.Int("queue", 128, "bounded decode wait queue capacity")
	seedShard := fs.Bool("seed-shard", true, "commit deterministic sample shard if absent")
	if err := fs.Parse(args); err != nil {
		return err
	}

	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	eng := engine.New(st, engine.Config{
		MemoryLimitBytes: *memoryMB << 20,
		MaxConcurrency:   *concurrency,
		QueueCapacity:    *queueCapacity,
	})
	defer eng.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	recovery, err := eng.Recover(ctx)
	if err != nil {
		return err
	}
	log.Printf("sqlite=%s recovery committed=%d rolled_back=%d needs_recompute=%d reusable_partials=%d",
		*dbPath, len(recovery.Committed), len(recovery.RolledBack), len(recovery.NeedsRecompute), len(recovery.ReusablePartialSums))

	if *seedShard {
		version, ok, err := st.CurrentShardVersion(ctx, sample.ShardID)
		if err != nil {
			return err
		}
		if !ok {
			if _, err := eng.CommitGeneratedShard(ctx, sample.ShardID, 1); err != nil {
				return err
			}
			log.Printf("committed deterministic shard %s v1", sample.ShardID)
		} else {
			pkg, err := st.LoadShardPackage(ctx, sample.ShardID, version)
			if err != nil {
				return err
			}
			if pkg != nil {
				if _, err := eng.Registry().Commit(pkg); err != nil {
					return err
				}
			}
		}
	}

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("bind loopback address %s: %w", *addr, err)
	}
	server := &http.Server{
		Handler:           api.NewServer(eng, log.Default()).Mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("listening on http://%s", listener.Addr().String())
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func runPrepareCrash(args []string) error {
	fs := flag.NewFlagSet("prepare-crash", flag.ContinueOnError)
	dbPath := fs.String("db", "artifacts/recovery/logagg.db", "SQLite database path")
	reset := fs.Bool("reset", true, "remove an existing database before preparing")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *reset {
		for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
			_ = os.Remove(*dbPath + suffix)
		}
	}
	if dir := filepath.Dir(*dbPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}

	ctx := context.Background()
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	eng := engine.New(st, engine.DefaultConfig())
	pkg := eng.Generator().Shard(1)
	if _, err := eng.CommitShard(ctx, pkg); err != nil {
		return err
	}

	block0 := pkg.BlockByNo(0)
	groups, err := codec.DecodeColumn(block0, codec.ColumnGroup, 0, 0)
	if err != nil {
		return err
	}
	metrics, err := codec.DecodeColumn(block0, codec.ColumnMetric, 0, 0)
	if err != nil {
		return err
	}
	definitionHash := engine.DefinitionHash(sample.GroupColumn, sample.MetricColumn, sample.MetricScale)
	committed := agg.NewSegmentSum("crash-session", sample.ShardID, 1, block0.Meta.Partition,
		block0.Meta.BlockNo, 0, block0.Meta.RowCount, definitionHash)
	if err := committed.AddRows(groups, metrics); err != nil {
		return err
	}
	if err := st.CommitPartialSum(ctx, committed, ""); err != nil {
		return err
	}

	block1 := pkg.BlockByNo(1)
	intent := store.Intent{
		ID: "segment-start:crash-pending-block-1", Kind: "segment_start", Status: "pending",
		SessionID: "crash-session", BatchID: "crash-resume-batch", ShardID: sample.ShardID, Version: 1,
		BlockNo: block1.Meta.BlockNo, SegmentStart: 0, SegmentEnd: block1.Meta.RowCount,
	}
	if err := st.BeginSegment(ctx, intent, block1.Meta.Partition); err != nil {
		return err
	}

	payload, _ := json.Marshal(map[string]any{
		"db": *dbPath, "pid": os.Getpid(), "committed_block": 0,
		"pending_block": 1, "action": "SIGKILL_self_after_durable_intent",
	})
	fmt.Println(string(payload))
	_ = os.Stdout.Sync()
	_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	select {}
}

func runRecover(args []string) error {
	fs := flag.NewFlagSet("recover", flag.ContinueOnError)
	dbPath := fs.String("db", "artifacts/recovery/logagg.db", "SQLite database path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	report, err := st.Recover(context.Background())
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}

func runResumeCrash(args []string) error {
	fs := flag.NewFlagSet("resume-crash", flag.ContinueOnError)
	dbPath := fs.String("db", "artifacts/recovery/logagg.db", "SQLite database path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	eng := engine.New(st, engine.DefaultConfig())
	defer eng.Close()

	recovery, err := eng.Recover(ctx)
	if err != nil {
		return err
	}
	pkg, err := st.LoadShardPackage(ctx, sample.ShardID, 1)
	if err != nil {
		return err
	}
	if pkg == nil {
		return fmt.Errorf("deterministic shard not found in %s", *dbPath)
	}
	if _, err := eng.Registry().Commit(pkg); err != nil {
		return err
	}

	resp, err := eng.Advance(ctx, engine.AdvanceRequest{
		SessionID: "crash-session", BatchID: "crash-resume-batch",
		Items: []engine.Item{
			{ShardID: sample.ShardID, Version: 1, BlockNo: 0},
			{ShardID: sample.ShardID, Version: 1, BlockNo: 1},
		},
		GroupColumn:  sample.GroupColumn,
		MetricColumn: sample.MetricColumn,
		InputScale:   sample.MetricScale,
		Precision:    agg.Precision{InputScale: sample.MetricScale, ResultScale: 2, Mode: agg.RoundHalfUp},
		ByteBudget:   1 << 40,
	})
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"recovery": recovery, "advance": resp})
}
