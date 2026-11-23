package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"upstreampool/internal/config"
	"upstreampool/internal/httpapi"
	"upstreampool/internal/model"
	"upstreampool/internal/service"
	"upstreampool/internal/store"
	"upstreampool/internal/upstream"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		if err := serve(os.Args[2:]); err != nil {
			log.Fatalf("serve: %v", err)
		}
	case "upstream":
		if err := runUpstream(os.Args[2:]); err != nil {
			log.Fatalf("upstream: %v", err)
		}
	case "inspect":
		if err := inspect(os.Args[2:]); err != nil {
			log.Fatalf("inspect: %v", err)
		}
	case "version":
		fmt.Println("upstream-pool-diagnostics 1.0.0")
	case "help", "-h", "--help":
		usage()
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: poolctl <serve|upstream|inspect|version> [flags]")
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	configPath := fs.String("config", "samples/lab.json", "path to JSON configuration")
	listen := fs.String("listen", "", "override HTTP listen address")
	dbPath := fs.String("db", "", "override SQLite database path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := model.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	if *listen != "" {
		cfg.ListenAddr = *listen
	}
	if *dbPath != "" {
		cfg.DBPath = *dbPath
	}
	if err := config.Validate(&cfg); err != nil {
		return err
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	svc, err := service.New(context.Background(), st, cfg)
	if err != nil {
		return err
	}
	defer svc.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	report, recoverErr := svc.Recover(ctx)
	if report != nil {
		log.Printf("recovery report: aborted_intents=%d pending_batches=%d resumed_batches=%d error_code=%s message=%s",
			len(report.AbortedIntents), len(report.PendingBatches), len(report.ResumedBatches), report.ErrorCode, report.Message)
	}
	if recoverErr != nil {
		log.Printf("recovery completed with error: %v", recoverErr)
	}

	api := httpapi.New(cfg.ListenAddr, svc)
	errCh := make(chan error, 1)
	go func() { errCh <- api.ListenAndServe() }()
	log.Printf("diagnostics HTTP listening on %s", cfg.ListenAddr)
	select {
	case <-ctx.Done():
		timeout, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = api.Shutdown()
		_ = timeout
		return nil
	case err := <-errCh:
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}
}

func runUpstream(args []string) error {
	fs := flag.NewFlagSet("upstream", flag.ContinueOnError)
	listen := fs.String("listen", "127.0.0.1:18701", "loopback TCP listen address")
	readRate := fs.Int64("read-rate", 0, "slow receiver read rate in bytes/sec (0 is unlimited)")
	maxFrame := fs.Uint64("max-frame", 64<<20, "maximum frame size in bytes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return upstream.Run(ctx, *listen, *readRate, *maxFrame)
}

func inspect(args []string) error {
	fs := flag.NewFlagSet("inspect", flag.ContinueOnError)
	dbPath := fs.String("db", "data/workbench.db", "SQLite database path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	db := st.DB()
	fmt.Printf("database: %s\n", *dbPath)
	for _, table := range []string{"endpoints", "tracks", "batches", "requests", "request_io", "checkpoints", "transitions", "request_results", "final_connections", "connection_uses", "intents", "recovery_reports"} {
		var count int64
		if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
			return err
		}
		fmt.Printf("%-20s %d\n", table, count)
	}
	report, err := st.LatestRecoveryReport(context.Background())
	if err == nil && report != nil {
		fmt.Printf("latest recovery: id=%d aborted=%d pending=%d resumed=%d error=%s\n", report.ID, len(report.AbortedIntents), len(report.PendingBatches), len(report.ResumedBatches), report.ErrorCode)
	}
	return nil
}
