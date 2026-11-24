package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"checkpointengine/internal/engine"
	"checkpointengine/internal/httpapi"
	"checkpointengine/internal/store"
)

func main() {
	addr := flag.String("addr", envOr("CHECKPOINT_ENGINE_ADDR", "127.0.0.1:18080"), "local HTTP listen address")
	dbPath := flag.String("db", envOr("CHECKPOINT_ENGINE_DB", filepath.Join("data", "checkpoint-engine.db")), "SQLite database path")
	dataDir := flag.String("data-dir", envOr("CHECKPOINT_ENGINE_DATA_DIR", "data"), "durable blob data directory")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	st, err := store.Open(*dbPath)
	if err != nil {
		logger.Error("open_database_failed", "error", err)
		os.Exit(1)
	}
	defer st.Close()
	eng, err := engine.New(st, engine.Options{DataDir: *dataDir, Failpoint: faultpointFromEnv(logger)})
	if err != nil {
		logger.Error("create_engine_failed", "error", err)
		os.Exit(1)
	}
	report, err := eng.Recover(context.Background())
	if err != nil {
		logger.Error("startup_recovery_failed", "error", err)
		os.Exit(1)
	}
	logger.Info("startup_recovery_complete", "report_id", report.ReportID,
		"interrupted_intents", report.InterruptedIntents, "rolled_back_writes", report.RolledBackWrites,
		"rolled_back_evictions", report.RolledBackEvictions, "durable_shards", report.DurableShards,
		"pending_shards", report.PendingShards)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           httpapi.New(eng, logger).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		logger.Info("listening", "addr", *addr, "db", st.Path, "data_dir", *dataDir)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http_server_failed", "error", err)
			os.Exit(1)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful_shutdown_failed", "error", err)
	}
	fmt.Println("checkpoint engine stopped")
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func faultpointFromEnv(logger *slog.Logger) func(string) error {
	target := os.Getenv("CHECKPOINT_ENGINE_FAULT")
	if target == "" {
		return nil
	}
	logger.Warn("fault_injection_enabled", "point", target)
	return func(point string) error {
		if point == target {
			logger.Error("fault_injection_terminating_process", "point", point)
			os.Exit(86)
		}
		return nil
	}
}
