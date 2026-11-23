package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"h2diag/internal/api"
	"h2diag/internal/model"
	"h2diag/internal/store"
)

func main() {
	addr := flag.String("addr", envOr("H2DIAG_ADDR", "127.0.0.1:18080"), "loopback listen address")
	dbPath := flag.String("db", envOr("H2DIAG_DB", "data/h2diag.sqlite"), "SQLite database path")
	readyFile := flag.String("ready-file", os.Getenv("H2DIAG_READY_FILE"), "write the bound loopback URL to this file")
	crashAfterRound := flag.Int64("crash-after-round", envInt64("H2DIAG_CRASH_AFTER_ROUND", 0), "self-SIGKILL after this scheduling round intent is durable (demo/testing only)")
	flag.Parse()

	if err := os.MkdirAll(filepath.Dir(*dbPath), 0o755); err != nil && filepath.Dir(*dbPath) != "." {
		log.Fatalf("create database directory: %v", err)
	}
	ctx := context.Background()
	dataStore, err := store.Open(ctx, *dbPath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer dataStore.Close()
	if *crashAfterRound > 0 {
		dataStore.SetAfterIntentHook(func(commit model.PlanCommit) {
			if commit.Round == nil || commit.Round.RoundNo != *crashAfterRound {
				return
			}
			fmt.Fprintf(os.Stderr, "forced termination after durable intent: routine=%d version=%d\n", commit.Round.RoundNo, commit.Version)
			_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
			os.Exit(137)
		})
	}

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	boundURL := "http://" + listener.Addr().String()
	if *readyFile != "" {
		if err := os.WriteFile(*readyFile, []byte(boundURL+"\n"), 0o644); err != nil {
			log.Fatalf("write ready file: %v", err)
		}
		defer os.Remove(*readyFile)
	}
	handler := api.New(dataStore).Handler()
	httpServer := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       2 * time.Minute,
		WriteTimeout:      2 * time.Minute,
		IdleTimeout:       60 * time.Second,
	}
	log.Printf("h2diag listening on %s db=%s", boundURL, *dbPath)

	errCh := make(chan error, 1)
	go func() {
		errCh <- httpServer.Serve(listener)
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve: %v", err)
		}
	case signal := <-signals:
		log.Printf("received %s, shutting down", signal)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown: %v", err)
		}
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envInt64(name string, fallback int64) int64 {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}
