package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example.com/replicabudget/internal/httpapi"
	"example.com/replicabudget/internal/store"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18080", "HTTP listen address")
	dbPath := flag.String("db", "data/policies.db", "SQLite database path")
	flag.Parse()
	crashStage := os.Getenv("POLICY_FAILPOINT")
	logger := log.New(os.Stderr, "policyd ", log.LstdFlags|log.Lmicroseconds)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := store.Open(ctx, *dbPath)
	if err != nil {
		logger.Fatalf("open store: %v", err)
	}
	defer st.Close()
	if crashStage != "" {
		st.SetFailpoint(func(stage string) error {
			if stage == crashStage {
				logger.Printf("FAILPOINT %s: terminating process without cleanup", stage)
				_ = os.Stdout.Sync()
				_ = os.Stderr.Sync()
				os.Exit(86)
			}
			return nil
		})
	}
	api := &httpapi.Server{Store: st, Log: logger}
	srv := &http.Server{
		Addr:              *addr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	logger.Printf("listening on http://%s db=%s failpoint=%q", *addr, *dbPath, crashStage)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigCh:
		logger.Printf("received %s; shutting down", sig)
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "server error: %v\n", err)
			os.Exit(1)
		}
	}
}
