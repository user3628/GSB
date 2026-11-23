package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"recoveryengine/internal/engine"
	"recoveryengine/internal/httpapi"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:19180", "HTTP listen address")
	databasePath := flag.String("db", filepath.Join("data", "recovery.db"), "SQLite database path")
	testHooks := flag.Bool("test-hooks", os.Getenv("RECOVERY_ENGINE_TEST_HOOKS") == "1", "enable deterministic crash-window test hooks")
	flag.Parse()

	if err := os.MkdirAll(filepath.Dir(*databasePath), 0o755); err != nil {
		log.Fatalf("create database directory: %v", err)
	}
	app, err := engine.New(context.Background(), *databasePath, *testHooks)
	if err != nil {
		log.Fatalf("initialize engine: %v", err)
	}
	defer app.Close()

	server := &http.Server{
		Addr:              *addr,
		Handler:           httpapi.New(app, *testHooks),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-shutdown
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
	log.Printf("recovery coordinator listening on http://%s (db=%s, test_hooks=%t, version=%d)", *addr, *databasePath, *testHooks, app.State().Version)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve: %v", err)
	}
	fmt.Fprintln(os.Stderr, "recovery coordinator stopped")
}
