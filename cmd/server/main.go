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

	"profilehotspot/internal/httpapi"
	"profilehotspot/internal/service"
	"profilehotspot/internal/storage"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
	dbPath := flag.String("db", "data/profile.db", "SQLite database path")
	flag.Parse()

	ctx := context.Background()
	store, err := storage.Open(ctx, *dbPath)
	if err != nil {
		log.Fatalf("open storage: %v", err)
	}
	defer store.Close()
	recovery, err := store.LatestRecoveryReport(ctx)
	if err != nil {
		log.Fatalf("read recovery report: %v", err)
	}
	log.Printf("storage=%s recovery_integrity=%s rolled_back=%d repaired=%d last_commit=%d",
		store.Path(), recovery.IntegrityCheck, recovery.RolledBackIntents, recovery.RepairedCommitted, recovery.LastCompleteCommitSeq)

	svc := service.New(store)
	api := httpapi.New(svc)
	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-shutdown
		log.Printf("shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(ctx)
	}()

	log.Printf("profile sampling service listening on http://%s", *addr)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}
