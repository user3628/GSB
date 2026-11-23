package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example.com/joinshuffle/internal/httpapi"
	"example.com/joinshuffle/internal/service"
	"example.com/joinshuffle/internal/store"
)

func main() {
	defaultAddr := os.Getenv("SHUFFLE_ADDR")
	if defaultAddr == "" {
		port := os.Getenv("PORT")
		if port == "" {
			port = "18080"
		}
		defaultAddr = "127.0.0.1:" + port
	}
	defaultDB := os.Getenv("SHUFFLE_DB")
	if defaultDB == "" {
		defaultDB = "data/engine.db"
	}
	addr := flag.String("addr", defaultAddr, "HTTP listen address")
	dbPath := flag.String("db", defaultDB, "SQLite database path")
	flag.Parse()

	ctx := context.Background()
	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("open SQLite store: %v", err)
	}
	defer st.Close()
	svc := service.New(st)
	report, err := svc.Recover(ctx)
	if err != nil {
		log.Fatalf("startup recovery scan: %v", err)
	}
	if len(report.AbortedRequests) > 0 {
		log.Printf("startup recovery aborted unfinished requests: %v", report.AbortedRequests)
	}
	if err := svc.RestoreSnapshots(ctx); err != nil {
		log.Printf("snapshot restore failed; service remains available: %v", err)
	}

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen on %s: %v", *addr, err)
	}
	httpServer := &http.Server{
		Handler:           httpapi.New(svc),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      5 * time.Minute,
		IdleTimeout:       60 * time.Second,
	}
	ready := map[string]any{
		"event":   "listening",
		"address": listener.Addr().String(),
		"db":      *dbPath,
	}
	_ = json.NewEncoder(os.Stdout).Encode(ready)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()
	if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve: %v", err)
	}
	fmt.Fprintln(os.Stderr, "server stopped")
}
