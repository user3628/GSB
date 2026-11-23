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

	"gatewayfreshness.local/workbench/internal/config"
	"gatewayfreshness.local/workbench/internal/httpapi"
	"gatewayfreshness.local/workbench/internal/service"
	"gatewayfreshness.local/workbench/internal/store"
)

func main() {
	configPath := flag.String("config", "samples/config.json", "path to local JSON config")
	dbOverride := flag.String("db", "", "override SQLite database path")
	addrOverride := flag.String("addr", "", "override loopback listen address")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	addr := cfg.HTTPAddr
	if *addrOverride != "" {
		if err := config.EnsureLoopback(*addrOverride); err != nil {
			log.Fatalf("addr: %v", err)
		}
		addr = *addrOverride
	}
	dbPath := cfg.ResolveDBPath(*dbOverride)
	if dir := filepath.Dir(dbPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Fatalf("database directory: %v", err)
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	st, err := store.Open(ctx, store.Options{
		Path: dbPath, ConfigVersion: cfg.Version(), ConfigSnapshotJSON: cfg.SnapshotJSON(),
		SigningKey: cfg.SigningKeyBytes(), SigningKeyID: cfg.SigningKeyID,
		FastPrecheckLimit: cfg.FastPrecheckLimit,
	})
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	defer st.Close()

	svc := service.New(st, cfg)
	recovery, err := svc.Recovery(ctx)
	if err != nil {
		log.Fatalf("startup recovery: %v", err)
	}
	reportJSON, _ := json.Marshal(recovery)
	log.Printf("startup_recovery=%s", reportJSON)

	api := httpapi.New(svc, cfg.MaxRequestBodyBytes)
	server := &http.Server{
		Addr:              addr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}
	log.Printf("gateway-freshness-workbench listening on http://%s (db=%s config=%s)", addr, dbPath, cfg.Version())
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(listener)
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown: %v", err)
		}
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve: %v", err)
		}
	}
	fmt.Println("workbench stopped")
}
