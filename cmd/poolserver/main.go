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
	"path/filepath"
	"syscall"
	"time"

	"local/sqlitepool/internal/dbconn"
	"local/sqlitepool/internal/domain"
	"local/sqlitepool/internal/httpapi"
	"local/sqlitepool/internal/pool"
	"local/sqlitepool/internal/sample"
	"local/sqlitepool/internal/store"
)

func main() {
	defaultSample := flag.String("sample", "samples/pool_sample.json", "path to the deterministic JSON sample")
	dataDir := flag.String("data-dir", ".e2e-data", "directory for SQLite database and metadata")
	addr := flag.String("addr", "", "optional loopback listen override")
	allowDebug := flag.Bool("allow-debug", false, "enable loopback-only crash fixture endpoint")
	flag.Parse()

	s, err := sample.Load(*defaultSample)
	if err != nil {
		log.Fatalf("load sample: %v", err)
	}
	listenAddr := s.Listen
	if *addr != "" {
		listenAddr = *addr
	}
	if err := requireLoopback(listenAddr); err != nil {
		log.Fatalf("listen address: %v", err)
	}
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}
	dbPath := filepath.Join(*dataDir, s.DatabaseFile)
	metaPath := filepath.Join(*dataDir, s.MetadataFile)

	ctx := context.Background()
	st, err := store.Open(ctx, metaPath)
	if err != nil {
		log.Fatalf("open metadata store: %v", err)
	}
	defer st.Close()
	seeded, err := st.SeedConfig(ctx, s.InitialPool)
	if err != nil {
		log.Fatalf("seed config: %v", err)
	}
	cfg, err := st.LoadConfig(ctx)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	report, err := st.Recover(ctx)
	if err != nil {
		log.Fatalf("recover persisted intents: %v", err)
	}
	if err := writeRecoveryReport(*dataDir, report); err != nil {
		log.Fatalf("write recovery report: %v", err)
	}

	p, err := pool.New(ctx, cfg, st, dbconn.SQLiteOpener{Path: dbPath})
	if err != nil {
		log.Fatalf("create pool: %v", err)
	}
	api := httpapi.New(p, st, httpapi.Options{AllowDebug: *allowDebug})

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", listenAddr, err)
	}
	httpServer := &http.Server{
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	ready, _ := json.Marshal(map[string]any{
		"event":              "pool_server_ready",
		"listen":             listener.Addr().String(),
		"database_path":      dbPath,
		"metadata_path":      metaPath,
		"config_version":     cfg.Version,
		"config_seeded":      seeded,
		"recovery_report_id": report.ReportID,
		"pending_intents":    report.PendingIntentsFound,
	})
	fmt.Println(string(ready))

	errCh := make(chan error, 1)
	go func() {
		if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigCh:
		log.Printf("received %s; shutting down", sig)
	case err := <-errCh:
		log.Printf("http server error: %v", err)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	_ = p.Shutdown(shutdownCtx)
}

func requireLoopback(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if host != "127.0.0.1" {
		return fmt.Errorf("host must be 127.0.0.1, got %q", host)
	}
	if port == "" {
		return fmt.Errorf("port is required")
	}
	return nil
}

func writeRecoveryReport(dataDir string, report domain.RecoveryReport) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dataDir, "recovery-latest.json"), data, 0o644)
}
