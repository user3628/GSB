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

	"localinfer/workbench/internal/api"
	"localinfer/workbench/internal/domain"
	"localinfer/workbench/internal/service"
	"localinfer/workbench/internal/store"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "local HTTP listen address (loopback only by default)")
	dbPath := flag.String("db", "./workbench.db", "SQLite database path")
	modelsPath := flag.String("models", "./samples/models.json", "deterministic model catalog JSON")
	webDir := flag.String("web", "./web", "diagnostic static asset directory")
	allowExitFault := flag.Bool("allow-exit-fault", false, "enable deliberate hard-exit crash injection")
	allowNonLoopback := flag.Bool("allow-non-loopback", false, "permit a non-loopback listen host")
	recoverOnly := flag.Bool("recover-only", false, "run recovery scan, print the report, and exit")
	flag.Parse()

	if err := ensureLoopback(*addr, *allowNonLoopback); err != nil {
		log.Fatalf("listen guard: %v", err)
	}
	modelsData, err := os.ReadFile(*modelsPath)
	if err != nil {
		log.Fatalf("read model catalog: %v", err)
	}
	catalog, err := domain.ParseModelCatalog(modelsData)
	if err != nil {
		log.Fatalf("parse model catalog: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(*dbPath), 0o755); err != nil {
		log.Fatalf("create database directory: %v", err)
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("open SQLite: %v", err)
	}
	defer st.Close()

	report, err := st.Recover()
	if err != nil {
		log.Fatalf("recovery scan: %v", err)
	}
	if *recoverOnly {
		body, _ := json.MarshalIndent(report, "", "  ")
		fmt.Println(string(body))
		return
	}
	log.Printf("recovery report=%s clean=%t aborted_runs=%d aborted_batches=%d preserved_batches=%d", report.ReportID, report.Clean, len(report.AbortedRuns), len(report.AbortedBatches), len(report.PreservedBatches))

	if err := st.InstallCatalog(catalog); err != nil {
		log.Fatalf("install model catalog: %v", err)
	}
	loaded, err := st.LoadCatalog()
	if err != nil {
		log.Fatalf("load model catalog: %v", err)
	}
	svc := service.New(st, loaded, *allowExitFault)
	handler := api.NewServer(svc, *webDir, log.Default())
	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("local inference batch workbench listening on http://%s (db=%s catalog=%s exit_faults=%t)", listener.Addr().String(), *dbPath, loaded.Hash[:12], *allowExitFault)
	log.Printf("diagnostics: http://%s/  health: http://%s/healthz", listener.Addr().String(), listener.Addr().String())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errCh := make(chan error, 1)
	go func() {
		errCh <- httpServer.Serve(listener)
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown: %v", err)
		}
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve: %v", err)
		}
	}
}

func ensureLoopback(addr string, allowNonLoopback bool) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if port == "" {
		return fmt.Errorf("port is required")
	}
	if allowNonLoopback {
		return nil
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("host %q is not loopback; use 127.0.0.1 or pass -allow-non-loopback", host)
	}
	return nil
}
