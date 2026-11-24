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

	"ampcheckpoint/internal/engine"
	"ampcheckpoint/internal/httpapi"
	"ampcheckpoint/internal/sample"
)

func main() {
	var (
		addr      = flag.String("addr", "127.0.0.1:8080", "local HTTP listen address")
		dbPath    = flag.String("db", "data/checkpoints.db", "SQLite database path")
		sampleDir = flag.String("sample-dir", "", "sample directory; empty creates a temporary directory")
		seed      = flag.Int64("seed", sample.DefaultSeed, "fixed deterministic sample seed")
		reset     = flag.Bool("reset", false, "remove the existing database before opening it")
	)
	flag.Parse()
	if err := run(*addr, *dbPath, *sampleDir, *seed, *reset); err != nil {
		log.Fatalf("server: %v", err)
	}
}

func run(addr, dbPath, sampleDir string, seed int64, reset bool) error {
	absDB, err := filepath.Abs(dbPath)
	if err != nil {
		return err
	}
	if reset {
		for _, suffix := range []string{"", "-wal", "-shm"} {
			if err := os.Remove(absDB + suffix); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("reset database: %w", err)
			}
		}
	}
	gen, err := sample.New(seed, sampleDir)
	if err != nil {
		return err
	}
	manifest, err := gen.Ensure()
	if err != nil {
		return fmt.Errorf("prepare deterministic sample: %w", err)
	}
	eng, err := engine.Open(absDB, gen)
	if err != nil {
		return fmt.Errorf("open engine: %w", err)
	}
	defer eng.Close()
	status, err := eng.Status(context.Background())
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	httpServer := &http.Server{
		Handler:           httpapi.New(eng),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       90 * time.Second,
	}
	ready := map[string]any{
		"event":              "ready",
		"addr":               listener.Addr().String(),
		"db":                 absDB,
		"sample_dir":         manifest.SampleDir,
		"seed":               manifest.Seed,
		"policy_version":     status["policy_version"],
		"checkpoint_version": status["checkpoint_version"],
		"recovered_intents":  status["recovered_intents"],
	}
	raw, _ := json.Marshal(ready)
	fmt.Println(string(raw))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errCh := make(chan error, 1)
	go func() {
		if serveErr := httpServer.Serve(listener); serveErr != nil && serveErr != http.ErrServerClosed {
			errCh <- serveErr
		}
		close(errCh)
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return httpServer.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}
