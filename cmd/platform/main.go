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

	"deployverify/internal/api"
	"deployverify/internal/demo"
	"deployverify/internal/store"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18080", "HTTP listen address")
	dbPath := flag.String("db", "workspace/platform.db", "SQLite database path")
	workdir := flag.String("workdir", "workspace", "deterministic sample and artifact directory")
	probeBaseURL := flag.String("probe-base-url", "", "loopback base URL used by probes; defaults to listen address")
	recoverOnly := flag.Bool("recover-only", false, "run intent recovery and exit")
	skipSamples := flag.Bool("skip-samples", false, "do not generate deterministic JSON samples at startup")
	writeSamplesOnly := flag.Bool("write-samples-only", false, "write deterministic JSON samples and exit")
	flag.Parse()

	if err := run(*addr, *dbPath, *workdir, *probeBaseURL, *recoverOnly, *skipSamples, *writeSamplesOnly); err != nil {
		log.Fatalf("platform failed: %v", err)
	}
}

func run(addr, dbPath, workdir, probeBaseURL string, recoverOnly, skipSamples, writeSamplesOnly bool) error {
	if err := os.MkdirAll(workdir, 0o755); err != nil {
		return err
	}
	if !skipSamples || writeSamplesOnly {
		manifest, err := demo.WriteSamples(workdir)
		if err != nil {
			return err
		}
		log.Printf("deterministic samples: %s", manifest.Workdir)
		if writeSamplesOnly {
			encoded, _ := json.MarshalIndent(manifest, "", "  ")
			fmt.Println(string(encoded))
			return nil
		}
	}
	if dbDir := filepath.Dir(dbPath); dbDir != "." && dbDir != "" {
		if err := os.MkdirAll(dbDir, 0o755); err != nil {
			return err
		}
	}
	st, err := store.Open(context.Background(), store.DSN(dbPath))
	if err != nil {
		return err
	}
	defer st.Close()
	recovery, err := st.Recover(context.Background())
	if err != nil {
		return fmt.Errorf("startup recovery: %w", err)
	}
	recoveryJSON, _ := json.Marshal(recovery)
	log.Printf("startup recovery: %s", recoveryJSON)
	if recoverOnly {
		fmt.Println(string(recoveryJSON))
		return nil
	}

	baseURL := probeBaseURL
	if baseURL == "" {
		host, port, splitErr := net.SplitHostPort(addr)
		if splitErr != nil {
			return splitErr
		}
		if host == "" || host == "0.0.0.0" || host == "::" {
			host = "127.0.0.1"
		}
		baseURL = "http://" + net.JoinHostPort(host, port)
	}
	server := api.NewServer(st, baseURL, workdir)
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		log.Printf("PLATFORM_READY addr=%s probe=%s db=%s", addr, baseURL, dbPath)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return httpServer.Shutdown(shutdownCtx)
}
