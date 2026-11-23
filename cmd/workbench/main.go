package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"local/rollout-guard/internal/engine"
	"local/rollout-guard/internal/httpapi"
	"local/rollout-guard/internal/store"
	"local/rollout-guard/internal/web"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "loopback listen address")
	dbPath := flag.String("db", "data/workbench.db", "SQLite database path")
	rollbackURL := flag.String("rollback-url", "", "local HTTP rollback target; defaults to this process's local target")
	crashPoint := flag.String("crash-point", os.Getenv("WORKBENCH_CRASH_POINT"), "test-only crash injection point")
	crashBatch := flag.String("crash-batch", os.Getenv("WORKBENCH_CRASH_BATCH"), "test-only crash batch filter")
	version := flag.String("version", "1.0.0", "build version")
	flag.Parse()

	if !strings.HasPrefix(*addr, "127.0.0.1:") && !strings.HasPrefix(*addr, "localhost:") {
		log.Fatalf("workbench must bind to loopback, got %q", *addr)
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("open sqlite: %v", err)
	}
	defer st.Close()

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	actualAddr := listener.Addr().String()
	target := strings.TrimSpace(*rollbackURL)
	if target == "" {
		target = "http://" + actualAddr + "/api/local-rollback-target"
	}
	if err := requireLoopbackURL(target); err != nil {
		log.Fatalf("rollback target rejected: %v", err)
	}
	eng := engine.New(st, engine.Options{
		RollbackURL: target, CrashPoint: *crashPoint, CrashBatch: *crashBatch,
		CrashHook: func(point string, intentID int64, batchID string) {
			log.Printf("CRASH_INJECTED point=%s intent_id=%d batch_id=%s pid=%d", point, intentID, batchID, os.Getpid())
			os.Exit(137)
		},
	})
	recovery, err := eng.RecoverOnStartup(context.Background())
	if err != nil {
		log.Fatalf("startup recovery: %v", err)
	}
	log.Printf("startup recovery clean=%v pending_aborted=%d rolling_back_resumed=%d commit=%d",
		recovery.Clean, len(recovery.AbortedIntents), len(recovery.ResumedRollbacks), recovery.LatestCommitSeq)

	assets, err := fs.Sub(web.Assets, ".")
	if err != nil {
		log.Fatalf("embed UI: %v", err)
	}
	handler := httpapi.New(eng, assets, *version)
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	fmt.Printf("WORKBENCH_LISTENING http://%s\n", actualAddr)
	fmt.Printf("ROLLBACK_TARGET %s\n", target)
	errCh := make(chan error, 1)
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-signals:
		log.Printf("shutdown signal %s", sig)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	case err := <-errCh:
		log.Fatalf("serve: %v", err)
	}
}

func requireLoopbackURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("unsupported scheme %q", parsed.Scheme)
	}
	host := parsed.Hostname()
	if host != "127.0.0.1" && host != "localhost" && host != "::1" {
		return fmt.Errorf("host %q is not loopback", host)
	}
	return nil
}
