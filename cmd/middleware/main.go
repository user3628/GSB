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

	"local/configmiddleware/internal/engine"
	"local/configmiddleware/internal/httpapi"
	"local/configmiddleware/internal/pipeline"
	"local/configmiddleware/internal/store"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:18080", "loopback listen address")
	dbPath := flag.String("db", "data/middleware.db", "SQLite database path")
	samplesPath := flag.String("samples", "samples/samples.json", "deterministic samples JSON")
	concurrency := flag.Int("concurrency", 2, "global push concurrency limit")
	queue := flag.Int("queue", 16, "bounded wait queue capacity per round")
	timeout := flag.Duration("timeout", 1500*time.Millisecond, "default per-push timeout")
	budget := flag.Duration("budget", 5*time.Second, "default round time budget")
	flag.Parse()

	doc, err := pipeline.Load(*samplesPath)
	if err != nil {
		log.Fatalf("load samples: %v", err)
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("open sqlite: %v", err)
	}
	defer st.Close()
	eng := engine.New(st, doc, engine.Config{
		Concurrency: *concurrency, QueueCapacity: *queue,
		DefaultTimeout: *timeout, DefaultRoundBudget: *budget,
	})
	recovery, err := eng.RecoverStartup(context.Background())
	if err != nil {
		log.Fatalf("startup recovery: %v", err)
	}
	recoverySummary, _ := json.Marshal(map[string]any{
		"report_id": recovery.ReportID, "scanned_intents": recovery.ScannedIntents,
		"actions": len(recovery.Actions), "restored_clients": recovery.RestoredClients,
	})
	log.Printf("startup recovery: %s", recoverySummary)

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	server := &http.Server{
		Handler:           httpapi.New(eng).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	fmt.Printf("middleware listening on http://%s db=%s concurrency=%d queue=%d\n", listener.Addr(), st.Path(), *concurrency, *queue)
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve: %v", err)
	}
}
