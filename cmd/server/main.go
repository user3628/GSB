package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gatewayworkbench/internal/api"
	"gatewayworkbench/internal/engine"
	"gatewayworkbench/internal/model"
	"gatewayworkbench/internal/scheduler"
	"gatewayworkbench/internal/store"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18080", "HTTP listen address")
	dbPath := flag.String("db", "data/workbench.db", "SQLite database path")
	flag.Int("global", 4, "global upstream concurrency (via WB_GLOBAL)")
	flag.Parse()

	cfg := model.RuntimeConfig{
		GlobalConcurrency: envInt("WB_GLOBAL", 4),
		PerInstanceLimit:  envInt("WB_PER_INSTANCE", 2),
		QueueCapacity:     envInt("WB_QUEUE", 8),
		RoundBudgetMillis: envInt("WB_BUDGET_MS", 3000),
		ProbeConcurrency:  envInt("WB_PROBE_CONCURRENCY", 8),
	}

	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("open sqlite: %v", err)
	}
	defer st.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	report, err := st.RecoverOnStartup(ctx)
	if err != nil {
		log.Fatalf("startup recovery: %v", err)
	}
	if len(report.OpenIntents) > 0 {
		log.Printf("recovery: aborted rounds=%v rolled back requests=%v kept snapshot v%d",
			report.AbortedRounds, report.RolledBackRequests, report.SnapshotKept)
	} else {
		log.Printf("recovery: clean, kept snapshot v%d", report.SnapshotKept)
	}

	eng := engine.New(st, cfg)
	sch := scheduler.New(st, cfg)
	handler := api.New(eng, sch, st)
	server := &http.Server{
		Addr:              *addr,
		Handler:           handler.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("gateway workbench listening on %s db=%s", *addr, *dbPath)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}

func envInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	var v int
	for _, ch := range raw {
		if ch < '0' || ch > '9' {
			return fallback
		}
		v = v*10 + int(ch-'0')
	}
	if v <= 0 {
		return fallback
	}
	return v
}
