package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
	dbPath := flag.String("db", "data/workbench.db", "SQLite database path")
	coordinatorURL := flag.String("coordinator", "", "local coordinator base URL (default derived from -addr)")
	leaseTTL := flag.Duration("lease-ttl", 2500*time.Millisecond, "consumer lease idle timeout")
	seedPath := flag.String("seed", "", "optional deterministic sample JSON to load")
	allowCrash := flag.Bool("allow-crash", false, "enable the process-termination recovery demo endpoint")
	globalConcurrency := flag.Int("global-concurrency", 2, "global coordinator/task concurrency")
	perPartitionLease := flag.Int("per-partition-lease-limit", 1, "in-flight lease tasks per partition")
	queueCapacity := flag.Int("queue-capacity", 32, "bounded scheduler queue capacity")
	roundBudgetMS := flag.Int("round-budget-ms", 250, "scheduler round dispatch budget")
	queryTimeoutMS := flag.Int("query-timeout-ms", 150, "coordinator query timeout")
	reaperInterval := flag.Duration("lease-reaper-interval", 100*time.Millisecond, "expired lease reaper interval")
	flag.Parse()

	if *coordinatorURL == "" {
		*coordinatorURL = localCoordinatorURL(*addr)
	}
	db, err := openDB(*dbPath)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	service := NewService(db, *coordinatorURL, *leaseTTL)
	defer service.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	recovery, err := service.RecoverIntents(ctx)
	if err != nil {
		log.Fatalf("recover prepared intents: %v", err)
	}
	log.Printf("sqlite=%s coordinator=%s recovered_intents=%d", *dbPath, *coordinatorURL, recovery.RecoveredIntents)

	if *seedPath != "" {
		spec, err := loadSampleFile(*seedPath)
		if err != nil {
			log.Fatalf("load sample: %v", err)
		}
		if err := service.SeedSample(ctx, spec); err != nil {
			log.Fatalf("seed sample: %v", err)
		}
		log.Printf("seeded sample %s (%d topics, %d messages)", *seedPath, len(spec.Topics), len(spec.Messages))
	}

	cfg := SchedulerConfig{GlobalConcurrency: *globalConcurrency,
		PerPartitionLeaseLimit: *perPartitionLease, QueueCapacity: *queueCapacity,
		RoundBudgetMS: *roundBudgetMS, QueryTimeoutMS: *queryTimeoutMS, MinDispatchMS: 2}
	scheduler := NewScheduler(service, cfg)
	service.StartLeaseReaper(ctx, *reaperInterval)

	app := NewApp(service, scheduler, *seedPath, *allowCrash)
	server := &http.Server{Addr: *addr, Handler: app.Routes(), ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout: 60 * time.Second}
	go func() {
		log.Printf("workbench listening on http://%s (go=%s)", *addr, runtime.Version())
		log.Printf("dashboard: http://%s/  coordinator: %s/test/txn/{txn_id}", *addr, *coordinatorURL)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	log.Printf("workbench stopped")
}

func localCoordinatorURL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "http://127.0.0.1:8080"
	}
	if host == "" || host == "0.0.0.0" || host == "::" || host == "[::]" {
		host = "127.0.0.1"
	}
	return fmt.Sprintf("http://%s:%s", host, port)
}
