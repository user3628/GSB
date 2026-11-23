package app

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"local/objectstore/internal/engine"
	"local/objectstore/internal/httpapi"
	"local/objectstore/internal/store"
)

func Run() {
	var (
		address        = flag.String("addr", "127.0.0.1:8080", "local HTTP listen address")
		databasePath   = flag.String("db", "objectstore.db", "SQLite database file")
		verifyWorkers  = flag.Int("verify-concurrency", 4, "maximum concurrent part digest checks")
		verifyQueue    = flag.Int("verify-queue", 32, "bounded waiting queue for part digest checks")
		verifyBudgetMS = flag.Int64("verify-budget-ms", 2000, "per-merge verification time budget; <= 0 skips all checks and reports all parts as skipped")
		reapInterval   = flag.Duration("reap-interval", time.Second, "expired-session reclamation interval")
	)
	flag.Parse()

	logger := log.New(os.Stdout, "objectstore ", log.LstdFlags|log.Lmicroseconds)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	database, err := store.Open(ctx, *databasePath)
	if err != nil {
		logger.Fatalf("open database: %v", err)
	}
	defer database.Close()

	budget := time.Duration(*verifyBudgetMS) * time.Millisecond
	service := engine.New(database, engine.Options{
		VerifyConcurrency:      *verifyWorkers,
		VerifyQueueCapacity:    *verifyQueue,
		VerifyQueueCapacitySet: true,
		VerifyBudget:           budget,
		VerifyBudgetSet:        true,
	})
	defer service.Close()

	report, recoveryErr := service.Recover(ctx)
	if recoveryErr != nil {
		logger.Fatalf("startup recovery: %v", recoveryErr)
	}
	logger.Printf("startup recovery report=%d interrupted_intents=%d interrupted_requests=%d checks_passed=%t",
		report.ReportID, len(report.InterruptedIntents), report.InterruptedRequests, report.Checks.Passed)

	if *reapInterval > 0 {
		go func() {
			ticker := time.NewTicker(*reapInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					reclaimed, reclaimErr := service.ReclaimExpired(ctx)
					if reclaimErr != nil {
						logger.Printf("expired-session reclamation failed: %v", reclaimErr)
						continue
					}
					if len(reclaimed) > 0 {
						logger.Printf("reclaimed %d expired upload session(s)", len(reclaimed))
					}
				}
			}
		}()
	}

	api := httpapi.New(service, httpapi.Config{Logger: logger})
	server := &http.Server{
		Addr:              *address,
		Handler:           api,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       2 * time.Minute,
		WriteTimeout:      2 * time.Minute,
		IdleTimeout:       2 * time.Minute,
	}
	go func() {
		logger.Printf("listening on http://%s (db=%s)", *address, *databasePath)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Printf("HTTP server failed: %v", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Printf("HTTP shutdown: %v", err)
	}
	logger.Printf("shutdown complete")
}
