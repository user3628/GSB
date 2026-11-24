package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"columnar-executor/internal/config"
	"columnar-executor/internal/httpapi"
	"columnar-executor/internal/model"
	"columnar-executor/internal/service"
	"columnar-executor/internal/store"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8080", "local loopback listen address")
	dbPath := flag.String("db", "data/columnar.db", "SQLite database file")
	autoSeed := flag.Bool("auto-seed", true, "generate the built-in deterministic snapshot when the database has none")
	seed := flag.Uint64("seed", config.DefaultSeed, "deterministic sample seed")
	rows := flag.Int("rows", config.DefaultRows, "deterministic sample row count")
	allowCrash := flag.Bool("allow-crash", os.Getenv("COLUMNAR_ALLOW_CRASH") == "1", "enable demo-only abrupt process termination after a batch")
	flag.Parse()

	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	options := service.Options{}
	if *allowCrash {
		options.CrashHook = func(executionID string, batchNo int) {
			log.Printf("FORCED TERMINATION requested after execution=%s batch=%d", executionID, batchNo)
			os.Exit(91)
		}
	}
	svc, err := service.New(st, options)
	if err != nil {
		log.Fatalf("initialize service: %v", err)
	}
	for _, report := range svc.StartupRecoveryReports() {
		log.Printf("recovery: execution=%s previous=%s recovered=%s rolled_back_batches=%v discarded_aggregates=%v",
			report.ExecutionID, report.PreviousStatus, report.RecoveredStatus, report.RolledBackBatches, report.DiscardedAggregates)
	}

	if *autoSeed {
		if _, err := svc.CurrentSnapshot(context.Background()); err != nil {
			if biz, ok := model.AsBizError(err); ok && biz.Code == model.CodeNotFound {
				snap, err := svc.GenerateSnapshot(context.Background(), model.SnapshotGenerateRequest{
					Version: config.DefaultSnapshotVersion,
					Rows:    *rows,
					Seed:    *seed,
				})
				if err != nil {
					log.Fatalf("auto seed snapshot: %v", err)
				}
				log.Printf("auto seeded snapshot version=%d rows=%d sha256=%s", snap.Version, snap.RowCount, snap.SHA256Digest)
			} else {
				log.Fatalf("read current snapshot: %v", err)
			}
		}
	}

	httpServer := &http.Server{
		Addr:              *listen,
		Handler:           httpapi.ErrorHandler(httpapi.New(svc)),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      120 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	log.Printf("columnar executor listening on http://%s db=%s allow_crash=%v", *listen, st.DBPath(), *allowCrash)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "http server: %v\n", err)
		os.Exit(1)
	}
}
