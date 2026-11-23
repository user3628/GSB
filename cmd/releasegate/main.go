package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"releasegate/httpapi"
	"releasegate/service"
	"releasegate/store"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:18181", "local HTTP listen address")
	dbPath := flag.String("db", "releasegate.db", "SQLite database path")
	flag.Parse()

	logger := log.New(os.Stdout, "releasegate ", log.LstdFlags|log.LUTC)
	st, err := store.Open(*dbPath)
	if err != nil {
		logger.Fatalf("open store: %v", err)
	}
	defer st.Close()

	ctx := context.Background()
	recovered, err := st.Recover(ctx)
	if err != nil {
		logger.Fatalf("startup recovery scan: %v", err)
	}
	logger.Printf("recovery scan report_id=%d pending=%d recovered=%d orphan_batches_aborted=%d versions_restored=%d",
		recovered.ReportID, recovered.PendingIntentsFound, recovered.IntentsRecovered,
		recovered.OrphanBatchesAborted, recovered.VersionsRestored)

	svc := service.New(st)
	opts := httpapi.Options{}
	if os.Getenv("RELEASEGATE_ENABLE_FAULT_INJECTION") == "1" {
		opts.EnableFaultInjection = true
		opts.FaultHook = func() {
			logger.Printf("intentional forced termination after durable intent (exit 70)")
			os.Exit(70)
		}
	}
	api := httpapi.New(svc, opts)
	server := &http.Server{
		Addr:              *listen,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	stop, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	go func() {
		<-stop.Done()
		logger.Printf("shutdown requested")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	logger.Printf("listening on http://%s db=%s", *listen, *dbPath)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatalf("HTTP server: %v", err)
	}
	fmt.Println("releasegate stopped")
}
