package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"local/windowengine/internal/engine"
	"local/windowengine/internal/httpapi"
	"local/windowengine/internal/store"
)

func main() {
	databasePath := flag.String("db", "artifacts/window-engine.db", "SQLite database path")
	address := flag.String("addr", "127.0.0.1:18099", "HTTP listen address")
	hardGlobal := flag.Int("max-global-concurrency", 256, "engine-wide hard cap on request global concurrency")
	flag.Parse()

	database, err := store.Open(*databasePath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer database.Close()

	appEngine := engine.New(database, *hardGlobal)
	report, err := appEngine.Recover(context.Background())
	if err != nil {
		log.Fatalf("startup recovery: %v", err)
	}
	reportJSON, _ := json.MarshalIndent(report, "", "  ")
	log.Printf("startup recovery report:\n%s", reportJSON)

	handler := httpapi.New(appEngine, database, report.ReportID)
	server := &http.Server{
		Addr:              *address,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       90 * time.Second,
	}

	shutdownContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-shutdownContext.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	fmt.Printf("window engine listening on http://%s (sqlite=%s, recovery=%s)\n", *address, *databasePath, report.ReportID)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http server: %v", err)
	}
}
