package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"netpolcompiler/internal/api"
	"netpolcompiler/internal/sample"
	"netpolcompiler/internal/service"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18080", "loopback HTTP listen address")
	dataDir := flag.String("data-dir", "/tmp/netpol-compiler-data", "directory for SQLite database and intent log")
	seedSample := flag.Bool("seed-sample", false, "load the fixed deterministic seed sample on first initialization")
	crashPoint := flag.String("crash-point", os.Getenv("NETPOL_CRASH_POINT"), "test crash point, e.g. after_intent")
	rebuildDelay := flag.Duration("rebuild-delay", 0, "optional deterministic delay while a rebuild is pending")
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("prepare data dir %s: %v", *dataDir, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	svc, recoveryReport, err := service.New(context.Background(), service.Config{
		DataDir:      *dataDir,
		CrashPoint:   *crashPoint,
		RebuildDelay: *rebuildDelay,
	})
	if err != nil {
		log.Fatalf("initialize service: %v", err)
	}
	defer func() { _ = svc.Close() }()

	if recoveryReport != nil {
		log.Printf("recovery status=%s lastCompleteVersion=%d unfinished=%d aborted=%d", recoveryReport.Status, recoveryReport.LastCompleteVersion, len(recoveryReport.UnfinishedIntents), len(recoveryReport.AbortedIntentIDs))
	}

	if *seedSample {
		current, err := svc.Store().CurrentVersion(context.Background())
		if err != nil {
			log.Fatalf("read current version before seed: %v", err)
		}
		if current == 0 {
			snapshot := sample.GenerateSnapshot(sample.DefaultSeed)
			result := svc.SubmitSnapshot(context.Background(), snapshot)
			if result.Status >= 400 {
				log.Fatalf("seed sample failed: %s", string(result.Body))
			}
			log.Printf("seeded fixed sample version=%d rules=%d", snapshot.Version, len(snapshot.Rules))
		}
	}

	server := &http.Server{
		Addr:              *addr,
		Handler:           api.NewHandler(svc),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("netpol-compiler listening on http://%s data=%s", *addr, *dataDir)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}
