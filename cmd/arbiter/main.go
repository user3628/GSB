// Command arbiter runs the local row-lease / versioned-snapshot arbitration
// service. It only binds loopback addresses and stores all state in one local
// SQLite file.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"leasearbiter/internal/arbiter"
	"leasearbiter/internal/clock"
	"leasearbiter/internal/httpapi"
	"leasearbiter/internal/store"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18080", "loopback bind address")
	dbPath := flag.String("db", "data/arbiter.db", "SQLite database file")
	seedPath := flag.String("seed", "sample/seed/rows.json", "deterministic seed rows JSON")
	reaperMs := flag.Int("reaper-ms", 100, "lease expiry sweep period")
	reportPath := flag.String("recovery-report", "", "optional path to write the startup recovery report JSON")
	injectFaults := flag.Bool("enable-fault-injection", false, "mount /debug/fault routes (demo only)")
	flag.Parse()

	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	eng := arbiter.New(arbiter.Config{
		Store:        st,
		Clock:        clock.NewSystem(),
		Sleeper:      clock.NewSystem(),
		StartReaper:  true,
		ReaperPeriod: time.Duration(*reaperMs) * time.Millisecond,
	})
	defer eng.Close()

	report, err := eng.Recover(context.Background())
	if err != nil {
		log.Fatalf("crash recovery: %v", err)
	}
	if report.PreparedIntents > 0 || report.AbortedRequests > 0 || report.OrphanedSnapshots > 0 {
		log.Printf("recovery applied %d intent action(s), aborted %d request(s), orphaned %d snapshot(s)",
			len(report.Actions), report.AbortedRequests, report.OrphanedSnapshots)
	}
	if *reportPath != "" {
		if err := writeJSONFile(*reportPath, report); err != nil {
			log.Fatalf("write recovery report: %v", err)
		}
	}

	if err := seedIfEmpty(eng, *seedPath); err != nil {
		log.Fatalf("seed: %v", err)
	}

	svc := arbiter.NewIdempotentService(eng)
	api := httpapi.NewServer(svc)
	if *injectFaults {
		api.RegisterDebug()
		log.Printf("WARNING: fault injection routes mounted at /debug/fault/*")
	}
	srv := &http.Server{
		Addr:              *addr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	listener, err := httpapi.ListenLoopback(*addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	go func() {
		log.Printf("arbiter listening on http://%s using %s", *addr, *dbPath)
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Printf("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
}

func seedIfEmpty(eng *arbiter.Engine, path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log.Printf("seed file %s absent; skipping deterministic seed", path)
			return nil
		}
		return err
	}
	var rows []arbiter.SeedRow
	if err := json.Unmarshal(b, &rows); err != nil {
		return fmt.Errorf("parse seed: %w", err)
	}
	res, err := eng.Seed(context.Background(), rows)
	if err != nil {
		return err
	}
	log.Printf("seed ready: %d row(s), global commit_ver=%d", len(res.Rows), res.FinalCommitVer)
	return nil
}

func writeJSONFile(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}
