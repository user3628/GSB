package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	grayrouter "grayrouter"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18080", "loopback HTTP listen address")
	dbPath := flag.String("db", "data/grayrouter.db", "SQLite database path")
	samplesDir := flag.String("samples-dir", "samples", "deterministic sample output directory")
	generateSamples := flag.Bool("generate-samples", true, "write deterministic sample JSON files")
	loadBaseline := flag.Bool("load-baseline", true, "load fixed baseline snapshot when database head is zero")
	faultPoint := flag.String("fault-point", "", "testing only: block at after_intent, after_stage, or after_binding")
	readyFile := flag.String("ready-file", "", "testing only: write checkpoint then block")
	flag.Parse()

	if err := ensureLoopback(*addr); err != nil {
		log.Fatal(err)
	}
	if *generateSamples {
		if err := grayrouter.WriteSampleFiles(*samplesDir); err != nil {
			log.Fatalf("write samples: %v", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	host, err := grayrouter.Open(ctx, grayrouter.Config{DBPath: *dbPath, CrashPoint: *faultPoint, ReadyFile: *readyFile})
	if err != nil {
		log.Fatalf("open grayrouter: %v", err)
	}
	defer host.Close()

	if *loadBaseline {
		state, err := host.State(ctx)
		if err != nil {
			log.Fatalf("read state: %v", err)
		}
		if state.HeadVersion == 0 {
			bundle := grayrouter.SampleBundleFixed()
			if _, err := host.SubmitSnapshot(ctx, grayrouter.CommitSnapshotInput{RequestID: "sample-baseline-v1", Snapshot: bundle.Baseline}); err != nil {
				log.Fatalf("load baseline: %v", err)
			}
			if _, err := host.SubmitDelta(ctx, grayrouter.CommitDeltaInput{RequestID: "sample-delta-v2-base1", Delta: bundle.Delta}); err != nil {
				log.Printf("baseline loaded; sample delta was not auto-applied: %v", err)
			}
		}
	}

	server := &http.Server{
		Addr:              *addr,
		Handler:           grayrouter.NewHTTPServer(host, *samplesDir).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Printf("GrayRouter diagnostic workbench: http://%s/", *addr)
	log.Printf("SQLite database: %s", *dbPath)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("http server: %v", err)
	}
}

func ensureLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid --addr %q: %w", addr, err)
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("refusing to listen on non-loopback address %q; use 127.0.0.1:port or [::1]:port", addr)
	}
	return nil
}
