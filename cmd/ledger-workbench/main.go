package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	ledger "ledgerworkbench"
)

func main() {
	dbPath := flag.String("db", "/tmp/ledger-workbench/ledger.db", "SQLite database path")
	addr := flag.String("addr", "127.0.0.1:18081", "HTTP listen address")
	portFile := flag.String("port-file", "", "optional file receiving the actual base URL")
	tolerance := flag.String("tolerance", "0.01", "default conservation tolerance")
	probeConcurrency := flag.Int("probe-concurrency", 4, "maximum concurrent probes per workbench")
	probeTimeout := flag.Duration("probe-timeout", time.Second, "default probe timeout")
	flag.Parse()

	workbench, err := ledger.NewWorkbench(ledger.Config{
		DBPath: *dbPath, DefaultTolerance: *tolerance,
		ProbeConcurrency: *probeConcurrency, ProbeTimeout: *probeTimeout,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "open workbench: %v\n", err)
		os.Exit(1)
	}
	defer workbench.Close()

	recovery := workbench.RecoveryReport()
	if recovery != nil && len(recovery.UnfinishedIntents) > 0 {
		fmt.Fprintf(os.Stderr, "recovery: %d unfinished intents, %d staged entries discarded, %d migrations pending\n",
			len(recovery.UnfinishedIntents), recovery.DiscardedStagedEntries, len(recovery.RecoveredMigrationIDs))
	}

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen %s: %v\n", *addr, err)
		os.Exit(1)
	}
	baseURL := "http://" + listener.Addr().String()
	if *portFile != "" {
		if err := os.WriteFile(*portFile, []byte(baseURL+"\n"), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write port file: %v\n", err)
			os.Exit(1)
		}
	}
	fmt.Println(baseURL)

	server := &http.Server{
		Handler:           ledger.NewHTTPServer(workbench).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}
