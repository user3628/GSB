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

	"nonceguard/internal/engine"
	"nonceguard/internal/httpapi"
	"nonceguard/internal/store"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18090", "HTTP API/UI listen address")
	dbPath := flag.String("db", "data/nonceguard.db", "SQLite database path")
	peerAddr := flag.String("peer-addr", "127.0.0.1:18091", "default loopback proxy peer address")
	fixedRef := flag.Int64("fixed-reference-ms", 0, "fixed deterministic clock in ms; default 0 uses system clock (demo script supplies a fixed value)")
	flag.Parse()

	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("open sqlite: %v", err)
	}
	defer st.Close()

	eng := engine.New(st)
	if *fixedRef > 0 {
		ref := *fixedRef
		eng.Clock = func() int64 { return ref }
	}
	ctx := context.Background()
	report, err := st.Recover(ctx, eng.Clock())
	if err != nil {
		log.Fatalf("recovery scan: %v", err)
	}
	if report.OpenIntentsFound > 0 || report.AbortedBatches > 0 {
		log.Printf("recovery: open_intents=%d aborted_batches=%d facts=%v", report.OpenIntentsFound, report.AbortedBatches, report.Facts)
	}
	if _, err := eng.EnsureDefaultConfig(ctx, eng.Clock()); err != nil {
		log.Fatalf("default config: %v", err)
	}

	api := httpapi.NewServer(eng, *peerAddr)
	httpSrv := &http.Server{Addr: *addr, Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("NonceGuard UI/API: http://%s", *addr)
		log.Printf("default loopback peer parameter: %s (standalone: go run ./cmd/peer -addr %s)", *peerAddr, *peerAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
}
