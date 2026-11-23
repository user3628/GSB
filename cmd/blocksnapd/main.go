package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"blocksnap/internal/httpapi"
	"blocksnap/internal/store"
)

func main() {
	var (
		addr           = flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
		dbPath         = flag.String("db", "blocksnap.db", "SQLite database path")
		faultInjection = flag.Bool("fault-injection", false, "enable the local forced-termination endpoint")
	)
	flag.Parse()

	st, err := store.Open(*dbPath)
	if err != nil {
		log.Printf("open store: %v", err)
		os.Exit(1)
	}
	defer st.Close()

	recovery, err := st.Recover(context.Background())
	if err != nil {
		log.Printf("startup recovery: %v", err)
		os.Exit(1)
	}
	log.Printf("recovery report id=%d status=%s pending_found=%d aborted=%d", recovery.ID, recovery.Status, recovery.PendingFound, recovery.AbortedCount)

	handler := httpapi.New(st, *faultInjection)
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Printf("listen: %v", err)
		os.Exit(1)
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	log.Printf("blocksnapd listening on http://%s (db=%s, fault_injection=%v)", ln.Addr().String(), *dbPath, *faultInjection)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	if err := server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("serve: %v", err)
		os.Exit(1)
	}
}
