package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"local/eventbus/internal/bus"
	"local/eventbus/internal/httpapi"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "local HTTP listen address")
	dbPath := flag.String("db", "eventbus.sqlite", "SQLite database file")
	lease := flag.Duration("lease", 5*time.Second, "delivery lease duration")
	flag.Parse()

	ctx := context.Background()
	service, err := bus.Open(ctx, bus.ServiceOptions{DBPath: *dbPath, LeaseDuration: *lease})
	if err != nil {
		fmt.Fprintln(os.Stderr, "open event bus:", err)
		os.Exit(1)
	}
	defer service.Close()

	logger := log.New(os.Stderr, "eventbus ", log.LstdFlags|log.LUTC)
	server := &http.Server{
		Addr:              *addr,
		Handler:           httpapi.NewWithLogger(service, logger),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "listen:", err)
		os.Exit(1)
	}
	logger.Printf("listening on http://%s, database=%s", listener.Addr(), *dbPath)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		fmt.Fprintln(os.Stderr, "serve:", err)
		os.Exit(1)
	}
}
