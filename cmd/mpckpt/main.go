package main

import (
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

	"mpckpt/internal/api"
	"mpckpt/internal/engine"
	"mpckpt/internal/format"
	"mpckpt/internal/store"
	"mpckpt/internal/verify"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		if err := serve(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	case "verify":
		if err := verify.Run(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	case "help", "-h", "--help":
		usage()
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: mpckpt <serve|verify> [flags]")
	fmt.Fprintln(os.Stderr, "  serve   start the loopback HTTP service")
	fmt.Fprintln(os.Stderr, "  verify  run deterministic end-to-end verification")
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	dir := fs.String("dir", "./run", "runtime directory for SQLite and artifacts")
	addr := fs.String("addr", "127.0.0.1:18080", "loopback listen address")
	compatPath := fs.String("compat", "", "optional JSON compatibility matrix override")
	queueCap := fs.Int("queue-cap", 32, "bounded pending write queue capacity")
	reset := fs.Bool("reset", false, "remove the runtime directory before opening SQLite")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := validateLoopback(*addr); err != nil {
		return err
	}
	if *reset {
		if err := os.RemoveAll(*dir); err != nil {
			return fmt.Errorf("reset runtime directory: %w", err)
		}
	}
	matrix, err := format.LoadCompatibilityMatrix(*compatPath)
	if err != nil {
		return fmt.Errorf("load compatibility matrix: %w", err)
	}
	st, err := store.Open(*dir)
	if err != nil {
		return err
	}
	defer st.Close()
	eng, err := engine.New(st, matrix, *queueCap)
	if err != nil {
		return err
	}
	server := &http.Server{
		Addr:              *addr,
		Handler:           api.New(eng),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       35 * time.Second,
		WriteTimeout:      0,
		IdleTimeout:       60 * time.Second,
	}
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *addr, err)
	}
	fmt.Printf("MPCKPT_READY addr=%s db=%s\n", listener.Addr().String(), st.Path)
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(listener)
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	select {
	case sig := <-signals:
		shutdownErr := server.Close()
		if shutdownErr != nil && !errors.Is(shutdownErr, http.ErrServerClosed) {
			return shutdownErr
		}
		fmt.Printf("MPCKPT_STOPPED signal=%s\n", sig)
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func validateLoopback(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid listen address: %w", err)
	}
	if port == "" {
		return fmt.Errorf("listen port is required")
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("HTTP test service must bind a loopback address, got %q", host)
	}
	return nil
}
