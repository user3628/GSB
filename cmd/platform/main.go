package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"elastic-scale/internal/app"
	"elastic-scale/internal/seed"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "generate":
		err = runGenerate(os.Args[2:])
	case "smoke":
		err = runSmoke(os.Args[2:])
	case "serve":
		err = runServe(os.Args[2:])
	case "demo":
		err = runDemo(os.Args[2:])
	case "arm-crash":
		err = runArmCrash(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Println(`elastic-scale platform

commands:
  generate [--out artifacts/seed.json]
  smoke    [--db artifacts/smoke.db]
  serve    [--addr 127.0.0.1:18080] [--db data/platform.db]
  demo     [--base-url http://127.0.0.1:18080] [--seed artifacts/seed.json]
  arm-crash [--base-url http://127.0.0.1:18080] [--seed artifacts/seed.json]`)
}

func runGenerate(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	out := fs.String("out", "artifacts/seed.json", "output seed JSON path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	bundle := seed.Generate()
	if *out == "" || *out == "-" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(bundle)
	}
	if dir := filepath.Dir(*out); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	b, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(*out, b, 0o644); err != nil {
		return err
	}
	fmt.Printf("generated fixed-seed=%d services=%d samples=%d digest=%s -> %s\n",
		bundle.Seed, len(bundle.Catalog.Services), len(bundle.Snapshot.Samples), bundle.Snapshot.Digest, *out)
	return nil
}

func runSmoke(args []string) error {
	fs := flag.NewFlagSet("smoke", flag.ContinueOnError)
	dbPath := fs.String("db", "artifacts/smoke.db", "sqlite path for writable smoke check")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(*dbPath), 0o755); err != nil {
		return fmt.Errorf("workdir/db directory not writable: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(*dbPath), "write-check-*")
	if err != nil {
		return fmt.Errorf("working directory not writable: %w", err)
	}
	tmpName := tmp.Name()
	_, _ = tmp.WriteString("ok")
	_ = tmp.Close()
	_ = os.Remove(tmpName)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("loopback port unavailable: %w", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()

	a, err := app.New(context.Background(), *dbPath)
	if err != nil {
		return fmt.Errorf("sqlite open/migration failed: %w", err)
	}
	defer a.Close()
	health := a.Health(context.Background())
	if !health.DBWritable {
		return errors.New("sqlite health check failed")
	}
	bundle := seed.Generate()
	fmt.Printf("smoke ok: workdir writable; sqlite=%s; free_loopback_port=%d; generated_samples=%d; digest=%s\n",
		*dbPath, port, len(bundle.Snapshot.Samples), bundle.Snapshot.Digest)
	return nil
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:18080", "listen address")
	dbPath := fs.String("db", "data/platform.db", "sqlite database path")
	portFile := fs.String("port-file", "", "optional file to receive the actual loopback port")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(*dbPath), 0o755); err != nil {
		return err
	}
	a, err := app.New(context.Background(), *dbPath)
	if err != nil {
		return err
	}
	defer a.Close()
	srv := newHTTPServer(a)
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", *addr, err)
	}
	actualAddr := "http://" + listener.Addr().String()
	if *portFile != "" {
		if err := os.WriteFile(*portFile, []byte(strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)), 0o644); err != nil {
			return err
		}
	}
	httpServer := &http.Server{
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	fmt.Printf("elastic-scale listening on %s db=%s\n", actualAddr, *dbPath)
	if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
