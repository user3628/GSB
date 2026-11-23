package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"meshmigrate/internal/compat"
	"meshmigrate/internal/engine"
	"meshmigrate/internal/httpapi"
	"meshmigrate/internal/samples"
	"meshmigrate/internal/store"
	"meshmigrate/internal/verify"
)

const version = "0.1.0"

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = runServe(os.Args[2:])
	case "samples":
		err = runSamples(os.Args[2:])
	case "verify":
		err = runVerify(os.Args[2:])
	case "version", "-version", "--version":
		fmt.Println("meshpolicy " + version)
		return
	case "help", "-h", "--help":
		usage()
		return
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		log.Printf("ERROR: %v", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `meshpolicy - local service-mesh policy migration and compatibility pipeline

Usage:
  meshpolicy serve   [-addr 127.0.0.1:18080] [-db data/meshpolicy.db]
  meshpolicy samples [-out samples]
  meshpolicy verify  [-base-url http://127.0.0.1:18080] [-samples samples]
  meshpolicy version

All HTTP listeners must bind to a loopback address. The sample and verification
commands use only local files and localhost HTTP endpoints.`)
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:18080", "loopback listen address")
	dbPath := fs.String("db", "data/meshpolicy.db", "SQLite database path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := requireLoopback(*addr); err != nil {
		return err
	}
	matrix, err := compat.LoadMatrix()
	if err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	hooks := os.Getenv("MESHPOLICY_ENABLE_TEST_HOOKS") == "1"
	pauseSeconds := 60
	if raw := os.Getenv("MESHPOLICY_TEST_PAUSE_SECONDS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return fmt.Errorf("MESHPOLICY_TEST_PAUSE_SECONDS must be a positive integer")
		}
		pauseSeconds = n
	}
	marker := os.Getenv("MESHPOLICY_TEST_MARKER")
	eng := engine.New(st, matrix, engine.Options{
		EnableTestHooks: hooks,
		MarkerPath:      marker,
		PauseDuration:   time.Duration(pauseSeconds) * time.Second,
	})
	recovery, err := eng.Recover(context.Background())
	if err != nil {
		return fmt.Errorf("startup recovery: %w", err)
	}
	if recovery != nil {
		b, _ := json.Marshal(recovery.Report)
		log.Printf("startup recovery report: %s", string(b))
	}
	server := &http.Server{
		Addr:              *addr,
		Handler:           httpapi.New(eng, st, hooks).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      90 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	log.Printf("meshpolicy %s listening on http://%s db=%s testHooks=%v", version, *addr, st.Path(), hooks)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func runSamples(args []string) error {
	fs := flag.NewFlagSet("samples", flag.ContinueOnError)
	out := fs.String("out", "samples", "sample output directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	m, err := samples.Generate(*out)
	if err != nil {
		return err
	}
	b, _ := json.MarshalIndent(m, "", "  ")
	fmt.Printf("generated fixed samples in %s\n%s\n", *out, string(b))
	return nil
}

func runVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	baseURL := fs.String("base-url", "http://127.0.0.1:18080", "local loopback server base URL")
	dir := fs.String("samples", "samples", "fixed sample directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	sum, err := verify.Run(*baseURL, *dir)
	fmt.Println(sum)
	return err
}

func requireLoopback(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid listen address %q: %w", addr, err)
	}
	if host == "" || port == "" {
		return fmt.Errorf("listen address must include an explicit loopback host and port")
	}
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("listen address %q is not loopback; external listeners are disabled", addr)
	}
	return nil
}
