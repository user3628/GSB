package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gpusched/internal/domain"
	"gpusched/internal/gen"
	"gpusched/internal/httpapi"
	"gpusched/internal/service"
	"gpusched/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "generate":
		runGenerate(os.Args[2:])
	case "serve":
		runServe(os.Args[2:])
	case "recover":
		runRecover(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `gpusched - single-node GPU quota scheduler & batch arbitration

Usage:
  gpusched generate [-out samples] [-seed 20261126]
  gpusched serve    [-db data/gpusched.db] [-addr 127.0.0.1:8080] [-auto-sample]
  gpusched recover  [-db data/gpusched.db]
`)
}

func runGenerate(args []string) {
	fs := flag.NewFlagSet("generate", flag.ExitOnError)
	out := fs.String("out", "samples", "output directory")
	seed := fs.Int64("seed", gen.DefaultSeed, "fixed RNG seed")
	_ = fs.Parse(args)
	path, err := gen.WriteFile(*out, *seed)
	if err != nil {
		log.Fatalf("generate: %v", err)
	}
	fmt.Printf("wrote deterministic sample to %s (seed=%d)\n", path, *seed)
}

func runServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	dbPath := fs.String("db", "data/gpusched.db", "sqlite database path")
	addr := fs.String("addr", "127.0.0.1:8080", "HTTP listen address")
	autoSample := fs.Bool("auto-sample", false, "load the fixed sample on startup if DB is empty")
	_ = fs.Parse(args)

	if err := os.MkdirAll(dirOf(*dbPath), 0o755); err != nil {
		log.Fatalf("mkdir: %v", err)
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer st.Close()

	app := service.New(st)

	// Startup recovery always runs; on a clean DB it is a no-op report.
	report, err := app.Recover(context.Background())
	if err != nil {
		log.Fatalf("recovery: %v", err)
	}
	if len(report.PendingIntents) > 0 || len(report.RunningRounds) > 0 {
		log.Printf("recovery applied: %+v", report)
	}

	if *autoSample {
		loadSampleIfEmpty(app)
	}

	mux := http.NewServeMux()
	httpapi.New(app).Routes(mux)
	srv := &http.Server{Addr: *addr, Handler: withLogging(mux), ReadHeaderTimeout: 5 * time.Second}

	go func() {
		log.Printf("gpusched listening on %s (db=%s)", *addr, *dbPath)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func runRecover(args []string) {
	fs := flag.NewFlagSet("recover", flag.ExitOnError)
	dbPath := fs.String("db", "data/gpusched.db", "sqlite database path")
	_ = fs.Parse(args)

	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer st.Close()
	app := service.New(st)
	report, err := app.Recover(context.Background())
	if err != nil {
		log.Fatalf("recovery: %v", err)
	}
	fmt.Printf("recovery report: %+v\n", report)
}

func loadSampleIfEmpty(app *service.App) {
	reqs, err := app.ListRequests(context.Background())
	if err != nil {
		log.Fatalf("probe requests: %v", err)
	}
	if len(reqs) > 0 {
		return
	}
	spec := gen.Generate(gen.DefaultSeed)
	set, err := domain.Normalize(spec)
	if err != nil {
		log.Fatalf("normalize sample: %v", err)
	}
	if err := app.Load(context.Background(), set); err != nil {
		log.Fatalf("auto load sample: %v", err)
	}
	log.Printf("auto-loaded fixed sample: %d requests", len(set.Requests))
}

func dirOf(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			if i == 0 {
				return "/"
			}
			return p[:i]
		}
	}
	return "."
}
