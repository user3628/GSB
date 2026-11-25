// Command configtree runs the local configuration convergence pipeline,
// optionally generating deterministic seed samples and serving the HTTP API.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"configtree/internal/engine"
	"configtree/internal/httpapi"
	"configtree/internal/seed"
)

func main() {
	var (
		dbPath     = flag.String("db", "data/configtree.db", "SQLite database path")
		sampleDir  = flag.String("samples", "samples", "directory for deterministic seed samples")
		addr       = flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
		genSeed    = flag.Bool("gen-seed", false, "write deterministic seed samples and exit")
		crashPhase = flag.String("crash-hook", "", "force os.Exit at a pipeline phase (debug)")
	)
	flag.Parse()

	if *genSeed {
		names, err := seed.WriteAll(*sampleDir)
		if err != nil {
			log.Fatalf("seed generation failed: %v", err)
		}
		for _, n := range names {
			fmt.Println("wrote", *sampleDir+"/"+n)
		}
		return
	}

	if err := os.MkdirAll(dbDir(*dbPath), 0o755); err != nil {
		log.Fatalf("create db dir: %v", err)
	}
	eng, err := engine.Open(*dbPath)
	if err != nil {
		log.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	if *crashPhase != "" {
		phase := *crashPhase
		eng.SetCrashHook(func(actual string) bool {
			return actual == phase
		})
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           httpapi.New(eng).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("configtree listening on %s (db=%s)", *addr, *dbPath)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func dbDir(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			if i == 0 {
				return "/"
			}
			return path[:i]
		}
	}
	return "."
}

var _ = json.Marshal
