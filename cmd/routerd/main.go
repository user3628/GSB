package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"example.com/gsb-router/internal/config"
	"example.com/gsb-router/internal/httpapi"
	"example.com/gsb-router/internal/sample"
	"example.com/gsb-router/internal/store"
)

func main() {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "samples" {
		runSamples(args[1:])
		return
	}
	if len(args) > 0 && args[0] == "serve" {
		args = args[1:]
	}
	runServe(args)
}

func runSamples(args []string) {
	flags := flag.NewFlagSet("samples", flag.ExitOnError)
	out := flags.String("out", "./samples", "sample output directory")
	_ = flags.Parse(args)
	if err := sample.WriteFiles(*out); err != nil {
		log.Fatalf("generate samples: %v", err)
	}
	fmt.Printf("samples written to %s\n", *out)
}

func runServe(args []string) {
	flags := flag.NewFlagSet("serve", flag.ExitOnError)
	configPath := flags.String("config", "config.json", "path to JSON configuration")
	crashEnabled := flags.Bool("crash-enabled", true, "allow E_REBUILD_INTERRUPTED to terminate the process after responding")
	_ = flags.Parse(args)

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(cfg.Database), 0o755); err != nil {
		log.Fatalf("create database directory: %v", err)
	}
	routerStore, err := store.Open(cfg.Database)
	if err != nil {
		log.Fatalf("open router store: %v", err)
	}
	defer routerStore.Close()

	logger := log.New(os.Stderr, "[routerd] ", log.LstdFlags|log.Lmicroseconds)
	api := httpapi.New(routerStore, logger, *crashEnabled)
	server := &http.Server{
		Addr:              cfg.Listen,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		logger.Printf("listening on http://%s database=%s seed=%d", cfg.Listen, cfg.Database, cfg.Seed)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatalf("http server: %v", err)
		}
	}()
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpapi.Shutdown(ctx, server); err != nil {
		logger.Printf("shutdown: %v", err)
	}
}
