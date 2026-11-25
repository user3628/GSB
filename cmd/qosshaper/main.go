package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"qosshaper/internal/httpapi"
	"qosshaper/internal/qos"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18080", "HTTP listen address; port 0 selects an available port")
	workdir := flag.String("workdir", ".", "workspace directory; SQLite and samples are stored beneath data/")
	seed := flag.Int64("seed", qos.DefaultSampleSeed, "fixed deterministic sample seed")
	flag.Parse()

	root, err := filepath.Abs(*workdir)
	if err != nil {
		log.Fatalf("resolve workdir: %v", err)
	}
	dataDir := filepath.Join(root, "data")
	meta, err := qos.EnsureSamples(dataDir, *seed)
	if err != nil {
		log.Fatalf("generate deterministic samples: %v", err)
	}
	store, err := qos.OpenStore(qos.StoreOptions{
		DBPath:         filepath.Join(dataDir, "qosshaper.db"),
		DataDir:        dataDir,
		SampleSeed:     *seed,
		FailpointStage: os.Getenv("QOS_FAILPOINT_STAGE"),
		FailpointFile:  os.Getenv("QOS_FAILPOINT_FILE"),
		FailpointSleep: durationFromEnv("QOS_FAILPOINT_SLEEP_MS", 20*time.Second),
	})
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer store.Close()

	logger := log.New(os.Stderr, "qosshaper ", log.LstdFlags|log.Lmicroseconds)
	api := httpapi.New(store, logger)
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		logger.Printf("listen %s failed (%v); selecting an available loopback port", *addr, err)
		ln, err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			log.Fatalf("listen: %v", err)
		}
	}
	actual := ln.Addr().String()
	server := &http.Server{Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	fmt.Printf("LISTEN=http://%s\n", actual)
	fmt.Printf("DB=%s\n", filepath.Join(dataDir, "qosshaper.db"))
	fmt.Printf("DATA_DIR=%s\n", dataDir)
	fmt.Printf("SAMPLE_SEED=%d\n", meta.Seed)
	fmt.Printf("SAMPLE_POLICY=%s\n", meta.PolicyFile)
	fmt.Printf("SAMPLE_TRAFFIC=%s\n", meta.TrafficFile)
	logger.Printf("listening on http://%s current_version=%d", actual, mustVersion(store))
	logger.Printf("deterministic sample seed=%d packets=%d policy_sha256=%s traffic_sha256=%s",
		meta.Seed, meta.TrafficPackets, meta.PolicySHA256, meta.TrafficSHA256)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
	if err := server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatalf("serve: %v", err)
	}
}

func durationFromEnv(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	ms, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || ms < 0 {
		return fallback
	}
	return time.Duration(ms) * time.Millisecond
}

func mustVersion(store *qos.Store) int64 {
	v, err := store.CurrentVersion(context.Background())
	if err != nil {
		return -1
	}
	return v
}
