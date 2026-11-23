package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"time"

	"example.com/logicplan/editor/internal/api"
	"example.com/logicplan/editor/internal/engine"
	"example.com/logicplan/editor/internal/fault"
)

func main() {
	addr := flag.String("addr", envOr("EDITOR_ADDR", "127.0.0.1:8080"), "HTTP listen address")
	dbPath := flag.String("db", envOr("EDITOR_DB", "data/editor.db"), "SQLite database path")
	crashAt := flag.String("crash-at", os.Getenv("EDITOR_CRASH_AT"), "fault checkpoint stage")
	crashMode := flag.String("crash-mode", envOr("EDITOR_CRASH_MODE", "crash"), "crash or pause")
	pauseMS := flag.Int64("pause-ms", envInt64("EDITOR_PAUSE_MS", 30000), "pause duration in milliseconds")
	skip := flag.Int("crash-skip", envInt("EDITOR_CRASH_SKIP", 0), "checkpoint hits to skip before firing")
	flag.Parse()

	controller := fault.New()
	if *crashAt != "" {
		if err := controller.Arm(*crashAt, *crashMode, *skip, time.Duration(*pauseMS)*time.Millisecond); err != nil {
			log.Fatalf("arm fault: %v", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	editor, err := engine.Open(ctx, *dbPath, controller)
	if err != nil {
		writeFatal(err)
	}
	defer editor.Close()

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		writeFatal(err)
	}
	server := &http.Server{
		Handler:           api.New(editor).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       70 * time.Second,
		WriteTimeout:      70 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	event := map[string]any{"event": "listening", "addr": listener.Addr().String(), "db": *dbPath, "recovery": editor.Recovery()}
	encoded, _ := json.Marshal(event)
	log.Print(string(encoded))

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, terminationSignals()...)
	done := make(chan struct{})
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Printf("http server stopped: %v", err)
		}
		close(done)
	}()
	select {
	case sig := <-signals:
		log.Printf("received %s; shutting down", sig)
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
		<-done
	case <-done:
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt64(key string, fallback int64) int64 {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			return parsed
		}
	}
	return fallback
}

func envInt(key string, fallback int) int {
	return int(envInt64(key, int64(fallback)))
}

func writeFatal(err error) {
	payload, _ := json.Marshal(map[string]any{"level": "fatal", "error": err.Error()})
	fmt.Fprintln(os.Stderr, string(payload))
	os.Exit(1)
}
