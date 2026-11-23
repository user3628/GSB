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
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type stats struct {
	active      int64
	completed   int64
	cancelled   int64
	connections int64
	mu          sync.Mutex
	modes       map[string]int64
}

func main() {
	addr := flag.String("addr", "127.0.0.1:19090", "test endpoint listen address")
	flag.Parse()

	state := &stats{modes: map[string]int64{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", state.handle)
	mux.HandleFunc("/recover", state.handle)
	mux.HandleFunc("/stats", state.handleStats)
	mux.HandleFunc("/reset", state.handleReset)

	server := &http.Server{
		Addr:              *addr,
		Handler:           withCORS(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ConnState: func(_ net.Conn, connState http.ConnState) {
			if connState == http.StateNew {
				atomic.AddInt64(&state.connections, 1)
			}
		},
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
	log.Printf("local probe endpoint listening on http://%s", *addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve: %v", err)
	}
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Access-Control-Allow-Origin", "*")
		writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		writer.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if request.Method == http.MethodOptions {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(writer, request)
	})
}

func (s *stats) handle(writer http.ResponseWriter, request *http.Request) {
	atomic.AddInt64(&s.active, 1)
	defer atomic.AddInt64(&s.active, -1)
	mode := request.URL.Query().Get("mode")
	if mode == "" {
		mode = "ok"
	}
	s.mu.Lock()
	s.modes[mode]++
	s.mu.Unlock()
	writer.Header().Set("Connection", "close")
	switch mode {
	case "timeout":
		select {
		case <-time.After(5 * time.Second):
			http.Error(writer, `{"error":"test endpoint timeout elapsed"}`+"\n", http.StatusGatewayTimeout)
			atomic.AddInt64(&s.completed, 1)
		case <-request.Context().Done():
			atomic.AddInt64(&s.cancelled, 1)
		}
	case "reject":
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusServiceUnavailable)
		_, _ = writer.Write([]byte(`{"error":"business rejection from local test endpoint"}` + "\n"))
		atomic.AddInt64(&s.completed, 1)
	default:
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
		payload, _ := json.Marshal(map[string]any{
			"status": "ok", "mode": mode, "path": request.URL.Path,
			"component": request.URL.Query().Get("component"), "method": request.Method,
			"at": time.Now().UTC().Format(time.RFC3339Nano),
		})
		_, _ = writer.Write(append(payload, '\n'))
		atomic.AddInt64(&s.completed, 1)
	}
}

func (s *stats) handleStats(writer http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	modes := make(map[string]int64, len(s.modes))
	for key, value := range s.modes {
		modes[key] = value
	}
	s.mu.Unlock()
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(map[string]any{
		"active": atomic.LoadInt64(&s.active), "completed": atomic.LoadInt64(&s.completed),
		"cancelled": atomic.LoadInt64(&s.cancelled), "connections": atomic.LoadInt64(&s.connections),
		"modes": modes,
	})
}

func (s *stats) handleReset(writer http.ResponseWriter, _ *http.Request) {
	atomic.StoreInt64(&s.active, 0)
	atomic.StoreInt64(&s.completed, 0)
	atomic.StoreInt64(&s.cancelled, 0)
	atomic.StoreInt64(&s.connections, 0)
	s.mu.Lock()
	s.modes = map[string]int64{}
	s.mu.Unlock()
	fmt.Fprintln(writer, `{"reset":true}`)
}
