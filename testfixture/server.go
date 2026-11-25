// Package testfixture runs deterministic loopback HTTP/TCP endpoints used by
// tests and the interactive diagnostic workbench. It never reaches the network
// beyond 127.0.0.1.
package testfixture

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"time"
)

const (
	BodyOK    = "SERVICE_OK"
	BodyWrong = "WRONG_MARKER"
	SlowDelay = 5 * time.Second
)

// Fixture owns one HTTP/TCP listener and one known-refused loopback address.
type Fixture struct {
	Server       *httptest.Server
	BaseURL      string
	TCPAddr      string
	RefusedAddr  string
	successCount atomic.Int64
	activeSlow   atomic.Int64
	maxSlow      atomic.Int64
}

// Start binds ephemeral loopback ports. The dynamic port is intentionally not
// part of sample determinism; fixed-seed fields (weights, TTLs, probes, order)
// are stable for identical options.
func Start() (*Fixture, error) {
	f := &Fixture{}
	mux := http.NewServeMux()
	mux.HandleFunc("/probe/ok", func(w http.ResponseWriter, r *http.Request) {
		f.successCount.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(BodyOK))
	})
	mux.HandleFunc("/probe/bad-status", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(BodyOK))
	})
	mux.HandleFunc("/probe/bad-marker", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(BodyWrong))
	})
	mux.HandleFunc("/probe/slow", func(w http.ResponseWriter, r *http.Request) {
		active := f.activeSlow.Add(1)
		for {
			maximum := f.maxSlow.Load()
			if active <= maximum || f.maxSlow.CompareAndSwap(maximum, active) {
				break
			}
		}
		defer f.activeSlow.Add(-1)
		timer := time.NewTimer(SlowDelay)
		defer timer.Stop()
		select {
		case <-timer.C:
			_, _ = w.Write([]byte(BodyOK))
		case <-r.Context().Done():
		}
	})
	mux.HandleFunc("/probe/flap", func(w http.ResponseWriter, r *http.Request) {
		if f.successCount.Add(1)%2 == 1 {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(BodyOK))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(BodyOK))
	})
	mux.HandleFunc("/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"probe":"heartbeat"}`))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	})
	f.Server = httptest.NewServer(mux)
	f.BaseURL = f.Server.URL
	f.TCPAddr = strings.TrimPrefix(f.Server.URL, "http://")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		f.Server.Close()
		return nil, err
	}
	f.RefusedAddr = ln.Addr().String()
	if err := ln.Close(); err != nil {
		f.Server.Close()
		return nil, err
	}
	return f, nil
}

// Close releases the loopback listener.
func (f *Fixture) Close() {
	if f.Server != nil {
		f.Server.CloseClientConnections()
		f.Server.Close()
	}
}

// URL appends a path to the fixture base URL.
func (f *Fixture) URL(path string) string {
	return strings.TrimRight(f.BaseURL, "/") + "/" + strings.TrimLeft(path, "/")
}

// WaitSlowProbe can be used by tests to confirm server cancellation happened.
func (f *Fixture) WaitSlowProbe(ctx context.Context) error {
	t := time.NewTimer(100 * time.Millisecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return fmt.Errorf("slow probe fixture remains active")
	}
}

// SlowActive reports HTTP slow-probe handlers currently holding a connection.
func (f *Fixture) SlowActive() int64 { return f.activeSlow.Load() }

// SlowMax reports the maximum concurrently active slow-probe handlers.
func (f *Fixture) SlowMax() int64 { return f.maxSlow.Load() }

// ResetSlowStats clears slow-handler concurrency observations.
func (f *Fixture) ResetSlowStats() {
	f.maxSlow.Store(f.activeSlow.Load())
}
