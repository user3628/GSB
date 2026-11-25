package loopback

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"leasearbiter/internal/core"
)

const (
	HeaderInject  = "X-Lease-Inject"
	HeaderDelayMS = "X-Lease-Delay-Ms"
)

type RequestEvent struct {
	Path       string
	Mode       string
	Remote     string
	At         time.Time
	Closed     bool
	CloseAfter time.Duration
}

type HandlerConfig struct {
	// DefaultInject and DefaultDelayMS are applied when a request does not set
	// the explicit injection headers. This lets the standalone endpoint model a
	// delayed, refused or late heal without coupling that choice into the engine.
	DefaultInject  string
	DefaultDelayMS int64
	// OnRequest is called synchronously after a request is accepted. It is used
	// by the forced-termination script to wait until a heal POST is in flight.
	OnRequest func(RequestEvent)
	// OnClosed reports a request context cancellation caused by a client-side
	// timeout or forced termination.
	OnClosed func(RequestEvent)
}

type Handler struct {
	config   HandlerConfig
	heals    atomic.Int64
	renews   atomic.Int64
	obs      atomic.Int64
	mu       sync.Mutex
	lastHeal core.HealRequest
}

func NewHandler(config HandlerConfig) *Handler {
	return &Handler{config: config}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}
	mode := r.Header.Get(HeaderInject)
	if mode == "" {
		mode = h.config.DefaultInject
	}
	delayHeader := r.Header.Get(HeaderDelayMS)
	if delayHeader == "" && h.config.DefaultDelayMS > 0 {
		delayHeader = strconv.FormatInt(h.config.DefaultDelayMS, 10)
	}
	delay := parseDelay(delayHeader)
	event := RequestEvent{Path: r.URL.Path, Mode: mode, Remote: r.RemoteAddr, At: time.Now()}
	if h.config.OnRequest != nil {
		h.config.OnRequest(event)
	}
	start := time.Now()
	defer func() {
		if r.Context().Err() != nil && h.config.OnClosed != nil {
			event.Closed = true
			event.CloseAfter = time.Since(start)
			h.config.OnClosed(event)
		}
	}()

	switch r.URL.Path {
	case "/v1/heal":
		h.handleHeal(w, r, mode, delay)
	case "/v1/renew":
		h.handleRenew(w, r, mode, delay)
	case "/v1/observe":
		h.handleObserve(w, r, mode, delay)
	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"code": "E_NOT_FOUND"})
	}
}

func (h *Handler) handleHeal(w http.ResponseWriter, r *http.Request, mode string, delay time.Duration) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"code": "E_METHOD"})
		return
	}
	if mode == "reject" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"code": core.CodeHealRejected, "message": "injected rejection"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"code": "E_BODY"})
		return
	}
	var request core.HealRequest
	if err := json.Unmarshal(body, &request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"code": "E_HEAL_REQUEST"})
		return
	}
	h.mu.Lock()
	h.lastHeal = request
	h.mu.Unlock()
	h.heals.Add(1)

	if mode == "block" {
		<-r.Context().Done()
		return
	}
	if mode == "late" {
		// Deliberately ignore cancellation and attempt a late response. The
		// client deadline should close the TCP connection first.
		time.Sleep(delay)
		writeJSON(w, http.StatusOK, core.HealReceipt{RequestID: request.RequestID, HolderID: request.HolderID, Applied: true, ServerTimeMS: time.Now().UnixMilli()})
		return
	}
	if !sleepRequest(r, delay) {
		return
	}
	writeJSON(w, http.StatusOK, core.HealReceipt{RequestID: request.RequestID, HolderID: request.HolderID, Applied: true, ServerTimeMS: time.Now().UnixMilli()})
}

func (h *Handler) handleRenew(w http.ResponseWriter, r *http.Request, mode string, delay time.Duration) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"code": "E_METHOD"})
		return
	}
	if mode == "reject" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"code": "E_RENEW_REJECTED"})
		return
	}
	if mode == "block" {
		<-r.Context().Done()
		return
	}
	h.renews.Add(1)
	if !sleepRequest(r, delay) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"accepted": true, "server_time_ms": time.Now().UnixMilli()})
}

func (h *Handler) handleObserve(w http.ResponseWriter, r *http.Request, mode string, delay time.Duration) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"code": "E_METHOD"})
		return
	}
	if mode == "reject" {
		writeJSON(w, http.StatusTooManyRequests, map[string]any{"code": "E_OBSERVATION_BACKPRESSURE"})
		return
	}
	h.obs.Add(1)
	if !sleepRequest(r, delay) {
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true, "server_arrival_ms": time.Now().UnixMilli()})
}

func (h *Handler) LastHeal() (core.HealRequest, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastHeal, h.lastHeal.RequestID != ""
}

func (h *Handler) Counts() (heals, renews, observations int64) {
	return h.heals.Load(), h.renews.Load(), h.obs.Load()
}

func parseDelay(value string) time.Duration {
	ms, err := strconv.ParseInt(value, 10, 64)
	if err != nil || ms <= 0 {
		return 0
	}
	if ms > 60_000 {
		ms = 60_000
	}
	return time.Duration(ms) * time.Millisecond
}

func sleepRequest(r *http.Request, delay time.Duration) bool {
	if delay <= 0 {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-r.Context().Done():
		return false
	case <-timer.C:
		return true
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

type Server struct {
	Listener net.Listener
	server   *http.Server
	handler  *Handler
}

func Start(addr string, config HandlerConfig) (*Server, error) {
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	handler := NewHandler(config)
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 2 * time.Second}
	wrapped := &Server{Listener: listener, server: server, handler: handler}
	go func() {
		_ = server.Serve(listener)
	}()
	return wrapped, nil
}

func (s *Server) URL() string {
	return fmt.Sprintf("http://%s", s.Listener.Addr().String())
}

func (s *Server) Handler() *Handler {
	return s.handler
}

func (s *Server) Close() error {
	if s == nil || s.server == nil {
		return nil
	}
	return s.server.Close()
}
