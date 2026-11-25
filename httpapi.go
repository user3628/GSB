package prefixcache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// HTTPServer exposes the cache through a small loopback-friendly JSON API.
type HTTPServer struct {
	engine *Engine
	server *http.Server

	mu       sync.Mutex
	listener net.Listener
	serveErr chan error
	once     sync.Once
}

// NewHTTPServer creates an HTTP facade over an Engine.
func NewHTTPServer(engine *Engine) *HTTPServer {
	facade := &HTTPServer{engine: engine, serveErr: make(chan error, 1)}
	facade.server = &http.Server{
		Handler:           facade.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return facade
}

// Handler returns the JSON API handler.
func (s *HTTPServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/healthz", s.handleHealth)
	mux.HandleFunc("GET /v1/status", s.handleStatus)
	mux.HandleFunc("GET /v1/snapshot", s.handleSnapshot)
	mux.HandleFunc("GET /v1/invalidations", s.handleInvalidations)
	mux.HandleFunc("GET /v1/evictions", s.handleEvictions)
	mux.HandleFunc("GET /v1/requests", s.handleRequests)
	mux.HandleFunc("GET /v1/request-state", s.handleRequestState)
	mux.HandleFunc("GET /v1/demo-data", s.handleDemoData)
	mux.HandleFunc("POST /v1/bootstrap", s.handleBootstrap)
	mux.HandleFunc("POST /v1/policy", s.handlePolicy)
	mux.HandleFunc("POST /v1/policy/patch", s.handlePatch)
	mux.HandleFunc("POST /v1/requests", s.handleImportRequests)
	mux.HandleFunc("POST /v1/hits", s.handleHit)
	mux.HandleFunc("POST /v1/shutdown", s.handleShutdown)
	return mux
}

// Listen binds a local TCP address and returns the actual listener.
func (s *HTTPServer) Listen(address string) (net.Listener, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", address, err)
	}
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()
	return listener, nil
}

// Serve serves an already-created listener. It exits with ErrServerClosed after
// a graceful shutdown.
func (s *HTTPServer) Serve(listener net.Listener) error {
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()
	err := s.server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	select {
	case s.serveErr <- err:
	default:
	}
	return err
}

// ListenAndServe binds the address and serves until shutdown.
func (s *HTTPServer) ListenAndServe(address string) error {
	listener, err := s.Listen(address)
	if err != nil {
		return err
	}
	return s.Serve(listener)
}

// Address returns the bound listener address.
func (s *HTTPServer) Address() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// ServeError exposes a fatal Serve error.
func (s *HTTPServer) ServeError() <-chan error { return s.serveErr }

// Shutdown gracefully stops accepting requests and drains active connections.
func (s *HTTPServer) Shutdown(ctx context.Context) error {
	var err error
	s.once.Do(func() {
		err = s.server.Shutdown(ctx)
	})
	return err
}

func (s *HTTPServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "status": "healthy"})
}

func (s *HTTPServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	status, err := s.engine.Status(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "result": status})
}

func (s *HTTPServer) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	version, err := int64Query(r, "version", 0)
	if err != nil {
		writeError(w, err)
		return
	}
	snapshot, err := s.engine.Snapshot(r.Context(), version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "result": snapshot})
}

func (s *HTTPServer) handleInvalidations(w http.ResponseWriter, r *http.Request) {
	version, err := int64Query(r, "version", 0)
	if err != nil {
		writeError(w, err)
		return
	}
	result, err := s.engine.Invalidations(r.Context(), version, r.URL.Query().Get("request_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "result": result})
}

func (s *HTTPServer) handleEvictions(w http.ResponseWriter, r *http.Request) {
	version, err := int64Query(r, "version", 0)
	if err != nil {
		writeError(w, err)
		return
	}
	result, err := s.engine.Evictions(r.Context(), version, r.URL.Query().Get("request_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "result": result})
}

func (s *HTTPServer) handleRequests(w http.ResponseWriter, r *http.Request) {
	result, err := s.engine.Requests(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "result": result})
}

func (s *HTTPServer) handleRequestState(w http.ResponseWriter, r *http.Request) {
	version, err := int64Query(r, "version", 0)
	if err != nil {
		writeError(w, err)
		return
	}
	requestID := r.URL.Query().Get("request_id")
	if requestID == "" {
		writeError(w, invalidRequestError("", "request_id is required"))
		return
	}
	result, err := s.engine.State(r.Context(), version, requestID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "result": result})
}

func (s *HTTPServer) handleDemoData(w http.ResponseWriter, r *http.Request) {
	seed, err := int64Query(r, "seed", DefaultSeed)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "result": GenerateDataset(seed)})
}

type bootstrapRequest struct {
	Policy   Policy    `json:"policy"`
	Requests []Request `json:"requests"`
}

func (s *HTTPServer) handleBootstrap(w http.ResponseWriter, r *http.Request) {
	var request bootstrapRequest
	if !decodeBody(w, r, &request) {
		return
	}
	result, err := s.engine.Initialize(r.Context(), request.Policy, request.Requests)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"ok": true, "result": result})
}

func (s *HTTPServer) handlePolicy(w http.ResponseWriter, r *http.Request) {
	var submission PolicySubmission
	if !decodeBody(w, r, &submission) {
		return
	}
	result, err := s.engine.SubmitPolicy(r.Context(), submission)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"ok": true, "result": result})
}

type patchRequest struct {
	ExpectedVersion int64       `json:"expected_version"`
	Patch           PolicyPatch `json:"patch"`
}

func (s *HTTPServer) handlePatch(w http.ResponseWriter, r *http.Request) {
	var request patchRequest
	if !decodeBody(w, r, &request) {
		return
	}
	result, err := s.engine.ApplyPatch(r.Context(), request.ExpectedVersion, request.Patch)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"ok": true, "result": result})
}

type importRequestsRequest struct {
	ExpectedVersion int64     `json:"expected_version"`
	Requests        []Request `json:"requests"`
	ReplaceAll      bool      `json:"replace_all"`
}

func (s *HTTPServer) handleImportRequests(w http.ResponseWriter, r *http.Request) {
	var request importRequestsRequest
	if !decodeBody(w, r, &request) {
		return
	}
	result, err := s.engine.ImportRequests(r.Context(), request.ExpectedVersion, request.Requests, request.ReplaceAll)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"ok": true, "result": result})
}

func (s *HTTPServer) handleHit(w http.ResponseWriter, r *http.Request) {
	var query HitQuery
	if !decodeBody(w, r, &query) {
		return
	}
	result, err := s.engine.Hit(r.Context(), query)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "result": result})
}

func (s *HTTPServer) handleShutdown(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "status": "stopping"})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	}()
}

func decodeBody(w http.ResponseWriter, r *http.Request, destination any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		writeError(w, invalidRequestError("", "invalid JSON body: "+err.Error()))
		return false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, invalidRequestError("", "request body must contain one JSON object"))
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	var typed *Error
	if !errors.As(err, &typed) {
		typed = &Error{Code: CodeInternal, Message: err.Error()}
	}
	status := http.StatusInternalServerError
	switch typed.Code {
	case CodeStaleVersion, CodeSlotConflict:
		status = http.StatusConflict
	case CodeInvalidPolicy, CodeInvalidRequest:
		status = http.StatusBadRequest
	case CodeNotFound:
		status = http.StatusNotFound
	case CodeLockTimeout:
		status = http.StatusServiceUnavailable
	case CodeInterrupted:
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{"ok": false, "error": typed})
}

func int64Query(r *http.Request, name string, fallback int64) (int64, error) {
	value := r.URL.Query().Get(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, invalidRequestError("", fmt.Sprintf("%s must be an integer", name))
	}
	return parsed, nil
}
