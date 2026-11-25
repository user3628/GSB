// Package workbench exposes the registry engine through a local HTTP API and a
// dependency-free interactive diagnostic page.
package workbench

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"registrylab/registry"
	"registrylab/sample"
	"registrylab/testfixture"
)

//go:embed index.html
var assets embed.FS

// Server binds domain operations to local HTTP routes.
type Server struct {
	Engine  *registry.Engine
	Fixture *testfixture.Fixture
	Mux     *http.ServeMux
}

// New constructs the API and workbench router.
func New(engine *registry.Engine, fixture *testfixture.Fixture) *Server {
	s := &Server{Engine: engine, Fixture: fixture, Mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler { return s.withRecovery(s.Mux) }

func (s *Server) routes() {
	s.Mux.HandleFunc("GET /healthz", s.handleHealth)
	s.Mux.HandleFunc("GET /api/v1/overview", s.handleOverview)
	s.Mux.HandleFunc("GET /api/v1/status", s.handleOverview)
	s.Mux.HandleFunc("GET /api/v1/snapshot", s.handleSnapshot)
	s.Mux.HandleFunc("GET /api/v1/derived", s.handleDerived)
	s.Mux.HandleFunc("POST /api/v1/snapshots", s.handleSubmitSnapshot)
	s.Mux.HandleFunc("POST /api/v1/deltas", s.handleSubmitDelta)
	s.Mux.HandleFunc("POST /api/v1/probe-rounds", s.handleRound)
	s.Mux.HandleFunc("GET /api/v1/probe-rounds/{id}", s.handleGetRound)
	s.Mux.HandleFunc("POST /api/v1/instances/{service}/{id}/heartbeat", s.handleHeartbeat)
	s.Mux.HandleFunc("POST /api/v1/instances/{service}/{id}/heartbeats", s.handleHeartbeat)
	s.Mux.HandleFunc("POST /api/v1/instances/{service}/{id}/state", s.handleState)
	s.Mux.HandleFunc("GET /api/v1/intents", s.handleIntents)
	s.Mux.HandleFunc("GET /api/v1/recovery", s.handleRecovery)
	s.Mux.HandleFunc("GET /api/v1/sample", s.handleSample)
	s.Mux.HandleFunc("GET /api/v1/sample/delta", s.handleSampleDelta)
	s.Mux.HandleFunc("GET /", s.handleUI)
	s.Mux.HandleFunc("/", s.handleNotFound)
}

type overview struct {
	Current  registry.CurrentSnapshot `json:"current"`
	Derived  registry.DerivedView     `json:"derived"`
	Recovery registry.RecoverySummary `json:"recovery"`
	Limits   map[string]any           `json:"limits"`
}

func (s *Server) handleUI(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		s.handleNotFound(w, r)
		return
	}
	raw, err := assets.ReadFile("index.html")
	if err != nil {
		http.Error(w, "workbench asset missing", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(raw)
}

func (s *Server) handleNotFound(w http.ResponseWriter, r *http.Request) {
	s.fail(w, "", registryError(registry.ErrNotFound, "route not found"))
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.ok(w, "", map[string]any{"status": "ok", "time": time.Now().UTC().Format(time.RFC3339Nano)})
}

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	view, opErr := s.Engine.ReadComplete(ctx)
	if opErr != nil {
		s.fail(w, requestID(r, ""), opErr)
		return
	}
	recovery, opErr := s.Engine.RecoveryStatus(ctx)
	if opErr != nil {
		s.fail(w, requestID(r, ""), opErr)
		return
	}
	s.ok(w, "", overview{Current: view.Current, Derived: view.Derived, Recovery: recovery, Limits: map[string]any{
		"globalConcurrency": 4, "perServiceConcurrency": 2, "budgetMs": 1500, "waitQueueCapacity": 64,
	}})
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	cur, opErr := s.Engine.Current(r.Context())
	if opErr != nil {
		s.fail(w, "", opErr)
		return
	}
	s.ok(w, "", cur)
}

func (s *Server) handleDerived(w http.ResponseWriter, r *http.Request) {
	view, opErr := s.Engine.Derived(r.Context())
	if opErr != nil {
		s.fail(w, "", opErr)
		return
	}
	s.ok(w, "", view)
}

func (s *Server) handleSubmitSnapshot(w http.ResponseWriter, r *http.Request) {
	var req registry.SnapshotRequest
	if !s.decode(w, r, &req) {
		return
	}
	req.RequestID = requestID(r, req.RequestID)
	result, opErr := s.Engine.SubmitSnapshot(r.Context(), req)
	if opErr != nil {
		s.fail(w, req.RequestID, opErr)
		return
	}
	s.ok(w, req.RequestID, result)
}

func (s *Server) handleSubmitDelta(w http.ResponseWriter, r *http.Request) {
	var req registry.DeltaRequest
	if !s.decode(w, r, &req) {
		return
	}
	req.RequestID = requestID(r, req.RequestID)
	result, opErr := s.Engine.SubmitDelta(r.Context(), req)
	if opErr != nil {
		s.fail(w, req.RequestID, opErr)
		return
	}
	s.ok(w, req.RequestID, result)
}

func (s *Server) handleRound(w http.ResponseWriter, r *http.Request) {
	var req registry.RoundOptions
	if !s.decode(w, r, &req) {
		return
	}
	req.RequestID = requestID(r, req.RequestID)
	result, opErr := s.Engine.RunProbeRound(r.Context(), req)
	if opErr != nil {
		s.fail(w, req.RequestID, opErr)
		return
	}
	s.ok(w, req.RequestID, result)
}

func (s *Server) handleGetRound(w http.ResponseWriter, r *http.Request) {
	result, opErr := s.Engine.GetRound(r.Context(), r.PathValue("id"))
	if opErr != nil {
		s.fail(w, "", opErr)
		return
	}
	s.ok(w, "", result)
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req registry.HeartbeatRequest
	if !s.decode(w, r, &req) {
		return
	}
	req.RequestID = requestID(r, req.RequestID)
	result, opErr := s.Engine.Heartbeat(r.Context(), r.PathValue("service"), r.PathValue("id"), req)
	if opErr != nil {
		s.fail(w, req.RequestID, opErr)
		return
	}
	s.ok(w, req.RequestID, result)
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	var req registry.SetStateRequest
	if !s.decode(w, r, &req) {
		return
	}
	req.RequestID = requestID(r, req.RequestID)
	result, opErr := s.Engine.SetState(r.Context(), r.PathValue("service"), r.PathValue("id"), req)
	if opErr != nil {
		s.fail(w, req.RequestID, opErr)
		return
	}
	s.ok(w, req.RequestID, result)
}

func (s *Server) handleIntents(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	intents, opErr := s.Engine.Intents(r.Context(), limit)
	if opErr != nil {
		s.fail(w, "", opErr)
		return
	}
	if intents == nil {
		intents = []registry.IntentView{}
	}
	s.ok(w, "", intents)
}

func (s *Server) handleRecovery(w http.ResponseWriter, r *http.Request) {
	report, opErr := s.Engine.RecoveryStatus(r.Context())
	if opErr != nil {
		s.fail(w, "", opErr)
		return
	}
	s.ok(w, "", report)
}

func (s *Server) handleSample(w http.ResponseWriter, r *http.Request) {
	if s.Fixture == nil {
		s.fail(w, "", registryError(registry.ErrInternal, "fixture endpoints are not configured"))
		return
	}
	cur, opErr := s.Engine.Current(r.Context())
	if opErr != nil {
		s.fail(w, "", opErr)
		return
	}
	version := cur.Version + 1
	if raw := r.URL.Query().Get("version"); raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil {
			version = parsed
		}
	}
	snap := sample.Generate(sample.Options{Seed: 20261126, Version: version, GeneratedAt: "2026-11-26T00:00:00Z",
		BaseURL: s.Fixture.BaseURL, TCPAddr: s.Fixture.TCPAddr, RefusedAddr: s.Fixture.RefusedAddr})
	delta := sample.DeltaExample(cur.Version, cur.Version+1, s.Fixture.BaseURL)
	s.ok(w, "", map[string]any{"snapshot": snap, "delta": delta, "fixture": map[string]any{
		"baseUrl": s.Fixture.BaseURL, "tcpAddr": s.Fixture.TCPAddr, "refusedAddr": s.Fixture.RefusedAddr,
	}})
}

func (s *Server) handleSampleDelta(w http.ResponseWriter, r *http.Request) {
	if s.Fixture == nil {
		s.fail(w, "", registryError(registry.ErrInternal, "fixture endpoints are not configured"))
		return
	}
	cur, opErr := s.Engine.Current(r.Context())
	if opErr != nil {
		s.fail(w, "", opErr)
		return
	}
	base := cur.Version
	if raw := r.URL.Query().Get("baseVersion"); raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil {
			base = parsed
		}
	}
	s.ok(w, "", sample.DeltaExample(base, base+1, s.Fixture.BaseURL))
}

func (s *Server) decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 4<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		s.fail(w, "", validation(fmt.Errorf("invalid JSON body: %w", err)))
		return false
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		s.fail(w, "", validation(errors.New("request body must contain one JSON value")))
		return false
	}
	return true
}

func requestID(r *http.Request, bodyID string) string {
	if bodyID != "" {
		return bodyID
	}
	return strings.TrimSpace(r.Header.Get("X-Request-ID"))
}

func (s *Server) ok(w http.ResponseWriter, reqID string, data any) {
	raw, err := json.Marshal(data)
	if err != nil {
		s.fail(w, reqID, registryError(registry.ErrInternal, err.Error()))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(registry.Envelope{OK: true, Data: raw, RequestID: reqID})
}

func (s *Server) fail(w http.ResponseWriter, reqID string, err *registry.Error) {
	if err == nil {
		err = registryError(registry.ErrInternal, "unknown error")
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(err.HTTPStatus())
	_ = json.NewEncoder(w).Encode(registry.Envelope{OK: false, Error: err, RequestID: reqID})
}

func (s *Server) withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.fail(w, "", registryError(registry.ErrInternal, fmt.Sprintf("panic: %v", recovered)))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func validation(err error) *registry.Error {
	return &registry.Error{Code: registry.ErrValidation, Message: err.Error(), Context: map[string]any{"reason": err.Error()}}
}

func registryError(code, message string) *registry.Error {
	return &registry.Error{Code: code, Message: message, Context: map[string]any{}}
}

// Run starts the local HTTP workbench.
func (s *Server) Run(ctx context.Context, addr string) error {
	server := &http.Server{Addr: addr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
