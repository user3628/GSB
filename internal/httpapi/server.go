// Package httpapi exposes the engine over loopback HTTP. Every listener binds
// 127.0.0.1 on a high port; a bind failure is reported explicitly as
// E_BIND_FAILED instead of being silently skipped.
package httpapi

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"time"

	"alarmengine/internal/engine"
	"alarmengine/internal/errcode"
	"alarmengine/internal/model"
)

// Server wraps the engine with an HTTP handler.
type Server struct {
	eng  *engine.Engine
	addr string
	srv  *http.Server
	ln   net.Listener
}

// NewServer constructs a loopback-only server on 127.0.0.1:port.
func NewServer(eng *engine.Engine, port int) *Server {
	return &Server{eng: eng, addr: "127.0.0.1:" + strconv.Itoa(port)}
}

// Addr returns the bound address.
func (s *Server) Addr() string {
	if s.ln != nil {
		return s.ln.Addr().String()
	}
	return s.addr
}

// Start binds the listener. A busy or unavailable port is returned as
// E_BIND_FAILED rather than retried on another port.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return errcode.New(errcode.EBindFailed, "listen %s failed: %v", s.addr, err)
	}
	s.ln = ln
	mux := http.NewServeMux()
	s.routes(mux)
	s.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = s.srv.Serve(ln) }()
	return nil
}

// Shutdown stops the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.srv == nil {
		return nil
	}
	return s.srv.Shutdown(ctx)
}

func (s *Server) routes(mux *http.ServeMux) {
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/bootstrap/services", s.handleBootstrapServices)
	mux.HandleFunc("/bootstrap/policy", s.handleBootstrapPolicy)
	mux.HandleFunc("/ingest", s.handleIngest)
	mux.HandleFunc("/snapshot/replace", s.handleReplaceSnapshot)
	mux.HandleFunc("/alerts/transition", s.handleTransition)
	mux.HandleFunc("/alerts", s.handleAlerts)
	mux.HandleFunc("/suppression", s.handleSuppression)
	mux.HandleFunc("/rootcauses", s.handleRootCauses)
	mux.HandleFunc("/noise/full", s.handleNoiseFull)
	mux.HandleFunc("/noise/sampled", s.handleNoiseSampled)
	mux.HandleFunc("/paths/diff", s.handlePathDiff)
	mux.HandleFunc("/patrol", s.handlePatrol)
	mux.HandleFunc("/recovery", s.handleRecovery)
}

type envelope struct {
	OK    bool   `json:"ok"`
	Code  string `json:"code,omitempty"`
	Error string `json:"error,omitempty"`
	Data  any    `json:"data,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v envelope) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeOK(w http.ResponseWriter, data any) {
	writeJSON(w, http.StatusOK, envelope{OK: true, Data: data})
}

func writeErr(w http.ResponseWriter, err error) {
	code := errcode.CodeOf(err)
	status := http.StatusBadRequest
	switch code {
	case errcode.EPatrolQueueFull:
		status = http.StatusServiceUnavailable
	case errcode.EConflictingRequest:
		status = http.StatusConflict
	case errcode.EStaleBase, errcode.ESnapshotInvalid:
		status = http.StatusConflict
	case errcode.EInvalidTransition:
		status = http.StatusUnprocessableEntity
	case errcode.EBindFailed:
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, envelope{OK: false, Code: code, Error: err.Error()})
}

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return errcode.New(errcode.ESampleInvalid, "invalid JSON body: %v", err)
	}
	return nil
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeOK(w, map[string]any{"version": s.eng.Version(), "addr": s.Addr()})
}

func (s *Server) handleBootstrapServices(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Services []model.Service `json:"services"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	if err := s.eng.BootstrapServices(req.Services); err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, map[string]any{"services": len(req.Services)})
}

func (s *Server) handleBootstrapPolicy(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Policy model.PolicySnapshot `json:"policy"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	if err := s.eng.BootstrapPolicy(req.Policy); err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, map[string]any{"version": req.Policy.Version})
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	var req engine.IngestRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	res, err := s.eng.IngestObservations(req)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, res)
}

func (s *Server) handleReplaceSnapshot(w http.ResponseWriter, r *http.Request) {
	var req engine.ReplaceSnapshotRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	res, err := s.eng.ReplaceSnapshot(req)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, res)
}

func (s *Server) handleTransition(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AlertID string           `json:"alert_id"`
		To      model.AlertState `json:"to"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	if err := s.eng.TransitionAlert(req.AlertID, req.To); err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, map[string]any{"alert_id": req.AlertID, "state": req.To})
}

func versionParam(r *http.Request) int64 {
	v, _ := strconv.ParseInt(r.URL.Query().Get("version"), 10, 64)
	return v
}

func (s *Server) handleAlerts(w http.ResponseWriter, r *http.Request) {
	alerts, err := s.eng.QueryAlerts(versionParam(r))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, alerts)
}

func (s *Server) handleSuppression(w http.ResponseWriter, r *http.Request) {
	hits, err := s.eng.QuerySuppressionHits(versionParam(r))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, hits)
}

func (s *Server) handleRootCauses(w http.ResponseWriter, r *http.Request) {
	scores, err := s.eng.QueryRootCauses(versionParam(r))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, scores)
}

func (s *Server) handleNoiseFull(w http.ResponseWriter, r *http.Request) {
	res, err := s.eng.FullNoiseReduction(versionParam(r))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, res)
}

func (s *Server) handleNoiseSampled(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Version int64 `json:"version"`
		Limit   int   `json:"limit"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	res, err := s.eng.SampledNoiseReduction(req.Version, req.Limit)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, res)
}

func (s *Server) handlePathDiff(w http.ResponseWriter, r *http.Request) {
	version := versionParam(r)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 20
	}
	sampled, err := s.eng.SampledNoiseReduction(version, limit)
	if err != nil {
		writeErr(w, err)
		return
	}
	full, err := s.eng.FullNoiseReduction(version)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, engine.ComparePaths(sampled, full))
}

func (s *Server) handlePatrol(w http.ResponseWriter, r *http.Request) {
	roundID := r.URL.Query().Get("round")
	if roundID == "" {
		roundID = "round-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	trace, err := s.eng.PatrolRound(r.Context(), roundID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, trace)
}

func (s *Server) handleRecovery(w http.ResponseWriter, r *http.Request) {
	writeOK(w, s.eng.RecoveryReportOf())
}
