// Package httpapi exposes the arbitration engine over a loopback-only HTTP API.
package httpapi

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"

	"leasearbiter/internal/arbiter"
)

// Server wires the idempotent service to HTTP routes.
type Server struct {
	svc *arbiter.IdempotentService
	mux *http.ServeMux
}

// NewServer builds the HTTP handler. It must be served on a loopback address.
func NewServer(svc *arbiter.IdempotentService) *Server {
	s := &Server{svc: svc, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

// ListenLoopback refuses non-loopback bind addresses.
func ListenLoopback(addr string) (net.Listener, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ip := net.ParseIP(host)
	if ip == nil || (!ip.IsLoopback()) {
		return nil, errors.New("refusing non-loopback bind: " + addr)
	}
	return net.Listen("tcp", addr)
}

func (s *Server) routes() {
	m := s.mux
	m.HandleFunc("/healthz", s.health)
	m.HandleFunc("/rows", s.handleRows)
	m.HandleFunc("/rows/open", s.handlePanelOpen)
	m.HandleFunc("/snapshots", s.handleSnapshots)
	m.HandleFunc("/leases/acquire", s.handleAcquire)
	m.HandleFunc("/leases/renew", s.handleRenew)
	m.HandleFunc("/leases/release", s.handleRelease)
	m.HandleFunc("/edits/precheck", s.handlePrecheck)
	m.HandleFunc("/edits/reconcile", s.handleReconcile)
	m.HandleFunc("/edits/commit", s.handleCommit)
	m.HandleFunc("/recycle", s.handleRecycle)
	m.HandleFunc("/recovery/report", s.handleRecoveryReport)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeAPIError(w http.ResponseWriter, err error) {
	var ae *arbiter.APIError
	if !errors.As(err, &ae) {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"ok":    false,
			"error": map[string]any{"code": "E_INTERNAL", "message": err.Error()},
		})
		return
	}
	status := httpStatusFor(ae.Code)
	body := map[string]any{
		"ok": false,
		"error": map[string]any{
			"code":    string(ae.Code),
			"message": ae.Message,
			"details": ae.Details,
		},
	}
	writeJSON(w, status, body)
}

func httpStatusFor(code arbiter.ErrorCode) int {
	switch code {
	case arbiter.CodeBadRequest:
		return http.StatusBadRequest
	case arbiter.CodeNotFound, arbiter.CodeRowDeleted:
		return http.StatusNotFound
	case arbiter.CodeSnapshotAhead, arbiter.CodeVersionRetired:
		return http.StatusConflict
	case arbiter.CodeLeaseHeld, arbiter.CodeLeaseStale, arbiter.CodeLeaseLost,
		arbiter.CodeEditStale, arbiter.CodeLeaseTimeout, arbiter.CodeConflictingRequest,
		arbiter.CodePrecheckIncomplete:
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

func writeResult(w http.ResponseWriter, v any) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "data": v})
}
