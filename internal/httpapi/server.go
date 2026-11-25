// Package httpapi exposes the pipeline over a local HTTP interface with
// uniform JSON business-error responses.
package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"configtree/internal/engine"
	"configtree/internal/spec"
)

type Server struct {
	eng *engine.Engine
	mux *http.ServeMux
}

func New(eng *engine.Engine) *Server {
	s := &Server{eng: eng, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	s.mux.HandleFunc("POST /v1/commits/snapshots", s.handleSnapshot)
	s.mux.HandleFunc("POST /v1/commits/deltas", s.handleDelta)
	s.mux.HandleFunc("GET /v1/components/{id}/view", s.handleView)
	s.mux.HandleFunc("GET /v1/affected/{version}", s.handleAffected)
	s.mux.HandleFunc("GET /v1/commits", s.handleCommits)
	s.mux.HandleFunc("GET /v1/recovery", s.handleRecovery)
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	var req engine.CommitRequest
	req.Kind = "snapshot"
	if err := decodeBody(r, &req); err != nil {
		writeError(w, err)
		return
	}
	var snap spec.Snapshot
	if err := reencode(req.RawBody, &snap); err != nil {
		writeError(w, spec.NewError(spec.CodeSpecInvalid, "invalid snapshot envelope",
			map[string]any{"detail": err.Error()}))
		return
	}
	req.Snapshot = &snap
	res, err := s.eng.Commit(r.Context(), &req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleDelta(w http.ResponseWriter, r *http.Request) {
	var req engine.CommitRequest
	req.Kind = "delta"
	if err := decodeBody(r, &req); err != nil {
		writeError(w, err)
		return
	}
	var d spec.Delta
	if err := reencode(req.RawBody, &d); err != nil {
		writeError(w, spec.NewError(spec.CodeSpecInvalid, "invalid delta envelope",
			map[string]any{"detail": err.Error()}))
		return
	}
	req.Delta = &d
	res, err := s.eng.Commit(r.Context(), &req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleView(w http.ResponseWriter, r *http.Request) {
	observed, err := queryInt64(r, "observed_version")
	if err != nil {
		writeError(w, spec.NewError(spec.CodeSpecInvalid,
			"observed_version query parameter must be a non-negative integer", nil))
		return
	}
	res, err := s.eng.ReadView(r.Context(), r.PathValue("id"), observed)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleAffected(w http.ResponseWriter, r *http.Request) {
	version, err := pathInt64(r, "version")
	if err != nil {
		writeError(w, spec.NewError(spec.CodeSpecInvalid, "invalid version path value", nil))
		return
	}
	res, err := s.eng.AffectedAt(r.Context(), version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleCommits(w http.ResponseWriter, r *http.Request) {
	res, err := s.eng.Commits(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"commits": res})
}

func (s *Server) handleRecovery(w http.ResponseWriter, r *http.Request) {
	report := s.eng.LastRecoveryReport()
	if report == nil {
		writeJSON(w, http.StatusOK, map[string]any{"recovered": false})
		return
	}
	writeJSON(w, http.StatusOK, report)
}

// writeError maps business errors onto HTTP status codes while always
// preserving the stable business code and structured context.
func writeError(w http.ResponseWriter, err error) {
	var biz *spec.Error
	if !errors.As(err, &biz) {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"code":    "E_INTERNAL",
			"message": err.Error(),
			"context": map[string]any{},
		})
		return
	}
	status := http.StatusBadRequest
	switch biz.Code {
	case spec.CodeStaleRead:
		status = http.StatusConflict
	case spec.CodeStaleSnapshot, spec.CodeStaleBase, spec.CodeConflictingRequest:
		status = http.StatusConflict
	case spec.CodeNotFound:
		status = http.StatusNotFound
	case spec.CodeSpecInvalid:
		status = http.StatusUnprocessableEntity
	}
	writeJSON(w, status, biz)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
