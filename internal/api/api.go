// Package api exposes the inspection service over a local HTTP JSON API.
// All failures use a uniform envelope: {"error":{"code","message","context"}}.
package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"blockinspect/internal/cerr"
	"blockinspect/internal/sample"
	"blockinspect/internal/service"
)

// Server wires HTTP routes to the domain service.
type Server struct {
	svc       *service.Service
	sampleDir string
	mux       *http.ServeMux
}

// NewServer builds the route table. sampleDir is where generated samples
// are written.
func NewServer(svc *service.Service, sampleDir string) *Server {
	s := &Server{svc: svc, sampleDir: sampleDir, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /v1/sample/generate", s.handleGenerateSample)
	s.mux.HandleFunc("POST /v1/manifest/load", s.handleLoadManifest)
	s.mux.HandleFunc("GET /v1/manifest/", s.handleGetManifest)
	s.mux.HandleFunc("POST /v1/reports", s.handleSubmitReport)
	s.mux.HandleFunc("POST /v1/verify", s.handleVerify)
	s.mux.HandleFunc("GET /v1/verify/", s.handleGetVerify)
	s.mux.HandleFunc("GET /v1/blocks/", s.handleBlockSub)
	s.mux.HandleFunc("GET /v1/recovery", s.handleRecovery)
	return s
}

// Handler returns the root HTTP handler.
func (s *Server) Handler() http.Handler { return s.mux }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	code := cerr.CodeOf(err)
	status := http.StatusBadRequest
	switch code {
	case cerr.NotFound, cerr.CheckpointNotFound:
		status = http.StatusNotFound
	case cerr.ConflictingRequest:
		status = http.StatusConflict
	case cerr.RecoveryIncomplete:
		status = http.StatusConflict
	case cerr.Internal:
		status = http.StatusInternalServerError
	}
	if e, ok := err.(*cerr.Error); ok {
		writeJSON(w, status, map[string]any{"error": e})
		return
	}
	writeJSON(w, status, map[string]any{"error": &cerr.Error{Code: code, Message: err.Error()}})
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeErr(w, cerr.New(cerr.BadRequest, "invalid JSON body", "detail", err.Error()))
		return false
	}
	return true
}

func (s *Server) handleGenerateSample(w http.ResponseWriter, r *http.Request) {
	spec := sample.DefaultSpec()
	if r.Body != nil && r.ContentLength > 0 {
		if !decode(w, r, &spec) {
			return
		}
	}
	m, err := sample.Generate(spec, s.sampleDir)
	if err != nil {
		writeErr(w, err)
		return
	}
	if _, err := s.svc.LoadManifest(s.sampleDir + "/manifest.json"); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"sample_dir": s.sampleDir, "manifest": m,
	})
}

func (s *Server) handleLoadManifest(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path string `json:"path"`
	}
	if !decode(w, r, &req) {
		return
	}
	m, err := s.svc.LoadManifest(req.Path)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, m)
}

func (s *Server) handleGetManifest(w http.ResponseWriter, r *http.Request) {
	blockID := strings.TrimPrefix(r.URL.Path, "/v1/manifest/")
	m, err := s.svc.Manifest(blockID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, m)
}

func (s *Server) handleSubmitReport(w http.ResponseWriter, r *http.Request) {
	var rep sample.ReplicaReport
	if !decode(w, r, &rep) {
		return
	}
	if err := s.svc.SubmitReport(&rep); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "accepted", "replica_id": rep.ReplicaID})
}

func (s *Server) handleVerify(w http.ResponseWriter, r *http.Request) {
	var req service.VerifyRequest
	if !decode(w, r, &req) {
		return
	}
	res, err := s.svc.Verify(&req)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleGetVerify(w http.ResponseWriter, r *http.Request) {
	requestID := strings.TrimPrefix(r.URL.Path, "/v1/verify/")
	res, err := s.svc.Result(requestID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleBlockSub(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/blocks/")
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) == 2 && parts[1] == "incremental" {
		stats, err := s.svc.IncrementalStats(parts[0])
		if err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, stats)
		return
	}
	writeErr(w, cerr.New(cerr.NotFound, "unknown block sub-resource", "path", r.URL.Path))
}

func (s *Server) handleRecovery(w http.ResponseWriter, r *http.Request) {
	rep, err := s.svc.Recover()
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

// ErrShutdown is returned by ListenAndServe on graceful shutdown.
var ErrShutdown = errors.New("server shutdown")
