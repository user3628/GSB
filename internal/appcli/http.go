package appcli

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"headprune/internal/model"
)

// Server wraps the service with an HTTP transport.
type Server struct {
	svc *Service
	srv *http.Server
}

// NewHTTPServer builds the router without starting to listen.
func NewHTTPServer(svc *Service, addr string) *Server {
	mux := http.NewServeMux()
	s := &Server{svc: svc}
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("POST /import/heads", s.importHeads)
	mux.HandleFunc("POST /import/sentences", s.importSentences)
	mux.HandleFunc("POST /policy", s.setPolicy)
	mux.HandleFunc("POST /generate", s.generate)
	mux.HandleFunc("POST /pipeline/run", s.runAll)
	mux.HandleFunc("GET /errors/heads", s.headErrors)
	mux.HandleFunc("GET /errors/coupling", s.coupling)
	mux.HandleFunc("GET /quantization", s.quantization)
	mux.HandleFunc("GET /versions", s.history)
	mux.HandleFunc("GET /lineage", s.lineage)
	mux.HandleFunc("POST /versions/commit", s.commit)
	mux.HandleFunc("GET /versions/{v}", s.getVersion)
	mux.HandleFunc("POST /rollback/{v}", s.rollback)
	mux.HandleFunc("POST /stop", s.stop)
	mux.HandleFunc("POST /resume", s.resume)
	s.srv = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return s
}

// Start listens and serves in the background.
func (s *Server) Start() error { return s.srv.ListenAndServe() }

// Shutdown performs a graceful HTTP stop.
func (s *Server) Shutdown(ctx context.Context) error { return s.srv.Shutdown(ctx) }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeBizError(w http.ResponseWriter, err error) {
	be, ok := model.AsBizError(err)
	if !ok {
		be = &model.BizError{Code: "E_INTERNAL", Message: err.Error()}
	}
	status := http.StatusBadRequest
	if be.Code == model.CodeStaleVersion || be.Code == model.CodeConflict {
		status = http.StatusConflict
	}
	writeJSON(w, status, map[string]any{"error": be})
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.svc.Status())
}

func (s *Server) generate(w http.ResponseWriter, _ *http.Request) {
	in, err := s.svc.Generate()
	if err != nil {
		writeBizError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, in.Dataset.Policy)
}

type importReq struct {
	Path string `json:"path"`
}

func (s *Server) importHeads(w http.ResponseWriter, r *http.Request) {
	var req importReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Path == "" {
		writeBizError(w, errors.New("body must contain a path"))
		return
	}
	if err := s.svc.ImportHeads(req.Path); err != nil {
		writeBizError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "imported"})
}

func (s *Server) importSentences(w http.ResponseWriter, r *http.Request) {
	var req importReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Path == "" {
		writeBizError(w, errors.New("body must contain a path"))
		return
	}
	if err := s.svc.ImportSentences(req.Path); err != nil {
		writeBizError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "imported"})
}

func (s *Server) setPolicy(w http.ResponseWriter, r *http.Request) {
	var p model.Policy
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		writeBizError(w, err)
		return
	}
	if err := s.svc.SetPolicy(p); err != nil {
		writeBizError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (s *Server) runAll(w http.ResponseWriter, _ *http.Request) {
	res, err := s.svc.RunAll()
	if err != nil {
		writeBizError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) commit(w http.ResponseWriter, _ *http.Request) {
	vr, err := s.svc.Commit()
	if err != nil {
		writeBizError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, vr)
}

func (s *Server) headErrors(w http.ResponseWriter, _ *http.Request) {
	res, err := s.svc.HeadErrors()
	if err != nil {
		writeBizError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) coupling(w http.ResponseWriter, _ *http.Request) {
	res, err := s.svc.Coupling()
	if err != nil {
		writeBizError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) quantization(w http.ResponseWriter, _ *http.Request) {
	res, err := s.svc.Quantization()
	if err != nil {
		writeBizError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) history(w http.ResponseWriter, _ *http.Request) {
	res, err := s.svc.History()
	if err != nil {
		writeBizError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) lineage(w http.ResponseWriter, _ *http.Request) {
	res, err := s.svc.Lineage()
	if err != nil {
		writeBizError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) getVersion(w http.ResponseWriter, r *http.Request) {
	v, err := strconv.ParseInt(r.PathValue("v"), 10, 64)
	if err != nil {
		writeBizError(w, err)
		return
	}
	d, vr, err := s.svc.RestoreVersion(v)
	if err != nil {
		writeBizError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": vr, "decision": d})
}

func (s *Server) rollback(w http.ResponseWriter, r *http.Request) {
	v, err := strconv.ParseInt(r.PathValue("v"), 10, 64)
	if err != nil {
		writeBizError(w, err)
		return
	}
	vr, err := s.svc.Rollback(v)
	if err != nil {
		writeBizError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, vr)
}

func (s *Server) stop(w http.ResponseWriter, _ *http.Request) {
	if err := s.svc.Stop(); err != nil {
		writeBizError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "stopped"})
}

func (s *Server) resume(w http.ResponseWriter, _ *http.Request) {
	if err := s.svc.Resume(); err != nil {
		writeBizError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "resumed"})
}
