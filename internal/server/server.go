// Package server exposes the workbench over a local-only HTTP JSON API.
package server

import (
	"encoding/json"
	"net"
	"net/http"

	"qaudit/internal/engine"
	"qaudit/internal/model"
)

// Server wraps the workbench engine with HTTP handlers.
type Server struct {
	eng *engine.Engine
	mux *http.ServeMux
}

// New builds the HTTP routing table.
func New(eng *engine.Engine) *Server {
	s := &Server{eng: eng, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("/healthz", s.health)
	s.mux.HandleFunc("/api/import", s.importData)
	s.mux.HandleFunc("/api/policy", s.setPolicy)
	s.mux.HandleFunc("/api/run", s.run)
	s.mux.HandleFunc("/api/recover", s.recover)
	s.mux.HandleFunc("/api/status", s.status)
	s.mux.HandleFunc("/api/fingerprints", s.fingerprints)
	s.mux.HandleFunc("/api/audit", s.audit)
	s.mux.HandleFunc("/api/versions", s.versions)
	s.mux.HandleFunc("/api/stop", s.stop)
	s.mux.HandleFunc("/api/export", s.exportReport)
	s.mux.HandleFunc("/api/inject", s.inject)
}

// Handler exposes the root mux.
func (s *Server) Handler() http.Handler { return s.mux }

// Listen picks a free local port when addr is empty.
func Listen(addr string) (string, net.Listener, error) {
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", nil, err
	}
	return ln.Addr().String(), ln, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	if be, ok := err.(*model.BizError); ok {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"ok": false, "error": be})
		return
	}
	writeJSON(w, http.StatusInternalServerError, map[string]any{
		"ok":    false,
		"error": model.BizError{Code: model.CodeIO, Message: err.Error()},
	})
}

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type importRequest struct {
	Dataset *model.Dataset `json:"dataset"`
}

func (s *Server) importData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
		return
	}
	var req importRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	if err := s.eng.SaveDataset(req.Dataset); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

type policyRequest struct {
	Policy *model.Policy `json:"policy"`
}

func (s *Server) setPolicy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
		return
	}
	var req policyRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	ds := s.eng.Dataset
	if ds == nil {
		writeErr(w, model.NewBizError(model.CodeState, "dataset not loaded", nil))
		return
	}
	ds.Policy = *req.Policy
	if err := s.eng.SaveDataset(ds); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) run(w http.ResponseWriter, r *http.Request) {
	if err := s.eng.Run(); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.eng.Snapshot())
}

func (s *Server) recover(w http.ResponseWriter, r *http.Request) {
	if err := s.eng.Recover(); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.eng.Snapshot())
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	statuses, err := s.eng.LayerStatuses()
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"layers": statuses, "snapshot": s.eng.Snapshot()})
}

func (s *Server) fingerprints(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.eng.FingerprintsView())
}

func (s *Server) audit(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.eng.AuditView())
}

func (s *Server) versions(w http.ResponseWriter, r *http.Request) {
	rows, err := s.eng.Versions()
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"versions": rows})
}

func (s *Server) stop(w http.ResponseWriter, r *http.Request) {
	if err := s.eng.RequestGracefulStop(); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) exportReport(w http.ResponseWriter, r *http.Request) {
	rep, err := s.eng.ExportReport()
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

func (s *Server) inject(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
		return
	}
	var inj engine.Injection
	if err := decode(r, &inj); err != nil {
		writeErr(w, err)
		return
	}
	ds, err := s.eng.CloneDataset()
	if err != nil {
		writeErr(w, err)
		return
	}
	if err := engine.Apply(ds, inj); err != nil {
		writeErr(w, err)
		return
	}
	if err := s.eng.RunWith(ds); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.eng.Snapshot())
}
