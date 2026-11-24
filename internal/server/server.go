// Package server exposes the validation engine over HTTP using only the
// standard library. It runs against the real process clock, so latency is
// observed via the runtime monotonic reading.
package server

import (
	"encoding/json"
	"net/http"

	"casengine/internal/engine"
)

// Server wraps an engine with HTTP handlers.
type Server struct {
	Engine *engine.Engine
}

type validateBody struct {
	Seq       int64  `json:"seq"`
	Ticket    string `json:"ticket"`
	PresentSP string `json:"presentSp"`
}

type issueBody struct {
	ID         string `json:"id"`
	TenantID   string `json:"tenantId"`
	SPID       string `json:"spId"`
	UserID     string `json:"userId"`
	TTLSeconds int64  `json:"ttlSeconds"`
}

// New returns an http.Handler with the engine routes registered.
func New(e *engine.Engine) http.Handler {
	s := &Server{Engine: e}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/issue", s.issue)
	mux.HandleFunc("/validate", s.validate)
	return mux
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// issue mints a ticket. It exists for the local offline demo only; the engine
// is otherwise intended to receive tickets from a central CAS issuer.
func (s *Server) issue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var body issueBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if body.TTLSeconds <= 0 {
		body.TTLSeconds = 300
	}
	issued, err := s.Engine.Issue(engine.IssueRequest{
		ID: body.ID, TenantID: body.TenantID, SPID: body.SPID,
		UserID: body.UserID, TTLSeconds: body.TTLSeconds,
	})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"ticket": issued.Ticket})
}

func (s *Server) validate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var body validateBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	result := s.Engine.Validate(r.Context(), engine.ValidateRequest{
		Seq: body.Seq, Ticket: body.Ticket, PresentSP: body.PresentSP,
	})
	writeJSON(w, http.StatusOK, result)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
