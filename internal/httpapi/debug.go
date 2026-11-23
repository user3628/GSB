package httpapi

import (
	"net/http"

	"leasearbiter/internal/arbiter"
)

// RegisterDebug mounts fault-injection routes. It is only called when the
// operator explicitly enables fault injection; production startup never mounts
// these handlers.
func (s *Server) RegisterDebug() {
	s.mux.HandleFunc("/debug/fault/acquire", s.debugAcquire)
	s.mux.HandleFunc("/debug/fault/recycle", s.debugRecycle)
	s.mux.HandleFunc("/debug/fault/commit", s.debugCommit)
}

func (s *Server) debugAcquire(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, &arbiter.APIError{Code: arbiter.CodeBadRequest, Message: "POST required"})
		return
	}
	var req struct {
		RowID  string `json:"row_id"`
		Holder string `json:"holder"`
		TTLMs  int64  `json:"ttl_ms"`
	}
	if err := decode(r, &req); err != nil {
		writeAPIError(w, &arbiter.APIError{Code: arbiter.CodeBadRequest, Message: err.Error()})
		return
	}
	id, err := s.svc.Engine().InjectPreparedAcquire(req.RowID, req.Holder, req.TTLMs)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeResult(w, map[string]any{"prepared_intent_id": id, "warning": "intent intentionally left uncompleted"})
}

func (s *Server) debugRecycle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, &arbiter.APIError{Code: arbiter.CodeBadRequest, Message: "POST required"})
		return
	}
	var req struct {
		RowID      string `json:"row_id"`
		RowVersion int64  `json:"row_version"`
	}
	if err := decode(r, &req); err != nil {
		writeAPIError(w, &arbiter.APIError{Code: arbiter.CodeBadRequest, Message: err.Error()})
		return
	}
	id, err := s.svc.Engine().InjectPreparedRecycle(req.RowID, req.RowVersion)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeResult(w, map[string]any{"prepared_intent_id": id})
}

func (s *Server) debugCommit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, &arbiter.APIError{Code: arbiter.CodeBadRequest, Message: "POST required"})
		return
	}
	var req struct {
		RowID          string `json:"row_id"`
		Holder         string `json:"holder"`
		LeaseVersion   int64  `json:"lease_version"`
		BaseRowVersion int64  `json:"base_row_version"`
		Content        string `json:"content"`
	}
	if err := decode(r, &req); err != nil {
		writeAPIError(w, &arbiter.APIError{Code: arbiter.CodeBadRequest, Message: err.Error()})
		return
	}
	id, err := s.svc.Engine().InjectPreparedCommit(req.RowID, req.Holder,
		req.LeaseVersion, req.BaseRowVersion, req.Content)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeResult(w, map[string]any{"prepared_intent_id": id})
}
