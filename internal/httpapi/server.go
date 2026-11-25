package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"voiceline/internal/model"
	"voiceline/internal/policy"
)

type Server struct {
	Policies *policy.Store
	Mux      *http.ServeMux
}

func NewServer(policies *policy.Store) *Server {
	if policies == nil {
		policies = policy.NewStore(policy.Strategy{})
	}
	s := &Server{Policies: policies, Mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler { return s.Mux }

func (s *Server) routes() {
	s.Mux.HandleFunc("GET /healthz", s.health)
	s.Mux.HandleFunc("GET /v1/policy", s.getCurrent)
	s.Mux.HandleFunc("POST /v1/policy/snapshots", s.applySnapshot)
	s.Mux.HandleFunc("POST /v1/policy/patches", s.applyPatch)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "service": "voiceline"})
}

func (s *Server) getCurrent(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "snapshot": s.Policies.Current()})
}

type patchResponse struct {
	OK           bool                `json:"ok"`
	Snapshot     policy.Strategy     `json:"snapshot"`
	Invalidation policy.Invalidation `json:"invalidation"`
}

func (s *Server) applyPatch(w http.ResponseWriter, r *http.Request) {
	var patch policy.Patch
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		writeError(w, http.StatusBadRequest, model.NewError(model.CodeParamInvalid, "", "invalid JSON patch: %v", err))
		return
	}
	next, invalidation, err := s.Policies.ApplyPatch(patch)
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, patchResponse{OK: true, Snapshot: next, Invalidation: invalidation})
}

type snapshotRequest struct {
	policy.Strategy
	Snapshot      *policy.Strategy `json:"snapshot"`
	AllowRollback bool             `json:"allow_rollback"`
}

func (s *Server) applySnapshot(w http.ResponseWriter, r *http.Request) {
	var req snapshotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, model.NewError(model.CodeParamInvalid, "", "invalid JSON snapshot: %v", err))
		return
	}
	strategy := req.Strategy
	if req.Snapshot != nil {
		strategy = *req.Snapshot
	}
	result, err := s.Policies.ApplySnapshot(strategy, req.AllowRollback)
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "result": result})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeBusinessError(w http.ResponseWriter, err error) {
	var business *model.BusinessError
	if errors.As(err, &business) {
		status := http.StatusBadRequest
		switch business.Code {
		case model.CodeStaleBase, model.CodeStaleCheckpoint, model.CodeConflict:
			status = http.StatusConflict
		case model.CodeNotFound:
			status = http.StatusNotFound
		case model.CodeToleranceExceeded:
			status = http.StatusUnprocessableEntity
		}
		w.Header().Set("X-Error-Code", business.Code)
		writeJSON(w, status, map[string]any{"ok": false, "error": business})
		return
	}
	writeError(w, http.StatusInternalServerError, err)
}

func writeError(w http.ResponseWriter, status int, err error) {
	business, ok := model.AsBusinessError(err)
	if !ok {
		business = model.NewError("E_INTERNAL", "", "%v", err)
	}
	w.Header().Set("X-Error-Code", business.Code)
	writeJSON(w, status, map[string]any{"ok": false, "error": business})
}
