// Package httpapi exposes the local audit test endpoints.
package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"configaudit/internal/audit"
	"configaudit/internal/fixedpoint"
)

// Server wires the audit service to net/http.
type Server struct {
	Service *audit.Service
	Mux     *http.ServeMux
}

// NewServer constructs the routed server.
func NewServer(svc *audit.Service) *Server {
	s := &Server{Service: svc, Mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.Mux.HandleFunc("GET /healthz", s.health)
	s.Mux.HandleFunc("POST /v1/audits", s.submitAudit)
	s.Mux.HandleFunc("POST /v1/audits/incremental", s.incremental)
	s.Mux.HandleFunc("GET /v1/snapshots/latest", s.latestSnapshot)
	s.Mux.HandleFunc("GET /v1/snapshots/{version}", s.snapshotByVersion)
	s.Mux.HandleFunc("GET /v1/audits/{requestID}", s.getAudit)
	s.Mux.HandleFunc("POST /v1/recover", s.recover)
}

type submitRequest struct {
	RequestID string          `json:"request_id"`
	Payload   json.RawMessage `json:"payload"`
}

type incrementalRequest struct {
	RequestID    string          `json:"request_id"`
	BaseVersion  int64           `json:"base_version"`
	AffectedKeys []string        `json:"affected_keys"`
	Payload      json.RawMessage `json:"payload"`
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) submitAudit(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	if req.RequestID == "" {
		writeError(w, fixedpoint.NewError(fixedpoint.EInvalidInput, "request_id is required"))
		return
	}
	snap, err := s.Service.FullAudit(r.Context(), req.RequestID, req.Payload)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"version": snap.Version, "checksum": snap.Checksum, "created_at": snap.CreatedAt,
	})
}

func (s *Server) incremental(w http.ResponseWriter, r *http.Request) {
	var req incrementalRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	if req.RequestID == "" {
		writeError(w, fixedpoint.NewError(fixedpoint.EInvalidInput, "request_id is required"))
		return
	}
	snap, err := s.Service.Incremental(r.Context(), req.RequestID, req.BaseVersion, req.AffectedKeys, req.Payload)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"version": snap.Version, "checksum": snap.Checksum,
	})
}

func (s *Server) latestSnapshot(w http.ResponseWriter, r *http.Request) {
	snap, err := s.Service.QuerySnapshot(r.Context(), 0)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) snapshotByVersion(w http.ResponseWriter, r *http.Request) {
	v, err := strconv.ParseInt(r.PathValue("version"), 10, 64)
	if err != nil {
		writeError(w, fixedpoint.NewError(fixedpoint.EInvalidInput, "invalid version"))
		return
	}
	snap, err := s.Service.QuerySnapshot(r.Context(), v)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) getAudit(w http.ResponseWriter, r *http.Request) {
	requestID := r.PathValue("requestID")
	// Reuse the store through a service helper below.
	row, err := s.Service.Store().GetAudit(r.Context(), requestID)
	if err != nil {
		writeError(w, err)
		return
	}
	if row == nil {
		writeError(w, fixedpoint.NewError(fixedpoint.ENotFound, "request %s not found", requestID))
		return
	}
	resp := map[string]any{
		"request_id": row.RequestID,
		"status":     row.Status,
		"version":    row.Version,
		"base":       row.Base,
	}
	if len(row.Result) > 0 {
		var raw json.RawMessage = row.Result
		resp["result"] = raw
	}
	if row.ErrorCode.Valid {
		resp["error_code"] = row.ErrorCode.String
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) recover(w http.ResponseWriter, r *http.Request) {
	rep, err := s.Service.Recover(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

func decode(r *http.Request, v any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fixedpoint.NewError(fixedpoint.EInvalidInput, "invalid json body: %v", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	code := fixedpoint.CodeOf(err)
	status := http.StatusInternalServerError
	switch code {
	case fixedpoint.ENotFound:
		status = http.StatusNotFound
	case fixedpoint.EInvalidInput, fixedpoint.EUnitInvalid, fixedpoint.ESpecInvalid,
		fixedpoint.ENumericOverflow, fixedpoint.EPriorityConflict, fixedpoint.EErrorBoundExceeded,
		fixedpoint.ESnapshotInvalid, fixedpoint.EStaleBase:
		status = http.StatusUnprocessableEntity
	case fixedpoint.EConflictingRequest:
		status = http.StatusConflict
	}
	var ce *fixedpoint.Error
	body := map[string]any{"error_code": string(code), "message": err.Error()}
	if errors.As(err, &ce) {
		if ce.Field != "" {
			body["field"] = ce.Field
		}
		if ce.Line > 0 {
			body["line"] = ce.Line
		}
		if ce.Step > 0 {
			body["step"] = ce.Step
		}
		if ce.ItemID != "" {
			body["item_id"] = ce.ItemID
		}
	}
	writeJSON(w, status, body)
}
