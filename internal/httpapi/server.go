package httpapi

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"mime"
	"net/http"
	"strconv"
	"strings"

	"gatewayfreshness.local/workbench/internal/freshness"
	"gatewayfreshness.local/workbench/internal/service"
)

//go:embed web/*
var webFS embed.FS

type Server struct {
	svc           *service.Service
	maxBody       int64
	defaultBudget int
	handler       http.Handler
}

func New(svc *service.Service, maxBody int64) *Server {
	s := &Server{svc: svc, maxBody: maxBody, defaultBudget: svc.Config().QueryBudgetMS}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/api/v1/config", s.handleConfig)
	mux.HandleFunc("/api/v1/samples", s.handleSamples)
	mux.HandleFunc("/api/v1/samples/", s.handleSample)
	mux.HandleFunc("/api/v1/adjudications", s.handleAdjudications)
	mux.HandleFunc("/api/v1/adjudications/", s.handleDecision)
	mux.HandleFunc("/api/v1/diagnostics/snapshot", s.handleSnapshot)
	mux.HandleFunc("/api/v1/diagnostics/occupancy", s.handleOccupancy)
	mux.HandleFunc("/api/v1/diagnostics/nonces", s.handleNonces)
	mux.HandleFunc("/api/v1/diagnostics/trails", s.handleTrails)
	mux.HandleFunc("/api/v1/diagnostics/mode-differences", s.handleModeDifferences)
	mux.HandleFunc("/api/v1/diagnostics/inconsistencies", s.handleInconsistencies)
	mux.HandleFunc("/api/v1/diagnostics/recovery", s.handleRecovery)
	mux.HandleFunc("/api/v1/diagnostics/recovery/history", s.handleRecoveryHistory)
	mux.HandleFunc("/", s.handleStatic)
	s.handler = withRecovery(mux)
	return s
}

func (s *Server) Handler() http.Handler { return s.handler }

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "data": map[string]any{
		"service": "gateway-freshness-workbench", "status": "ok",
		"config_version": s.svc.Config().Version, "loopback_only": true,
	}})
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "data": s.svc.Config()})
}

func (s *Server) handleAdjudications(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var in freshness.BatchInput
	if err := dec.Decode(&in); err != nil {
		writeError(w, decodeError(err, s.maxBody))
		return
	}
	if err := ensureSingleJSON(dec); err != nil {
		writeError(w, freshness.NewAppError(freshness.CodeRequestInvalid, "request body must contain exactly one JSON object", nil))
		return
	}
	decision, err := s.svc.Adjudicate(r.Context(), in)
	if err != nil {
		writeError(w, asAppError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "data": decision})
}

func (s *Server) handleDecision(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/adjudications/")
	if id == "" || strings.Contains(id, "/") {
		writeError(w, freshness.NewAppError(freshness.CodeRequestInvalid, "batch id is required", nil))
		return
	}
	decision, err := s.svc.GetDecision(r.Context(), id)
	if err != nil {
		writeError(w, asAppError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "data": decision})
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	s.snapshot(w, r, nil)
}

func (s *Server) handleOccupancy(w http.ResponseWriter, r *http.Request) {
	s.snapshot(w, r, []string{"occupancy"})
}
func (s *Server) handleNonces(w http.ResponseWriter, r *http.Request) {
	s.snapshot(w, r, []string{"nonces"})
}
func (s *Server) handleTrails(w http.ResponseWriter, r *http.Request) {
	s.snapshot(w, r, []string{"trails"})
}

func (s *Server) snapshot(w http.ResponseWriter, r *http.Request, forced []string) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	q := r.URL.Query()
	version, err := parseInt64(q.Get("version"), 0)
	if err != nil {
		writeError(w, freshness.NewAppError(freshness.CodeRequestInvalid, "version must be an integer", nil))
		return
	}
	budget, err := parseInt(q.Get("budget_ms"), s.defaultBudget)
	if err != nil {
		writeError(w, freshness.NewAppError(freshness.CodeRequestInvalid, "budget_ms must be an integer", nil))
		return
	}
	limit, err := parseInt(q.Get("trail_limit"), 500)
	if err != nil {
		writeError(w, freshness.NewAppError(freshness.CodeRequestInvalid, "trail_limit must be an integer", nil))
		return
	}
	include := forced
	if include == nil {
		if raw := q.Get("include"); raw != "" {
			include = strings.Split(raw, ",")
		}
	}
	snapshot, err := s.svc.Snapshot(r.Context(), version, budget, include, q.Get("client_id"), q.Get("batch_id"), limit)
	if err != nil {
		writeError(w, asAppError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "data": snapshot})
}

func (s *Server) handleModeDifferences(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	q := r.URL.Query()
	batchID := q.Get("batch_id")
	if batchID == "" {
		writeError(w, freshness.NewAppError(freshness.CodeRequestInvalid, "batch_id is required", nil))
		return
	}
	budget, err := parseInt(q.Get("budget_ms"), s.defaultBudget)
	if err != nil {
		writeError(w, freshness.NewAppError(freshness.CodeRequestInvalid, "budget_ms must be an integer", nil))
		return
	}
	report, err := s.svc.ModeDifferences(r.Context(), batchID, budget)
	if err != nil {
		writeError(w, asAppError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "data": report})
}

type inconsistencyRequest struct {
	BatchID  string                 `json:"batch_id"`
	BudgetMS *int                   `json:"budget_ms,omitempty"`
	Views    []freshness.ClientView `json:"views"`
}

func (s *Server) handleInconsistencies(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		q := r.URL.Query()
		version, err := parseInt64(q.Get("picked_window_version"), 0)
		if err != nil {
			writeError(w, freshness.NewAppError(freshness.CodeRequestInvalid, "picked_window_version must be an integer", nil))
			return
		}
		budget, err := parseInt(q.Get("budget_ms"), s.defaultBudget)
		if err != nil {
			writeError(w, freshness.NewAppError(freshness.CodeRequestInvalid, "budget_ms must be an integer", nil))
			return
		}
		report, err := s.svc.Inconsistencies(r.Context(), q.Get("batch_id"), []freshness.ClientView{{
			ClientID: q.Get("client_id"), PickedWindowVersion: version, PickedConfigVersion: q.Get("picked_config_version"),
		}}, budget)
		if err != nil {
			writeError(w, asAppError(err))
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "data": report})
	case http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, s.maxBody)
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		var in inconsistencyRequest
		if err := dec.Decode(&in); err != nil {
			writeError(w, decodeError(err, s.maxBody))
			return
		}
		budget := s.defaultBudget
		if in.BudgetMS != nil {
			budget = *in.BudgetMS
		}
		report, err := s.svc.Inconsistencies(r.Context(), in.BatchID, in.Views, budget)
		if err != nil {
			writeError(w, asAppError(err))
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "data": report})
	default:
		methodNotAllowed(w, http.MethodGet, http.MethodPost)
	}
}

func (s *Server) handleRecovery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	report, err := s.svc.Store().LatestRecovery(r.Context())
	if err != nil {
		writeError(w, freshness.NewAppError(freshness.CodeRecoveryUnavailable, "read recovery report failed", err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "data": report})
}

func (s *Server) handleRecoveryHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	limit, err := parseInt(r.URL.Query().Get("limit"), 20)
	if err != nil {
		writeError(w, freshness.NewAppError(freshness.CodeRequestInvalid, "limit must be an integer", nil))
		return
	}
	reports, err := s.svc.RecoveryHistory(r.Context(), limit)
	if err != nil {
		writeError(w, freshness.NewAppError(freshness.CodeRecoveryUnavailable, "read recovery history failed", err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "data": reports})
}

func (s *Server) handleSamples(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	names, err := sampleNames()
	if err != nil {
		writeError(w, freshness.NewAppError(freshness.CodeInternal, "read embedded samples failed", err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "data": names})
}

func (s *Server) handleSample(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/api/v1/samples/")
	data, err := sampleFile(name)
	if err != nil {
		writeError(w, freshness.NewAppError(freshness.CodeBatchNotFound, "sample not found", map[string]any{"name": name}))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (s *Server) handleStatic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		path = "index.html"
	}
	if strings.Contains(path, "..") {
		http.NotFound(w, r)
		return
	}
	data, err := fs.ReadFile(webFS, "web/"+path)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if ct := mime.TypeByExtension("." + extension(path)); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func extension(path string) string {
	i := strings.LastIndexByte(path, '.')
	if i < 0 {
		return ""
	}
	return path[i+1:]
}

func withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("panic serving %s: %v", r.URL.Path, rec)
				writeError(w, freshness.NewAppError(freshness.CodeInternal, "internal server error", nil))
			}
		}()
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

func decodeError(err error, maxBody int64) *freshness.AppError {
	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		return freshness.NewAppError(freshness.CodeRequestTooLarge, "request body exceeds configured limit", map[string]any{"max_body_bytes": maxBody})
	}
	if errors.Is(err, io.EOF) {
		return freshness.NewAppError(freshness.CodeRequestInvalid, "request body is empty", nil)
	}
	return freshness.NewAppError(freshness.CodeRequestInvalid, "request JSON is invalid or contains an int64 overflow", map[string]any{"detail": err.Error()})
}

func ensureSingleJSON(dec *json.Decoder) error {
	var extra any
	err := dec.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return fmt.Errorf("extra JSON value")
	}
	return err
}

func asAppError(err error) *freshness.AppError {
	var appErr *freshness.AppError
	if errors.As(err, &appErr) {
		return appErr
	}
	return freshness.NewAppError(freshness.CodeInternal, "internal error", err.Error())
}

func writeError(w http.ResponseWriter, err *freshness.AppError) {
	status := http.StatusInternalServerError
	switch err.Code {
	case freshness.CodeRequestInvalid:
		status = http.StatusBadRequest
	case freshness.CodeRequestTooLarge:
		status = http.StatusRequestEntityTooLarge
	case freshness.CodeConflictingRequest:
		status = http.StatusConflict
	case freshness.CodeBatchNotFound:
		status = http.StatusNotFound
	case freshness.CodeVersionInvalid:
		status = http.StatusBadRequest
	case freshness.CodeRecoveryUnavailable:
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{"ok": false, "error": err})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func methodNotAllowed(w http.ResponseWriter, allowed ...string) {
	w.Header().Set("Allow", strings.Join(allowed, ", "))
	writeJSON(w, http.StatusMethodNotAllowed, map[string]any{
		"ok":    false,
		"error": freshness.AppError{Code: freshness.CodeRequestInvalid, Message: "method not allowed"},
	})
}

func parseInt(raw string, fallback int) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, err
	}
	return v, nil
}

func parseInt64(raw string, fallback int64) (int64, error) {
	if strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, err
	}
	return v, nil
}
