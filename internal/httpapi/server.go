package httpapi

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"example.com/replicabudget/internal/apperr"
	"example.com/replicabudget/internal/policy"
	"example.com/replicabudget/internal/store"
)

type Server struct {
	Store *store.Store
	Log   *log.Logger
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /v1/compatibility", s.compatibility)
	mux.HandleFunc("POST /v1/import", s.importPolicies)
	mux.HandleFunc("POST /v1/migrate", s.migrate)
	mux.HandleFunc("POST /v1/quota-groups", s.setQuotaGroups)
	mux.HandleFunc("PUT /v1/quota-groups/{id}", s.updateQuotaGroup)
	mux.HandleFunc("POST /v1/incremental", s.incremental)
	mux.HandleFunc("GET /v1/state", s.state)
	mux.HandleFunc("GET /v1/recovery-report", s.recoveryReport)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, apperr.WithDetails(apperr.NotFound, "HTTP route not found", map[string]any{"method": r.Method, "path": r.URL.Path}))
	})
	return s.logging(mux)
}

func (s *Server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		if s.Log != nil {
			s.Log.Printf("%s %s %s", r.Method, r.URL.RequestURI(), time.Since(start).Round(time.Millisecond))
		}
	})
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) compatibility(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"matrix": policy.CompatibilityMatrix(), "current_format_version": policy.CurrentFormatVersion,
		"default_weight_permille":    policy.DefaultWeightPermille,
		"default_threshold_micros":   policy.DefaultThresholdMicros,
		"default_tolerance_permille": policy.DefaultRelativeTolerancePermille,
		"index_scale":                policy.IndexScale,
	})
}

func (s *Server) importPolicies(w http.ResponseWriter, r *http.Request) {
	data, err := readBody(r, 64<<20)
	if err != nil {
		writeError(w, err)
		return
	}
	base, err := queryInt(r, "base_version", 0)
	if err != nil {
		writeError(w, err)
		return
	}
	threshold, err := optionalQueryInt64(r, "threshold_micros")
	if err != nil {
		writeError(w, err)
		return
	}
	tolerance, err := optionalQueryInt(r, "tolerance_permille")
	if err != nil {
		writeError(w, err)
		return
	}
	req := store.ImportRequest{
		BaseVersion: base, Data: data, FormatHint: r.URL.Query().Get("format"),
		Mode: r.URL.Query().Get("mode"), ThresholdMicros: threshold,
	}
	if tolerance != nil {
		req.Tolerance = &policy.Tolerance{RelativePermille: *tolerance}
	}
	result, err := s.Store.Import(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) migrate(w http.ResponseWriter, r *http.Request) {
	data, err := readBody(r, 64<<20)
	if err != nil {
		writeError(w, err)
		return
	}
	base, err := queryInt(r, "base_version", 0)
	if err != nil {
		writeError(w, err)
		return
	}
	threshold, err := optionalQueryInt64(r, "threshold_micros")
	if err != nil {
		writeError(w, err)
		return
	}
	tolerance, err := optionalQueryInt(r, "tolerance_permille")
	if err != nil {
		writeError(w, err)
		return
	}
	req := store.ImportRequest{BaseVersion: base, Data: data, FormatHint: policy.FormatLegacyJSON, Mode: "replace", ThresholdMicros: threshold}
	if tolerance != nil {
		req.Tolerance = &policy.Tolerance{RelativePermille: *tolerance}
	}
	result, err := s.Store.Import(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) setQuotaGroups(w http.ResponseWriter, r *http.Request) {
	base, err := queryInt(r, "base_version", 0)
	if err != nil {
		writeError(w, err)
		return
	}
	data, err := readBody(r, 8<<20)
	if err != nil {
		writeError(w, err)
		return
	}
	var groups []policy.QuotaGroup
	if err := json.Unmarshal(data, &groups); err != nil {
		writeError(w, apperr.New(apperr.SpecInvalid, "quota body must be a JSON array: "+err.Error()))
		return
	}
	result, err := s.Store.SetQuotaGroups(r.Context(), base, groups)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) updateQuotaGroup(w http.ResponseWriter, r *http.Request) {
	base, err := queryInt(r, "base_version", 0)
	if err != nil {
		writeError(w, err)
		return
	}
	data, err := readBody(r, 1<<20)
	if err != nil {
		writeError(w, err)
		return
	}
	var group policy.QuotaGroup
	if err := json.Unmarshal(data, &group); err != nil {
		writeError(w, apperr.New(apperr.SpecInvalid, "quota body must be a JSON object: "+err.Error()))
		return
	}
	group.ID = r.PathValue("id")
	result, err := s.Store.UpdateQuotaGroup(r.Context(), base, group)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) incremental(w http.ResponseWriter, r *http.Request) {
	data, err := readBody(r, 8<<20)
	if err != nil {
		writeError(w, err)
		return
	}
	var req store.IncrementalRequest
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, apperr.New(apperr.SpecInvalid, "invalid incremental request: "+err.Error()))
		return
	}
	result, err := s.Store.ApplyIncremental(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) state(w http.ResponseWriter, r *http.Request) {
	state, err := s.Store.State(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (s *Server) recoveryReport(w http.ResponseWriter, r *http.Request) {
	report, err := s.Store.RecoveryReport(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func readBody(r *http.Request, limit int64) ([]byte, error) {
	if r.Body == nil {
		return nil, apperr.New(apperr.SpecInvalid, "request body is required")
	}
	defer r.Body.Close()
	data, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		return nil, apperr.New(apperr.Internal, "reading request body: "+err.Error())
	}
	if int64(len(data)) > limit {
		return nil, apperr.WithDetails(apperr.SpecInvalid, "request body exceeds limit", map[string]any{"limit_bytes": limit})
	}
	return data, nil
}

func queryInt(r *http.Request, key string, fallback int) (int, error) {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, apperr.WithDetails(apperr.SpecInvalid, "query parameter must be an integer", map[string]any{"parameter": key})
	}
	return v, nil
}

func optionalQueryInt(r *http.Request, key string) (*int, error) {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return nil, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return nil, apperr.WithDetails(apperr.SpecInvalid, "query parameter must be an integer", map[string]any{"parameter": key})
	}
	return &v, nil
}

func optionalQueryInt64(r *http.Request, key string) (*int64, error) {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return nil, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil, apperr.WithDetails(apperr.SpecInvalid, "query parameter must be an integer", map[string]any{"parameter": key})
	}
	return &v, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(value)
}

type errorEnvelope struct {
	Error *apperr.Error `json:"error"`
}

func writeError(w http.ResponseWriter, err error) {
	be := apperr.As(err)
	status := http.StatusInternalServerError
	switch be.Code {
	case apperr.SpecInvalid:
		status = http.StatusBadRequest
	case apperr.RecordCorrupt:
		status = http.StatusUnprocessableEntity
	case apperr.FormatUnsupported:
		status = http.StatusUnsupportedMediaType
	case apperr.MigrationAmbiguous, apperr.StaleBase:
		status = http.StatusConflict
	case apperr.NotFound:
		status = http.StatusNotFound
	}
	writeJSON(w, status, errorEnvelope{Error: be})
}
