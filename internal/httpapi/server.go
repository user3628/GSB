package httpapi

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"meshmigrate/internal/engine"
	"meshmigrate/internal/model"
	"meshmigrate/internal/store"
)

const maxBodyBytes = 16 << 20

type Server struct {
	engine    *engine.Engine
	store     *store.Store
	testHooks bool
	mux       *http.ServeMux
}

func New(eng *engine.Engine, st *store.Store, testHooks bool) *Server {
	s := &Server{engine: eng, store: st, testHooks: testHooks, mux: http.NewServeMux()}
	s.mux.HandleFunc("/healthz", s.handleHealth)
	s.mux.HandleFunc("/v1/matrix", s.handleMatrix)
	s.mux.HandleFunc("/v1/negotiate", s.handleNegotiate)
	s.mux.HandleFunc("/v1/import", s.handleImport)
	s.mux.HandleFunc("/v1/policies", s.handlePolicies)
	s.mux.HandleFunc("/v1/policies/", s.handlePolicyDetail)
	s.mux.HandleFunc("/v1/derived", s.handleDerived)
	s.mux.HandleFunc("/v1/versions", s.handleVersions)
	s.mux.HandleFunc("/v1/imports/", s.handleReport)
	s.mux.HandleFunc("/v1/recovery-report", s.handleRecoveryReport)
	s.mux.HandleFunc("/v1/debug/pending-intents", s.handlePendingIntents)
	return s
}

func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		s.mux.ServeHTTP(w, r)
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	active, err := s.store.ActiveVersion(r.Context())
	if err != nil {
		writeJSON(w, 500, map[string]any{"status": "error", "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"status": "ok", "activeSnapshotVersion": active, "loopbackOnly": true})
}

func (s *Server) handleMatrix(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	writeJSON(w, 200, s.engine.Matrix())
}

func (s *Server) handleNegotiate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	truncation := r.URL.Query().Get("truncation")
	format := formatHint(r)
	out, ae := s.engine.Negotiate(body, format, truncation)
	status := 200
	if ae != nil {
		status = ae.Status
	}
	writeRawJSON(w, status, out)
}

func (s *Server) handleImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	requestID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
	if requestID == "" {
		ae := &engine.APIError{Code: engine.CodeInvalidRequest, Message: "X-Request-ID header is required", Status: 400}
		writeRawJSON(w, ae.Status, ae.Envelope())
		return
	}
	if !s.testHooks && (r.Header.Get("X-Test-Pause-After-Intent") != "" || r.Header.Get("X-Test-Pause-Mid-Rebuild") != "") {
		ae := &engine.APIError{Code: engine.CodeInvalidRequest, Message: "test pause headers are disabled", Status: 400}
		writeRawJSON(w, ae.Status, ae.Envelope())
		return
	}
	resp := s.engine.Import(r.Context(), engine.ImportOptions{
		RequestID:        requestID,
		Body:             body,
		FormatHint:       formatHint(r),
		Truncation:       r.URL.Query().Get("truncation"),
		PauseAfterIntent: s.testHooks && r.Header.Get("X-Test-Pause-After-Intent") == "1",
		PauseMidRebuild:  s.testHooks && r.Header.Get("X-Test-Pause-Mid-Rebuild") == "1",
	})
	if resp.Status == 0 {
		resp.Status = 500
	}
	writeRawJSON(w, resp.Status, resp.Body)
}

func (s *Server) handlePolicies(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	version, ok := parseVersion(w, r)
	if !ok {
		return
	}
	policyID := r.URL.Query().Get("policyId")
	out, ae := s.engine.Policies(r.Context(), version, policyID)
	writeEngineResult(w, out, ae)
}

func (s *Server) handlePolicyDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	raw := strings.TrimPrefix(r.URL.Path, "/v1/policies/")
	policyID, err := url.PathUnescape(raw)
	if err != nil || policyID == "" {
		writeJSON(w, 400, map[string]any{"error": map[string]any{"code": engine.CodeInvalidRequest, "message": "invalid policy id"}})
		return
	}
	version, ok := parseVersion(w, r)
	if !ok {
		return
	}
	out, ae := s.engine.Policies(r.Context(), version, policyID)
	writeEngineResult(w, out, ae)
}

func (s *Server) handleDerived(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	version, ok := parseVersion(w, r)
	if !ok {
		return
	}
	out, ae := s.engine.Derived(r.Context(), version)
	writeEngineResult(w, out, ae)
}

func (s *Server) handleVersions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	out, ae := s.engine.VersionList(r.Context())
	writeEngineResult(w, out, ae)
}

func (s *Server) handleReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	reportID := strings.TrimPrefix(r.URL.Path, "/v1/imports/")
	if reportID == "" {
		writeJSON(w, 400, map[string]any{"error": map[string]any{"code": engine.CodeInvalidRequest, "message": "report id is required"}})
		return
	}
	out, ae := s.engine.Report(r.Context(), reportID)
	writeEngineResult(w, out, ae)
}

func (s *Server) handleRecoveryReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	out, ae := s.engine.RecoveryReport(r.Context())
	writeEngineResult(w, out, ae)
}

func (s *Server) handlePendingIntents(w http.ResponseWriter, r *http.Request) {
	if !s.testHooks {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	intents, err := s.store.ListPendingIntents(r.Context())
	if err != nil {
		writeJSON(w, 500, map[string]any{"error": map[string]any{"code": engine.CodeInternal, "message": err.Error()}})
		return
	}
	writeJSON(w, 200, map[string]any{"pendingIntents": intents})
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		ae := &engine.APIError{Code: engine.CodeInvalidRequest, Message: "read request body: " + err.Error(), Status: 400}
		writeRawJSON(w, ae.Status, ae.Envelope())
		return nil, false
	}
	return body, true
}

func formatHint(r *http.Request) string {
	f := r.URL.Query().Get("format")
	if f == "" {
		f = r.Header.Get("X-Format")
	}
	if f == "" {
		return model.FormatAuto
	}
	return f
}

func parseVersion(w http.ResponseWriter, r *http.Request) (uint64, bool) {
	raw := r.URL.Query().Get("version")
	if raw == "" {
		raw = r.Header.Get("X-Policy-Version")
	}
	if raw == "" || strings.EqualFold(raw, "active") {
		return 0, true
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || v == 0 {
		ae := &engine.APIError{Code: engine.CodeInvalidRequest, Message: "version must be a positive integer or 'active'", Status: 400}
		writeRawJSON(w, ae.Status, ae.Envelope())
		return 0, false
	}
	return v, true
}

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeJSON(w, 405, map[string]any{"error": map[string]any{"code": engine.CodeInvalidRequest, "message": "method not allowed"}})
}

func writeEngineResult(w http.ResponseWriter, body []byte, ae *engine.APIError) {
	if ae != nil {
		writeRawJSON(w, ae.Status, body)
		return
	}
	writeRawJSON(w, 200, body)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "encode response: "+err.Error(), 500)
		return
	}
	writeRawJSON(w, status, b)
}

func writeRawJSON(w http.ResponseWriter, status int, b []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if _, err := w.Write(b); err != nil {
		log.Printf("write response: %v", err)
	}
}
