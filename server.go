package grayrouter

import (
	"embed"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
)

//go:embed web/index.html
var indexHTML []byte

//go:embed web
var webFS embed.FS

type APIEnvelope struct {
	OK    bool      `json:"ok"`
	Data  any       `json:"data,omitempty"`
	Error *APIError `json:"error,omitempty"`
}

type HTTPServer struct {
	Host       *Host
	Mux        *http.ServeMux
	SamplesDir string
}

func NewHTTPServer(host *Host, samplesDir string) *HTTPServer {
	server := &HTTPServer{Host: host, Mux: http.NewServeMux(), SamplesDir: samplesDir}
	server.routes()
	return server
}

func (s *HTTPServer) Handler() http.Handler { return s.Mux }

func (s *HTTPServer) routes() {
	mux := s.Mux
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("GET /api/health", func(w http.ResponseWriter, r *http.Request) {
		writeOK(w, http.StatusOK, map[string]any{"status": "ok", "service": "grayrouter-workbench", "loopback_only": true})
	})
	mux.HandleFunc("GET /api/state", s.handleState)
	mux.HandleFunc("GET /api/samples/bundle", s.handleSampleBundle)
	mux.HandleFunc("POST /api/samples/write", s.handleWriteSamples)
	mux.HandleFunc("POST /api/samples/load-baseline", s.handleLoadBaseline)
	mux.HandleFunc("POST /api/samples/load-delta", s.handleLoadDelta)
	mux.HandleFunc("POST /api/graphs/validate", s.handleValidate)
	mux.HandleFunc("POST /api/snapshots", s.handleSnapshot)
	mux.HandleFunc("POST /api/deltas", s.handleDelta)
	mux.HandleFunc("GET /api/migration", s.handleMigration)
	mux.HandleFunc("GET /api/artifacts", s.handleArtifacts)
	mux.HandleFunc("POST /api/infer", s.handleInfer)
	mux.HandleFunc("POST /api/gray/decide", s.handleGrayDecide)
	mux.HandleFunc("POST /api/trail/decisions", s.handleTrail)
	mux.HandleFunc("GET /api/recovery", s.handleRecoveryGet)
	mux.HandleFunc("POST /api/recovery/run", s.handleRecoveryRun)
	static, _ := fs.Sub(webFS, "web")
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(static))))
}

func (s *HTTPServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/api/") {
		writeError(w, NewAPIError(CodeNotFound, "api route not found", map[string]any{"method": r.Method, "path": r.URL.Path}))
		return
	}
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeError(w, NewAPIError(CodeInvalidRequest, "method not allowed", map[string]any{"method": r.Method}))
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(indexHTML)
}

func writeOK(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(APIEnvelope{OK: true, Data: data})
}

func writeError(w http.ResponseWriter, err error) {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		apiErr = WrapInternal(err, nil).(*APIError)
	}
	status := http.StatusInternalServerError
	switch apiErr.Code {
	case CodeGraphInvalid, CodeInvalidRequest:
		status = http.StatusUnprocessableEntity
		if apiErr.Code == CodeInvalidRequest {
			status = http.StatusBadRequest
		}
	case CodeStaleBase, CodeConflictingUpdate, CodeVersionConflict, CodeIdempotency, CodeRecoveryAborted:
		status = http.StatusConflict
	case CodeNotFound:
		status = http.StatusNotFound
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(APIEnvelope{OK: false, Error: apiErr})
}

func decodeBody(r *http.Request, target any) error {
	defer r.Body.Close()
	data, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		return NewAPIError(CodeInvalidRequest, "cannot read request body", map[string]any{"reason": err.Error()})
	}
	if err := json.Unmarshal(data, target); err != nil {
		return NewAPIError(CodeInvalidRequest, "invalid JSON request body", map[string]any{"reason": err.Error()})
	}
	return nil
}

func requestID(r *http.Request, bodyID string) string {
	if id := strings.TrimSpace(r.Header.Get("Idempotency-Key")); id != "" {
		return id
	}
	if id := strings.TrimSpace(r.Header.Get("X-Request-ID")); id != "" {
		return id
	}
	return strings.TrimSpace(bodyID)
}

func (s *HTTPServer) handleState(w http.ResponseWriter, r *http.Request) {
	state, err := s.Host.State(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeOK(w, http.StatusOK, state)
}

func (s *HTTPServer) handleSampleBundle(w http.ResponseWriter, r *http.Request) {
	writeOK(w, http.StatusOK, SampleBundleFixed())
}

func (s *HTTPServer) handleWriteSamples(w http.ResponseWriter, r *http.Request) {
	if err := WriteSampleFiles(s.SamplesDir); err != nil {
		writeError(w, err)
		return
	}
	writeOK(w, http.StatusOK, map[string]any{"samples_dir": s.SamplesDir, "files": []string{"snapshot-v1.json", "delta-v2.json", "invalid-graph.json", "request-trail.json"}})
}

func (s *HTTPServer) handleLoadBaseline(w http.ResponseWriter, r *http.Request) {
	bundle := SampleBundleFixed()
	report, err := s.Host.SubmitSnapshot(r.Context(), CommitSnapshotInput{RequestID: "sample-baseline-v1", Snapshot: bundle.Baseline})
	if err != nil {
		writeError(w, err)
		return
	}
	writeOK(w, http.StatusOK, report)
}

func (s *HTTPServer) handleLoadDelta(w http.ResponseWriter, r *http.Request) {
	bundle := SampleBundleFixed()
	report, err := s.Host.SubmitDelta(r.Context(), CommitDeltaInput{RequestID: "sample-delta-v2-base1", Delta: bundle.Delta})
	if err != nil {
		writeError(w, err)
		return
	}
	writeOK(w, http.StatusOK, report)
}

type validateRequest struct {
	Graph *Graph        `json:"graph,omitempty"`
	Model *ModelVersion `json:"model,omitempty"`
}

func (s *HTTPServer) handleValidate(w http.ResponseWriter, r *http.Request) {
	var req validateRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, err)
		return
	}
	graph := req.Graph
	if graph == nil && req.Model != nil {
		graph = &req.Model.Graph
	}
	if graph == nil {
		writeError(w, NewAPIError(CodeInvalidRequest, "graph or model is required", nil))
		return
	}
	report := ValidateGraph(*graph)
	if !report.Valid {
		writeError(w, NewAPIError(CodeGraphInvalid, "operator graph validation failed", map[string]any{"errors": report.Errors}))
		return
	}
	writeOK(w, http.StatusOK, map[string]any{"valid": true, "validation": report, "signature": GraphSignature(*graph), "structural_signature": StructuralSignature(*graph)})
}

func (s *HTTPServer) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	var raw map[string]json.RawMessage
	if err := decodeBody(r, &raw); err != nil {
		writeError(w, err)
		return
	}
	var input CommitSnapshotInput
	if rawSnapshot, ok := raw["snapshot"]; ok {
		if err := json.Unmarshal(rawSnapshot, &input.Snapshot); err != nil {
			writeError(w, NewAPIError(CodeInvalidRequest, "invalid snapshot", map[string]any{"reason": err.Error()}))
			return
		}
		_ = json.Unmarshal(raw["request_id"], &input.RequestID)
	} else {
		if err := json.Unmarshal(canonicalJSON(raw), &input.Snapshot); err != nil {
			writeError(w, NewAPIError(CodeInvalidRequest, "body must be a Snapshot or CommitSnapshotInput", map[string]any{"reason": err.Error()}))
			return
		}
	}
	input.RequestID = requestID(r, input.RequestID)
	report, err := s.Host.SubmitSnapshot(r.Context(), input)
	if err != nil {
		writeError(w, err)
		return
	}
	writeOK(w, http.StatusOK, report)
}

func (s *HTTPServer) handleDelta(w http.ResponseWriter, r *http.Request) {
	var raw map[string]json.RawMessage
	if err := decodeBody(r, &raw); err != nil {
		writeError(w, err)
		return
	}
	var input CommitDeltaInput
	if rawDelta, ok := raw["delta"]; ok {
		if err := json.Unmarshal(rawDelta, &input.Delta); err != nil {
			writeError(w, NewAPIError(CodeInvalidRequest, "invalid delta", map[string]any{"reason": err.Error()}))
			return
		}
		_ = json.Unmarshal(raw["request_id"], &input.RequestID)
	} else {
		if err := json.Unmarshal(canonicalJSON(raw), &input.Delta); err != nil {
			writeError(w, NewAPIError(CodeInvalidRequest, "body must be a Delta or CommitDeltaInput", map[string]any{"reason": err.Error()}))
			return
		}
	}
	input.RequestID = requestID(r, input.RequestID)
	report, err := s.Host.SubmitDelta(r.Context(), input)
	if err != nil {
		writeError(w, err)
		return
	}
	writeOK(w, http.StatusOK, report)
}

func (s *HTTPServer) handleMigration(w http.ResponseWriter, r *http.Request) {
	var version int64
	if v := r.URL.Query().Get("version"); v != "" {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeError(w, NewAPIError(CodeInvalidRequest, "invalid version", map[string]any{"version": v}))
			return
		}
		version = parsed
	}
	report, err := s.Host.CommitReport(r.Context(), version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeOK(w, http.StatusOK, report)
}

func (s *HTTPServer) handleArtifacts(w http.ResponseWriter, r *http.Request) {
	items, err := s.Host.Artifacts(r.Context(), r.URL.Query().Get("model_key"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeOK(w, http.StatusOK, items)
}

func (s *HTTPServer) handleInfer(w http.ResponseWriter, r *http.Request) {
	var req InferRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, err)
		return
	}
	req.RequestID = requestID(r, req.RequestID)
	resp, err := s.Host.Infer(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeOK(w, http.StatusOK, resp)
}

func (s *HTTPServer) handleGrayDecide(w http.ResponseWriter, r *http.Request) {
	var req GrayDecisionRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, err)
		return
	}
	req.RequestID = requestID(r, req.RequestID)
	decision, err := s.Host.DecideGray(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeOK(w, http.StatusOK, decision)
}

func (s *HTTPServer) handleTrail(w http.ResponseWriter, r *http.Request) {
	bundle := SampleBundleFixed()
	decisions, err := s.Host.RequestTrailDecisions(r.Context(), bundle.Trail, r.URL.Query().Get("route_key"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeOK(w, http.StatusOK, map[string]any{"seed": bundle.Trail.Seed, "decisions": decisions})
}

func (s *HTTPServer) handleRecoveryGet(w http.ResponseWriter, r *http.Request) {
	report := s.Host.LatestRecoveryReport()
	if report == nil || r.URL.Query().Get("scan") == "1" {
		var err error
		report, err = s.Host.Recover(r.Context())
		if err != nil {
			writeError(w, err)
			return
		}
	}
	writeOK(w, http.StatusOK, report)
}

func (s *HTTPServer) handleRecoveryRun(w http.ResponseWriter, r *http.Request) {
	report, err := s.Host.Recover(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeOK(w, http.StatusOK, report)
}
