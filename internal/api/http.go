package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"mpckpt/internal/engine"
	"mpckpt/internal/model"
)

const maxRequestBody = 32 << 20

type Server struct {
	engine *engine.Engine
	mux    *http.ServeMux
}

type importHTTPRequest struct {
	CommitID   string `json:"commit_id"`
	Format     string `json:"format"`
	Version    *int   `json:"version,omitempty"`
	DataBase64 string `json:"data_base64,omitempty"`
	Data       string `json:"data,omitempty"`
}

func New(engine *engine.Engine) *Server {
	server := &Server{engine: engine, mux: http.NewServeMux()}
	server.mux.HandleFunc("GET /healthz", server.health)
	server.mux.HandleFunc("POST /v1/import", server.importManifest)
	server.mux.HandleFunc("POST /v1/strategy", server.submitStrategy)
	server.mux.HandleFunc("POST /v1/checkpoints", server.commitCheckpoint)
	server.mux.HandleFunc("POST /v1/incremental", server.submitIncremental)
	server.mux.HandleFunc("POST /v1/pools/reside", server.reside)
	server.mux.HandleFunc("GET /v1/state", server.state)
	server.mux.HandleFunc("GET /v1/pools", server.pools)
	server.mux.HandleFunc("GET /v1/precision", server.precision)
	server.mux.HandleFunc("GET /v1/derived", server.derived)
	server.mux.HandleFunc("GET /v1/recovery", server.recovery)
	server.mux.HandleFunc("GET /v1/imports", server.imports)
	server.mux.HandleFunc("GET /v1/compatibility", server.compatibilityDecisions)
	server.mux.HandleFunc("POST /v1/debug/crash-fault", server.debugCrashFault)
	server.mux.HandleFunc("/", server.notFound)
	return server
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost && r.Body != nil {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "store": s.engine.StorePath()})
}

func (s *Server) submitStrategy(w http.ResponseWriter, r *http.Request) {
	var raw json.RawMessage
	if !decodeJSON(w, r, &raw) {
		return
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		writeBizError(w, model.NewError(model.CodeInvalidJSON, "request body must be a JSON object", map[string]any{"detail": err.Error()}))
		return
	}
	var req engine.StrategyRequest
	if nested, ok := fields["strategy"]; ok {
		if commitRaw, exists := fields["commit_id"]; exists {
			if err := json.Unmarshal(commitRaw, &req.CommitID); err != nil {
				writeBizError(w, model.NewError(model.CodeInvalidJSON, "commit_id must be a string", map[string]any{"detail": err.Error()}))
				return
			}
		}
		if err := json.Unmarshal(nested, &req.Strategy); err != nil {
			writeBizError(w, model.NewError(model.CodeInvalidJSON, "strategy snapshot is invalid", map[string]any{"detail": err.Error()}))
			return
		}
	} else if nested, ok := fields["snapshot"]; ok {
		if commitRaw, exists := fields["commit_id"]; exists {
			if err := json.Unmarshal(commitRaw, &req.CommitID); err != nil {
				writeBizError(w, model.NewError(model.CodeInvalidJSON, "commit_id must be a string", map[string]any{"detail": err.Error()}))
				return
			}
		}
		if err := json.Unmarshal(nested, &req.Strategy); err != nil {
			writeBizError(w, model.NewError(model.CodeInvalidJSON, "strategy snapshot is invalid", map[string]any{"detail": err.Error()}))
			return
		}
	} else {
		var flat struct {
			CommitID string `json:"commit_id"`
			model.StrategySnapshot
		}
		if err := json.Unmarshal(raw, &flat); err != nil {
			writeBizError(w, model.NewError(model.CodeInvalidJSON, "flat strategy snapshot is invalid", map[string]any{"detail": err.Error()}))
			return
		}
		req.CommitID = flat.CommitID
		req.Strategy = flat.StrategySnapshot
	}
	response, berr := s.engine.SubmitStrategy(req)
	writeResult(w, response, berr)
}

func (s *Server) importManifest(w http.ResponseWriter, r *http.Request) {
	var req importHTTPRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	data := []byte(req.Data)
	if req.DataBase64 != "" {
		decoded, err := base64.StdEncoding.DecodeString(req.DataBase64)
		if err != nil {
			writeBizError(w, model.NewError(model.CodeSpecInvalid, "data_base64 is not valid base64", map[string]any{"detail": err.Error()}))
			return
		}
		data = decoded
	}
	if len(data) == 0 {
		writeBizError(w, model.NewError(model.CodeSpecInvalid, "import payload is empty", map[string]any{"fields": []string{"data", "data_base64"}}))
		return
	}
	response, berr := s.engine.Import(model.ImportRequest{CommitID: req.CommitID, Format: req.Format, Version: req.Version, Data: data})
	writeResult(w, response, berr)
}

func (s *Server) commitCheckpoint(w http.ResponseWriter, r *http.Request) {
	var req engine.CheckpointRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	response, berr := s.engine.CommitCheckpoint(req)
	writeResult(w, response, berr)
}

func (s *Server) submitIncremental(w http.ResponseWriter, r *http.Request) {
	var req engine.IncrementalRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	response, berr := s.engine.SubmitIncremental(req)
	writeResult(w, response, berr)
}

func (s *Server) reside(w http.ResponseWriter, r *http.Request) {
	var req engine.ResideRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	response, berr := s.engine.Reside(req)
	writeResult(w, response, berr)
}

func (s *Server) state(w http.ResponseWriter, r *http.Request) {
	version, ok := versionQuery(w, r)
	if !ok {
		return
	}
	response, berr := s.engine.State(version)
	if berr != nil {
		writeBizError(w, berr)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) pools(w http.ResponseWriter, r *http.Request) {
	version, ok := versionQuery(w, r)
	if !ok {
		return
	}
	response, berr := s.engine.Pools(version)
	if berr != nil {
		writeBizError(w, berr)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) precision(w http.ResponseWriter, r *http.Request) {
	version, ok := versionQuery(w, r)
	if !ok {
		return
	}
	response, berr := s.engine.Precision(version)
	if berr != nil {
		writeBizError(w, berr)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) derived(w http.ResponseWriter, r *http.Request) {
	version, ok := versionQuery(w, r)
	if !ok {
		return
	}
	response, berr := s.engine.Derived(version)
	if berr != nil {
		writeBizError(w, berr)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) recovery(w http.ResponseWriter, _ *http.Request) {
	events, berr := s.engine.Recovery()
	if berr != nil {
		writeBizError(w, berr)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

func (s *Server) imports(w http.ResponseWriter, r *http.Request) {
	version, ok := versionQuery(w, r)
	if !ok {
		return
	}
	response, berr := s.engine.ImportReports(version)
	if berr != nil {
		writeBizError(w, berr)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(response)
}

func (s *Server) compatibilityDecisions(w http.ResponseWriter, _ *http.Request) {
	response, berr := s.engine.CompatibilityDecisions()
	if berr != nil {
		writeBizError(w, berr)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(response)
}

func (s *Server) debugCrashFault(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CommitID string `json:"commit_id"`
		HoldMS   int    `json:"hold_ms"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	response, berr := s.engine.PrepareCrashFault(req.CommitID)
	if berr != nil {
		writeBizError(w, berr)
		return
	}
	writeJSON(w, http.StatusOK, response)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	hold := time.Duration(req.HoldMS) * time.Millisecond
	if hold <= 0 {
		hold = 120 * time.Second
	}
	time.Sleep(hold)
}

func (s *Server) notFound(w http.ResponseWriter, r *http.Request) {
	writeBizError(w, model.NewError(model.CodeNotFound, "route not found", map[string]any{
		"method": r.Method, "path": r.URL.Path,
	}))
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(target); err != nil {
		code := model.CodeInvalidJSON
		if strings.Contains(err.Error(), "request body too large") {
			code = model.CodeSpecInvalid
		}
		writeBizError(w, model.NewError(code, "request body is not valid JSON", map[string]any{"detail": err.Error()}))
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeBizError(w, model.NewError(model.CodeInvalidJSON, "request body must contain one JSON object", nil))
		return false
	}
	return true
}

func versionQuery(w http.ResponseWriter, r *http.Request) (int64, bool) {
	raw := r.URL.Query().Get("version")
	if raw == "" {
		return 0, true
	}
	version, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || version < 0 {
		writeBizError(w, model.NewError(model.CodeSpecInvalid, "version query parameter must be a non-negative integer", map[string]any{"version": raw}))
		return 0, false
	}
	return version, true
}

func writeResult(w http.ResponseWriter, response json.RawMessage, berr *model.BizError) {
	if berr != nil {
		writeBizError(w, berr)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if len(response) == 0 {
		response = []byte(`{"ok":true}`)
	}
	_, _ = w.Write(response)
}

func writeBizError(w http.ResponseWriter, berr *model.BizError) {
	if berr == nil {
		berr = model.NewError(model.CodeInternal, "unknown error", nil)
	}
	status := http.StatusBadRequest
	switch berr.Code {
	case model.CodeNotFound:
		status = http.StatusNotFound
	case model.CodeInternal:
		status = http.StatusInternalServerError
	case model.CodeInProgress, model.CodeConflictingRequest:
		status = http.StatusConflict
	}
	writeJSON(w, status, map[string]any{
		"code": berr.Code, "message": berr.Message, "context": berr.Context,
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		_, _ = fmt.Fprintln(w, `{"code":"E_INTERNAL","message":"response encoding failed","context":{}}`)
	}
}
