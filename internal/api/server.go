package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"

	"logagg/internal/agg"
	"logagg/internal/engine"
	"logagg/internal/errs"
	"logagg/internal/model"
	"logagg/internal/plan"
	"logagg/internal/sample"
)

type Server struct {
	Engine *engine.Engine
	Mux    *http.ServeMux
	Logger *log.Logger
}

func NewServer(engine *engine.Engine, logger *log.Logger) *Server {
	s := &Server{Engine: engine, Mux: http.NewServeMux(), Logger: logger}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.Mux.HandleFunc("/healthz", s.health)
	s.Mux.HandleFunc("/v1/events", s.events)
	s.Mux.HandleFunc("/v1/demo/shards", s.commitDemoShard)
	s.Mux.HandleFunc("/v1/shards", s.commitShard)
	s.Mux.HandleFunc("/v1/shards/", s.getShard)
	s.Mux.HandleFunc("/v1/plan/block", s.planBlock)
	s.Mux.HandleFunc("/v1/sessions/", s.sessionDispatch)
	s.Mux.HandleFunc("/v1/recovery", s.recover)
}

func (s *Server) sessionDispatch(w http.ResponseWriter, r *http.Request) {
	tail := strings.TrimPrefix(r.URL.Path, "/v1/sessions/")
	parts := strings.Split(strings.Trim(tail, "/"), "/")
	if len(parts) != 2 || parts[0] == "" {
		writeError(w, errs.New(errs.ENotFound, "expected /v1/sessions/{id}/advance|continue|query"))
		return
	}
	sessionID, action := parts[0], parts[1]
	switch {
	case action == "advance" && r.Method == http.MethodPost:
		s.advance(w, r, sessionID)
	case action == "continue" && r.Method == http.MethodPost:
		s.continueBatch(w, r, sessionID)
	case action == "query" && r.Method == http.MethodGet:
		s.query(w, r, sessionID)
	default:
		writeError(w, errs.New(errs.ENotFound, "unknown session action %q", action))
	}
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "service": "local-log-columnar-aggregator"})
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	count := 12
	if raw := r.URL.Query().Get("count"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 || parsed > 1000 {
			writeError(w, errs.New(errs.EInvalidRequest, "count must be between 1 and 1000"))
			return
		}
		count = parsed
	}
	sessionID := r.URL.Query().Get("session_id")
	if sessionID == "" {
		sessionID = "demo-session"
	}
	events := sample.NewGenerator().OfflineEvents(sessionID, "demo", count)
	writeJSON(w, http.StatusOK, map[string]any{"seed": sample.Seed, "events": events})
}

type demoShardRequest struct {
	ShardID string `json:"shard_id"`
	Version int64  `json:"version"`
}

func (s *Server) commitDemoShard(w http.ResponseWriter, r *http.Request) {
	var req demoShardRequest
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, errs.New(errs.EInvalidRequest, "invalid JSON: %v", err))
			return
		}
	}
	if req.Version <= 0 {
		req.Version = 1
	}
	committed, err := s.Engine.CommitGeneratedShard(r.Context(), req.ShardID, req.Version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"shard_id":   committed.Package.Manifest.ShardID,
		"version":    committed.Package.Manifest.Version,
		"index_hash": committed.IndexHash,
		"blocks":     len(committed.Package.Blocks),
	})
}

type blockDTO struct {
	Meta          model.BlockMeta `json:"meta"`
	GroupPayload  string          `json:"group_payload_base64"`
	MetricPayload string          `json:"metric_payload_base64"`
}

type shardDTO struct {
	Manifest model.Shard `json:"manifest"`
	Blocks   []blockDTO  `json:"blocks"`
}

func (s *Server) commitShard(w http.ResponseWriter, r *http.Request) {
	var dto shardDTO
	if err := json.NewDecoder(r.Body).Decode(&dto); err != nil {
		writeError(w, errs.New(errs.EInvalidRequest, "invalid JSON: %v", err))
		return
	}
	pkg := &model.ShardPackage{Manifest: dto.Manifest}
	for _, block := range dto.Blocks {
		group, err := base64.StdEncoding.DecodeString(block.GroupPayload)
		if err != nil {
			writeError(w, errs.New(errs.EInvalidRequest, "group payload base64: %v", err))
			return
		}
		metric, err := base64.StdEncoding.DecodeString(block.MetricPayload)
		if err != nil {
			writeError(w, errs.New(errs.EInvalidRequest, "metric payload base64: %v", err))
			return
		}
		pkg.Blocks = append(pkg.Blocks, &model.Block{Meta: block.Meta, GroupPayload: group, MetricPayload: metric})
	}
	committed, err := s.Engine.CommitShard(r.Context(), pkg)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"shard_id": committed.Package.Manifest.ShardID, "version": committed.Package.Manifest.Version, "index_hash": committed.IndexHash})
}

func (s *Server) getShard(w http.ResponseWriter, r *http.Request) {
	shardID := strings.TrimPrefix(r.URL.Path, "/v1/shards/")
	shardID = strings.Trim(shardID, "/")
	if shardID == "" || strings.Contains(shardID, "/") {
		writeError(w, errs.New(errs.EInvalidRequest, "shard_id is required"))
		return
	}
	versionText := r.URL.Query().Get("version")
	if versionText != "" {
		version, err := strconv.ParseInt(versionText, 10, 64)
		if err != nil {
			writeError(w, errs.New(errs.EInvalidRequest, "invalid version"))
			return
		}
		committed, ok := s.Engine.Registry().Get(shardID, version)
		if !ok {
			pkg, err := s.Engine.Store().LoadShardPackage(r.Context(), shardID, version)
			if err != nil {
				writeError(w, err)
				return
			}
			if pkg == nil {
				writeError(w, errs.New(errs.ENotFound, "shard %s v%d not found", shardID, version))
				return
			}
			writeJSON(w, http.StatusOK, pkg.Manifest)
			return
		}
		writeJSON(w, http.StatusOK, committed.Package.Manifest)
		return
	}
	committed, ok := s.Engine.Registry().Current(shardID)
	if !ok {
		pkg, err := s.Engine.Store().LoadShardPackage(r.Context(), shardID, 0)
		if err != nil {
			writeError(w, err)
			return
		}
		if pkg == nil {
			writeError(w, errs.New(errs.ENotFound, "shard %s not found", shardID))
			return
		}
		writeJSON(w, http.StatusOK, pkg.Manifest)
		return
	}
	writeJSON(w, http.StatusOK, committed.Package.Manifest)
}

func (s *Server) planBlock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, errs.New(errs.EInvalidRequest, "POST is required"))
		return
	}
	var req plan.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, errs.New(errs.EInvalidRequest, "invalid JSON: %v", err))
		return
	}
	result, err := plan.PlanBlock(req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) advance(w http.ResponseWriter, r *http.Request, sessionID string) {
	req, err := decodeAdvanceRequest(r)
	if err != nil {
		writeError(w, err)
		return
	}
	req.SessionID = sessionID
	resp, err := s.Engine.Advance(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusOK
	if resp.Status == "deferred" {
		status = http.StatusAccepted
	}
	writeJSON(w, status, resp)
}

func (s *Server) continueBatch(w http.ResponseWriter, r *http.Request, sessionID string) {
	req, err := decodeAdvanceRequest(r)
	if err != nil {
		writeError(w, err)
		return
	}
	req.SessionID = sessionID
	resp, err := s.Engine.Continue(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func decodeAdvanceRequest(r *http.Request) (engine.AdvanceRequest, error) {
	var req engine.AdvanceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return req, errs.New(errs.EInvalidRequest, "invalid JSON: %v", err)
	}
	return req, nil
}

func (s *Server) query(w http.ResponseWriter, r *http.Request, sessionID string) {
	precision := agg.Precision{InputScale: sample.MetricScale, ResultScale: 2, Mode: agg.RoundHalfUp}
	if raw := r.URL.Query().Get("input_scale"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil {
			writeError(w, errs.New(errs.EInvalidRequest, "invalid input_scale"))
			return
		}
		precision.InputScale = value
	}
	if raw := r.URL.Query().Get("result_scale"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil {
			writeError(w, errs.New(errs.EInvalidRequest, "invalid result_scale"))
			return
		}
		precision.ResultScale = value
	}
	if mode := r.URL.Query().Get("rounding_mode"); mode != "" {
		precision.Mode = agg.RoundingMode(mode)
	}
	result, version, err := s.Engine.Query(r.Context(), sessionID, precision)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"session_id": sessionID, "session_version": version, "result": result})
}

func (s *Server) recover(w http.ResponseWriter, r *http.Request) {
	report, err := s.Engine.Recover(r.Context())
	if err != nil {
		writeError(w, errs.New(errs.EInternal, "recovery failed: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	var coded *errs.Error
	if errors.As(err, &coded) {
		switch coded.Code {
		case errs.EInvalidRequest:
			status = http.StatusBadRequest
		case errs.ENotFound:
			status = http.StatusNotFound
		case errs.EConflictBatch, errs.EConflictQuery:
			status = http.StatusConflict
		case errs.EDecodeQueueFull:
			status = http.StatusTooManyRequests
		case errs.EShardInvalid, errs.EBlockCorrupt, errs.EBlockUnbounded, errs.ENumericOverflow:
			status = http.StatusUnprocessableEntity
		}
		writeJSON(w, status, map[string]any{"error": coded})
		return
	}
	writeJSON(w, status, map[string]any{"error": map[string]any{"code": errs.EInternal, "message": err.Error()}})
}
