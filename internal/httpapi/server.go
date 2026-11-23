package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"local/rollout-guard/internal/domain"
	"local/rollout-guard/internal/engine"
	"local/rollout-guard/internal/store"
	"local/rollout-guard/samples"
)

type Server struct {
	engine    *engine.Engine
	webFS     fs.FS
	startedAt time.Time
	version   string
}

type responseMeta struct {
	ElapsedMS  int64 `json:"elapsed_ms"`
	ServedAtMS int64 `json:"served_at_ms"`
}

type apiResponse struct {
	OK    bool         `json:"ok"`
	Data  any          `json:"data,omitempty"`
	Error *apiError    `json:"error,omitempty"`
	Meta  responseMeta `json:"meta"`
}

type apiError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
}

func New(engine *engine.Engine, webFS fs.FS, version string) http.Handler {
	server := &Server{engine: engine, webFS: webFS, startedAt: time.Now(), version: version}
	return server.routes()
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.handleHealth)
	mux.HandleFunc("POST /api/policies", s.handleSubmitPolicy)
	mux.HandleFunc("GET /api/policies/current", s.handleCurrentPolicy)
	mux.HandleFunc("POST /api/batches", s.handleSubmitBatch)
	mux.HandleFunc("GET /api/batches", s.handleBatches)
	mux.HandleFunc("GET /api/batches/{id}", s.handleBatch)
	mux.HandleFunc("POST /api/batches/{id}/observations", s.handleObservations)
	mux.HandleFunc("GET /api/batches/{id}/risk", s.handleRiskPreview)
	mux.HandleFunc("POST /api/batches/{id}/evaluate", s.handleEvaluate)
	mux.HandleFunc("POST /api/batches/{id}/rollback", s.handleManualRollback)
	mux.HandleFunc("GET /api/batches/{id}/cooldown", s.handleCooldown)
	mux.HandleFunc("POST /api/batches/{id}/transition", s.handleTransition)
	mux.HandleFunc("GET /api/cooldowns", s.handleCooldowns)
	mux.HandleFunc("GET /api/recovery", s.handleRecovery)
	mux.HandleFunc("GET /api/snapshot", s.handleSnapshot)
	mux.HandleFunc("GET /api/intents", s.handleIntents)
	mux.HandleFunc("POST /api/local-rollback-target", s.handleLocalRollbackTarget)
	mux.HandleFunc("GET /api/local-rollback-target", s.handleLocalRollbackTarget)
	mux.HandleFunc("POST /api/demo/seed", s.handleSeed)

	static := http.FileServer(http.FS(s.webFS))
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			s.writeError(w, r, time.Now(), domain.NewBizError("E_NOT_FOUND", "接口不存在", map[string]any{"path": r.URL.Path}))
			return
		}
		if r.URL.Path != "/" {
			if _, err := fs.Stat(s.webFS, strings.TrimPrefix(r.URL.Path, "/")); err != nil {
				r.URL.Path = "/"
			}
		}
		static.ServeHTTP(w, r)
	})
	return s.withMiddleware(mux)
}

func (s *Server) withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Cross-Origin-Resource-Policy", "same-origin")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'")
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	s.writeSuccess(w, r, start, map[string]any{
		"status": "ok", "version": s.version, "uptime_ms": time.Since(s.startedAt).Milliseconds(), "mode": "local-only",
	})
}

func (s *Server) handleSubmitPolicy(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var policy domain.Policy
	if err := decodeJSON(r, &policy); err != nil {
		s.writeError(w, r, start, err)
		return
	}
	result, err := s.engine.SubmitPolicy(r.Context(), policy)
	if err != nil {
		s.writeError(w, r, start, err)
		return
	}
	s.writeSuccess(w, r, start, result)
}

func (s *Server) handleCurrentPolicy(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	policy, commit, err := store.CurrentPolicy(r.Context(), s.engine.Store().DB())
	if err != nil {
		s.writeError(w, r, start, err)
		return
	}
	s.writeSuccess(w, r, start, map[string]any{"policy": policy, "commit_seq": commit})
}

func (s *Server) handleSubmitBatch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var batch domain.Batch
	if err := decodeJSON(r, &batch); err != nil {
		s.writeError(w, r, start, err)
		return
	}
	result, err := s.engine.SubmitBatch(r.Context(), batch)
	if err != nil {
		s.writeError(w, r, start, err)
		return
	}
	s.writeSuccess(w, r, start, result)
}

func (s *Server) handleBatches(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	batches, err := store.ListBatches(r.Context(), s.engine.Store().DB())
	if err != nil {
		s.writeError(w, r, start, err)
		return
	}
	s.writeSuccess(w, r, start, map[string]any{"batches": batches})
}

func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	batch, err := store.GetBatch(r.Context(), s.engine.Store().DB(), r.PathValue("id"))
	if err != nil {
		s.writeError(w, r, start, err)
		return
	}
	s.writeSuccess(w, r, start, map[string]any{"batch": batch})
}

type observationRequest struct {
	Observations []domain.Observation `json:"observations"`
}

func (s *Server) handleObservations(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var request observationRequest
	if err := decodeJSON(r, &request); err != nil {
		s.writeError(w, r, start, err)
		return
	}
	result, err := s.engine.IngestObservations(r.Context(), r.PathValue("id"), request.Observations)
	if err != nil {
		s.writeError(w, r, start, err)
		return
	}
	s.writeSuccess(w, r, start, result)
}

func (s *Server) handleRiskPreview(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	mode := r.URL.Query().Get("mode")
	if mode == "" {
		mode = "compare"
	}
	result, err := s.engine.Preview(r.Context(), r.PathValue("id"), mode)
	if err != nil {
		s.writeError(w, r, start, err)
		return
	}
	s.writeSuccess(w, r, start, result)
}

func (s *Server) handleEvaluate(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	mode := r.URL.Query().Get("mode")
	if mode == "" {
		mode = "full"
	}
	persist := true
	if raw := r.URL.Query().Get("persist"); raw != "" {
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			s.writeError(w, r, start, domain.NewBizError("E_INVALID_MODE", "persist 必须为布尔值", map[string]any{"persist": raw}))
			return
		}
		persist = parsed
	}
	result, err := s.engine.Evaluate(r.Context(), r.PathValue("id"), mode, persist)
	if err != nil {
		s.writeError(w, r, start, err)
		return
	}
	s.writeSuccess(w, r, start, result)
}

func (s *Server) handleManualRollback(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var body struct {
		Reason string `json:"reason"`
	}
	if err := decodeOptionalJSON(r, &body); err != nil {
		s.writeError(w, r, start, err)
		return
	}
	if body.Reason == "" {
		body.Reason = "manual rollback requested from workbench"
	}
	result, err := s.engine.TriggerManualRollback(r.Context(), r.PathValue("id"), body.Reason)
	if err != nil {
		s.writeError(w, r, start, err)
		return
	}
	s.writeSuccess(w, r, start, result)
}

func (s *Server) handleCooldown(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	result, err := s.engine.Cooldown(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeError(w, r, start, err)
		return
	}
	s.writeSuccess(w, r, start, result)
}

func (s *Server) handleCooldowns(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	result, err := s.engine.ListCooldowns(r.Context())
	if err != nil {
		s.writeError(w, r, start, err)
		return
	}
	s.writeSuccess(w, r, start, map[string]any{"cooldowns": result})
}

func (s *Server) handleTransition(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var body struct {
		To     string `json:"to"`
		Reason string `json:"reason"`
	}
	if err := decodeJSON(r, &body); err != nil {
		s.writeError(w, r, start, err)
		return
	}
	result, err := s.engine.Transition(r.Context(), r.PathValue("id"), body.To, body.Reason)
	if err != nil {
		s.writeError(w, r, start, err)
		return
	}
	s.writeSuccess(w, r, start, map[string]any{"batch": result})
}

func (s *Server) handleRecovery(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	result, err := s.engine.Recovery(r.Context())
	if err != nil {
		if biz, ok := domain.AsBizError(err); ok && biz.Code == "E_NOT_FOUND" {
			s.writeSuccess(w, r, start, map[string]any{"report": nil, "message": "尚无恢复报告"})
			return
		}
		s.writeError(w, r, start, err)
		return
	}
	s.writeSuccess(w, r, start, result)
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	result, err := s.engine.Snapshot(r.Context())
	if err != nil {
		s.writeError(w, r, start, err)
		return
	}
	s.writeSuccess(w, r, start, result)
}

func (s *Server) handleIntents(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	limit := 20
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 && parsed <= 100 {
			limit = parsed
		}
	}
	result, err := s.engine.ListIntents(r.Context(), limit)
	if err != nil {
		s.writeError(w, r, start, err)
		return
	}
	s.writeSuccess(w, r, start, map[string]any{"intents": result})
}

func (s *Server) handleLocalRollbackTarget(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if r.Method == http.MethodGet {
		s.writeSuccess(w, r, start, map[string]any{"status": "ready", "endpoint": "local-rollback-target"})
		return
	}
	defer r.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	log.Printf("rollback target received: %s", strings.TrimSpace(string(body)))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleSeed(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var body struct {
		Reset bool `json:"reset"`
	}
	if err := decodeOptionalJSON(r, &body); err != nil {
		s.writeError(w, r, start, err)
		return
	}
	if err := store.ResetAll(r.Context(), s.engine.Store()); err != nil {
		s.writeError(w, r, start, err)
		return
	}
	policy, err := samples.Policy()
	if err != nil {
		s.writeError(w, r, start, err)
		return
	}
	policyResult, err := s.engine.SubmitPolicy(r.Context(), policy)
	if err != nil {
		s.writeError(w, r, start, err)
		return
	}
	batches, err := samples.Batches()
	if err != nil {
		s.writeError(w, r, start, err)
		return
	}
	for _, batch := range batches {
		if _, err := s.engine.SubmitBatch(r.Context(), batch); err != nil {
			s.writeError(w, r, start, err)
			return
		}
	}
	observationSet, err := samples.Observations()
	if err != nil {
		s.writeError(w, r, start, err)
		return
	}
	ingested := map[string]int{}
	for batchID, observations := range observationSet {
		result, err := s.engine.IngestObservations(r.Context(), batchID, observations)
		if err != nil {
			s.writeError(w, r, start, err)
			return
		}
		ingested[batchID] = result.Accepted
	}
	s.writeSuccess(w, r, start, map[string]any{
		"policy_version": policyResult.Policy.Version, "policy_commit_seq": policyResult.CommitSeq,
		"batches": len(batches), "ingested": ingested, "reset": true,
	})
}

func decodeJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, 4<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return domain.NewBizError("E_REQUEST_INVALID", "请求 JSON 无法解析", map[string]any{"cause": err.Error()})
	}
	if decoder.More() {
		return domain.NewBizError("E_REQUEST_INVALID", "请求只能包含一个 JSON 对象", nil)
	}
	return nil
}

func decodeOptionalJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return domain.NewBizError("E_REQUEST_INVALID", "请求体读取失败", map[string]any{"cause": err.Error()})
	}
	if strings.TrimSpace(string(raw)) == "" {
		return nil
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return domain.NewBizError("E_REQUEST_INVALID", "请求 JSON 无法解析", map[string]any{"cause": err.Error()})
	}
	return nil
}

func (s *Server) writeSuccess(w http.ResponseWriter, r *http.Request, start time.Time, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(apiResponse{OK: true, Data: data, Meta: responseMeta{
		ElapsedMS: time.Since(start).Milliseconds(), ServedAtMS: time.Now().UnixMilli(),
	}})
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, start time.Time, err error) {
	biz := &domain.BizError{Code: "E_INTERNAL", Message: "内部错误"}
	if parsed, ok := domain.AsBizError(err); ok {
		biz = parsed
	} else if !errors.Is(err, context.Canceled) {
		log.Printf("request error %s %s: %v", r.Method, r.URL.Path, err)
		biz.Context = map[string]any{"cause": err.Error()}
	}
	status := statusForCode(biz.Code)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if biz.Code == "E_ROLLBACK_COOLDOWN" {
		if remaining, ok := biz.Context["remaining_ms"].(int64); ok && remaining > 0 {
			w.Header().Set("Retry-After", strconv.FormatInt((remaining+999)/1000, 10))
		}
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(apiResponse{Error: &apiError{
		Code: biz.Code, Message: biz.Message, Context: biz.Context,
	}, Meta: responseMeta{ElapsedMS: time.Since(start).Milliseconds(), ServedAtMS: time.Now().UnixMilli()}})
}

func statusForCode(code string) int {
	switch code {
	case "E_NOT_FOUND":
		return http.StatusNotFound
	case "E_STALE_SNAPSHOT", "E_CONFLICTING_REPORT", "E_INVALID_TRANSITION", "E_BATCH_EXISTS", "E_POLICY_MISSING":
		return http.StatusConflict
	case "E_ROLLBACK_COOLDOWN":
		return http.StatusTooManyRequests
	case "E_OBSERVATION_INVALID", "E_INSUFFICIENT_SAMPLES", "E_POLICY_INVALID", "E_BATCH_INVALID", "E_REQUEST_INVALID", "E_INVALID_MODE":
		return http.StatusUnprocessableEntity
	default:
		return http.StatusInternalServerError
	}
}
