package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"profilehotspot/internal/apperr"
	"profilehotspot/internal/generator"
	"profilehotspot/internal/model"
	"profilehotspot/internal/service"
)

const maxBodyBytes = 32 << 20

type Server struct {
	service *service.Service
	mux     *http.ServeMux
	nextID  atomic.Uint64
}

func New(svc *service.Service) *Server {
	server := &Server{service: svc, mux: http.NewServeMux()}
	server.routes()
	return server
}

func (s *Server) Handler() http.Handler {
	return s.recoverMiddleware(s.requestMiddleware(s.mux))
}

func (s *Server) routes() {
	s.mux.HandleFunc("/healthz", s.guard("GET", s.health))
	s.mux.HandleFunc("/v1/current", s.guard("GET", s.current))
	s.mux.HandleFunc("/v1/policies", s.guard("POST", s.commitPolicy))
	s.mux.HandleFunc("/v1/policies/increments", s.guard("POST", s.commitIncrement))
	s.mux.HandleFunc("/v1/policies/{version}", s.guard("GET", s.getPolicy))
	s.mux.HandleFunc("/v1/policies/{version}/rebuild", s.guard("GET", s.getRebuild))
	s.mux.HandleFunc("/v1/samples", s.guard("POST", s.ingest))
	s.mux.HandleFunc("/v1/batches/{batch_id}", s.guard("GET", s.getBatch))
	s.mux.HandleFunc("/v1/versions/{version}/reservoirs", s.guard("GET", s.getReservoirs))
	s.mux.HandleFunc("/v1/versions/{version}/hotspots", s.guard("GET", s.getHotspots))
	s.mux.HandleFunc("/v1/versions/{version}/snapshot", s.guard("GET", s.getVersionSnapshot))
	s.mux.HandleFunc("/v1/versions/{version}/differences", s.guard("GET", s.getDifferences))
	s.mux.HandleFunc("/v1/versions/{version}/modes", s.guard("GET", s.getModes))
	s.mux.HandleFunc("/v1/recovery/report", s.guard("GET", s.getRecovery))
	s.mux.HandleFunc("/v1/demo/policy", s.guard("GET", s.demoPolicy))
	s.mux.HandleFunc("/v1/demo/chunk", s.guard("GET", s.demoChunk))
	s.mux.HandleFunc("/", s.notFound)
}

func (s *Server) guard(method string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.Header().Set("Allow", method)
			s.writeError(w, r, apperr.New(apperr.CodeMethodNotAllowed, "method not allowed", http.StatusMethodNotAllowed, map[string]any{
				"method": r.Method,
				"path":   r.URL.Path,
				"allow":  method,
			}))
			return
		}
		next(w, r)
	}
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (s *Server) requestMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		id := fmt.Sprintf("req-%d-%d", time.Now().UnixNano(), s.nextID.Add(1))
		w.Header().Set("X-Request-ID", id)
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		log.Printf("http method=%s path=%s status=%d duration=%s request_id=%s", r.Method, r.URL.Path, sw.status, time.Since(started).Round(time.Microsecond), id)
	})
}

func (s *Server) recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.writeError(w, r, apperr.Internal("panic recovered", fmt.Errorf("%v", recovered)))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) notFound(w http.ResponseWriter, r *http.Request) {
	s.writeError(w, r, apperr.NotFound("endpoint", map[string]any{"path": r.URL.Path}))
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	state, err := s.service.CurrentState(r.Context())
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "state": state})
}

func (s *Server) current(w http.ResponseWriter, r *http.Request) {
	state, err := s.service.CurrentState(r.Context())
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	response := map[string]any{
		"state":      state,
		"has_policy": state.CurrentPolicyVersion > 0,
	}
	if state.CurrentPolicyVersion > 0 {
		policy, err := s.service.Policy(r.Context(), state.CurrentPolicyVersion)
		if err != nil {
			s.writeError(w, r, err)
			return
		}
		response["policy"] = policy
	}
	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) commitPolicy(w http.ResponseWriter, r *http.Request) {
	var policy model.Policy
	if err := s.decodeJSON(r, &policy); err != nil {
		s.writeError(w, r, err)
		return
	}
	response, err := s.service.CommitPolicy(r.Context(), policy)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusCreated, response)
}

func (s *Server) commitIncrement(w http.ResponseWriter, r *http.Request) {
	var increment model.PolicyIncrement
	if err := s.decodeJSON(r, &increment); err != nil {
		s.writeError(w, r, err)
		return
	}
	response, err := s.service.CommitIncrement(r.Context(), increment)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusCreated, response)
}

func (s *Server) getPolicy(w http.ResponseWriter, r *http.Request) {
	version, err := pathVersion(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	response, err := s.service.Policy(r.Context(), version)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) getRebuild(w http.ResponseWriter, r *http.Request) {
	version, err := pathVersion(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	response, err := s.service.RebuildReport(r.Context(), version)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) ingest(w http.ResponseWriter, r *http.Request) {
	var chunk model.SampleChunk
	if err := s.decodeJSON(r, &chunk); err != nil {
		s.writeError(w, r, err)
		return
	}
	response, err := s.service.Ingest(r.Context(), chunk)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	status := http.StatusCreated
	if response.IdempotentReplay {
		status = http.StatusOK
	}
	s.writeJSON(w, status, response)
}

func (s *Server) getBatch(w http.ResponseWriter, r *http.Request) {
	response, err := s.service.Batch(r.Context(), r.PathValue("batch_id"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) getReservoirs(w http.ResponseWriter, r *http.Request) {
	version, err := pathVersion(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	commit, err := optionalInt64Query(r, "commit_seq")
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	response, err := s.service.Reservoirs(r.Context(), version, commit, r.URL.Query().Get("service"), r.URL.Query().Get("channel"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) getHotspots(w http.ResponseWriter, r *http.Request) {
	version, err := pathVersion(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	commit, err := optionalInt64Query(r, "commit_seq")
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 || parsed > 100000 {
			s.writeError(w, r, apperr.SampleInvalid("limit must be an integer in [0,100000]", map[string]any{"field": "limit", "value": raw}))
			return
		}
		limit = parsed
	}
	response, err := s.service.Hotspots(r.Context(), version, commit, r.URL.Query().Get("service"), limit)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) getVersionSnapshot(w http.ResponseWriter, r *http.Request) {
	version, err := pathVersion(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	commit, err := optionalInt64Query(r, "commit_seq")
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 || parsed > 100000 {
			s.writeError(w, r, apperr.SampleInvalid("limit must be an integer in [0,100000]", map[string]any{"field": "limit", "value": raw}))
			return
		}
		limit = parsed
	}
	response, err := s.service.VersionSnapshot(r.Context(), version, commit, r.URL.Query().Get("service"), r.URL.Query().Get("channel"), limit)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) getDifferences(w http.ResponseWriter, r *http.Request) {
	version, err := pathVersion(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	commit, err := optionalInt64Query(r, "commit_seq")
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	response, err := s.service.Differences(r.Context(), version, commit)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) getModes(w http.ResponseWriter, r *http.Request) {
	version, err := pathVersion(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	commit, err := optionalInt64Query(r, "commit_seq")
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	response, err := s.service.Modes(r.Context(), version, commit)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) getRecovery(w http.ResponseWriter, r *http.Request) {
	response, err := s.service.RecoveryReport(r.Context())
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) demoPolicy(w http.ResponseWriter, r *http.Request) {
	version := uint64(1)
	if raw := r.URL.Query().Get("version"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || parsed == 0 {
			s.writeError(w, r, apperr.SampleInvalid("version must be a positive integer", map[string]any{"field": "version", "value": raw}))
			return
		}
		version = parsed
	}
	policy := generator.DemoPolicy(version)
	s.writeJSON(w, http.StatusOK, policy)
}

func (s *Server) demoChunk(w http.ResponseWriter, r *http.Request) {
	state, err := s.service.CurrentState(r.Context())
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	if state.CurrentPolicyVersion == 0 {
		s.writeError(w, r, apperr.NotFound("policy version", map[string]any{"reason": "commit a policy before requesting a demo chunk"}))
		return
	}
	policySnapshot, err := s.service.Policy(r.Context(), state.CurrentPolicyVersion)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	seed := uint64(42)
	if raw := r.URL.Query().Get("seed"); raw != "" {
		seed, err = strconv.ParseUint(raw, 10, 64)
		if err != nil {
			s.writeError(w, r, apperr.SampleInvalid("seed must be an unsigned integer", map[string]any{"field": "seed", "value": raw}))
			return
		}
	}
	count := 32
	if raw := r.URL.Query().Get("count"); raw != "" {
		count, err = strconv.Atoi(raw)
		if err != nil || count < 1 || count > 10000 {
			s.writeError(w, r, apperr.SampleInvalid("count must be in [1,10000]", map[string]any{"field": "count", "value": raw}))
			return
		}
	}
	startIndex := 0
	if raw := r.URL.Query().Get("start"); raw != "" {
		startIndex, err = strconv.Atoi(raw)
		if err != nil || startIndex < 0 {
			s.writeError(w, r, apperr.SampleInvalid("start must be a non-negative integer", map[string]any{"field": "start", "value": raw}))
			return
		}
	}
	duplicateEvery := 0
	if raw := r.URL.Query().Get("duplicate_every"); raw != "" {
		duplicateEvery, err = strconv.Atoi(raw)
		if err != nil || duplicateEvery < 0 {
			s.writeError(w, r, apperr.SampleInvalid("duplicate_every must be a non-negative integer", map[string]any{"field": "duplicate_every", "value": raw}))
			return
		}
	}
	var services []string
	if raw := r.URL.Query().Get("services"); raw != "" {
		for _, item := range strings.Split(raw, ",") {
			if trimmed := strings.TrimSpace(item); trimmed != "" {
				services = append(services, trimmed)
			}
		}
	}
	batchID := r.URL.Query().Get("batch_id")
	if batchID == "" {
		batchID = fmt.Sprintf("demo-%d-%d", seed, startIndex)
	}
	version := state.CurrentPolicyVersion
	chunk := generator.Chunk(policySnapshot.Policy, generator.ChunkOptions{
		Seed:           seed,
		BatchID:        batchID,
		PolicyVersion:  &version,
		Count:          count,
		StartIndex:     startIndex,
		Services:       services,
		DuplicateEvery: duplicateEvery,
	})
	s.writeJSON(w, http.StatusOK, chunk)
}

func (s *Server) decodeJSON(r *http.Request, destination any) error {
	contentType := r.Header.Get("Content-Type")
	if contentType != "" {
		mediaType, _, err := mime.ParseMediaType(contentType)
		if err != nil || (mediaType != "application/json" && !strings.HasSuffix(mediaType, "+json")) {
			return apperr.New(apperr.CodeUnsupportedMedia, "Content-Type must be application/json", http.StatusUnsupportedMediaType, map[string]any{"content_type": contentType})
		}
	}
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return apperr.SampleInvalid("invalid JSON request", map[string]any{"reason": err.Error()})
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return apperr.SampleInvalid("request body must contain exactly one JSON value", nil)
		}
		return apperr.SampleInvalid("invalid trailing JSON", map[string]any{"reason": err.Error()})
	}
	return nil
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	app := apperr.From(err)
	if app.Status == 0 {
		app.Status = http.StatusInternalServerError
	}
	if app.Context == nil {
		app.Context = map[string]any{}
	}
	payload := map[string]any{
		"code":       app.Code,
		"message":    app.Message,
		"context":    app.Context,
		"request_id": w.Header().Get("X-Request-ID"),
	}
	if app.Code == apperr.CodeInternal && app.Cause != nil {
		payload["context"].(map[string]any)["cause"] = app.Cause.Error()
	}
	s.writeJSON(w, app.Status, payload)
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
}

func pathVersion(r *http.Request) (uint64, error) {
	raw := r.PathValue("version")
	version, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || version == 0 {
		return 0, apperr.SampleInvalid("policy version path segment must be a positive integer", map[string]any{"field": "version", "value": raw})
	}
	return version, nil
}

func optionalInt64Query(r *http.Request, key string) (*int64, error) {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return nil, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return nil, apperr.SampleInvalid(key+" must be a non-negative integer", map[string]any{"field": key, "value": raw})
	}
	return &value, nil
}
