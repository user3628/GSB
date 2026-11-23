package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"cdn-edge-consistency/internal/cache"
	"cdn-edge-consistency/internal/engine"
	"cdn-edge-consistency/internal/model"
	"cdn-edge-consistency/internal/policy"
	"cdn-edge-consistency/internal/store"
)

type Server struct {
	engine *engine.Engine
	origin *engine.OriginSource
	cfg    model.Config
	logger *log.Logger
	start  time.Time
}

func New(eng *engine.Engine, origin *engine.OriginSource, cfg model.Config, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.Default()
	}
	return &Server{engine: eng, origin: origin, cfg: cfg, logger: logger, start: time.Now()}
}

type apiError struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	Details   map[string]any `json:"details,omitempty"`
	RequestID string         `json:"request_id,omitempty"`
	TraceID   string         `json:"trace_id,omitempty"`
}

func (s *Server) Handler() http.Handler {
	// A custom router is intentional: net/http.ServeMux redirects duplicate
	// slashes and dot segments, which would bypass the cache-key normalizer and
	// violate the requirement that equivalent URL shapes share one entry.
	router := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/healthz":
			s.health(w, r)
		case r.URL.Path == "/origin" || strings.HasPrefix(r.URL.Path, "/origin/"):
			s.originRequest(w, r)
		case strings.HasPrefix(r.URL.Path, "/edge/"):
			s.edgeRequest(w, r)
		case strings.HasPrefix(r.URL.Path, "/admin/"):
			s.admin(w, r)
		default:
			s.root(w, r)
		}
	})
	return s.recoverer(s.logRequests(router))
}

func (s *Server) root(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		writeError(w, model.NewBiz(model.CodeNotFound, "route not found"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service": "cdn-edge-consistency",
		"endpoints": []string{
			"GET /origin/{path}", "GET /edge/{node}/{path}", "POST /admin/policies",
			"GET /admin/policies/current", "POST /admin/purge", "GET /admin/entries",
			"GET /admin/traces", "GET /admin/metrics", "GET /admin/recovery",
		},
		"fault_controls": []string{"__fault", "__fault_node", "__delay_ms", "__status", "__corrupt", "__corrupt_bytes", "__request_id"},
		"started_at":     s.start.UTC().Format(time.RFC3339),
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "now": time.Now().UTC().Format(time.RFC3339Nano)})
}

func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		s.logger.Printf("%s %s %s", r.Method, r.URL.RequestURI(), time.Since(start))
	})
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.logger.Printf("panic: %v", rec)
				writeError(w, model.NewBiz(model.CodeInternal, fmt.Sprintf("panic: %v", rec)))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) originRequest(w http.ResponseWriter, r *http.Request) {
	rawEscaped := strings.TrimPrefix(r.URL.EscapedPath(), "/origin")
	rawPath := strings.TrimPrefix(r.URL.Path, "/origin")
	if rawEscaped == "" && rawPath != "" {
		rawEscaped = rawPath
	}
	clone := r.Clone(r.Context())
	clone.URL.Path = rawPath
	clone.URL.RawPath = rawEscaped
	s.origin.ServeHTTP(w, clone)
}

func (s *Server) edgeRequest(w http.ResponseWriter, r *http.Request) {
	escaped := r.URL.EscapedPath()
	rest := strings.TrimPrefix(escaped, "/edge/")
	slash := strings.IndexByte(rest, '/')
	nodeID := rest
	rawPath := "/"
	if slash >= 0 {
		nodeID = rest[:slash]
		rawPath = rest[slash:]
	}
	if nodeID == "" {
		writeError(w, model.NewBiz(model.CodeBadRequest, "edge node id is missing"))
		return
	}
	resp := s.engine.HandleEdge(r.Context(), r, nodeID, rawPath, r.URL.RawQuery)
	for k, v := range resp.Headers {
		if strings.EqualFold(k, "Content-Length") && r.Method == http.MethodHead {
			continue
		}
		w.Header().Set(k, v)
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(resp.Status)
		return
	}
	w.WriteHeader(resp.Status)
	_, _ = w.Write(resp.Body)
}

func (s *Server) admin(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/admin/")
	switch {
	case path == "policies" && r.Method == http.MethodPost:
		s.submitPolicy(w, r)
	case path == "policies" && r.Method == http.MethodGet:
		policies, err := s.engine.Store().ListPolicies(r.Context())
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"policies": policies})
	case path == "policies/current" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, s.engine.CurrentPolicy())
	case strings.HasPrefix(path, "policies/") && r.Method == http.MethodGet:
		version, err := strconv.ParseInt(strings.TrimPrefix(path, "policies/"), 10, 64)
		if err != nil {
			writeError(w, model.NewBiz(model.CodeBadRequest, "invalid policy version"))
			return
		}
		p, err := s.engine.Store().PolicyByVersion(r.Context(), version)
		if err != nil {
			writeError(w, err)
			return
		}
		if p == nil {
			writeError(w, model.WithDetails(model.NewBiz(model.CodeVersionRetired, "policy version is not retained"), map[string]any{"snapshot_version": version}))
			return
		}
		writeJSON(w, http.StatusOK, p)
	case path == "purge" && r.Method == http.MethodPost:
		s.purge(w, r)
	case path == "purges" && r.Method == http.MethodGet:
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		purges, err := s.engine.ListPurges(r.Context(), limit)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"purges": purges})
	case strings.HasPrefix(path, "purges/") && r.Method == http.MethodGet:
		rec, err := s.engine.GetPurge(r.Context(), strings.TrimPrefix(path, "purges/"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, rec)
	case path == "entries" && r.Method == http.MethodGet:
		s.listEntries(w, r)
	case path == "traces" && r.Method == http.MethodGet:
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		traces, err := s.engine.Store().ListTraces(r.Context(), limit)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"traces": traces})
	case strings.HasPrefix(path, "traces/") && r.Method == http.MethodGet:
		id := strings.TrimPrefix(path, "traces/")
		trace, err := s.engine.TraceFor(r.Context(), id)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, trace)
	case path == "metrics" && r.Method == http.MethodGet:
		m, err := s.engine.Metrics(r.Context())
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, m)
	case path == "scheduler" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, s.engine.SchedulerReport())
	case path == "recovery" && r.Method == http.MethodGet:
		report := s.engine.LatestRecoveryReport()
		if report == nil {
			writeJSON(w, http.StatusOK, map[string]any{"facts_only": true, "message": "no recovery report"})
			return
		}
		writeJSON(w, http.StatusOK, report)
	case path == "debug/crash" && r.Method == http.MethodPost:
		if !s.cfg.AllowDebugCrash {
			writeError(w, model.NewBiz(model.CodeBadRequest, "debug crash endpoint is disabled"))
			return
		}
		s.debugCrash(w, r)
	default:
		writeError(w, model.NewBiz(model.CodeNotFound, "admin route not found"))
	}
}

func (s *Server) submitPolicy(w http.ResponseWriter, r *http.Request) {
	var p policy.Snapshot
	if err := decodeJSON(r, &p); err != nil {
		writeError(w, err)
		return
	}
	requestID := requestID(r)
	saved, err := s.engine.SubmitPolicy(r.Context(), &p, requestID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"committed": true, "policy": saved})
}

func (s *Server) purge(w http.ResponseWriter, r *http.Request) {
	var req engine.PurgeRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, err)
		return
	}
	if req.RequestID == "" {
		req.RequestID = requestID(r)
	}
	result, err := s.engine.Purge(r.Context(), req)
	if err != nil {
		be := model.AsBiz(err)
		details := map[string]any{}
		for k, v := range be.Details {
			details[k] = v
		}
		if result != nil {
			details["purge_result"] = result
		}
		writeJSON(w, be.HTTPStatus, apiError{Code: be.Code, Message: be.Message, Details: details, RequestID: req.RequestID})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) listEntries(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	filter := store.EntryFilter{
		NodeID: q.Get("node"), NormPath: q.Get("path"), CacheKey: q.Get("key"),
		State: cacheState(q.Get("state")), Tag: q.Get("tag"), Limit: limit,
	}
	entries, err := s.engine.Store().ListEntries(r.Context(), filter)
	if err != nil {
		writeError(w, err)
		return
	}
	for i := range entries {
		entries[i].Body = nil
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
}

func (s *Server) debugCrash(w http.ResponseWriter, r *http.Request) {
	if err := s.engine.DebugCrash(r.Context(), r.URL.Query().Get("node"), r.URL.Query().Get("path"), requestID(r)); err != nil {
		writeError(w, err)
		return
	}
	s.logger.Printf("debug crash fixture committed; terminating process without response")
	w.WriteHeader(http.StatusAccepted)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	time.Sleep(50 * time.Millisecond)
	os.Exit(90)
}

func decodeJSON(r *http.Request, out any) error {
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		return model.NewBiz(model.CodeBadRequest, "failed to read request body")
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return model.NewBiz(model.CodeBadRequest, "JSON request body is required")
	}
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return model.WithDetails(model.NewBiz(model.CodeBadRequest, "invalid JSON request body"), map[string]any{"error": err.Error()})
	}
	return nil
}

func requestID(r *http.Request) string {
	return first(r.Header.Get("X-Request-ID"), r.Header.Get("Idempotency-Key"))
}

func first(v ...string) string {
	for _, s := range v {
		if strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

func writeError(w http.ResponseWriter, err error) {
	be := model.AsBiz(err)
	writeJSON(w, be.HTTPStatus, apiError{Code: be.Code, Message: be.Message, Details: be.Details})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func cacheState(s string) cache.State {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "MISS", "REVALIDATING", "FRESH", "STALE", "PURGED":
		return cache.State(strings.ToUpper(strings.TrimSpace(s)))
	default:
		return ""
	}
}
