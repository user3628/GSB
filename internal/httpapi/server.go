package httpapi

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"example.com/gsb-router/internal/errs"
	"example.com/gsb-router/internal/model"
	"example.com/gsb-router/internal/routing"
	"example.com/gsb-router/internal/sample"
	"example.com/gsb-router/internal/store"
)

//go:embed ui/*
var uiFiles embed.FS

type Server struct {
	store        *store.Store
	logger       *log.Logger
	crashEnabled bool
	mux          *http.ServeMux
}

func New(routerStore *store.Store, logger *log.Logger, crashEnabled bool) *Server {
	if logger == nil {
		logger = log.Default()
	}
	server := &Server{store: routerStore, logger: logger, crashEnabled: crashEnabled, mux: http.NewServeMux()}
	server.routes()
	return server
}

func (s *Server) Handler() http.Handler {
	return s.recoverMiddleware(s.logMiddleware(s.mux))
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /", s.serveUI)
	s.mux.HandleFunc("GET /app.js", s.serveUIAsset("ui/app.js", "application/javascript; charset=utf-8"))
	s.mux.HandleFunc("GET /style.css", s.serveUIAsset("ui/style.css", "text/css; charset=utf-8"))

	s.mux.HandleFunc("GET /api/health", s.health)
	s.mux.HandleFunc("GET /api/state", s.state)
	s.mux.HandleFunc("GET /api/snapshots", s.snapshots)
	s.mux.HandleFunc("POST /api/snapshots", s.commitSnapshot)
	s.mux.HandleFunc("GET /api/snapshots/{version}", s.snapshotByVersion)
	s.mux.HandleFunc("POST /api/increments", s.commitIncrement)
	s.mux.HandleFunc("POST /api/query", s.query)
	s.mux.HandleFunc("GET /api/caches", s.caches)
	s.mux.HandleFunc("GET /api/intents", s.intents)
	s.mux.HandleFunc("GET /api/commits", s.commits)
	s.mux.HandleFunc("GET /api/commits/{id}", s.commitByID)
	s.mux.HandleFunc("GET /api/recovery", s.recovery)
	s.mux.HandleFunc("GET /api/samples", s.samples)
	s.mux.HandleFunc("GET /api/samples/{name}", s.sampleByName)
}

func (s *Server) serveUI(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	raw, err := uiFiles.ReadFile("ui/index.html")
	if err != nil {
		http.Error(w, "UI unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(raw)
}

func (s *Server) serveUIAsset(name, contentType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, err := uiFiles.ReadFile(name)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(raw)
	}
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeOK(w, http.StatusOK, map[string]any{
		"status":          "ok",
		"current_version": s.store.CurrentVersion(),
		"time":            time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func (s *Server) state(w http.ResponseWriter, r *http.Request) {
	writeOK(w, http.StatusOK, map[string]any{
		"state":    s.store.StateSummary(),
		"recovery": s.store.RecoveryReport(),
	})
}

func (s *Server) snapshots(w http.ResponseWriter, r *http.Request) {
	items, err := s.store.Snapshots(r.Context())
	if err != nil {
		writeError(w, internal("list snapshots", err))
		return
	}
	writeOK(w, http.StatusOK, map[string]any{"snapshots": items, "current_version": s.store.CurrentVersion()})
}

func (s *Server) snapshotByVersion(w http.ResponseWriter, r *http.Request) {
	version, err := strconv.ParseInt(r.PathValue("version"), 10, 64)
	if err != nil {
		writeError(w, errs.New(errs.CodeInvalidJSON, "snapshot version must be an integer", map[string]any{"version": r.PathValue("version")}))
		return
	}
	snapshot, err := s.store.Snapshot(r.Context(), version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeOK(w, http.StatusOK, snapshot)
}

func (s *Server) commitSnapshot(w http.ResponseWriter, r *http.Request) {
	req, err := decodeSnapshotCommit(r)
	if err != nil {
		writeError(w, errs.Snapshot(errs.CodeSnapshotInvalid, "snapshot JSON is invalid", map[string]any{"cause": err.Error()}))
		return
	}
	result, err := s.store.CommitSnapshot(r.Context(), req)
	if err != nil {
		s.writeCommitError(w, r, err)
		return
	}
	writeOK(w, http.StatusOK, result)
}

func (s *Server) commitIncrement(w http.ResponseWriter, r *http.Request) {
	var req model.Increment
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, invalidJSON(err))
		return
	}
	result, err := s.store.CommitIncrement(r.Context(), req)
	if err != nil {
		s.writeCommitError(w, r, err)
		return
	}
	writeOK(w, http.StatusOK, result)
}

func (s *Server) writeCommitError(w http.ResponseWriter, r *http.Request, err error) {
	writeError(w, err)
	biz := errs.As(err)
	if biz == nil || biz.Code != errs.CodeRebuildInterrupted || !s.crashEnabled {
		return
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	s.logger.Printf("FATAL injection: E_REBUILD_INTERRUPTED intent persisted; terminating process to exercise recovery")
	time.AfterFunc(350*time.Millisecond, func() {
		terminateProcess()
	})
}

type queryRequest struct {
	TableID      string   `json:"table_id"`
	Observed     int64    `json:"observed_version"`
	Kind         string   `json:"kind"`
	Value        *string  `json:"value,omitempty"`
	Values       []string `json:"values,omitempty"`
	Lower        *string  `json:"lower,omitempty"`
	Upper        *string  `json:"upper,omitempty"`
	IncludeLower *bool    `json:"include_lower,omitempty"`
	IncludeUpper *bool    `json:"include_upper,omitempty"`
	Inject       string   `json:"inject,omitempty"`
}

func (s *Server) query(w http.ResponseWriter, r *http.Request) {
	var req queryRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, invalidJSON(err))
		return
	}
	if req.Inject != "" && req.Inject != store.InjectObservedAhead {
		writeError(w, errs.New(errs.CodeQueryInvalid, "query inject must be observed_ahead", map[string]any{"inject": req.Inject}))
		return
	}
	if req.Inject == store.InjectObservedAhead {
		req.Observed = s.store.CurrentVersion() + 1
	}
	result, err := s.store.Route(routing.Query{
		TableID:      req.TableID,
		Observed:     req.Observed,
		Kind:         req.Kind,
		Value:        req.Value,
		Values:       req.Values,
		Lower:        req.Lower,
		Upper:        req.Upper,
		IncludeLower: req.IncludeLower,
		IncludeUpper: req.IncludeUpper,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeOK(w, http.StatusOK, result)
}

func (s *Server) caches(w http.ResponseWriter, r *http.Request) {
	writeOK(w, http.StatusOK, map[string]any{
		"route_caches":     s.store.RouteCaches(),
		"aggregate_caches": s.store.AggregateCaches(),
		"current_version":  s.store.CurrentVersion(),
	})
}

func (s *Server) intents(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	items, err := s.store.Intents(r.Context(), limit)
	if err != nil {
		writeError(w, internal("list intents", err))
		return
	}
	writeOK(w, http.StatusOK, map[string]any{"intents": items})
}

func (s *Server) commits(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	items, err := s.store.Commits(r.Context(), limit)
	if err != nil {
		writeError(w, internal("list commits", err))
		return
	}
	writeOK(w, http.StatusOK, map[string]any{"commits": items})
}

func (s *Server) commitByID(w http.ResponseWriter, r *http.Request) {
	receipt, err := s.store.CommitReceipt(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeOK(w, http.StatusOK, receipt)
}

func (s *Server) recovery(w http.ResponseWriter, r *http.Request) {
	report := s.store.RecoveryReport()
	status := "clean"
	if report != nil {
		status = "recovered"
	}
	writeOK(w, http.StatusOK, map[string]any{
		"status":          status,
		"report":          report,
		"current_version": s.store.CurrentVersion(),
	})
}

func (s *Server) samples(w http.ResponseWriter, r *http.Request) {
	writeOK(w, http.StatusOK, sample.All())
}

func (s *Server) sampleByName(w http.ResponseWriter, r *http.Request) {
	files := sample.All()
	var value any
	switch r.PathValue("name") {
	case "snapshot-v1":
		value = files.SnapshotV1
	case "increment-v2":
		value = files.IncrementV2
	case "increment-crash-v3":
		value = files.LedgerCrash
	case "snapshot-invalid-mapping":
		value = files.InvalidMapping
	case "snapshot-invalid-overlap":
		value = files.InvalidOverlap
	case "increment-stale-base":
		value = files.StaleBase
	default:
		writeError(w, errs.New(errs.CodeNotFound, "sample not found", map[string]any{"name": r.PathValue("name")}))
		return
	}
	writeOK(w, http.StatusOK, value)
}

func decodeJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, 8<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request body must contain one JSON value")
		}
		return err
	}
	return nil
}

func decodeSnapshotCommit(r *http.Request) (store.SnapshotCommitRequest, error) {
	defer r.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		return store.SnapshotCommitRequest{}, err
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		return store.SnapshotCommitRequest{}, err
	}
	if _, wrapped := probe["snapshot"]; wrapped {
		var req store.SnapshotCommitRequest
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil {
			return store.SnapshotCommitRequest{}, err
		}
		return req, nil
	}
	var snapshot model.Snapshot
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&snapshot); err != nil {
		return store.SnapshotCommitRequest{}, err
	}
	return store.SnapshotCommitRequest{Snapshot: snapshot}, nil
}

type okEnvelope struct {
	OK   bool `json:"ok"`
	Data any  `json:"data"`
}

type errorEnvelope struct {
	OK    bool        `json:"ok"`
	Error *errs.Error `json:"error"`
}

func writeOK(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(okEnvelope{OK: true, Data: data})
}

func writeError(w http.ResponseWriter, err error) {
	biz := errs.As(err)
	if biz == nil {
		biz = internal("request failed", err)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(statusFor(biz.Code))
	_ = json.NewEncoder(w).Encode(errorEnvelope{OK: false, Error: biz})
}

func statusFor(code string) int {
	switch code {
	case errs.CodeStaleBase, errs.CodeStaleRead, errs.CodeObservedAhead,
		errs.CodeConflictingRequest, errs.CodeRebuildInterrupted, errs.CodeVersionNotMonotonic:
		return http.StatusConflict
	case errs.CodeSnapshotInvalid, errs.CodePhysicalTableMiss, errs.CodeRangeOverlap,
		errs.CodeQueryInvalid, errs.CodeInvalidJSON, errs.CodeCacheInconsistent:
		return http.StatusBadRequest
	case errs.CodeTableNotFound, errs.CodeNotFound:
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
	}
}

func invalidJSON(err error) *errs.Error {
	return errs.New(errs.CodeInvalidJSON, "request JSON is invalid", map[string]any{"cause": err.Error()})
}

func internal(operation string, err error) *errs.Error {
	return errs.New(errs.CodeInternal, operation, map[string]any{"cause": err.Error()})
}

func (s *Server) logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			s.logger.Printf("%s %s", r.Method, r.URL.Path)
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.logger.Printf("panic serving %s: %v", r.URL.Path, recovered)
				writeError(w, errs.New(errs.CodeInternal, "internal panic", map[string]any{"cause": fmt.Sprint(recovered)}))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// Shutdown is a small helper for command wiring and integration tests.
func Shutdown(ctx context.Context, server *http.Server) error {
	return server.Shutdown(ctx)
}
