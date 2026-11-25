package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"

	"featureplatform/internal/configx"
	"featureplatform/internal/engine"
	"featureplatform/internal/model"
	"featureplatform/internal/samplegen"
	"featureplatform/internal/store"
)

type Server struct {
	Engine          *engine.Engine
	MigrationCtx    context.Context
	CancelMigration context.CancelFunc
	OnStop          func()
}

func New(eng *engine.Engine) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{Engine: eng, MigrationCtx: ctx, CancelMigration: cancel}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /api/v1/demo/seed", s.seedDemo)
	mux.HandleFunc("POST /api/v1/shards/import", s.importShards)
	mux.HandleFunc("POST /api/v1/config/snapshots", s.putConfig)
	mux.HandleFunc("POST /api/v1/config/fragments", s.submitFragment)
	mux.HandleFunc("POST /api/v1/migrate", s.migrate)
	mux.HandleFunc("GET /api/v1/status", s.status)
	mux.HandleFunc("GET /api/v1/fingerprints", s.status)
	mux.HandleFunc("GET /api/v1/read", s.read)
	mux.HandleFunc("GET /api/v1/records", s.records)
	mux.HandleFunc("GET /api/v1/rejections", s.rejections)
	mux.HandleFunc("GET /api/v1/invalidations", s.invalidations)
	mux.HandleFunc("GET /api/v1/summary", s.summary)
	mux.HandleFunc("GET /api/v1/export", s.export)
	mux.HandleFunc("POST /api/v1/admin/stop", s.stop)
	return logging(mux)
}

func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	var coded *model.CodedError
	if errors.As(err, &coded) {
		status := http.StatusBadRequest
		switch coded.Code {
		case model.CodeStaleRead, model.CodeStaleCheckpoint, model.CodeStaleVersion, model.CodeConflict:
			status = http.StatusConflict
		case model.CodeNotFound:
			status = http.StatusNotFound
		}
		writeJSON(w, status, map[string]any{"ok": false, "error": coded})
		return
	}
	if errors.Is(err, engine.ErrGracefulStop) || errors.Is(err, context.Canceled) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ok": false, "error": &model.CodedError{Code: "E_STOPPING", Message: err.Error()}})
		return
	}
	writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "error": err.Error()})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "service": "featureplatform"})
}

type importRequest struct {
	BatchID string        `json:"batch_id"`
	Shards  []model.Shard `json:"shards"`
}

func (s *Server) seedDemo(w http.ResponseWriter, r *http.Request) {
	shards := samplegen.GenerateShards()
	importResult, err := s.Engine.Store.ImportShards(r.Context(), shards, "http-demo-seed")
	if err != nil {
		writeError(w, err)
		return
	}
	cfg := samplegen.DefaultConfig()
	current, _, err := s.Engine.Store.CurrentVersion(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	if current == 0 {
		if _, err := s.Engine.CommitInitialConfig(r.Context(), cfg); err != nil {
			writeError(w, err)
			return
		}
	}
	report, err := s.Engine.RunMigration(s.MigrationCtx, engine.RunOptions{Budget: 1000})
	if err != nil {
		writeError(w, err)
		return
	}
	summary, err := s.Engine.Summary(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "import": importResult, "migration": report, "summary": summary,
	})
}

func (s *Server) importShards(w http.ResponseWriter, r *http.Request) {
	var req importRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err)
		return
	}
	if req.BatchID == "" {
		req.BatchID = "http-import"
	}
	result, err := s.Engine.Store.ImportShards(r.Context(), req.Shards, req.BatchID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "result": result})
}

func (s *Server) putConfig(w http.ResponseWriter, r *http.Request) {
	var cfg configx.Config
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		writeError(w, err)
		return
	}
	current, _, err := s.Engine.Store.CurrentVersion(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	var fp string
	if current == 0 {
		fp, err = s.Engine.CommitInitialConfig(r.Context(), cfg)
	} else {
		fp, err = s.Engine.Store.CommitConfig(r.Context(), store.ConfigCommit{Config: cfg})
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "config_fingerprint": fp})
}

func (s *Server) submitFragment(w http.ResponseWriter, r *http.Request) {
	var fragment configx.Fragment
	if err := json.NewDecoder(r.Body).Decode(&fragment); err != nil {
		writeError(w, err)
		return
	}
	next, plan, err := s.Engine.SubmitFragment(r.Context(), fragment)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "config": next, "plan": plan})
}

func (s *Server) migrate(w http.ResponseWriter, r *http.Request) {
	var opts engine.RunOptions
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&opts); err != nil {
			writeError(w, err)
			return
		}
	}
	report, err := s.Engine.RunMigration(s.MigrationCtx, opts)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "report": report})
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	includeUnits := r.URL.Query().Get("include_units") == "1" || r.URL.Query().Get("include_units") == "true"
	st, err := s.Engine.Status(r.Context(), includeUnits)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "status": st})
}

func (s *Server) read(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	sampleID := q.Get("sample_id")
	if sampleID == "" {
		sampleID = q.Get("sample")
	}
	var version int64
	if _, err := parseIntQuery(q.Get("version"), &version); err != nil {
		writeError(w, &model.CodedError{Code: "E_INVALID_REQUEST", Message: "version query parameter is required integer"})
		return
	}
	rec, coded := s.Engine.Store.ReadAdjudicated(r.Context(), sampleID, version)
	if coded != nil {
		writeError(w, coded)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "record": rec})
}

func (s *Server) records(w http.ResponseWriter, r *http.Request) {
	rows, err := s.Engine.Store.ListValidRecordsSorted(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "records": rows})
}

func (s *Server) rejections(w http.ResponseWriter, r *http.Request) {
	items, err := s.Engine.Rejections(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "rejections": items})
}

func (s *Server) invalidations(w http.ResponseWriter, r *http.Request) {
	items, err := s.Engine.Store.ListInvalidations(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "invalidations": items})
}

func (s *Server) summary(w http.ResponseWriter, r *http.Request) {
	summary, err := s.Engine.Summary(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "summary": summary})
}

func (s *Server) export(w http.ResponseWriter, r *http.Request) {
	b, digest, n, err := s.Engine.ExportCanonical(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Canonical-Digest", digest)
	w.Header().Set("X-Record-Count", strconv.Itoa(n))
	_, _ = w.Write(b)
}

func (s *Server) stop(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "stopping": true})
	s.CancelMigration()
	if s.OnStop != nil {
		go s.OnStop()
	}
}

func parseIntQuery(raw string, target *int64) (bool, error) {
	if raw == "" {
		return false, errors.New("missing")
	}
	v, err := json.Number(raw).Int64()
	if err != nil {
		return false, err
	}
	*target = v
	return true, nil
}
