package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"

	"multirecall/internal/configx"
	"multirecall/internal/model"
	"multirecall/internal/orchestrator"
	"multirecall/internal/recall"
	"multirecall/internal/sample"
	"multirecall/internal/store"
)

type App struct {
	DataDir string
	mu      sync.RWMutex
	Dataset model.Dataset
	Config  configx.Snapshot
	Engine  *recall.Engine
	DB      *store.Store
}

func NewApp(dataDir string, withDB bool) (*App, error) {
	ds, cfg, err := sample.Ensure(dataDir)
	if err != nil {
		return nil, err
	}
	app := &App{DataDir: dataDir, Dataset: ds, Config: cfg, Engine: recall.NewEngine(ds, cfg)}
	if withDB {
		db, err := store.Open(filepath.Join(dataDir, "recall.db"))
		if err != nil {
			return nil, err
		}
		app.DB = db
	}
	return app, nil
}

func (a *App) Close() error {
	if a.DB != nil {
		return a.DB.Close()
	}
	return nil
}

func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.health)
	mux.HandleFunc("/", a.index)
	mux.HandleFunc("/v1/config", a.config)
	mux.HandleFunc("/v1/config/fragment", a.fragment)
	mux.HandleFunc("/v1/config/replace", a.replace)
	mux.HandleFunc("/v1/dataset", a.dataset)
	mux.HandleFunc("/v1/budget", a.budget)
	mux.HandleFunc("/v1/recall", a.recall)
	mux.HandleFunc("/v1/recall/batch", a.batch)
	mux.HandleFunc("/v1/checkpoints", a.checkpoints)
	return mux
}

func (a *App) ListenAndServe(addr string) error {
	srv := &http.Server{Addr: addr, Handler: a.Handler(), ReadHeaderTimeout: 5_000_000_000}
	return srv.ListenAndServe()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(model.CanonicalJSON(v))
	_, _ = w.Write([]byte("\n"))
}

func writeError(w http.ResponseWriter, status int, apiErr *model.APIError) {
	writeJSON(w, status, map[string]any{"ok": false, "error": apiErr})
}

func (a *App) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "service": "multirecall", "loopback_only": true})
}

func (a *App) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service": "multirecall",
		"endpoints": []string{
			"GET /healthz", "GET /v1/config", "POST /v1/config/fragment", "POST /v1/config/replace",
			"GET /v1/dataset", "GET /v1/budget?query_count=10", "GET/POST /v1/recall",
			"POST /v1/recall/batch", "GET /v1/checkpoints?query_id=Q001",
		},
	})
}

func (a *App) config(w http.ResponseWriter, r *http.Request) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	writeJSON(w, http.StatusOK, a.Config)
}

func (a *App) dataset(w http.ResponseWriter, r *http.Request) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	writeJSON(w, http.StatusOK, a.Dataset)
}

func (a *App) budget(w http.ResponseWriter, r *http.Request) {
	n := len(a.Dataset.Queries)
	if raw := r.URL.Query().Get("query_count"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			n = parsed
		}
	}
	a.mu.RLock()
	cfg := a.Config
	a.mu.RUnlock()
	writeJSON(w, http.StatusOK, recall.AdjudicateBudget(cfg, n))
}

type recallRequest struct {
	QueryID       string   `json:"query_id"`
	RawQuery      string   `json:"raw_query"`
	Locale        string   `json:"locale"`
	History       []string `json:"history_click_categories"`
	Workers       int      `json:"workers"`
	UseCheckpoint *bool    `json:"use_checkpoint"`
}

func (a *App) resolveQuery(req recallRequest) (model.Query, bool) {
	if req.RawQuery != "" || req.QueryID == "" {
		q := model.Query{QueryID: req.QueryID, RawQuery: req.RawQuery, Locale: req.Locale, HistoryClickCategories: req.History}
		if q.QueryID == "" {
			q.QueryID = "Q-HTTP"
		}
		if q.Locale == "" {
			q.Locale = "und"
		}
		return q, true
	}
	for _, q := range a.Dataset.Queries {
		if q.QueryID == req.QueryID {
			return q, true
		}
	}
	return model.Query{}, false
}

func statusForCode(code string) int {
	switch code {
	case model.CodeStaleBase, model.CodeStaleVersion:
		return http.StatusConflict
	case model.CodeInvalidRequest:
		return http.StatusBadRequest
	default:
		return http.StatusUnprocessableEntity
	}
}

func (a *App) recall(w http.ResponseWriter, r *http.Request) {
	var req recallRequest
	if r.Method == http.MethodPost {
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, &model.APIError{Code: model.CodeInvalidRequest, Message: err.Error()})
			return
		}
	} else {
		req.QueryID = r.URL.Query().Get("query_id")
		req.RawQuery = r.URL.Query().Get("raw_query")
	}
	if req.QueryID == "" && req.RawQuery == "" {
		writeError(w, http.StatusBadRequest, &model.APIError{Code: model.CodeInvalidRequest, Message: "query_id or raw_query is required"})
		return
	}
	q, ok := a.resolveQuery(req)
	if !ok {
		writeError(w, http.StatusNotFound, &model.APIError{Code: model.CodeInvalidRequest, QueryID: req.QueryID, Message: "unknown query_id"})
		return
	}
	a.mu.RLock()
	eng := a.Engine
	a.mu.RUnlock()
	rep := eng.ProcessQuery(q, nil)
	if rep.Error != nil {
		writeJSON(w, statusForCode(rep.Error.Code), rep)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

type batchRequest struct {
	Workers       int  `json:"workers"`
	UseCheckpoint bool `json:"use_checkpoint"`
}

func (a *App) batch(w http.ResponseWriter, r *http.Request) {
	req := batchRequest{}
	if r.Method == http.MethodPost {
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, &model.APIError{Code: model.CodeInvalidRequest, Message: err.Error()})
			return
		}
	}
	a.mu.RLock()
	eng := a.Engine
	db := a.DB
	cfg := a.Config
	a.mu.RUnlock()

	var report *recall.Report
	var err error
	if req.UseCheckpoint && db != nil {
		report, err = orchestrator.New(eng, db).RunAll(r.Context(), req.Workers)
	} else {
		report, err = eng.RunAll(recall.RunOptions{Context: r.Context(), Workers: req.Workers})
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, &model.APIError{Code: model.CodeCheckpoint, Message: err.Error()})
		return
	}
	_ = cfg
	writeJSON(w, http.StatusOK, report)
}

func (a *App) checkpoints(w http.ResponseWriter, r *http.Request) {
	if a.DB == nil {
		writeError(w, http.StatusServiceUnavailable, &model.APIError{Code: model.CodeCheckpoint, Message: "checkpoint database is disabled"})
		return
	}
	views, err := a.DB.ListCheckpoints(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, &model.APIError{Code: model.CodeCheckpoint, Message: err.Error()})
		return
	}
	if qid := r.URL.Query().Get("query_id"); qid != "" {
		filtered := make([]store.CheckpointView, 0, len(views))
		for _, v := range views {
			if v.QueryID == qid {
				filtered = append(filtered, v)
			}
		}
		views = filtered
	}
	writeJSON(w, http.StatusOK, map[string]any{"checkpoints": views, "count": len(views)})
}

func (a *App) fragment(w http.ResponseWriter, r *http.Request) {
	var f configx.Fragment
	if err := decodeJSON(r, &f); err != nil {
		writeError(w, http.StatusBadRequest, &model.APIError{Code: model.CodeInvalidRequest, Message: err.Error()})
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	next, kind, err := configx.ApplyFragment(a.Config, f)
	if err != nil {
		writeConfigError(w, err)
		return
	}
	invalidated, err := a.commitConfigLocked(r.Context(), next, f.BaseVersion, kind)
	if err != nil {
		writeError(w, http.StatusInternalServerError, &model.APIError{Code: model.CodeCheckpoint, Message: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "change_kind": kind, "config": next,
		"earliest_affected_stage": configx.EarliestAffectedStage(a.Config, next),
		"invalidated_checkpoints": invalidated,
	})
	a.Config = next
	a.Engine = recall.NewEngine(a.Dataset, next)
}

func (a *App) replace(w http.ResponseWriter, r *http.Request) {
	var incoming configx.Snapshot
	if err := decodeJSON(r, &incoming); err != nil {
		writeError(w, http.StatusBadRequest, &model.APIError{Code: model.CodeInvalidRequest, Message: err.Error()})
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	next, kind, err := configx.ClassifyReplacement(a.Config, incoming)
	if err != nil {
		writeConfigError(w, err)
		return
	}
	if kind == "unchanged" {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "change_kind": kind, "config": next, "invalidated_checkpoints": 0})
		return
	}
	invalidated, err := a.commitConfigLocked(r.Context(), next, a.Config.ConfigVersion, kind)
	if err != nil {
		writeError(w, http.StatusInternalServerError, &model.APIError{Code: model.CodeCheckpoint, Message: err.Error()})
		return
	}
	earliest := configx.EarliestAffectedStage(a.Config, next)
	a.Config = next
	a.Engine = recall.NewEngine(a.Dataset, next)
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "change_kind": kind, "config": next,
		"earliest_affected_stage": earliest, "invalidated_checkpoints": invalidated,
	})
}

func (a *App) commitConfigLocked(ctx context.Context, next configx.Snapshot, baseVersion int64, kind string) (int64, error) {
	if err := sample.SaveConfig(a.DataDir, next); err != nil {
		return 0, err
	}
	var invalidated int64
	var err error
	if a.DB != nil {
		earliest := configx.EarliestAffectedStage(a.Config, next)
		invalidated, err = a.DB.InvalidateFromStage(ctx, earliest, recall.StageOrder)
		if err != nil {
			return 0, err
		}
		raw := model.CanonicalJSON(next)
		if err := a.DB.SaveConfigRevision(ctx, next.ConfigVersion, baseVersion, kind, next.ContentDigest, raw); err != nil {
			return invalidated, err
		}
	}
	return invalidated, nil
}

func writeConfigError(w http.ResponseWriter, err error) {
	var apiErr *model.APIError
	if errors.As(err, &apiErr) {
		writeError(w, statusForCode(apiErr.Code), apiErr)
		return
	}
	writeError(w, http.StatusBadRequest, &model.APIError{Code: model.CodeInvalidRequest, Message: err.Error()})
}

func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}
