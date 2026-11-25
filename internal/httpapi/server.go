package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"searchlab/internal/apperr"
	"searchlab/internal/checkpoint"
	"searchlab/internal/config"
	"searchlab/internal/engine"
	"searchlab/internal/model"
)

type Options struct {
	DBPath     string
	LockPath   string
	ConfigPath string
	ShardsDir  string
	Addr       string
}

type App struct {
	mu        sync.RWMutex
	store     *checkpoint.Store
	dbPath    string
	lockPath  string
	cfgPath   string
	shardsDir string
	cfg       config.Config
	corpus    model.Corpus
	hasState  bool
	runtime   *engine.Runtime
	query     atomic.Pointer[engine.QueryEngine]
	addr      string

	reportMu sync.Mutex
	reports  map[string][]byte

	server *http.Server
}

func New(opts Options) (*App, error) {
	if opts.Addr == "" {
		opts.Addr = "127.0.0.1:8080"
	}
	if err := validateLoopback(opts.Addr); err != nil {
		return nil, err
	}
	store, err := checkpoint.Open(opts.DBPath, opts.LockPath)
	if err != nil {
		return nil, err
	}
	app := &App{
		store:     store,
		dbPath:    opts.DBPath,
		lockPath:  opts.LockPath,
		cfgPath:   opts.ConfigPath,
		shardsDir: opts.ShardsDir,
		addr:      opts.Addr,
		reports:   make(map[string][]byte),
	}
	if opts.ConfigPath != "" || opts.ShardsDir != "" {
		if opts.ConfigPath == "" || opts.ShardsDir == "" {
			store.Close()
			return nil, apperr.E(apperr.CodeInput, "config and shards must be supplied together", nil)
		}
		if err := app.importState(opts.ConfigPath, opts.ShardsDir); err != nil {
			store.Close()
			return nil, err
		}
	}
	if runtime, err := engine.LoadRuntime(context.Background(), store); err == nil {
		app.runtime = runtime
		app.query.Store(engine.NewQueryEngine(runtime))
	} else if !apperr.IsCode(err, apperr.CodePrereqMissing) {
		store.Close()
		return nil, err
	}
	return app, nil
}

func validateLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return apperr.E(apperr.CodeInput, "invalid listen address", err)
	}
	switch strings.ToLower(host) {
	case "127.0.0.1", "localhost", "::1":
		return nil
	default:
		return apperr.E(apperr.CodeInput, "HTTP server must bind to a loopback address", nil)
	}
}

func (a *App) Close() error {
	return a.store.Close()
}

func (a *App) importState(configPath, shardsDir string) error {
	cfg, _, err := config.Load(configPath)
	if err != nil {
		return err
	}
	corpus, err := model.LoadShards(shardsDir)
	if err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cfg = cfg
	a.corpus = corpus
	a.hasState = true
	a.cfgPath = configPath
	a.shardsDir = shardsDir
	return nil
}

func (a *App) Import(configPath, shardsDir string) error {
	return a.importState(configPath, shardsDir)
}

func (a *App) Build(ctx context.Context, force bool) (*engine.BuildReport, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.hasState {
		return nil, apperr.E(apperr.CodePrereqMissing, "no imported shards/config; call /v1/import first", nil)
	}
	report, err := engine.Build(ctx, a.cfg, a.corpus, a.store, force)
	if err != nil {
		return nil, err
	}
	runtime, err := engine.LoadRuntime(ctx, a.store)
	if err != nil {
		return nil, err
	}
	a.runtime = runtime
	a.query.Store(engine.NewQueryEngine(runtime))
	return report, nil
}

func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/health", a.handleHealth)
	mux.HandleFunc("/v1/import", a.handleImport)
	mux.HandleFunc("/v1/build", a.handleBuild)
	mux.HandleFunc("/v1/resume", a.handleResume)
	mux.HandleFunc("/v1/status", a.handleStatus)
	mux.HandleFunc("/v1/version", a.handleVersion)
	mux.HandleFunc("/v1/query", a.handleQuery)
	mux.HandleFunc("/v1/errors", a.handleReport)
	mux.HandleFunc("/v1/diagnostics", a.handleReport)
	mux.HandleFunc("/v1/cache", a.handleCache)
	mux.HandleFunc("/v1/shutdown", a.handleShutdown)
	return mux
}

func (a *App) Run(ctx context.Context) error {
	a.server = &http.Server{Addr: a.addr, Handler: a.Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	listener, err := net.Listen("tcp", a.addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", a.addr, err)
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = a.server.Shutdown(shutdownCtx)
			cancel()
		case <-done:
		}
	}()
	err = a.server.Serve(listener)
	close(done)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (a *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		a.methodNotAllowed(w)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "addr": a.addr})
}

func (a *App) handleImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		a.methodNotAllowed(w)
		return
	}
	var req struct {
		ConfigPath string `json:"config_path"`
		ShardsDir  string `json:"shards_dir"`
	}
	if err := decodeJSON(r, &req); err != nil {
		a.writeError(w, err)
		return
	}
	if req.ConfigPath == "" || req.ShardsDir == "" {
		a.writeError(w, apperr.E(apperr.CodeInput, "config_path and shards_dir are required", nil))
		return
	}
	if err := a.Import(req.ConfigPath, req.ShardsDir); err != nil {
		a.writeError(w, err)
		return
	}
	a.mu.RLock()
	cfg, corpus := a.cfg, a.corpus
	a.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "imported",
		"config_version": cfg.Version,
		"config_digest":  cfg.Digest(),
		"shard_digest":   corpus.ShardDigest,
		"documents":      len(corpus.Docs),
	})
}

func (a *App) handleBuild(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		a.methodNotAllowed(w)
		return
	}
	var req struct {
		Force bool `json:"force"`
	}
	if r.ContentLength > 0 {
		if err := decodeJSON(r, &req); err != nil {
			a.writeError(w, err)
			return
		}
	}
	report, err := a.Build(r.Context(), req.Force)
	if err != nil {
		a.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (a *App) handleResume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		a.methodNotAllowed(w)
		return
	}
	report, err := a.Build(r.Context(), false)
	if err != nil {
		a.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (a *App) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		a.methodNotAllowed(w)
		return
	}
	records, _ := a.store.Records(r.Context())
	current, _ := a.store.CurrentSeq(r.Context())
	status := map[string]any{
		"current_seq": current,
		"records":     records,
	}
	if version, err := a.store.IndexVersion(r.Context()); err == nil {
		status["index_version"] = version
	}
	a.mu.RLock()
	if a.runtime != nil {
		status["config_version"] = a.runtime.ConfigVersion
		status["pipeline_id"] = a.runtime.PipelineID
		status["shard_digest"] = a.runtime.ShardDigest
	}
	if a.hasState {
		status["documents"] = len(a.corpus.Docs)
		status["config_path"] = a.cfgPath
		status["shards_dir"] = a.shardsDir
	}
	a.mu.RUnlock()
	if query := a.query.Load(); query != nil {
		status["cache"] = query.CacheStats()
	}
	writeJSON(w, http.StatusOK, status)
}

func (a *App) handleVersion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		a.methodNotAllowed(w)
		return
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	version := map[string]any{"format": "searchlab-demo/v1"}
	if a.hasState {
		version["config_version"] = a.cfg.Version
		version["config_digest"] = a.cfg.Digest()
		version["shard_digest"] = a.corpus.ShardDigest
	}
	if a.runtime != nil {
		version["index_version"] = a.runtime.IndexVersion
		version["pipeline_id"] = a.runtime.PipelineID
		for _, record := range a.runtime.Records {
			version[record.Stage+"_artifact"] = record.ArtifactDigest
		}
	}
	writeJSON(w, http.StatusOK, version)
}

func (a *App) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		a.methodNotAllowed(w)
		return
	}
	var req engine.QueryRequest
	if err := decodeJSON(r, &req); err != nil {
		a.writeError(w, err)
		return
	}
	query := a.query.Load()
	if query == nil {
		a.writeError(w, apperr.E(apperr.CodePrereqMissing, "index runtime is not loaded; run /v1/build", nil))
		return
	}
	result, payload, cacheHit, err := query.Execute(r.Context(), req)
	if err != nil {
		business := apperr.As(err)
		if business.QueryID != "" {
			a.saveReport(business.QueryID, business.JSONBytes())
		}
		a.writeError(w, err)
		return
	}
	if result.QueryID != "" {
		a.saveReport(result.QueryID, payload)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", map[bool]string{true: "hit", false: "miss"}[cacheHit])
	w.Header().Set("X-Query-ID", result.QueryID)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
}

func (a *App) handleReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		a.methodNotAllowed(w)
		return
	}
	queryID := r.URL.Query().Get("query_id")
	a.reportMu.Lock()
	report, ok := a.reports[queryID]
	a.reportMu.Unlock()
	if queryID != "" {
		if !ok {
			business := &apperr.Error{Code: apperr.CodeInput, Message: "query report not found", QueryID: queryID}
			payload, _ := json.Marshal(business)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write(payload)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(report)
		return
	}
	a.mu.RLock()
	runtime := a.runtime
	a.mu.RUnlock()
	if runtime == nil {
		a.writeError(w, apperr.E(apperr.CodePrereqMissing, "diagnostics runtime is not loaded", nil))
		return
	}
	writeJSON(w, http.StatusOK, runtime.Diagnostics)
}

func (a *App) handleCache(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		a.methodNotAllowed(w)
		return
	}
	query := a.query.Load()
	if query == nil {
		a.writeError(w, apperr.E(apperr.CodePrereqMissing, "query runtime is not loaded", nil))
		return
	}
	stats := query.CacheStats()
	rate := 0.0
	if total := stats.Hits + stats.Misses; total > 0 {
		rate = float64(stats.Hits) / float64(total)
	}
	writeJSON(w, http.StatusOK, map[string]any{"cache": stats, "hit_rate": rate})
}

func (a *App) handleShutdown(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		a.methodNotAllowed(w)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "shutting_down"})
	if a.server != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = a.server.Shutdown(ctx)
		}()
	}
}

func (a *App) saveReport(queryID string, payload []byte) {
	a.reportMu.Lock()
	defer a.reportMu.Unlock()
	a.reports[queryID] = append([]byte(nil), payload...)
	if len(a.reports) > 1024 {
		for key := range a.reports {
			if key != queryID {
				delete(a.reports, key)
				break
			}
		}
	}
}

func (a *App) methodNotAllowed(w http.ResponseWriter) {
	writeJSON(w, http.StatusMethodNotAllowed, apperr.E(apperr.CodeInput, "method not allowed", nil))
}

func (a *App) writeError(w http.ResponseWriter, err error) {
	business := apperr.As(err)
	status := statusForCode(business.Code)
	payload, _ := json.Marshal(business)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(payload)
}

func statusForCode(code string) int {
	switch code {
	case apperr.CodeInput:
		return http.StatusBadRequest
	case apperr.CodeStaleCheckpoint, apperr.CodePrereqMissing:
		return http.StatusConflict
	case apperr.CodeLatencyBudget:
		return http.StatusGatewayTimeout
	case apperr.CodeNumeric, apperr.CodeDecodeMismatch:
		return http.StatusUnprocessableEntity
	default:
		return http.StatusInternalServerError
	}
}

func decodeJSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return apperr.E(apperr.CodeInput, "invalid JSON request", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
}
