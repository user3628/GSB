// Package cli implements the interactive, subcommand-driven demo entry point.
package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"casengine/internal/clock"
	"casengine/internal/engine"
	"casengine/internal/metrics"
	"casengine/internal/sample"
	"casengine/internal/store"
)

// App holds shared runtime wiring for a work directory.
type App struct {
	WorkDir string
	DBPath  string
	Params  *sample.Params
}

// NewApp resolves paths and (re)generates the deterministic parameter file.
func NewApp(workDir string) (*App, error) {
	if workDir == "" {
		workDir = filepath.Join(os.TempDir(), "casengine-work")
	}
	if err := os.MkdirAll(filepath.Join(workDir, "reports"), 0o755); err != nil {
		return nil, err
	}
	app := &App{
		WorkDir: workDir,
		DBPath:  filepath.Join(workDir, "casengine.db"),
		Params:  sample.Generate(sample.DefaultSeed),
	}
	if err := app.writeParams(); err != nil {
		return nil, err
	}
	return app, nil
}

func (a *App) paramsPath() string { return filepath.Join(a.WorkDir, "params.json") }

func (a *App) writeParams() error {
	raw, err := json.MarshalIndent(a.Params, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(a.paramsPath(), append(raw, '\n'), 0o644)
}

// openStore opens the SQLite database and registers the deterministic tenants.
func (a *App) openStore() (*store.Store, error) {
	st, err := store.Open(a.DBPath)
	if err != nil {
		return nil, err
	}
	for _, t := range a.Params.Tenants {
		if err := st.UpsertTenant(t.ID, t.Name, t.Secret); err != nil {
			_ = st.Close()
			return nil, err
		}
	}
	return st, nil
}

// Runtime bundles a fully wired engine for one process run.
type Runtime struct {
	App    *App
	Store  *store.Store
	Engine *engine.Engine
	Clock  clock.Clock
	Metric *metrics.Collector
}

// Close releases all runtime resources in order.
func (rt *Runtime) Close() {
	if rt.Engine != nil {
		rt.Engine.Close()
	}
	if rt.Metric != nil {
		rt.Metric.Stop()
	}
	if rt.Store != nil {
		_ = rt.Store.Close()
	}
}

// newRuntime builds either a real-clock or scripted-clock runtime.
func (a *App) newRuntime(scripted bool, cfg engine.Config) (*Runtime, *clock.ScriptedClock, error) {
	st, err := a.openStore()
	if err != nil {
		return nil, nil, err
	}
	mc := metrics.NewCollector(cfg.LatencyBuffer)
	rt := &Runtime{App: a, Store: st, Metric: mc}
	var sc *clock.ScriptedClock
	if scripted {
		sc = clock.NewScripted()
		rt.Clock = sc
		cfg.CleanupInterval = 0 // replay drives expiry deterministically
	} else {
		rt.Clock = clock.NewReal()
	}
	rt.Engine = engine.New(st, rt.Clock, mc, cfg)
	var tenantIDs []string
	for _, t := range a.Params.Tenants {
		tenantIDs = append(tenantIDs, t.ID)
	}
	if _, err := rt.Engine.EnsureAllChains(tenantIDs); err != nil {
		rt.Close()
		return nil, nil, err
	}
	return rt, sc, nil
}

func deterministicConfig() engine.Config {
	cfg := engine.DefaultConfig()
	cfg.CleanupInterval = 0
	cfg.AcquireTimeout = 500 * time.Millisecond
	return cfg
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
