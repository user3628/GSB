package app

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"csrouter/internal/domain"
	"csrouter/internal/store"
)

type App struct {
	Root        string
	SampleDir   string
	ReportDir   string
	MutationDir string
	DB          *store.Store
	runMu       sync.Mutex
}

func New(root string) (*App, error) {
	if root == "" {
		root = "data"
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	app := &App{
		Root:        absRoot,
		SampleDir:   filepath.Join(absRoot, "samples"),
		ReportDir:   filepath.Join(absRoot, "reports"),
		MutationDir: filepath.Join(absRoot, "mutations"),
	}
	for _, dir := range []string{absRoot, app.SampleDir, app.ReportDir, app.MutationDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	db, err := store.Open(filepath.Join(absRoot, "csrouter.db"))
	if err != nil {
		return nil, err
	}
	app.DB = db
	return app, nil
}

func (a *App) Close() error { return a.DB.Close() }

func (a *App) ManifestPath() string { return filepath.Join(a.SampleDir, "manifest.json") }

func (a *App) LoadManifest() (*domain.Manifest, error) {
	b, err := os.ReadFile(a.ManifestPath())
	if err != nil {
		return nil, err
	}
	var manifest domain.Manifest
	if err := json.Unmarshal(b, &manifest); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func (a *App) loadPolicy(tier string) (domain.Policy, error) {
	policy, err := a.DB.LatestPolicy(tier)
	if err != nil {
		return domain.Policy{}, err
	}
	if policy == nil {
		manifest, err := a.LoadManifest()
		if err != nil {
			return domain.Policy{}, err
		}
		tm, ok := manifest.Tiers[tier]
		if !ok {
			return domain.Policy{}, domain.NewBizError(domain.ErrNotFound, "", "", 0, "unknown tier %q", tier)
		}
		b, err := os.ReadFile(filepath.Join(a.SampleDir, tm.PolicyPath))
		if err != nil {
			return domain.Policy{}, err
		}
		var fromFile domain.Policy
		if err := json.Unmarshal(b, &fromFile); err != nil {
			return domain.Policy{}, err
		}
		if err := a.DB.SavePolicy(fromFile); err != nil {
			return domain.Policy{}, err
		}
		policy = &fromFile
	}
	return *policy, nil
}

func writeJSONFile(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o644)
}

func readJSONFile(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	return nil
}
