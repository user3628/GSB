// Package run orchestrates one end-to-end workbench run: fixture generation,
// concurrent session resolution, snapshot commits (including externally
// forced version gaps) and deterministic report rendering.
package run

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"pathcourt/internal/bizcode"
	"pathcourt/internal/inject"
	"pathcourt/internal/reparse"
	"pathcourt/internal/report"
	"pathcourt/internal/resolver"
	"pathcourt/internal/sample"
	"pathcourt/internal/session"
	"pathcourt/internal/snapshot"
)

// Options controls a run.
type Options struct {
	WorkDir string
	Config  inject.Config
	DBPath  string
}

// Outcome summarizes the run for the CLI.
type Outcome struct {
	Workspace    *sample.Workspace
	Store        *snapshot.Store
	Versions     []snapshot.Version
	LoadErr      error
	ReportPath   string
	ReportBytes  []byte
	SessionStats session.Stats
	Outcomes     []*session.SessionOutcome
}

// Execute performs a full deterministic run.
func Execute(ctx context.Context, opts Options) (*Outcome, error) {
	cfg, err := opts.Config.Validate()
	if err != nil {
		return nil, err
	}
	workDir := opts.WorkDir
	if workDir == "" {
		workDir, err = os.MkdirTemp("", "pathcourt-")
		if err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return nil, err
	}

	factory, err := reparse.NewFactory(filepath.Join(workDir, "probe"))
	if err != nil {
		return nil, err
	}
	ws, err := sample.Generate(workDir, cfg.Scenario, factory)
	if err != nil {
		return nil, err
	}

	dbPath := opts.DBPath
	if dbPath == "" {
		dbPath = filepath.Join(workDir, "snapshots.db")
	}
	store, err := snapshot.Open(ctx, dbPath)
	if err != nil {
		return nil, err
	}

	r := resolver.New(resolver.Config{Root: ws.Root, MaxJumps: cfg.MaxJumps}, reparse.NewInspector())
	mgr := session.NewManager(session.Config{
		MaxConcurrent:      cfg.MaxConcurrent,
		MaxPathsPerSession: cfg.MaxPathsPerSession,
	}, r, store)

	manifests := append([]sample.Manifest(nil), ws.Manifests...)
	if len(cfg.ExtraPaths) > 0 {
		id := cfg.ExtraManifestID
		if id == "" {
			id = "m99-extra"
		}
		manifests = append(manifests, sample.Manifest{ID: id, Paths: cfg.ExtraPaths})
	}

	out := &Outcome{Workspace: ws, Store: store}
	for _, manifest := range manifests {
		so, err := mgr.Submit(ctx, manifest.ID, manifest.Paths)
		if err != nil {
			return nil, err
		}
		out.Outcomes = append(out.Outcomes, so)
	}
	out.SessionStats = mgr.Stats()

	// Externally driven snapshot version-gap injection: all legitimate
	// manifests have already committed contiguous versions. The forced
	// probe commit jumps the version number; loading then detects the gap
	// and rolls back to the most recent contiguous version, leaving the
	// already committed snapshots untouched.
	if cfg.Scenario == sample.ScenarioSnapshot && cfg.ForceSnapshotGap > 0 {
		probe := []snapshot.Row{{
			ManifestID: "gap-probe",
			PathIndex:  0,
			Input:      "__gap_probe__",
			Result: &resolver.Result{
				ManifestID: "gap-probe",
				Input:      "__gap_probe__",
				Root:       ws.Root,
				Code:       bizcode.OK,
				Steps:      []resolver.Step{},
				Hops:       []resolver.Hop{},
			},
		}}
		if _, err := store.CommitAtVersion(ctx, cfg.ForceSnapshotGap, ws.Root, "gap-probe", probe); err != nil {
			return nil, err
		}
	}

	versions, loadErr := store.LoadChain(ctx)
	out.Versions = versions
	out.LoadErr = loadErr
	if loadErr != nil && !errors.Is(loadErr, snapshot.ErrVersionGap) {
		return nil, loadErr
	}

	builder := report.NewBuilder(workDir, report.DefaultWorkspaceAlias)
	reportBytes, err := builder.Build(versions, loadErr)
	if err != nil {
		return nil, err
	}
	out.ReportBytes = reportBytes
	out.ReportPath = filepath.Join(workDir, "report.json")
	if err := os.WriteFile(out.ReportPath, append(reportBytes, '\n'), 0o644); err != nil {
		return nil, err
	}
	return out, nil
}

// PrintCapabilities emits a short host capability summary.
func PrintCapabilities(workDir string) (reparse.Capabilities, error) {
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return reparse.Capabilities{}, err
	}
	factory, err := reparse.NewFactory(filepath.Join(workDir, "probe"))
	if err != nil {
		return reparse.Capabilities{}, err
	}
	caps := factory.Capabilities()
	if !caps.Symlinks {
		return caps, fmt.Errorf("symlink creation unavailable: %s; junctions/relative links will be used as fallback", caps.SymlinkProbe)
	}
	return caps, nil
}
