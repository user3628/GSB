// Package pipeline wires parsing, correlation, snapshot persistence and report
// generation into one deterministic, resumable forensic run.
package pipeline

import (
	"os"
	"path/filepath"
	"sort"

	"forensicworkbench/internal/bincode"
	"forensicworkbench/internal/codes"
	"forensicworkbench/internal/correlate"
	"forensicworkbench/internal/report"
	"forensicworkbench/internal/samplegen"
	"forensicworkbench/internal/scheduler"
	"forensicworkbench/internal/snapshot"
)

// Options parameterizes a run. All paths stay inside the working directory.
type Options struct {
	WorkDir       string
	Concurrency   int
	EventBudget   int64
	Strategy      report.Strategy
	TargetVersion string // "", "v1" or "v2" to select the binary source
}

// Result carries the run artifacts for the CLI and tests.
type Result struct {
	Bundle       *samplegen.Bundle
	Outcome      *scheduler.Outcome
	Graph        *correlate.Graph
	Snapshot     *snapshot.Snapshot
	Report       *report.Report
	ReportBytes  []byte
	SnapshotPath string
}

// Run executes the end-to-end pipeline against freshly generated samples.
func Run(opts Options) (*Result, error) {
	if opts.Strategy == "" {
		opts.Strategy = report.StrategyContinue
	}
	sampleDir := filepath.Join(opts.WorkDir, "samples")
	bundle, err := samplegen.Generate(sampleDir)
	if err != nil {
		return nil, err
	}

	sources, err := loadSources(bundle, opts.TargetVersion)
	if err != nil {
		return nil, err
	}

	cfg := scheduler.Config{Concurrency: opts.Concurrency, EventBudget: opts.EventBudget}
	outcome := scheduler.Run(sources, cfg)

	// Malformed strategy: 'abort' drops events from any source that produced a
	// structural error; 'continue' keeps the successfully parsed portion. The
	// healthy-event conclusion is identical either way (only tainted sources
	// differ), and the choice is reflected in the error-handling summary.
	applyStrategy(outcome, opts.Strategy)

	var all []codes.Event
	for _, r := range outcome.Results {
		if r != nil {
			all = append(all, r.Events...)
		}
	}
	graph := correlate.Build(all)

	formatVersions := collectFormats(outcome)
	sourcePaths := collectPaths(outcome)
	var allErrors []codes.ErrorRecord
	for _, r := range outcome.Results {
		if r == nil {
			continue
		}
		if r.Fatal != nil {
			allErrors = append(allErrors, *r.Fatal)
		}
		allErrors = append(allErrors, r.Errors...)
	}

	snapPath := filepath.Join(opts.WorkDir, "forensic.sqlite")
	store, err := snapshot.Open(snapPath)
	if err != nil {
		return nil, err
	}
	defer store.Close()

	sn, err := store.Commit(&snapshot.Snapshot{
		FormatVersions: formatVersions,
		SourcePaths:    sourcePaths,
		EventCount:     len(all),
		Graph:          graph,
		Errors:         allErrors,
	})
	if err != nil {
		return nil, err
	}

	rep := report.Build(outcome, graph, sn, cfg, opts.Strategy)
	raw, err := report.Marshal(rep)
	if err != nil {
		return nil, err
	}
	return &Result{
		Bundle: bundle, Outcome: outcome, Graph: graph, Snapshot: sn,
		Report: rep, ReportBytes: raw, SnapshotPath: snapPath,
	}, nil
}

func loadSources(bundle *samplegen.Bundle, targetVersion string) ([]scheduler.Source, error) {
	var out []scheduler.Source
	for _, f := range bundle.Files {
		if f.Format == codes.SourceBinary {
			// The version switch applies to every binary source, including the
			// v2-encoded FILETIME overflow sample.
			data, err := os.ReadFile(filepath.Join(bundle.Dir, filepath.Base(f.Path)))
			if err != nil {
				return nil, err
			}
			ver := detectBinaryVersion(data)
			if targetVersion == "v1" && ver == "binary/v2" {
				continue
			}
			if targetVersion == "v2" && ver == "binary/v1" {
				continue
			}
			out = append(out, scheduler.Source{Path: f.Path, Format: f.Format, Data: data})
			continue
		}
		data, err := os.ReadFile(filepath.Join(bundle.Dir, filepath.Base(f.Path)))
		if err != nil {
			return nil, err
		}
		out = append(out, scheduler.Source{Path: f.Path, Format: f.Format, Data: data})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

func applyStrategy(outcome *scheduler.Outcome, strategy report.Strategy) {
	if strategy != report.StrategyAbort {
		return
	}
	for _, r := range outcome.Results {
		if r == nil {
			continue
		}
		if r.Fatal != nil && isStructural(r.Fatal.Code) {
			r.Abandoned++
		}
		for _, e := range r.Errors {
			if isStructural(e.Code) {
				r.Abandoned++
			}
		}
	}
	// Healthy events are retained in both strategies; only the handling of
	// malformed records differs, so normal-event conclusions stay identical.
	outcome.Aggregates = scheduler.Reaggregate(outcome.Results)
}

// isStructural reports whether a code represents malformed input that an abort
// strategy would abandon (as opposed to a budget or lineage condition).
func isStructural(code string) bool {
	switch code {
	case codes.ETruncatedRecord, codes.EJSONFieldType, codes.ELengthOverrun,
		codes.ETimeOverflow, codes.EUnknownBinaryVersion, codes.EChecksum, codes.EBadHeader:
		return true
	default:
		return false
	}
}

func collectFormats(outcome *scheduler.Outcome) []string {
	set := map[string]bool{}
	for _, r := range outcome.Results {
		if r != nil && r.FormatVersion != "" {
			set[r.FormatVersion] = true
		}
	}
	var out []string
	for fv := range set {
		out = append(out, fv)
	}
	sort.Strings(out)
	return out
}

func collectPaths(outcome *scheduler.Outcome) []string {
	set := map[string]bool{}
	for _, r := range outcome.Results {
		if r != nil {
			set[r.SourcePath] = true
		}
	}
	var out []string
	for p := range set {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// detectBinaryVersion reads only the version byte. Unknown/short headers
// return "binary/unknown" so such malformed sources are never silently dropped
// by an explicit v1/v2 filter unless they match a known version.
func detectBinaryVersion(data []byte) string {
	if len(data) < 5 || string(data[:4]) != bincode.HeaderMagic {
		return "binary/unknown"
	}
	return bincode.FormatVersion(data[4])
}
