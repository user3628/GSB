package verify

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"baselineworkbench/internal/codes"
	"baselineworkbench/internal/engine"
	"baselineworkbench/internal/ledger"
	"baselineworkbench/internal/policy"
	"baselineworkbench/internal/samples"
	"baselineworkbench/internal/snapshot"
)

type Options struct {
	Workdir     string
	SamplesRoot string
	Injections  []string
	Config      engine.Config
}

type Case struct {
	Name                    string     `json:"name"`
	PackagePath             string     `json:"package_path"`
	ExpectedCode            codes.Code `json:"expected_code"`
	ActualCode              codes.Code `json:"actual_code"`
	LedgerTerminal          string     `json:"ledger_terminal"`
	LedgerEntryCount        int        `json:"ledger_entry_count"`
	LedgerUnchanged         bool       `json:"ledger_unchanged_after_injection"`
	SnapshotVersionBefore   int64      `json:"snapshot_version_before"`
	SnapshotVersionAfter    int64      `json:"snapshot_version_after"`
	SnapshotActiveDigest    string     `json:"snapshot_active_digest"`
	SnapshotUnchanged       bool       `json:"snapshot_unchanged"`
	FallbackChecked         bool       `json:"fallback_checked,omitempty"`
	FallbackCode            codes.Code `json:"fallback_code,omitempty"`
	FallbackVersion         int64      `json:"fallback_version,omitempty"`
	FallbackDiagnosticScore int64      `json:"fallback_diagnostic_score,omitempty"`
	FallbackPassed          bool       `json:"fallback_passed,omitempty"`
	Passed                  bool       `json:"passed"`
	Detail                  string     `json:"detail,omitempty"`
}

type Report struct {
	Schema    string `json:"schema"`
	Mode      string `json:"mode"`
	AllPassed bool   `json:"all_passed"`
	Cases     []Case `json:"cases"`
}

var caseOrder = []string{"unknown-unit", "mul-overflow", "filetime-overflow", "version-gap", "error-bound"}

var casePackages = map[string]string{
	"unknown-unit":      "packages/unknown-unit",
	"mul-overflow":      "packages/overflow-multiply",
	"filetime-overflow": "packages/filetime-overflow",
	"version-gap":       "packages/normal",
	"error-bound":       "packages/error-bound",
}

var caseCodes = map[string]codes.Code{
	"unknown-unit":      codes.UnitUnknown,
	"mul-overflow":      codes.NumericOverflow,
	"filetime-overflow": codes.NumericOverflow,
	"version-gap":       codes.SnapshotVersion,
	"error-bound":       codes.ErrorBound,
}

func Run(ctx context.Context, options Options) (Report, error) {
	manifest, err := samples.LoadManifest(options.SamplesRoot)
	if err != nil {
		return Report{}, err
	}
	selected, err := selectCases(options.Injections)
	if err != nil {
		return Report{}, err
	}
	referenceInput, err := loadPackage(options.SamplesRoot, manifest, manifest.Reference)
	if err != nil {
		return Report{}, err
	}
	reference := engine.Prepare(referenceInput)
	if len(reference.Issues) > 0 {
		return Report{}, fmt.Errorf("reference package invalid: %s", reference.Issues[0].Message)
	}
	cfg := options.Config.WithDefaults()
	cfg.Mode = engine.ModeStrict
	if cfg.MaxErrorBoundMicros == 0 {
		cfg.MaxErrorBoundMicros = 100
	}
	report := Report{
		Schema: "baseline.workbench.verification/v1",
		Mode:   string(cfg.Mode),
		Cases:  []Case{},
	}
	for _, name := range selected {
		result, err := runCase(ctx, options, manifest, reference, name, cfg)
		if err != nil {
			return Report{}, err
		}
		report.Cases = append(report.Cases, result)
		if !result.Passed {
			report.AllPassed = false
		}
	}
	report.AllPassed = true
	for _, result := range report.Cases {
		if !result.Passed {
			report.AllPassed = false
			break
		}
	}
	return report, nil
}

func runCase(ctx context.Context, options Options, manifest samples.Manifest, reference engine.Prepared, name string, cfg engine.Config) (Case, error) {
	caseRoot := filepath.Join(options.Workdir, "cases", name)
	if err := os.RemoveAll(caseRoot); err != nil {
		return Case{}, err
	}
	if err := os.MkdirAll(caseRoot, 0o755); err != nil {
		return Case{}, err
	}
	store, err := snapshot.Open(filepath.Join(caseRoot, "snapshots.db"))
	if err != nil {
		return Case{}, err
	}
	defer store.Close()
	packagePath := casePackages[name]
	seedPayload, err := snapshot.EncodeContent(packagePath, manifest.SampleMarker, reference.Effective)
	if err != nil {
		return Case{}, err
	}
	seed, business := store.Append(ctx, packagePath, seedPayload, manifest.SampleMarker, snapshot.AppendOptions{})
	if business != nil {
		return Case{}, business
	}
	chainBefore, err := store.LoadChain(ctx, packagePath)
	if err != nil {
		return Case{}, err
	}
	versionBefore := seed.Version
	seedDigest := seed.ContentDigest
	if chainBefore.Active == nil || chainBefore.Active.ContentDigest != seedDigest {
		return Case{}, fmt.Errorf("seed snapshot chain is inconsistent")
	}
	input, err := loadPackage(options.SamplesRoot, manifest, packagePath)
	if err != nil {
		return Case{}, err
	}
	results := engine.Run(ctx, []engine.Input{input}, reference, cfg)
	if len(results) != 1 {
		return Case{}, fmt.Errorf("expected one result for %s", name)
	}
	result := results[0]
	ledgerDigestBefore := ledgerDigest(result.Ledger)
	actualCode := result.Code
	detail := ""
	fallbackChecked := false
	fallbackCode := codes.None
	fallbackVersion := int64(0)
	fallbackScore := int64(0)
	fallbackPassed := false
	if name == "version-gap" {
		payload, err := snapshot.EncodeContent(result.PackagePath, manifest.SampleMarker, result.Items)
		if err != nil {
			return Case{}, err
		}
		forcedVersion := int64(3)
		for _, injection := range manifest.Injections {
			if injection.Class == "snapshot_version_gap" && injection.PackagePath == packagePath {
				forcedVersion = injection.ForcedVersion
			}
		}
		_, gapErr := store.Append(ctx, result.PackagePath, payload, manifest.SampleMarker, snapshot.AppendOptions{ForcedVersion: forcedVersion})
		if gapErr == nil {
			actualCode = codes.None
			detail = "forced version gap was unexpectedly accepted"
		} else {
			actualCode = gapErr.Code
			detail = gapErr.Message
		}
		fallbackChecked = true
		fallbackCode, fallbackVersion, fallbackScore, fallbackPassed, err = verifyLoadFallback(ctx, caseRoot, packagePath, manifest)
		if err != nil {
			return Case{}, err
		}
	} else if result.Complete {
		payload, err := snapshot.EncodeContent(result.PackagePath, manifest.SampleMarker, result.Items)
		if err != nil {
			return Case{}, err
		}
		if _, business := store.Append(ctx, result.PackagePath, payload, manifest.SampleMarker, snapshot.AppendOptions{}); business != nil {
			return Case{}, business
		}
		detail = "invalid sample unexpectedly produced a snapshot"
	}
	ledgerDigestAfter := ledgerDigest(result.Ledger)
	versionAfter, err := store.Version(ctx, packagePath)
	if err != nil {
		return Case{}, err
	}
	chainAfter, err := store.LoadChain(ctx, packagePath)
	if err != nil {
		return Case{}, err
	}
	activeDigest := ""
	if chainAfter.Active != nil {
		activeDigest = chainAfter.Active.ContentDigest
	}
	ledgerUnchanged := ledgerDigestBefore == ledgerDigestAfter
	snapshotUnchanged := versionAfter == versionBefore && activeDigest == seedDigest
	expectedCode := caseCodes[name]
	passed := actualCode == expectedCode && ledgerUnchanged && snapshotUnchanged
	if name != "version-gap" {
		passed = passed && result.Ledger.Terminal == "rolled_back" && len(result.Ledger.Entries) == 0
	} else {
		passed = passed && result.Ledger.Terminal == "committed" && fallbackPassed
	}
	if passed && detail == "" {
		detail = "expected code observed; ledger stable; snapshot version unchanged"
	}
	return Case{
		Name:                    name,
		PackagePath:             packagePath,
		ExpectedCode:            expectedCode,
		ActualCode:              actualCode,
		LedgerTerminal:          result.Ledger.Terminal,
		LedgerEntryCount:        len(result.Ledger.Entries),
		LedgerUnchanged:         ledgerUnchanged,
		SnapshotVersionBefore:   versionBefore,
		SnapshotVersionAfter:    versionAfter,
		SnapshotActiveDigest:    activeDigest,
		SnapshotUnchanged:       snapshotUnchanged,
		FallbackChecked:         fallbackChecked,
		FallbackCode:            fallbackCode,
		FallbackVersion:         fallbackVersion,
		FallbackDiagnosticScore: fallbackScore,
		FallbackPassed:          fallbackPassed,
		Passed:                  passed,
		Detail:                  detail,
	}, nil
}

func verifyLoadFallback(ctx context.Context, caseRoot, packagePath string, manifest samples.Manifest) (codes.Code, int64, int64, bool, error) {
	dbPath := filepath.Join(caseRoot, "gap-load.db")
	store, err := snapshot.Open(dbPath)
	if err != nil {
		return codes.None, 0, 0, false, err
	}
	fake := engine.Result{
		PackagePath:      packagePath,
		Complete:         true,
		Items:            []policy.Item{{ID: "recovered.item", Unit: "second", RawValue: "1", Normalized: 1, NormalizedScaled: 1_000_000}},
		Comparisons:      []engine.Comparison{},
		ScoreMicros:      777,
		ErrorUpperMicros: 3,
		ScoreInterval:    engine.Interval{Lower: 774, Upper: 780, Unit: "score_1e-6"},
		Codes:            []codes.Code{},
		Ledger:           ledger.Ledger{Entries: []ledger.Entry{}, Terminal: ledger.TerminalCommitted},
	}
	payload, err := snapshot.EncodeResult(packagePath, manifest.SampleMarker, fake)
	if err != nil {
		_ = store.Close()
		return codes.None, 0, 0, false, err
	}
	if _, business := store.Append(ctx, packagePath, payload, manifest.SampleMarker, snapshot.AppendOptions{}); business != nil {
		_ = store.Close()
		return codes.None, 0, 0, false, business
	}
	if err := store.Close(); err != nil {
		return codes.None, 0, 0, false, err
	}
	gapPayload, err := snapshot.EncodeContent(packagePath, manifest.SampleMarker, []policy.Item{{ID: "gap.item", Unit: "second", RawValue: "9", Normalized: 9, NormalizedScaled: 9_000_000}})
	if err != nil {
		return codes.None, 0, 0, false, err
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return codes.None, 0, 0, false, err
	}
	sum := sha256.Sum256(gapPayload)
	_, err = db.ExecContext(ctx, `
INSERT INTO snapshots (package_path, version, parent_version, content_digest, parent_digest,
                       content_changed, whole_replacement, sample_marker, payload)
VALUES (?, 3, 2, ?, ?, 1, 0, ?, ?)`,
		packagePath, hex.EncodeToString(sum[:]), "missing-parent", manifest.SampleMarker, gapPayload)
	if err != nil {
		_ = db.Close()
		return codes.None, 0, 0, false, err
	}
	if err := db.Close(); err != nil {
		return codes.None, 0, 0, false, err
	}
	store, err = snapshot.Open(dbPath)
	if err != nil {
		return codes.None, 0, 0, false, err
	}
	defer store.Close()
	chain, err := store.LoadChain(ctx, packagePath)
	if err != nil {
		return codes.None, 0, 0, false, err
	}
	if chain.ErrorCode != codes.SnapshotVersion || chain.Active == nil || chain.Active.Version != 1 {
		return chain.ErrorCode, 0, 0, false, nil
	}
	content, err := snapshot.DecodeContent(chain.Active.Payload)
	if err != nil {
		return chain.ErrorCode, chain.Active.Version, 0, false, err
	}
	if content.Diagnostic == nil {
		return chain.ErrorCode, chain.Active.Version, 0, false, nil
	}
	passed := content.Diagnostic.ScoreMicros == 777 && content.Diagnostic.ErrorUpperMicros == 3
	return chain.ErrorCode, chain.Active.Version, content.Diagnostic.ScoreMicros, passed, nil
}

func selectCases(injections []string) ([]string, error) {
	if len(injections) == 0 {
		return append([]string{}, caseOrder...), nil
	}
	seen := make(map[string]bool)
	for _, value := range injections {
		for _, name := range strings.Split(value, ",") {
			name = strings.TrimSpace(name)
			if name == "" || name == "all" {
				continue
			}
			if _, ok := casePackages[name]; !ok {
				return nil, fmt.Errorf("unknown injection %q", name)
			}
			seen[name] = true
		}
	}
	if len(seen) == 0 {
		return append([]string{}, caseOrder...), nil
	}
	out := make([]string, 0, len(seen))
	for _, name := range caseOrder {
		if seen[name] {
			out = append(out, name)
		}
	}
	return out, nil
}

func loadPackage(root string, manifest samples.Manifest, packagePath string) (engine.Input, error) {
	for _, pkg := range manifest.Packages {
		if slash(pkg.Path) != slash(packagePath) {
			continue
		}
		baseline, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(pkg.Path), filepath.FromSlash(pkg.Baseline)))
		if err != nil {
			return engine.Input{}, err
		}
		repairs, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(pkg.Path), filepath.FromSlash(pkg.Repairs)))
		if err != nil {
			return engine.Input{}, err
		}
		return engine.Input{PackagePath: slash(pkg.Path), Baseline: baseline, Repairs: repairs}, nil
	}
	return engine.Input{}, fmt.Errorf("package %q not found", packagePath)
}

func ledgerDigest(value any) string {
	encoded, _ := json.Marshal(value)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func JSONBytes(report Report) ([]byte, error) {
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func Summary(report Report) string {
	var builder strings.Builder
	builder.WriteString("Anomaly injection verification\n")
	for _, result := range report.Cases {
		status := "PASS"
		if !result.Passed {
			status = "FAIL"
		}
		fmt.Fprintf(&builder, "%s %-18s expected=%-18s actual=%-18s ledger=%-11s snapshot=v%d->v%d\n",
			status, result.Name, result.ExpectedCode, result.ActualCode, result.LedgerTerminal,
			result.SnapshotVersionBefore, result.SnapshotVersionAfter)
		if result.FallbackChecked {
			fmt.Fprintf(&builder, "     fallback=%s version=v%d score=%d passed=%t\n",
				result.FallbackCode, result.FallbackVersion, result.FallbackDiagnosticScore, result.FallbackPassed)
		}
	}
	fmt.Fprintf(&builder, "all_passed=%t\n", report.AllPassed)
	return builder.String()
}

func SortCases(cases []Case) {
	sort.Slice(cases, func(i, j int) bool { return cases[i].Name < cases[j].Name })
}

func slash(path string) string { return strings.ReplaceAll(path, "\\", "/") }
