package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"path/filepath"
	"testing"

	"baselineworkbench/internal/codes"
	"baselineworkbench/internal/engine"
	"baselineworkbench/internal/ledger"
	"baselineworkbench/internal/policy"
	"baselineworkbench/internal/samples"
	"baselineworkbench/internal/snapshot"
)

func TestRunProducesDeterministicReportAndExpectedCodes(t *testing.T) {
	root := t.TempDir()
	sampleRoot := filepath.Join(root, "samples")
	if _, err := samples.Generate(sampleRoot); err != nil {
		t.Fatal(err)
	}
	cfg := engine.Config{Mode: engine.ModeStrict, MaxConcurrency: 2, MaxItems: 64, MaxErrorBoundMicros: 100}
	run := func(name string) RunOutput {
		output, err := Run(context.Background(), RunOptions{
			SamplesRoot:  sampleRoot,
			DatabasePath: filepath.Join(root, name, "snapshots.db"),
			Config:       cfg,
		})
		if err != nil {
			t.Fatal(err)
		}
		return output
	}
	first := run("first")
	second := run("second")
	if !bytes.Equal(first.ReportJSON, second.ReportJSON) {
		t.Fatal("report bytes differ across identical runs")
	}
	if first.SummaryText != second.SummaryText {
		t.Fatal("terminal summary differs across identical runs")
	}
	expected := map[string]codes.Code{
		"packages/reference":         codes.None,
		"packages/normal":            codes.None,
		"packages/mixed-units":       codes.None,
		"packages/truncated-text":    codes.PolicyMalformed,
		"packages/unknown-unit":      codes.UnitUnknown,
		"packages/overflow-multiply": codes.NumericOverflow,
		"packages/filetime-overflow": codes.NumericOverflow,
		"packages/error-bound":       codes.ErrorBound,
	}
	for _, pkg := range first.Report.Packages {
		if pkg.Code != expected[pkg.PackagePath] {
			t.Fatalf("%s: got code %q want %q", pkg.PackagePath, pkg.Code, expected[pkg.PackagePath])
		}
		if pkg.Complete && pkg.Snapshot.ActiveVersion != 1 {
			t.Fatalf("%s: complete package missing snapshot v1", pkg.PackagePath)
		}
		if !pkg.Complete && pkg.Snapshot.ActiveVersion != 0 {
			t.Fatalf("%s: failed package advanced snapshot", pkg.PackagePath)
		}
	}
}

func TestRunRecoversFallbackDiagnosticAfterVersionGap(t *testing.T) {
	root := t.TempDir()
	sampleRoot := filepath.Join(root, "samples")
	if _, err := samples.Generate(sampleRoot); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(root, "snapshots.db")
	store, err := snapshot.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	recoveredItem := policy.Item{ID: "recovered.item", Unit: "second", RawValue: "1", Normalized: 1, NormalizedScaled: 1_000_000}
	fake := engine.Result{
		PackagePath:      "packages/normal",
		Complete:         true,
		Items:            []policy.Item{recoveredItem},
		Comparisons:      []engine.Comparison{},
		ScoreMicros:      12_345,
		ErrorUpperMicros: 7,
		ScoreInterval:    engine.Interval{Lower: 12_338, Upper: 12_352, Unit: "score_1e-6"},
		Codes:            []codes.Code{},
		Ledger:           ledger.Ledger{Entries: []ledger.Entry{}, Terminal: ledger.TerminalCommitted},
	}
	payload, err := snapshot.EncodeResult("packages/normal", samples.SampleMarker, fake)
	if err != nil {
		t.Fatal(err)
	}
	if _, business := store.Append(context.Background(), "packages/normal", payload, samples.SampleMarker, snapshot.AppendOptions{}); business != nil {
		t.Fatal(business)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	gapPayload, _ := snapshot.EncodeContent("packages/normal", samples.SampleMarker, []policy.Item{recoveredItem})
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(gapPayload)
	_, err = db.Exec(`
INSERT INTO snapshots (package_path, version, parent_version, content_digest, parent_digest,
                       content_changed, whole_replacement, sample_marker, payload)
VALUES (?, 3, 2, ?, ?, 1, 0, ?, ?)`,
		"packages/normal", hex.EncodeToString(sum[:]), "missing", samples.SampleMarker, gapPayload)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := Run(context.Background(), RunOptions{
		SamplesRoot:  sampleRoot,
		DatabasePath: dbPath,
		Config:       engine.Config{Mode: engine.ModeStrict, MaxConcurrency: 2, MaxItems: 64, MaxErrorBoundMicros: 100},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, pkg := range output.Report.Packages {
		if pkg.PackagePath != "packages/normal" {
			continue
		}
		if pkg.Code != codes.SnapshotVersion || pkg.Complete {
			t.Fatalf("gap was not surfaced: code=%s complete=%t", pkg.Code, pkg.Complete)
		}
		if pkg.RecoveredFromVersion != 1 || pkg.Snapshot.ActiveVersion != 1 {
			t.Fatalf("unexpected recovered version: %+v", pkg.Snapshot)
		}
		if pkg.ScoreMicros != 12_345 || pkg.ErrorUpperMicros != 7 {
			t.Fatalf("conclusion does not match fallback snapshot: score=%d error=%d", pkg.ScoreMicros, pkg.ErrorUpperMicros)
		}
		return
	}
	t.Fatal("packages/normal not found in report")
}
