package pipeline

import (
	"path/filepath"
	"testing"

	"forensicworkbench/internal/codes"
	"forensicworkbench/internal/report"
	"forensicworkbench/internal/samplegen"
	"forensicworkbench/internal/snapshot"
	"forensicworkbench/internal/view"
)

func runOnce(t *testing.T, dir string, strategy report.Strategy, version string) *Result {
	t.Helper()
	res, err := Run(Options{
		WorkDir: dir, Concurrency: 3, EventBudget: 0,
		Strategy: strategy, TargetVersion: version,
	})
	if err != nil {
		t.Fatalf("pipeline run: %v", err)
	}
	return res
}

func codeSet(r *Result) map[string]int {
	m := map[string]int{}
	for _, e := range r.Report.Errors {
		m[e.Code]++
	}
	return m
}

func TestDeterministicByteIdenticalReports(t *testing.T) {
	a := runOnce(t, filepath.Join(t.TempDir(), "a"), report.StrategyContinue, "")
	b := runOnce(t, filepath.Join(t.TempDir(), "b"), report.StrategyContinue, "")
	if string(a.ReportBytes) != string(b.ReportBytes) {
		t.Fatal("repeated runs with identical input must produce byte-identical reports")
	}
	if a.Snapshot.ContentHash != b.Snapshot.ContentHash {
		t.Fatal("snapshot content hash must be deterministic")
	}
}

func TestFiveMalformedCodesStable(t *testing.T) {
	r := runOnce(t, t.TempDir(), report.StrategyContinue, "")
	want := map[string]int{
		codes.ETruncatedRecord:      1,
		codes.EJSONFieldType:        1,
		codes.ELengthOverrun:        1,
		codes.ETimeOverflow:         1,
		codes.EUnknownBinaryVersion: 1,
	}
	got := codeSet(r)
	for code, n := range want {
		if got[code] != n {
			t.Fatalf("code %s expected %d got %d (all=%v)", code, n, got[code], got)
		}
	}
	// Every malformed record carries an explicit skip range.
	for _, e := range r.Report.Errors {
		if e.EndOffset < e.Offset {
			t.Fatalf("error %s has invalid skip range %d-%d", e.Code, e.Offset, e.EndOffset)
		}
	}
}

func TestSnapshotVersionComesFromCommit(t *testing.T) {
	r := runOnce(t, t.TempDir(), report.StrategyContinue, "")
	if r.Report.SnapshotVersion != 1 || r.Snapshot.Version != 1 {
		t.Fatal("first committed snapshot must be version 1")
	}
	store, err := snapshot.Open(r.SnapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	chain, err := store.LoadChain()
	if err != nil {
		t.Fatalf("committed chain must load: %v", err)
	}
	if len(chain) != 1 {
		t.Fatalf("expected one snapshot, got %d", len(chain))
	}
}

func TestStrategySwitchPreservesNormalEvents(t *testing.T) {
	cont := runOnce(t, filepath.Join(t.TempDir(), "c"), report.StrategyContinue, "")
	abort := runOnce(t, filepath.Join(t.TempDir(), "d"), report.StrategyAbort, "")
	if cont.Report.NormalEventCount != abort.Report.NormalEventCount {
		t.Fatalf("normal event conclusion changed across strategies: %d vs %d",
			cont.Report.NormalEventCount, abort.Report.NormalEventCount)
	}
	if cont.Report.MalformedStrategy == abort.Report.MalformedStrategy {
		t.Fatal("strategy must be reflected in the report summary")
	}
	// Abort records abandoned malformed errors but keeps healthy events.
	if abort.Report.AbandonedErrors == 0 {
		t.Fatal("abort strategy must report abandoned malformed records")
	}
	if len(abort.Report.Timeline) != len(cont.Report.Timeline) {
		t.Fatal("healthy timeline must be identical across strategies")
	}
}

func TestVersionSelectionFiltersBinarySource(t *testing.T) {
	v1 := runOnce(t, filepath.Join(t.TempDir(), "v1"), report.StrategyContinue, "v1")
	v2 := runOnce(t, filepath.Join(t.TempDir(), "v2"), report.StrategyContinue, "v2")
	has := func(r *Result, name string) bool {
		for _, s := range r.Report.SourceSummaries {
			if filepath.Base(s.SourcePath) == name {
				return true
			}
		}
		return false
	}
	if has(v1, samplegen.BinV2File) || !has(v1, samplegen.BinV1File) {
		t.Fatal("v1 selection did not exclude the v2 binary source")
	}
	if has(v2, samplegen.BinV1File) || !has(v2, samplegen.BinV2File) {
		t.Fatal("v2 selection did not exclude the v1 binary source")
	}
	for _, fv := range v1.Report.FormatVersions {
		if fv == codes.FmtBinaryV2 {
			t.Fatal("v1 run must not depend on binary/v2")
		}
	}
}

func TestViewSessionFilterAndMultiFormat(t *testing.T) {
	r := runOnce(t, t.TempDir(), report.StrategyContinue, "")
	sessions := view.Sessions(r.Report)
	if len(sessions) == 0 {
		t.Fatal("report has no sessions to view")
	}
	filtered := view.FilterBySession(r.Report, sessions[0])
	if len(filtered) == 0 {
		t.Fatal("session filter returned no events")
	}
	for _, ev := range filtered {
		if ev.SessionID != sessions[0] {
			t.Fatal("filter leaked events from another session")
		}
	}
	// Pick an event and verify multi-format comparison resolves a key.
	match, err := view.MultiFormatSources(r.Report, r.Report.Timeline[0].EventID)
	if err != nil {
		t.Fatal(err)
	}
	if len(match.Sources) == 0 {
		t.Fatal("multi-format comparison returned no sources")
	}
}

func TestTimelineStableOrdering(t *testing.T) {
	r := runOnce(t, t.TempDir(), report.StrategyContinue, "")
	tl := r.Report.Timeline
	for i := 1; i < len(tl); i++ {
		a, b := tl[i-1], tl[i]
		if a.TimeUnixNano != nil && b.TimeUnixNano != nil && *a.TimeUnixNano > *b.TimeUnixNano {
			t.Fatal("timeline not sorted by normalized time")
		}
	}
}
