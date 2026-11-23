package dnsdetect

import (
	"context"
	"os"
	"testing"
)

func testReportContent(label string) []byte {
	report := &Report{
		Schema:    ReportSchema,
		TimeScale: TimeScale,
		Sampling:  SamplingMarker,
		Config: ReportConfig{
			AssociationWindowTicks: 500,
			WindowSpanTicks:        1000,
			MaxConcurrency:         2,
			MaxEventsPerSession:    10,
			MaxSpanTicksPerSession: 1000,
			StrictTXIDQName:        true,
		},
		Sources:   []SourceSummary{{Path: label, Kind: SourceJSONL, Priority: 20, EventCount: 1}},
		Windows:   []WindowProgress{{Index: 1, StartTicks: 1000, EndTicks: 2000}},
		Pairs:     []Pair{},
		Findings:  []Finding{},
		Uncertain: []UncertainAssociation{},
		Ledger:    []LedgerAggregate{},
	}
	ensureReportSlices(report)
	content, _ := report.CanonicalBytes()
	return content
}

func commitTestSnapshot(t *testing.T, store *SnapshotStore, content []byte) *Snapshot {
	t.Helper()
	snapshot, err := store.Commit(context.Background(), SnapshotDraft{
		CreatedTicks:     1_704_067_200_000_000_00,
		FormatVersions:   []int{2, 1},
		SourcePriorities: []SourcePriority{{Kind: SourceText, Priority: 10}, {Kind: SourceBinary, Priority: 30}},
		LedgerDigest:     "ledger-digest",
		WindowProgress:   []WindowProgress{{Index: 1, StartTicks: 1000, EndTicks: 2000}},
		Content:          content,
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return snapshot
}

func TestSnapshotChainReplayAndTamper(t *testing.T) {
	store := testStore(t, "chain")
	firstContent := testReportContent("first")
	secondContent := testReportContent("second")
	first := commitTestSnapshot(t, store, firstContent)
	second := commitTestSnapshot(t, store, secondContent)
	if first.Version != 1 || second.Version != 2 {
		t.Fatalf("versions = %d,%d", first.Version, second.Version)
	}
	if second.PrevContentDigest != first.ContentDigest {
		t.Fatalf("content chain link mismatch")
	}
	idempotent := commitTestSnapshot(t, store, secondContent)
	if idempotent.Version != 2 {
		t.Fatalf("idempotent commit advanced to %d", idempotent.Version)
	}
	load, err := store.Load(context.Background())
	if err != nil || len(load.Snapshots) != 2 || load.Latest.Version != 2 {
		t.Fatalf("Load = %+v, %v", load, err)
	}
	if len(load.Latest.FormatVersions) != 2 || load.Latest.FormatVersions[0] != 1 || load.Latest.FormatVersions[1] != 2 {
		t.Fatalf("format versions not persisted: %v", load.Latest.FormatVersions)
	}
	if load.Latest.LedgerDigest != "ledger-digest" || len(load.Latest.SourcePriorities) != 2 || load.Latest.SourcePriorities[0].Kind != SourceBinary {
		t.Fatalf("snapshot metadata not persisted: %+v", load.Latest)
	}
	replay, err := store.Replay(context.Background(), 1)
	if err != nil || replay.Sources[0].Path != "first" || replay.Snapshot.Version != 1 {
		t.Fatalf("replay v1 = %+v, %v", replay, err)
	}

	if _, err := store.db.ExecContext(context.Background(), "UPDATE snapshots SET content = ? WHERE version = 2", []byte("tampered")); err != nil {
		t.Fatal(err)
	}
	tampered, err := store.Load(context.Background())
	requireCode(t, err, CodeSnapshotReplaced)
	if tampered.Latest == nil || tampered.Latest.Version != 1 {
		t.Fatalf("tampered fallback = %+v", tampered.Latest)
	}
}

func TestWholeDatabaseReplacementNeedsContentAnchor(t *testing.T) {
	path := t.TempDir() + "/replacement.db"
	store, err := OpenSnapshotStore(path)
	if err != nil {
		t.Fatal(err)
	}
	original := commitTestSnapshot(t, store, testReportContent("trusted"))
	expectedHead := original.Head()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Remove(path + suffix); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	replacement, err := OpenSnapshotStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	replacedSnapshot := commitTestSnapshot(t, replacement, testReportContent("replaced"))
	if replacedSnapshot.Version != expectedHead.Version {
		t.Fatalf("test setup versions differ")
	}
	err = replacement.VerifyHead(context.Background(), expectedHead)
	requireCode(t, err, CodeSnapshotReplaced)
}

func TestVerifyHeadRequiresCurrentHeadAndMetadataAffectsIdempotence(t *testing.T) {
	store := testStore(t, "verify-head")
	content := testReportContent("metadata")
	first := commitTestSnapshot(t, store, content)
	if err := store.VerifyHead(context.Background(), first.Head()); err != nil {
		t.Fatalf("current head rejected: %v", err)
	}

	changedMetadata := first
	changedMetadata.LedgerDigest = "different-ledger"
	snapshot, err := store.Commit(context.Background(), SnapshotDraft{
		CreatedTicks:     first.CreatedTicks,
		FormatVersions:   first.FormatVersions,
		SourcePriorities: first.SourcePriorities,
		LedgerDigest:     changedMetadata.LedgerDigest,
		WindowProgress:   first.WindowProgress,
		Content:          content,
	})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Version != 2 {
		t.Fatalf("metadata-only change did not advance version: %d", snapshot.Version)
	}
	if err := store.VerifyHead(context.Background(), first.Head()); CodeOf(err) != CodeSnapshotVersion {
		t.Fatalf("old head should not be accepted as current head: %v", err)
	}
	if err := store.VerifyHead(context.Background(), snapshot.Head()); err != nil {
		t.Fatalf("new current head rejected: %v", err)
	}
}
