package snapshot

import (
	"path/filepath"
	"testing"

	"forensicworkbench/internal/codes"
	"forensicworkbench/internal/correlate"
)

func newTestSnapshot(events int, fmts []string) *Snapshot {
	g := correlate.Build(nil)
	return &Snapshot{
		FormatVersions: fmts,
		SourcePaths:    []string{"a.bin"},
		EventCount:     events,
		Graph:          g,
		Errors:         nil,
	}
}

func openStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "snap.sqlite")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestCommitIncrementsAndLoadChain(t *testing.T) {
	s := openStore(t)
	s1, err := s.Commit(newTestSnapshot(1, []string{"binary/v1"}))
	if err != nil {
		t.Fatal(err)
	}
	s2, err := s.Commit(newTestSnapshot(2, []string{"binary/v1", "jsonl/v1"}))
	if err != nil {
		t.Fatal(err)
	}
	if s1.Version != 1 || s2.Version != 2 {
		t.Fatalf("versions not incrementing: %d %d", s1.Version, s2.Version)
	}
	chain, err := s.LoadChain()
	if err != nil {
		t.Fatalf("chain load rejected: %v", err)
	}
	if len(chain) != 2 || chain[0].Version != 1 || chain[1].Version != 2 {
		t.Fatalf("chain not gap-free: %+v", chain)
	}
}

func TestSameVersionDifferentContentDetected(t *testing.T) {
	s := openStore(t)
	if _, err := s.Commit(newTestSnapshot(1, []string{"binary/v1"})); err != nil {
		t.Fatal(err)
	}
	// Force a replacement snapshot that reuses version 1 with different content.
	dup := newTestSnapshot(99, []string{"binary/v1"})
	dup.Version = 1
	_, err := s.Commit(dup)
	if err == nil {
		t.Fatal("expected rejection of same-version different content")
	}
	if ie, ok := err.(*IncompatError); !ok || ie.Code != codes.ESnapshotVersionIncompat {
		t.Fatalf("wrong error: %v", err)
	}
}

func TestUnsupportedFormatVersionRejectsWholeChain(t *testing.T) {
	s := openStore(t)
	if _, err := s.Commit(newTestSnapshot(1, []string{"binary/v1"})); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(newTestSnapshot(2, []string{"binary/v9"})); err != nil {
		t.Fatal(err)
	}
	chain, err := s.LoadChain()
	if err == nil {
		t.Fatalf("expected incompatibility, loaded %d snapshots", len(chain))
	}
	if ie, ok := err.(*IncompatError); !ok || ie.Code != codes.ESnapshotVersionIncompat {
		t.Fatalf("wrong error type: %v", err)
	}
	// No partial load: caller receives nil chain.
	if chain != nil {
		t.Fatal("chain must not be partially loaded")
	}
}

func TestGapRejectedAndRollsBackToContinuousPrefix(t *testing.T) {
	s := openStore(t)
	if _, err := s.Commit(newTestSnapshot(1, []string{"binary/v1"})); err != nil {
		t.Fatal(err)
	}
	// Insert a version 3 directly, skipping 2, at the database layer.
	gap := newTestSnapshot(3, []string{"binary/v1"})
	gap.Version = 3
	payload, hash, err := canonicalPayload(gap)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(
		`INSERT INTO snapshots(version,content_hash,payload,created_seq) VALUES(?,?,?,?)`,
		3, hash, payload, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT OR IGNORE INTO format_versions(version,fmt) VALUES(3,'binary/v1')`); err != nil {
		t.Fatal(err)
	}
	chain, err := s.LoadChain()
	if err == nil {
		t.Fatalf("expected gap rejection, got chain len %d", len(chain))
	}
	if ie, ok := err.(*IncompatError); !ok || ie.Code != codes.ESnapshotVersionIncompat {
		t.Fatalf("wrong error: %v", err)
	}
}

func TestIdempotentRecommit(t *testing.T) {
	s := openStore(t)
	sn := newTestSnapshot(1, []string{"binary/v1"})
	a, err := s.Commit(sn)
	if err != nil {
		t.Fatal(err)
	}
	sn2 := newTestSnapshot(1, []string{"binary/v1"})
	b, err := s.Commit(sn2)
	if err != nil {
		t.Fatal(err)
	}
	if a.ContentHash != b.ContentHash || b.Version != 1 {
		t.Fatal("identical content should be an idempotent no-op at version 1")
	}
}
