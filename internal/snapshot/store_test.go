package snapshot

import (
	"path/filepath"
	"testing"

	"flowtrace/internal/codes"
	"flowtrace/internal/model"
)

func openTemp(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func marks() map[string][]string { return map[string][]string{"dw.copy": {"ID_CARD"}} }
func judgments() []model.Judgment {
	return []model.Judgment{{EventSeq: 1, DstField: "dw.copy", Code: codes.OK, Version: 1}}
}

func TestCommitIncrementsVersion(t *testing.T) {
	s := openTemp(t)
	s1, err := s.Commit("W1", 1, marks(), judgments(), codes.OK, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := s.Commit("W1", 1, marks(), judgments(), codes.OK, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if s1.Version != 1 || s2.Version != 2 {
		t.Fatalf("versions %d, %d", s1.Version, s2.Version)
	}
	if s2.PrevHash != s1.ContentHash {
		t.Fatal("hash linkage broken")
	}
	chain, err := s.LoadChain("W1")
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) != 2 {
		t.Fatalf("chain len %d", len(chain))
	}
}

func TestVersionGapDetectedWithPrefixFallback(t *testing.T) {
	s := openTemp(t)
	for i := 0; i < 3; i++ {
		if _, err := s.Commit("W1", 1, marks(), judgments(), codes.OK, 1, 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.DeleteVersion("W1", 2); err != nil {
		t.Fatal(err)
	}
	chain, err := s.LoadChain("W1")
	if err == nil {
		t.Fatal("expected chain error")
	}
	ce, ok := err.(*ChainError)
	if !ok {
		t.Fatalf("want ChainError, got %T", err)
	}
	if ce.Code != codes.ESnapshotVersion {
		t.Fatalf("code %s", ce.Code)
	}
	if ce.ValidPrefix != 1 || len(chain) != 1 {
		t.Fatalf("fallback prefix wrong: %+v chain=%d", ce, len(chain))
	}
}

func TestWholesaleReplacementDetectedByContent(t *testing.T) {
	s := openTemp(t)
	if _, err := s.Commit("W1", 1, marks(), judgments(), codes.OK, 1, 1); err != nil {
		t.Fatal(err)
	}
	// Simulate a wholesale replacement: v2 with a forged prev_hash.
	db := s.db
	if _, err := db.Exec(`INSERT INTO snapshots(window_id,version,policy_version,marks_json,judgments_json,content_hash,prev_hash)
		VALUES('W1',2,1,'{}','[]','deadbeef','forged')`); err != nil {
		t.Fatal(err)
	}
	_, err := s.LoadChain("W1")
	ce, ok := err.(*ChainError)
	if !ok || ce.Code != codes.ESnapshotVersion {
		t.Fatalf("replacement must be detected: %v", err)
	}
	if ce.ValidPrefix != 1 {
		t.Fatalf("prefix %d", ce.ValidPrefix)
	}
}

func TestCrashRecoveryCommittedWindowsSurvive(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "crash.db")
	s1, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s1.Commit("W1", 1, marks(), judgments(), codes.OK, 1, 1); err != nil {
		t.Fatal(err)
	}
	// W2 is traced but never committed (simulated kill).
	s1.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	rows, err := s2.CommittedWindows()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].WindowID != "W1" {
		t.Fatalf("committed rows %+v", rows)
	}
	chain, err := s2.LoadChain("W1")
	if err != nil || len(chain) != 1 {
		t.Fatalf("chain %v %d", err, len(chain))
	}
}
