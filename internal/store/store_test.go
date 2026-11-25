package store

import (
	"path/filepath"
	"sync"
	"testing"

	"headprune/internal/generate"
	"headprune/internal/model"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "ws")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func sampleCommit(v int64, heads model.HeadSet) CommitInput {
	return CommitInput{
		HeadFP:         heads.Fingerprint(),
		ProbeDigest:    "probe-" + heads.Fingerprint()[:8],
		QuantFP:        "quant",
		DecisionDigest: "dec-" + heads.Fingerprint(),
		InputDigest:    "input",
		DecisionJSON:   []byte(`{"selected_heads":[]}`),
		ExpectedNext:   v,
	}
}

func TestCommitAndRollbackRestore(t *testing.T) {
	s := openTestStore(t)
	ds := generate.Build().Dataset
	setA := model.HeadSet{ds.Heads[0].ID()}
	setB := model.HeadSet{ds.Heads[0].ID(), ds.Heads[1].ID()}

	v1, err := s.CommitVersion(sampleCommit(1, setA))
	if err != nil {
		t.Fatal(err)
	}
	v2, err := s.CommitVersion(sampleCommit(2, setB))
	if err != nil {
		t.Fatal(err)
	}
	if v2.Version != 2 {
		t.Fatalf("version = %d, want 2", v2.Version)
	}

	rolled, err := s.Rollback(v1.Version, 5)
	if err != nil {
		t.Fatal(err)
	}
	if rolled.HeadFP != v1.HeadFP {
		t.Fatal("rollback must restore the exact pruning set fingerprint")
	}
	got, err := s.GetVersion(rolled.Version)
	if err != nil {
		t.Fatal(err)
	}
	if got.HeadFP != setA.Fingerprint() {
		t.Fatal("restored version fingerprint mismatch")
	}
	// Intermediate versions remain queryable but are marked rolled_back.
	old2, err := s.GetVersion(2)
	if err != nil {
		t.Fatal(err)
	}
	if old2.Status != "rolled_back" {
		t.Fatalf("version 2 status = %s, want rolled_back", old2.Status)
	}
	active, err := s.ActiveVersion()
	if err != nil {
		t.Fatal(err)
	}
	if active != rolled.Version {
		t.Fatalf("active version = %d, want %d", active, rolled.Version)
	}
}

func TestStaleAndDuplicateVersion(t *testing.T) {
	s := openTestStore(t)
	ds := generate.Build().Dataset
	set := model.HeadSet{ds.Heads[0].ID()}
	if _, err := s.CommitVersion(sampleCommit(1, set)); err != nil {
		t.Fatal(err)
	}
	// Backwards / skipped version number is rejected.
	_, err := s.CommitVersion(sampleCommit(1, set))
	if err == nil {
		t.Fatal("duplicate version must be rejected")
	}
	be, ok := model.AsBizError(err)
	if !ok || be.Code != model.CodeStaleVersion {
		t.Fatalf("expected E_STALE_VERSION, got %v", err)
	}
	if be.CurrentVer != 1 {
		t.Fatalf("current version payload = %d, want 1", be.CurrentVer)
	}
	// Digest mismatch is also a stale version.
	if err := s.VerifyDigest(1, "wrong-digest"); err == nil {
		t.Fatal("digest mismatch must fail")
	} else {
		be, _ := model.AsBizError(err)
		if be.Code != model.CodeStaleVersion {
			t.Fatalf("expected E_STALE_VERSION, got %s", be.Code)
		}
	}
}

func TestConcurrentCommitsOnlyOneWins(t *testing.T) {
	s := openTestStore(t)
	ds := generate.Build().Dataset
	set := model.HeadSet{ds.Heads[0].ID()}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.CommitVersion(sampleCommit(1, set))
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	success, failures := 0, 0
	for err := range errs {
		if err == nil {
			success++
			continue
		}
		failures++
		be, ok := model.AsBizError(err)
		if !ok || be.Code != model.CodeStaleVersion {
			t.Fatalf("unexpected concurrent error: %v", err)
		}
	}
	if success != 1 || failures != 7 {
		t.Fatalf("success=%d failures=%d, want 1/7", success, failures)
	}
	cur, err := s.CurrentVersion()
	if err != nil {
		t.Fatal(err)
	}
	if cur != 1 {
		t.Fatalf("current version = %d, want 1", cur)
	}
}

func TestNoHalfCommittedRead(t *testing.T) {
	s := openTestStore(t)
	hist, err := s.History()
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 0 {
		t.Fatal("fresh store must expose no versions")
	}
}

func TestRollbackRetentionArchivesOldRows(t *testing.T) {
	s := openTestStore(t)
	ds := generate.Build().Dataset
	for i := int64(1); i <= 6; i++ {
		set := model.HeadSet{ds.Heads[int(i%int64(len(ds.Heads)))].ID()}
		if _, err := s.CommitVersion(sampleCommit(i, set)); err != nil {
			t.Fatal(err)
		}
	}
	rolled, err := s.Rollback(2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if rolled.Version != 7 {
		t.Fatalf("rollback version = %d, want 7", rolled.Version)
	}
	// Rows beyond the 3-generation window stay queryable but are archived.
	v1, err := s.GetVersion(1)
	if err != nil {
		t.Fatal(err)
	}
	if v1.Status != "archived" {
		t.Fatalf("version 1 status = %s, want archived", v1.Status)
	}
	if _, err := s.Rollback(1, 3); err == nil {
		t.Fatal("rollback to an archived version must be rejected")
	} else {
		be, ok := model.AsBizError(err)
		if !ok || be.Code != model.CodeStaleVersion {
			t.Fatalf("expected E_STALE_VERSION, got %v", err)
		}
	}
}
