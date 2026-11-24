package snapshot_test

import (
	"path/filepath"
	"testing"

	"privescworkbench/internal/detector"
	"privescworkbench/internal/domain"
	"privescworkbench/internal/sample"
	"privescworkbench/internal/snapshot"
)

func openTestStore(t *testing.T) *snapshot.Store {
	t.Helper()
	dir := t.TempDir()
	s, err := snapshot.Open(filepath.Join(dir, "snap.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func bundle(t *testing.T, seed int64) *domain.Bundle {
	t.Helper()
	g := sample.Generate(int64(seed), sample.ScenarioBaseline)
	b, err := sample.ParseBundle(g.Bundle.Version, g.ManifestBytes, g.StreamBytes)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCommitVersionsAndReload(t *testing.T) {
	s := openTestStore(t)
	for i := 1; i <= 3; i++ {
		res, err := detector.Run(bundle(t, int64(i)), s, detector.Options{
			SessionID: "s", CommitSnapshot: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if res.Version != int64(i) {
			t.Fatalf("version=%d want %d", res.Version, i)
		}
	}
	last, err := s.VerifyChain()
	if err != nil || last != 3 {
		t.Fatalf("chain last=%d err=%v", last, err)
	}
	round, ledger, err := s.LoadVersion(2)
	if err != nil {
		t.Fatal(err)
	}
	if round.SampleVersion != bundle(t, 2).Version {
		t.Fatal("sample version mismatch on reload")
	}
	if len(round.Conclusions) == 0 || len(ledger) == 0 {
		t.Fatal("reloaded snapshot missing conclusions or ledger")
	}
	if round.SampledAt != domain.FixedSampledAt {
		t.Fatal("time field must use fixed sample mark")
	}
}

func TestWholeReplacementDetection(t *testing.T) {
	s := openTestStore(t)
	r1, err := detector.Run(bundle(t, 1), s, detector.Options{SessionID: "a", CommitSnapshot: true})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := detector.Run(bundle(t, 2), s, detector.Options{SessionID: "b", CommitSnapshot: true})
	if err != nil {
		t.Fatal(err)
	}
	replaced, from, to, err := s.ReplacementDetected()
	if err != nil {
		t.Fatal(err)
	}
	if !replaced || from == to || r1.Version == 0 || r2.Version != 2 {
		t.Fatalf("replacement detection failed: %v %s->%s", replaced, from, to)
	}
}

func TestVersionGapRejectedWithFallback(t *testing.T) {
	s := openTestStore(t)
	for i := 1; i <= 3; i++ {
		if _, err := detector.Run(bundle(t, int64(i)), s, detector.Options{
			SessionID: "s", CommitSnapshot: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.DeleteVersion(2); err != nil {
		t.Fatal(err)
	}
	last, err := s.VerifyChain()
	if err == nil {
		t.Fatal("expected E_SNAPSHOT_VERSION for gap")
	}
	ce, ok := err.(*domain.CodeError)
	if !ok || ce.Code != domain.CodeSnapshotVersion {
		t.Fatalf("wrong error: %v", err)
	}
	if last != 1 {
		t.Fatalf("fallback must stop at last contiguous version, got %d", last)
	}
	// contiguous head still loads
	if _, _, err := s.LoadVersion(1); err != nil {
		t.Fatal(err)
	}
}

func TestProgressMarkerSurvivesForRecovery(t *testing.T) {
	s := openTestStore(t)
	b := bundle(t, 1)
	if err := s.BeginProgress("killed-session", b.Version, snapshot.HashBundle(b)); err != nil {
		t.Fatal(err)
	}
	pending, err := s.PendingProgress()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0] != "killed-session" {
		t.Fatalf("pending=%v", pending)
	}
	if v, _ := s.LatestVersion(); v != 0 {
		t.Fatal("uncommitted round must not advance snapshot version")
	}
	if err := s.ClearProgress("killed-session"); err != nil {
		t.Fatal(err)
	}
	pending, _ = s.PendingProgress()
	if len(pending) != 0 {
		t.Fatal("progress marker not cleared")
	}
}
