// Package recovery holds integration tests for hard-kill consistency.
// It is a test-only package; production recovery logic lives in snapshot.
package recovery_test

import (
	"path/filepath"
	"testing"

	"privescworkbench/internal/detector"
	"privescworkbench/internal/domain"
	"privescworkbench/internal/sample"
	"privescworkbench/internal/snapshot"
)

func bundle(t *testing.T, seed int64) *domain.Bundle {
	t.Helper()
	g := sample.Generate(int64(seed), sample.ScenarioBaseline)
	b, err := sample.ParseBundle(g.Bundle.Version, g.ManifestBytes, g.StreamBytes)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestHardKillDuringCommitKeepsOnlyCommittedVersion(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "recovery.db")

	// phase 1: one committed snapshot, then a progress marker simulating
	// a process killed mid-round (transaction never committed).
	store, err := snapshot.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	res, err := detector.Run(bundle(t, 1), store, detector.Options{
		SessionID: "phase1", CommitSnapshot: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Version != 1 {
		t.Fatalf("version=%d", res.Version)
	}
	b2 := bundle(t, 2)
	if err := store.BeginProgress("killed", b2.Version, snapshot.HashBundle(b2)); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// phase 2: reopen as a fresh process would after the hard kill
	reopened, err := snapshot.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	head, err := reopened.LatestVersion()
	if err != nil {
		t.Fatal(err)
	}
	if head != 1 {
		t.Fatalf("uncommitted transaction must be discarded, head=%d", head)
	}
	last, err := reopened.VerifyChain()
	if err != nil || last != 1 {
		t.Fatalf("committed chain must survive restart: last=%d err=%v", last, err)
	}
	pending, err := reopened.PendingProgress()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0] != "killed" {
		t.Fatalf("resumable marker expected, got %v", pending)
	}

	// phase 3: the killed batch is safely re-detectable and becomes v2
	res2, err := detector.Run(b2, reopened, detector.Options{
		SessionID: "phase3", CommitSnapshot: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res2.Version != 2 {
		t.Fatalf("re-detected batch version=%d", res2.Version)
	}
	last, err = reopened.VerifyChain()
	if err != nil || last != 2 {
		t.Fatalf("chain after redetect: last=%d err=%v", last, err)
	}
	round1, _, err := reopened.LoadVersion(1)
	if err != nil {
		t.Fatal(err)
	}
	if round1.SampleHash == "" {
		t.Fatal("committed conclusions must not roll back")
	}
}

func TestGapAfterRecoveryIsRejected(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "gap.db")
	store, err := snapshot.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 2; i++ {
		if _, err := detector.Run(bundle(t, int64(i)), store, detector.Options{
			SessionID: "s", CommitSnapshot: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.DeleteVersion(1); err != nil {
		t.Fatal(err)
	}
	store.Close()

	reopened, err := snapshot.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	_, err = reopened.VerifyChain()
	ce, ok := err.(*domain.CodeError)
	if !ok || ce.Code != domain.CodeSnapshotVersion {
		t.Fatalf("expected E_SNAPSHOT_VERSION after restart, got %v", err)
	}
}
