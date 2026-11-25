package pipeline

import (
	"path/filepath"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := OpenStore(filepath.Join(t.TempDir(), "recon.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestCheckpointContiguousSequence(t *testing.T) {
	s := newTestStore(t)
	cp := func(seq int64) Checkpoint {
		return Checkpoint{Seq: seq, ShardID: "shard-00", Stage: StageProjection,
			ViewID: "view-00", InputDigest: "in", ConfigVer: "cfg",
			ArtifactHash: "h", ArtifactPath: "p", UpdatedAt: "now"}
	}
	if _, err := s.PublishCheckpoint(cp(1)); err != nil {
		t.Fatalf("first publish failed: %v", err)
	}
	// Gap (seq 3 while expecting 2) must be rejected.
	if _, err := s.PublishCheckpoint(cp(3)); asBiz(err).Code != CodeStaleCheckpoint {
		t.Fatalf("expected stale, got %v", err)
	}
	// Duplicate identical publish is rejected.
	if _, err := s.PublishCheckpoint(cp(1)); asBiz(err).Code != CodeStaleCheckpoint {
		t.Fatalf("expected stale duplicate, got %v", err)
	}
	// Tamper: same seq, different hash.
	tamper := cp(1)
	tamper.ArtifactHash = "changed"
	if _, err := s.PublishCheckpoint(tamper); asBiz(err).Code != CodeCheckpointTampered {
		t.Fatalf("expected tampered, got %v", err)
	}
	// Rollback attempt on another view row.
	other := cp(2)
	other.ViewID = "view-01"
	if _, err := s.PublishCheckpoint(other); err != nil {
		t.Fatal(err)
	}
	rb := cp(1)
	rb.ViewID = "view-01"
	if _, err := s.PublishCheckpoint(rb); asBiz(err).Code != CodeCheckpointRollback {
		t.Fatalf("expected rollback, got %v", err)
	}
	// Normal contiguous advance on a new view row (cursor expects seq 3).
	adv := cp(3)
	adv.ViewID = "view-02"
	if _, err := s.PublishCheckpoint(adv); err != nil {
		t.Fatalf("advance failed: %v", err)
	}
}

func TestCrossShardIndependentCursors(t *testing.T) {
	s := newTestStore(t)
	one := Checkpoint{Seq: 1, ShardID: "shard-00", Stage: StageProjection, ViewID: "v",
		InputDigest: "i", ConfigVer: "c", ArtifactHash: "h", ArtifactPath: "p", UpdatedAt: "n"}
	two := one
	two.ShardID = "shard-01"
	if _, err := s.PublishCheckpoint(one); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PublishCheckpoint(two); err != nil {
		t.Fatalf("independent shard cursor rejected: %v", err)
	}
	// Misaligned publish into shard-00 (expects seq 2, sends 5).
	bad := one
	bad.Seq = 5
	if _, err := s.PublishCheckpoint(bad); asBiz(err).Code != CodeStaleCheckpoint {
		t.Fatalf("expected stale misaligned, got %v", err)
	}
}

func TestFileLockMutualExclusion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ws.lock")
	l1, err := AcquireFileLock(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireFileLock(path); asBiz(err).Code != CodeLocked {
		t.Fatal("second lock should fail")
	}
	if err := l1.Release(); err != nil {
		t.Fatal(err)
	}
	l2, err := AcquireFileLock(path)
	if err != nil {
		t.Fatalf("lock not reacquirable after release: %v", err)
	}
	l2.Release()
}

func TestAtomicWriteNoPartial(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "cp.json")
	if err := AtomicWriteFile(path, []byte("{\"ok\":true}")); err != nil {
		t.Fatal(err)
	}
	// No temp files remain after a successful atomic write.
	ents, _ := filepath.Glob(filepath.Join(dir, "sub", ".tmp-*"))
	if len(ents) != 0 {
		t.Fatalf("leftover temp files: %v", ents)
	}
}
