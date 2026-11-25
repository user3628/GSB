package store

import (
	"testing"

	"blockinspect/internal/manifest"
)

func openMem(t *testing.T) *Store {
	t.Helper()
	st, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestIntentCommitRecover(t *testing.T) {
	st := openMem(t)
	leaves := []manifest.Digest{manifest.LeafDigest([]byte("a")), manifest.LeafDigest([]byte("b"))}
	tree := manifest.BuildTree(leaves)

	// a pending intent that never commits simulates a crash
	if err := st.WriteIntent("intent-crash", "blk-1", "verify-full", map[string]any{"x": 1}); err != nil {
		t.Fatal(err)
	}
	// a completed intent + committed checkpoint
	if err := st.WriteIntent("intent-ok", "blk-1", "verify-full", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RegisterRequest("req-1", "blk-1", "hash"); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitCheckpoint("intent-ok", "blk-1", 0, tree, []byte(`{"q":true}`), "req-1", []byte(`{"ok":1}`)); err != nil {
		t.Fatal(err)
	}

	rep, err := st.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if rep.NewlyAborted != 1 || len(rep.AbortedIntents) != 1 ||
		rep.AbortedIntents[0].ID != "intent-crash" {
		t.Fatalf("want intent-crash aborted, got %+v", rep)
	}
	if len(rep.LastCheckpoints) != 1 || rep.LastCheckpoints[0].CheckpointID != 0 {
		t.Fatalf("want checkpoint 0 intact, got %+v", rep.LastCheckpoints)
	}
	// tree round-trip
	loaded, err := st.LoadTree("blk-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Root() != tree.Root() {
		t.Fatal("loaded tree root differs from committed root")
	}
	// recovery is idempotent: the second run aborts nothing new but still
	// reports the persisted aborted intent
	rep2, err := st.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if rep2.NewlyAborted != 0 || len(rep2.AbortedIntents) != 1 {
		t.Fatalf("second recovery must abort nothing new: %+v", rep2)
	}
}

func TestRegisterRequestIdempotent(t *testing.T) {
	st := openMem(t)
	ok, err := st.RegisterRequest("r1", "b", "h1")
	if err != nil || !ok {
		t.Fatalf("first register must succeed: ok=%v err=%v", ok, err)
	}
	ok, err = st.RegisterRequest("r1", "b", "h1")
	if err != nil || ok {
		t.Fatalf("duplicate register must report false: ok=%v err=%v", ok, err)
	}
}
