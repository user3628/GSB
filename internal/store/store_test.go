package store

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"example.com/ntfsacl/internal/domain"
)

type testSnapshot struct {
	TreeHash string `json:"tree_hash"`
	Nodes    []struct {
		RelPath string `json:"rel_path"`
		ACLHash string `json:"acl_hash"`
	} `json:"nodes"`
}

func payload(t *testing.T, tree string, nodes map[string]string) []byte {
	t.Helper()
	var p testSnapshot
	p.TreeHash = tree
	for path, hash := range nodes {
		p.Nodes = append(p.Nodes, struct {
			RelPath string `json:"rel_path"`
			ACLHash string `json:"acl_hash"`
		}{path, hash})
	}
	// Canonical JSON sorts map-derived nodes only after the caller sorts them.
	// Tests intentionally use one or two nodes and normalize via sorting below.
	b, err := domain.CanonicalJSON(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestSnapshotReplayExactAndVersionGapFallback(t *testing.T) {
	db, err := Open(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	p1 := payload(t, "tree-1", map[string]string{"a": "1"})
	p2 := payload(t, "tree-2", map[string]string{"a": "2"})
	p3 := payload(t, "tree-3", map[string]string{"a": "3"})
	var snaps []Snapshot
	for i, p := range [][]byte{p1, p2, p3} {
		data := p
		snap, err := db.CommitSnapshot(ctx, KindAudit, func(v, pv int64, ph string) ([]byte, error) { return data, nil })
		if err != nil {
			t.Fatal(err)
		}
		if snap.Version != int64(i+1) {
			t.Fatalf("version=%d", snap.Version)
		}
		snaps = append(snaps, snap)
	}
	replay, err := db.Replay(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(replay.Payload, p2) {
		t.Fatal("replay did not return exact committed bytes")
	}
	// Delete the middle version to force a chain gap. Recovery must return the
	// healthy prefix (v1), and its bytes must equal the original v1 payload.
	if _, err := db.db.ExecContext(ctx, `DELETE FROM snapshots WHERE version=2`); err != nil {
		t.Fatal(err)
	}
	fallback, err := db.Replay(ctx, 3)
	var sv *SnapshotVersionError
	if err == nil || !errors.As(err, &sv) {
		t.Fatalf("want E_SNAPSHOT_VERSION, got %v", err)
	}
	if sv.Code != CodeSnapshotVersion || sv.Fallback != 1 {
		t.Fatalf("unexpected fallback error: %+v", sv)
	}
	if !bytes.Equal(fallback.Payload, p1) {
		t.Fatal("fallback payload differs from committed v1 bytes")
	}
}

func TestWholeReplacementClassification(t *testing.T) {
	db, err := Open(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	first := payload(t, "tree-a", map[string]string{"a": "1", "b": "2"})
	second := payload(t, "tree-b", map[string]string{"x": "8", "y": "9"})
	for _, b := range [][]byte{first, second} {
		data := b
		if _, err := db.CommitSnapshot(ctx, KindAudit, func(v, pv int64, ph string) ([]byte, error) { return data, nil }); err != nil {
			t.Fatal(err)
		}
	}
	chain, err := db.SnapshotChain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) != 2 || chain[1].ChangeKind != "whole_replace" {
		t.Fatalf("whole replacement not identified: %+v", chain)
	}
}
