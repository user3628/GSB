package snapshot

import (
	"context"
	"path/filepath"
	"testing"

	"secverify/internal/model"
)

func testItems(value float64) []model.Item {
	return []model.Item{{
		ID: "acct.base", Kind: model.KindAccount,
		Account: &model.AccountPolicy{Name: "minimum_password_length", Value: &value, Unit: "count", Objective: "at_least"},
	}}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "engine.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestSnapshotChainAndOldVersionsRetained(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	genesis, err := store.Initialize(ctx, "ns", testItems(12))
	if err != nil {
		t.Fatal(err)
	}
	v1, err := store.AppendRound(ctx, "ns", 0, genesis.ContentHash, "change-1", testItems(14), []byte(`{"round":1}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := store.AppendRound(ctx, "ns", 1, v1.ContentHash, "change-2", testItems(16), []byte(`{"round":2}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	chain, bizErr := store.LoadChain(ctx, "ns")
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	if len(chain.Snapshots) != 3 || chain.Latest.Version != 2 || chain.FallbackVersion != 2 {
		t.Fatalf("unexpected chain: versions=%v latest=%v", chain.Versions, chain.Latest)
	}
	if chain.Snapshots[2].ParentHash != v1.ContentHash || v2.ParentHash != v1.ContentHash {
		t.Fatal("parent hash chain is not preserved")
	}
}

func TestVersionGapReturnsCodeAndFallback(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	genesis, err := store.Initialize(ctx, "ns", testItems(12))
	if err != nil {
		t.Fatal(err)
	}
	v1, err := store.AppendRound(ctx, "ns", 0, genesis.ContentHash, "change-1", testItems(14), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendRound(ctx, "ns", 1, v1.ContentHash, "change-2", testItems(16), nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteSnapshotForInjection(ctx, "ns", 1); err != nil {
		t.Fatal(err)
	}
	chain, bizErr := store.LoadChain(ctx, "ns")
	if bizErr == nil || bizErr.Code != model.CodeSnapshotVersion {
		t.Fatalf("expected E_SNAPSHOT_VERSION, got %+v", bizErr)
	}
	if bizErr.FallbackVersion != 0 || chain.FallbackVersion != 0 || len(chain.Snapshots) != 1 {
		t.Fatalf("unexpected fallback: err=%+v versions=%v", bizErr, chain.Versions)
	}
}

func TestContentHashTamperDetected(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	if _, err := store.Initialize(ctx, "ns", testItems(12)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE snapshots SET content_hash='tampered' WHERE namespace='ns' AND version=0`); err != nil {
		t.Fatal(err)
	}
	_, bizErr := store.LoadChain(ctx, "ns")
	if bizErr == nil || bizErr.Code != model.CodeSnapshotCorrupt {
		t.Fatalf("expected E_SNAPSHOT_CORRUPT, got %+v", bizErr)
	}
}

func TestCompareSnapshotsDetectsSameVersionReplacement(t *testing.T) {
	left := Snapshot{
		Version: 3, ContentHash: "hash-a", Content: []byte("a"),
		Payload: Payload{Items: []model.Item{{ID: "a", Kind: model.KindService, Service: &model.ServicePolicy{Name: "a", StartType: "manual"}}}},
	}
	right := Snapshot{
		Version: 3, ContentHash: "hash-b", Content: []byte("b"),
		Payload: Payload{Items: []model.Item{{ID: "b", Kind: model.KindService, Service: &model.ServicePolicy{Name: "b", StartType: "manual"}}}},
	}
	comparison := CompareSnapshots(left, right)
	if !comparison.SameVersion || comparison.ContentEqual || !comparison.Different || !comparison.WholeReplacement {
		t.Fatalf("unexpected comparison: %+v", comparison)
	}
	same := CompareSnapshots(left, left)
	if !same.ContentEqual || same.Different {
		t.Fatalf("identical snapshots compared unequal: %+v", same)
	}
	lyingHash := right
	lyingHash.ContentHash = left.ContentHash
	comparison = CompareSnapshots(left, lyingHash)
	if comparison.ContentEqual || !comparison.Different || !comparison.WholeReplacement {
		t.Fatalf("content bytes were not authoritative: %+v", comparison)
	}
}

func TestStaleVersionCommitRejected(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	genesis, err := store.Initialize(ctx, "ns", testItems(12))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendRound(ctx, "ns", 0, genesis.ContentHash, "change-1", testItems(14), nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendRound(ctx, "ns", 0, genesis.ContentHash, "stale", testItems(20), nil, nil); err == nil {
		t.Fatal("stale expected version was accepted")
	}
}
