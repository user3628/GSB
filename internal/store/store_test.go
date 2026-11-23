package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"cdn-edge-consistency/internal/cache"
	"cdn-edge-consistency/internal/model"
)

func TestIllegalTransitionDoesNotMutatePersistedState(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	entry := Entry{
		NodeID: "edge-a", CacheKey: "key-1", NormPath: "/x", NormQuery: "", State: cache.StateMiss,
		SnapshotVersion: 1, UpdatedAtMs: 1, ETag: `"sha256-test"`, Body: []byte("x"), BodyLength: 1,
		ContentType: "text/plain", Tags: []string{"tag"}, WrittenAtMs: 1, FreshUntilMs: 100, StaleUntilMs: 200,
	}
	if err := s.Do(ctx, func(ctx context.Context, tx *sql.Tx) error {
		id, err := UpsertEntryTx(ctx, tx, entry)
		if err != nil {
			return err
		}
		entry.ID = id
		if err := TransitionTx(ctx, tx, entry, cache.StateRevalidating, "r1", 2); err != nil {
			return err
		}
		entry.State = cache.StateRevalidating
		if err := TransitionTx(ctx, tx, entry, cache.StateFresh, "r1", 3); err != nil {
			return err
		}
		entry.State = cache.StateFresh
		return ReplaceIndexesTx(ctx, tx, entry, true)
	}); err != nil {
		t.Fatal(err)
	}
	err = s.Do(ctx, func(ctx context.Context, tx *sql.Tx) error {
		current, err := GetEntryTx(ctx, tx, "edge-a", "key-1")
		if err != nil {
			return err
		}
		return TransitionTx(ctx, tx, *current, cache.StateMiss, "bad", 4)
	})
	if err == nil || model.AsBiz(err).Code != model.CodeInvalidTransition {
		t.Fatalf("expected E_INVALID_TRANSITION, got %v", err)
	}
	got, err := s.GetEntry(ctx, "edge-a", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != cache.StateFresh {
		t.Fatalf("illegal transition mutated state to %s", got.State)
	}
	var transitions int
	if err := s.Reader().QueryRow(`SELECT COUNT(*) FROM entry_transitions`).Scan(&transitions); err != nil {
		t.Fatal(err)
	}
	if transitions != 2 {
		t.Fatalf("illegal transition wrote audit row; transitions=%d", transitions)
	}
}

func TestRequestClaimIsSingleWriterIdempotent(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "claim.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	rec := RequestRecord{RequestID: "r-1", Method: "GET", NodeID: "edge-a", SnapshotVersion: 1, CreatedAtMs: 1}
	var first, second bool
	if err := s.Do(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		first, err = InsertRequestClaimTx(ctx, tx, rec)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Do(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		second, err = InsertRequestClaimTx(ctx, tx, rec)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !first || second {
		t.Fatalf("claim flags first=%v second=%v", first, second)
	}
}
