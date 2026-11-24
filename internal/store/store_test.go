package store_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"keylife/internal/model"
	"keylife/internal/service"
	"keylife/internal/store"
)

func seededStore(t *testing.T, count int) (*store.Store, context.Context) {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.UpsertSubject(ctx, model.SubjectPolicy{Subject: "alpha", MinStrengthBits: 128, RotationInterval: model.D(100), DefaultLifetime: model.D(1000), MaxPendingRequests: 8}); err != nil {
		t.Fatal(err)
	}
	clock := model.NewVirtualClock(100)
	svc := service.New(st, clock, service.Config{MaxConcurrency: 2, DefaultSubjectLimit: 8}, service.Hooks{})
	for i := 0; i < count; i++ {
		id := "k" + string(rune('0'+i))
		if _, err := svc.Generate(ctx, service.GenerateRequest{KeyID: id, Actor: "alpha", Subject: "alpha", Algorithm: "aes-128-gcm"}); err != nil {
			t.Fatal(err)
		}
		clock.Set(int64(101 + i))
	}
	return st, ctx
}

func TestSnapshotGapRejectedAndRecovered(t *testing.T) {
	st, ctx := seededStore(t, 3)
	if err := st.InjectSnapshotGap(ctx, 3, 5, 200); err != nil {
		t.Fatal(err)
	}
	chain, err := st.LoadChain(ctx)
	if model.CodeOf(err) != model.CodeSnapshotVersion {
		t.Fatalf("code=%s err=%v", model.CodeOf(err), err)
	}
	if chain.RecoveredThrough != 3 || len(chain.Entries) != 3 || chain.PhysicalMaxVersion != 5 {
		t.Fatalf("unexpected chain fallback: %+v", chain)
	}
}

func TestSnapshotReplacementDetected(t *testing.T) {
	st, ctx := seededStore(t, 3)
	entry, err := st.GetSnapshot(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	replacement := *entry.State
	replacement.Schema = "keylife.snapshot/replaced"
	payload, _ := json.Marshal(replacement)
	if err := st.InjectSnapshotReplacement(ctx, 3, payload); err != nil {
		t.Fatal(err)
	}
	chain, err := st.LoadChain(ctx)
	if model.CodeOf(err) != model.CodeSnapshotReplaced {
		t.Fatalf("code=%s err=%v", model.CodeOf(err), err)
	}
	if chain.RecoveredThrough != 2 {
		t.Fatalf("recovered through %d, want 2", chain.RecoveredThrough)
	}
}

func TestWriteRejectsInvalidChain(t *testing.T) {
	st, ctx := seededStore(t, 3)
	if err := st.InjectSnapshotGap(ctx, 3, 5, 200); err != nil {
		t.Fatal(err)
	}
	err := st.Write(ctx, func(tx *store.Tx) error { return nil })
	if model.CodeOf(err) != model.CodeSnapshotVersion {
		t.Fatalf("write code=%s err=%v", model.CodeOf(err), err)
	}
}
