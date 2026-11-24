package service

import (
	"context"
	"path/filepath"
	"testing"

	"keylife/internal/model"
	"keylife/internal/store"
)

func setup(t *testing.T) (*store.Store, *model.VirtualClock, *Service, context.Context) {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.UpsertSubject(ctx, model.SubjectPolicy{
		Subject: "alpha", MinStrengthBits: 256, RotationInterval: model.D(100),
		DefaultLifetime: model.D(1000), MaxPendingRequests: 8,
	}); err != nil {
		t.Fatal(err)
	}
	clock := model.NewVirtualClock(100)
	return st, clock, New(st, clock, Config{MaxConcurrency: 4, DefaultSubjectLimit: 8}, Hooks{}), ctx
}

func TestLifecycleAndSnapshotReplay(t *testing.T) {
	st, clock, svc, ctx := setup(t)
	generated, err := svc.Generate(ctx, GenerateRequest{KeyID: "k1", Actor: "alpha", Subject: "alpha", Algorithm: "aes-256-gcm"})
	if err != nil {
		t.Fatal(err)
	}
	if generated.Key.Phase != model.PhaseGenerated || generated.SnapshotVersion != 1 {
		t.Fatalf("generate result: %+v", generated)
	}
	clock.Set(110)
	activated, err := svc.Activate(ctx, TransitionRequest{Actor: "alpha", KeyID: "k1"})
	if err != nil {
		t.Fatal(err)
	}
	if activated.Key.Phase != model.PhaseActive || activated.SnapshotVersion != 2 {
		t.Fatalf("activate result: %+v", activated)
	}
	clock.Set(120)
	rotated, err := svc.Rotate(ctx, TransitionRequest{Actor: "alpha", KeyID: "k1"})
	if err != nil {
		t.Fatal(err)
	}
	if rotated.Key.Phase != model.PhaseRotated || rotated.Successor == nil || rotated.SnapshotVersion != 3 {
		t.Fatalf("rotate result: %+v", rotated)
	}
	successorID := rotated.Successor.ID
	clock.Set(130)
	if _, err := svc.Activate(ctx, TransitionRequest{Actor: "alpha", KeyID: successorID}); err != nil {
		t.Fatal(err)
	}
	clock.Set(140)
	if _, err := svc.Archive(ctx, TransitionRequest{Actor: "alpha", KeyID: "k1"}); err != nil {
		t.Fatal(err)
	}
	clock.Set(150)
	destroyed, err := svc.Destroy(ctx, TransitionRequest{Actor: "alpha", KeyID: "k1"})
	if err != nil {
		t.Fatal(err)
	}
	if destroyed.Key.Phase != model.PhaseDestroyed || destroyed.SnapshotVersion != 6 {
		t.Fatalf("destroy result: %+v", destroyed)
	}
	chain, err := st.LoadChain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if chain.RecoveredThrough != 6 || len(chain.Entries) != 6 {
		t.Fatalf("chain: %+v", chain)
	}
	replay, err := st.GetSnapshot(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, key := range replay.State.Keys {
		if key.ID == "k1" {
			found = key.Phase == model.PhaseActive
		}
	}
	if !found {
		t.Fatal("snapshot v2 did not replay the active phase")
	}
}

func TestBusinessErrorsDoNotMutateSnapshots(t *testing.T) {
	st, clock, svc, ctx := setup(t)
	if _, err := svc.Generate(ctx, GenerateRequest{KeyID: "k1", Actor: "alpha", Subject: "alpha", Algorithm: "aes-256-gcm", Lifetime: model.D(50)}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Activate(ctx, TransitionRequest{Actor: "alpha", KeyID: "k1"}); err != nil {
		t.Fatal(err)
	}
	before, _ := st.LoadChain(ctx)
	keyBefore, _ := st.GetKey(ctx, "k1")

	if _, err := svc.Archive(ctx, TransitionRequest{Actor: "alpha", KeyID: "k1"}); model.CodeOf(err) != model.CodePhaseTransition {
		t.Fatalf("archive active code=%s", model.CodeOf(err))
	}
	if _, err := svc.Rotate(ctx, TransitionRequest{Actor: "beta", KeyID: "k1"}); model.CodeOf(err) != model.CodeSubjectScope {
		t.Fatalf("cross-subject code=%s", model.CodeOf(err))
	}
	if _, err := svc.Generate(ctx, GenerateRequest{KeyID: "weak", Actor: "alpha", Subject: "alpha", Algorithm: "aes-128-gcm"}); model.CodeOf(err) != model.CodeWeakAlgo {
		t.Fatalf("weak code=%s", model.CodeOf(err))
	}
	clock.Set(500)
	decision := svc.Evaluate(ctx, model.DecisionRequest{Subject: "alpha", KeyID: "k1", Actor: "alpha", Operation: "use"})
	if decision.Code != model.CodeKeyExpired {
		t.Fatalf("expired decision=%s", decision.Code)
	}
	after, _ := st.LoadChain(ctx)
	keyAfter, _ := st.GetKey(ctx, "k1")
	if before.RecoveredThrough != after.RecoveredThrough || keyBefore.Phase != keyAfter.Phase {
		t.Fatalf("failed operations changed state: version %d->%d phase %s->%s", before.RecoveredThrough, after.RecoveredThrough, keyBefore.Phase, keyAfter.Phase)
	}
}

func TestRotationContentionOnlyOneSuccess(t *testing.T) {
	st, clock, _, ctx := setup(t)
	if _, err := svcGenerateActivate(t, st, clock, ctx, "k1"); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	hooked := New(st, clock, Config{MaxConcurrency: 4, DefaultSubjectLimit: 8}, Hooks{BeforeCommit: func(operation, key string) {
		if operation == "rotate" {
			close(entered)
			<-release
		}
	}})
	type result struct{ err error }
	first := make(chan result, 1)
	go func() {
		_, err := hooked.Rotate(ctx, TransitionRequest{Actor: "alpha", KeyID: "k1"})
		first <- result{err}
	}()
	<-entered
	contenders := make(chan error, 7)
	for i := 0; i < 7; i++ {
		go func() {
			_, err := hooked.Rotate(ctx, TransitionRequest{Actor: "alpha", KeyID: "k1"})
			contenders <- err
		}()
	}
	for i := 0; i < 7; i++ {
		if code := model.CodeOf(<-contenders); code != model.CodeRotationContention {
			t.Fatalf("contender code=%s", code)
		}
	}
	close(release)
	if got := <-first; got.err != nil {
		t.Fatalf("winner failed: %v", got.err)
	}
	chain, err := st.LoadChain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if chain.RecoveredThrough != 3 {
		t.Fatalf("unexpected version after rotation: %d", chain.RecoveredThrough)
	}
}

func svcGenerateActivate(t *testing.T, st *store.Store, clock *model.VirtualClock, ctx context.Context, id string) (struct{}, error) {
	t.Helper()
	svc := New(st, clock, Config{MaxConcurrency: 4, DefaultSubjectLimit: 8}, Hooks{})
	if _, err := svc.Generate(ctx, GenerateRequest{KeyID: id, Actor: "alpha", Subject: "alpha", Algorithm: "aes-256-gcm"}); err != nil {
		return struct{}{}, err
	}
	if _, err := svc.Activate(ctx, TransitionRequest{Actor: "alpha", KeyID: id}); err != nil {
		return struct{}{}, err
	}
	return struct{}{}, nil
}
