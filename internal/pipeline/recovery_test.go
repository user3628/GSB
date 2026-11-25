package pipeline

import (
	"context"
	"errors"
	"testing"

	"imretrace/internal/model"
)

func newTestService(t *testing.T) *Service {
	t.Helper()
	svc, err := NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

func TestForcedStopRecoveryMatchesContinuousRun(t *testing.T) {
	ctx := context.Background()

	stopped := newTestService(t)
	if _, err := stopped.EnsureDemo(ctx); err != nil {
		t.Fatal(err)
	}
	_, err := stopped.Build(ctx, BuildOptions{Hooks: Hooks{AfterPublish: func(seq int, _, _ string) error {
		if seq == 2 {
			return ErrStopAfterCommit
		}
		return nil
	}}})
	if !errors.Is(err, ErrStopAfterCommit) {
		t.Fatalf("expected stop after commit, got %v", err)
	}
	stopRoot := stopped.Paths.Root
	if err := stopped.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := NewService(stopRoot)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	if _, err := recovered.Build(ctx, BuildOptions{}); err != nil {
		t.Fatal(err)
	}
	recoveryHash, _, err := recovered.StateHash(ctx)
	if err != nil {
		t.Fatal(err)
	}
	recoveryResult, err := recovered.SearchText(ctx, "red circle 红色圆形 blue gradient", 5)
	if err != nil {
		t.Fatal(err)
	}

	continuous := newTestService(t)
	if _, err := continuous.Build(ctx, BuildOptions{}); err != nil {
		t.Fatal(err)
	}
	continuousHash, _, err := continuous.StateHash(ctx)
	if err != nil {
		t.Fatal(err)
	}
	continuousResult, err := continuous.SearchText(ctx, "red circle 红色圆形 blue gradient", 5)
	if err != nil {
		t.Fatal(err)
	}
	if recoveryHash != continuousHash {
		t.Fatalf("state hash mismatch\nrecovery=%s\ncontinuous=%s", recoveryHash, continuousHash)
	}
	rb := model.MustCanonical(recoveryResult)
	cb := model.MustCanonical(continuousResult)
	if string(rb) != string(cb) {
		t.Fatal("search result bytes differ after recovery")
	}
}

func TestFullReplacementRebuildAndRollbackRejected(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	if _, err := svc.Build(ctx, BuildOptions{}); err != nil {
		t.Fatal(err)
	}
	current, err := svc.Store.CurrentPolicy(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next := *current
	next.Version++
	next.Kind = "full"
	next.Grid = 4
	next.ContentHash = ""
	next.Refresh()
	if _, _, err := svc.ReplacePolicy(ctx, next); err != nil {
		t.Fatal(err)
	}
	rep, err := svc.Build(ctx, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Policy.Version != 2 || rep.Policy.Grid != 4 {
		t.Fatalf("unexpected rebuilt policy: %+v", rep.Policy)
	}
	checkpoints, err := svc.Store.ActiveCheckpoints(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(checkpoints) != 9*4 {
		t.Fatalf("active checkpoints=%d want 36", len(checkpoints))
	}

	rollback := next
	rollback.Version = 1
	_, _, err = svc.ReplacePolicy(ctx, rollback)
	biz, ok := model.AsBiz(err)
	if !ok || biz.Code != model.CodeStaleVersion {
		t.Fatalf("expected E_STALE_VERSION, got %v", err)
	}
	if biz.CurrentVersion != 2 || biz.MinAcceptableVersion != 2 {
		t.Fatalf("unexpected stale version details: %+v", biz)
	}
}

func TestQueryInvalidRetainsPartialComponents(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	if _, err := svc.Build(ctx, BuildOptions{}); err != nil {
		t.Fatal(err)
	}
	_, err := svc.SearchText(ctx, "   ", 3)
	if biz, ok := model.AsBiz(err); !ok || biz.Code != model.CodeQueryInvalid || biz.Partial == nil {
		t.Fatalf("empty query error=%#v", err)
	}

	res, err := svc.SearchText(ctx, "red", 99)
	biz, ok := model.AsBiz(err)
	if !ok || biz.Code != model.CodeQueryInvalid {
		t.Fatalf("k-too-large error=%#v", err)
	}
	if biz.Partial == nil || res == nil || len(res.Candidates) != 9 {
		t.Fatalf("expected all computed partial candidates, res=%+v biz=%+v", res, biz)
	}

	version := 1
	zero := model.Weights{Cosine: 0, Term: 0}
	if _, _, err := svc.ApplyPatch(ctx, model.Patch{BaseVersion: &version, FusionWeights: &zero}); err != nil {
		t.Fatal(err)
	}
	res, err = svc.SearchText(ctx, "red circle", 3)
	biz, ok = model.AsBiz(err)
	if !ok || biz.Code != model.CodeQueryInvalid {
		t.Fatalf("zero weights error=%#v", err)
	}
	if res == nil || len(res.Candidates) == 0 || len(res.PartialComponents) == 0 {
		t.Fatal("expected computed components retained for zero-weight query")
	}
}

func TestNumericMismatchWithStrictTolerance(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	if _, err := svc.Build(ctx, BuildOptions{}); err != nil {
		t.Fatal(err)
	}
	current, _ := svc.Store.CurrentPolicy(ctx)
	strict := *current
	strict.Version++
	strict.Kind = "full"
	strict.NumericAbsTolerance = 1e-9
	strict.NumericRelTolerance = 1e-9
	strict.ContentHash = ""
	strict.Refresh()
	if _, _, err := svc.ReplacePolicy(ctx, strict); err != nil {
		t.Fatal(err)
	}
	_, err := svc.SearchText(ctx, "red circle", 3)
	biz, ok := model.AsBiz(err)
	if !ok || biz.Code != model.CodeNumericMismatch || biz.ItemID == "" || biz.MaxAbsDiff == nil || biz.MaxRelDiff == nil {
		t.Fatalf("expected E_NUMERIC_MISMATCH with diff fields, got %#v", err)
	}
}
