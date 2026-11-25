package service_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"flashattention/internal/dataset"
	"flashattention/internal/errs"
	"flashattention/internal/service"
)

func newTestService(t *testing.T) *service.Service {
	t.Helper()
	dir := t.TempDir()
	svc, err := service.NewWithReports(filepath.Join(dir, "data"), filepath.Join(dir, "reports"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { svc.Close() })
	return svc
}

func testSpec(id string) dataset.Spec {
	return dataset.Spec{
		ID: id, B: 1, H: 1, S: 20, D: 7, RowBlock: 4, ColBlock: 3, Group: 1,
		Bits: 24, ExpSumBits: 64, Strategy: "scaled", Seed: 4242,
	}
}

func TestRunIncrementalAndAudit(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	spec := testSpec("inc")
	if _, err := svc.Generate(spec, true); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Run(ctx, "inc", service.RunOptions{}); err != nil {
		t.Fatal(err)
	}

	rep, err := svc.Incremental(ctx, service.UpdateRequest{
		ShardID: "inc", Mode: "correct", Start: 5, End: 9, Seed: 99,
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.ByteIdentical {
		t.Fatalf("not identical: %+v", rep.FirstDifference)
	}
	reused, recomputed, fallback := 0, 0, 0
	for _, rb := range rep.RowBlocks {
		reused += len(rb.ReusedColBlocks)
		recomputed += len(rb.RecomputedColBlocks)
		fallback += len(rb.FallbackColBlocks)
	}
	if recomputed == 0 || reused == 0 || fallback != 0 {
		t.Fatalf("unexpected block reuse recomp=%d reuse=%d fallback=%d", recomputed, reused, fallback)
	}
	if rep.Audit == nil || !rep.Audit.OK {
		t.Fatal("audit did not pass")
	}
}

func TestGracefulStopAndRecoveryMatchesBaseline(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	if _, err := svc.Generate(testSpec("base"), true); err != nil {
		t.Fatal(err)
	}
	base, err := svc.Run(ctx, "base", service.RunOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := svc.Generate(testSpec("rec"), true); err != nil {
		t.Fatal(err)
	}
	stopped, err := svc.Run(ctx, "rec", service.RunOptions{StopAfterCheckpoint: 2})
	if !errors.Is(err, service.ErrGracefulStop) {
		t.Fatalf("expected graceful stop, got %v", err)
	}
	if stopped == nil || stopped.CheckpointsWritten != 2 {
		t.Fatalf("expected two writes, got %+v", stopped)
	}
	resumed, err := svc.Run(ctx, "rec", service.RunOptions{Resume: true})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.OutputHash != base.OutputHash {
		t.Fatalf("recovery hash %s != baseline %s", resumed.OutputHash, base.OutputHash)
	}
}

func TestCorruptCheckpointIsIgnored(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	if _, err := svc.Generate(testSpec("corrupt"), true); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Run(ctx, "corrupt", service.RunOptions{}); err != nil {
		t.Fatal(err)
	}
	res, err := svc.Store.LoadRecovery(ctx, "corrupt")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.States) == 0 {
		t.Fatal("expected recovery states")
	}
	_, err = svc.Store.RawDB().Exec(`UPDATE checkpoints SET acc_blob=X'deadbeef' WHERE id=(SELECT MAX(id) FROM checkpoints)`)
	if err != nil {
		t.Fatal(err)
	}
	res, err = svc.Store.LoadRecovery(ctx, "corrupt")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ignored) == 0 {
		t.Fatal("expected corrupt record to be ignored")
	}
	rep, err := svc.Run(ctx, "corrupt", service.RunOptions{Resume: true})
	if err != nil {
		t.Fatalf("recovery run failed: %v", err)
	}
	if rep.Audit == nil || !rep.Audit.OK {
		t.Fatal("recovered audit failed")
	}
}

func TestErrorBoundCode(t *testing.T) {
	// The audit code path is also exercised through a tampered output in audit_test;
	// this assertion keeps the stable error code visible from service-level tests.
	if errs.EErrorBound != "E_ERROR_BOUND" {
		t.Fatal("bad error code")
	}
}
