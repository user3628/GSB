package store

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"ropechunk/internal/model"
)

func testBundle(version int64, base float64) model.Bundle {
	cfg := model.Config{SnapshotVersion: version, BaseFrequency: base, ScalingMode: model.ScaleNone, ScaleFactor: 1,
		AngleBlockSize: 4, CausalMask: true, RowBlockSize: 2, KeyBlockSize: 2, MaxConcurrentBlock: 2,
		WorkerBlockQuota: 4, RoundTimeBudgetMS: 1000, PeakMemoryBytes: 8 << 20}
	shape := model.Shape{1, 1, 2, 4}
	data := []int64{1, 2, 3, 4, 5, 6, 7, 8}
	shard := model.Shard{ID: "s", Shape: shape,
		Q: model.QuantTensor{Shape: shape, Scale: 1, Data: data},
		K: model.QuantTensor{Shape: shape, Scale: 1, Data: append([]int64(nil), data...)}}
	return model.Bundle{FormatVersion: 1, Config: cfg, Shards: []model.Shard{shard},
		Declarations: []model.ShapeDeclaration{{ShardID: "s", Shape: shape}}}
}

func openStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "store.db")
	if err := Clean(path); err != nil {
		t.Fatal(err)
	}
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func checkReq(expected int64) model.PublishRequest {
	return model.PublishRequest{
		Checkpoint: model.Checkpoint{ShardID: "s", Stage: model.StageRotation, Batch: 0, Head: 0, RowBlock: 0,
			RowStart: 0, RowEnd: 2, CoveredKeyStart: 0, CoveredKeyEnd: 2, RunningMax: 0, ExpSum: 1,
			PartialDigest: "digest", InputFingerprint: "input", VersionFingerprint: "version", Complete: false, ConfigVersion: 1},
		AssignedShardID: "s", AssignedRowBlock: 0, ExpectedSequence: expected,
	}
}

func TestCheckpointStaleAndConcurrentPublish(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()
	req := checkReq(1)
	if seq, err := st.PublishCheckpoint(ctx, req); err != nil || seq != 1 {
		t.Fatalf("initial publish seq=%d err=%v", seq, err)
	}
	if _, err := st.PublishCheckpoint(ctx, req); !isStale(err) {
		t.Fatalf("duplicate publish: %v", err)
	}
	gap := checkReq(3)
	if _, err := st.PublishCheckpoint(ctx, gap); !isStale(err) {
		t.Fatalf("gap publish: %v", err)
	}
	crossShard := checkReq(2)
	crossShard.AssignedShardID = "other"
	if _, err := st.PublishCheckpoint(ctx, crossShard); !isStale(err) {
		t.Fatalf("cross-shard publish: %v", err)
	}
	crossRow := checkReq(2)
	crossRow.AssignedRowBlock = 9
	if _, err := st.PublishCheckpoint(ctx, crossRow); !isStale(err) {
		t.Fatalf("cross-row publish: %v", err)
	}
	req2 := checkReq(2)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := st.PublishCheckpoint(ctx, req2)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	success, stale := 0, 0
	for err := range errs {
		if err == nil {
			success++
		} else if isStale(err) {
			stale++
		} else {
			t.Fatalf("unexpected concurrent error: %v", err)
		}
	}
	if success != 1 || stale != 1 {
		t.Fatalf("concurrent publish success=%d stale=%d", success, stale)
	}
	rec, err := st.GetCheckpoint(ctx, "s", model.StageRotation, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Sequence != 2 {
		t.Fatalf("final sequence=%d want 2", rec.Sequence)
	}
}

func TestCheckpointTransactionRollback(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()
	_, err := st.db.ExecContext(ctx, `CREATE TRIGGER fail_checkpoint BEFORE INSERT ON checkpoints
		BEGIN SELECT RAISE(ABORT, 'injected checkpoint failure'); END`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.PublishCheckpoint(ctx, checkReq(1)); err == nil {
		t.Fatal("expected injected insert failure")
	}
	var count int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM checkpoints`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("half-committed checkpoint rows: %d", count)
	}
	if _, err := st.db.ExecContext(ctx, `DROP TRIGGER fail_checkpoint`); err != nil {
		t.Fatal(err)
	}
}

func TestConfigVersionClassification(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()
	next, err := st.SaveBundle(ctx, testBundle(1, 10_000))
	if err != nil || next.Kind != "initial" {
		t.Fatalf("initial transition=%+v err=%v", next, err)
	}
	next, err = st.SaveBundle(ctx, testBundle(1, 10_000))
	if err != nil || next.Kind != "idempotent" {
		t.Fatalf("idempotent transition=%+v err=%v", next, err)
	}
	next, err = st.SaveBundle(ctx, testBundle(1, 20_000))
	if err != nil || next.Kind != "replacement" {
		t.Fatalf("replacement transition=%+v err=%v", next, err)
	}
	next, err = st.SaveBundle(ctx, testBundle(2, 20_000))
	if err != nil || next.Kind != "advance" {
		t.Fatalf("advance transition=%+v err=%v", next, err)
	}
	next, err = st.SaveBundle(ctx, testBundle(1, 10_000))
	if err != nil || next.Kind != "rollback" {
		t.Fatalf("rollback transition=%+v err=%v", next, err)
	}
	events, err := st.ListVersionEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, len(events))
	for i, event := range events {
		got[i] = event.Kind
	}
	want := []string{"initial", "idempotent", "replacement", "advance", "rollback"}
	if len(got) != len(want) {
		t.Fatalf("events=%v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events=%v want=%v", got, want)
		}
	}
}

func isStale(err error) bool {
	app, ok := model.AsAppError(err)
	return ok && app.Code == model.CodeStaleCheckpoint
}
