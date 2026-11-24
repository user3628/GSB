package walgroup

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpointReclaimAndSnapshotRules(t *testing.T) {
	ctx := context.Background()
	e, dir := testEngine(t, Options{SegmentMaxBytes: 360, MaxTxnsPerSegment: 2})
	for i := 0; i < 5; i++ {
		id := "tx" + string(rune('a'+i))
		_, err := e.Commit(ctx, TransactionInput{ID: id, Records: []PageUpdate{{PageNo: 1, Value: "value-" + id}}})
		if err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	segs, err := e.Segments(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) < 3 {
		t.Fatalf("test setup expected rotation, got %+v", segs)
	}
	cp, err := e.AdvanceCheckpoint(ctx, CheckpointRequest{Sequence: 1, ReplayStartLSN: 5})
	if err != nil {
		t.Fatalf("AdvanceCheckpoint: %v", err)
	}
	if len(cp.Pages) != 1 || cp.Pages[0].Version != 5 {
		t.Fatalf("checkpoint pages = %+v", cp.Pages)
	}
	page, err := e.ReadPage(ctx, 1, 5)
	if err != nil || page.Version != 5 || page.Value != "value-txe" {
		t.Fatalf("read at checkpoint: %+v err=%v", page, err)
	}
	reclaimed, err := e.ReclaimSegments(ctx, ReclaimRequest{SegmentNos: []uint64{segs[0].SegmentNo, segs[1].SegmentNo}})
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(reclaimed.Segments) != 2 {
		t.Fatalf("reclaim result: %+v", reclaimed)
	}
	for _, seg := range reclaimed.Segments {
		if seg.State != SegmentReclaimed {
			t.Fatalf("state = %s", seg.State)
		}
		if _, err := os.Stat(filepath.Join(dir, segmentFileName(seg.SegmentNo))); !os.IsNotExist(err) {
			t.Fatalf("segment %d file still exists: %v", seg.SegmentNo, err)
		}
	}
	_, err = e.ReadPage(ctx, 1, 3)
	if CodeOf(err) != CodeSnapshotRetired {
		t.Fatalf("retired code=%q err=%v", CodeOf(err), err)
	}
	er, _ := AsError(err)
	if er.MinServiceableLSN != 5 {
		t.Fatalf("min serviceable = %d", er.MinServiceableLSN)
	}
	_, err = e.ReclaimSegments(ctx, ReclaimRequest{SegmentNos: []uint64{segs[2].SegmentNo}})
	if CodeOf(err) != CodeReclaimUnsafe {
		t.Fatalf("unsafe reclaim code=%q err=%v", CodeOf(err), err)
	}
	er, _ = AsError(err)
	if er.EarliestUncovered != 6 {
		t.Fatalf("expected earliest uncovered 6, got %+v", er)
	}

	// New durable page content is not servable from the old checkpoint.
	res, err := e.Commit(ctx, TransactionInput{ID: "late", Records: []PageUpdate{{PageNo: 1, Value: "late"}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.StartLSN != 6 {
		t.Fatalf("late start LSN = %d", res.StartLSN)
	}
	_, err = e.ReadPage(ctx, 1, 5)
	if CodeOf(err) != CodeCheckpointLag {
		t.Fatalf("checkpoint lag code=%q err=%v", CodeOf(err), err)
	}
	er, _ = AsError(err)
	if er.EarliestUncovered != 6 || er.MinServiceableLSN != 5 {
		t.Fatalf("lag details: %+v", er)
	}
	// Advancing to the newest durable LSN repairs lag.
	if _, err := e.AdvanceCheckpoint(ctx, CheckpointRequest{Sequence: 2, ReplayStartLSN: 6}); err != nil {
		t.Fatalf("checkpoint 2: %v", err)
	}
	page, err = e.ReadPage(ctx, 1, 6)
	if err != nil || page.Version != 6 || page.Value != "late" {
		t.Fatalf("read after checkpoint 2: %+v err=%v", page, err)
	}
	replay, err := e.ReplayFromCheckpoint(ctx)
	if err != nil {
		t.Fatalf("ReplayFromCheckpoint: %v", err)
	}
	if replay.CheckpointSequence != 2 || replay.ReplayStartLSN != 6 || replay.RecordsReplayed != 0 {
		t.Fatalf("replay = %+v", replay)
	}
}

func TestCheckpointValidationAndNoPartialReplace(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t, Options{})
	if _, err := e.AdvanceCheckpoint(ctx, CheckpointRequest{Sequence: 0, ReplayStartLSN: 0}); CodeOf(err) != CodeCheckpointInvalid {
		t.Fatalf("zero sequence code=%q err=%v", CodeOf(err), err)
	}
	if _, err := e.Commit(ctx, TransactionInput{ID: "a", Records: []PageUpdate{{PageNo: 1, Value: "a"}}}); err != nil {
		t.Fatal(err)
	}
	cp1, err := e.AdvanceCheckpoint(ctx, CheckpointRequest{Sequence: 5, ReplayStartLSN: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.AdvanceCheckpoint(ctx, CheckpointRequest{Sequence: 5, ReplayStartLSN: 1}); CodeOf(err) != CodeCheckpointInvalid {
		t.Fatalf("non-monotonic code=%q err=%v", CodeOf(err), err)
	}
	if _, err := e.AdvanceCheckpoint(ctx, CheckpointRequest{Sequence: 6, ReplayStartLSN: 1, ExpectedDigest: "deadbeef"}); CodeOf(err) != CodeCheckpointInvalid {
		t.Fatalf("digest mismatch code=%q err=%v", CodeOf(err), err)
	}
	got, err := e.CurrentCheckpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Sequence != cp1.Sequence || got.Digest != cp1.Digest {
		t.Fatalf("old checkpoint changed: got %+v want %+v", got, cp1)
	}
	if _, err := e.AdvanceCheckpoint(ctx, CheckpointRequest{Sequence: 7, ReplayStartLSN: 0}); CodeOf(err) != CodeCheckpointInvalid {
		t.Fatalf("backward replay start code=%q err=%v", CodeOf(err), err)
	}
}

func TestStateMachineIllegalTransitions(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t, Options{})
	if _, err := e.StageTransaction(ctx, TransactionInput{ID: "staged", Records: []PageUpdate{{PageNo: 1, Value: "x"}}}); err != nil {
		t.Fatal(err)
	}
	_, err := e.TransitionTransaction(ctx, "staged", TxnDurable)
	if CodeOf(err) != CodeInvalidTransition {
		t.Fatalf("txn transition code=%q err=%v", CodeOf(err), err)
	}
	txn, err := e.Transaction(ctx, "staged")
	if err != nil || txn.State != TxnAppending {
		t.Fatalf("illegal transition changed state: %+v err=%v", txn, err)
	}
	if _, err := e.TransitionTransaction(ctx, "staged", TxnAborted); err != nil {
		t.Fatalf("legal abort transition: %v", err)
	}
	aborted, err := e.Transaction(ctx, "staged")
	if err != nil || aborted.State != TxnAborted {
		t.Fatalf("abort transition not persisted: %+v err=%v", aborted, err)
	}
	_, err = e.TransitionSegment(ctx, 1, SegmentReclaimable)
	if CodeOf(err) != CodeInvalidTransition {
		t.Fatalf("segment transition code=%q err=%v", CodeOf(err), err)
	}
	segs, err := e.Segments(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 0 {
		t.Fatalf("illegal segment transition modified catalog: %+v", segs)
	}
}
