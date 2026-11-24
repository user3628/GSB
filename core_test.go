package walgroup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func testEngine(t *testing.T, opts Options) (*Engine, string) {
	t.Helper()
	dir := t.TempDir()
	e, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e, dir
}

func TestCommitGroupOffsetsIdempotencyAndConflict(t *testing.T) {
	ctx := context.Background()
	e, dir := testEngine(t, Options{SegmentMaxBytes: 4096, MaxTxnsPerSegment: 10})
	inputs := []TransactionInput{
		{ID: "t1", Records: []PageUpdate{{PageNo: 1, Value: "alpha"}}},
		{ID: "t2", Records: []PageUpdate{{PageNo: 2, Value: "beta"}, {PageNo: 1, Value: "alpha-2"}}},
	}
	res, err := e.CommitGroup(ctx, inputs)
	if err != nil {
		t.Fatalf("CommitGroup: %v", err)
	}
	if res.FsyncCount != 1 || res.StartLSN != 1 || res.EndLSN != 3 {
		t.Fatalf("unexpected group result: %+v", res)
	}
	if len(res.Transactions) != 2 || res.Transactions[0].StartOffset == res.Transactions[0].EndOffset {
		t.Fatalf("missing per-txn offsets: %+v", res.Transactions)
	}
	if res.Transactions[0].EndOffset <= res.Transactions[0].StartOffset || res.Transactions[1].StartOffset != res.Transactions[0].EndOffset {
		t.Fatalf("non-contiguous offsets: %+v", res.Transactions)
	}
	dup, err := e.CommitGroup(ctx, inputs)
	if err != nil {
		t.Fatalf("idempotent CommitGroup: %v", err)
	}
	if dup.FsyncCount != 0 || !dup.Transactions[0].Duplicate || !dup.Transactions[1].Duplicate {
		t.Fatalf("duplicate group not detected: %+v", dup)
	}
	_, err = e.Commit(ctx, TransactionInput{ID: "t1", Records: []PageUpdate{{PageNo: 1, Value: "different"}}})
	if CodeOf(err) != CodeConflictingTxn {
		t.Fatalf("conflict code = %q, err=%v", CodeOf(err), err)
	}
	seg, err := ParseSegment(filepath.Join(dir, segmentFileName(1)), 1, 1)
	if err != nil {
		t.Fatalf("ParseSegment: %v", err)
	}
	if len(seg.Records) != 3 || seg.Records[0].LSN != 1 || seg.Records[2].LSN != 3 {
		t.Fatalf("unexpected segment records: %+v", seg.Records)
	}
}

func TestGroupWriteFailureRollsBack(t *testing.T) {
	ctx := context.Background()
	faults := 0
	e, dir := testEngine(t, Options{
		SegmentMaxBytes: 4096, MaxTxnsPerSegment: 10,
		Fault: func(p FaultPoint) error {
			if p.Point == FaultBeforeGroupWrite && faults == 0 {
				faults++
				return errors.New("injected write failure")
			}
			return nil
		},
	})
	_, err := e.CommitGroup(ctx, []TransactionInput{
		{ID: "bad1", Records: []PageUpdate{{PageNo: 1, Value: "one"}}},
		{ID: "bad2", Records: []PageUpdate{{PageNo: 2, Value: "two"}}},
	})
	if CodeOf(err) != CodeGroupCommitFailed {
		t.Fatalf("group failure code=%q err=%v", CodeOf(err), err)
	}
	for _, id := range []string{"bad1", "bad2"} {
		txn, err := e.Transaction(ctx, id)
		if err != nil || txn.State != TxnAborted {
			t.Fatalf("txn %s state=%+v err=%v", id, txn, err)
		}
	}
	segs, err := e.Segments(ctx)
	if err != nil || len(segs) != 1 || segs[0].RecordCount != 0 || segs[0].TxnCount != 0 {
		t.Fatalf("segment was not rolled back: %+v err=%v", segs, err)
	}
	parsed, err := ParseSegment(filepath.Join(dir, segmentFileName(1)), 1, 1)
	if err != nil || len(parsed.Records) != 0 {
		t.Fatalf("physical rollback failed: %+v err=%v", parsed, err)
	}
	res, err := e.CommitGroup(ctx, []TransactionInput{{ID: "good", Records: []PageUpdate{{PageNo: 3, Value: "ok"}}}})
	if err != nil || res.StartLSN != 1 || res.EndLSN != 1 {
		t.Fatalf("post-rollback commit failed: %+v err=%v", res, err)
	}
}

func TestRotationAndLSNContinuity(t *testing.T) {
	ctx := context.Background()
	e, dir := testEngine(t, Options{SegmentMaxBytes: 420, MaxTxnsPerSegment: 2})
	var last uint64
	for i := 0; i < 7; i++ {
		in := TransactionInput{ID: "r" + string(rune('a'+i)), Records: []PageUpdate{{PageNo: int64(i), Value: "012345678901234567890123456789"}}}
		res, err := e.Commit(ctx, in)
		if err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
		if i > 0 && res.StartLSN != last+1 {
			t.Fatalf("LSN gap at %d: start=%d previous=%d", i, res.StartLSN, last)
		}
		last = res.EndLSN
	}
	segs, err := e.Segments(ctx)
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if len(segs) < 3 {
		t.Fatalf("expected rotation, got %+v", segs)
	}
	var prev uint64
	for _, seg := range segs {
		p, err := ParseSegment(filepath.Join(dir, segmentFileName(seg.SegmentNo)), seg.SegmentNo, seg.StartLSN)
		if err != nil {
			t.Fatalf("parse seg %d: %v", seg.SegmentNo, err)
		}
		for _, rec := range p.Records {
			if prev != 0 && rec.LSN != prev+1 {
				t.Fatalf("record LSN gap %d -> %d", prev, rec.LSN)
			}
			prev = rec.LSN
		}
	}
}

func TestSegmentCorruptionHasCodeAndExpectedLSN(t *testing.T) {
	ctx := context.Background()
	e, dir := testEngine(t, Options{})
	if _, err := e.Commit(ctx, TransactionInput{ID: "c1", Records: []PageUpdate{{PageNo: 1, Value: "x"}}}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, segmentFileName(1))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[int(segmentHeaderSize)+10] ^= 0xff
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = ParseSegment(path, 1, 1)
	er, ok := AsError(err)
	if !ok || er.Code != CodeSegmentCorrupt || er.SegmentNo != 1 || er.ExpectedStartLSN != 1 {
		t.Fatalf("unexpected segment error: %#v", err)
	}
}

func TestStagedGroupCommitIsIdempotent(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t, Options{})
	for _, id := range []string{"st1", "st2"} {
		if _, err := e.StageTransaction(ctx, TransactionInput{ID: id, Records: []PageUpdate{{PageNo: 1, Value: id}}}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := e.CommitStagedGroup(ctx, []string{"st1", "st2"})
	if err != nil || first.FsyncCount != 1 || len(first.Transactions) != 2 {
		t.Fatalf("first staged commit: %+v err=%v", first, err)
	}
	second, err := e.CommitStagedGroup(ctx, []string{"st1", "st2"})
	if err != nil || second.FsyncCount != 0 {
		t.Fatalf("second staged commit: %+v err=%v", second, err)
	}
	for _, r := range second.Transactions {
		if !r.Duplicate || r.State != TxnDurable {
			t.Fatalf("not idempotent: %+v", r)
		}
	}
}
