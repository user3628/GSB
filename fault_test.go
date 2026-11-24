package walgroup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRotationFailureKeepsOriginalOpen(t *testing.T) {
	ctx := context.Background()
	rotations := 0
	e, _ := testEngine(t, Options{
		SegmentMaxBytes: 360, MaxTxnsPerSegment: 2,
		Fault: func(p FaultPoint) error {
			if p.Point == FaultBeforeRotate && p.SegmentNo != 0 && rotations == 0 {
				rotations++
				return errors.New("injected rotation failure")
			}
			return nil
		},
	})
	if _, err := e.Commit(ctx, TransactionInput{ID: "a", Records: []PageUpdate{{PageNo: 1, Value: "a"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Commit(ctx, TransactionInput{ID: "b", Records: []PageUpdate{{PageNo: 2, Value: "b"}}}); err != nil {
		t.Fatal(err)
	}
	_, err := e.Commit(ctx, TransactionInput{ID: "c", Records: []PageUpdate{{PageNo: 3, Value: "c"}}})
	if CodeOf(err) != CodeRotateFailed {
		t.Fatalf("rotation error code=%q err=%v", CodeOf(err), err)
	}
	segs, err := e.Segments(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 1 || segs[0].State != SegmentOpen || segs[0].TxnCount != 2 {
		t.Fatalf("original segment not preserved: %+v", segs)
	}
	res, err := e.Commit(ctx, TransactionInput{ID: "c", Records: []PageUpdate{{PageNo: 3, Value: "c"}}})
	if err != nil || res.SegmentNo != 2 || res.StartLSN != 3 {
		t.Fatalf("retry after rotation failure: %+v err=%v", res, err)
	}
}

func TestSyncFailureRollsBackHeaderAndRecords(t *testing.T) {
	ctx := context.Background()
	failSync := true
	e, dir := testEngine(t, Options{
		SegmentMaxBytes: 4096, MaxTxnsPerSegment: 8,
		Fault: func(p FaultPoint) error {
			if p.Point == FaultBeforeGroupSync && failSync {
				failSync = false
				return errors.New("injected fsync failure")
			}
			return nil
		},
	})
	_, err := e.CommitGroup(ctx, []TransactionInput{
		{ID: "s1", Records: []PageUpdate{{PageNo: 1, Value: "one"}}},
		{ID: "s2", Records: []PageUpdate{{PageNo: 2, Value: "two"}}},
	})
	if CodeOf(err) != CodeGroupCommitFailed {
		t.Fatalf("sync failure code=%q err=%v", CodeOf(err), err)
	}
	segs, _ := e.Segments(ctx)
	if len(segs) != 1 || segs[0].RecordCount != 0 || segs[0].TxnCount != 0 {
		t.Fatalf("catalog not rolled back: %+v", segs)
	}
	parsed, err := ParseSegment(segmentPath(dir, 1), 1, 1)
	if err != nil || len(parsed.Records) != 0 {
		t.Fatalf("physical segment not rolled back: %+v err=%v", parsed, err)
	}
	if _, err := e.Commit(ctx, TransactionInput{ID: "after", Records: []PageUpdate{{PageNo: 3, Value: "ok"}}}); err != nil {
		t.Fatal(err)
	}
}

func TestGroupRollbackFailurePreservesPendingIntent(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	var original []byte
	var savedPath string
	inject := true
	e, err := Open(dir, Options{
		SegmentMaxBytes: 4096, MaxTxnsPerSegment: 8,
		Fault: func(p FaultPoint) error {
			if p.Point != FaultBeforeGroupWrite || !inject {
				return nil
			}
			inject = false
			savedPath = segmentPath(dir, p.SegmentNo)
			data, readErr := os.ReadFile(savedPath)
			if readErr != nil {
				return readErr
			}
			original = data
			if err := os.Remove(savedPath); err != nil {
				return err
			}
			return errors.New("injected write failure with rollback unavailable")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.CommitGroup(ctx, []TransactionInput{
		{ID: "rollback-a", Records: []PageUpdate{{PageNo: 1, Value: "one"}}},
		{ID: "rollback-b", Records: []PageUpdate{{PageNo: 2, Value: "two"}}},
	})
	if CodeOf(err) != CodeGroupCommitFailed {
		t.Fatalf("group failure code=%q err=%v", CodeOf(err), err)
	}
	if len(original) < int(segmentHeaderSize) {
		t.Fatalf("fault hook did not save the original segment: %v", err)
	}
	for _, id := range []string{"rollback-a", "rollback-b"} {
		txn, err := e.Transaction(ctx, id)
		if err != nil || txn.State != TxnGrouped {
			t.Fatalf("txn %s state=%+v err=%v", id, txn, err)
		}
	}
	var status string
	if err := e.db.QueryRowContext(ctx, `SELECT status FROM intents WHERE kind='GROUP_COMMIT' ORDER BY id DESC LIMIT 1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "PENDING" {
		t.Fatalf("rollback failure terminated intent as %s", status)
	}
	if _, err := e.Commit(ctx, TransactionInput{ID: "must-not-write", Records: []PageUpdate{{PageNo: 3, Value: "x"}}}); CodeOf(err) != CodeRecoveryRequired {
		t.Fatalf("post-rollback-failure write code=%q err=%v", CodeOf(err), err)
	}
	if err := os.WriteFile(savedPath, original, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e, err = Open(dir, Options{SegmentMaxBytes: 4096, MaxTxnsPerSegment: 8})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e.Close()
	report := e.RecoveryReport()
	if report.Status != "RECOVERED" || !containsAll(report.AbortedTxnIDs, "rollback-a", "rollback-b") {
		t.Fatalf("recovery report=%+v", report)
	}
	segs, err := e.Segments(ctx)
	if err != nil || len(segs) != 1 || segs[0].RecordCount != 0 || segs[0].State != SegmentOpen {
		t.Fatalf("segments after recovery=%+v err=%v", segs, err)
	}
	if _, err := os.Stat(filepath.Join(dir, segmentFileName(segs[0].SegmentNo))); err != nil {
		t.Fatalf("restored segment missing: %v", err)
	}
}

func TestReclaimRecoveryContinuesMarkedIntent(t *testing.T) {
	ctx := context.Background()
	failDelete := true
	e, dir := testEngine(t, Options{
		SegmentMaxBytes: 360, MaxTxnsPerSegment: 2,
		Fault: func(p FaultPoint) error {
			if p.Point == FaultBeforeReclaimDelete && failDelete {
				failDelete = false
				return errors.New("injected delete failure")
			}
			return nil
		},
	})
	for i := 0; i < 5; i++ {
		if _, err := e.Commit(ctx, TransactionInput{ID: "r" + string(rune('a'+i)), Records: []PageUpdate{{PageNo: 1, Value: "x"}}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.AdvanceCheckpoint(ctx, CheckpointRequest{Sequence: 1, ReplayStartLSN: 5}); err != nil {
		t.Fatal(err)
	}
	segs, _ := e.Segments(ctx)
	var targets []uint64
	for _, s := range segs {
		if s.State == SegmentSealed {
			targets = append(targets, s.SegmentNo)
		}
	}
	if len(targets) == 0 {
		t.Fatal("no sealed segment")
	}
	_, err := e.ReclaimSegments(ctx, ReclaimRequest{SegmentNos: targets})
	if CodeOf(err) != CodeReclaimFailed {
		t.Fatalf("delete failure code=%q err=%v", CodeOf(err), err)
	}
	afterFail, _ := e.Segments(ctx)
	for _, s := range afterFail {
		for _, n := range targets {
			if s.SegmentNo == n && s.State != SegmentReclaimable {
				t.Fatalf("segment %d state after failed delete = %s", n, s.State)
			}
		}
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(dir, Options{SegmentMaxBytes: 360, MaxTxnsPerSegment: 2})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e.Close()
	report := e.RecoveryReport()
	if report.Status != "RECOVERED" {
		t.Fatalf("report = %+v", report)
	}
	all, err := e.Segments(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range all {
		for _, n := range targets {
			if s.SegmentNo == n {
				t.Fatalf("reclaimed segment %d still visible: %+v", n, s)
			}
		}
	}
	for _, n := range targets {
		if _, err := os.Stat(segmentPath(dir, n)); !os.IsNotExist(err) {
			t.Fatalf("segment %d file not removed: %v", n, err)
		}
	}
	found := false
	for _, it := range report.Intents {
		if it.Kind == "RECLAIM" && it.Action == "CONTINUED_RECLAIM_TO_RECLAIMED" {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing reclaim recovery detail: %+v", report.Intents)
	}
}
