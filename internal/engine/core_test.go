package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"mvccengine/pkg/mvcc"
	"mvccengine/samples"
)

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func codeOf(t *testing.T, err error) mvcc.Code {
	t.Helper()
	var me *mvcc.Error
	if !errors.As(err, &me) {
		t.Fatalf("expected *mvcc.Error, got %T: %v", err, err)
	}
	return me.Code
}

func TestVisibilityReasonsSnapshotAndDelete(t *testing.T) {
	e := newTestEngine(t)
	sample, err := samples.Deterministic()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.ImportSample(sample); err != nil {
		t.Fatal(err)
	}

	first, err := e.Read(ReadRequest{TxnSeq: 4, RowKey: "acct:alpha"})
	if err != nil {
		t.Fatal(err)
	}
	if !first.Visible || first.HitVersionSeq == nil || *first.HitVersionSeq != 2 {
		t.Fatalf("expected v2 visible, got %+v", first)
	}
	if first.Reason != mvcc.ReasonVisible {
		t.Fatalf("unexpected reason %s", first.Reason)
	}
	reasons := map[int64]string{}
	for _, candidate := range first.Candidates {
		reasons[candidate.VersionSeq] = candidate.Reason
	}
	if reasons[5] != mvcc.ReasonSelfUncommitted {
		t.Fatalf("v5 reason = %q, want SELF_UNCOMMITTED", reasons[5])
	}
	if reasons[3] != mvcc.ReasonRolledBack {
		t.Fatalf("v3 reason = %q, want ROLLED_BACK", reasons[3])
	}

	second, err := e.Read(ReadRequest{TxnSeq: 4, RowKey: "acct:alpha"})
	if err != nil {
		t.Fatal(err)
	}
	if string(first.CanonicalJSON()) != string(second.CanonicalJSON()) {
		t.Fatalf("repeated read differs:\n%s\n%s", first.CanonicalJSON(), second.CanonicalJSON())
	}

	outsideReader, err := e.BeginTxn(BeginTxnRequest{})
	if err != nil {
		t.Fatal(err)
	}
	outsideWriter, err := e.BeginTxn(BeginTxnRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.WriteVersion(WriteVersionRequest{TxnSeq: outsideWriter.Seq, RowKey: "outside:row", Content: "future"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.CommitTxn(CommitTxnRequest{TxnSeq: outsideWriter.Seq}); err != nil {
		t.Fatal(err)
	}
	outsideResult, err := e.Read(ReadRequest{TxnSeq: outsideReader.Seq, RowKey: "outside:row"})
	if err != nil {
		t.Fatal(err)
	}
	foundOutsideReason := false
	for _, candidate := range outsideResult.Candidates {
		if candidate.Reason == mvcc.ReasonSnapshotOutsideCommit {
			foundOutsideReason = true
		}
	}
	if !foundOutsideReason {
		t.Fatalf("expected snapshot-outside reason, got %+v", outsideResult.Candidates)
	}

	deleteEngine := newTestEngine(t)
	t1 := mustBegin(t, deleteEngine)
	mustWriteCommit(t, deleteEngine, t1.Seq, "delete:row", "before-delete")
	t2 := mustBegin(t, deleteEngine)
	t3 := mustBegin(t, deleteEngine)
	if _, err := deleteEngine.WriteVersion(WriteVersionRequest{TxnSeq: t3.Seq, RowKey: "delete:row", IsDelete: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := deleteEngine.CommitTxn(CommitTxnRequest{TxnSeq: t3.Seq}); err != nil {
		t.Fatal(err)
	}
	t4 := mustBegin(t, deleteEngine)
	deletedRead, err := deleteEngine.Read(ReadRequest{TxnSeq: t4.Seq, RowKey: "delete:row"})
	if err != nil {
		t.Fatal(err)
	}
	if deletedRead.Visible || deletedRead.Reason != mvcc.ReasonDeletedInSnapshot || deletedRead.ShadowVersionSeq == nil {
		t.Fatalf("expected delete shadow, got %+v", deletedRead)
	}
	oldRead, err := deleteEngine.Read(ReadRequest{TxnSeq: t2.Seq, RowKey: "delete:row"})
	if err != nil {
		t.Fatal(err)
	}
	if !oldRead.Visible || *oldRead.HitVersionSeq != 1 {
		t.Fatalf("long snapshot should still see v1, got %+v", oldRead)
	}
}

func TestInvalidSampleBatchIsNotPersisted(t *testing.T) {
	valid, err := samples.Deterministic()
	if err != nil {
		t.Fatal(err)
	}

	badTxn := valid
	badTxn.Name = "bad-txn"
	badTxn.Transactions = append([]mvcc.SampleTxn{}, valid.Transactions...)
	badTxn.Transactions[1].TxnSeq = 3
	e := newTestEngine(t)
	_, err = e.ImportSample(badTxn)
	if code := codeOf(t, err); code != mvcc.CodeTxnInvalid {
		t.Fatalf("code = %s, want E_TXN_INVALID", code)
	}
	if e.State().TxnCount != 0 || e.State().VersionCount != 0 {
		t.Fatalf("invalid txn batch persisted: %+v", e.State())
	}

	badVersion := valid
	badVersion.Name = "bad-version"
	badVersion.Versions = append([]mvcc.SampleVersion{}, valid.Versions...)
	badVersion.Versions[0].RowKey = ""
	e2 := newTestEngine(t)
	_, err = e2.ImportSample(badVersion)
	if code := codeOf(t, err); code != mvcc.CodeVersionInvalid {
		t.Fatalf("code = %s, want E_VERSION_INVALID", code)
	}
	if e2.State().VersionCount != 0 {
		t.Fatalf("invalid version batch persisted")
	}
}

func TestConcurrentIdempotentCommit(t *testing.T) {
	e := newTestEngine(t)
	txn := mustBegin(t, e)
	if _, err := e.WriteVersion(WriteVersionRequest{RequestID: "write-1", TxnSeq: txn.Seq, RowKey: "idem:row", Content: "once"}); err != nil {
		t.Fatal(err)
	}

	const waiters = 8
	var wg sync.WaitGroup
	results := make([]*CommitResult, waiters)
	errs := make([]error, waiters)
	start := make(chan struct{})
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = e.CommitTxn(CommitTxnRequest{RequestID: "same-commit", TxnSeq: txn.Seq})
		}(i)
	}
	close(start)
	wg.Wait()

	var intent string
	for i, err := range errs {
		if err != nil {
			t.Fatalf("waiter %d: %v", i, err)
		}
		if results[i] == nil || results[i].IntentID == "" {
			t.Fatalf("waiter %d missing result", i)
		}
		if intent == "" {
			intent = results[i].IntentID
		} else if results[i].IntentID != intent {
			t.Fatalf("intent IDs differ: %s vs %s", intent, results[i].IntentID)
		}
	}

	_, conflict := e.CommitTxn(CommitTxnRequest{RequestID: "same-commit", TxnSeq: txn.Seq + 99})
	if code := codeOf(t, conflict); code != mvcc.CodeConflictingRequest {
		t.Fatalf("conflict code = %s, want E_CONFLICTING_REQUEST", code)
	}
}

func TestConcurrentBeginWriteCommitUniqueRows(t *testing.T) {
	e := newTestEngine(t)
	const workers = 12
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rowKey := fmt.Sprintf("worker:%02d", i)
			txn, err := e.BeginTxn(BeginTxnRequest{RequestID: fmt.Sprintf("begin-%d", i)})
			if err != nil {
				errs <- err
				return
			}
			if _, err := e.WriteVersion(WriteVersionRequest{
				RequestID: fmt.Sprintf("write-%d", i),
				TxnSeq:    txn.Seq,
				RowKey:    rowKey,
				Content:   fmt.Sprintf("content-%d", i),
			}); err != nil {
				errs <- err
				return
			}
			if _, err := e.CommitTxn(CommitTxnRequest{RequestID: fmt.Sprintf("commit-%d", i), TxnSeq: txn.Seq}); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	state := e.State()
	if state.TxnCount != workers || state.NextTxnSeq != workers {
		t.Fatalf("txn state = %+v", state)
	}
	reader := mustBegin(t, e)
	for i := 0; i < workers; i++ {
		result, err := e.Read(ReadRequest{TxnSeq: reader.Seq, RowKey: fmt.Sprintf("worker:%02d", i)})
		if err != nil {
			t.Fatal(err)
		}
		if !result.Visible || result.HitContent != fmt.Sprintf("content-%d", i) {
			t.Fatalf("worker row %d not visible: %+v", i, result)
		}
	}
}

func TestChainForkDetectAndRepair(t *testing.T) {
	e := newTestEngine(t)
	seedFork(t, e)

	report, err := e.ValidateChain("chain:fork")
	if code := codeOf(t, err); code != mvcc.CodeChainBroken {
		t.Fatalf("validate err = %v, want E_VERSION_CHAIN_BROKEN", err)
	}
	if report.ForkVersionSeq == nil || *report.ForkVersionSeq != 17 {
		t.Fatalf("fork point = %+v", report.ForkVersionSeq)
	}
	if len(report.TipVersionSeqs) != 2 || report.TipVersionSeqs[0] != 18 || report.TipVersionSeqs[1] != 19 {
		t.Fatalf("tips = %v", report.TipVersionSeqs)
	}

	repaired, err := e.RepairChain(RepairChainRequest{RequestID: "repair-fork", RowKey: "chain:fork"})
	if err != nil {
		t.Fatal(err)
	}
	if repaired.ResetHeadVersionSeq != 17 || len(repaired.DetachedVersionSeqs) != 2 {
		t.Fatalf("repair = %+v", repaired)
	}
	if _, err := e.ValidateChain("chain:fork"); err != nil {
		t.Fatalf("chain invalid after repair: %v", err)
	}
}

func TestSnapshotSwitchValidationAndAffected(t *testing.T) {
	e := newTestEngine(t)
	for i := 0; i < 3; i++ {
		mustBegin(t, e)
	}

	badPath := filepath.Join(t.TempDir(), "bad-snapshot.json")
	bad := mvcc.Snapshot{SnapshotSeq: 2, GeneratedAtMS: 123, TxnUpperBound: 3, Checksum: "0000000000000000000000000000000000000000000000000000000000000000"}
	badJSON := fmt.Sprintf(`{"snapshot_seq":2,"generated_at_ms":123,"txn_upper_bound":3,"checksum":%q}`, bad.Checksum)
	if err := os.WriteFile(badPath, []byte(badJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := e.SwitchSnapshot(SwitchSnapshotRequest{RequestID: "bad-switch", Path: badPath})
	if code := codeOf(t, err); code != mvcc.CodeSnapshotInvalid {
		t.Fatalf("switch bad snapshot code = %s", code)
	}
	if e.State().CurrentSnapshot.SnapshotSeq != 1 {
		t.Fatalf("old snapshot not retained: %+v", e.State().CurrentSnapshot)
	}

	valid := mvcc.Snapshot{SnapshotSeq: 2, GeneratedAtMS: 1764000099000, TxnUpperBound: 3}.WithChecksum()
	validPath := filepath.Join(t.TempDir(), "snapshot-2.json")
	if err := WriteSnapshotFile(validPath, valid); err != nil {
		t.Fatal(err)
	}
	switched, err := e.SwitchSnapshot(SwitchSnapshotRequest{RequestID: "good-switch", Path: validPath})
	if err != nil {
		t.Fatal(err)
	}
	if switched.OldSnapshotSeq != 1 || switched.NewSnapshotSeq != 2 || switched.AffectedTransactions != 3 {
		t.Fatalf("switch result = %+v", switched)
	}
	if fmt.Sprint(switched.PinnedTransactionSeqs) != "[1 2 3]" {
		t.Fatalf("pinned = %v", switched.PinnedTransactionSeqs)
	}
	if e.State().CurrentSnapshot.Checksum != valid.Checksum {
		t.Fatal("current snapshot did not switch atomically")
	}

	_, err = e.SwitchSnapshot(SwitchSnapshotRequest{RequestID: "stale-switch", SnapshotSeq: 1, TxnUpperBound: 1})
	if code := codeOf(t, err); code != mvcc.CodeSnapshotInvalid {
		t.Fatalf("stale switch code = %s", code)
	}
}

func TestRecoveryAndReclaimSemantics(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mvcc.db")
	e, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t1 := mustBegin(t, e)
	if _, err := e.WriteVersion(WriteVersionRequest{TxnSeq: t1.Seq, RowKey: "recover:row1", Content: "row1-v1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.WriteVersion(WriteVersionRequest{TxnSeq: t1.Seq, RowKey: "recover:row2", Content: "row2-v1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.CommitTxn(CommitTxnRequest{TxnSeq: t1.Seq}); err != nil {
		t.Fatal(err)
	}
	longTxn := mustBegin(t, e)
	t3 := mustBegin(t, e)
	if _, err := e.WriteVersion(WriteVersionRequest{RequestID: "write-crash", TxnSeq: t3.Seq, RowKey: "recover:row1", Content: "row1-uncommitted"}); err != nil {
		t.Fatal(err)
	}

	before1, err := e.Read(ReadRequest{TxnSeq: longTxn.Seq, RowKey: "recover:row1"})
	if err != nil {
		t.Fatal(err)
	}
	before2, err := e.Read(ReadRequest{TxnSeq: longTxn.Seq, RowKey: "recover:row2"})
	if err != nil {
		t.Fatal(err)
	}
	beforeProjection1 := recoveryProjection(before1)
	beforeProjection2 := recoveryProjection(before2)

	_, err = e.db.Exec(`INSERT INTO idempotent_requests(request_id,scope,request_hash,status,intent_id,created_at_ms,updated_at_ms)
		VALUES('crash-commit','commit_txn','hash','PENDING','intent-commit-crash',?,?)`, e.now(), e.now())
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.db.Exec(`INSERT INTO intents(intent_id,kind,request_id,payload_json,status,created_at_ms,updated_at_ms)
		VALUES('intent-commit-crash','commit_txn','crash-commit',?,'PENDING',?,?)`,
		`{"txn_seq":3,"request_id":"crash-commit"}`, e.now(), e.now())
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	report, err := reopened.RecoverPending()
	if err != nil {
		t.Fatal(err)
	}
	if report.PendingIntentsScanned != 1 {
		t.Fatalf("scanned = %d", report.PendingIntentsScanned)
	}
	if len(report.AbortedTxnSeqs) != 1 || report.AbortedTxnSeqs[0] != 3 {
		t.Fatalf("aborted txns = %v", report.AbortedTxnSeqs)
	}
	if len(report.InterruptedRequestKeys) != 1 || report.InterruptedRequestKeys[0] != "crash-commit" {
		t.Fatalf("interrupted = %v", report.InterruptedRequestKeys)
	}

	after1, err := reopened.Read(ReadRequest{TxnSeq: longTxn.Seq, RowKey: "recover:row1"})
	if err != nil {
		t.Fatal(err)
	}
	after2, err := reopened.Read(ReadRequest{TxnSeq: longTxn.Seq, RowKey: "recover:row2"})
	if err != nil {
		t.Fatal(err)
	}
	afterProjection1 := recoveryProjection(after1)
	afterProjection2 := recoveryProjection(after2)
	if afterProjection1 != beforeProjection1 || afterProjection2 != beforeProjection2 {
		t.Fatalf("visible read sets differ after recovery\nrow1 before %s after %s\nrow2 before %s after %s", beforeProjection1, afterProjection1, beforeProjection2, afterProjection2)
	}

	t4 := mustBegin(t, reopened)
	if t4.Seq != 4 {
		t.Fatalf("txn seq after recovery = %d", t4.Seq)
	}
	if _, err := reopened.WriteVersion(WriteVersionRequest{TxnSeq: t4.Seq, RowKey: "recover:row1", Content: "row1-v2"}); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.CommitTxn(CommitTxnRequest{TxnSeq: t4.Seq}); err != nil {
		t.Fatal(err)
	}
	_, unsafe := reopened.Reclaim(ReclaimRequest{RequestID: "reclaim-blocked", RowKey: "recover:row1", VersionSeq: 1})
	if code := codeOf(t, unsafe); code != mvcc.CodeReclaimUnsafe {
		t.Fatalf("reclaim code = %s", code)
	}
	unsafeContext := mvcc.AsError(unsafe).Context
	if unsafeContext["earliest_blocking_snapshot_txn_seq"] != int64(2) {
		t.Fatalf("context = %#v", unsafeContext)
	}

	if _, err := reopened.RollbackTxn(RollbackTxnRequest{TxnSeq: longTxn.Seq}); err != nil {
		t.Fatal(err)
	}
	reader5 := mustBegin(t, reopened)
	preSweep, err := reopened.Read(ReadRequest{TxnSeq: reader5.Seq, RowKey: "recover:row1"})
	if err != nil {
		t.Fatal(err)
	}
	sweep, err := reopened.Reclaim(ReclaimRequest{RequestID: "reclaim-safe", All: true})
	if err != nil {
		t.Fatal(err)
	}
	if sweep.RemovedCount != 1 || len(sweep.RemovedVersionSeqs) != 1 || sweep.RemovedVersionSeqs[0] != 1 {
		t.Fatalf("sweep = %+v", sweep)
	}
	postSweep, err := reopened.Read(ReadRequest{TxnSeq: reader5.Seq, RowKey: "recover:row1"})
	if err != nil {
		t.Fatal(err)
	}
	if string(preSweep.CanonicalJSON()) != string(postSweep.CanonicalJSON()) {
		t.Fatalf("visibility changed by reclaim:\n%s\n%s", preSweep.CanonicalJSON(), postSweep.CanonicalJSON())
	}
	chainReport, err := reopened.ValidateChain("recover:row1")
	if err != nil || !chainReport.OK {
		t.Fatalf("reclaimed audit boundary should remain structurally valid: report=%+v err=%v", chainReport, err)
	}
}

func mustBegin(t *testing.T, e *Engine) *mvcc.Txn {
	t.Helper()
	txn, err := e.BeginTxn(BeginTxnRequest{})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	return txn
}

func recoveryProjection(result *mvcc.ReadResult) string {
	hit := int64(0)
	if result.HitVersionSeq != nil {
		hit = *result.HitVersionSeq
	}
	return fmt.Sprintf("%v|%s|%s|%d|%s", result.Visible, result.Outcome, result.Reason, hit, result.HitDigest)
}

func mustWriteCommit(t *testing.T, e *Engine, txnSeq int64, rowKey, content string) {
	t.Helper()
	if _, err := e.WriteVersion(WriteVersionRequest{TxnSeq: txnSeq, RowKey: rowKey, Content: content}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := e.CommitTxn(CommitTxnRequest{TxnSeq: txnSeq}); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func seedFork(t *testing.T, e *Engine) {
	t.Helper()
	now := e.now()
	if _, err := e.db.Exec(`INSERT INTO txns(seq,status,started_at_ms,snapshot_seq,upper_bound) VALUES(1,'COMMITTED',?,1,1)`, now); err != nil {
		t.Fatal(err)
	}
	digest := mvcc.DigestContent("fork")
	for _, seq := range []int64{17, 18, 19} {
		var prev any
		if seq != 17 {
			prev = int64(17)
		}
		if _, err := e.db.Exec(`INSERT INTO versions(row_key,version_seq,create_txn_seq,commit_state,digest,content,prev_version_seq,is_delete,created_at_ms,updated_at_ms)
			VALUES('chain:fork',?,1,'COMMITTED',?,'fork',?,0,?,?)`, seq, digest, prev, now, now); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.db.Exec(`INSERT INTO row_heads(row_key,version_seq) VALUES('chain:fork',19)`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.Exec(`UPDATE meta SET value='19' WHERE key='next_version_seq'`); err != nil {
		t.Fatal(err)
	}
	if err := e.loadState(); err != nil {
		t.Fatal(err)
	}
}

var _ = json.Marshal
