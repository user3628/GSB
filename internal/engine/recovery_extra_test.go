package engine

import (
	"encoding/json"
	"testing"

	"mvccengine/pkg/mvcc"
)

func TestRecoveryPendingWriteIntentDeletesVersion(t *testing.T) {
	e := newTestEngine(t)
	txn := mustBegin(t, e)
	if _, err := e.WriteVersion(WriteVersionRequest{TxnSeq: txn.Seq, RowKey: "pending:write", Content: "v1"}); err != nil {
		t.Fatal(err)
	}

	pendingVersion := mvcc.RowVersion{
		RowKey:       "pending:write",
		VersionSeq:   2,
		CreateTxnSeq: txn.Seq,
		CommitState:  mvcc.VersionUncommitted,
		Digest:       mvcc.DigestContent("v2"),
		Content:      "v2",
	}
	prev := int64(1)
	pendingVersion.PrevVersionSeq = &prev
	payload, _ := json.Marshal(pendingVersion)
	now := e.now()
	_, err := e.db.Exec(`INSERT INTO versions(row_key,version_seq,create_txn_seq,commit_state,digest,content,prev_version_seq,is_delete,created_at_ms,updated_at_ms)
		VALUES('pending:write',2,?,'UNCOMMITTED',?,'v2',1,0,?,?)`, txn.Seq, pendingVersion.Digest, now, now)
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.db.Exec(`UPDATE row_heads SET version_seq=2 WHERE row_key='pending:write'`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.db.Exec(`INSERT INTO intents(intent_id,kind,request_id,payload_json,status,created_at_ms,updated_at_ms)
		VALUES('intent-write-pending','write_version','',?,'PENDING',?,?)`, string(payload), now, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Reload(); err != nil {
		t.Fatal(err)
	}

	report, err := e.RecoverPending()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, seq := range report.DeletedVersionSeqs {
		if seq == 2 {
			found = true
		}
	}
	if !found {
		t.Fatalf("deleted versions=%v report=%+v", report.DeletedVersionSeqs, report)
	}
	if _, exists := e.versions[2]; exists {
		t.Fatal("pending write version still exists")
	}
	if e.heads["pending:write"] != 1 {
		t.Fatalf("head = %d, want 1", e.heads["pending:write"])
	}
}

func TestRecoveryPendingSnapshotIntentKeepsOldSnapshot(t *testing.T) {
	e := newTestEngine(t)
	snapshot := mvcc.Snapshot{SnapshotSeq: 99, GeneratedAtMS: 123, TxnUpperBound: 99}.WithChecksum()
	payload, _ := json.Marshal(snapshot)
	_, err := e.db.Exec(`INSERT INTO intents(intent_id,kind,request_id,payload_json,status,created_at_ms,updated_at_ms)
		VALUES('intent-snapshot-pending','switch_snapshot','',?,'PENDING',?,?)`, string(payload), e.now(), e.now())
	if err != nil {
		t.Fatal(err)
	}
	report, err := e.RecoverPending()
	if err != nil {
		t.Fatal(err)
	}
	if len(report.AbortedIntentIDs) != 1 || report.AbortedIntentIDs[0] != "intent-snapshot-pending" {
		t.Fatalf("aborted intents=%v", report.AbortedIntentIDs)
	}
	if e.State().CurrentSnapshot.SnapshotSeq != 1 {
		t.Fatalf("current snapshot changed to %d", e.State().CurrentSnapshot.SnapshotSeq)
	}
	if _, ok := e.snapshots[99]; ok {
		t.Fatal("pending snapshot was persisted as a loaded snapshot")
	}
}
