package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestPreparedIntentRecoveryAndReportConsistency(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "recover.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(db, "http://127.0.0.1:1", 10*time.Second)
	ctx := context.Background()
	if _, err := svc.CreateTopic(ctx, Topic{Name: "orders", Partitions: 1, RoutingRule: "explicit"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		p := 0
		if _, err := svc.Publish(ctx, PublishRequest{MessageID: "m" + string(rune('0'+i)),
			Topic: "orders", Partition: &p, Body: "body", RequestID: "p" + string(rune('0'+i))}); err != nil {
			t.Fatal(err)
		}
	}
	pull, err := svc.Pull(ctx, "orders", 0, PullRequest{Owner: "safe", Limit: 1, RequestID: "safe-pull"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Commit(ctx, "orders", 0, CommitRequest{Owner: "safe",
		LeaseVersion: pull.Lease.LeaseVersion, ThroughSeq: 0, RequestID: "safe-commit"}); err != nil {
		t.Fatal(err)
	}
	p := 0
	if _, err := svc.Publish(ctx, PublishRequest{MessageID: "m2", Topic: "orders", Partition: &p,
		Body: "txn", Transactional: true, TxnID: "tx-crash", RequestID: "p2"}); err != nil {
		t.Fatal(err)
	}

	operations := []struct {
		op      string
		target  string
		payload any
	}{
		{"pull", "orders/0", PullRequest{Owner: "crashed", Limit: 1}},
		{"commit", "orders/0", CommitRequest{Owner: "crashed", ThroughSeq: 1}},
		{"replay", "orders/0", ReplayRequest{Owner: "crashed", FromSeq: 0}},
		{"txn_resolve", "tx-crash", ResolveRequest{QueryTimeoutMS: 100}},
	}
	for i, op := range operations {
		decision, err := svc.prepareOperation(ctx, "crash-request-"+string(rune('a'+i)),
			op.op, op.target, op.payload, nil)
		if err != nil {
			t.Fatalf("prepare %s: %v", op.op, err)
		}
		if decision.Op == nil {
			t.Fatalf("prepare %s did not create an intent", op.op)
		}
	}
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := openDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	svc2 := NewService(db2, "http://127.0.0.1:1", 10*time.Second)
	report, err := svc2.RecoverIntents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.RecoveredIntents != 4 || len(report.Details) != 4 {
		t.Fatalf("recovery count mismatch: %+v", report)
	}
	actions := map[string]bool{}
	for _, detail := range report.Details {
		actions[detail.Action] = true
	}
	for _, wanted := range []string{"pull_rolled_back_to_consumable", "commit_rolled_back_to_old_cursor",
		"replay_aborted", "transaction_left_pending"} {
		if !actions[wanted] {
			t.Fatalf("missing recovery action %s: %+v", wanted, report.Details)
		}
	}
	partition, err := svc2.GetPartition(ctx, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if partition.ConsumeCursor != 1 || partition.LeaseState != LeaseFree || partition.ReplayActive {
		t.Fatalf("recovery mutated or failed to preserve partition state: %+v", partition)
	}
	txn, err := svc2.GetTransaction(ctx, "tx-crash")
	if err != nil {
		t.Fatal(err)
	}
	if txn.Status != TxnPending {
		t.Fatalf("recovery changed txn state: %+v", txn)
	}
	messages, err := svc2.ListMessages(ctx, "orders", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range messages {
		if msg.MessageID == "m2" && msg.Status != MsgPendingInvisible {
			t.Fatalf("uncommitted transaction became visible: %+v", msg)
		}
	}
	latest, err := svc2.LatestRecoveryReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if latest == nil || latest.ID != report.ID || latest.RecoveredIntents != report.RecoveredIntents {
		t.Fatalf("diagnostic recovery report differs from startup report: %+v %+v", latest, report)
	}
	var running int
	if err := db2.QueryRow(`SELECT COUNT(*) FROM requests WHERE status='RUNNING'`).Scan(&running); err != nil {
		t.Fatal(err)
	}
	if running != 0 {
		t.Fatalf("recovery left %d RUNNING requests", running)
	}
	var recovered int
	if err := db2.QueryRow(`SELECT COUNT(*) FROM intents WHERE status='RECOVERED'`).Scan(&recovered); err != nil {
		t.Fatal(err)
	}
	if recovered != 4 {
		t.Fatalf("recovered intents in DB = %d", recovered)
	}
}
