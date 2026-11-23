package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestTransactionVisibilityTimeoutRetryAndConflict(t *testing.T) {
	env := newTestEnv(t, 10*time.Second)
	env.topic("orders", 1, "explicit")
	env.publish("m0", "orders", 0, "zero", "tx-commit", "p0")
	env.publish("m1", "orders", 0, "one", "tx-rollback", "p1")
	env.publish("m2", "orders", 0, "two", "", "p2")
	env.publish("m3", "orders", 0, "three", "tx-hang", "p3")
	ctx := context.Background()

	if got := env.partition("orders", 0); got.VisibleFrontier != 0 || got.PendingTxnCount != 3 {
		t.Fatalf("pending transactions leaked into frontier: %+v", got)
	}
	commit, err := env.svc.ResolveTransaction(ctx, "tx-commit", ResolveRequest{
		RequestID: "resolve-commit", InjectionMode: CoordinatorCommit})
	if err != nil {
		t.Fatal(err)
	}
	if commit.Transaction.Status != TxnCommitted || commit.Message.Status != MsgVisible ||
		commit.Partition.VisibleFrontier != 1 {
		t.Fatalf("commit visibility was not atomic: %+v", commit)
	}
	rollback, err := env.svc.ResolveTransaction(ctx, "tx-rollback", ResolveRequest{
		RequestID: "resolve-rollback", InjectionMode: CoordinatorRollback})
	if err != nil {
		t.Fatal(err)
	}
	if rollback.Transaction.Status != TxnRolledBack || rollback.Message.Status != MsgRolledBack ||
		rollback.Partition.VisibleFrontier != 3 {
		t.Fatalf("rollback frontier mismatch: %+v", rollback)
	}
	if rollback.Partition.PendingTxnCount != 1 {
		t.Fatalf("pending count = %d", rollback.Partition.PendingTxnCount)
	}

	start := time.Now()
	_, err = env.svc.ResolveTransaction(ctx, "tx-hang", ResolveRequest{
		RequestID: "resolve-hang", InjectionMode: CoordinatorHang, QueryTimeoutMS: 60})
	requireCode(t, err, CodeTxnTimeout)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("coordinator timeout did not close promptly: %v", elapsed)
	}
	stillPending, err := env.svc.GetTransaction(ctx, "tx-hang")
	if err != nil {
		t.Fatal(err)
	}
	if stillPending.Status != TxnPending {
		t.Fatalf("timeout changed transaction status: %+v", stillPending)
	}
	retry, err := env.svc.ResolveTransaction(ctx, "tx-hang", ResolveRequest{
		RequestID: "resolve-hang-retry", InjectionMode: CoordinatorCommit})
	if err != nil {
		t.Fatal(err)
	}
	if retry.Transaction.Status != TxnCommitted || retry.Partition.VisibleFrontier != 4 {
		t.Fatalf("retry did not commit: %+v", retry)
	}

	_, err = env.svc.ResolveTransaction(ctx, "tx-commit", ResolveRequest{
		RequestID: "resolve-conflict", InjectionMode: CoordinatorConflict, ForceRecheck: true})
	requireCode(t, err, CodeTxnConflict)
	afterConflict, err := env.svc.GetTransaction(ctx, "tx-commit")
	if err != nil {
		t.Fatal(err)
	}
	if afterConflict.Status != TxnCommitted || afterConflict.Decision != TxnDecisionCommit {
		t.Fatalf("conflicting conclusion mutated persisted state: %+v", afterConflict)
	}

	idem, err := env.svc.ResolveTransaction(ctx, "tx-commit", ResolveRequest{RequestID: "resolve-idem"})
	if err != nil {
		t.Fatal(err)
	}
	if !idem.Idempotent || idem.Transaction.Decision != TxnDecisionCommit {
		t.Fatalf("existing conclusion was not reused: %+v", idem)
	}
	observations, err := env.svc.ListObservations(ctx, 30)
	if err != nil {
		t.Fatal(err)
	}
	codes := map[string]bool{}
	for _, o := range observations {
		if o.ObservedCode != "" {
			codes[o.ObservedCode] = true
		}
	}
	if !codes[CodeTxnTimeout] || !codes[CodeTxnConflict] {
		t.Fatalf("timeout/conflict observations missing: %+v", observations)
	}
}

func TestReplayDoesNotDuplicateFrontierAndBlocksNewMessages(t *testing.T) {
	env := newTestEnv(t, 10*time.Second)
	env.topic("orders", 1, "explicit")
	for i := 0; i < 4; i++ {
		env.publish("m"+string(rune('0'+i)), "orders", 0, "body", "", "p"+string(rune('0'+i)))
	}
	ctx := context.Background()
	for i := int64(0); i < 4; i++ {
		pull, err := env.svc.Pull(ctx, "orders", 0, PullRequest{Owner: "consumer", Limit: 1,
			RequestID: "consume-pull-" + string(rune('0'+i))})
		if err != nil {
			t.Fatal(err)
		}
		if len(pull.Messages) != 1 || pull.Messages[0].Seq != i {
			t.Fatalf("consume order at %d: %+v", i, pull.Messages)
		}
		if _, err := env.svc.Commit(ctx, "orders", 0, CommitRequest{Owner: "consumer",
			LeaseVersion: pull.Lease.LeaseVersion, ThroughSeq: i,
			RequestID: "consume-commit-" + string(rune('0'+i))}); err != nil {
			t.Fatal(err)
		}
	}
	empty, err := env.svc.Pull(ctx, "orders", 0, PullRequest{Owner: "replayer", Limit: 1, RequestID: "replay-lease"})
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.Messages) != 0 {
		t.Fatalf("expected empty pull before replay, got %+v", empty.Messages)
	}
	replay, err := env.svc.Replay(ctx, "orders", 0, ReplayRequest{Owner: "replayer",
		LeaseVersion: empty.Lease.LeaseVersion, FromSeq: 1,
		LeaseInjection: InjectionReplayNewMessage, RequestID: "replay-start"})
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Partition.ReplayActive || replay.ReplayFrom != 1 || replay.ReplayLimit != 4 {
		t.Fatalf("replay state: %+v", replay)
	}
	if replay.Partition.VisibleFrontier != 4 || replay.Partition.NextSeq != 5 {
		t.Fatalf("new message advanced visibility during replay: %+v", replay.Partition)
	}
	if replay.Injection == nil || replay.Injection.ObservedCode != "" {
		t.Fatalf("replay arrival observation missing: %+v", replay.Injection)
	}

	leaseVersion := replay.Partition.LeaseVersion
	for i := int64(1); i < 4; i++ {
		pull, err := env.svc.Pull(ctx, "orders", 0, PullRequest{Owner: "replayer", Limit: 1,
			LeaseVersion: &leaseVersion, RequestID: "replay-pull-" + string(rune('0'+i))})
		if err != nil {
			t.Fatal(err)
		}
		if len(pull.Messages) != 1 || pull.Messages[0].Seq != i {
			t.Fatalf("replay order at %d: %+v", i, pull.Messages)
		}
		if i < 3 && pull.Lease.VisibleFrontier != 4 {
			t.Fatalf("frontier advanced during replay: %d", pull.Lease.VisibleFrontier)
		}
		committed, err := env.svc.Commit(ctx, "orders", 0, CommitRequest{Owner: "replayer",
			LeaseVersion: pull.Lease.LeaseVersion, ThroughSeq: i,
			RequestID: "replay-commit-" + string(rune('0'+i))})
		if err != nil {
			t.Fatal(err)
		}
		leaseVersion = 0
		if i < 3 {
			next, err := env.svc.Pull(ctx, "orders", 0, PullRequest{Owner: "replayer", Limit: 1,
				RequestID: "replay-lease-" + string(rune('0'+i))})
			if err != nil {
				t.Fatal(err)
			}
			leaseVersion = next.Lease.LeaseVersion
		} else {
			if committed.Partition.ReplayActive {
				t.Fatalf("replay did not complete: %+v", committed.Partition)
			}
			if committed.Partition.VisibleFrontier != 5 {
				t.Fatalf("visibility frontier did not catch up after replay: %+v", committed.Partition)
			}
		}
	}
	after, err := env.svc.Pull(ctx, "orders", 0, PullRequest{Owner: "after", Limit: 1, RequestID: "after-replay"})
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Messages) != 1 || after.Messages[0].Seq != 4 {
		t.Fatalf("new message did not become consumable after replay: %+v", after.Messages)
	}

	// Retention makes an otherwise valid older replay target expired.
	if _, err := env.svc.Commit(ctx, "orders", 0, CommitRequest{Owner: "after",
		LeaseVersion: after.Lease.LeaseVersion, ThroughSeq: 4, RequestID: "consume-new"}); err != nil {
		t.Fatal(err)
	}
	if _, err := env.svc.RetainFrom(ctx, "orders", 0, RetainRequest{FromSeq: 2, RequestID: "retain"}); err != nil {
		t.Fatal(err)
	}
	lease, err := env.svc.Pull(ctx, "orders", 0, PullRequest{Owner: "expired", Limit: 1, RequestID: "expired-lease"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = env.svc.Replay(ctx, "orders", 0, ReplayRequest{Owner: "expired",
		LeaseVersion: lease.Lease.LeaseVersion, FromSeq: 1, RequestID: "replay-expired"})
	requireCode(t, err, CodeReplayExpired)
	_, err = env.svc.Replay(ctx, "orders", 0, ReplayRequest{Owner: "expired",
		LeaseVersion: lease.Lease.LeaseVersion, FromSeq: 5, RequestID: "replay-invalid"})
	requireCode(t, err, CodeReplayInvalid)
}

func TestConcurrentSnapshotsNeverSeePartialTxnCommit(t *testing.T) {
	env := newTestEnv(t, 20*time.Second)
	env.topic("orders", 1, "explicit")
	const count = 12
	for i := 0; i < count; i++ {
		id := "m" + string(rune('a'+i))
		env.publish(id, "orders", 0, "body", "tx-"+id, "p-"+id)
	}
	ctx := context.Background()
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	readErr := make(chan error, 1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			dashboard, err := env.svc.DashboardSnapshot(ctx, env.sched)
			if err != nil {
				readErr <- err
				return
			}
			var partition *PartitionView
			for i := range dashboard.Partitions {
				if dashboard.Partitions[i].Topic == "orders" && dashboard.Partitions[i].Partition == 0 {
					partition = &dashboard.Partitions[i]
				}
			}
			if partition == nil {
				readErr <- context.Canceled
				return
			}
			for _, msg := range dashboard.RecentMessages {
				if msg.Topic == "orders" && msg.Partition == 0 && msg.Status == MsgVisible {
					if partition.VisibleFrontier <= msg.Seq {
						readErr <- &partialCommitError{seq: msg.Seq, frontier: partition.VisibleFrontier}
						return
					}
				}
			}
		}
	}()
	for i := 0; i < count; i++ {
		id := "tx-m" + string(rune('a'+i))
		if _, err := env.svc.ResolveTransaction(ctx, id, ResolveRequest{
			RequestID: "resolve-" + id, InjectionMode: CoordinatorCommit}); err != nil {
			close(done)
			wg.Wait()
			t.Fatal(err)
		}
	}
	close(done)
	wg.Wait()
	select {
	case err := <-readErr:
		t.Fatalf("snapshot observed partial commit: %v", err)
	default:
	}
	partition := env.partition("orders", 0)
	if partition.VisibleFrontier != count {
		t.Fatalf("final frontier = %d, want %d", partition.VisibleFrontier, count)
	}
}

type partialCommitError struct {
	seq      int64
	frontier int64
}

func (e *partialCommitError) Error() string {
	return "message visible before frontier advanced"
}
