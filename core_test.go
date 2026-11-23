package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPublishOrderRoutingAndIdempotency(t *testing.T) {
	env := newTestEnv(t, 10*time.Second)
	env.topic("orders", 2, "explicit", 100, 10)
	env.topic("audit", 4, "hash")

	a := env.publish("m0", "orders", 0, "body-0", "", "pub-0")
	b := env.publish("m1", "orders", 0, "body-1", "", "pub-1")
	c := env.publish("m2", "orders", 1, "body-2", "", "pub-2")
	if a.Message.Seq != 0 || b.Message.Seq != 1 || c.Message.Seq != 0 {
		t.Fatalf("unexpected sequence allocation: %d %d %d", a.Message.Seq, b.Message.Seq, c.Message.Seq)
	}

	dup, err := env.svc.Publish(context.Background(), PublishRequest{
		MessageID: "m0", Topic: "orders", Partition: intPtr(0), Body: "body-0", RequestID: "pub-dup",
	})
	if err != nil {
		t.Fatalf("duplicate publish: %v", err)
	}
	if !dup.Duplicate || dup.Message.Seq != 0 {
		t.Fatalf("duplicate did not return existing sequence: %+v", dup)
	}
	_, err = env.svc.Publish(context.Background(), PublishRequest{
		MessageID: "m0", Topic: "orders", Partition: intPtr(0), Body: "changed", RequestID: "pub-conflict",
	})
	requireCode(t, err, CodeConflictingMessage)

	_, err = env.svc.Publish(context.Background(), PublishRequest{
		MessageID: "bad-route", Topic: "orders", Body: "x", RequestID: "route-missing",
	})
	requireCode(t, err, CodeRouteInvalid)
	_, err = env.svc.Publish(context.Background(), PublishRequest{
		MessageID: "bad-range", Topic: "orders", Partition: intPtr(9), Body: "x", RequestID: "route-range",
	})
	requireCode(t, err, CodeRouteInvalid)

	h1, err := env.svc.Publish(context.Background(), PublishRequest{
		MessageID: "h1", Topic: "audit", PartitionKey: "tenant-a", Body: "a", RequestID: "hash-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	h2, err := env.svc.Publish(context.Background(), PublishRequest{
		MessageID: "h2", Topic: "audit", PartitionKey: "tenant-a", Body: "b", RequestID: "hash-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if h1.Message.Partition != h2.Message.Partition || h2.Message.Seq != h1.Message.Seq+1 {
		t.Fatalf("hash routing not deterministic: %+v %+v", h1.Message, h2.Message)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := env.svc.Publish(context.Background(), PublishRequest{
				MessageID: "concurrent-" + string(rune('a'+i)), Topic: "orders", Partition: intPtr(1),
				Body: "concurrent", RequestID: "concurrent-" + string(rune('a'+i)),
			})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent publish: %v", err)
	}
	msgs, err := env.svc.ListMessages(context.Background(), "orders", 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int64]bool{}
	for _, msg := range msgs {
		if seen[msg.Seq] {
			t.Fatalf("duplicate sequence %d", msg.Seq)
		}
		seen[msg.Seq] = true
	}
	if len(seen) != 21 {
		t.Fatalf("expected 21 unique sequences on orders/1, got %d", len(seen))
	}
}

func TestStrictConsumptionLeaseCommitAndStale(t *testing.T) {
	env := newTestEnv(t, 10*time.Second)
	env.topic("orders", 1, "explicit")
	for i := 0; i < 3; i++ {
		env.publish("m"+string(rune('0'+i)), "orders", 0, "body", "", "p"+string(rune('0'+i)))
	}
	ctx := context.Background()
	first, err := env.svc.Pull(ctx, "orders", 0, PullRequest{Owner: "a", Limit: 1, RequestID: "pull-a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Messages) != 1 || first.Messages[0].Seq != 0 {
		t.Fatalf("first pull = %+v", first.Messages)
	}
	v1 := first.Lease.LeaseVersion
	_, err = env.svc.Pull(ctx, "orders", 0, PullRequest{Owner: "a", Limit: 1, RequestID: "pull-a-no-version"})
	requireCode(t, err, CodeLeaseStale)
	_, err = env.svc.Pull(ctx, "orders", 0, PullRequest{Owner: "b", Limit: 1, RequestID: "pull-b"})
	requireCode(t, err, CodeLeaseHeld)

	committed, err := env.svc.Commit(ctx, "orders", 0, CommitRequest{
		Owner: "a", LeaseVersion: v1, ThroughSeq: 0, RequestID: "commit-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if committed.Partition.ConsumeCursor != 1 || committed.Partition.LeaseState != LeaseFree {
		t.Fatalf("commit state = %+v", committed.Partition)
	}
	_, err = env.svc.Commit(ctx, "orders", 0, CommitRequest{
		Owner: "a", LeaseVersion: v1, ThroughSeq: 0, RequestID: "commit-stale",
	})
	requireCode(t, err, CodeLeaseStale)

	second, err := env.svc.Pull(ctx, "orders", 0, PullRequest{Owner: "b", Limit: 1, RequestID: "pull-b2"})
	if err != nil {
		t.Fatal(err)
	}
	if second.Messages[0].Seq != 1 {
		t.Fatalf("consumer did not resume at seq 1: %+v", second.Messages)
	}
	if _, err := env.svc.Commit(ctx, "orders", 0, CommitRequest{
		Owner: "b", LeaseVersion: second.Lease.LeaseVersion, ThroughSeq: 1, RequestID: "commit-b",
	}); err != nil {
		t.Fatal(err)
	}
	third, err := env.svc.Pull(ctx, "orders", 0, PullRequest{Owner: "c", Limit: 1, RequestID: "pull-c"})
	if err != nil {
		t.Fatal(err)
	}
	if third.Messages[0].Seq != 2 {
		t.Fatalf("order violated: %+v", third.Messages)
	}
}

func intPtr(v int) *int { return &v }

func TestLeaseTimeoutResumeAndInjectedPreemption(t *testing.T) {
	env := newTestEnv(t, 70*time.Millisecond)
	env.topic("orders", 1, "explicit")
	env.publish("m0", "orders", 0, "zero", "", "p0")
	env.publish("m1", "orders", 0, "one", "", "p1")
	ctx := context.Background()
	first, err := env.svc.Pull(ctx, "orders", 0, PullRequest{Owner: "a", Limit: 1, RequestID: "timeout-pull-a"})
	if err != nil {
		t.Fatal(err)
	}
	oldVersion := first.Lease.LeaseVersion
	time.Sleep(110 * time.Millisecond)
	second, err := env.svc.Pull(ctx, "orders", 0, PullRequest{Owner: "b", Limit: 1, RequestID: "timeout-pull-b"})
	if err != nil {
		t.Fatal(err)
	}
	if second.Messages[0].Seq != 0 {
		t.Fatalf("expired lease skipped to %d instead of resuming at 0", second.Messages[0].Seq)
	}
	_, err = env.svc.Commit(ctx, "orders", 0, CommitRequest{Owner: "a", LeaseVersion: oldVersion,
		ThroughSeq: 0, RequestID: "timeout-stale-commit"})
	requireCode(t, err, CodeLeaseStale)

	preempted, err := env.svc.Pull(ctx, "orders", 0, PullRequest{Owner: "c", Limit: 1,
		LeaseInjection: InjectionLeaseTimeoutPreempt, RequestID: "preempt"})
	if err != nil {
		t.Fatal(err)
	}
	if preempted.Messages[0].Seq != 0 || preempted.Lease.LeaseOwner != "c" {
		t.Fatalf("preempt did not acquire same sequence: %+v", preempted)
	}
	_, err = env.svc.Commit(ctx, "orders", 0, CommitRequest{Owner: "b",
		LeaseVersion: second.Lease.LeaseVersion, ThroughSeq: 0, RequestID: "preempt-stale"})
	requireCode(t, err, CodeLeaseStale)
	obs, err := env.svc.ListObservations(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, o := range obs {
		if o.Mode == InjectionLeaseTimeoutPreempt && o.Phase == "pull" {
			found = true
		}
	}
	if !found {
		t.Fatalf("preemption observation not recorded: %+v", obs)
	}
}

func TestInvalidTransitionDoesNotMutate(t *testing.T) {
	env := newTestEnv(t, 10*time.Second)
	env.topic("t", 1, "explicit")
	env.publish("m", "t", 0, "body", "tx-1", "p")
	ctx := context.Background()
	_, err := env.svc.DeadLetter(ctx, "m", "dl-before-visible")
	requireCode(t, err, CodeInvalidTransition)
	txn, err := env.svc.GetTransaction(ctx, "tx-1")
	if err != nil {
		t.Fatal(err)
	}
	if txn.Status != TxnPending {
		t.Fatalf("illegal transition mutated txn: %+v", txn)
	}
	msg, err := env.svc.ListMessages(ctx, "t", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msg) != 1 || msg[0].Status != MsgPendingInvisible {
		t.Fatalf("illegal transition mutated message: %+v", msg)
	}
}

func TestRequestIdempotencyAndConflictingRequest(t *testing.T) {
	env := newTestEnv(t, 10*time.Second)
	env.topic("orders", 1, "explicit")
	payload := PublishRequest{MessageID: "m0", Topic: "orders", Partition: intPtr(0),
		Body: "body", RequestID: "request-1"}
	first, err := env.svc.Publish(context.Background(), payload)
	if err != nil {
		t.Fatal(err)
	}
	second, err := env.svc.Publish(context.Background(), payload)
	if err != nil {
		t.Fatal(err)
	}
	if first.Message.Seq != second.Message.Seq || second.RequestID != "request-1" {
		t.Fatalf("stored response mismatch: %+v %+v", first, second)
	}
	changed := payload
	changed.MessageID = "m1"
	_, err = env.svc.Publish(context.Background(), changed)
	requireCode(t, err, CodeConflictingRequest)

	var wg sync.WaitGroup
	results := make(chan PublishResult, 8)
	errs := make(chan error, 8)
	concurrent := PublishRequest{MessageID: "m2", Topic: "orders", Partition: intPtr(0),
		Body: "concurrent", RequestID: "request-2"}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := env.svc.Publish(context.Background(), concurrent)
			if err != nil {
				errs <- err
				return
			}
			results <- out
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent idempotent call: %v", err)
	}
	seq := int64(-1)
	for out := range results {
		if seq == -1 {
			seq = out.Message.Seq
		} else if out.Message.Seq != seq {
			t.Fatalf("different result for duplicate request: %d vs %d", seq, out.Message.Seq)
		}
	}
	if seq != 1 {
		t.Fatalf("expected one new message at seq 1, got %d", seq)
	}
	var count int
	if err := env.db.QueryRow(`SELECT COUNT(*) FROM messages WHERE message_id='m2'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("idempotent request inserted %d messages", count)
	}
}

var _ = errors.Is
