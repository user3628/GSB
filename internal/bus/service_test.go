package bus_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"local/eventbus/internal/agg"
	"local/eventbus/internal/bus"
	"local/eventbus/internal/sample"
)

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) Advance(delta time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(delta)
}

var requestCounter atomic.Int64

func requestID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, requestCounter.Add(1))
}

func newTestService(t *testing.T, lease time.Duration, clock *testClock) *bus.Service {
	t.Helper()
	if clock == nil {
		clock = &testClock{t: sample.FixedClock()}
	}
	path := filepath.Join(t.TempDir(), "eventbus.sqlite")
	service, err := bus.Open(context.Background(), bus.ServiceOptions{
		DBPath:        path,
		LeaseDuration: lease,
		Now:           clock.Now,
	})
	if err != nil {
		t.Fatalf("open service: %v", err)
	}
	t.Cleanup(func() { _ = service.Close() })
	return service
}

func createTopic(t *testing.T, service *bus.Service, topic string, partitions int, groups ...bus.GroupSpec) bus.Topic {
	t.Helper()
	created, err := service.CreateTopic(context.Background(), bus.CreateTopicRequest{
		RequestID: requestID("topic"), Topic: topic, Partitions: partitions, Groups: groups,
	})
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	return created
}

func publishSpecs(t *testing.T, service *bus.Service, topic string, specs []sample.Spec) []bus.PublishResult {
	t.Helper()
	results := make([]bus.PublishResult, 0, len(specs))
	for _, spec := range specs {
		result, err := service.Publish(context.Background(), sample.ToPublishRequest(requestID("pub"), topic, spec))
		if err != nil {
			t.Fatalf("publish %s: %v", spec.EventID, err)
		}
		results = append(results, result)
	}
	return results
}

func consumeN(t *testing.T, service *bus.Service, topic, group string, partition int, count, maxEvents int) []bus.AckResult {
	t.Helper()
	var acks []bus.AckResult
	consumed := 0
	for consumed < count {
		remaining := count - consumed
		batchSize := maxEvents
		if batchSize <= 0 || batchSize > remaining {
			batchSize = remaining
		}
		pulled, err := service.Pull(context.Background(), bus.PullRequest{
			RequestID: requestID("pull"), Topic: topic, Group: group, Partition: partition,
			ConsumerID: "test-consumer", MaxEvents: batchSize,
		})
		if err != nil {
			t.Fatalf("pull at offset %d: %v", consumed, err)
		}
		seqs := make([]int64, 0, len(pulled.Events))
		for _, event := range pulled.Events {
			seqs = append(seqs, event.Seq)
		}
		ack, err := service.Ack(context.Background(), bus.AckRequest{
			RequestID: requestID("ack"), Topic: topic, Group: group, Partition: partition,
			ConsumerID: "test-consumer", LeaseToken: pulled.LeaseToken, Seqs: seqs,
			AckBatchID: requestID("batch"),
		})
		if err != nil {
			t.Fatalf("ack seqs %v: %v", seqs, err)
		}
		acks = append(acks, ack)
		consumed += len(seqs)
	}
	return acks
}

func mustCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s, got nil", code)
	}
	if got := bus.ErrorCode(err); got != code {
		t.Fatalf("expected %s, got %s (%v)", code, got, err)
	}
}

func TestPublishIdempotencyGaplessRoutingAndConflict(t *testing.T) {
	service := newTestService(t, time.Minute, nil)
	createTopic(t, service, "orders", 2, bus.GroupSpec{Name: "analytics", Algorithm: string(agg.Kahan)})
	specs := sample.Generate(42, 40, 5)
	results := publishSpecs(t, service, "orders", specs)
	routes := make(map[string]int)
	for i, result := range results {
		if i > 0 && result.Partition == results[i-1].Partition && result.Seq != results[i-1].Seq+1 {
			t.Fatalf("partition sequence gap: %+v then %+v", results[i-1], result)
		}
		if result.Seq <= 0 {
			t.Fatalf("non-positive seq: %+v", result)
		}
		if known, ok := routes[specs[i].PartitionKey]; ok && known != result.Partition {
			t.Fatalf("partition key %q routed inconsistently: %d vs %d", specs[i].PartitionKey, known, result.Partition)
		}
		routes[specs[i].PartitionKey] = result.Partition
	}

	firstRequest := sample.ToPublishRequest(requestID("dup"), "orders", specs[0])
	firstRequest.RequestID = requestID("dup")
	first, err := service.Publish(context.Background(), firstRequest)
	if err != nil {
		t.Fatal(err)
	}
	secondRequest := firstRequest
	secondRequest.RequestID = requestID("dup")
	second, err := service.Publish(context.Background(), secondRequest)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Idempotent || second.Seq != first.Seq || second.Partition != first.Partition {
		t.Fatalf("duplicate publish was not idempotent: first=%+v second=%+v", first, second)
	}
	conflicting := secondRequest
	conflicting.RequestID = requestID("dup-conflict")
	conflicting.Metric += 1
	_, err = service.Publish(context.Background(), conflicting)
	mustCode(t, err, bus.CodeConflictingEvent)

	sameRequestDifferentContent := firstRequest
	sameRequestDifferentContent.Metric += 1
	_, err = service.Publish(context.Background(), sameRequestDifferentContent)
	mustCode(t, err, bus.CodeConflictingRequest)

	var count, minSeq, maxSeq int64
	if err := service.DB().QueryRow(`SELECT COUNT(*), MIN(seq), MAX(seq) FROM events WHERE topic='orders' AND partition=?`, first.Partition).
		Scan(&count, &minSeq, &maxSeq); err != nil {
		t.Fatal(err)
	}
	if minSeq != 1 || count != maxSeq {
		t.Fatalf("expected gapless seq 1..%d, got count=%d min=%d max=%d", maxSeq, count, minSeq, maxSeq)
	}
}

func TestLeaseConcurrencyExpiryAndOrderedRedelivery(t *testing.T) {
	clock := &testClock{t: sample.FixedClock()}
	service := newTestService(t, 10*time.Second, clock)
	createTopic(t, service, "leases", 1, bus.GroupSpec{Name: "workers", Algorithm: string(agg.Naive)})
	specs := sample.Generate(7, 4, 2)
	publishSpecs(t, service, "leases", specs)

	type pullOutcome struct {
		result bus.PullResult
		err    error
	}
	start := make(chan struct{})
	outcomes := make(chan pullOutcome, 2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			<-start
			result, err := service.Pull(context.Background(), bus.PullRequest{
				RequestID: requestID("concurrent-pull"), Topic: "leases", Group: "workers",
				Partition: 0, ConsumerID: fmt.Sprintf("consumer-%d", i), MaxEvents: 1,
			})
			outcomes <- pullOutcome{result: result, err: err}
		}(i)
	}
	close(start)
	var winner bus.PullResult
	successes, leaseFailures := 0, 0
	for i := 0; i < 2; i++ {
		outcome := <-outcomes
		switch {
		case outcome.err == nil:
			successes++
			winner = outcome.result
		case bus.ErrorCode(outcome.err) == bus.CodeLeaseHeld:
			leaseFailures++
		default:
			t.Fatalf("unexpected concurrent pull error: %v", outcome.err)
		}
	}
	if successes != 1 || leaseFailures != 1 {
		t.Fatalf("expected one lease winner and one holder, got success=%d held=%d", successes, leaseFailures)
	}
	if len(winner.Events) != 1 || winner.Events[0].Seq != 1 {
		t.Fatalf("unexpected winner payload: %+v", winner)
	}

	clock.Advance(11 * time.Second)
	redelivered, err := service.Pull(context.Background(), bus.PullRequest{
		RequestID: requestID("after-expiry"), Topic: "leases", Group: "workers",
		Partition: 0, ConsumerID: "consumer-new", MaxEvents: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(redelivered.Events) != 2 || redelivered.Events[0].Seq != 1 {
		t.Fatalf("expired lease skipped the original sequence: %+v", redelivered)
	}
	if redelivered.Events[0].DeliveryCount != 2 {
		t.Fatalf("expected redelivery count 2, got %d", redelivered.Events[0].DeliveryCount)
	}
	_, err = service.Ack(context.Background(), bus.AckRequest{
		RequestID: requestID("bad-ack"), Topic: "leases", Group: "workers", Partition: 0,
		ConsumerID: "consumer-new", LeaseToken: redelivered.LeaseToken, Seqs: []int64{2},
	})
	mustCode(t, err, bus.CodeOutOfOrder)
	// The failed ack must not have changed the persisted delivery state.
	state, err := service.GetEventState(context.Background(), "leases", "workers", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if state.State != bus.StateDispatched {
		t.Fatalf("failed ack mutated state: %+v", state)
	}
}

func TestThreeSumAlgorithmsBoundsAndBatchBoundaryIndependence(t *testing.T) {
	specs := sample.Generate(20261125, 96, 7)
	for _, algorithm := range []agg.Algorithm{agg.Naive, agg.Kahan, agg.Magnitude} {
		t.Run(string(algorithm), func(t *testing.T) {
			service := newTestService(t, time.Minute, nil)
			createTopic(t, service, "metrics", 1,
				bus.GroupSpec{Name: "split", Algorithm: string(algorithm)},
				bus.GroupSpec{Name: "bulk", Algorithm: string(algorithm)})
			publishSpecs(t, service, "metrics", specs)

			consumeN(t, service, "metrics", "split", 0, len(specs), 1)
			bulk := consumeN(t, service, "metrics", "bulk", 0, len(specs), len(specs))
			if len(bulk) != 1 {
				t.Fatalf("expected one bulk acknowledgement, got %d", len(bulk))
			}
			splitResult, err := service.GetAggregate(context.Background(), "metrics", "split", 0)
			if err != nil {
				t.Fatal(err)
			}
			bulkResult, err := service.GetAggregate(context.Background(), "metrics", "bulk", 0)
			if err != nil {
				t.Fatal(err)
			}
			if splitResult.StateDigest != bulkResult.StateDigest {
				t.Fatalf("split/bulk aggregation differs: %s vs %s", splitResult.StateDigest, bulkResult.StateDigest)
			}
			if splitResult.RelativeTolerance != bulkResult.RelativeTolerance {
				t.Fatalf("declared tolerance differs")
			}

			expected := make(map[string]*big.Rat)
			for _, spec := range specs {
				if expected[spec.AggregateKey] == nil {
					expected[spec.AggregateKey] = new(big.Rat)
				}
				expected[spec.AggregateKey].Add(expected[spec.AggregateKey], new(big.Rat).SetFloat64(spec.Metric))
			}
			for key, item := range splitResult.Keys {
				if item.Count == 0 {
					t.Fatalf("empty key %s", key)
				}
				actual := new(big.Rat).SetFloat64(item.Sum)
				diff := new(big.Rat).Sub(actual, expected[key])
				diff.Abs(diff)
				bound := new(big.Rat).SetFloat64(item.AbsErrorBound)
				if bound != nil && diff.Cmp(bound) > 0 {
					t.Fatalf("key %s error %s exceeds bound %s", key, diff.FloatString(30), bound.FloatString(30))
				}
			}
			ledger, err := service.GetErrorLedger(context.Background(), "metrics", "split", 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(ledger.Batches) != len(specs) {
				t.Fatalf("expected %d batch ledger entries, got %d", len(specs), len(ledger.Batches))
			}
			if ledger.LedgerAbsBound < splitResult.AbsErrorBound {
				t.Fatalf("ledger bound %.17g is below cumulative state bound %.17g", ledger.LedgerAbsBound, splitResult.AbsErrorBound)
			}
		})
	}
}

func TestRewindReplayAndAggregateRollbackMatchDirectConsumption(t *testing.T) {
	service := newTestService(t, time.Minute, nil)
	createTopic(t, service, "replay", 1,
		bus.GroupSpec{Name: "rewound", Algorithm: string(agg.Magnitude)},
		bus.GroupSpec{Name: "baseline", Algorithm: string(agg.Magnitude)})
	specs := sample.Generate(991, 15, 4)
	publishSpecs(t, service, "replay", specs)

	// The baseline follows the same event sequence with different ack boundaries.
	consumeN(t, service, "replay", "baseline", 0, 6, 1)
	baselineAtSix, err := service.GetAggregate(context.Background(), "replay", "baseline", 0)
	if err != nil {
		t.Fatal(err)
	}
	consumeN(t, service, "replay", "baseline", 0, len(specs)-6, 3)
	baselineFull, err := service.GetAggregate(context.Background(), "replay", "baseline", 0)
	if err != nil {
		t.Fatal(err)
	}

	consumeN(t, service, "replay", "rewound", 0, len(specs), 2)
	fullBefore, err := service.GetAggregate(context.Background(), "replay", "rewound", 0)
	if err != nil {
		t.Fatal(err)
	}
	if fullBefore.StateDigest != baselineFull.StateDigest {
		t.Fatalf("initial paths differ: %s vs %s", fullBefore.StateDigest, baselineFull.StateDigest)
	}
	for key, item := range fullBefore.Keys {
		if item.Sum != baselineFull.Keys[key].Sum || item.Count != baselineFull.Keys[key].Count {
			t.Fatalf("bitwise mismatch for key %s: %+v vs %+v", key, item, baselineFull.Keys[key])
		}
	}

	rewound, err := service.Rewind(context.Background(), bus.RewindRequest{
		RequestID: requestID("rewind"), Topic: "replay", Group: "rewound", Partition: 0, TargetSeq: 6,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rewound.OldOffset != int64(len(specs)) || rewound.NewOffset != 6 || rewound.ReplayUntil != int64(len(specs)) {
		t.Fatalf("unexpected rewind result: %+v", rewound)
	}
	afterRewind, err := service.GetAggregate(context.Background(), "replay", "rewound", 0)
	if err != nil {
		t.Fatal(err)
	}
	if afterRewind.StateDigest != baselineAtSix.StateDigest {
		t.Fatalf("rewind snapshot %s does not match direct consumption %s", afterRewind.StateDigest, baselineAtSix.StateDigest)
	}
	for key, item := range afterRewind.Keys {
		if item.Sum != baselineAtSix.Keys[key].Sum || item.Count != baselineAtSix.Keys[key].Count {
			t.Fatalf("rewind count/sum mismatch for key %s", key)
		}
	}
	ledger, err := service.GetErrorLedger(context.Background(), "replay", "rewound", 0)
	if err != nil {
		t.Fatal(err)
	}
	if ledger.RevertedBatchCount == 0 {
		t.Fatalf("expected reverted aggregate batches in ledger")
	}

	// New events are accepted but must not be dispatched until replay finishes.
	extra := sample.Generate(992, 1, 1)[0]
	extra.EventID = "evt-extra"
	extraSpecs := append(append([]sample.Spec(nil), specs...), extra)
	publishSpecs(t, service, "replay", []sample.Spec{extra})
	consumeN(t, service, "replay", "baseline", 0, 1, 1)
	replayPull, err := service.Pull(context.Background(), bus.PullRequest{
		RequestID: requestID("replay-pull"), Topic: "replay", Group: "rewound", Partition: 0,
		ConsumerID: "replay-consumer", MaxEvents: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !replayPull.Replay || replayPull.Events[0].Seq != 7 {
		t.Fatalf("expected replay from seq 7, got %+v", replayPull)
	}
	replayAck, err := service.Ack(context.Background(), bus.AckRequest{
		RequestID: requestID("replay-ack"), Topic: "replay", Group: "rewound", Partition: 0,
		ConsumerID: "replay-consumer", LeaseToken: replayPull.LeaseToken, Seqs: []int64{7},
		AckBatchID: requestID("replay-batch"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if replayAck.ReplayComplete {
		t.Fatalf("replay unexpectedly completed at seq 7")
	}

	consumeN(t, service, "replay", "rewound", 0, len(extraSpecs)-7, 2)
	afterReplay, err := service.GetAggregate(context.Background(), "replay", "rewound", 0)
	if err != nil {
		t.Fatal(err)
	}
	if afterReplay.Offset != int64(len(extraSpecs)) || afterReplay.ReplayActive {
		t.Fatalf("replay did not finish cleanly: %+v", afterReplay)
	}
	baselinePlusExtra, err := service.GetAggregate(context.Background(), "replay", "baseline", 0)
	if err != nil {
		t.Fatal(err)
	}
	if afterReplay.StateDigest != baselinePlusExtra.StateDigest {
		t.Fatalf("replayed digest %s differs from direct %s", afterReplay.StateDigest, baselinePlusExtra.StateDigest)
	}
	for key, item := range afterReplay.Keys {
		if item.Sum != baselinePlusExtra.Keys[key].Sum || item.Count != baselinePlusExtra.Keys[key].Count {
			t.Fatalf("post-replay mismatch for key %s", key)
		}
	}
}

func TestRewindValidationDoesNotMutate(t *testing.T) {
	service := newTestService(t, time.Minute, nil)
	createTopic(t, service, "validate", 1, bus.GroupSpec{Name: "g", Algorithm: string(agg.Kahan)})
	specs := sample.Generate(5, 5, 1)
	publishSpecs(t, service, "validate", specs)
	consumeN(t, service, "validate", "g", 0, 3, 1)
	before, err := service.GetAggregate(context.Background(), "validate", "g", 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Rewind(context.Background(), bus.RewindRequest{
		RequestID: requestID("bad-future"), Topic: "validate", Group: "g", Partition: 0, TargetSeq: 4,
	})
	mustCode(t, err, bus.CodeOffsetInvalid)
	after, err := service.GetAggregate(context.Background(), "validate", "g", 0)
	if err != nil {
		t.Fatal(err)
	}
	if before.StateDigest != after.StateDigest || before.Offset != after.Offset {
		t.Fatalf("failed rewind mutated state")
	}
	if _, err := service.SetRetention(context.Background(), bus.SetRetentionRequest{
		RequestID: requestID("retention"), Topic: "validate", Partition: 0, EarliestRetain: 3,
	}); err != nil {
		t.Fatal(err)
	}
	_, err = service.Rewind(context.Background(), bus.RewindRequest{
		RequestID: requestID("expired"), Topic: "validate", Group: "g", Partition: 0, TargetSeq: 2,
	})
	mustCode(t, err, bus.CodeOffsetExpired)
	afterExpired, err := service.GetAggregate(context.Background(), "validate", "g", 0)
	if err != nil {
		t.Fatal(err)
	}
	if afterExpired.StateDigest != before.StateDigest || afterExpired.Offset != before.Offset {
		t.Fatalf("expired rewind mutated aggregate or offset")
	}
}

func TestRewindInsideAckBatchRetainsLedgerPrefix(t *testing.T) {
	service := newTestService(t, time.Minute, nil)
	createTopic(t, service, "mid-batch", 1,
		bus.GroupSpec{Name: "rewound", Algorithm: string(agg.Magnitude)},
		bus.GroupSpec{Name: "baseline", Algorithm: string(agg.Magnitude)})
	publishSpecs(t, service, "mid-batch", sample.Generate(616, 8, 8))

	consumeN(t, service, "mid-batch", "baseline", 0, 4, 1)
	baselineTarget, err := service.GetAggregate(context.Background(), "mid-batch", "baseline", 0)
	if err != nil {
		t.Fatal(err)
	}
	consumeN(t, service, "mid-batch", "baseline", 0, 4, 2)
	baselineFull, err := service.GetAggregate(context.Background(), "mid-batch", "baseline", 0)
	if err != nil {
		t.Fatal(err)
	}
	consumeN(t, service, "mid-batch", "rewound", 0, 8, 8)
	before, err := service.GetAggregate(context.Background(), "mid-batch", "rewound", 0)
	if err != nil {
		t.Fatal(err)
	}
	if before.StateDigest != baselineFull.StateDigest {
		t.Fatalf("initial full states differ")
	}
	if _, err := service.Rewind(context.Background(), bus.RewindRequest{
		RequestID: requestID("rewind-mid"), Topic: "mid-batch", Group: "rewound", Partition: 0, TargetSeq: 4,
	}); err != nil {
		t.Fatal(err)
	}
	atTarget, err := service.GetAggregate(context.Background(), "mid-batch", "rewound", 0)
	if err != nil {
		t.Fatal(err)
	}
	if atTarget.StateDigest != baselineTarget.StateDigest {
		t.Fatalf("mid-batch rewind state differs from direct consumption")
	}
	ledger, err := service.GetErrorLedger(context.Background(), "mid-batch", "rewound", 0)
	if err != nil {
		t.Fatal(err)
	}
	if ledger.BatchCount != 1 || ledger.RevertedBatchCount != 1 {
		t.Fatalf("unexpected ledger batch counts: %+v", ledger)
	}
	var activeEvents int64
	for _, batch := range ledger.Batches {
		if batch.Active {
			activeEvents += batch.EventCount
		}
	}
	if activeEvents != 4 {
		t.Fatalf("active ledger does not account for retained prefix: %d", activeEvents)
	}
	consumeN(t, service, "mid-batch", "rewound", 0, 4, 2)
	afterReplay, err := service.GetAggregate(context.Background(), "mid-batch", "rewound", 0)
	if err != nil {
		t.Fatal(err)
	}
	if afterReplay.StateDigest != baselineFull.StateDigest {
		t.Fatalf("mid-batch replay state differs from direct consumption")
	}
}

func TestConcurrentDuplicateAckExecutesOnce(t *testing.T) {
	service := newTestService(t, time.Minute, nil)
	createTopic(t, service, "idem", 1, bus.GroupSpec{Name: "g", Algorithm: string(agg.Kahan)})
	publishSpecs(t, service, "idem", sample.Generate(17, 3, 1))
	pulled, err := service.Pull(context.Background(), bus.PullRequest{
		RequestID: requestID("pull"), Topic: "idem", Group: "g", Partition: 0,
		ConsumerID: "c", MaxEvents: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	ackRequest := bus.AckRequest{
		RequestID: "shared-ack", Topic: "idem", Group: "g", Partition: 0,
		ConsumerID: "c", LeaseToken: pulled.LeaseToken, Seqs: []int64{1, 2}, AckBatchID: "batch-shared",
	}
	type outcome struct {
		result bus.AckResult
		err    error
	}
	start := make(chan struct{})
	results := make(chan outcome, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			result, err := service.Ack(context.Background(), ackRequest)
			results <- outcome{result: result, err: err}
		}()
	}
	close(start)
	first, second := <-results, <-results
	if first.err != nil || second.err != nil {
		t.Fatalf("duplicate ack failed: %v / %v", first.err, second.err)
	}
	if first.result.Offset != 2 || second.result.Offset != 2 || first.result.Batch.ID != second.result.Batch.ID {
		t.Fatalf("duplicate ack results differ: %+v / %+v", first.result, second.result)
	}
	var ackCount, batchCount int
	if err := service.DB().QueryRow(`SELECT COUNT(*) FROM acks WHERE topic='idem' AND active=1`).Scan(&ackCount); err != nil {
		t.Fatal(err)
	}
	if err := service.DB().QueryRow(`SELECT COUNT(*) FROM agg_batches WHERE topic='idem'`).Scan(&batchCount); err != nil {
		t.Fatal(err)
	}
	if ackCount != 2 || batchCount != 1 {
		t.Fatalf("duplicate execution detected: acks=%d batches=%d", ackCount, batchCount)
	}
	conflicting := ackRequest
	conflicting.Seqs = []int64{1}
	_, err = service.Ack(context.Background(), conflicting)
	mustCode(t, err, bus.CodeConflictingRequest)
}

func TestRecoveryRollsBackUncommittedAckAndCommittedAckSurvives(t *testing.T) {
	t.Run("before_commit", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "crash-before.sqlite")
		clock := &testClock{t: sample.FixedClock()}
		service, err := bus.Open(context.Background(), bus.ServiceOptions{DBPath: path, LeaseDuration: time.Minute, Now: clock.Now})
		if err != nil {
			t.Fatal(err)
		}
		createTopic(t, service, "crash", 1, bus.GroupSpec{Name: "g", Algorithm: string(agg.Kahan)})
		publishSpecs(t, service, "crash", sample.Generate(33, 3, 1))
		pulled, err := service.Pull(context.Background(), bus.PullRequest{
			RequestID: requestID("pull"), Topic: "crash", Group: "g", Partition: 0, ConsumerID: "c", MaxEvents: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		service.SetFaultInjector(func(stage string) error {
			if stage == "ack_before_commit" {
				return bus.ErrSimulatedCrash
			}
			return nil
		})
		_, err = service.Ack(context.Background(), bus.AckRequest{
			RequestID: requestID("crash-ack"), Topic: "crash", Group: "g", Partition: 0,
			ConsumerID: "c", LeaseToken: pulled.LeaseToken, Seqs: []int64{1},
		})
		if !errors.Is(err, bus.ErrSimulatedCrash) {
			t.Fatalf("expected simulated crash, got %v", err)
		}
		if err := service.Close(); err != nil {
			t.Fatal(err)
		}

		restarted, err := bus.Open(context.Background(), bus.ServiceOptions{DBPath: path, LeaseDuration: time.Minute, Now: clock.Now})
		if err != nil {
			t.Fatal(err)
		}
		defer restarted.Close()
		report, err := restarted.LatestRecoveryReport(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if report.PendingScanned != 1 || report.RolledBack != 1 {
			t.Fatalf("unexpected recovery report: %+v", report)
		}
		offset, err := restarted.GetOffset(context.Background(), "crash", "g", 0)
		if err != nil {
			t.Fatal(err)
		}
		if offset.AckedOffset != 0 || offset.LeaseHeld {
			t.Fatalf("uncommitted ack or lease survived restart: %+v", offset)
		}
		redelivery, err := restarted.Pull(context.Background(), bus.PullRequest{
			RequestID: requestID("restart-pull"), Topic: "crash", Group: "g", Partition: 0, ConsumerID: "c2", MaxEvents: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		if redelivery.Events[0].Seq != 1 || redelivery.Events[0].DeliveryCount != 2 {
			t.Fatalf("uncommitted ack did not roll back to delivery: %+v", redelivery)
		}
	})

	t.Run("after_commit", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "crash-after.sqlite")
		clock := &testClock{t: sample.FixedClock()}
		service, err := bus.Open(context.Background(), bus.ServiceOptions{DBPath: path, LeaseDuration: time.Minute, Now: clock.Now})
		if err != nil {
			t.Fatal(err)
		}
		createTopic(t, service, "crash", 1, bus.GroupSpec{Name: "g", Algorithm: string(agg.Kahan)})
		publishSpecs(t, service, "crash", sample.Generate(34, 2, 1))
		pulled, err := service.Pull(context.Background(), bus.PullRequest{
			RequestID: requestID("pull"), Topic: "crash", Group: "g", Partition: 0, ConsumerID: "c", MaxEvents: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		service.SetFaultInjector(func(stage string) error {
			if stage == "ack_after_commit" {
				return bus.ErrSimulatedCrash
			}
			return nil
		})
		_, err = service.Ack(context.Background(), bus.AckRequest{
			RequestID: requestID("crash-ack"), Topic: "crash", Group: "g", Partition: 0,
			ConsumerID: "c", LeaseToken: pulled.LeaseToken, Seqs: []int64{1},
		})
		if !errors.Is(err, bus.ErrSimulatedCrash) {
			t.Fatalf("expected simulated crash, got %v", err)
		}
		service.Close()
		restarted, err := bus.Open(context.Background(), bus.ServiceOptions{DBPath: path, LeaseDuration: time.Minute, Now: clock.Now})
		if err != nil {
			t.Fatal(err)
		}
		defer restarted.Close()
		report, err := restarted.LatestRecoveryReport(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if report.PendingScanned != 1 || report.CommittedRecovered != 1 || report.RolledBack != 0 {
			t.Fatalf("unexpected recovery report: %+v", report)
		}
		offset, err := restarted.GetOffset(context.Background(), "crash", "g", 0)
		if err != nil {
			t.Fatal(err)
		}
		if offset.AckedOffset != 1 {
			t.Fatalf("committed ack was lost: %+v", offset)
		}
		aggregate, err := restarted.GetAggregate(context.Background(), "crash", "g", 0)
		if err != nil {
			t.Fatal(err)
		}
		if aggregate.EventCount != 1 {
			t.Fatalf("committed aggregate was lost: %+v", aggregate)
		}
	})
}

func TestCounterAndMetricReferenceSanity(t *testing.T) {
	// Keep floating-point test helpers honest: big.Rat must see finite samples.
	for _, spec := range sample.Generate(1, 1000, 20) {
		if math.IsNaN(spec.Metric) || math.IsInf(spec.Metric, 0) {
			t.Fatalf("sample generator emitted non-finite metric: %v", spec.Metric)
		}
	}
}

func TestInvalidTransitionLeavesPersistedStateUntouched(t *testing.T) {
	service := newTestService(t, time.Minute, nil)
	createTopic(t, service, "transition", 1, bus.GroupSpec{Name: "g", Algorithm: string(agg.Kahan)})
	publishSpecs(t, service, "transition", sample.Generate(19, 1, 1))
	pulled, err := service.Pull(context.Background(), bus.PullRequest{
		RequestID: requestID("pull"), Topic: "transition", Group: "g", Partition: 0,
		ConsumerID: "c", MaxEvents: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Force a state that cannot legally transition to ACKED while retaining the
	// lease, then verify the service rejects it without any side effects.
	if _, err := service.DB().Exec(`
		UPDATE event_states SET state = 'ACKED'
		WHERE topic='transition' AND group_name='g' AND partition=0 AND seq=1`); err != nil {
		t.Fatal(err)
	}
	_, err = service.Ack(context.Background(), bus.AckRequest{
		RequestID: requestID("invalid-transition"), Topic: "transition", Group: "g", Partition: 0,
		ConsumerID: "c", LeaseToken: pulled.LeaseToken, Seqs: []int64{1},
	})
	mustCode(t, err, bus.CodeInvalidTransition)
	state, err := service.GetEventState(context.Background(), "transition", "g", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if state.State != bus.StateAcked {
		t.Fatalf("state changed after failed transition: %+v", state)
	}
	offset, err := service.GetOffset(context.Background(), "transition", "g", 0)
	if err != nil {
		t.Fatal(err)
	}
	if offset.AckedOffset != 0 {
		t.Fatalf("offset changed after failed transition: %+v", offset)
	}
	var ackCount, batchCount int
	_ = service.DB().QueryRow(`SELECT COUNT(*) FROM acks WHERE topic='transition'`).Scan(&ackCount)
	_ = service.DB().QueryRow(`SELECT COUNT(*) FROM agg_batches WHERE topic='transition'`).Scan(&batchCount)
	if ackCount != 0 || batchCount != 0 {
		t.Fatalf("failed transition wrote ack/aggregate rows: %d/%d", ackCount, batchCount)
	}
}

func TestDeadLetterTransitionsAndAdvancesOffsetWithoutAggregation(t *testing.T) {
	service := newTestService(t, time.Minute, nil)
	createTopic(t, service, "deadletter", 1, bus.GroupSpec{Name: "g", Algorithm: string(agg.Kahan)})
	publishSpecs(t, service, "deadletter", sample.Generate(5150, 2, 1))
	pulled, err := service.Pull(context.Background(), bus.PullRequest{
		RequestID: requestID("dl-pull"), Topic: "deadletter", Group: "g", Partition: 0,
		ConsumerID: "c", MaxEvents: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	deadLettered, err := service.DeadLetter(context.Background(), bus.DeadLetterRequest{
		RequestID: requestID("dl"), Topic: "deadletter", Group: "g", Partition: 0,
		ConsumerID: "c", LeaseToken: pulled.LeaseToken, Seq: 1, Reason: "poison",
	})
	if err != nil {
		t.Fatal(err)
	}
	if deadLettered.State != bus.StateDeadLettered || deadLettered.Offset != 1 {
		t.Fatalf("unexpected dead-letter result: %+v", deadLettered)
	}
	state, err := service.GetEventState(context.Background(), "deadletter", "g", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if state.State != bus.StateDeadLettered {
		t.Fatalf("unexpected dead-letter state: %+v", state)
	}
	next, err := service.Pull(context.Background(), bus.PullRequest{
		RequestID: requestID("dl-next"), Topic: "deadletter", Group: "g", Partition: 0,
		ConsumerID: "c2", MaxEvents: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if next.Events[0].Seq != 2 {
		t.Fatalf("dead letter did not advance delivery to seq 2: %+v", next)
	}
	if _, err := service.Ack(context.Background(), bus.AckRequest{
		RequestID: requestID("dl-ack"), Topic: "deadletter", Group: "g", Partition: 0,
		ConsumerID: "c2", LeaseToken: next.LeaseToken, Seqs: []int64{2},
	}); err != nil {
		t.Fatal(err)
	}
	aggregate, err := service.GetAggregate(context.Background(), "deadletter", "g", 0)
	if err != nil {
		t.Fatal(err)
	}
	if aggregate.EventCount != 1 || aggregate.Offset != 2 {
		t.Fatalf("dead letter leaked into aggregation: %+v", aggregate)
	}
}

func TestAggregateOverflowAbortsAckWithoutPartialState(t *testing.T) {
	service := newTestService(t, time.Minute, nil)
	createTopic(t, service, "overflow", 1, bus.GroupSpec{Name: "g", Algorithm: string(agg.Naive)})
	specs := sample.Generate(818, 2, 1)
	specs[0].AggregateKey = "same"
	specs[0].Metric = math.MaxFloat64
	specs[1].AggregateKey = "same"
	specs[1].Metric = math.MaxFloat64
	publishSpecs(t, service, "overflow", specs)
	pulled, err := service.Pull(context.Background(), bus.PullRequest{
		RequestID: requestID("overflow-pull"), Topic: "overflow", Group: "g", Partition: 0,
		ConsumerID: "c", MaxEvents: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Ack(context.Background(), bus.AckRequest{
		RequestID: requestID("overflow-ack"), Topic: "overflow", Group: "g", Partition: 0,
		ConsumerID: "c", LeaseToken: pulled.LeaseToken, Seqs: []int64{1, 2},
	})
	mustCode(t, err, bus.CodeInvalidArgument)
	offset, err := service.GetOffset(context.Background(), "overflow", "g", 0)
	if err != nil {
		t.Fatal(err)
	}
	if offset.AckedOffset != 0 {
		t.Fatalf("overflow ack advanced offset: %+v", offset)
	}
	var ackCount, stateCount int
	_ = service.DB().QueryRow(`SELECT COUNT(*) FROM acks WHERE topic='overflow'`).Scan(&ackCount)
	_ = service.DB().QueryRow(`SELECT COUNT(*) FROM agg_states WHERE topic='overflow'`).Scan(&stateCount)
	if ackCount != 0 || stateCount != 0 {
		t.Fatalf("overflow ack left partial rows: acks=%d agg_states=%d", ackCount, stateCount)
	}
	state, err := service.GetEventState(context.Background(), "overflow", "g", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if state.State != bus.StateDispatched {
		t.Fatalf("overflow ack changed delivery state: %+v", state)
	}
}

func TestRequestIdempotencySurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart-idem.sqlite")
	clock := &testClock{t: sample.FixedClock()}
	service, err := bus.Open(context.Background(), bus.ServiceOptions{
		DBPath: path, LeaseDuration: time.Minute, Now: clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	createTopic(t, service, "restart-idem", 1, bus.GroupSpec{Name: "g", Algorithm: string(agg.Kahan)})
	spec := sample.Generate(919, 1, 1)[0]
	publishRequest := sample.ToPublishRequest("durable-publish", "restart-idem", spec)
	firstPublish, err := service.Publish(context.Background(), publishRequest)
	if err != nil {
		t.Fatal(err)
	}
	pulled, err := service.Pull(context.Background(), bus.PullRequest{
		RequestID: "durable-pull", Topic: "restart-idem", Group: "g", Partition: 0,
		ConsumerID: "c", MaxEvents: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ackRequest := bus.AckRequest{
		RequestID: "durable-ack", Topic: "restart-idem", Group: "g", Partition: 0,
		ConsumerID: "c", LeaseToken: pulled.LeaseToken, Seqs: []int64{1}, AckBatchID: "durable-batch",
	}
	firstAck, err := service.Ack(context.Background(), ackRequest)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := bus.Open(context.Background(), bus.ServiceOptions{
		DBPath: path, LeaseDuration: time.Minute, Now: clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	secondPublish, err := restarted.Publish(context.Background(), publishRequest)
	if err != nil {
		t.Fatal(err)
	}
	if secondPublish.Seq != firstPublish.Seq {
		t.Fatalf("durable publish idempotency changed seq: %+v vs %+v", secondPublish, firstPublish)
	}
	secondAck, err := restarted.Ack(context.Background(), ackRequest)
	if err != nil {
		t.Fatal(err)
	}
	if secondAck.Offset != firstAck.Offset || secondAck.Batch.ID != firstAck.Batch.ID || secondAck.StateDigest != firstAck.StateDigest {
		t.Fatalf("durable ack idempotency mismatch: %+v vs %+v", secondAck, firstAck)
	}
	var ackCount int
	if err := restarted.DB().QueryRow(`SELECT COUNT(*) FROM acks WHERE topic='restart-idem'`).Scan(&ackCount); err != nil {
		t.Fatal(err)
	}
	if ackCount != 1 {
		t.Fatalf("durable idempotency re-executed ack: %d", ackCount)
	}
}

func TestMultipleGroupsCanConcurrentlyHoldIndependentLeases(t *testing.T) {
	service := newTestService(t, time.Minute, nil)
	createTopic(t, service, "groups", 1,
		bus.GroupSpec{Name: "g1", Algorithm: string(agg.Naive)},
		bus.GroupSpec{Name: "g2", Algorithm: string(agg.Kahan)})
	publishSpecs(t, service, "groups", sample.Generate(21, 1, 1))
	type outcome struct {
		group  string
		result bus.PullResult
		err    error
	}
	start := make(chan struct{})
	results := make(chan outcome, 2)
	for _, group := range []string{"g1", "g2"} {
		go func(group string) {
			<-start
			result, err := service.Pull(context.Background(), bus.PullRequest{
				RequestID: requestID("group-pull"), Topic: "groups", Group: group, Partition: 0,
				ConsumerID: group + "-consumer", MaxEvents: 1,
			})
			results <- outcome{group: group, result: result, err: err}
		}(group)
	}
	close(start)
	leases := map[string]string{}
	for i := 0; i < 2; i++ {
		outcome := <-results
		if outcome.err != nil {
			t.Fatalf("group %s pull failed: %v", outcome.group, outcome.err)
		}
		leases[outcome.group] = outcome.result.LeaseToken
	}
	if leases["g1"] == "" || leases["g2"] == "" || leases["g1"] == leases["g2"] {
		t.Fatalf("groups did not receive independent leases: %+v", leases)
	}
}

func TestConcurrentReadersObserveWholeCommittedVersions(t *testing.T) {
	service := newTestService(t, time.Minute, nil)
	createTopic(t, service, "reads", 1, bus.GroupSpec{Name: "g", Algorithm: string(agg.Magnitude)})
	const eventCount = 120
	publishSpecs(t, service, "reads", sample.Generate(31337, eventCount, 5))

	done := make(chan struct{})
	errs := make(chan error, 8)
	var readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				result, err := service.GetAggregate(context.Background(), "reads", "g", 0)
				if err != nil {
					errs <- err
					return
				}
				if result.EventCount != result.Offset {
					errs <- fmt.Errorf("torn snapshot: event_count=%d offset=%d", result.EventCount, result.Offset)
					return
				}
				ledger, err := service.GetErrorLedger(context.Background(), "reads", "g", 0)
				if err != nil {
					errs <- err
					return
				}
				if ledger.Cumulative.EventCount != ledger.Cumulative.Offset {
					errs <- fmt.Errorf("torn ledger snapshot: event_count=%d offset=%d", ledger.Cumulative.EventCount, ledger.Cumulative.Offset)
					return
				}
				select {
				case <-done:
					return
				default:
				}
			}
		}()
	}

	consumeN(t, service, "reads", "g", 0, eventCount, 3)
	close(done)
	readers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}
