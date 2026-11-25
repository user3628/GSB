package edgeconfig

import (
	"context"
	"sync"
	"testing"
	"time"
)

func newTestService(t *testing.T) *Service {
	t.Helper()
	store, err := OpenStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return NewService(store)
}

func commitTestTopologyAndSnapshot(t *testing.T, service *Service) (*Topology, *CommitResult) {
	t.Helper()
	ctx := context.Background()
	policy := DefaultPolicy()
	topology := Topology{
		BatchID: "test-topology",
		Keys: []ConfigKey{
			{Path: "feature.flag", Type: "bool", Value: false, OrderNo: 10},
			{Path: "rate.limit", Type: "number", Value: 100, OrderNo: 20},
		},
		Nodes: []Node{
			{ID: "a", Region: "east", SubscribedKeys: []string{"feature.flag", "rate.limit"}, Endpoint: "http://127.0.0.1:19001/applied"},
			{ID: "b", Region: "west", SubscribedKeys: []string{"feature.flag"}, Endpoint: "http://127.0.0.1:19002/applied", DependsOn: []string{"a"}},
		},
		Policy: policy,
	}
	if _, err := service.SaveTopology(ctx, topology); err != nil {
		t.Fatal(err)
	}
	snapshot := Snapshot{CommitID: "snap-1", Version: 1, GeneratedAtMS: 123, Keys: topology.Keys}
	digest, err := SnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Digest = digest
	result, err := service.SubmitSnapshot(ctx, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return &topology, result
}

func TestIncrementStaleReadAndConvergedReport(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	_, snap := commitTestTopologyAndSnapshot(t, service)

	increment := Increment{
		CommitID:    "inc-1",
		BaseVersion: 1,
		Changes:     []ConfigKey{{Path: "feature.flag", Type: "bool", Value: true, OrderNo: 10}},
	}
	committed, err := service.SubmitIncrement(ctx, increment)
	if err != nil {
		t.Fatal(err)
	}
	if committed.Version != 2 || committed.Affected == nil || len(committed.Affected.Nodes) != 2 {
		t.Fatalf("commit=%+v", committed)
	}

	view, ok := viewByID(committed.Views, "b")
	if !ok {
		t.Fatal("missing b view")
	}
	staleV1, ok := viewByID(snap.Views, "b")
	if !ok {
		t.Fatal("missing v1 b view")
	}
	_, err = service.SubmitReport(ctx, Report{
		ReportID:        "stale-1",
		NodeID:          "b",
		AppliedDigest:   staleV1.ExpectedDigest,
		ObservedVersion: 1,
		EffectiveAtMS:   1,
	})
	if err == nil || AsAPIError(err).Code != CodeStaleRead {
		t.Fatalf("expected stale read, got %v", err)
	}

	decision, err := service.SubmitReport(ctx, Report{
		ReportID:        "fresh-1",
		NodeID:          "b",
		AppliedDigest:   view.ExpectedDigest,
		ObservedVersion: view.ExpectedVersion,
		EffectiveAtMS:   2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !decision.Converged || decision.UsedVersion != 2 {
		t.Fatalf("decision=%+v", decision)
	}
}

func TestConcurrentSameCommitExecutesOnceAndConflict(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	commitTestTopologyAndSnapshot(t, service)

	increment := Increment{
		CommitID:    "same-inc",
		BaseVersion: 1,
		Changes:     []ConfigKey{{Path: "rate.limit", Type: "number", Value: 200, OrderNo: 20}},
	}
	const count = 12
	var wg sync.WaitGroup
	results := make([]*CommitResult, count)
	errs := make([]error, count)
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(index int) {
			defer wg.Done()
			results[index], errs[index] = service.SubmitIncrement(ctx, increment)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if results[i].Version != 2 {
			t.Fatalf("request %d version=%d", i, results[i].Version)
		}
	}
	var commitCount int
	if err := service.db.QueryRow(`SELECT COUNT(*) FROM increments WHERE commit_id=?`, increment.CommitID).Scan(&commitCount); err != nil {
		t.Fatal(err)
	}
	if commitCount != 1 {
		t.Fatalf("commit rows=%d, want 1", commitCount)
	}

	conflicting := increment
	conflicting.Reason = "different content with same commit id"
	_, err := service.SubmitIncrement(ctx, conflicting)
	if err == nil || AsAPIError(err).Code != CodeConflictRequest {
		t.Fatalf("expected conflict, got %v", err)
	}
}

func TestRecoveryMarksPendingIntentAborted(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	commitTestTopologyAndSnapshot(t, service)
	intent, err := service.CreatePendingIntent(ctx, "probe_round", "probe-crash-1", 1, map[string]string{"round": "r1"})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := service.ListIntents(ctx, IntentPending)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	recovery, err := service.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.LastCompleteVersion != 1 || len(recovery.AbortedIntentIDs) != 1 {
		t.Fatalf("recovery=%+v", recovery)
	}
	if recovery.AbortedIntentIDs[0] != intent.IntentID {
		t.Fatalf("aborted=%v want %s", recovery.AbortedIntentIDs, intent.IntentID)
	}
}

func viewByID(views []ExpectedView, id string) (ExpectedView, bool) {
	for _, view := range views {
		if view.NodeID == id {
			return view, true
		}
	}
	return ExpectedView{}, false
}

func TestProbeTimeoutReleasesConcurrencySlots(t *testing.T) {
	service := newTestService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mocks := make([]*MockNode, 4)
	servers := make([]*MockServer, 4)
	nodes := make([]Node, 4)
	for i := 0; i < 4; i++ {
		mock := NewMockNode("n"+string(rune('a'+i)), "", 1)
		mock.SetHang(2 * time.Second)
		server, err := StartFreeMockServer(mock)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = server.Close(context.Background()) })
		mocks[i] = mock
		servers[i] = server
		nodes[i] = Node{ID: mock.NodeID, Region: "r", SubscribedKeys: []string{"k"}, Endpoint: server.URL}
	}
	topology := Topology{
		Keys:   []ConfigKey{{Path: "k", Type: "string", Value: "v"}},
		Nodes:  nodes,
		Policy: Policy{Probe: ProbePolicy{TimeoutMS: 80, Concurrency: 2}, Numeric: DefaultPolicy().Numeric},
	}
	if _, err := service.SaveTopology(ctx, topology); err != nil {
		t.Fatal(err)
	}
	snapshot := Snapshot{CommitID: "snap-probe", Version: 1, GeneratedAtMS: 1, Keys: topology.Keys}
	digest, _ := SnapshotDigest(snapshot)
	snapshot.Digest = digest
	if _, err := service.SubmitSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	round, err := service.RunProbeRound(ctx, ProbeRequest{RoundID: "r-timeout", RequestID: "req-timeout", TimeoutMS: 120, Concurrency: 2})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if len(round.Outcomes) != 4 || round.TimeoutCount != 4 {
		t.Fatalf("round=%+v", round)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("round took %s; slots likely were not released", elapsed)
	}
	for i, mock := range mocks {
		if !waitForTestCondition(time.Second, 5*time.Millisecond, func() bool {
			_, active, _ := mock.Stats()
			return active == 0
		}) {
			t.Fatalf("mock %d still has active handler", i)
		}
	}
}

func waitForTestCondition(timeout, interval time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(interval)
	}
	return fn()
}
