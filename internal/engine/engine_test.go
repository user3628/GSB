package engine

import (
	"context"
	"path/filepath"
	"testing"

	"configtree/internal/seed"
	"configtree/internal/spec"
)

func newTestEngine(t *testing.T) (*Engine, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pipeline.db")
	eng, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return eng, path
}

func commitSnapshot(t *testing.T, eng *Engine, id string, snap *spec.Snapshot) *commitResult {
	t.Helper()
	res, err := eng.Commit(context.Background(), &CommitRequest{
		CommitID: id, Kind: "snapshot", Snapshot: snap,
	})
	if err != nil {
		t.Fatalf("commit snapshot: %v", err)
	}
	return res
}

func TestPrefixAdjudicationAndConflicts(t *testing.T) {
	eng, _ := newTestEngine(t)
	commitSnapshot(t, eng, "s1", seed.V1())

	// auth subscribes service.http.tls (nested under gateway's service.http)
	// and service.db; it also depends on gateway. Nested prefixes must keep
	// parent keys out of the child prefix.
	res, err := eng.ReadView(context.Background(), "auth", 1)
	if err != nil {
		t.Fatalf("read auth: %v", err)
	}
	byKey := map[string]ViewEntry{}
	for _, e := range res.View.Entries {
		byKey[e.KeyPath] = e
	}
	tls, ok := byKey["service.http.tls.enabled"]
	if !ok {
		t.Fatal("tls entry missing from auth view")
	}
	if tls.Source.Prefix != "service.http.tls" || tls.Source.Hops != 1 {
		t.Fatalf("nested prefix match wrong: %+v", tls.Source)
	}
	if tls.Source.ComponentID != "auth" || tls.Source.Kind != "self" {
		t.Fatalf("tls entry should be self-sourced: %+v", tls.Source)
	}
	// service.http.host is only under the parent prefix owned by gateway, so
	// auth receives it through the upstream dependency edge.
	host, ok := byKey["service.http.host"]
	if !ok {
		t.Fatal("upstream http host missing")
	}
	if host.Source.Kind != "upstream" || host.Source.ComponentID != "gateway" {
		t.Fatalf("host should be upstream-sourced: %+v", host.Source)
	}
	// Wildcard component sees every key with hop distance per segment count.
	ops, err := eng.ReadView(context.Background(), "ops-wildcard", 1)
	if err != nil {
		t.Fatalf("read wildcard: %v", err)
	}
	if len(ops.View.Entries) != len(seed.V1().Items) {
		t.Fatalf("wildcard must see all items, got %d", len(ops.View.Entries))
	}
}

func TestStaleSnapshotAndSpecInvalid(t *testing.T) {
	eng, _ := newTestEngine(t)
	commitSnapshot(t, eng, "s1", seed.V1())

	_, err := eng.Commit(context.Background(), &CommitRequest{
		CommitID: "s-stale", Kind: "snapshot", Snapshot: seed.StaleSnapshot(),
	})
	assertCode(t, err, spec.CodeStaleSnapshot)

	_, err = eng.Commit(context.Background(), &CommitRequest{
		CommitID: "s-bad", Kind: "snapshot", Snapshot: seed.InvalidV1(),
	})
	assertCode(t, err, spec.CodeSpecInvalid)

	// Rejected batches must not mutate committed state.
	res, err := eng.ReadView(context.Background(), "gateway", 1)
	if err != nil || res.ViewVersion != 1 {
		t.Fatalf("state mutated by rejected batch: %v %v", res, err)
	}
}

func TestDeltaBaseAndConcurrentWinner(t *testing.T) {
	eng, _ := newTestEngine(t)
	commitSnapshot(t, eng, "s1", seed.V1())

	_, err := eng.Commit(context.Background(), &CommitRequest{
		CommitID: "d-wrong-base", Kind: "delta", Delta: seed.DeltaWrongBase(),
	})
	assertCode(t, err, spec.CodeStaleBase)

	first, err := eng.Commit(context.Background(), &CommitRequest{
		CommitID: "d-v2-a", Kind: "delta", Delta: seed.DeltaV2(),
	})
	if err != nil {
		t.Fatalf("first delta: %v", err)
	}
	if first.Version != 2 {
		t.Fatalf("want version 2, got %d", first.Version)
	}

	// The second delta still claims base version 1, which is no longer current.
	_, err = eng.Commit(context.Background(), &CommitRequest{
		CommitID: "d-v2-b", Kind: "delta", Delta: seed.DeltaV2Concurrent(),
	})
	assertCode(t, err, spec.CodeStaleBase)
}

func TestIdempotencyAndConflictingRequest(t *testing.T) {
	eng, _ := newTestEngine(t)
	commitSnapshot(t, eng, "s1", seed.V1())

	first, err := eng.Commit(context.Background(), &CommitRequest{
		CommitID: "dup-delta", Kind: "delta", Delta: seed.DeltaV2(),
	})
	if err != nil {
		t.Fatalf("delta: %v", err)
	}
	again, err := eng.Commit(context.Background(), &CommitRequest{
		CommitID: "dup-delta", Kind: "delta", Delta: seed.DeltaV2(),
	})
	if err != nil {
		t.Fatalf("retry should reuse result: %v", err)
	}
	if !again.Reused || again.Version != first.Version {
		t.Fatalf("idempotent reuse broken: %+v", again)
	}

	// Same commit id, different body.
	different := seed.DeltaV2Concurrent()
	_, err = eng.Commit(context.Background(), &CommitRequest{
		CommitID: "dup-delta", Kind: "delta", Delta: different,
	})
	assertCode(t, err, spec.CodeConflictingRequest)
}

func TestAffectedSetPropagation(t *testing.T) {
	eng, _ := newTestEngine(t)
	commitSnapshot(t, eng, "s1", seed.V1())
	dbOnly := &spec.Delta{
		BaseVersion:   1,
		TargetVersion: 2,
		Items: []spec.ItemChange{
			{Op: "upsert", Item: spec.ConfigItem{
				KeyPath: "service.http.host", Type: "string",
				Value: []byte(`"edge.local"`), Order: 50,
			}},
		},
	}
	res, err := eng.Commit(context.Background(), &CommitRequest{
		CommitID: "d-db", Kind: "delta", Delta: dbOnly,
	})
	if err != nil {
		t.Fatalf("delta: %v", err)
	}
	set := res.Affected
	hit := map[string]AffectedNode{}
	for _, n := range set.Nodes {
		hit[n.ComponentID] = n
	}
	worker, ok := hit["worker"]
	if !ok {
		t.Fatal("worker must be in affected set")
	}
	if worker.Direct || worker.PropHops == 0 {
		t.Fatalf("worker is a downstream component: %+v", worker)
	}
	if worker.PropHops != 2 || len(worker.PropPath) != 3 ||
		worker.PropPath[0] != "gateway" || worker.PropPath[2] != "worker" {
		t.Fatalf("propagation path wrong: %+v", worker.PropPath)
	}
	gateway := hit["gateway"]
	if !gateway.Direct {
		t.Fatalf("wildcard gateway must be directly hit: %+v", gateway)
	}
}

func TestCascadeRedirectAndAtomicRebuild(t *testing.T) {
	eng, _ := newTestEngine(t)
	commitSnapshot(t, eng, "s1", seed.V1())
	if _, err := eng.Commit(context.Background(), &CommitRequest{
		CommitID: "d-v2", Kind: "delta", Delta: seed.DeltaV2(),
	}); err != nil {
		t.Fatalf("delta v2: %v", err)
	}
	if _, err := eng.Commit(context.Background(), &CommitRequest{
		CommitID: "d-v3", Kind: "delta", Delta: seed.DeltaV3Redirect(),
	}); err != nil {
		t.Fatalf("redirect delta: %v", err)
	}
	worker, err := eng.ReadView(context.Background(), "worker", 3)
	if err != nil {
		t.Fatalf("read worker: %v", err)
	}
	// Dependency edge was re-pointed to gateway; old auth upstream sourcing
	// must be gone from the rebuilt view.
	for _, entry := range worker.View.Entries {
		if entry.Source.Kind == "upstream" && entry.Source.ComponentID == "auth" {
			t.Fatalf("stale dependency edge survived rebuild: %+v", entry)
		}
	}
	// view_refs must describe the new edge, not the old one.
	var count int
	if err := eng.db.QueryRow(`
		SELECT COUNT(*) FROM view_refs
		WHERE component_id='worker' AND ref_key='worker->auth'`).Scan(&count); err != nil {
		t.Fatalf("view_refs query: %v", err)
	}
	if count != 0 {
		t.Fatal("old dependency edge still referenced after cascade rebuild")
	}
}

func TestStaleReadAfterHardKillAndRecovery(t *testing.T) {
	eng, path := newTestEngine(t)
	commitSnapshot(t, eng, "s1", seed.V1())
	eng.SetTestSkipExit(true)
	eng.SetCrashHook(func(phase string) bool {
		return phase == "after_facts_before_derived"
	})
	if _, err := eng.Commit(context.Background(), &CommitRequest{
		CommitID: "d-v2-kill", Kind: "delta", Delta: seed.DeltaV2(),
	}); err == nil {
		t.Fatal("simulated hard kill should surface an error in test mode")
	}
	_ = eng.Close()

	inspector, err := InspectWithoutRecovery(path)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	_, err = inspector.ReadView(context.Background(), "worker", 2)
	assertCode(t, err, spec.CodeStaleRead)
	_ = inspector.Close()

	recovered, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer recovered.Close()
	report := recovered.LastRecoveryReport()
	if report == nil || report.LastGoodVersion != 1 {
		t.Fatalf("recovery report wrong: %+v", report)
	}
	if len(report.AbortedIntents) != 1 {
		t.Fatalf("want one aborted intent, got %v", report.AbortedIntents)
	}
	res, err := recovered.ReadView(context.Background(), "worker", 1)
	if err != nil {
		t.Fatalf("rolled-back read should succeed: %v", err)
	}
	if res.ViewVersion != 1 || !res.Converged {
		t.Fatalf("recovery state wrong: %+v", res)
	}
	// The recovered view still reports version 1. A reader presenting the
	// never-converged observed version 2 must not be silently downgraded:
	// the aborted intent is surfaced through the recovery report and commits.
	commits, err := recovered.Commits(context.Background())
	if err != nil {
		t.Fatalf("commits: %v", err)
	}
	foundAborted := false
	for _, c := range commits {
		if c["status"] == "aborted" && c["commit_id"] == "d-v2-kill" {
			foundAborted = true
		}
	}
	if !foundAborted {
		t.Fatalf("aborted commit not recorded: %+v", commits)
	}
}

func assertCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s, got nil", code)
	}
	biz, ok := err.(*spec.Error)
	if !ok || biz.Code != code {
		t.Fatalf("expected %s, got %v", code, err)
	}
}

func TestConcurrentSingleWriterSerializes(t *testing.T) {
	eng, _ := newTestEngine(t)
	commitSnapshot(t, eng, "s1", seed.V1())

	const n = 5
	errs := make(chan error, n)
	results := make(chan int64, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			d := &spec.Delta{
				BaseVersion:   int64(i + 1),
				TargetVersion: int64(i + 2),
				Items: []spec.ItemChange{{
					Op: "upsert",
					Item: spec.ConfigItem{
						KeyPath: "service.cache.ttl", Type: "int",
						Value: []byte("100"), Order: int64(100 + i),
					},
				}},
			}
			res, err := eng.Commit(context.Background(), &CommitRequest{
				CommitID: "concurrent-" + string(rune('a'+i)),
				Kind:     "delta",
				Delta:    d,
			})
			if err != nil {
				errs <- err
				return
			}
			results <- res.Version
		}(i)
	}
	successes, stale := 0, 0
	for i := 0; i < n; i++ {
		select {
		case <-results:
			successes++
		case err := <-errs:
			if biz, ok := err.(*spec.Error); ok && biz.Code == spec.CodeStaleBase {
				stale++
			} else {
				t.Fatalf("unexpected error: %v", err)
			}
		}
	}
	if successes+stale != n {
		t.Fatalf("all goroutines must finish: successes=%d stale=%d", successes, stale)
	}
	// Every goroutine claims a distinct base version. Under single-writer
	// serialization they either form a strictly serial commit chain or get
	// rejected as stale; the log must never skip or duplicate a version.
	if successes+stale != n || successes == 0 {
		t.Fatalf("single-writer expectation violated: successes=%d stale=%d", successes, stale)
	}
	current, err := eng.currentVersion(context.Background())
	if err != nil {
		t.Fatalf("current version: %v", err)
	}
	if current != int64(1+successes) {
		t.Fatalf("version chain broken: current=%d successes=%d", current, successes)
	}
}

func TestCrashOnFirstSnapshot(t *testing.T) {
	path := t.TempDir() + "/first.db"
	eng, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	eng.SetTestSkipExit(true)
	eng.SetCrashHook(func(phase string) bool { return phase == "after_facts_before_derived" })
	if _, err := eng.Commit(context.Background(), &CommitRequest{
		CommitID: "first", Kind: "snapshot", Snapshot: seed.V1(),
	}); err == nil {
		t.Fatal("expected simulated kill")
	}
	_ = eng.Close()

	eng2, err := Open(path)
	if err != nil {
		t.Fatalf("recover empty lineage: %v", err)
	}
	defer eng2.Close()
	r := eng2.LastRecoveryReport()
	if r == nil || r.LastGoodVersion != 0 || len(r.AbortedIntents) != 1 {
		t.Fatalf("bad report: %+v", r)
	}
}

func TestInvalidDeltaRejectedWholesale(t *testing.T) {
	eng, _ := newTestEngine(t)
	commitSnapshot(t, eng, "s1", seed.V1())
	bad := &spec.Delta{
		BaseVersion:   1,
		TargetVersion: 2,
		Subscriptions: []spec.SubscriptionChange{{
			Op:        "replace",
			Component: spec.Component{ID: "ghost", SubPrefixes: []string{"a.b"}},
		}},
	}
	_, err := eng.Commit(context.Background(), &CommitRequest{
		CommitID: "bad-delta", Kind: "delta", Delta: bad,
	})
	assertCode(t, err, spec.CodeSpecInvalid)
	v, err := eng.currentVersion(context.Background())
	if err != nil || v != 1 {
		t.Fatalf("state changed after rejected delta: v=%d err=%v", v, err)
	}
}

func TestOrderBeatsSourcePriority(t *testing.T) {
	// Effective order number is the first adjudication criterion, ahead of
	// the explicit source priority. Validate the comparator directly.
	winner := Source{Kind: "upstream", ComponentID: "up", Order: 99, Priority: 50}
	loser := Source{Kind: "self", ComponentID: "down", Order: 10, Priority: 100}
	ok, resolution := beats(winner, loser)
	if !ok || resolution != "order" {
		t.Fatalf("effective order must win first: ok=%v resolution=%s", ok, resolution)
	}
}
