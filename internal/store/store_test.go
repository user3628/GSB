package store

import (
	"context"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"example.com/gsb-router/internal/errs"
	"example.com/gsb-router/internal/model"
	"example.com/gsb-router/internal/routing"
	"example.com/gsb-router/internal/sample"
)

func testStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "router.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, path
}

func commitV1(t *testing.T, store *Store) CommitResult {
	t.Helper()
	result, err := store.CommitSnapshot(context.Background(), SnapshotCommitRequest{Snapshot: sample.SnapshotV1()})
	if err != nil {
		t.Fatalf("commit snapshot v1: %v", err)
	}
	return result
}

func route(t *testing.T, store *Store, tableID string, observed int64, kind string, value string) (RouteResponse, error) {
	t.Helper()
	query := routing.Query{TableID: tableID, Observed: observed, Kind: kind}
	if kind == routing.KindEqual {
		query.Value = &value
	}
	return store.Route(query)
}

func assertBiz(t *testing.T, err error, code string) *errs.Error {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s, got nil", code)
	}
	biz := errs.As(err)
	if biz == nil {
		t.Fatalf("expected business error, got %T: %v", err, err)
	}
	if biz.Code != code {
		t.Fatalf("expected %s, got %s: %v", code, biz.Code, err)
	}
	return biz
}

func TestSnapshotCommitAndVersionBoundRead(t *testing.T) {
	store, _ := testStore(t)
	result := commitV1(t, store)
	if result.Version != 1 || store.CurrentVersion() != 1 {
		t.Fatalf("unexpected v1 result: %#v", result)
	}
	response, err := route(t, store, "orders", 1, routing.KindEqual, "5")
	if err != nil {
		t.Fatal(err)
	}
	if response.CacheVersion != 1 || response.UsedVersion != 1 || !response.VersionBound {
		t.Fatalf("bad version annotation: %#v", response)
	}
	if len(response.PhysicalTables) != 1 || response.PhysicalTables[0] != "db0.orders_01" {
		t.Fatalf("bad route: %#v", response.PhysicalTables)
	}
	if _, err := route(t, store, "orders", 2, routing.KindEqual, "5"); err == nil {
		t.Fatal("expected observed-ahead rejection")
	} else {
		assertBiz(t, err, errs.CodeObservedAhead)
	}
}

func TestInvalidSnapshotLeavesOldStateUsable(t *testing.T) {
	store, _ := testStore(t)
	commitV1(t, store)
	_, err := store.CommitSnapshot(context.Background(), SnapshotCommitRequest{Snapshot: sample.InvalidMappingSample()})
	assertBiz(t, err, errs.CodePhysicalTableMiss)
	_, err = store.CommitSnapshot(context.Background(), SnapshotCommitRequest{Snapshot: sample.InvalidOverlapSample()})
	assertBiz(t, err, errs.CodeRangeOverlap)
	bad := sample.SnapshotV1()
	bad.Version = 2
	bad.Checksum = "bad"
	_, err = store.CommitSnapshot(context.Background(), SnapshotCommitRequest{Snapshot: bad})
	assertBiz(t, err, errs.CodeSnapshotInvalid)
	if store.CurrentVersion() != 1 {
		t.Fatalf("invalid commits changed version to %d", store.CurrentVersion())
	}
	response, err := route(t, store, "orders", 1, routing.KindEqual, "1")
	if err != nil {
		t.Fatalf("old snapshot not usable: %v", err)
	}
	if response.PhysicalTables[0] != "db0.orders_01" {
		t.Fatalf("old routing changed: %#v", response.PhysicalTables)
	}
}

func TestIncrementStaleReadAndReplacement(t *testing.T) {
	store, _ := testStore(t)
	commitV1(t, store)
	inc := sample.IncrementV2()
	result, err := store.CommitIncrement(context.Background(), inc)
	if err != nil {
		t.Fatalf("increment v2: %v", err)
	}
	if result.Version != 2 || store.CurrentVersion() != 2 {
		t.Fatalf("unexpected increment result: %#v", result)
	}
	// Slot 1 moved from orders_01 to orders_00 in v2.
	current, err := route(t, store, "orders", 2, routing.KindEqual, "1")
	if err != nil {
		t.Fatal(err)
	}
	if len(current.PhysicalTables) != 1 || current.PhysicalTables[0] != "db0.orders_00" {
		t.Fatalf("v2 routing wrong: %#v", current.PhysicalTables)
	}
	_, err = route(t, store, "orders", 1, routing.KindEqual, "1")
	biz := assertBiz(t, err, errs.CodeStaleRead)
	if biz.Context["cache_version"] != int64(2) || biz.Context["observed_version"] != int64(1) {
		t.Fatalf("stale-read context missing facts: %#v", biz.Context)
	}
}

func TestIncrementIdempotencyAndConflict(t *testing.T) {
	store, _ := testStore(t)
	commitV1(t, store)
	inc := sample.IncrementV2()
	first, err := store.CommitIncrement(context.Background(), inc)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.CommitIncrement(context.Background(), inc)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Reused || second.Version != first.Version || store.CurrentVersion() != 2 {
		t.Fatalf("idempotent replay failed: first=%#v second=%#v", first, second)
	}
	conflicting := inc
	conflicting.GeneratedAt = "2026-01-02T00:00:01Z"
	_, err = store.CommitIncrement(context.Background(), conflicting)
	assertBiz(t, err, errs.CodeConflictingRequest)
	if store.CurrentVersion() != 2 {
		t.Fatalf("conflict changed version: %d", store.CurrentVersion())
	}
}

func TestConcurrentSameBaseOnlyOneWins(t *testing.T) {
	store, _ := testStore(t)
	commitV1(t, store)
	incA := sample.IncrementV2()
	incA.CommitID = "concurrent-a"
	incA.Checksum = ""
	incA.Checksum, _ = model.IncrementChecksum(incA)
	incB := incA
	incB.CommitID = "concurrent-b"
	incB.Changes = append([]model.TableChange(nil), incA.Changes...)
	incB.Changes[0].Ranges = append([]model.ShardRange(nil), incA.Changes[0].Ranges...)
	incB.Changes[0].Ranges[0].Weight = 3
	incB.Checksum = ""
	incB.Checksum, _ = model.IncrementChecksum(incB)

	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, inc := range []model.Increment{incA, incB} {
		inc := inc
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := store.CommitIncrement(context.Background(), inc)
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	success, stale := 0, 0
	for err := range results {
		if err == nil {
			success++
			continue
		}
		if biz := errs.As(err); biz != nil && biz.Code == errs.CodeStaleBase {
			stale++
			continue
		}
		t.Fatalf("unexpected concurrent error: %v", err)
	}
	if success != 1 || stale != 1 {
		t.Fatalf("expected one success and one stale base, got success=%d stale=%d", success, stale)
	}
	if store.CurrentVersion() != 2 {
		t.Fatalf("expected exactly one committed increment, got version %d", store.CurrentVersion())
	}
}

func TestIncrementBatchRollsBackAtomically(t *testing.T) {
	store, _ := testStore(t)
	commitV1(t, store)
	inc := sample.IncrementV2()
	inc.CommitID = "batch-invalid"
	inc.Changes = append(inc.Changes, model.TableChange{
		Op:      model.ChangeReplaceRanges,
		TableID: "ledger",
		Ranges: []model.ShardRange{
			{ID: "a", Lower: 0, Upper: 1000, LowerInclusive: true, UpperInclusive: true, PhysicalTable: "db2.ledger_00", Weight: 1},
			{ID: "b", Lower: 1000, Upper: 2000, LowerInclusive: true, UpperInclusive: true, PhysicalTable: "db2.ledger_01", Weight: 1},
		},
	})
	inc.Checksum = ""
	_, err := store.CommitIncrement(context.Background(), inc)
	assertBiz(t, err, errs.CodeRangeOverlap)
	if store.CurrentVersion() != 1 {
		t.Fatalf("invalid batch changed snapshot version: %d", store.CurrentVersion())
	}
	ordersCache := cacheFor(t, store.RouteCaches(), "orders")
	if ordersCache.CacheVersion != 1 {
		t.Fatalf("orders cache changed on failed batch: %#v", ordersCache)
	}
	snapshots, err := store.Snapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("failed batch wrote snapshot rows: %#v", snapshots)
	}
	response, err := route(t, store, "orders", 1, routing.KindEqual, "1")
	if err != nil {
		t.Fatal(err)
	}
	if response.PhysicalTables[0] != "db0.orders_01" {
		t.Fatalf("failed batch changed routes: %#v", response.PhysicalTables)
	}
}

func TestCacheCascadeIsAtomic(t *testing.T) {
	store, _ := testStore(t)
	commitV1(t, store)
	_, err := store.CommitIncrement(context.Background(), sample.IncrementV2())
	if err != nil {
		t.Fatal(err)
	}
	caches := cacheMap(store.RouteCaches())
	orders := caches["orders"]
	items := caches["order_items"]
	events := caches["events"]
	if orders.CacheVersion != 2 || orders.ExpectedVersion != 2 {
		t.Fatalf("orders cache not rebuilt: %#v", orders)
	}
	if items.CacheVersion != 1 || events.CacheVersion != 1 {
		t.Fatalf("unaffected caches should retain their versions: items=%#v events=%#v", items, events)
	}
	if len(orders.PhysicalTables) != 3 {
		t.Fatalf("orders physical table set not cascaded: %#v", orders.PhysicalTables)
	}
	aggregates := aggregateMap(store.AggregateCaches())
	if aggregates["order_overview"].CacheVersion != 2 {
		t.Fatalf("dependent aggregate not rebuilt: %#v", aggregates["order_overview"])
	}
	if aggregates["audit_rollup"].CacheVersion != 1 {
		t.Fatalf("unrelated aggregate rebuilt unexpectedly: %#v", aggregates["audit_rollup"])
	}
	if !store.StateSummary().CacheAligned {
		t.Fatal("summary reports unaligned caches")
	}
}

func TestPropagationLagReturnsStaleRead(t *testing.T) {
	store, _ := testStore(t)
	commitV1(t, store)
	if _, err := store.CommitIncrement(context.Background(), sample.IncrementV2()); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	cache := store.state.caches["orders"]
	cache.ExpectedVersion = 3
	store.state.caches["orders"] = cache
	store.mu.Unlock()
	_, err := route(t, store, "orders", 2, routing.KindEqual, "1")
	biz := assertBiz(t, err, errs.CodeStaleRead)
	if biz.Context["reason"] != "cache_propagation_incomplete" {
		t.Fatalf("unexpected stale reason: %#v", biz.Context)
	}
	if store.StateSummary().CacheAligned {
		t.Fatal("summary should expose propagation lag as unaligned")
	}
}

func TestUnaffectedTableServesOldCacheVersionAtCurrentVersion(t *testing.T) {
	store, _ := testStore(t)
	commitV1(t, store)
	if _, err := store.CommitIncrement(context.Background(), sample.IncrementV2()); err != nil {
		t.Fatal(err)
	}
	response, err := route(t, store, "events", 2, routing.KindEqual, "tenant-42")
	if err != nil {
		t.Fatalf("unaffected table should remain readable: %v", err)
	}
	if response.CurrentVersion != 2 || response.CacheVersion != 1 || response.UsedVersion != 1 {
		t.Fatalf("unexpected version annotation: %#v", response)
	}
	if len(response.PhysicalTables) != 1 {
		t.Fatalf("unexpected route: %#v", response.PhysicalTables)
	}
}

func TestConcurrentSameCommitIDReusesResult(t *testing.T) {
	store, _ := testStore(t)
	commitV1(t, store)
	inc := sample.IncrementV2()
	inc.CommitID = "same-id-concurrent"
	inc.Checksum = ""
	inc.Checksum, _ = model.IncrementChecksum(inc)
	start := make(chan struct{})
	results := make(chan CommitResult, 2)
	errsCh := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := store.CommitIncrement(context.Background(), inc)
			if err != nil {
				errsCh <- err
				return
			}
			results <- result
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errsCh)
	for err := range errsCh {
		t.Fatalf("same-id replay failed: %v", err)
	}
	got := []CommitResult{}
	for result := range results {
		got = append(got, result)
	}
	if len(got) != 2 || got[0].Version != 2 || got[1].Version != 2 {
		t.Fatalf("unexpected replay results: %#v", got)
	}
	reused := 0
	for _, result := range got {
		if result.Reused {
			reused++
		}
	}
	if reused != 1 {
		t.Fatalf("expected exactly one reused receipt, got %#v", got)
	}
	commits, err := store.Commits(context.Background(), 20)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, commit := range commits {
		if commit.CommitID == "same-id-concurrent" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("commit executed more than once: %#v", commits)
	}
}

func TestConcurrentReadsNeverObservePartialCommit(t *testing.T) {
	store, _ := testStore(t)
	commitV1(t, store)
	stop := make(chan struct{})
	failures := make(chan string, 20)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				response, err := route(t, store, "orders", 1, routing.KindEqual, "1")
				if err == nil {
					if response.CurrentVersion != 1 || response.CacheVersion != 1 ||
						len(response.PhysicalTables) != 1 || response.PhysicalTables[0] != "db0.orders_01" {
						select {
						case failures <- "mixed successful state":
						default:
						}
						return
					}
					continue
				}
				biz := errs.As(err)
				if biz == nil || biz.Code != errs.CodeStaleRead || biz.Context["current_version"] != int64(2) {
					select {
					case failures <- "unexpected read error: " + err.Error():
					default:
					}
					return
				}
			}
		}()
	}
	if _, err := store.CommitIncrement(context.Background(), sample.IncrementV2()); err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()
	close(failures)
	for failure := range failures {
		t.Fatal(failure)
	}
}

func TestLogicalTableAndAggregateCanBeAddedIncrementally(t *testing.T) {
	store, _ := testStore(t)
	commitV1(t, store)
	inc := model.Increment{
		CommitID:    "add-returns-table",
		BaseVersion: 1,
		GeneratedAt: "2026-01-04T00:00:00Z",
		AddPhysicalTables: []model.PhysicalTable{
			{ID: "db1.returns_00", LogicalDB: "db1", Name: "returns_00"},
		},
		Changes: []model.TableChange{
			{
				Op:      model.ChangeUpsertTable,
				TableID: "returns",
				Table: &model.LogicalTable{
					ID:           "returns",
					LogicalDB:    "db1",
					ShardKey:     "user_id",
					ShardKeyType: model.ShardKeyInt,
					Algorithm:    model.Algorithm{Type: model.AlgorithmMod, Modulus: 4, Seed: sample.Seed},
					PrimaryKey:   "return_id",
					Ranges: []model.ShardRange{
						{ID: "returns-0-3", Lower: 0, Upper: 3, LowerInclusive: true, UpperInclusive: true, PhysicalTable: "db1.returns_00", Weight: 1},
					},
				},
			},
		},
		Aggregates: []model.AggregateView{
			{ID: "returns_rollup", DependsOn: []string{"returns"}, Kind: "returns_summary"},
		},
	}
	result, err := store.CommitIncrement(context.Background(), inc)
	if err != nil {
		t.Fatalf("add logical table: %v", err)
	}
	if result.Version != 2 {
		t.Fatalf("unexpected version: %#v", result)
	}
	response, err := route(t, store, "returns", 2, routing.KindEqual, "9")
	if err != nil {
		t.Fatalf("new table route: %v", err)
	}
	if !reflect.DeepEqual(response.PhysicalTables, []string{"db1.returns_00"}) {
		t.Fatalf("new table routed incorrectly: %#v", response.PhysicalTables)
	}
	if cacheFor(t, store.RouteCaches(), "returns").CacheVersion != 2 {
		t.Fatal("new table cache was not created at the target version")
	}
	if aggregateMap(store.AggregateCaches())["returns_rollup"].CacheVersion != 2 {
		t.Fatal("new aggregate cache was not created")
	}
}

func TestShardKeyAndAlgorithmChangeCascade(t *testing.T) {
	store, _ := testStore(t)
	commitV1(t, store)
	snapshot := sample.SnapshotV1()
	var events model.LogicalTable
	for _, table := range snapshot.LogicalTables {
		if table.ID == "events" {
			events = table
			break
		}
	}
	if events.ID == "" {
		t.Fatal("events sample missing")
	}
	events.ShardKey = "tenant_id"
	events.ShardKeyType = model.ShardKeyInt
	events.Algorithm = model.Algorithm{Type: model.AlgorithmMod, Modulus: 4, Seed: sample.Seed}
	inc := model.Increment{
		CommitID:    "events-hash-to-mod",
		BaseVersion: 1,
		GeneratedAt: "2026-01-05T00:00:00Z",
		Changes: []model.TableChange{
			{Op: model.ChangeUpsertTable, TableID: "events", Table: &events},
		},
	}
	if _, err := store.CommitIncrement(context.Background(), inc); err != nil {
		t.Fatalf("change events algorithm: %v", err)
	}
	caches := cacheMap(store.RouteCaches())
	if caches["events"].CacheVersion != 2 {
		t.Fatalf("changed table cache not rebuilt: %#v", caches["events"])
	}
	views := aggregateMap(store.AggregateCaches())
	if views["audit_rollup"].CacheVersion != 2 {
		t.Fatalf("dependent audit view not cascaded: %#v", views["audit_rollup"])
	}
	if views["order_overview"].CacheVersion != 1 {
		t.Fatalf("unrelated view rebuilt: %#v", views["order_overview"])
	}
	response, err := route(t, store, "events", 2, routing.KindEqual, "5")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(response.PhysicalTables, []string{"db0.events_01"}) {
		t.Fatalf("changed algorithm routing wrong: %#v", response.PhysicalTables)
	}
}

func TestRebuildInterruptIntentAndRecoveryAbort(t *testing.T) {
	store, path := testStore(t)
	commitV1(t, store)
	inc := sample.IncrementV2()
	inc.CommitID = "interrupt-demo"
	inc.Inject = InjectRebuildInterrupt
	inc.Checksum = ""
	inc.Checksum, _ = model.IncrementChecksum(inc)
	_, err := store.CommitIncrement(context.Background(), inc)
	biz := assertBiz(t, err, errs.CodeRebuildInterrupted)
	if biz.Context["persisted"] != true {
		t.Fatalf("interrupt context did not record persistence: %#v", biz.Context)
	}
	if store.CurrentVersion() != 1 {
		t.Fatalf("interrupted increment changed version: %d", store.CurrentVersion())
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen after interruption: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	report := reopened.RecoveryReport()
	if report == nil || report.PendingFound != 1 || len(report.Actions) != 1 {
		t.Fatalf("unexpected recovery report: %#v", report)
	}
	if report.Actions[0].Decision != "aborted_incomplete" {
		t.Fatalf("pending increment was not aborted: %#v", report.Actions[0])
	}
	if reopened.CurrentVersion() != 1 {
		t.Fatalf("recovery changed current version: %d", reopened.CurrentVersion())
	}
	response, err := route(t, reopened, "orders", 1, routing.KindEqual, "1")
	if err != nil || response.PhysicalTables[0] != "db0.orders_01" {
		t.Fatalf("old route not usable after recovery: response=%#v err=%v", response, err)
	}
	intents, err := reopened.Intents(context.Background(), 20)
	if err != nil {
		t.Fatal(err)
	}
	foundAborted := false
	for _, intent := range intents {
		if intent.CommitID == "interrupt-demo" {
			foundAborted = intent.Status == "aborted"
		}
	}
	if !foundAborted {
		t.Fatalf("interrupt intent not marked aborted: %#v", intents)
	}
}

func TestRecoveryRebuildsDriftedCaches(t *testing.T) {
	store, path := testStore(t)
	commitV1(t, store)
	if _, err := store.CommitIncrement(context.Background(), sample.IncrementV2()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := store.persistIntent(ctx, "orphan-intent", "increment", 2, 3, map[string]any{"kind": "simulated"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE route_cache SET boundary_index_json='[]' WHERE table_id='orders'`); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	report := reopened.RecoveryReport()
	if report == nil || report.CacheAction != "rebuilt_current_snapshot" {
		t.Fatalf("cache drift was not reported: %#v", report)
	}
	if report.CurrentVersion != 2 {
		t.Fatalf("recovery report version mismatch: %#v", report)
	}
	orders := cacheFor(t, reopened.RouteCaches(), "orders")
	if len(orders.BoundaryIndex) != 3 {
		t.Fatalf("cache not rebuilt from last complete snapshot: %#v", orders)
	}
	response, err := route(t, reopened, "orders", 2, routing.KindEqual, "1")
	if err != nil || response.PhysicalTables[0] != "db0.orders_00" {
		t.Fatalf("route after recovery failed: response=%#v err=%v", response, err)
	}
}

func TestSnapshotPersistsAcrossReopen(t *testing.T) {
	store, path := testStore(t)
	commitV1(t, store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if reopened.CurrentVersion() != 1 {
		t.Fatalf("snapshot not restored: %d", reopened.CurrentVersion())
	}
	response, err := route(t, reopened, "events", 1, routing.KindEqual, "tenant-9")
	if err != nil || len(response.PhysicalTables) != 1 {
		t.Fatalf("restored hash route failed: %#v %v", response, err)
	}
}

func TestCleanReopenAfterIncrementDoesNotFabricateRecovery(t *testing.T) {
	store, path := testStore(t)
	commitV1(t, store)
	if _, err := store.CommitIncrement(context.Background(), sample.IncrementV2()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if report := reopened.RecoveryReport(); report != nil {
		t.Fatalf("clean reopen fabricated a recovery report: %#v", report)
	}
	caches := cacheMap(reopened.RouteCaches())
	if caches["orders"].CacheVersion != 2 || caches["events"].CacheVersion != 1 {
		t.Fatalf("normal cache versions were rewritten during recovery: %#v", caches)
	}
	if reopened.CurrentVersion() != 2 {
		t.Fatalf("unexpected version after clean reopen: %d", reopened.CurrentVersion())
	}
}

func cacheFor(t *testing.T, caches []RouteCache, tableID string) RouteCache {
	t.Helper()
	cache, ok := cacheMap(caches)[tableID]
	if !ok {
		t.Fatalf("cache for %s not found", tableID)
	}
	return cache
}

func cacheMap(caches []RouteCache) map[string]RouteCache {
	out := make(map[string]RouteCache, len(caches))
	for _, cache := range caches {
		out[cache.TableID] = cache
	}
	return out
}

func aggregateMap(caches []AggregateCache) map[string]AggregateCache {
	out := make(map[string]AggregateCache, len(caches))
	for _, cache := range caches {
		out[cache.ViewID] = cache
	}
	return out
}
