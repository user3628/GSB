package app

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"elastic-scale/internal/engine"
	"elastic-scale/internal/model"
	"elastic-scale/internal/seed"
)

func newTestApp(t *testing.T) *App {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "platform.db")
	a, err := New(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func loadSeed(t *testing.T, a *App) *seed.Bundle {
	t.Helper()
	bundle := seed.Generate()
	ctx := context.Background()
	if _, err := a.ReplaceCatalog(ctx, CatalogRequest{CommitID: "catalog-test", Services: bundle.Catalog.Services}); err != nil {
		t.Fatalf("ReplaceCatalog: %v", err)
	}
	if _, err := a.LoadSnapshot(ctx, model.SnapshotRequest{CommitID: "snapshot-test", Snapshot: bundle.Snapshot}); err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	return bundle
}

func businessCode(err error) string {
	var be *model.BusinessError
	if errors.As(err, &be) {
		return be.Code
	}
	return ""
}

func TestSnapshotInvalidKeepsOldVersion(t *testing.T) {
	a := newTestApp(t)
	bundle := loadSeed(t, a)
	ctx := context.Background()

	bad := bundle.Snapshot
	bad.Digest = "sha256:bad"
	_, err := a.LoadSnapshot(ctx, model.SnapshotRequest{CommitID: "bad-digest", Snapshot: bad})
	if code := businessCode(err); code != model.CodeSnapshotInvalid {
		t.Fatalf("bad digest code = %q, want %s, err=%v", code, model.CodeSnapshotInvalid, err)
	}
	current, mat, err := a.Versions(ctx)
	if err != nil || current != 1 || mat != 1 {
		t.Fatalf("versions after bad digest = %d/%d err=%v", current, mat, err)
	}
	if _, err := a.ReadMetrics(ctx, "inventory-svc", nil); err != nil {
		t.Fatalf("old snapshot unavailable after invalid load: %v", err)
	}

	sameVersion := model.SnapshotRequest{CommitID: "same-version-new-commit", Snapshot: bundle.Snapshot}
	_, err = a.LoadSnapshot(ctx, sameVersion)
	if code := businessCode(err); code != model.CodeSnapshotInvalid {
		t.Fatalf("same version code = %q, want %s", code, model.CodeSnapshotInvalid)
	}
	current, _, _ = a.Versions(ctx)
	if current != 1 {
		t.Fatalf("current version changed to %d after monotonic violation", current)
	}
}

func TestConcurrentIncrementsExactlyOneStaysOnBase(t *testing.T) {
	a := newTestApp(t)
	bundle := loadSeed(t, a)
	ctx := context.Background()
	const n = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	success, stale, other := 0, 0, 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			req := bundle.SingleIncrement
			req.CommitID = "concurrent-" + string(rune('a'+i))
			resp, err := a.ApplyIncrement(ctx, req)
			mu.Lock()
			defer mu.Unlock()
			if err == nil && resp.Version == 2 {
				success++
				return
			}
			if businessCode(err) == model.CodeStaleBase {
				stale++
				return
			}
			other++
			t.Errorf("unexpected increment result i=%d resp=%+v err=%v", i, resp, err)
		}(i)
	}
	close(start)
	wg.Wait()
	if success != 1 || stale != n-1 || other != 0 {
		t.Fatalf("success=%d stale=%d other=%d, want success=1 stale=%d other=0", success, stale, other, n-1)
	}
	current, _, _ := a.Versions(ctx)
	if current != 2 {
		t.Fatalf("current=%d want 2", current)
	}
}

func TestIdempotentReplayAndContentConflict(t *testing.T) {
	a := newTestApp(t)
	bundle := loadSeed(t, a)
	ctx := context.Background()
	snapshotV2 := bundle.Snapshot
	snapshotV2.Version = 2
	digest, err := model.DigestSnapshot(snapshotV2)
	if err != nil {
		t.Fatal(err)
	}
	snapshotV2.Digest = digest
	req := model.SnapshotRequest{CommitID: "idempotent-snapshot", Snapshot: snapshotV2}
	if _, err := a.LoadSnapshot(ctx, req); err != nil {
		t.Fatal(err)
	}
	replay, err := a.LoadSnapshot(ctx, req)
	if err != nil || !replay.IdempotentReplay {
		t.Fatalf("replay resp=%+v err=%v", replay, err)
	}
	conflict := req
	conflict.Snapshot.GeneratedAtMS++
	_, err = a.LoadSnapshot(ctx, conflict)
	if code := businessCode(err); code != model.CodeConflictRequest {
		t.Fatalf("conflict code=%q err=%v", code, err)
	}
}

func TestConcurrentSameCommitIDReusesResult(t *testing.T) {
	a := newTestApp(t)
	bundle := loadSeed(t, a)
	ctx := context.Background()
	const n = 6
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	executed, replayed, failed := 0, 0, 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			req := bundle.SingleIncrement
			req.CommitID = "same-commit-id"
			resp, err := a.ApplyIncrement(ctx, req)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failed++
				t.Errorf("same commit request failed: %v", err)
				return
			}
			if resp.IdempotentReplay {
				replayed++
			} else {
				executed++
			}
		}()
	}
	close(start)
	wg.Wait()
	if executed != 1 || replayed != n-1 || failed != 0 {
		t.Fatalf("executed=%d replayed=%d failed=%d", executed, replayed, failed)
	}
}

func TestStaleReadIncludesMaterializedVersion(t *testing.T) {
	a := newTestApp(t)
	loadSeed(t, a)
	observed := int64(42)
	_, err := a.ReadMetrics(context.Background(), "inventory-svc", &observed)
	var be *model.BusinessError
	if !errors.As(err, &be) || be.Code != model.CodeStaleRead {
		t.Fatalf("err=%v want E_STALE_READ", err)
	}
	if be.Context["current_materialized_version"] != int64(1) {
		t.Fatalf("context=%v missing current materialized version", be.Context)
	}
}

func TestSparseSnapshotReturnsInsufficientSamples(t *testing.T) {
	a := newTestApp(t)
	bundle := loadSeed(t, a)
	ctx := context.Background()
	_, err := a.LoadSnapshot(ctx, model.SnapshotRequest{CommitID: "sparse", Snapshot: bundle.SparseSnapshot})
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.ReadDecision(ctx, "auth-svc", nil, 0)
	if code := businessCode(err); code != model.CodeInsufficientSamples {
		t.Fatalf("code=%q err=%v want E_INSUFFICIENT_SAMPLES", code, err)
	}
}

func TestImpactSetHopsAndCascadeRefs(t *testing.T) {
	a := newTestApp(t)
	bundle := loadSeed(t, a)
	ctx := context.Background()
	resp, err := a.ApplyIncrement(ctx, bundle.SingleIncrement)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Rebuild.SelectedPath != "incremental" {
		t.Fatalf("path=%s want incremental", resp.Rebuild.SelectedPath)
	}
	impact, err := a.ReadImpact(ctx, "inventory-svc", nil)
	if err != nil {
		t.Fatal(err)
	}
	hops := map[string]int{}
	for _, node := range impact.ImpactSet {
		hops[node.ServiceID] = node.Hops
	}
	if hops["catalog-svc"] != 1 || hops["edge-gateway"] != 2 {
		t.Fatalf("hops=%v want catalog=1 edge=2", hops)
	}
	var refCount int
	if err := a.db.QueryRowContext(ctx, `SELECT count(*) FROM view_refs WHERE consumer_service='edge-gateway'`).Scan(&refCount); err != nil {
		t.Fatal(err)
	}
	if refCount != 6 {
		t.Fatalf("edge refs=%d want 6 (two dependencies times three view kinds)", refCount)
	}
}

func TestIncrementalAndFullPathsCanonicallyEqual(t *testing.T) {
	a := newTestApp(t)
	bundle := loadSeed(t, a)
	ctx := context.Background()
	inc, err := a.ApplyIncrement(ctx, bundle.SingleIncrement)
	if err != nil {
		t.Fatal(err)
	}
	viewsBefore, err := loadDerivedMapTx(ctx, a.db)
	if err != nil {
		t.Fatal(err)
	}
	metricsBefore, err := engine.CanonicalMetricsJSON(viewsBefore)
	if err != nil {
		t.Fatal(err)
	}
	decisionsBefore, err := engine.CanonicalDecisionsJSON(viewsBefore)
	if err != nil {
		t.Fatal(err)
	}
	full, err := a.Rebuild(ctx, model.RebuildRequest{CommitID: "full-compare", Mode: "full"})
	if err != nil {
		t.Fatal(err)
	}
	if full.Rebuild.MetricsChecksum != inc.Rebuild.MetricsChecksum || full.Rebuild.DecisionsChecksum != inc.Rebuild.DecisionsChecksum {
		t.Fatalf("checksum mismatch\ninc=%s/%s\nfull=%s/%s",
			inc.Rebuild.MetricsChecksum, inc.Rebuild.DecisionsChecksum,
			full.Rebuild.MetricsChecksum, full.Rebuild.DecisionsChecksum)
	}
	viewsAfter, err := loadDerivedMapTx(ctx, a.db)
	if err != nil {
		t.Fatal(err)
	}
	metricsAfter, _ := engine.CanonicalMetricsJSON(viewsAfter)
	decisionsAfter, _ := engine.CanonicalDecisionsJSON(viewsAfter)
	if string(metricsBefore) != string(metricsAfter) {
		t.Fatalf("metrics canonical JSON differs\nbefore=%s\nafter=%s", metricsBefore, metricsAfter)
	}
	if string(decisionsBefore) != string(decisionsAfter) {
		t.Fatalf("decisions canonical JSON differs\nbefore=%s\nafter=%s", decisionsBefore, decisionsAfter)
	}
}

func TestCrashIntentIsAbortedOnRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "crash.db")
	ctx := context.Background()
	a, err := New(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	bundle := seed.Generate()
	if _, err := a.ReplaceCatalog(ctx, CatalogRequest{CommitID: "cat", Services: bundle.Catalog.Services}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.LoadSnapshot(ctx, model.SnapshotRequest{CommitID: "snap", Snapshot: bundle.Snapshot}); err != nil {
		t.Fatal(err)
	}
	armed, err := a.ApplyIncrement(ctx, model.IncrementRequest{CommitID: "crashed-commit", BaseVersion: 1, CrashAfterIntent: true})
	if err != nil || !armed.CrashArmed {
		t.Fatalf("arm resp=%+v err=%v", armed, err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	a2, err := New(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer a2.Close()
	report, err := a2.LatestRecovery(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.FoundPendingIntents != 1 || len(report.AbortedCommitIDs) != 1 || report.AbortedCommitIDs[0] != "crashed-commit" {
		t.Fatalf("recovery report=%+v", report)
	}
	current, mat, _ := a2.Versions(ctx)
	if current != 1 || mat != 1 || report.RecoveredToVersion != 1 {
		t.Fatalf("versions current=%d mat=%d report=%+v", current, mat, report)
	}
}
