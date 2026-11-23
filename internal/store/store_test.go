package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"path/filepath"
	"testing"
	"time"

	"blocksnap/internal/apperr"
	"blocksnap/internal/metrics"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	clock := func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }
	s, err := Open(filepath.Join(t.TempDir(), "test.db"), WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func makeBlock(seed byte, size int) []byte {
	out := make([]byte, size)
	for i := range out {
		out[i] = seed + byte(i*31)
	}
	return out
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func mustCreateVolume(t *testing.T, s *Store) {
	t.Helper()
	res, err := s.CreateVolume(context.Background(), CreateVolumeRequest{
		VolumeID: "vol", RequestID: "req-create", BlockCount: 8, BlockSize: 64, OverheadBytes: 4,
	})
	if err != nil {
		t.Fatalf("create volume: %v", err)
	}
	if res.HTTPStatus != 200 {
		t.Fatalf("status=%d", res.HTTPStatus)
	}
}

func mustWrite(t *testing.T, s *Store, reqID string, block int64, expected int64, seed byte) WriteBlockResult {
	t.Helper()
	content := makeBlock(seed, 64)
	res, err := s.WriteBlock(context.Background(), WriteBlockRequest{
		VolumeID: "vol", BlockNo: block, RequestID: reqID, ExpectedVersion: expected,
		Content: content, Digest: digest(content),
	})
	if err != nil {
		t.Fatalf("write block: %v", err)
	}
	var out WriteBlockResult
	if err := json.Unmarshal(res.Body, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestVolumeWriteSnapshotMergeAndRollback(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	mustCreateVolume(t, s)

	w := mustWrite(t, s, "w1", 0, 0, 7)
	if w.Block.Version != 1 {
		t.Fatalf("version=%d", w.Block.Version)
	}
	view, content, err := s.GetBlock(ctx, "vol", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(content, makeBlock(7, 64)) || view.Digest != digest(content) {
		t.Fatal("stored block content/digest mismatch")
	}

	snapRes, err := s.CreateSnapshot(ctx, CreateSnapshotRequest{
		VolumeID: "vol", SnapshotID: "snap-full", RequestID: "s1", Kind: "full", Weight: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	var full CreateSnapshotResult
	json.Unmarshal(snapRes.Body, &full)
	if full.Snapshot.DeltaBlocks != 8 || full.Snapshot.EffectiveBlocks != 8 {
		t.Fatalf("full snapshot counts: %+v", full.Snapshot)
	}

	w = mustWrite(t, s, "w2", 0, 1, 9)
	if w.Block.Version != 2 {
		t.Fatalf("version=%d", w.Block.Version)
	}
	incRes, err := s.CreateSnapshot(ctx, CreateSnapshotRequest{
		VolumeID: "vol", SnapshotID: "snap-inc", RequestID: "s2", Kind: "incremental", ParentID: "snap-full", Weight: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	var inc CreateSnapshotResult
	json.Unmarshal(incRes.Body, &inc)
	if inc.Snapshot.DeltaBlocks != 1 {
		t.Fatalf("incremental delta=%d, want 1", inc.Snapshot.DeltaBlocks)
	}

	analysis, err := s.GetSnapshotAnalysis(ctx, "vol", "snap-full")
	if err != nil {
		t.Fatal(err)
	}
	if len(analysis.Chain) != 1 {
		t.Fatalf("chain=%v", analysis.Chain)
	}
	if analysis.Metrics.RollbackCost.WritebackBlocks != 1 {
		t.Fatalf("writeback=%d", analysis.Metrics.RollbackCost.WritebackBlocks)
	}

	_, err = s.DeleteSnapshot(ctx, DeleteSnapshotRequest{VolumeID: "vol", SnapshotID: "snap-full", RequestID: "del1"})
	if apperr.As(err).Code != apperr.CodeSnapshotReferenced {
		t.Fatalf("delete parent error=%v", err)
	}

	if _, err := s.MergeSnapshots(ctx, MergeRequest{VolumeID: "vol", RequestID: "m1", ParentID: "snap-full", ChildID: "snap-inc"}); err != nil {
		t.Fatal(err)
	}
	analysis, err = s.GetSnapshotAnalysis(ctx, "vol", "snap-full")
	if err != nil {
		t.Fatal(err)
	}
	if analysis.Snapshot.Weight != 8 {
		t.Fatalf("merged weight=%d", analysis.Snapshot.Weight)
	}
	if analysis.Metrics.RollbackCost.WritebackBlocks != 0 {
		t.Fatalf("after merge writeback=%d", analysis.Metrics.RollbackCost.WritebackBlocks)
	}
}

func TestChecksumAndVersionConflictDoNotMutateBlock(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	mustCreateVolume(t, s)
	content := makeBlock(3, 64)
	_, err := s.WriteBlock(ctx, WriteBlockRequest{
		VolumeID: "vol", BlockNo: 1, RequestID: "bad", ExpectedVersion: 0, Content: content, Digest: digest([]byte("other")),
	})
	if apperr.As(err).Code != apperr.CodeBlockChecksum {
		t.Fatalf("want checksum error, got %v", err)
	}
	view, _, err := s.GetBlock(ctx, "vol", 1)
	if err != nil {
		t.Fatal(err)
	}
	if view.Version != 0 {
		t.Fatalf("block mutated on checksum failure: version=%d", view.Version)
	}
	mustWrite(t, s, "ok", 1, 0, 4)
	_, err = s.WriteBlock(ctx, WriteBlockRequest{
		VolumeID: "vol", BlockNo: 1, RequestID: "stale", ExpectedVersion: 0, Content: content, Digest: digest(content),
	})
	if apperr.As(err).Code != apperr.CodeBlockConflict {
		t.Fatalf("want block conflict, got %v", err)
	}
}

func TestIdempotencyAndConflictingRequest(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	mustCreateVolume(t, s)
	content := makeBlock(1, 64)
	req := WriteBlockRequest{VolumeID: "vol", BlockNo: 0, RequestID: "same", ExpectedVersion: 0, Content: content, Digest: digest(content)}
	first, err := s.WriteBlock(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.WriteBlock(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Body, second.Body) {
		t.Fatalf("replay differs:\n%s\n%s", first.Body, second.Body)
	}
	req.Content = makeBlock(2, 64)
	req.Digest = digest(req.Content)
	req.ExpectedVersion = 1
	_, err = s.WriteBlock(ctx, req)
	if apperr.As(err).Code != apperr.CodeConflictingRequest {
		t.Fatalf("want conflicting request, got %v", err)
	}
}

func TestAnalysisStoredProjectionMatchesFresh(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	mustCreateVolume(t, s)
	mustWrite(t, s, "w", 2, 0, 9)
	if _, err := s.CreateSnapshot(ctx, CreateSnapshotRequest{VolumeID: "vol", SnapshotID: "s", RequestID: "s", Kind: "full", Weight: 1}); err != nil {
		t.Fatal(err)
	}
	stored, err := s.ReadMetricsJSON(ctx, "vol", "s")
	if err != nil {
		t.Fatal(err)
	}
	a1, err := s.GetSnapshotAnalysis(ctx, "vol", "s")
	if err != nil {
		t.Fatal(err)
	}
	a2, err := s.GetSnapshotAnalysis(ctx, "vol", "s")
	if err != nil {
		t.Fatal(err)
	}
	b1, _ := json.Marshal(a1.Metrics)
	b2, _ := json.Marshal(a2.Metrics)
	if !bytes.Equal(b1, b2) || !bytes.Equal(b1, stored) {
		t.Fatal("metrics are not byte-stable")
	}
}

func TestConcurrentSameBlockOneWinner(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	mustCreateVolume(t, s)
	content := makeBlock(5, 64)
	type result struct {
		code string
	}
	const n = 8
	ch := make(chan result, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			_, err := s.WriteBlock(ctx, WriteBlockRequest{
				VolumeID: "vol", BlockNo: 0, RequestID: "c" + string(rune('a'+i)), ExpectedVersion: 0,
				Content: content, Digest: digest(content),
			})
			code := ""
			if err != nil {
				code = apperr.As(err).Code
			}
			ch <- result{code: code}
		}(i)
	}
	success := 0
	conflicts := 0
	for i := 0; i < n; i++ {
		r := <-ch
		switch r.code {
		case "":
			success++
		case apperr.CodeBlockConflict:
			conflicts++
		default:
			t.Fatalf("unexpected code %s", r.code)
		}
	}
	if success != 1 || conflicts != n-1 {
		t.Fatalf("success=%d conflicts=%d", success, conflicts)
	}
}

func TestMetricsEnginesRejectCorruptCoverage(t *testing.T) {
	state := &metrics.State{VolumeID: "v", BlockCount: 2, BlockSize: 8, Live: map[int64]metrics.BlockRef{}, Snapshots: map[string]metrics.Snapshot{}, Refs: map[string]map[int64]int64{}, Versions: map[metrics.VersionKey]string{}}
	if _, err := metrics.Analyze(state, "x"); err == nil || !errors.Is(err, metrics.ErrCorrupt) {
		t.Fatalf("want corrupt error, got %v", err)
	}
}

func TestRollbackRejectedByMarginContractLeavesLiveState(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	mustCreateVolume(t, s)
	mustWrite(t, s, "r1", 0, 0, 1)
	if _, err := s.CreateSnapshot(ctx, CreateSnapshotRequest{VolumeID: "vol", SnapshotID: "full", RequestID: "rs", Kind: "full", Weight: 1}); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, s, "r2", 0, 1, 2)
	_, err := s.Rollback(ctx, RollbackRequest{
		VolumeID: "vol", SnapshotID: "full", RequestID: "rollback-rejected",
		Decision: DecisionInput{
			MarginFloor: big.NewInt(900000), CostBudgetBytes: big.NewInt(1000000),
			RelativeTolerance: big.NewInt(0),
		},
	})
	if apperr.As(err).Code != apperr.CodeRollbackRejected {
		t.Fatalf("want E_ROLLBACK_REJECTED, got %v", err)
	}
	view, _, err := s.GetBlock(ctx, "vol", 0)
	if err != nil {
		t.Fatal(err)
	}
	if view.Version != 2 {
		t.Fatalf("rejected rollback changed block version to %d", view.Version)
	}
}

func TestMetricProjectionTamperIsDetected(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	mustCreateVolume(t, s)
	if _, err := s.CreateSnapshot(ctx, CreateSnapshotRequest{VolumeID: "vol", SnapshotID: "full", RequestID: "s", Kind: "full", Weight: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE snapshot_metrics SET metrics_json='{}' WHERE volume_id='vol' AND snapshot_id='full'`); err != nil {
		t.Fatal(err)
	}
	_, err := s.GetSnapshotAnalysis(ctx, "vol", "full")
	if apperr.As(err).Code != apperr.CodeMetricStale {
		t.Fatalf("want E_METRIC_STALE, got %v", err)
	}
}

func TestCrashRecoveryMarksIntentAbortedAndPreservesMetrics(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	mustCreateVolume(t, s)
	mustWrite(t, s, "w1", 0, 0, 1)
	mustWrite(t, s, "w2", 0, 1, 2)
	if _, err := s.CreateSnapshot(ctx, CreateSnapshotRequest{VolumeID: "vol", SnapshotID: "full", RequestID: "sf", Kind: "full", Weight: 2}); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, s, "w3", 0, 2, 3)
	if _, err := s.CreateSnapshot(ctx, CreateSnapshotRequest{VolumeID: "vol", SnapshotID: "inc", RequestID: "si", Kind: "incremental", ParentID: "full", Weight: 1}); err != nil {
		t.Fatal(err)
	}
	before, err := s.ReadMetricsJSON(ctx, "vol", "inc")
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected injected crash panic")
			}
		}()
		_ = s.SimulateCrash(ctx, SimulateCrashRequest{
			VolumeID: "vol", Operation: "merge", ParentID: "full", ChildID: "inc", RequestID: "crash-req",
		}, func(exitCode int) { panic(exitCode) })
	}()
	report, err := s.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.PendingFound != 1 || report.AbortedCount != 1 || report.Status != "complete" {
		t.Fatalf("unexpected report: %+v", report)
	}
	after, err := s.ReadMetricsJSON(ctx, "vol", "inc")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("metrics changed after recovery\nbefore=%s\nafter=%s", before, after)
	}
	snaps, err := s.ListSnapshots(ctx, "vol")
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps.Snapshots) != 2 {
		t.Fatalf("snapshot metadata changed: %+v", snaps.Snapshots)
	}
	latest, err := s.LatestRecoveryReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if latest.ID != report.ID || latest.Details == nil {
		t.Fatalf("latest report mismatch: %+v", latest)
	}
	rec, found, err := s.readRequest(ctx, "crash-req")
	if err != nil || !found {
		t.Fatalf("recovered request lookup: found=%v err=%v", found, err)
	}
	if rec.Status != "completed" || rec.HTTPStatus != 409 || !bytes.Contains([]byte(rec.Body), []byte("E_OPERATION_ABORTED")) {
		t.Fatalf("interrupted request was not replayed as aborted: %+v", rec)
	}
}

func TestConcurrentDuplicateRequestReusesOneResult(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	mustCreateVolume(t, s)
	content := makeBlock(8, 64)
	req := WriteBlockRequest{
		VolumeID: "vol", BlockNo: 0, RequestID: "same-concurrent", ExpectedVersion: 0,
		Content: content, Digest: digest(content),
	}
	const n = 8
	type response struct {
		body []byte
		err  error
	}
	ch := make(chan response, n)
	for i := 0; i < n; i++ {
		go func() {
			res, err := s.WriteBlock(ctx, req)
			ch <- response{body: res.Body, err: err}
		}()
	}
	var first []byte
	for i := 0; i < n; i++ {
		got := <-ch
		if got.err != nil {
			t.Fatalf("duplicate failed: %v", got.err)
		}
		if first == nil {
			first = got.body
		} else if !bytes.Equal(first, got.body) {
			t.Fatal("concurrent duplicate responses differ")
		}
	}
	view, _, err := s.GetBlock(ctx, "vol", 0)
	if err != nil {
		t.Fatal(err)
	}
	if view.Version != 1 {
		t.Fatalf("duplicate requests applied more than once: version=%d", view.Version)
	}
}

func TestRollbackRestoresContentWithMonotonicVersion(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	mustCreateVolume(t, s)
	original := makeBlock(1, 64)
	first := mustWrite(t, s, "rw1", 0, 0, 1)
	if _, err := s.CreateSnapshot(ctx, CreateSnapshotRequest{VolumeID: "vol", SnapshotID: "full", RequestID: "rws", Kind: "full", Weight: 1}); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, s, "rw2", 0, 1, 2)
	_, err := s.Rollback(ctx, RollbackRequest{
		VolumeID: "vol", SnapshotID: "full", RequestID: "rw-rollback",
		Decision: DecisionInput{
			MarginFloor: big.NewInt(0), CostBudgetBytes: big.NewInt(1000000),
			RelativeTolerance: big.NewInt(0),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	view, content, err := s.GetBlock(ctx, "vol", 0)
	if err != nil {
		t.Fatal(err)
	}
	if view.Version != first.Block.Version+2 {
		t.Fatalf("rollback version=%d, want %d", view.Version, first.Block.Version+2)
	}
	if !bytes.Equal(content, original) || view.Digest != digest(original) {
		t.Fatal("rollback did not restore original content")
	}
	analysis, err := s.GetSnapshotAnalysis(ctx, "vol", "full")
	if err != nil {
		t.Fatal(err)
	}
	if analysis.Metrics.RollbackCost.WritebackBlocks != 0 || analysis.Metrics.RollbackMargin.Ratio.Decimal != "1.000000" {
		t.Fatalf("post-rollback metrics: %+v", analysis.Metrics)
	}
}
