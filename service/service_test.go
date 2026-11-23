package service

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"releasegate/domain"
	"releasegate/store"
)

func testDefinition() domain.ReleaseDefinition {
	gate := domain.GateThresholds{
		MaxAbsSuccessDropPPM: 60000, MaxRelativeSuccessDropPPM: 100000,
		MaxLatencyDeltaUS: 2000, MaxCIHalfWidthPPM: 80000, RelativeTolerancePPM: 1000,
	}
	return domain.ReleaseDefinition{
		ReleaseID: "release-test", QuantileMethod: "linear", ConfidenceLevelPPM: 9500,
		Stages: []domain.StageDefinition{
			{Number: 1, TargetTrafficPerMille: 100, MinObservationSamples: 100, LatencyQuantilePerMille: 950, Gate: gate},
			{Number: 2, TargetTrafficPerMille: 300, MinObservationSamples: 100, LatencyQuantilePerMille: 950, Gate: gate},
			{Number: 3, TargetTrafficPerMille: 1000, MinObservationSamples: 100, LatencyQuantilePerMille: 950, Gate: gate},
		},
	}
}

func newTestService(t *testing.T) (*Service, *store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "releasegate.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return New(st), st
}

func makeBatch(stage int, batchID string, startTS int64, canarySuccesses, stableSuccesses, groupSamples int, canaryBase, stableBase int64) domain.BatchInput {
	var observations []domain.ObservationInput
	ts := startTS
	for i := 0; i < groupSamples; i++ {
		for _, g := range []string{domain.GroupCanary, domain.GroupStable} {
			success := false
			base := canaryBase
			limit := canarySuccesses
			if g == domain.GroupStable {
				base = stableBase
				limit = stableSuccesses
			}
			if i < limit {
				success = true
			}
			latency := base + int64((i*37)%131)
			observations = append(observations, domain.ObservationInput{
				TimestampMS: ts, InstanceID: g + "-" + intString(i), Group: g,
				Success: &success, LatencyUS: &latency, Stage: stage,
			})
			ts += 2
		}
	}
	return domain.BatchInput{ReleaseID: "release-test", BatchID: batchID, Stage: stage, Observations: observations}
}

func intString(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}

func prepareRelease(t *testing.T, svc *Service) {
	t.Helper()
	ctx := context.Background()
	if _, err := svc.CreateRelease(ctx, CreateReleaseRequest{ReleaseID: "release-test", Definition: testDefinition()}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Start(ctx, "release-test"); err != nil {
		t.Fatal(err)
	}
}

func TestPromotePauseAndTransactionalHeader(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	prepareRelease(t, svc)
	res, err := svc.IngestBatch(ctx, makeBatch(1, "stage-1", 1000, 194, 190, 200, 1900, 2100))
	if err != nil {
		t.Fatal(err)
	}
	if res.StatsVersion != 1 || res.ObservationCount != 400 {
		t.Fatalf("unexpected batch result: %+v", res)
	}
	detail, err := svc.StatsDetail(ctx, "release-test", 1, "stage", 1)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Estimate.AbsSuccessDropPPM != 0 || detail.Estimate.CIHalfWidthPPM != 38357 || detail.Estimate.ErrorBoundPPM != 97998 {
		t.Fatalf("unexpected stage1 estimate: %+v", detail.Estimate)
	}
	gate1, err := svc.Gate(ctx, "release-test", 1)
	if err != nil {
		t.Fatal(err)
	}
	if gate1.Decision.Outcome != domain.OutcomePromote {
		t.Fatalf("gate1 outcome=%s", gate1.Decision.Outcome)
	}
	rel, err := svc.GetRelease(ctx, "release-test")
	if err != nil {
		t.Fatal(err)
	}
	if rel.State != domain.StatePromoting || rel.CurrentStage != 2 || rel.TrafficPerMille != 300 || rel.CurrentStatsVersion != 1 {
		t.Fatalf("promotion header not atomic: %+v", rel)
	}

	if _, err := svc.IngestBatch(ctx, makeBatch(2, "stage-2", 5000, 155, 190, 200, 2400, 2100)); err != nil {
		t.Fatal(err)
	}
	gate2, err := svc.Gate(ctx, "release-test", 2)
	if err == nil {
		t.Fatal("expected gate failure")
	}
	be, ok := domain.AsBusinessError(err)
	if !ok || be.Code != domain.CodeGateFailed {
		t.Fatalf("gate2 error=%v", err)
	}
	if gate2.Decision.TriggeredThreshold != domain.ThresholdAbsSuccessDrop || gate2.Decision.Measured.MeasuredValue != 240281 {
		t.Fatalf("unexpected gate2 decision: %+v", gate2.Decision)
	}
	rel, _ = svc.GetRelease(ctx, "release-test")
	if rel.State != domain.StateObserving || rel.CurrentStage != 2 || rel.TrafficPerMille != 300 || rel.CurrentStatsVersion != 2 {
		t.Fatalf("pause header not atomic: %+v", rel)
	}
}

func TestDuplicateBatchIsIdempotentAndConflictRejected(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	prepareRelease(t, svc)
	in := makeBatch(1, "stage-1", 1000, 194, 190, 200, 1900, 2100)
	first, err := svc.IngestBatch(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.IngestBatch(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if !second.IdempotentReuse || second.StatsVersion != first.StatsVersion {
		t.Fatalf("duplicate not reused: %+v", second)
	}
	conflict := in
	conflict.Observations[0].InstanceID = "changed"
	if _, err := svc.IngestBatch(ctx, conflict); err == nil {
		t.Fatal("expected conflicting batch error")
	} else if be, ok := domain.AsBusinessError(err); !ok || be.Code != domain.CodeConflictingBatch {
		t.Fatalf("unexpected conflict error: %v", err)
	}
}

func TestObserveMoreAndInsufficientSamples(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	prepareRelease(t, svc)
	if _, err := svc.IngestBatch(ctx, makeBatch(1, "two-each", 1000, 2, 2, 2, 1900, 2100)); err != nil {
		t.Fatal(err)
	}
	_, err := svc.Gate(ctx, "release-test", 0)
	if be, ok := domain.AsBusinessError(err); !ok || be.Code != domain.CodeObserveMore {
		t.Fatalf("expected observe-more, got %v", err)
	}

	svc2, _ := newTestService(t)
	prepareRelease(t, svc2)
	_, err = svc2.IngestBatch(ctx, makeBatch(1, "one-each", 1000, 1, 1, 1, 1900, 2100))
	if be, ok := domain.AsBusinessError(err); !ok || be.Code != domain.CodeInsufficientSamples {
		t.Fatalf("expected insufficient samples, got %v", err)
	}
	rel, _ := svc2.GetRelease(ctx, "release-test")
	if rel.CurrentStatsVersion != 0 {
		t.Fatalf("insufficient batch partially committed: %+v", rel)
	}
}

func TestVersionRetirementAndBoundVersion(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	prepareRelease(t, svc)
	for i := 1; i <= 9; i++ {
		in := makeBatch(1, "batch-"+intString(i), int64(1000+i*10000), 2, 2, 2, 1900, 2100)
		if _, err := svc.IngestBatch(ctx, in); err != nil {
			t.Fatalf("batch %d: %v", i, err)
		}
	}
	if _, err := svc.StatsDetail(ctx, "release-test", 1, "stage", 1); err == nil {
		t.Fatal("expected retired version error")
	} else if be, ok := domain.AsBusinessError(err); !ok || be.Code != domain.CodeVersionRetired {
		t.Fatalf("unexpected retired error: %v", err)
	}
	if _, err := svc.Gate(ctx, "release-test", 1); err == nil {
		t.Fatal("expected bound version retired")
	} else if be, ok := domain.AsBusinessError(err); !ok || be.Code != domain.CodeVersionRetired {
		t.Fatalf("unexpected bound error: %v", err)
	}
}

func TestConcurrentDuplicateIngestWaitsAndReuses(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	prepareRelease(t, svc)
	in := makeBatch(1, "same", 1000, 194, 190, 200, 1900, 2100)
	results := make([]domain.BatchResult, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = svc.IngestBatch(ctx, in)
		}(i)
	}
	wg.Wait()
	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("concurrent ingest errors: %v %v", errs[0], errs[1])
	}
	if results[0].IdempotentReuse == results[1].IdempotentReuse {
		t.Fatalf("expected exactly one writer and one reuse: %+v", results)
	}
}

func TestRecoveryReportsOnlyDurableFacts(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "recovery.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	svc := New(st)
	ctx := context.Background()
	prepareRelease(t, svc)
	if _, err := svc.IngestBatch(ctx, makeBatch(1, "stage-1", 1000, 194, 190, 200, 1900, 2100)); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.PrepareFaultIntent(ctx, "release-test", IntentTransition); err != nil {
		t.Fatal(err)
	}
	before, _ := svc.GetRelease(ctx, "release-test")
	st.Close()

	restarted, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	report, err := restarted.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.PendingIntentsFound != 1 || report.IntentsRecovered != 1 {
		t.Fatalf("unexpected recovery report: %+v", report)
	}
	if report.ClaimBoundary == "" {
		t.Fatal("missing claim boundary")
	}
	svc2 := New(restarted)
	after, _ := svc2.GetRelease(ctx, "release-test")
	if before.State != after.State || before.CurrentStage != after.CurrentStage || before.TrafficPerMille != after.TrafficPerMille || before.CurrentStatsVersion != after.CurrentStatsVersion {
		t.Fatalf("recovery changed committed header: before=%+v after=%+v", before, after)
	}
}

func TestRecoveryAbortsOrphanStagingBatch(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "orphan.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	svc := New(st)
	ctx := context.Background()
	prepareRelease(t, svc)
	rel, _ := svc.GetRelease(ctx, "release-test")
	batch, err := domain.NormalizeBatch(rel.Definition, makeBatch(1, "orphan", 1000, 194, 190, 200, 1900, 2100))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.BeginIntent(ctx, store.Intent{IntentID: "intent-orphan", ReleaseID: rel.ReleaseID, Kind: IntentIngestBatch, FromState: rel.State, FromStage: rel.CurrentStage, FromVersion: rel.CurrentStatsVersion}); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertStagingBatch(ctx, batch, "sha256:orphan", 1234); err != nil {
		t.Fatal(err)
	}
	st.Close()

	restarted, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	report, err := restarted.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.OrphanBatchesAborted != 1 || report.PendingIntentsFound != 1 {
		t.Fatalf("unexpected orphan recovery report: %+v", report)
	}
	svc2 := New(restarted)
	after, _ := svc2.GetRelease(ctx, rel.ReleaseID)
	if after.CurrentStatsVersion != rel.CurrentStatsVersion {
		t.Fatalf("orphan batch changed version: %+v", after)
	}
}

func TestSnapshotIsIndependentOfObservationOrder(t *testing.T) {
	def := testDefinition()
	batch, err := domain.NormalizeBatch(def, makeBatch(1, "det", 1000, 194, 190, 200, 1900, 2100))
	if err != nil {
		t.Fatal(err)
	}
	forward := append([]domain.Observation(nil), batch.Observations...)
	reverse := append([]domain.Observation(nil), batch.Observations...)
	for i, j := 0, len(reverse)-1; i < j; i, j = i+1, j-1 {
		reverse[i], reverse[j] = reverse[j], reverse[i]
	}
	a, err := ComputeSnapshot(def, "release-test", 7, 1, 999, forward)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ComputeSnapshot(def, "release-test", 7, 1, 999, reverse)
	if err != nil {
		t.Fatal(err)
	}
	aj, _ := json.Marshal(a)
	bj, _ := json.Marshal(b)
	if !reflect.DeepEqual(aj, bj) {
		t.Fatal("snapshot differs when observation submission order changes")
	}
}

func TestConcurrentReadersSeeCompleteSnapshots(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	prepareRelease(t, svc)
	if _, err := svc.IngestBatch(ctx, makeBatch(1, "stage-1", 1000, 194, 190, 200, 1900, 2100)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			detail, err := svc.StatsDetail(ctx, "release-test", 1, "stage", 1)
			if err != nil {
				errCh <- err
				return
			}
			if detail.Estimate.CIHalfWidthPPM != 38357 || detail.StageMetrics.Canary.Samples != 200 {
				errCh <- domain.NewError(domain.CodeInternal, "mixed snapshot")
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestRecoveryRestoresLastCompleteStatsVersion(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "version-repair.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	svc := New(st)
	ctx := context.Background()
	prepareRelease(t, svc)
	if _, err := svc.IngestBatch(ctx, makeBatch(1, "stage-1", 1000, 194, 190, 200, 1900, 2100)); err != nil {
		t.Fatal(err)
	}
	// Simulate a release pointer that survived without its complete snapshot,
	// plus a pending recomputation intent.
	if _, err := st.ExecContext(ctx, `UPDATE releases SET current_stats_version=2 WHERE release_id=?`, "release-test"); err != nil {
		t.Fatal(err)
	}
	if err := st.BeginIntent(ctx, store.Intent{IntentID: "intent-recompute", ReleaseID: "release-test", Kind: IntentRecompute, FromState: domain.StateCanary, FromStage: 1, FromVersion: 2}); err != nil {
		t.Fatal(err)
	}
	st.Close()

	restarted, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	report, err := restarted.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.VersionsRestored != 1 {
		t.Fatalf("expected one restored version: %+v", report)
	}
	svc2 := New(restarted)
	rel, err := svc2.GetRelease(ctx, "release-test")
	if err != nil {
		t.Fatal(err)
	}
	if rel.CurrentStatsVersion != 1 {
		t.Fatalf("current version = %d, want 1", rel.CurrentStatsVersion)
	}
}
