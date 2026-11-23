package service

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"example.com/joinshuffle/internal/engine"
	"example.com/joinshuffle/internal/model"
	"example.com/joinshuffle/internal/sample"
	"example.com/joinshuffle/internal/snapshot"
	"example.com/joinshuffle/internal/store"
)

type testEnvironment struct {
	dir  string
	svc  *Service
	st   *store.Store
	meta model.SnapshotPairMeta
}

func newTestEnvironment(t *testing.T) *testEnvironment {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "engine.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := New(st)
	env := &testEnvironment{dir: dir, svc: svc, st: st}
	env.meta = env.writeAndLoad(t, 1, 1, 160, 120)
	return env
}

func (e *testEnvironment) writeAndLoad(t *testing.T, leftVersion, rightVersion uint64, leftRows, rightRows int) model.SnapshotPairMeta {
	t.Helper()
	left, right, err := sample.GeneratePair(99, leftRows, rightRows, leftVersion, rightVersion, sample.DefaultGeneratedAt)
	if err != nil {
		t.Fatal(err)
	}
	leftPath := filepath.Join(e.dir, "left.snapshot")
	rightPath := filepath.Join(e.dir, "right.snapshot")
	if _, err := snapshot.WriteFile(leftPath, left); err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.WriteFile(rightPath, right); err != nil {
		t.Fatal(err)
	}
	meta, err := e.svc.LoadSnapshots(context.Background(), model.LoadSnapshotsRequest{
		Left:  model.SnapshotLoadInput{Path: leftPath},
		Right: model.SnapshotLoadInput{Path: rightPath},
	})
	if err != nil {
		t.Fatal(err)
	}
	return meta
}

func defaultRequest(id string) model.AggregateRequest {
	return model.AggregateRequest{
		RequestID:         id,
		JoinType:          "left",
		PartitionCount:    4,
		SkewThreshold:     0.35,
		Algorithm:         "double_double_fixed",
		FixedScaleBits:    80,
		RelativeTolerance: 1e-12,
	}
}

func TestSnapshotFailurePreservesOldPairAndVersionMustIncrease(t *testing.T) {
	env := newTestEnvironment(t)
	original := env.svc.SnapshotsStatus()
	if !original.Loaded || original.Snapshots.Left.Version != 1 || original.Snapshots.Right.Version != 1 {
		t.Fatalf("unexpected initial snapshot status: %+v", original)
	}

	left, right, err := sample.GeneratePair(99, 180, 140, 2, 2, sample.DefaultGeneratedAt)
	if err != nil {
		t.Fatal(err)
	}
	leftPath := filepath.Join(env.dir, "left-v2.snapshot")
	rightPath := filepath.Join(env.dir, "right-v2.snapshot")
	if _, err := snapshot.WriteFile(leftPath, left); err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.WriteFile(rightPath, right); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(leftPath+".sha256", []byte(strings.Repeat("0", 64)+"  left-v2.snapshot\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = env.svc.LoadSnapshots(context.Background(), model.LoadSnapshotsRequest{
		Left:  model.SnapshotLoadInput{Path: leftPath},
		Right: model.SnapshotLoadInput{Path: rightPath},
	})
	business, ok := err.(*model.BusinessError)
	if !ok || business.Code != model.CodeSnapshotInvalid {
		t.Fatalf("expected E_SNAPSHOT_INVALID, got %v", err)
	}
	afterFailure := env.svc.SnapshotsStatus()
	if afterFailure.Snapshots.Left.Version != 1 || afterFailure.Snapshots.Right.Version != 1 {
		t.Fatalf("old snapshot pair was not preserved: %+v", afterFailure)
	}

	if _, err := snapshot.WriteFile(leftPath, left); err != nil {
		t.Fatal(err)
	}
	loaded, err := env.svc.LoadSnapshots(context.Background(), model.LoadSnapshotsRequest{
		Left:  model.SnapshotLoadInput{Path: leftPath},
		Right: model.SnapshotLoadInput{Path: rightPath},
	})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Left.Version != 2 || loaded.Right.Version != 2 {
		t.Fatalf("expected version 2, got %+v", loaded)
	}
	_, err = env.svc.LoadSnapshots(context.Background(), model.LoadSnapshotsRequest{
		Left:  model.SnapshotLoadInput{Path: leftPath},
		Right: model.SnapshotLoadInput{Path: rightPath},
	})
	business, ok = err.(*model.BusinessError)
	if !ok || business.Code != model.CodeSnapshotInvalid {
		t.Fatalf("expected non-monotonic version to fail, got %v", err)
	}
	if env.svc.SnapshotsStatus().Snapshots.Left.Version != 2 {
		t.Fatal("non-monotonic reload replaced the active snapshot")
	}
}

func TestPersistedVersionFloorSurvivesRestoreFailure(t *testing.T) {
	env := newTestEnvironment(t)
	leftPath := filepath.Join(env.dir, "left.snapshot")
	rightPath := filepath.Join(env.dir, "right.snapshot")
	if err := os.Remove(leftPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(rightPath); err != nil {
		t.Fatal(err)
	}
	restarted := New(env.st)
	if err := restarted.RestoreSnapshots(context.Background()); err == nil {
		t.Fatal("expected restore failure with missing snapshot files")
	}
	leftV1, rightV1, err := sample.GeneratePair(99, 160, 120, 1, 1, sample.DefaultGeneratedAt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.WriteFile(leftPath, leftV1); err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.WriteFile(rightPath, rightV1); err != nil {
		t.Fatal(err)
	}
	_, err = restarted.LoadSnapshots(context.Background(), model.LoadSnapshotsRequest{
		Left:  model.SnapshotLoadInput{Path: leftPath},
		Right: model.SnapshotLoadInput{Path: rightPath},
	})
	business, ok := err.(*model.BusinessError)
	if !ok || business.Code != model.CodeSnapshotInvalid {
		t.Fatalf("persisted version floor was not applied after restore failure: %v", err)
	}
	leftV2, rightV2, err := sample.GeneratePair(99, 180, 140, 2, 2, sample.DefaultGeneratedAt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.WriteFile(leftPath, leftV2); err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.WriteFile(rightPath, rightV2); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.LoadSnapshots(context.Background(), model.LoadSnapshotsRequest{
		Left:  model.SnapshotLoadInput{Path: leftPath},
		Right: model.SnapshotLoadInput{Path: rightPath},
	}); err != nil {
		t.Fatalf("version above persisted floor should load: %v", err)
	}
}

func TestSubmitIsIdempotentAndReusesStoredResult(t *testing.T) {
	env := newTestEnvironment(t)
	ctx := context.Background()
	request := defaultRequest("unit-idempotent")
	first, err := env.svc.Submit(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != "completed" || first.CommitVersion == 0 || first.Attempt != 1 {
		t.Fatalf("unexpected first response: %+v", first)
	}
	if first.Route.Path != "high_precision" || first.Route.ResolvedAlgorithm != "double_double_fixed" {
		t.Fatalf("unexpected route: %+v", first.Route)
	}
	if len(first.Partitions) != 4 || len(first.Groups) == 0 {
		t.Fatalf("missing persisted result artifacts: partitions=%d groups=%d", len(first.Partitions), len(first.Groups))
	}
	second, err := env.svc.Submit(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if second.CommitVersion != first.CommitVersion || second.ComputedAt != first.ComputedAt || second.Attempt != first.Attempt {
		t.Fatalf("duplicate request was re-executed: first=%+v second=%+v", first, second)
	}
	firstJSON, _ := json.Marshal(first.Groups)
	secondJSON, _ := json.Marshal(second.Groups)
	if string(firstJSON) != string(secondJSON) {
		t.Fatal("duplicate request did not return the stored groups")
	}
	partitions, err := env.svc.GetPartitions(ctx, request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if len(partitions) != 4 {
		t.Fatalf("expected four persisted partitions, got %d", len(partitions))
	}
	results, err := env.svc.GetResults(ctx, request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if results["partial_sums_persisted"].(int) != len(first.Groups) {
		t.Fatalf("unexpected persisted partial count: %+v", results["partial_sums_persisted"])
	}
	declaration, err := env.svc.GetErrorDeclaration(ctx, request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if declaration.Algorithm != "double_double_fixed" || declaration.CompensationDigest == "" {
		t.Fatalf("unexpected error declaration: %+v", declaration)
	}

	conflicting := request
	conflicting.Algorithm = "naive"
	_, err = env.svc.Submit(ctx, conflicting)
	business, ok := err.(*model.BusinessError)
	if !ok || business.Code != model.CodeConflictingRequest {
		t.Fatalf("expected E_CONFLICTING_REQUEST, got %v", err)
	}
}

func TestPartitionResultsAndAlgorithmsAreReproducible(t *testing.T) {
	env := newTestEnvironment(t)
	ctx := context.Background()
	byAlgorithm := make(map[string]*model.AggregateResponse)
	for _, algorithm := range []string{"naive", "kahan", "double_double_fixed"} {
		request := defaultRequest("unit-alg-" + algorithm)
		request.Algorithm = algorithm
		request.RelativeTolerance = 1e-3
		response, err := env.svc.Submit(ctx, request)
		if err != nil {
			t.Fatalf("%s: %v", algorithm, err)
		}
		if response.Route.ResolvedAlgorithm != algorithm {
			t.Fatalf("algorithm silently switched: requested=%s resolved=%s", algorithm, response.Route.ResolvedAlgorithm)
		}
		byAlgorithm[algorithm] = response
	}
	for _, algorithm := range []string{"naive", "kahan", "double_double_fixed"} {
		repeat := defaultRequest("unit-alg-" + algorithm + "-repeat")
		repeat.Algorithm = algorithm
		repeat.RelativeTolerance = 1e-3
		repeated, err := env.svc.Submit(ctx, repeat)
		if err != nil {
			t.Fatal(err)
		}
		firstMap := groupSumBits(byAlgorithm[algorithm])
		repeatMap := groupSumBits(repeated)
		if len(firstMap) != len(repeatMap) {
			t.Fatalf("repeated %s run changed group cardinality", algorithm)
		}
		for key, bits := range firstMap {
			if repeatMap[key] != bits {
				t.Fatalf("repeated %s run changed key %d: %016x != %016x", algorithm, key, bits, repeatMap[key])
			}
		}
	}
}

func TestRecoveryMarksIntentAbortedThenAllowsRetry(t *testing.T) {
	env := newTestEnvironment(t)
	ctx := context.Background()
	request := defaultRequest("unit-recover")
	normalized, businessErr := normalize(request)
	if businessErr != nil {
		t.Fatal(businessErr)
	}
	fingerprint, err := requestFingerprint(normalized)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := env.st.BeginAttempt(ctx, request.RequestID, fingerprint, model.JSON(normalized))
	if err != nil {
		t.Fatal(err)
	}
	if attempt != 1 {
		t.Fatalf("unexpected attempt number: %d", attempt)
	}
	_, err = env.svc.GetJob(ctx, request.RequestID)
	business, ok := err.(*model.BusinessError)
	if !ok || business.Code != model.CodeExecutionInProgress {
		t.Fatalf("running request exposed partial state: %v", err)
	}
	partials, err := env.st.PartialSums(ctx, request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if len(partials) != 0 {
		t.Fatal("partials became visible before atomic commit")
	}
	groups, err := env.st.Groups(ctx, request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 0 {
		t.Fatal("aggregates became visible before atomic commit")
	}

	report, err := env.svc.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.AbortedRequests) != 1 || report.AbortedRequests[0] != request.RequestID {
		t.Fatalf("recovery did not report aborted request: %+v", report)
	}
	if report.PendingIntentsRemaining != 0 {
		t.Fatalf("pending intents remain after recovery: %+v", report)
	}
	persistedReport, err := env.svc.RecoveryReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(persistedReport.AbortedRequests) != 1 || persistedReport.AbortedRequests[0] != request.RequestID {
		t.Fatalf("persisted recovery report did not reconstruct aborted request: %+v", persistedReport)
	}
	_, err = env.svc.GetJob(ctx, request.RequestID)
	business, ok = err.(*model.BusinessError)
	if !ok || business.Code != model.CodeExecutionAborted {
		t.Fatalf("expected E_EXECUTION_ABORTED after recovery, got %v", err)
	}

	response, err := env.svc.Submit(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Attempt != 2 || response.Status != "completed" {
		t.Fatalf("retry did not complete as attempt 2: %+v", response)
	}
}

func TestConcurrentDuplicateRequestsWaitAndShareResult(t *testing.T) {
	env := newTestEnvironment(t)
	request := defaultRequest("unit-concurrent")
	request.PartitionCount = 6
	responses := make([]*model.AggregateResponse, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			responses[index], errs[index] = env.svc.Submit(context.Background(), request)
		}(i)
	}
	wg.Wait()
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("duplicate %d failed: %v", i, errs[i])
		}
	}
	if responses[0].CommitVersion != responses[1].CommitVersion {
		t.Fatalf("concurrent duplicates did not share a commit: %d vs %d", responses[0].CommitVersion, responses[1].CommitVersion)
	}
}

func groupSumBits(response *model.AggregateResponse) map[int64]uint64 {
	out := make(map[int64]uint64, len(response.Groups))
	for _, group := range response.Groups {
		if group.Sum != nil {
			out[group.Key] = math.Float64bits(*group.Sum)
		}
	}
	return out
}

func TestAutoRouteSelectionDeclaresPrecisionBudgetAndSkewBasis(t *testing.T) {
	base := model.AggregateRequest{
		Algorithm:         "auto",
		PartitionCount:    4,
		SkewThreshold:     0.35,
		RelativeTolerance: 1e-14,
		ComputeBudget:     1,
	}
	dist := engine.Distribution{TotalRecords: 200, EstimatedJoinRows: 200, MaxRecordShare: 0.1, MagnitudeRatio: 10}
	strict, _ := selectRoute(base, dist)
	if strict.ResolvedAlgorithm != "double_double_fixed" || strict.Path != "high_precision" || strict.BudgetSatisfied {
		t.Fatalf("strict precision route did not dominate tiny budget: %+v", strict)
	}

	fastRequest := base
	fastRequest.RelativeTolerance = 1e-6
	fastRequest.ComputeBudget = 10000
	fast, _ := selectRoute(fastRequest, dist)
	if fast.ResolvedAlgorithm != "naive" || fast.Path != "fast" || !fast.BudgetSatisfied {
		t.Fatalf("budget-friendly route did not choose fast path: %+v", fast)
	}

	skewed := dist
	skewed.MaxRecordShare = 0.5
	skewRoute, _ := selectRoute(fastRequest, skewed)
	if skewRoute.ResolvedAlgorithm != "kahan" || skewRoute.Path != "high_precision" {
		t.Fatalf("skew did not trigger compensated high-precision path: %+v", skewRoute)
	}
}
