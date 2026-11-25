package prefixcache

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func openTestEngine(t *testing.T, path string) *Engine {
	t.Helper()
	engine, err := Open(testContext(t), path, WithBusyTimeout(5*time.Second), WithLockTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

func testDBPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "cache.db")
}

func initializeDataset(t *testing.T, engine *Engine, seed int64) (*CommitResult, Dataset) {
	t.Helper()
	dataset := GenerateDataset(seed)
	result, err := engine.Initialize(testContext(t), dataset.Policy, dataset.Requests)
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	return result, dataset
}

func requireCode(t *testing.T, err error, code string) *Error {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error %s, got nil", code)
	}
	var typed *Error
	if !errors.As(err, &typed) {
		t.Fatalf("expected *Error, got %T: %v", err, err)
	}
	if typed.Code != code {
		t.Fatalf("expected code %s, got %s: %v", code, typed.Code, typed)
	}
	return typed
}

func TestNormalizePlanDeterminismAndExplicitShortBlocks(t *testing.T) {
	normalized := NormalizeText("  ＡＢＣ\t\n１２３  ")
	if normalized != "abc 123" {
		t.Fatalf("normalized = %q, want %q", normalized, "abc 123")
	}

	policy := DefaultPolicy()
	request := Request{
		ID:         "req-plan",
		TemplateID: "tpl",
		Messages: []Message{
			{Turn: 1, Role: "system", Language: "en-US", Text: "   "},
			{Turn: 1, Role: "user", Language: "en-US", Text: "ABCDEF"},
		},
	}
	first, err := BuildRequestPlan(request, policy)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildRequestPlan(request, policy)
	if err != nil {
		t.Fatal(err)
	}
	firstJSON, err := CanonicalRequestPlanJSON(first)
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := CanonicalRequestPlanJSON(second)
	if err != nil {
		t.Fatal(err)
	}
	if string(firstJSON) != string(secondJSON) {
		t.Fatal("same request and policy produced different serialized plans")
	}
	if len(first.Blocks) != 3 {
		t.Fatalf("blocks = %d, want 3", len(first.Blocks))
	}
	if first.Blocks[0].Cacheable || first.Blocks[0].InvalidReason != "empty_message" || first.Blocks[0].CharSpan != 0 {
		t.Fatalf("empty message was not explicitly marked: %+v", first.Blocks[0])
	}
	if first.Blocks[2].Cacheable || first.Blocks[2].InvalidReason != "short_block" || first.Blocks[2].CharSpan != 2 {
		t.Fatalf("short block was not explicitly marked: %+v", first.Blocks[2])
	}
	for _, prefix := range first.Prefixes {
		if prefix.Cacheable {
			t.Fatalf("prefix crossing an empty/short block must be uncacheable: %+v", prefix)
		}
	}
}

func TestHitAdjudicationAndStaleRead(t *testing.T) {
	engine := openTestEngine(t, testDBPath(t))
	initResult, _ := initializeDataset(t, engine, DefaultSeed)
	if initResult.Version != 1 || initResult.Kind != "bootstrap" {
		t.Fatalf("unexpected bootstrap result: %+v", initResult)
	}

	base, err := engine.Hit(testContext(t), HitQuery{ObservedVersion: 1, RequestID: "req-base"})
	if err != nil {
		t.Fatalf("base hit: %v", err)
	}
	if base.Decision != DecisionExact {
		t.Fatalf("base decision = %s, want exact; evidence=%+v", base.Decision, base.Evidence)
	}
	if base.Evidence.ThresholdMargin != 1-base.Evidence.Threshold {
		t.Fatalf("exact margin = %v", base.Evidence.ThresholdMargin)
	}

	near := DemoNearPrefix()
	merged, err := engine.Hit(testContext(t), HitQuery{ObservedVersion: 1, Request: &near})
	if err != nil {
		t.Fatalf("near hit: %v", err)
	}
	if merged.Decision != DecisionMerged {
		t.Fatalf("near decision = %s, want merged; evidence=%+v reasons=%v", merged.Decision, merged.Evidence, merged.Reasons)
	}
	if merged.Evidence.Similarity < merged.Evidence.Threshold || merged.Evidence.ThresholdMargin < 0 {
		t.Fatalf("merged evidence does not retain threshold margin: %+v", merged.Evidence)
	}
	if merged.Evidence.CommonPrefixBlocks == 0 || merged.Evidence.QueryBlocks == 0 || merged.Evidence.CachedBlocks == 0 {
		t.Fatalf("merged evidence missing block counts: %+v", merged.Evidence)
	}

	unrelated := DemoUnrelatedQuery()
	miss, err := engine.Hit(testContext(t), HitQuery{ObservedVersion: 1, Request: &unrelated})
	if err != nil {
		t.Fatalf("unrelated hit: %v", err)
	}
	if miss.Decision != DecisionNotReusable {
		t.Fatalf("unrelated decision = %s, want not_reusable; evidence=%+v", miss.Decision, miss.Evidence)
	}

	_, err = engine.Hit(testContext(t), HitQuery{ObservedVersion: 0, RequestID: "req-base"})
	stale := requireCode(t, err, CodeStaleVersion)
	if stale.CurrentVersion != 1 || stale.ObservedVersion != 0 {
		t.Fatalf("stale versions = current %d observed %d", stale.CurrentVersion, stale.ObservedVersion)
	}
}

func TestIncrementalInvalidationAndUnaffectedSlotReuse(t *testing.T) {
	engine := openTestEngine(t, testDBPath(t))
	first, _ := initializeDataset(t, engine, DefaultSeed)
	before, err := engine.Status(testContext(t))
	if err != nil {
		t.Fatal(err)
	}

	threshold := 0.83
	hitPatch, err := engine.ApplyPatch(testContext(t), 1, PolicyPatch{MergeThreshold: &threshold})
	if err != nil {
		t.Fatalf("threshold patch: %v", err)
	}
	if hitPatch.InvalidatedStage != StageHit {
		t.Fatalf("stage = %s, want hit", hitPatch.InvalidatedStage)
	}
	if hitPatch.SlotDigest != first.SlotDigest || hitPatch.ReusedSlotCount != hitPatch.SlotCount {
		t.Fatalf("hit-only change did not reuse slots: old digest=%s new=%s reused=%d/%d", first.SlotDigest, hitPatch.SlotDigest, hitPatch.ReusedSlotCount, hitPatch.SlotCount)
	}
	if len(hitPatch.Invalidations) == 0 {
		t.Fatal("expected hit-stage invalidations")
	}
	for _, invalidation := range hitPatch.Invalidations {
		if invalidation.Field != "merge_threshold" || invalidation.Stage != StageHit || invalidation.RequestID == "" {
			t.Fatalf("unexpected invalidation: %+v", invalidation)
		}
	}
	after, err := engine.Status(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if after.StageDigests.Normalization != before.StageDigests.Normalization ||
		after.StageDigests.Fingerprint != before.StageDigests.Fingerprint ||
		after.StageDigests.Slotting != before.StageDigests.Slotting {
		t.Fatalf("threshold changed an upstream digest: before=%+v after=%+v", before.StageDigests, after.StageDigests)
	}

	minLen := 20
	slotPatch, err := engine.ApplyPatch(testContext(t), hitPatch.Version, PolicyPatch{MinPrefixLen: &minLen})
	if err != nil {
		t.Fatalf("min prefix patch: %v", err)
	}
	if slotPatch.InvalidatedStage != StageSlotting || slotPatch.ReusedSlotCount != 0 {
		t.Fatalf("slotting patch stage=%s reused=%d", slotPatch.InvalidatedStage, slotPatch.ReusedSlotCount)
	}
	if len(slotPatch.Invalidations) == 0 || slotPatch.Invalidations[0].Field != "min_prefix_len" {
		t.Fatalf("unexpected slotting invalidations: %+v", slotPatch.Invalidations)
	}

	granularity := 6
	fingerprintPatch, err := engine.ApplyPatch(testContext(t), slotPatch.Version, PolicyPatch{Granularity: &granularity})
	if err != nil {
		t.Fatalf("granularity patch: %v", err)
	}
	if fingerprintPatch.InvalidatedStage != StageFingerprint || fingerprintPatch.ReusedSlotCount != 0 {
		t.Fatalf("fingerprint patch stage=%s reused=%d", fingerprintPatch.InvalidatedStage, fingerprintPatch.ReusedSlotCount)
	}
}

func TestReplacementAndRollback(t *testing.T) {
	engine := openTestEngine(t, testDBPath(t))
	initResult, dataset := initializeDataset(t, engine, DefaultSeed)

	next := dataset.Policy
	next.Version = 2
	next.MergeThreshold = 0.9
	replacement, err := engine.SubmitPolicy(testContext(t), PolicySubmission{
		Policy: next,
		Mode:   "replace",
	})
	if err != nil {
		t.Fatalf("replacement: %v", err)
	}
	if replacement.Kind != "replacement" || replacement.InvalidatedStage != StageNormalization || replacement.ReusedSlotCount != 0 {
		t.Fatalf("unexpected replacement result: %+v", replacement)
	}
	if len(replacement.Invalidations) == 0 {
		t.Fatal("replacement did not emit invalidations")
	}

	rollback := dataset.Policy
	rollback.Version = 1
	_, err = engine.SubmitPolicy(testContext(t), PolicySubmission{Policy: rollback, Mode: "replace"})
	stale := requireCode(t, err, CodeStaleVersion)
	if stale.CurrentVersion != 2 || stale.ObservedVersion != 1 {
		t.Fatalf("rollback stale versions current=%d observed=%d", stale.CurrentVersion, stale.ObservedVersion)
	}

	_, err = engine.SubmitPolicy(testContext(t), PolicySubmission{Policy: next, Mode: "replace"})
	requireCode(t, err, CodeStaleVersion)

	currentStatus, err := engine.Status(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	advance := currentStatus.Policy
	advance.Version = currentStatus.CurrentVersion + 1
	advance.MergeThreshold = 0.66
	result, err := engine.SubmitPolicy(testContext(t), PolicySubmission{
		Policy:     advance,
		Mode:       "advance",
		BaseDigest: currentStatus.PolicyDigest,
	})
	if err != nil {
		t.Fatalf("normal full-policy advance: %v", err)
	}
	if result.Kind != "advance" || result.InvalidatedStage != StageHit {
		t.Fatalf("unexpected advance result: %+v", result)
	}
	if initResult.SlotDigest == replacement.SlotDigest {
		t.Fatal("replacement unexpectedly preserved the old slot digest")
	}
}

func TestInterruptedCommitRollsBackAndResumes(t *testing.T) {
	engine := openTestEngine(t, testDBPath(t))
	initializeDataset(t, engine, DefaultSeed)
	wantFailure := errors.New("simulated process stop")
	engine.beforeCommit = func(commitInfo) error { return wantFailure }

	threshold := 0.8
	_, err := engine.ApplyPatch(testContext(t), 1, PolicyPatch{MergeThreshold: &threshold})
	interrupted := requireCode(t, err, CodeInterrupted)
	if interrupted.Cause == nil {
		t.Fatal("interrupted error lost the cause")
	}
	status, err := engine.Status(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if status.CurrentVersion != 1 {
		t.Fatalf("rollback left current version %d", status.CurrentVersion)
	}
	_, err = engine.Snapshot(testContext(t), 2)
	requireCode(t, err, CodeNotFound)

	engine.beforeCommit = nil
	recovered, err := engine.ApplyPatch(testContext(t), 1, PolicyPatch{MergeThreshold: &threshold})
	if err != nil {
		t.Fatalf("resume after interruption: %v", err)
	}
	if recovered.Version != 2 {
		t.Fatalf("recovered version = %d, want 2", recovered.Version)
	}
}

func TestReadersNeverSeeHalfCommittedVersion(t *testing.T) {
	engine := openTestEngine(t, testDBPath(t))
	initializeDataset(t, engine, DefaultSeed)
	entered := make(chan struct{})
	release := make(chan struct{})
	engine.beforeCommit = func(commitInfo) error {
		close(entered)
		<-release
		return nil
	}

	done := make(chan error, 1)
	go func() {
		threshold := 0.84
		_, err := engine.ApplyPatch(testContext(t), 1, PolicyPatch{MergeThreshold: &threshold})
		done <- err
	}()
	<-entered
	status, err := engine.Status(testContext(t))
	if err != nil {
		t.Fatalf("status while commit paused: %v", err)
	}
	if status.CurrentVersion != 1 {
		t.Fatalf("reader saw uncommitted version %d", status.CurrentVersion)
	}
	_, err = engine.Snapshot(testContext(t), 2)
	requireCode(t, err, CodeNotFound)
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("paused commit: %v", err)
	}
	status, err = engine.Status(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if status.CurrentVersion != 2 {
		t.Fatalf("committed version = %d, want 2", status.CurrentVersion)
	}
}

func TestConcurrentCommitsOnlyOneContiguousAdvance(t *testing.T) {
	path := testDBPath(t)
	first := openTestEngine(t, path)
	initializeDataset(t, first, DefaultSeed)
	second := openTestEngine(t, path)

	start := make(chan struct{})
	type outcome struct {
		result *CommitResult
		err    error
	}
	outcomes := make(chan outcome, 2)
	var wg sync.WaitGroup
	run := func(engine *Engine, threshold float64) {
		defer wg.Done()
		<-start
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		value := threshold
		result, err := engine.ApplyPatch(ctx, 1, PolicyPatch{MergeThreshold: &value})
		outcomes <- outcome{result: result, err: err}
	}
	wg.Add(2)
	go run(first, 0.8)
	go run(second, 0.81)
	close(start)
	wg.Wait()
	close(outcomes)

	successes := 0
	stales := 0
	for outcome := range outcomes {
		if outcome.err == nil {
			successes++
			if outcome.result.Version != 2 {
				t.Fatalf("success version = %d", outcome.result.Version)
			}
			continue
		}
		var typed *Error
		if errors.As(outcome.err, &typed) && typed.Code == CodeStaleVersion {
			stales++
		} else {
			t.Fatalf("unexpected concurrent error: %v", outcome.err)
		}
	}
	if successes != 1 || stales != 1 {
		t.Fatalf("successes=%d stales=%d", successes, stales)
	}
	status, err := first.Status(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if status.CurrentVersion != 2 || status.AllocationSequence != 2 {
		t.Fatalf("head after race version=%d sequence=%d", status.CurrentVersion, status.AllocationSequence)
	}
}

func TestSlotConflictsAreTypedAndBudgetIsNeverExceeded(t *testing.T) {
	path := testDBPath(t)
	engine := openTestEngine(t, path)
	dataset := GenerateDataset(DefaultSeed)
	dataset.Policy.SlotCount = 0
	_, err := engine.Initialize(testContext(t), dataset.Policy, dataset.Requests)
	conflict := requireCode(t, err, CodeSlotConflict)
	if conflict.RequestID == "" || conflict.SlotNo != 0 {
		t.Fatalf("slot conflict lacks request/slot: %+v", conflict)
	}

	otherPath := filepath.Join(t.TempDir(), "budget.db")
	budgetEngine := openTestEngine(t, otherPath)
	dataset.Policy = DefaultPolicy()
	dataset.Policy.OccupancyBudget = 3
	result, err := budgetEngine.Initialize(testContext(t), dataset.Policy, dataset.Requests)
	if err != nil {
		t.Fatalf("budget initialize: %v", err)
	}
	if result.SlotCount > 3 {
		t.Fatalf("slot count %d exceeds budget 3", result.SlotCount)
	}
	foundBudget := false
	for _, eviction := range result.Evictions {
		if eviction.Reason == "occupancy_budget" {
			foundBudget = true
		}
	}
	if !foundBudget {
		t.Fatalf("expected occupancy budget evictions, got %+v", result.Evictions)
	}
	snapshot, err := budgetEngine.Snapshot(testContext(t), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Slots) > snapshot.Policy.OccupancyBudget {
		t.Fatalf("snapshot occupancy %d exceeds budget %d", len(snapshot.Slots), snapshot.Policy.OccupancyBudget)
	}
}

func TestRequestUpsertReusesUnchangedPrefixSlots(t *testing.T) {
	engine := openTestEngine(t, testDBPath(t))
	first, dataset := initializeDataset(t, engine, DefaultSeed)
	updated := dataset.Requests[0]
	updated.Messages = append(append([]Message(nil), updated.Messages...), Message{Turn: 2, Role: "user", Language: "en-US", Text: "Add one sentence about recovery."})
	result, err := engine.ImportRequests(testContext(t), 1, []Request{updated}, false)
	if err != nil {
		t.Fatalf("import request: %v", err)
	}
	if result.Kind != "reallocate" || result.InvalidatedStage != StageSlotting {
		t.Fatalf("unexpected request import result: %+v", result)
	}
	if result.ReusedSlotCount == 0 {
		t.Fatal("unchanged prefixes did not reuse slots")
	}
	if len(result.Invalidations) != 1 || result.Invalidations[0].RequestID != updated.ID || result.Invalidations[0].Field != "request_set" {
		t.Fatalf("request invalidation not traceable: %+v", result.Invalidations)
	}
	if first.SlotDigest == "" {
		t.Fatal("bootstrap slot digest missing")
	}
}
