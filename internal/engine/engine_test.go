package engine_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"leasearbiter/internal/core"
	"leasearbiter/internal/engine"
	"leasearbiter/internal/loopback"
	"leasearbiter/internal/orchestrator"
	"leasearbiter/internal/sample"
	"leasearbiter/internal/store"
)

type rig struct {
	engine *engine.Engine
	store  *store.Store
	server *loopback.Server
}

func fixedClock() time.Time {
	return time.UnixMilli(sample.BaseEpochMS + 100_000)
}

func newRig(t *testing.T, handlerConfig loopback.HandlerConfig, timeout time.Duration, config orchestrator.Config) *rig {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	stateStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stateStore.Close() })
	server, err := loopback.Start("127.0.0.1:0", handlerConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	coordinator, err := orchestrator.New(config)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := loopback.NewDispatcher(server.URL(), timeout)
	app, err := engine.New(engine.Config{
		Store:       stateStore,
		Dispatcher:  dispatcher,
		Coordinator: coordinator,
		Clock:       fixedClock,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &rig{engine: app, store: stateStore, server: server}
}

func snapshotFor(version int64, dataset core.Dataset) core.Snapshot {
	snapshot := core.Snapshot{Version: version, GeneratedAtMS: sample.BaseEpochMS + 60_000 + version, Dataset: dataset}
	digest, err := core.ComputeSnapshotDigest(snapshot)
	if err != nil {
		panic(err)
	}
	snapshot.Digest = digest
	return snapshot
}

func emptySnapshot(version int64) core.Snapshot {
	dataset := sample.Generate(sample.DefaultSeed)
	dataset.Observations = nil
	return snapshotFor(version, dataset)
}

func TestSnapshotIngestHealReadAndRejections(t *testing.T) {
	r := newRig(t, loopback.HandlerConfig{}, time.Second, orchestrator.Config{Concurrency: 4, QueueCapacity: 4, Budget: 2 * time.Second})
	ctx := context.Background()
	submission, bizErr := r.engine.SubmitSnapshot(ctx, emptySnapshot(1))
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	if submission.SnapshotVersion != 1 || submission.StateVersion != 1 {
		t.Fatalf("unexpected submission: %+v", submission)
	}

	dataset := sample.Generate(sample.DefaultSeed)
	report, bizErr := r.engine.Ingest(ctx, dataset, engine.ProcessOptions{Source: "integration", RunHeal: true})
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	if !report.Committed || report.StateVersion != 2 {
		t.Fatalf("unexpected batch report: %+v", report)
	}
	if len(report.Results) != 4 || len(report.Heals) != 3 || len(report.Deferred) != 0 {
		t.Fatalf("results=%d heals=%d deferred=%d", len(report.Results), len(report.Heals), len(report.Deferred))
	}
	for _, heal := range report.Heals {
		if heal.Status != "SENT" {
			t.Fatalf("heal %s status = %s, want SENT", heal.Decision.HolderID, heal.Status)
		}
	}
	heals, _, _ := r.server.Handler().Counts()
	if heals != 3 {
		t.Fatalf("loopback endpoint received %d heals, want 3", heals)
	}

	read, bizErr := r.engine.Read(ctx, report.StateVersion)
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	if read.StateVersion != 2 || read.SnapshotVersion != 1 || len(read.Results) != 4 {
		t.Fatalf("unexpected read result: %+v", read)
	}
	holderStatus := make(map[string]core.HolderStatus)
	for _, holder := range read.Holders {
		holderStatus[holder.ID] = holder.Status
	}
	if holderStatus["holder-delta"] != core.HolderRevoked {
		t.Fatalf("delta status = %s, want REVOKED", holderStatus["holder-delta"])
	}

	_, bizErr = r.engine.Read(ctx, 3)
	if bizErr == nil || bizErr.Code != core.CodeStaleRead || bizErr.CurrentVersion != 2 || bizErr.ObservedVersion != 3 {
		t.Fatalf("stale read error = %#v", bizErr)
	}

	stale := sample.Snapshot(1, sample.DefaultSeed)
	_, bizErr = r.engine.SubmitSnapshot(ctx, stale)
	if bizErr == nil || bizErr.Code != core.CodeStaleSnapshot || bizErr.CurrentVersion != 1 {
		t.Fatalf("stale snapshot error = %#v", bizErr)
	}
	staleAndMalformed := sample.Snapshot(1, sample.DefaultSeed)
	staleAndMalformed.Digest = "sha256:bad"
	_, bizErr = r.engine.SubmitSnapshot(ctx, staleAndMalformed)
	if bizErr == nil || bizErr.Code != core.CodeStaleSnapshot {
		t.Fatalf("stale version must win over malformed digest, got %#v", bizErr)
	}

	badDigest := emptySnapshot(2)
	badDigest.Digest = core.SHA256Digest([]byte("wrong"))
	_, bizErr = r.engine.SubmitSnapshot(ctx, badDigest)
	if bizErr == nil || bizErr.Code != core.CodeSnapshotInvalid {
		t.Fatalf("bad digest error = %#v", bizErr)
	}
	still, bizErr := r.engine.Read(ctx, 2)
	if bizErr != nil || still.SnapshotVersion != 1 || len(still.Results) != 4 {
		t.Fatalf("old snapshot not fully available after failed replacement: %+v %v", still, bizErr)
	}

	replacementDataset := sample.Generate(sample.DefaultSeed)
	replacementDataset.Holders = replacementDataset.Holders[:1]
	replacementDataset.Observations = nil
	replacement := snapshotFor(2, replacementDataset)
	replacementResult, bizErr := r.engine.SubmitSnapshot(ctx, replacement)
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	if replacementResult.StateVersion != 3 || replacementResult.SnapshotVersion != 2 {
		t.Fatalf("replacement result: %+v", replacementResult)
	}
	replaced, bizErr := r.engine.Read(ctx, 3)
	if bizErr != nil || len(replaced.Holders) != 1 || replaced.Holders[0].ID != "holder-alpha" || len(replaced.Results) != 0 {
		t.Fatalf("replacement was not whole: %+v %v", replaced, bizErr)
	}

	invalidDataset := sample.Generate(sample.DefaultSeed)
	invalidDataset.Holders[0].LeaseDurationMS = 0
	invalid := snapshotFor(3, invalidDataset)
	_, bizErr = r.engine.SubmitSnapshot(ctx, invalid)
	if bizErr == nil || bizErr.Code != core.CodeObservationInvalid {
		t.Fatalf("invalid dataset error = %#v", bizErr)
	}
	still, bizErr = r.engine.Read(ctx, 3)
	if bizErr != nil || still.SnapshotVersion != 2 || len(still.Holders) != 1 {
		t.Fatalf("state changed after invalid dataset: %+v %v", still, bizErr)
	}
}

func TestQueueFullRejectsBatchWithoutChangingState(t *testing.T) {
	r := newRig(t, loopback.HandlerConfig{}, time.Second, orchestrator.Config{Concurrency: 1, QueueCapacity: 0, Budget: time.Second})
	ctx := context.Background()
	if _, bizErr := r.engine.SubmitSnapshot(ctx, emptySnapshot(1)); bizErr != nil {
		t.Fatal(bizErr)
	}
	report, bizErr := r.engine.Ingest(ctx, sample.Generate(sample.DefaultSeed), engine.ProcessOptions{RunHeal: true})
	if bizErr == nil || bizErr.Code != core.CodeQueueFull {
		t.Fatalf("expected %s, got %#v", core.CodeQueueFull, bizErr)
	}
	if report.Committed {
		t.Fatalf("queue-full report must not be committed: %+v", report)
	}
	current, currentErr := r.engine.Current(ctx)
	if currentErr != nil || current.Version != 1 || len(current.Observations) != 0 || len(current.Results) != 0 {
		t.Fatalf("queue-full batch changed state: %+v %v", current, currentErr)
	}
	intents, err := r.store.Intents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	aborted := 0
	for _, intent := range intents {
		if intent.State == store.IntentAborted {
			aborted++
		}
	}
	if aborted != 1 {
		t.Fatalf("aborted intents = %d, want 1", aborted)
	}
}

func TestCrossBatchDedupeMismatchAndInvalidBatchAtomicity(t *testing.T) {
	r := newRig(t, loopback.HandlerConfig{}, time.Second, orchestrator.Config{Concurrency: 4, QueueCapacity: 4, Budget: 2 * time.Second})
	ctx := context.Background()
	if _, bizErr := r.engine.SubmitSnapshot(ctx, emptySnapshot(1)); bizErr != nil {
		t.Fatal(bizErr)
	}
	dataset := sample.Generate(sample.DefaultSeed)
	first, bizErr := r.engine.Ingest(ctx, dataset, engine.ProcessOptions{RunHeal: false})
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	second, bizErr := r.engine.Ingest(ctx, dataset, engine.ProcessOptions{RunHeal: false})
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	if second.StateVersion != first.StateVersion+1 {
		t.Fatalf("versions: first=%d second=%d", first.StateVersion, second.StateVersion)
	}
	var alpha core.RebuildResult
	for _, result := range second.Results {
		if result.HolderID == "holder-alpha" {
			alpha = result
		}
	}
	if alpha.UniqueReportCount != 6 || alpha.DuplicateDeliveryCount != 8 {
		t.Fatalf("dedupe after replay: unique=%d duplicates=%d, want 6/8", alpha.UniqueReportCount, alpha.DuplicateDeliveryCount)
	}
	if len(alpha.Rebuilt) != 6 {
		t.Fatalf("alpha rebuilt rows = %d, want 6", len(alpha.Rebuilt))
	}

	conflicting := sample.WithConflictingDuplicate(dataset)
	_, bizErr = r.engine.Ingest(ctx, conflicting, engine.ProcessOptions{RunHeal: false})
	if bizErr == nil || bizErr.Code != core.CodeReportMismatch {
		t.Fatalf("mismatch error = %#v", bizErr)
	}
	current, bizErr := r.engine.Current(ctx)
	if bizErr != nil || current.Version != second.StateVersion {
		t.Fatalf("mismatch changed state: version=%d err=%v", current.Version, bizErr)
	}
	for _, record := range current.Observations {
		if record.HolderID == "holder-alpha" && record.ReportSeq == 2 && record.Digest == conflicting.Observations[2].Digest {
			t.Fatal("conflicting digest replaced the first record")
		}
	}

	invalid := sample.Generate(sample.DefaultSeed)
	invalid.Observations[0].Digest = "bad"
	_, bizErr = r.engine.Ingest(ctx, invalid, engine.ProcessOptions{RunHeal: false})
	if bizErr == nil || bizErr.Code != core.CodeObservationInvalid {
		t.Fatalf("invalid batch error = %#v", bizErr)
	}
	afterInvalid, bizErr := r.engine.Current(ctx)
	if bizErr != nil || afterInvalid.Version != second.StateVersion {
		t.Fatalf("invalid batch was persisted: version=%d err=%v", afterInvalid.Version, bizErr)
	}
}

func TestHealTimeoutCommitsAuditStateAndReturnsBusinessCode(t *testing.T) {
	r := newRig(t,
		loopback.HandlerConfig{DefaultInject: "block"},
		60*time.Millisecond,
		orchestrator.Config{Concurrency: 4, QueueCapacity: 4, Budget: 2 * time.Second},
	)
	ctx := context.Background()
	if _, bizErr := r.engine.SubmitSnapshot(ctx, emptySnapshot(1)); bizErr != nil {
		t.Fatal(bizErr)
	}
	report, bizErr := r.engine.Ingest(ctx, sample.Generate(sample.DefaultSeed), engine.ProcessOptions{RunHeal: true})
	if bizErr == nil || bizErr.Code != core.CodeHealTimeout {
		t.Fatalf("expected heal timeout, got %#v", bizErr)
	}
	if !report.Committed || report.StateVersion != 2 || len(report.Deferred) != 0 {
		t.Fatalf("timeout report: %+v", report)
	}
	if len(report.Heals) != 3 {
		t.Fatalf("heals = %d, want 3", len(report.Heals))
	}
	for _, heal := range report.Heals {
		if heal.Status != "TIMEOUT" || heal.ErrorCode != core.CodeHealTimeout {
			t.Fatalf("heal not recorded as timeout: %+v", heal)
		}
	}
	read, readErr := r.engine.Read(ctx, report.StateVersion)
	if readErr != nil || len(read.Heals) != 3 {
		t.Fatalf("timeout audit state not readable: %+v %v", read, readErr)
	}
}

type blockingDispatcher struct{}

func (blockingDispatcher) DispatchRaw(ctx context.Context, decision core.HealDecision, _ loopback.DispatchOptions, _ string, _ int64) (core.HealReceipt, *core.BizError) {
	<-ctx.Done()
	return core.HealReceipt{}, core.Biz(core.CodeHealTimeout, "budget deadline closed dispatch")
}

func TestBudgetDeferralPreservesDedupeAndDriftInCommittedState(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "budget.db")
	stateStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer stateStore.Close()
	coordinator, err := orchestrator.New(orchestrator.Config{Concurrency: 1, QueueCapacity: 8, Budget: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	app, err := engine.New(engine.Config{Store: stateStore, Dispatcher: blockingDispatcher{}, Coordinator: coordinator, Clock: fixedClock})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, bizErr := app.SubmitSnapshot(ctx, emptySnapshot(1)); bizErr != nil {
		t.Fatal(bizErr)
	}
	report, bizErr := app.Ingest(ctx, sample.Generate(sample.DefaultSeed), engine.ProcessOptions{RunHeal: true})
	if bizErr == nil || bizErr.Code != core.CodeHealTimeout {
		t.Fatalf("expected timeout from started dispatch, got %#v", bizErr)
	}
	if !report.Committed || len(report.Deferred) < 2 {
		t.Fatalf("expected committed deferred work, report=%+v", report)
	}
	if len(report.Results) != 4 {
		t.Fatalf("deferred drift results lost: %d", len(report.Results))
	}
	for i := 1; i < len(report.Deferred); i++ {
		previous, current := report.Deferred[i-1], report.Deferred[i]
		if previous.DriftMagnitudeMS < current.DriftMagnitudeMS {
			t.Fatalf("deferred order not by magnitude: %+v", report.Deferred)
		}
		if previous.DriftMagnitudeMS == current.DriftMagnitudeMS && previous.HolderID > current.HolderID {
			t.Fatalf("deferred tie not by holder ID: %+v", report.Deferred)
		}
	}
	current, bizErr := app.Current(ctx)
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	if len(current.Observations) != 18 {
		t.Fatalf("committed observations = %d, want 18", len(current.Observations))
	}
	if len(current.Results) != 4 || len(current.Deferred) != len(report.Deferred) {
		t.Fatalf("committed drift/deferred state missing: %+v", current)
	}
}

func TestRecoveryAbortsPreparedIntentAndKeepsLastCompleteVersion(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "recovery.db")
	stateStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	firstIntent, err := stateStore.BeginIntent(ctx, "snapshot_switch", map[string]any{"snapshot_version": 1})
	if err != nil {
		t.Fatal(err)
	}
	firstState := core.NewState()
	firstState.Version = firstIntent.TargetVersion
	firstState.SnapshotVersion = 1
	firstState.SnapshotDigest = core.SHA256Digest([]byte("complete"))
	if err := stateStore.CommitIntent(ctx, firstIntent.ID, firstState); err != nil {
		t.Fatal(err)
	}
	prepared, err := stateStore.BeginIntent(ctx, "batch_rebuild", map[string]any{"holder": "holder-alpha", "action": "SHORTEN_LEASE"})
	if err != nil {
		t.Fatal(err)
	}
	if err := stateStore.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	current, err := reopened.CurrentState(ctx)
	if err != nil || current.Version != 1 || current.SnapshotDigest != firstState.SnapshotDigest {
		t.Fatalf("last complete state unavailable: %+v %v", current, err)
	}
	report, err := reopened.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.LastCommittedVersion != 1 || len(report.PreparedIntents) != 1 {
		t.Fatalf("unexpected recovery report: %+v", report)
	}
	if report.PreparedIntents[0].ID != prepared.ID || report.PreparedIntents[0].Outcome != "ABORTED" {
		t.Fatalf("prepared intent not aborted: %+v", report.PreparedIntents[0])
	}
	if report.VersionDiff.BaseVersion != 1 || report.VersionDiff.IntendedTarget != 2 || report.VersionDiff.LastCommittedVersion != 1 {
		t.Fatalf("version diff: %+v", report.VersionDiff)
	}
	aborted, err := reopened.Intent(ctx, prepared.ID)
	if err != nil || aborted.State != store.IntentAborted {
		t.Fatalf("intent state = %+v err=%v", aborted, err)
	}
	still, err := reopened.CurrentState(ctx)
	if err != nil || still.Version != 1 {
		t.Fatalf("recovery advanced state: %+v %v", still, err)
	}

	nextIntent, err := reopened.BeginIntent(ctx, "batch_rebuild", map[string]any{"after": "recovery"})
	if err != nil {
		t.Fatal(err)
	}
	nextState := still.Clone()
	nextState.Version = nextIntent.TargetVersion
	if err := reopened.CommitIntent(ctx, nextIntent.ID, nextState); err != nil {
		t.Fatalf("writer did not resume after recovery: %v", err)
	}
}

func TestConcurrentReadersSeeOneCompleteVersion(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "reads.db")
	stateStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer stateStore.Close()
	ctx := context.Background()
	intent, err := stateStore.BeginIntent(ctx, "snapshot_switch", map[string]any{"snapshot_version": 1})
	if err != nil {
		t.Fatal(err)
	}
	state := core.NewState()
	state.Version = intent.TargetVersion
	state.SnapshotVersion = 1
	state.SnapshotDigest = core.SHA256Digest([]byte("version-one"))
	state.Holders = []core.Holder{{ID: "h1", LeaseDurationMS: 1000, MaxRenewals: 3, Status: core.HolderActive, HealStrategy: core.HealAuto}}
	if err := stateStore.CommitIntent(ctx, intent.ID, state); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			read, err := stateStore.StateAtVersion(ctx, 1)
			if err != nil {
				errCh <- err
				return
			}
			if read.Version != 1 || read.SnapshotDigest != state.SnapshotDigest || len(read.Holders) != 1 {
				errCh <- context.Canceled
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent read inconsistency: %v", err)
	}
}

func TestStoreEnforcesSingleWriterProcess(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "single-writer.db")
	first, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Open(dbPath); err == nil {
		t.Fatal("second writer unexpectedly acquired the same database")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("lock was not released on close: %v", err)
	}
	_ = second.Close()
}
