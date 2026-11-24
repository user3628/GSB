package engine_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"ampcheckpoint/internal/domain"
	"ampcheckpoint/internal/engine"
	"ampcheckpoint/internal/sample"
)

type rig struct {
	engine    *engine.Engine
	gen       *sample.Generator
	manifest  sample.Manifest
	dbPath    string
	sampleDir string
}

func newRig(t *testing.T) *rig {
	t.Helper()
	dir := t.TempDir()
	sampleDir := filepath.Join(dir, "sample")
	gen, err := sample.New(sample.DefaultSeed, sampleDir)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := gen.Ensure()
	if err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(dir, "checkpoints.db")
	eng, err := engine.Open(dbPath, gen)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return &rig{engine: eng, gen: gen, manifest: manifest, dbPath: dbPath, sampleDir: sampleDir}
}

func requireCode(t *testing.T, err error, want string) *domain.BusinessError {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s, got nil", want)
	}
	be, ok := err.(*domain.BusinessError)
	if !ok {
		t.Fatalf("expected business error %s, got %T: %v", want, err, err)
	}
	if be.Code != want {
		t.Fatalf("expected %s, got %s: %v", want, be.Code, be)
	}
	return be
}

func mustSubmitPolicy(t *testing.T, eng *engine.Engine, policy domain.PolicySnapshot) domain.Policy {
	t.Helper()
	result, err := eng.SubmitPolicy(context.Background(), policy)
	if err != nil {
		t.Fatalf("submit policy v%d: %v", policy.Version, err)
	}
	return result
}

func mustSubmitDelta(t *testing.T, eng *engine.Engine, delta domain.DeltaFragment) domain.Policy {
	t.Helper()
	result, err := eng.SubmitDelta(context.Background(), delta)
	if err != nil {
		t.Fatalf("submit delta base=%d: %v", delta.BaseVersion, err)
	}
	return result
}

func mustCheckpoint(t *testing.T, eng *engine.Engine, req domain.CheckpointRequest) domain.CheckpointResult {
	t.Helper()
	result, err := eng.CommitCheckpoint(context.Background(), req)
	if err != nil {
		t.Fatalf("checkpoint round=%d: %v", req.Round, err)
	}
	return result
}

func TestPolicyVersioningAndLossScale(t *testing.T) {
	r := newRig(t)
	policy := mustSubmitPolicy(t, r.engine, r.manifest.Policy)
	if policy.Version != 1 {
		t.Fatalf("policy version=%d", policy.Version)
	}
	if _, err := r.engine.SubmitPolicy(context.Background(), r.manifest.Policy); err != nil {
		requireCode(t, err, domain.ErrStaleSnapshot)
	}
	_, err := r.engine.SubmitDelta(context.Background(), domain.DeltaFragment{BaseVersion: 0, GeneratedAt: time.Now(), LossScale: &domain.LossScalePatch{Action: "grow"}})
	requireCode(t, err, domain.ErrStaleBase)

	grown := mustSubmitDelta(t, r.engine, domain.DeltaFragment{BaseVersion: 1, GeneratedAt: time.Now(), LossScale: &domain.LossScalePatch{Action: "grow"}})
	if grown.Version != 2 || grown.LossScaleFixed != 256*domain.FixedScale {
		t.Fatalf("grow result=%+v", grown.PolicySnapshot)
	}
	rolledBack := mustSubmitDelta(t, r.engine, domain.DeltaFragment{BaseVersion: 2, GeneratedAt: time.Now(), LossScale: &domain.LossScalePatch{Action: "rollback"}})
	if rolledBack.Version != 3 || rolledBack.LossScaleFixed != 128*domain.FixedScale {
		t.Fatalf("rollback result=%+v", rolledBack.PolicySnapshot)
	}

	overflowSeen := false
	for i := 0; i < 12; i++ {
		current, err := r.engine.CurrentPolicy(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		delta := domain.DeltaFragment{BaseVersion: current.Version, GeneratedAt: time.Now(), LossScale: &domain.LossScalePatch{Action: "grow"}}
		_, err = r.engine.SubmitDelta(context.Background(), delta)
		if err != nil {
			be := requireCode(t, err, domain.ErrLossScaleOverflow)
			if be.Context["overflow_threshold_fixed"] == nil {
				t.Fatalf("overflow context missing threshold: %#v", be.Context)
			}
			overflowSeen = true
			break
		}
	}
	if !overflowSeen {
		t.Fatal("loss scale never overflowed")
	}
	afterOverflow, err := r.engine.CurrentPolicy(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if afterOverflow.LossScaleFixed > afterOverflow.OverflowThresholdFixed {
		t.Fatalf("failed overflow mutated committed loss scale: %+v", afterOverflow.PolicySnapshot)
	}

	bad := r.manifest.Policy
	bad.Version = 100
	bad.Checksum = "not-the-checksum"
	_, err = r.engine.SubmitPolicy(context.Background(), bad)
	requireCode(t, err, domain.ErrChecksumMismatch)
}

func TestCheckpointResidencyEvictionBudgetPrecisionAndDerived(t *testing.T) {
	r := newRig(t)
	mustSubmitPolicy(t, r.engine, r.manifest.Policy)
	cp1 := mustCheckpoint(t, r.engine, domain.CheckpointRequest{Round: 1})
	if cp1.Version != 1 || len(cp1.Evictions) != 0 {
		t.Fatalf("first checkpoint=%+v", cp1)
	}
	pools := poolsByKind(t, r.engine, cp1.Version)
	if pools[domain.PoolVRAM].TotalBytes != 19_968 || pools[domain.PoolVRAM].TotalBytes > pools[domain.PoolVRAM].LimitBytes {
		t.Fatalf("unexpected initial VRAM: %+v", pools[domain.PoolVRAM])
	}
	shards1, err := r.engine.Shards(context.Background(), cp1.Version)
	if err != nil {
		t.Fatal(err)
	}
	if len(shards1.Copies) != 18 || countPool(shards1.Copies, domain.PoolVRAM) != 8 {
		t.Fatalf("unexpected copy set: copies=%d vram=%d", len(shards1.Copies), countPool(shards1.Copies, domain.PoolVRAM))
	}
	precision1, err := r.engine.Precision(context.Background(), cp1.Version)
	if err != nil {
		t.Fatal(err)
	}
	if precision1.CheckedShardCount != 8 || precision1.CumulativeAbsDeviationFixed <= 0 {
		t.Fatalf("precision report=%+v", precision1)
	}
	for _, report := range precision1.Shards {
		if !report.ConsistencyOK || len(report.Elements) != report.ElementCount || report.MaxRelativeDeviationPPM <= 0 {
			t.Fatalf("bad precision shard=%+v", report)
		}
	}

	mustSubmitDelta(t, r.engine, r.manifest.Delta)
	cp2 := mustCheckpoint(t, r.engine, domain.CheckpointRequest{Round: 2})
	if cp2.Version != 2 || len(cp2.Evictions) < 1 {
		t.Fatalf("expected eviction: %+v", cp2)
	}
	pools = poolsByKind(t, r.engine, cp2.Version)
	if pools[domain.PoolVRAM].TotalBytes > pools[domain.PoolVRAM].LimitBytes {
		t.Fatalf("VRAM exceeded: %+v", pools[domain.PoolVRAM])
	}
	shards2, err := r.engine.Shards(context.Background(), cp2.Version)
	if err != nil {
		t.Fatal(err)
	}
	for _, eviction := range cp2.Evictions {
		found := false
		for _, copy := range shards2.Copies {
			if copy.CopyID == eviction.CopyID {
				found = true
				if copy.Pool != domain.PoolPersistent || copy.Status != domain.StatusPersisted {
					t.Fatalf("evicted copy not persisted: %+v", copy)
				}
			}
		}
		if !found {
			t.Fatalf("eviction references missing copy: %s", eviction.CopyID)
		}
	}
	precision2, err := r.engine.Precision(context.Background(), cp2.Version)
	if err != nil {
		t.Fatal(err)
	}
	if precision2.CheckedShardCount != 10 || precision2.Shards[0].LossScaleFixed != 256*domain.FixedScale {
		t.Fatalf("loss-scale-derived precision not rebuilt: %+v", precision2)
	}
	derived, err := r.engine.Derived(context.Background(), cp2.Version)
	if err != nil {
		t.Fatal(err)
	}
	if len(derived) != 20 {
		t.Fatalf("derived copies=%d", len(derived))
	}
	for _, d := range derived {
		if !d.ConsistencyOK || len(d.DependencyRefs) < 4 || d.RecoveryBaseline.MasterDigest == "" {
			t.Fatalf("incomplete derived structure: %+v", d)
		}
		if d.RecoveryBaseline.Precision == domain.PrecisionFP16 && d.RecoveryBaseline.RestoredDigest == "" {
			t.Fatalf("FP16 derived baseline lacks declaration/restored digest ref: %+v", d)
		}
	}
	recovery2, err := r.engine.Recovery(context.Background(), cp2.Version)
	if err != nil {
		t.Fatal(err)
	}
	if !recovery2.CommittedOnly || len(recovery2.Baselines) != 20 || len(recovery2.AbortedOperations) != 0 {
		t.Fatalf("recovery report=%+v", recovery2)
	}

	mustSubmitPolicy(t, r.engine, r.manifest.TightPolicy)
	cp3 := mustCheckpoint(t, r.engine, domain.CheckpointRequest{Round: 3})
	if cp3.Version != 3 {
		t.Fatalf("cp3 version=%d", cp3.Version)
	}
	if cp3.Writes.UsedBytes > cp3.Writes.BudgetBytes || len(cp3.Writes.Skipped) == 0 || len(cp3.Writes.Queued) > cp3.Writes.QueueCapacity {
		t.Fatalf("budget scheduling violation: %+v", cp3.Writes)
	}
	if cp3.Writes.RemainingBytes != cp3.Writes.BudgetBytes-cp3.Writes.UsedBytes {
		t.Fatalf("remaining bytes accounting: %+v", cp3.Writes)
	}

	mustSubmitPolicy(t, r.engine, r.manifest.QueueFullPolicy)
	_, err = r.engine.CommitCheckpoint(context.Background(), domain.CheckpointRequest{Round: 4})
	queueErr := requireCode(t, err, domain.ErrWriteQueueFull)
	if queueErr.Context["queue_capacity"] != 2 {
		t.Fatalf("queue context=%#v", queueErr.Context)
	}
	latest, err := r.engine.LatestCheckpointVersion(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if latest != 3 {
		t.Fatalf("failed queue checkpoint advanced version to %d", latest)
	}

	timePolicy := r.manifest.TightPolicy
	timePolicy.Version = 30
	timePolicy.GeneratedAt = time.Now().UTC()
	timePolicy.WriteBudgetBytes = 80_000
	timePolicy.RoundTimeBudgetMS = 4
	timePolicy.TaskTimeMS = 2
	timePolicy.Checksum = ""
	mustSubmitPolicy(t, r.engine, timePolicy)
	cp4 := mustCheckpoint(t, r.engine, domain.CheckpointRequest{Round: 4})
	if cp4.Writes.TimeUsedMS != 4 || len(cp4.Writes.Written) != 2 || len(cp4.Writes.Skipped) == 0 {
		t.Fatalf("time budget not enforced: %+v", cp4.Writes)
	}
	for _, skipped := range cp4.Writes.Skipped {
		if skipped.Reason != "TIME_BUDGET" {
			t.Fatalf("unexpected time skip reason: %+v", skipped)
		}
	}
}

func TestVRAMPoolFullLeavesNoCommittedCheckpoint(t *testing.T) {
	r := newRig(t)
	mustSubmitPolicy(t, r.engine, r.manifest.VRAMFullPolicy)
	_, err := r.engine.CommitCheckpoint(context.Background(), domain.CheckpointRequest{Round: 1})
	be := requireCode(t, err, domain.ErrVRAMPoolFull)
	if be.Context["limit_bytes"] != int64(1024) || be.Context["required_bytes"] != int64(2048) {
		t.Fatalf("VRAM error context=%#v", be.Context)
	}
	latest, err := r.engine.LatestCheckpointVersion(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if latest != 0 {
		t.Fatalf("failed VRAM plan committed checkpoint %d", latest)
	}
	status, err := r.engine.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status["running_intents"] != 0 {
		t.Fatalf("failed pre-intent plan left an intent: %#v", status)
	}
}

func TestRecoveryAbortsUncommittedIntent(t *testing.T) {
	dir := t.TempDir()
	sampleDir := filepath.Join(dir, "sample")
	gen, err := sample.New(sample.DefaultSeed, sampleDir)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := gen.Ensure()
	if err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(dir, "checkpoints.db")
	eng, err := engine.Open(dbPath, gen)
	if err != nil {
		t.Fatal(err)
	}
	mustSubmitPolicy(t, eng, manifest.Policy)
	cp1 := mustCheckpoint(t, eng, domain.CheckpointRequest{Round: 1})
	mustSubmitDelta(t, eng, manifest.Delta)
	beforePools := poolsByKind(t, eng, cp1.Version)
	fault, err := eng.CommitCheckpoint(context.Background(), domain.CheckpointRequest{Round: 2, FaultAfterIntent: "evict"})
	if err != nil {
		t.Fatal(err)
	}
	if fault.FaultInjected != "evict" || fault.IntentID == "" || len(fault.Evictions) == 0 {
		t.Fatalf("fault result=%+v", fault)
	}
	latest, err := eng.LatestCheckpointVersion(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if latest != cp1.Version {
		t.Fatalf("fault committed checkpoint %d", latest)
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}

	gen2, err := sample.New(sample.DefaultSeed, sampleDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gen2.Ensure(); err != nil {
		t.Fatal(err)
	}
	reopened, err := engine.Open(dbPath, gen2)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	status, err := reopened.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status["recovered_intents"] != 1 || status["running_intents"] != 0 {
		t.Fatalf("recovery status=%#v", status)
	}
	latest, err = reopened.LatestCheckpointVersion(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if latest != cp1.Version {
		t.Fatalf("recovered latest version=%d want %d", latest, cp1.Version)
	}
	report, err := reopened.Recovery(context.Background(), cp1.Version)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.AbortedOperations) != 1 {
		t.Fatalf("aborted operations=%+v", report.AbortedOperations)
	}
	aborted := report.AbortedOperations[0]
	if aborted.OperationID != fault.IntentID || len(aborted.PendingShards) == 0 || len(aborted.AbortedEvictions) == 0 {
		t.Fatalf("aborted intent not recovered as PENDING/ABORTED: %+v", aborted)
	}
	afterPools := poolsByKind(t, reopened, cp1.Version)
	if beforePools[domain.PoolVRAM].TotalBytes != afterPools[domain.PoolVRAM].TotalBytes || beforePools[domain.PoolPersistent].TotalBytes != afterPools[domain.PoolPersistent].TotalBytes {
		t.Fatalf("committed pools changed after recovery: before=%+v after=%+v", beforePools, afterPools)
	}
}

func TestConcurrentReadersSeeCompleteImmutableVersion(t *testing.T) {
	r := newRig(t)
	mustSubmitPolicy(t, r.engine, r.manifest.Policy)
	cp1 := mustCheckpoint(t, r.engine, domain.CheckpointRequest{Round: 1})
	done := make(chan error, 16)
	for i := 0; i < 16; i++ {
		go func() {
			for j := 0; j < 30; j++ {
				ctx := context.Background()
				shards, err := r.engine.Shards(ctx, cp1.Version)
				if err != nil {
					done <- err
					return
				}
				pools, err := r.engine.Pools(ctx, cp1.Version)
				if err != nil {
					done <- err
					return
				}
				if shards.Version != cp1.Version || len(shards.Copies) != 18 || countPool(shards.Copies, domain.PoolVRAM) != 8 || len(pools) != 2 {
					done <- domain.BizError(domain.ErrInternal, "reader observed partial version", map[string]any{"version": shards.Version, "copies": len(shards.Copies)})
					return
				}
			}
			done <- nil
		}()
	}
	for round := int64(2); round <= 4; round++ {
		if _, err := r.engine.CommitCheckpoint(context.Background(), domain.CheckpointRequest{Round: round}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 16; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func poolsByKind(t *testing.T, eng *engine.Engine, version int64) map[domain.Pool]domain.PoolSummary {
	t.Helper()
	pools, err := eng.Pools(context.Background(), version)
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[domain.Pool]domain.PoolSummary, len(pools))
	for _, pool := range pools {
		out[pool.Pool] = pool
	}
	return out
}

func countPool(copies []domain.Copy, pool domain.Pool) int {
	count := 0
	for _, c := range copies {
		if c.Pool == pool {
			count++
		}
	}
	return count
}
