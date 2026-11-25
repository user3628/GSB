package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"mpckpt/internal/format"
	"mpckpt/internal/model"
	"mpckpt/internal/sample"
	"mpckpt/internal/store"
)

func newTestEngine(t *testing.T, queueCap int) *Engine {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	eng, err := New(st, format.DefaultCompatibilityMatrix(), queueCap)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

func submitStrategy(t *testing.T, eng *Engine, commitID string, strategy model.StrategySnapshot) {
	t.Helper()
	strategy.Checksum = strategy.ComputedChecksum()
	_, berr := eng.SubmitStrategy(StrategyRequest{CommitID: commitID, Strategy: strategy})
	if berr != nil {
		t.Fatalf("submit strategy: %v", berr)
	}
}

func encodeSample(t *testing.T, version int, formatName string) []byte {
	t.Helper()
	manifest := sample.Manifest(version)
	manifest.Format = formatName
	raw, berr := format.Encode(manifest)
	if berr != nil {
		t.Fatalf("encode sample: %v", berr)
	}
	return raw
}

func importSample(t *testing.T, eng *Engine, commitID string, version int, formatName string) map[string]any {
	t.Helper()
	raw := encodeSample(t, version, formatName)
	response, berr := eng.Import(model.ImportRequest{CommitID: commitID, Format: formatName, Data: raw})
	if berr != nil {
		t.Fatalf("import sample: %v", berr)
	}
	var decoded map[string]any
	_ = json.Unmarshal(response, &decoded)
	return decoded
}

func commitSample(t *testing.T, eng *Engine, commitID string) map[string]any {
	t.Helper()
	batch := sample.Build()
	response, berr := eng.CommitCheckpoint(CheckpointRequest{CommitID: commitID, Shards: batch.Shards, Baseline: batch.Baseline})
	if berr != nil {
		t.Fatalf("commit checkpoint: %v", berr)
	}
	var decoded map[string]any
	_ = json.Unmarshal(response, &decoded)
	return decoded
}

func TestSpecInvalidLeavesNoHeldState(t *testing.T) {
	eng := newTestEngine(t, 32)
	submitStrategy(t, eng, "strategy-1", sample.StrategyV1())
	shards := sample.Build().Shards
	shards[0].Precision = "BF16"
	_, berr := eng.CommitCheckpoint(CheckpointRequest{CommitID: "bad-spec", Shards: shards})
	if berr == nil || berr.Code != model.CodeSpecInvalid {
		t.Fatalf("expected E_SPEC_INVALID, got %v", berr)
	}
	_, stateErr := eng.State(0)
	if stateErr == nil || stateErr.Code != model.CodeNotFound {
		t.Fatalf("state changed after invalid spec: %v", stateErr)
	}
}

func TestImportCompatibilityPersistedAndDegradedDefaults(t *testing.T) {
	eng := newTestEngine(t, 32)
	submitStrategy(t, eng, "strategy-1", sample.StrategyV1())
	response := importSample(t, eng, "import-v1", 1, format.FormatJSONL)
	if response["verdict"] != "DEGRADE_ACCEPT" {
		t.Fatalf("verdict=%v", response["verdict"])
	}
	details, _ := response["details"].([]any)
	if len(details) < 7 {
		t.Fatalf("expected default pool and loss-scale details, got %v", details)
	}
	state, berr := eng.State(0)
	if berr != nil {
		t.Fatalf("state: %v", berr)
	}
	for _, shard := range state.Shards {
		if shard.Pool != model.PoolPersistent {
			t.Fatalf("degraded shard pool=%s", shard.Pool)
		}
	}
	rawReports, berr := eng.ImportReports(state.DatasetVersion)
	if berr != nil {
		t.Fatalf("reports: %v", berr)
	}
	if !bytes.Contains(rawReports, []byte("LOSS_SCALE_PRECISION_UPCAST")) || !bytes.Contains(rawReports, []byte("DEFAULT_POOL")) {
		t.Fatalf("persisted report lacks details: %s", rawReports)
	}
	decisions, berr := eng.CompatibilityDecisions()
	if berr != nil || !bytes.Contains(decisions, []byte("LOSS_SCALE_PRECISION_UPCAST")) || !bytes.Contains(decisions, []byte("DEFAULT_POOL")) {
		t.Fatalf("compatibility decisions not persisted: %s err=%v", decisions, berr)
	}
}

func TestRejectedCompatibilityDecisionIsPersisted(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	matrix := format.DefaultCompatibilityMatrix()
	matrix.Rules[format.FormatBinary][1] = format.VerdictReject
	eng, err := New(st, matrix, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	submitStrategy(t, eng, "strategy-1", sample.StrategyV1())
	raw := encodeSample(t, 1, format.FormatBinary)
	_, berr := eng.Import(model.ImportRequest{CommitID: "rejected-import", Format: format.FormatBinary, Data: raw})
	if berr == nil || berr.Code != model.CodeFormatUnsupported {
		t.Fatalf("expected E_FORMAT_UNSUPPORTED, got %v", berr)
	}
	decisions, berr := eng.CompatibilityDecisions()
	if berr != nil || !bytes.Contains(decisions, []byte("MATRIX_REJECT")) || !bytes.Contains(decisions, []byte("REJECT")) {
		t.Fatalf("rejected decision not persisted: %s err=%v", decisions, berr)
	}
}

func TestCorruptImportIsAtomicAndReportsIndex(t *testing.T) {
	eng := newTestEngine(t, 32)
	submitStrategy(t, eng, "strategy-1", sample.StrategyV1())
	importSample(t, eng, "valid-import", 2, format.FormatBinary)
	before, berr := eng.State(0)
	if berr != nil {
		t.Fatalf("state before: %v", berr)
	}
	raw := encodeSample(t, 2, format.FormatBinary)
	order := binary.LittleEndian
	offset := 28
	firstLen := int(order.Uint32(raw[offset : offset+4]))
	offset += 4 + firstLen
	secondLen := int(order.Uint32(raw[offset : offset+4]))
	raw[offset+4+secondLen-1] ^= 0xff
	_, berr = eng.Import(model.ImportRequest{CommitID: "corrupt-import", Format: format.FormatBinary, Data: raw})
	if berr == nil || berr.Code != model.CodeRecordCorrupt {
		t.Fatalf("expected E_RECORD_CORRUPT, got %v", berr)
	}
	if fmt.Sprint(berr.Context["record_index"]) != "2" {
		t.Fatalf("record index not reported: %v", berr.Context)
	}
	after, berr := eng.State(0)
	if berr != nil {
		t.Fatalf("state after: %v", berr)
	}
	if before.DatasetVersion != after.DatasetVersion || !sameShards(before.Shards, after.Shards) {
		t.Fatalf("failed import changed state: before=%+v after=%+v", before, after)
	}
}

func TestPoolEvictionAndFullError(t *testing.T) {
	eng := newTestEngine(t, 32)
	submitStrategy(t, eng, "strategy-1", sample.StrategyV1())
	importSample(t, eng, "import-v2", 2, format.FormatBinary)
	batch := sample.Build()
	response, berr := eng.Reside(ResideRequest{CommitID: "reside-1", Shard: batch.ExtraResident})
	if berr != nil {
		t.Fatalf("reside: %v", berr)
	}
	var decoded map[string]any
	_ = json.Unmarshal(response, &decoded)
	evictions, _ := decoded["evictions"].([]any)
	if len(evictions) != 1 {
		t.Fatalf("expected one eviction, got %v", decoded)
	}
	state, berr := eng.State(0)
	if berr != nil {
		t.Fatalf("state: %v", berr)
	}
	if state.PoolUsage["VRAM"] != 64 {
		t.Fatalf("VRAM usage=%d, want 64", state.PoolUsage["VRAM"])
	}
	huge := model.Shard{
		ID: "huge-fp16", Round: 9, Precision: model.PrecisionFP16, SizeBytes: 1024,
		Digest: sample.FP16Digest(batch.Shards[0].Digest), Pool: model.PoolVRAM,
		LastAccess: 999, SourceDigest: batch.Shards[0].Digest, Priority: 100,
	}
	_, berr = eng.Reside(ResideRequest{CommitID: "reside-too-large", Shard: huge})
	if berr == nil || berr.Code != model.CodePoolFull {
		t.Fatalf("expected E_POOL_FULL, got %v", berr)
	}
	if berr.Context["reclaimable_bytes"] != int64(64) {
		t.Fatalf("reclaimable bytes missing: %v", berr.Context)
	}
	after, _ := eng.State(0)
	if after.DatasetVersion != state.DatasetVersion {
		t.Fatalf("failed residency changed version: %d -> %d", state.DatasetVersion, after.DatasetVersion)
	}
}

func TestCheckpointCanTriggerEviction(t *testing.T) {
	eng := newTestEngine(t, 32)
	submitStrategy(t, eng, "strategy-1", sample.StrategyV1())
	importSample(t, eng, "import-v2", 2, format.FormatBinary)
	batch := sample.Build()
	response, berr := eng.CommitCheckpoint(CheckpointRequest{
		CommitID: "checkpoint-add-resident", Shards: []model.Shard{batch.ExtraResident}, Baseline: batch.Baseline,
	})
	if berr != nil {
		t.Fatalf("checkpoint with resident addition: %v", berr)
	}
	var decoded map[string]any
	_ = json.Unmarshal(response, &decoded)
	evictions, _ := decoded["evictions"].([]any)
	if len(evictions) != 1 {
		t.Fatalf("checkpoint did not evict: %v", decoded)
	}
	state, berr := eng.State(0)
	if berr != nil || state.PoolUsage["VRAM"] != 64 {
		t.Fatalf("unexpected post-checkpoint pool: %+v err=%v", state.PoolUsage, berr)
	}
}

func TestHistoricalVersionRemainsReadable(t *testing.T) {
	eng := newTestEngine(t, 32)
	submitStrategy(t, eng, "strategy-1", sample.StrategyV1())
	importSample(t, eng, "import-v2", 2, format.FormatBinary)
	batch := sample.Build()
	if _, berr := eng.CommitCheckpoint(CheckpointRequest{
		CommitID: "checkpoint-evicts", Shards: []model.Shard{batch.ExtraResident}, Baseline: batch.Baseline,
	}); berr != nil {
		t.Fatalf("checkpoint: %v", berr)
	}
	oldState, berr := eng.State(1)
	if berr != nil {
		t.Fatalf("old state: %v", berr)
	}
	newState, berr := eng.State(2)
	if berr != nil {
		t.Fatalf("new state: %v", berr)
	}
	oldByID := map[string]model.Shard{}
	newByID := map[string]model.Shard{}
	for _, shard := range oldState.Shards {
		oldByID[shard.ID] = shard
	}
	for _, shard := range newState.Shards {
		newByID[shard.ID] = shard
	}
	if oldByID["r1-embed-fp16-a"].Pool != model.PoolVRAM || oldByID["r1-embed-fp16-a"].Status != model.StatusResident {
		t.Fatalf("historical version was mutated: %+v", oldByID["r1-embed-fp16-a"])
	}
	if newByID["r1-embed-fp16-a"].Pool != model.PoolPersistent || newByID["r3-ffn-fp16-extra"].Status != model.StatusResident {
		t.Fatalf("new version residency incorrect: old=%+v extra=%+v", newByID["r1-embed-fp16-a"], newByID["r3-ffn-fp16-extra"])
	}
	if _, berr := eng.Precision(1); berr != nil {
		t.Fatalf("historical precision report missing: %v", berr)
	}
}

func TestBaselineShardCannotBeEvicted(t *testing.T) {
	eng := newTestEngine(t, 32)
	submitStrategy(t, eng, "strategy-1", sample.StrategyV1())
	batch := sample.Build()
	baseline := append([]string(nil), batch.Baseline...)
	baseline = append(baseline, batch.Shards[4].ID)
	_, berr := eng.CommitCheckpoint(CheckpointRequest{CommitID: "checkpoint-protect", Shards: batch.Shards, Baseline: baseline})
	if berr != nil {
		t.Fatalf("checkpoint: %v", berr)
	}
	_, berr = eng.Reside(ResideRequest{CommitID: "protected-eviction", Shard: batch.ExtraResident})
	if berr == nil || berr.Code != model.CodePoolFull {
		t.Fatalf("expected E_POOL_FULL, got %v", berr)
	}
	if berr.Context["reclaimable_bytes"] != int64(0) {
		t.Fatalf("baseline shard should not be reclaimable: %v", berr.Context)
	}
}

func TestPrecisionDriftAbortsCommit(t *testing.T) {
	eng := newTestEngine(t, 32)
	strategy := sample.StrategyV1()
	strategy.MaxAbsErrorFixed = 1
	submitStrategy(t, eng, "strategy-1", strategy)
	raw := encodeSample(t, 2, format.FormatBinary)
	_, berr := eng.Import(model.ImportRequest{CommitID: "drift", Format: format.FormatBinary, Data: raw})
	if berr == nil || berr.Code != model.CodePrecisionDrift {
		t.Fatalf("expected E_PRECISION_DRIFT, got %v", berr)
	}
	if berr.Context["shard_id"] == "" {
		t.Fatalf("drift has no shard id: %v", berr.Context)
	}
	_, stateErr := eng.State(0)
	if stateErr == nil || stateErr.Code != model.CodeNotFound {
		t.Fatalf("drift changed state: %v", stateErr)
	}
}

func TestStaleSnapshotAndBase(t *testing.T) {
	eng := newTestEngine(t, 32)
	submitStrategy(t, eng, "strategy-1", sample.StrategyV1())
	stale := sample.StrategyV1()
	stale.Checksum = stale.ComputedChecksum()
	_, berr := eng.SubmitStrategy(StrategyRequest{CommitID: "strategy-stale", Strategy: stale})
	if berr == nil || berr.Code != model.CodeStaleSnapshot {
		t.Fatalf("expected E_STALE_SNAPSHOT, got %v", berr)
	}
	importSample(t, eng, "import-v2", 2, format.FormatBinary)
	batch := sample.Build()
	_, berr = eng.SubmitIncremental(IncrementalRequest{
		CommitID: "stale-base", BaseVersion: 0, Changes: []model.IncrementalPatch{batch.IncrementPatch},
	})
	if berr == nil || berr.Code != model.CodeStaleBase {
		t.Fatalf("expected E_STALE_BASE, got %v", berr)
	}
	stateBefore, _ := eng.State(0)
	response, berr := eng.SubmitIncremental(IncrementalRequest{
		CommitID: "valid-increment", BaseVersion: 1, GeneratedAt: "2026-01-02T04:05:06Z",
		Changes: []model.IncrementalPatch{batch.IncrementPatch},
	})
	if berr != nil {
		t.Fatalf("valid incremental: %v", berr)
	}
	var decoded map[string]any
	_ = json.Unmarshal(response, &decoded)
	if decoded["strategy_version"] != float64(2) {
		t.Fatalf("strategy did not advance: %v", decoded)
	}
	stateAfter, _ := eng.State(0)
	if stateAfter.DatasetVersion != stateBefore.DatasetVersion+1 {
		t.Fatalf("dataset version did not advance once")
	}
	_, berr = eng.SubmitIncremental(IncrementalRequest{
		CommitID: "stale-base-2", BaseVersion: 1, Changes: []model.IncrementalPatch{batch.IncrementPatch},
	})
	if berr == nil || berr.Code != model.CodeStaleBase {
		t.Fatalf("expected E_STALE_BASE after increment, got %v", berr)
	}
	stateAfterStale, _ := eng.State(0)
	if stateAfterStale.DatasetVersion != stateAfter.DatasetVersion {
		t.Fatalf("stale base changed state")
	}
}

func TestStrategyCapReductionCannotViolatePoolLimit(t *testing.T) {
	eng := newTestEngine(t, 32)
	submitStrategy(t, eng, "strategy-1", sample.StrategyV1())
	importSample(t, eng, "import-v2", 2, format.FormatBinary)
	lower := sample.StrategyV1()
	lower.Version = 2
	lower.GPUCapBytes = 32
	lower.Checksum = lower.ComputedChecksum()
	_, berr := eng.SubmitStrategy(StrategyRequest{CommitID: "strategy-too-small", Strategy: lower})
	if berr == nil || berr.Code != model.CodePoolFull {
		t.Fatalf("expected E_POOL_FULL, got %v", berr)
	}
	current, berr := eng.CurrentStrategy()
	if berr != nil || current.Version != 1 {
		t.Fatalf("failed snapshot changed active strategy: %+v err=%v", current, berr)
	}
	state, berr := eng.State(0)
	if berr != nil || state.PoolUsage["VRAM"] > current.GPUCapBytes {
		t.Fatalf("pool invariant violated: state=%+v strategy=%+v", state, current)
	}
}

func TestIdempotencyAndConflictingRequest(t *testing.T) {
	eng := newTestEngine(t, 32)
	submitStrategy(t, eng, "strategy-1", sample.StrategyV1())
	batch := sample.Build()
	first, berr := eng.CommitCheckpoint(CheckpointRequest{CommitID: "same-id", Shards: batch.Shards, Baseline: batch.Baseline})
	if berr != nil {
		t.Fatalf("first commit: %v", berr)
	}
	second, berr := eng.CommitCheckpoint(CheckpointRequest{CommitID: "same-id", Shards: batch.Shards, Baseline: batch.Baseline})
	if berr != nil {
		t.Fatalf("replay commit: %v", berr)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("replayed response differs\nfirst=%s\nsecond=%s", first, second)
	}
	stateBefore, _ := eng.State(0)
	altered := append([]model.Shard(nil), batch.Shards...)
	altered[0].SizeBytes++
	_, berr = eng.CommitCheckpoint(CheckpointRequest{CommitID: "same-id", Shards: altered, Baseline: batch.Baseline})
	if berr == nil || berr.Code != model.CodeConflictingRequest {
		t.Fatalf("expected E_CONFLICTING_REQUEST, got %v", berr)
	}
	stateAfter, _ := eng.State(0)
	if stateBefore.DatasetVersion != stateAfter.DatasetVersion {
		t.Fatalf("conflicting request changed state")
	}
}

func TestWriteQueueFullDoesNotCommit(t *testing.T) {
	eng := newTestEngine(t, 1)
	strategy := sample.StrategyV1()
	strategy.WriteBudgetBytes = 0
	strategy.TimeBudgetMillis = 0
	submitStrategy(t, eng, "strategy-1", strategy)
	batch := sample.Build()
	_, berr := eng.CommitCheckpoint(CheckpointRequest{CommitID: "queue-full", Shards: batch.Shards, Baseline: batch.Baseline})
	if berr == nil || berr.Code != model.CodeWriteQueueFull {
		t.Fatalf("expected E_WRITE_QUEUE_FULL, got %v", berr)
	}
	if berr.Context["queue_capacity"] != 1 {
		t.Fatalf("queue context missing: %v", berr.Context)
	}
	_, stateErr := eng.State(0)
	if stateErr == nil || stateErr.Code != model.CodeNotFound {
		t.Fatalf("queue failure changed state: %v", stateErr)
	}
}

func TestWritePlannerUsesPriorityRoundAndBudgets(t *testing.T) {
	tasks := []WriteTask{
		{ShardID: "low", SizeBytes: 100, Round: 1, Priority: 1},
		{ShardID: "high-round-2", SizeBytes: 100, Round: 2, Priority: 10},
		{ShardID: "high-round-1", SizeBytes: 100, Round: 1, Priority: 10},
	}
	selected, queued, berr := planWrites(nil, tasks, 8, 100, 1000)
	if berr != nil {
		t.Fatalf("plan writes: %v", berr)
	}
	if len(selected) != 1 || !selected["high-round-1"] {
		t.Fatalf("priority/round selection wrong: %v", selected)
	}
	if len(queued) != 2 || queued[0].ShardID != "high-round-2" || queued[1].ShardID != "low" {
		t.Fatalf("queue ordering wrong: %+v", queued)
	}
	selected, queued, berr = planWrites(nil, tasks, 8, 1000, 2)
	if berr != nil {
		t.Fatalf("time-budget plan: %v", berr)
	}
	if len(selected) != 2 || len(queued) != 1 {
		t.Fatalf("time budget not applied: selected=%v queued=%+v", selected, queued)
	}
}

func TestCheckpointPartialWriteLeavesBoundedPendingQueue(t *testing.T) {
	eng := newTestEngine(t, 32)
	strategy := sample.StrategyV1()
	strategy.WriteBudgetBytes = 100
	submitStrategy(t, eng, "strategy-1", strategy)
	batch := sample.Build()
	response, berr := eng.CommitCheckpoint(CheckpointRequest{CommitID: "partial-write", Shards: batch.Shards, Baseline: batch.Baseline})
	if berr != nil {
		t.Fatalf("partial checkpoint: %v", berr)
	}
	var decoded map[string]any
	_ = json.Unmarshal(response, &decoded)
	written, _ := decoded["written_shards"].([]any)
	queued, _ := decoded["queued_shards"].([]any)
	if len(written) != 1 || written[0] != "r1-embed-fp16-a" || len(queued) != len(batch.Shards)-1 {
		t.Fatalf("unexpected write scheduling: written=%v queued=%v", written, queued)
	}
	state, berr := eng.State(0)
	if berr != nil {
		t.Fatalf("state: %v", berr)
	}
	pending := 0
	for _, shard := range state.Shards {
		if shard.Status == model.StatusPending {
			pending++
		}
	}
	if pending != len(batch.Shards)-1 || state.PoolUsage["VRAM"] != 64 {
		t.Fatalf("pending/pool state incorrect: pending=%d usage=%v", pending, state.PoolUsage)
	}
}

func TestDerivedCascadeRebuildOnPrecisionChange(t *testing.T) {
	eng := newTestEngine(t, 32)
	submitStrategy(t, eng, "strategy-1", sample.StrategyV1())
	commitSample(t, eng, "checkpoint-1")
	before, berr := eng.Derived(0)
	if berr != nil {
		t.Fatalf("derived before: %v", berr)
	}
	beforeByID := map[string]DerivedState{}
	for _, item := range before.Items {
		beforeByID[item.ShardID] = item
	}
	batch := sample.Build()
	_, berr = eng.SubmitIncremental(IncrementalRequest{
		CommitID: "increment-1", BaseVersion: 1, GeneratedAt: "2026-01-02T04:05:06Z",
		Changes: []model.IncrementalPatch{batch.IncrementPatch},
	})
	if berr != nil {
		t.Fatalf("increment: %v", berr)
	}
	after, berr := eng.Derived(0)
	if berr != nil {
		t.Fatalf("derived after: %v", berr)
	}
	afterByID := map[string]DerivedState{}
	for _, item := range after.Items {
		afterByID[item.ShardID] = item
	}
	changed := batch.IncrementPatch.ShardID
	if beforeByID[changed].PrimaryDigest == afterByID[changed].PrimaryDigest {
		t.Fatalf("primary digest did not cascade")
	}
	var reason string
	err := eng.store.DB().QueryRow(`SELECT reason FROM derivation_events WHERE data_version = ? AND shard_id = ? ORDER BY id DESC LIMIT 1`, after.DatasetVersion, changed).Scan(&reason)
	if err != nil {
		t.Fatalf("read derivation event: %v", err)
	}
	if reason != "PRECISION_CHANGED" {
		t.Fatalf("derivation reason=%s, want PRECISION_CHANGED", reason)
	}
}

func TestBaselineDigestCascadesOnMasterContentChange(t *testing.T) {
	eng := newTestEngine(t, 32)
	submitStrategy(t, eng, "strategy-1", sample.StrategyV1())
	commitSample(t, eng, "checkpoint-1")
	before, berr := eng.Derived(0)
	if berr != nil {
		t.Fatalf("derived before: %v", berr)
	}
	beforeDigests := map[string]string{}
	for _, item := range before.Items {
		beforeDigests[item.ShardID] = item.BaselineDigest
	}
	newDigest := strings.Repeat("f", 64)
	response, berr := eng.SubmitIncremental(IncrementalRequest{
		CommitID: "increment-master-digest", BaseVersion: 1, GeneratedAt: "2026-01-02T04:05:06Z",
		Changes: []model.IncrementalPatch{{ShardID: "r1-embed-master", Digest: newDigest}},
	})
	if berr != nil {
		t.Fatalf("increment master digest: %v; response=%s", berr, response)
	}
	after, berr := eng.Derived(0)
	if berr != nil {
		t.Fatalf("derived after: %v", berr)
	}
	if len(after.Items) != len(before.Items) {
		t.Fatalf("derived item count changed")
	}
	for _, item := range after.Items {
		if item.ShardID == "r1-embed-master" {
			continue
		}
		if beforeDigests[item.ShardID] == item.BaselineDigest {
			t.Fatalf("baseline digest did not cascade for %s", item.ShardID)
		}
	}
}

func TestCrashRecoveryRestoresIntent(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng, err := New(st, format.DefaultCompatibilityMatrix(), 32)
	if err != nil {
		t.Fatal(err)
	}
	submitStrategy(t, eng, "strategy-1", sample.StrategyV1())
	commitSample(t, eng, "checkpoint-1")
	faultRaw, berr := eng.PrepareCrashFault("crash-1")
	if berr != nil {
		t.Fatalf("prepare crash: %v", berr)
	}
	var fault map[string]any
	_ = json.Unmarshal(faultRaw, &fault)
	pendingID := fmt.Sprint(fault["pending_shard"])
	evictedID := fmt.Sprint(fault["evicted_shard"])
	if pendingID == "" || evictedID == "" {
		t.Fatalf("fault did not create both partial states: %s", faultRaw)
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}

	st2, err := store.Open(dir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	recovered, err := New(st2, format.DefaultCompatibilityMatrix(), 32)
	if err != nil {
		t.Fatalf("recover engine: %v", err)
	}
	defer recovered.Close()
	events, berr := recovered.Recovery()
	if berr != nil || len(events) != 1 {
		t.Fatalf("recovery events=%v err=%v", events, berr)
	}
	if !bytes.Contains(events[0].Details, []byte(pendingID)) || !bytes.Contains(events[0].Details, []byte(evictedID)) {
		t.Fatalf("recovery details missing ids: %s", events[0].Details)
	}
	var pendingStatus, evictedPool, evictedStatus string
	if err := st2.DB().QueryRow(`SELECT status FROM shards WHERE id = ?`, pendingID).Scan(&pendingStatus); err != nil {
		t.Fatal(err)
	}
	if err := st2.DB().QueryRow(`SELECT pool, status FROM shards WHERE id = ?`, evictedID).Scan(&evictedPool, &evictedStatus); err != nil {
		t.Fatal(err)
	}
	if pendingStatus != string(model.StatusPending) {
		t.Fatalf("pending shard status=%s", pendingStatus)
	}
	if evictedPool != string(model.PoolVRAM) || evictedStatus != string(model.StatusResident) {
		t.Fatalf("eviction not restored: pool=%s status=%s", evictedPool, evictedStatus)
	}
	state, berr := recovered.State(0)
	if berr != nil || state.DatasetVersion != 1 {
		t.Fatalf("committed version changed after recovery: %+v err=%v", state, berr)
	}
}

func sameShards(left, right []model.Shard) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func TestRecoveryQueueScanDoesNotLosePendingShards(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng, err := New(st, format.DefaultCompatibilityMatrix(), 32)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	ctx := context.Background()
	_, err = st.DB().ExecContext(ctx, `INSERT INTO shards(id, round_no, precision, size_bytes, digest, pool, last_access, source_digest, momentum_digest, priority, status, data_version, updated_at)
		VALUES ('pending-shard', 1, 'FP32', 128, ?, 'PERSISTED', 1, '', '', 1, 'PENDING', 1, '2026-01-02T03:04:05Z')`, strings.Repeat("a", 64))
	if err != nil {
		t.Fatalf("seed pending: %v", err)
	}
	if err := eng.loadPendingQueue(); err != nil {
		t.Fatalf("load pending: %v", err)
	}
	if len(eng.queue) != 1 || eng.queue[0].ShardID != "pending-shard" {
		t.Fatalf("pending queue not recovered: %+v", eng.queue)
	}
}
