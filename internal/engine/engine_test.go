package engine

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"checkpointengine/internal/model"
	"checkpointengine/internal/store"
)

type testEnv struct {
	t       *testing.T
	engine  *Engine
	store   *store.Store
	path    string
	dataDir string
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	dataDir := filepath.Join(dir, "data")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	fixed := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)
	eng, err := New(st, Options{DataDir: dataDir, Now: func() time.Time { return fixed }})
	if err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	env := &testEnv{t: t, engine: eng, store: st, path: path, dataDir: dataDir}
	if _, err := eng.Recover(context.Background()); err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return env
}

func (e *testEnv) reopen() *testEnv {
	e.t.Helper()
	_ = e.store.Close()
	st, err := store.Open(e.path)
	if err != nil {
		e.t.Fatal(err)
	}
	fixed := time.Date(2026, 11, 24, 1, 0, 0, 0, time.UTC)
	eng, err := New(st, Options{DataDir: e.dataDir, Now: func() time.Time { return fixed }})
	if err != nil {
		_ = st.Close()
		e.t.Fatal(err)
	}
	return &testEnv{t: e.t, engine: eng, store: st, path: e.path, dataDir: e.dataDir}
}

func readJSONFile[T any](t *testing.T, path string) T {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return value
}

func sampleSnapshot(t *testing.T, name string) model.SnapshotRequest {
	return readJSONFile[model.SnapshotRequest](t, filepath.Join("..", "..", "samples", name))
}

func sampleIncrement(t *testing.T, name string) model.IncrementRequest {
	return readJSONFile[model.IncrementRequest](t, filepath.Join("..", "..", "samples", name))
}

func apiCode(err error) string {
	if err == nil {
		return ""
	}
	var apiErr *model.APIError
	if errors.As(err, &apiErr) {
		return apiErr.Code
	}
	return ""
}

func rawInt(v int64) json.RawMessage   { return json.RawMessage([]byte(fmtInt(v))) }
func rawText(v string) json.RawMessage { b, _ := json.Marshal(v); return b }
func fmtInt(v int64) string            { b, _ := json.Marshal(v); return string(b) }
func intPtr(v int) *int                { return &v }
func int64Ptr(v int64) *int64          { return &v }

func testPolicy(version, vram, writeBudget int64, queue int, preference string) model.SnapshotRequest {
	return model.SnapshotRequest{
		Version: version, GeneratedAt: "2026-11-24T00:00:00Z",
		VRAMLimitBytes: rawInt(vram), WriteBudgetBytesPerRound: rawInt(writeBudget),
		TimeBudgetMS: rawInt(10), WriteQueueCapacity: intPtr(queue),
		LossScaleInitial: rawText("1024"), LossScaleGrowth: rawText("2"),
		PrecisionAbsLimitNanos: rawInt(2_000_000), PrecisionRelLimitPPM: rawInt(50_000),
		WritePreference: preference,
	}
}

func testPair(round int, submission string, base int64, fp16Bytes, fp32Bytes int64, lastAccess int64) model.IncrementRequest {
	bits := []uint16{0x6000}
	nanos := []int64{500_000_000}
	return model.IncrementRequest{
		SubmissionID: submission, BaseVersion: base, Round: round,
		Shards: []model.ShardInput{
			{ShardID: shardID(round, "fp16"), ParameterID: "p", Precision: model.PrecisionFP16,
				Bytes: fp16Bytes, FP16Bits: bits, LastAccessMS: int64Ptr(lastAccess), WriteMS: 1},
			{ShardID: shardID(round, "fp32"), ParameterID: "p", Precision: model.PrecisionFP32,
				Bytes: fp32Bytes, FP32Nanos: nanos, LastAccessMS: int64Ptr(lastAccess), WriteMS: 1,
				Priority: 100},
		},
	}
}

func shardID(round int, suffix string) string {
	return "r" + fmtInt(int64(round)) + "." + suffix
}

func TestSampleFlowAndBusinessCodes(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	snap1, err := env.engine.SubmitSnapshot(ctx, sampleSnapshot(t, "policy_v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	if snap1.StateRevision != 1 {
		t.Fatalf("snapshot revision = %d, want 1", snap1.StateRevision)
	}
	if _, err := env.engine.SubmitSnapshot(ctx, sampleSnapshot(t, "policy_v1.json")); apiCode(err) != model.CodeStaleSnapshot {
		t.Fatalf("duplicate snapshot code = %s, want %s", apiCode(err), model.CodeStaleSnapshot)
	}

	r1, err := env.engine.SubmitIncrement(ctx, sampleIncrement(t, "round1_increment.json"))
	if err != nil {
		t.Fatal(err)
	}
	if r1.Revision != 2 || len(r1.Writes) != 2 || len(r1.Deferred) != 2 || len(r1.Evictions) != 0 {
		t.Fatalf("unexpected round1 result: revision=%d writes=%d deferred=%d evictions=%d",
			r1.Revision, len(r1.Writes), len(r1.Deferred), len(r1.Evictions))
	}
	if r1.RemainingBytes != 128 {
		t.Fatalf("round1 remaining bytes = %d, want 128", r1.RemainingBytes)
	}
	pools, _, err := env.engine.Pools(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := poolBytes(pools, model.PoolVRAM); got != 1792 {
		t.Fatalf("round1 VRAM bytes = %d, want 1792", got)
	}

	r2, err := env.engine.SubmitIncrement(ctx, sampleIncrement(t, "round2_increment.json"))
	if err != nil {
		t.Fatal(err)
	}
	if r2.Revision != 3 || len(r2.Evictions) != 2 || len(r2.Writes) != 2 || len(r2.Deferred) != 4 {
		t.Fatalf("unexpected round2 result: revision=%d evictions=%d writes=%d deferred=%d",
			r2.Revision, len(r2.Evictions), len(r2.Writes), len(r2.Deferred))
	}
	if r2.Evictions[0].ShardID != "r1.attn.fp16" || r2.Evictions[1].ShardID != "r1.mlp.fp16" {
		t.Fatalf("unexpected eviction order: %+v", r2.Evictions)
	}
	pools, _, err = env.engine.Pools(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := poolBytes(pools, model.PoolVRAM); got != 2000 {
		t.Fatalf("round2 VRAM bytes = %d, want 2000", got)
	}
	if got := poolBytes(pools, model.PoolPersisted); got != 9376 {
		t.Fatalf("round2 persisted bytes = %d, want 9376", got)
	}
	rounds, err := env.engine.RoundSummaries(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(rounds) != 1 || rounds[0].Status != "INCOMPLETE" || rounds[0].PendingShards != 2 {
		t.Fatalf("unexpected round1 consistency: %+v", rounds)
	}
	pres, err := env.engine.PrecisionResults(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pres {
		if !p.Passed || p.MaxAbsDeviationNanos != 0 || p.CheckMode != "NUMERIC_RNE_FIXED_NANOS" {
			t.Fatalf("unexpected precision result: %+v", p)
		}
	}

	_, err = env.engine.SubmitIncrement(ctx, sampleIncrement(t, "round3_drift_increment.json"))
	if apiCode(err) != model.CodePrecisionDrift {
		t.Fatalf("drift code = %s, want %s", apiCode(err), model.CodePrecisionDrift)
	}
	state, err := env.engine.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.Revision != 3 {
		t.Fatalf("drift submission changed revision to %d", state.Revision)
	}
	r3Shards, err := env.engine.ListShards(ctx, 3, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(r3Shards.Shards) != 0 {
		t.Fatalf("drift submission persisted shards: %+v", r3Shards.Shards)
	}

	r4, err := env.engine.SubmitIncrement(ctx, sampleIncrement(t, "round4_complete_increment.json"))
	if err != nil {
		t.Fatal(err)
	}
	if r4.Revision != 4 || len(r4.Writes) != 2 || len(r4.Deferred) != 4 {
		t.Fatalf("unexpected round4 result: revision=%d writes=%d deferred=%d", r4.Revision, len(r4.Writes), len(r4.Deferred))
	}
	round4, err := env.engine.RoundSummaries(ctx, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(round4) != 1 || round4[0].Status != "PASS" || round4[0].WrittenShards != 2 {
		t.Fatalf("round4 did not complete durably: %+v", round4)
	}

	snap2, err := env.engine.SubmitSnapshot(ctx, sampleSnapshot(t, "policy_v2.json"))
	if err != nil {
		t.Fatal(err)
	}
	if snap2.StateRevision != 5 {
		t.Fatalf("snapshot2 revision = %d, want 5", snap2.StateRevision)
	}
	pools, _, err = env.engine.Pools(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := poolBytes(pools, model.PoolVRAM); got != 1200 {
		t.Fatalf("post-policy2 VRAM bytes = %d, want 1200", got)
	}

	replayed, err := env.engine.SubmitIncrement(ctx, sampleIncrement(t, "round1_increment.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.IdempotentReplay || replayed.Revision != 2 {
		t.Fatalf("idempotent replay = %+v", replayed)
	}
	conflict := sampleIncrement(t, "round1_increment.json")
	conflict.SubmissionID = "sub-round-2"
	if _, err := env.engine.SubmitIncrement(ctx, conflict); apiCode(err) != model.CodeConflictingSubmit {
		t.Fatalf("conflict code = %s, want %s", apiCode(err), model.CodeConflictingSubmit)
	}
	staleBase := sampleIncrement(t, "round3_drift_increment.json")
	if _, err := env.engine.SubmitIncrement(ctx, staleBase); apiCode(err) != model.CodeStaleBase {
		t.Fatalf("stale base code = %s, want %s", apiCode(err), model.CodeStaleBase)
	}
}

func poolBytes(pools []model.PoolUsage, name string) int64 {
	for _, p := range pools {
		if p.Pool == name {
			return p.UsedBytes
		}
	}
	return -1
}

func TestShardValidationAndAtomicity(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	if _, err := env.engine.SubmitSnapshot(ctx, testPolicy(1, 100_000, 100_000, 20, model.WritePreferenceFP32First)); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name   string
		mutate func(*model.IncrementRequest)
		code   string
	}{
		{"negative bytes", func(r *model.IncrementRequest) { r.Shards[0].Bytes = -1 }, model.CodeShardInvalid},
		{"bad precision", func(r *model.IncrementRequest) { r.Shards[0].Precision = "BF16" }, model.CodeShardInvalid},
		{"duplicate id", func(r *model.IncrementRequest) { r.Shards[1].ShardID = r.Shards[0].ShardID }, model.CodeShardInvalid},
		{"digest mismatch", func(r *model.IncrementRequest) { r.Shards[0].Digest = "not-the-payload-digest" }, model.CodeShardInvalid},
		{"missing fp16 relation", func(r *model.IncrementRequest) { r.Shards = r.Shards[1:] }, model.CodeShardInvalid},
		{"bad pool", func(r *model.IncrementRequest) { r.Shards[0].TargetPool = "CPU" }, model.CodeShardInvalid},
	}
	for i, tc := range cases {
		req := testPair(1, "validation-"+fmtInt(int64(i)), 1, 100, 200, 10)
		tc.mutate(&req)
		if _, err := env.engine.SubmitIncrement(ctx, req); apiCode(err) != tc.code {
			t.Fatalf("%s code = %s, want %s", tc.name, apiCode(err), tc.code)
		}
		state, _ := env.engine.State(ctx)
		if state.Revision != 1 {
			t.Fatalf("%s changed revision to %d", tc.name, state.Revision)
		}
		shards, _ := env.engine.ListShards(ctx, 1, "")
		if len(shards.Shards) != 0 {
			t.Fatalf("%s persisted shards despite validation failure", tc.name)
		}
	}

	ok := testPair(1, "valid-pair", 1, 100, 200, 10)
	resp, err := env.engine.SubmitIncrement(ctx, ok)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Revision != 2 {
		t.Fatalf("revision = %d, want 2", resp.Revision)
	}
	changed := testPair(1, "content-change", 2, 100, 200, 11)
	changed.Shards[0].FP16Bits = []uint16{0x6400}
	changed.Shards[1].FP32Nanos = []int64{1_000_000_000}
	if _, err := env.engine.SubmitIncrement(ctx, changed); apiCode(err) != model.CodeShardMismatch {
		t.Fatalf("mismatch code = %s, want %s", apiCode(err), model.CodeShardMismatch)
	}
	state, _ := env.engine.State(ctx)
	if state.Revision != 2 {
		t.Fatalf("mismatch changed revision to %d", state.Revision)
	}
}

func TestPoolExhaustionAndQueueFullAreAtomic(t *testing.T) {
	ctx := context.Background()
	t.Run("pool exhausted", func(t *testing.T) {
		env := newTestEnv(t)
		if _, err := env.engine.SubmitSnapshot(ctx, testPolicy(1, 100, 100_000, 20, model.WritePreferenceFP32First)); err != nil {
			t.Fatal(err)
		}
		_, err := env.engine.SubmitIncrement(ctx, testPair(1, "pool-full", 1, 1024, 2048, 10))
		if apiCode(err) != model.CodePoolExhausted {
			t.Fatalf("pool code = %s, want %s (%v)", apiCode(err), model.CodePoolExhausted, err)
		}
		var apiErr *model.APIError
		_ = errors.As(err, &apiErr)
		if apiErr.Details["required_bytes"] != int64(1024) || apiErr.Details["reclaimable_bytes"] != int64(0) {
			t.Fatalf("pool details = %+v", apiErr.Details)
		}
		state, _ := env.engine.State(ctx)
		if state.Revision != 1 {
			t.Fatalf("pool failure changed revision")
		}
	})
	t.Run("queue full", func(t *testing.T) {
		env := newTestEnv(t)
		if _, err := env.engine.SubmitSnapshot(ctx, testPolicy(1, 100_000, 0, 0, model.WritePreferenceFP32First)); err != nil {
			t.Fatal(err)
		}
		_, err := env.engine.SubmitIncrement(ctx, testPair(1, "queue-full", 1, 100, 200, 10))
		if apiCode(err) != model.CodeWriteQueueFull {
			t.Fatalf("queue code = %s, want %s (%v)", apiCode(err), model.CodeWriteQueueFull, err)
		}
		list, _ := env.engine.ListShards(ctx, 1, "")
		if len(list.Shards) != 0 {
			t.Fatalf("queue failure persisted shards")
		}
	})
}

func TestWritePreferenceAndTimeBudget(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	policy := testPolicy(1, 100_000, 1500, 20, model.WritePreferenceFP32First)
	if _, err := env.engine.SubmitSnapshot(ctx, policy); err != nil {
		t.Fatal(err)
	}
	resp, err := env.engine.SubmitIncrement(ctx, testPair(1, "fp32-first", 1, 800, 1000, 10))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Writes) != 1 || resp.Writes[0].Precision != model.PrecisionFP32 || len(resp.Deferred) != 1 {
		t.Fatalf("FP32_FIRST selection = writes=%+v deferred=%+v", resp.Writes, resp.Deferred)
	}

	env2 := newTestEnv(t)
	policy2 := testPolicy(1, 100_000, 1500, 20, model.WritePreferenceFP16First)
	if _, err := env2.engine.SubmitSnapshot(ctx, policy2); err != nil {
		t.Fatal(err)
	}
	resp2, err := env2.engine.SubmitIncrement(ctx, testPair(1, "fp16-first", 1, 800, 1000, 10))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp2.Writes) != 1 || resp2.Writes[0].Precision != model.PrecisionFP16 || len(resp2.Deferred) != 1 {
		t.Fatalf("FP16_FIRST selection = writes=%+v deferred=%+v", resp2.Writes, resp2.Deferred)
	}

	env3 := newTestEnv(t)
	policy3 := testPolicy(1, 100_000, 100_000, 20, model.WritePreferenceFP32First)
	policy3.TimeBudgetMS = rawInt(1)
	if _, err := env3.engine.SubmitSnapshot(ctx, policy3); err != nil {
		t.Fatal(err)
	}
	resp3, err := env3.engine.SubmitIncrement(ctx, testPair(1, "time-budget", 1, 800, 1000, 10))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp3.Writes) != 1 || len(resp3.Deferred) != 1 || resp3.Deferred[0].QueueReason != "TIME_BUDGET_EXHAUSTED" {
		t.Fatalf("time budget result = %+v", resp3)
	}
}

func TestDerivedRebuildCascades(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	if _, err := env.engine.SubmitSnapshot(ctx, testPolicy(1, 100_000, 100_000, 20, model.WritePreferenceFP32First)); err != nil {
		t.Fatal(err)
	}
	req := testPair(1, "derive-1", 1, 100, 200, 10)
	if _, err := env.engine.SubmitIncrement(ctx, req); err != nil {
		t.Fatal(err)
	}
	before, err := env.engine.Derivatives(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != 2 {
		t.Fatalf("derivative count = %d, want 2", len(before))
	}
	req2 := testPair(1, "derive-2", 2, 100, 200, 20)
	req2.Shards[1].MomentumDigest = "momentum-changed"
	if _, err := env.engine.SubmitIncrement(ctx, req2); err != nil {
		t.Fatal(err)
	}
	after, err := env.engine.Derivatives(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	beforeByID := map[string]model.Derivative{}
	for _, d := range before {
		beforeByID[d.ShardID] = d
	}
	changedSeen := false
	for _, d := range after {
		if d.ShardID == "r1.fp32" && d.SourceFingerprint != beforeByID[d.ShardID].SourceFingerprint {
			changedSeen = true
		}
	}
	if !changedSeen {
		t.Fatalf("momentum change did not rebuild the dependent derivative: before=%+v after=%+v", before, after)
	}
	rounds, err := env.engine.RoundSummaries(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(rounds) != 1 || rounds[0].DecidedRevision != 3 || rounds[0].Status != "PASS" {
		t.Fatalf("round conclusion was not atomically rebuilt: %+v", rounds)
	}
}

func TestPrecisionRepeatedCheckIsStable(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	if _, err := env.engine.SubmitSnapshot(ctx, testPolicy(1, 100_000, 100_000, 20, model.WritePreferenceFP32First)); err != nil {
		t.Fatal(err)
	}
	if _, err := env.engine.SubmitIncrement(ctx, testPair(1, "precision", 1, 100, 200, 10)); err != nil {
		t.Fatal(err)
	}
	first, err := env.engine.VerifyPrecision(ctx, model.VerifyPrecisionRequest{Round: 1, Submission: "verify-1"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := env.engine.VerifyPrecision(ctx, model.VerifyPrecisionRequest{Round: 1, Submission: "verify-2"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("repeated precision verification differs:\nfirst=%+v\nsecond=%+v", first, second)
	}
	if first.VerifiedPairs != 1 || first.MaxAbsDeviationNanos != 0 || first.ResultDigest == "" {
		t.Fatalf("unexpected precision report: %+v", first)
	}
}

func TestSimulatedCrashRecovery(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	if _, err := env.engine.SubmitSnapshot(ctx, sampleSnapshot(t, "policy_v1.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := env.engine.SubmitIncrement(ctx, sampleIncrement(t, "round1_increment.json")); err != nil {
		t.Fatal(err)
	}
	shards, err := env.engine.ListShards(ctx, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]model.Shard{}
	for _, s := range shards.Shards {
		byID[s.ShardID] = s
	}
	vramShard := byID["r1.attn.fp16"]
	pendingShard := byID["r1.mlp.fp32"]
	missingBlobShard := byID["r1.mlp.fp16"]

	_, err = env.engine.insertIntents(ctx, []intentSpec{
		{Kind: "EVICT", SubmissionID: "crash-evict", Before: vramShard,
			After: evictionIntentJSON{Round: vramShard.Round, ShardID: vramShard.ShardID,
				Before: vramShard, AfterPool: model.PoolPersisted, Reason: "SIMULATED_PARTIAL"}},
		{Kind: "WRITE", SubmissionID: "crash-write",
			After: writeIntentJSON{Round: pendingShard.Round, ShardID: pendingShard.ShardID,
				Digest: pendingShard.Digest, Precision: pendingShard.Precision, Bytes: pendingShard.Bytes,
				BlobPath: env.engine.blobPathFor(pendingShard.Round, pendingShard.ShardID, pendingShard.Digest)}},
		{Kind: "CHECKPOINT", SubmissionID: "crash-checkpoint"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Simulate partial, externally visible state from a process that died
	// between phases. Recovery must reconstruct facts from durable records.
	if _, err := env.store.DB.ExecContext(ctx, `UPDATE shards SET location='PERSISTED', desired_pool='PERSISTED', write_state='WRITTEN' WHERE shard_id=?`, vramShard.ShardID); err != nil {
		t.Fatal(err)
	}
	if _, err := env.store.DB.ExecContext(ctx, `UPDATE shards SET write_state='WRITTEN' WHERE shard_id=?`, pendingShard.ShardID); err != nil {
		t.Fatal(err)
	}
	if _, err := env.store.DB.ExecContext(ctx, `INSERT INTO write_records(submission_id, round, shard_id,
		precision, digest, bytes, pool, committed_at, blob_path) VALUES('partial',?,?,?,?,?,?,?,?)`,
		missingBlobShard.Round, missingBlobShard.ShardID, missingBlobShard.Precision,
		missingBlobShard.Digest, missingBlobShard.Bytes, missingBlobShard.Location,
		"2026-11-24T00:00:00Z", filepath.Join(env.dataDir, "blobs", "missing.json")); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(env.dataDir, "blobs", "orphan.json")
	if err := os.WriteFile(orphan, []byte("orphan"), 0o644); err != nil {
		t.Fatal(err)
	}

	env2 := env.reopen()
	report, err := env2.engine.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.InterruptedIntents < 3 || report.RolledBackWrites < 1 || report.RolledBackEvictions < 1 || report.OrphanBlobsRemoved < 1 {
		t.Fatalf("unexpected recovery report: %+v", report)
	}
	after, err := env2.engine.ListShards(ctx, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	afterByID := map[string]model.Shard{}
	for _, s := range after.Shards {
		afterByID[s.ShardID] = s
	}
	if afterByID[vramShard.ShardID].Location != model.PoolVRAM {
		t.Fatalf("eviction was not rolled back: %+v", afterByID[vramShard.ShardID])
	}
	if afterByID[pendingShard.ShardID].WriteState != model.WritePending {
		t.Fatalf("in-flight write was not rolled back: %+v", afterByID[pendingShard.ShardID])
	}
	if afterByID[missingBlobShard.ShardID].WriteState != model.WritePending {
		t.Fatalf("missing blob record was not revoked: %+v", afterByID[missingBlobShard.ShardID])
	}
	pools, _, err := env2.engine.Pools(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := poolBytes(pools, model.PoolVRAM); got != 1792 {
		t.Fatalf("recovery VRAM = %d, want 1792", got)
	}
	if len(report.Consistency) != 1 || report.Consistency[0].Status != "INCOMPLETE" {
		t.Fatalf("recovery consistency = %+v", report.Consistency)
	}
	if report.FactsBasis == "" || report.Details["note"] == nil {
		t.Fatalf("recovery report does not identify its facts basis: %+v", report)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan blob was not removed")
	}
}

func TestSubprocessCrashRecovery(t *testing.T) {
	if os.Getenv("CPE_CRASH_HELPER") == "1" {
		t.Skip("helper process invoked separately")
	}
	env := newTestEnv(t)
	ctx := context.Background()
	if _, err := env.engine.SubmitSnapshot(ctx, sampleSnapshot(t, "policy_v1.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := env.engine.SubmitIncrement(ctx, sampleIncrement(t, "round1_increment.json")); err != nil {
		t.Fatal(err)
	}
	_ = env.store.Close()

	cmd := exec.Command(os.Args[0], "-test.run=TestSubprocessCrashHelper", "-test.v")
	cmd.Env = append(os.Environ(),
		"CPE_CRASH_HELPER=1",
		"CPE_CRASH_DB="+env.path,
		"CPE_CRASH_DATA="+env.dataDir,
	)
	output, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 86 {
		t.Fatalf("child process was not force-terminated at intent boundary: err=%v output=%s", err, output)
	}

	st, err := store.Open(env.path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	eng, err := New(st, Options{DataDir: env.dataDir, Now: func() time.Time { return time.Date(2026, 11, 24, 2, 0, 0, 0, time.UTC) }})
	if err != nil {
		t.Fatal(err)
	}
	report, err := eng.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.InterruptedIntents == 0 || report.RolledBackWrites == 0 || report.RolledBackEvictions == 0 {
		t.Fatalf("subprocess recovery did not observe in-flight facts: %+v", report)
	}
	round2, err := eng.ListShards(ctx, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(round2.Shards) != 0 {
		t.Fatalf("force-terminated submission became visible after restart: %+v", round2.Shards)
	}
	pools, _, err := eng.Pools(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if poolBytes(pools, model.PoolVRAM) != 1792 {
		t.Fatalf("recovered VRAM pool = %d, want 1792", poolBytes(pools, model.PoolVRAM))
	}
	if report.Details["note"] == nil {
		t.Fatalf("recovery report lacks facts-only disclosure")
	}
}

func TestSubprocessCrashHelper(t *testing.T) {
	if os.Getenv("CPE_CRASH_HELPER") != "1" {
		return
	}
	st, err := store.Open(os.Getenv("CPE_CRASH_DB"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	eng, err := New(st, Options{DataDir: os.Getenv("CPE_CRASH_DATA"), Now: func() time.Time {
		return time.Date(2026, 11, 24, 0, 30, 0, 0, time.UTC)
	}, Failpoint: func(point string) error {
		if point == "after_intents" {
			os.Exit(86)
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	req := readJSONFile[model.IncrementRequest](t, filepath.Join("..", "..", "samples", "round2_increment.json"))
	if _, err := eng.SubmitIncrement(context.Background(), req); err != nil {
		t.Fatalf("crash helper returned before hitting failpoint: %v", err)
	}
	t.Fatal("crash helper unexpectedly completed")
}

func TestConcurrentSingleWriterAndReaderConsistency(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	if _, err := env.engine.SubmitSnapshot(ctx, sampleSnapshot(t, "policy_v1.json")); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 16)
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 20; j++ {
				state, err := env.engine.State(ctx)
				if err != nil {
					errCh <- err
					return
				}
				seen := int64(-1)
				for _, p := range state.Pools {
					if seen == -1 || p.Revision < seen {
						seen = p.Revision
					}
				}
				if seen != state.Revision {
					errCh <- errors.New("pool view revision does not match state revision")
					return
				}
				if _, err := env.engine.ListShards(ctx, 0, model.PoolVRAM); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	close(start)

	req := testPair(1, "concurrent-same", 1, 100, 200, 10)
	results := make([]*model.SubmissionResponse, 2)
	errs := make([]error, 2)
	var submitWG sync.WaitGroup
	barrier := make(chan struct{})
	for i := 0; i < 2; i++ {
		submitWG.Add(1)
		go func(i int) {
			defer submitWG.Done()
			<-barrier
			results[i], errs[i] = env.engine.SubmitIncrement(ctx, req)
		}(i)
	}
	close(barrier)
	submitWG.Wait()
	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("concurrent same submission errors: %v %v", errs[0], errs[1])
	}
	replays := 0
	if results[0].IdempotentReplay {
		replays++
	}
	if results[1].IdempotentReplay {
		replays++
	}
	if replays != 1 {
		t.Fatalf("concurrent same submission replay count = %d, results=%+v", replays, results)
	}

	reqA := testPair(2, "concurrent-base-a", 2, 100, 200, 20)
	reqB := testPair(3, "concurrent-base-b", 2, 100, 200, 30)
	type outcome struct {
		resp *model.SubmissionResponse
		err  error
	}
	outcomes := make(chan outcome, 2)
	go func() { r, err := env.engine.SubmitIncrement(ctx, reqA); outcomes <- outcome{r, err} }()
	go func() { r, err := env.engine.SubmitIncrement(ctx, reqB); outcomes <- outcome{r, err} }()
	successes, stale := 0, 0
	for i := 0; i < 2; i++ {
		out := <-outcomes
		if out.err == nil {
			successes++
		} else if apiCode(out.err) == model.CodeStaleBase {
			stale++
		} else {
			t.Fatalf("unexpected concurrent base error: %v", out.err)
		}
	}
	if successes != 1 || stale != 1 {
		t.Fatalf("concurrent base outcomes: successes=%d stale=%d", successes, stale)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}
