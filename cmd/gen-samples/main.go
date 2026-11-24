package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"checkpointengine/internal/model"
)

func main() {
	dir := "samples"
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		panic(err)
	}
	policy1 := snapshot1()
	policy2 := snapshot2()
	writeJSON(filepath.Join(dir, "policy_v1.json"), snapshotRequest(policy1))
	writeJSON(filepath.Join(dir, "policy_v2.json"), snapshotRequest(policy2))
	writeJSON(filepath.Join(dir, "round1_increment.json"), round1())
	writeJSON(filepath.Join(dir, "round2_increment.json"), round2())
	writeJSON(filepath.Join(dir, "round3_drift_increment.json"), round3Drift())
	writeJSON(filepath.Join(dir, "round4_complete_increment.json"), round4Complete())
	manifest := map[string]any{
		"generated_at":        "2026-11-24T00:00:00Z",
		"policy_v1_checksum":  policy1.Checksum,
		"policy_v2_checksum":  policy2.Checksum,
		"expected_flow":       []string{"policy_v1", "round1_increment", "round2_increment", "round3_drift_increment", "round4_complete_increment", "policy_v2"},
		"expected_errors":     map[string]string{"round3_drift_increment": model.CodePrecisionDrift},
		"fixture_contract":    "All numeric payloads are deterministic integers; FP16 values are IEEE-754 binary16 bit patterns and FP32 masters are nanounits.",
		"recommended_command": "go run ./cmd/checkpoint-engine -db data/sample.db -data-dir data",
	}
	writeJSON(filepath.Join(dir, "manifest.json"), manifest)
	fmt.Printf("wrote deterministic samples to %s\n", dir)
}

func snapshot1() model.Snapshot {
	s := model.Snapshot{
		Version: 1, GeneratedAt: "2026-11-24T00:00:00Z", VRAMLimitBytes: 2600,
		WriteBudgetBytesPerRound: 3200, TimeBudgetMS: 10, WriteQueueCapacity: 8,
		LossScaleInitial: "1024", LossScaleGrowth: "2", PrecisionAbsLimitNanos: 2_000_000,
		PrecisionRelLimitPPM: 50_000, WritePreference: model.WritePreferenceFP32First,
	}
	s.Checksum, _ = model.CanonicalSnapshotChecksum(s)
	return s
}

func snapshot2() model.Snapshot {
	s := model.Snapshot{
		Version: 2, GeneratedAt: "2026-11-24T00:10:00Z", VRAMLimitBytes: 1800,
		WriteBudgetBytesPerRound: 1024, TimeBudgetMS: 5, WriteQueueCapacity: 8,
		LossScaleInitial: "1024", LossScaleGrowth: "2", PrecisionAbsLimitNanos: 2_000_000,
		PrecisionRelLimitPPM: 50_000, WritePreference: model.WritePreferenceFP16First,
	}
	s.Checksum, _ = model.CanonicalSnapshotChecksum(s)
	return s
}

func snapshotRequest(s model.Snapshot) model.SnapshotRequest {
	return model.SnapshotRequest{
		Version: s.Version, GeneratedAt: s.GeneratedAt,
		VRAMLimitBytes:           rawInt(s.VRAMLimitBytes),
		WriteBudgetBytesPerRound: rawInt(s.WriteBudgetBytesPerRound),
		TimeBudgetMS:             rawInt(s.TimeBudgetMS),
		WriteQueueCapacity:       intPtr(s.WriteQueueCapacity),
		LossScaleInitial:         rawString(s.LossScaleInitial),
		LossScaleGrowth:          rawString(s.LossScaleGrowth),
		PrecisionAbsLimitNanos:   rawInt(s.PrecisionAbsLimitNanos),
		PrecisionRelLimitPPM:     rawInt(s.PrecisionRelLimitPPM),
		WritePreference:          s.WritePreference,
		Checksum:                 s.Checksum,
	}
}

func round1() model.IncrementRequest {
	shards := []model.ShardInput{
		halfShard("r1.attn.fp16", "attn.weight", 1024, []uint16{0x6000, 0x6500, 0xE800, 0x5800}, 1000, 10, 1),
		fp32Shard("r1.attn.fp32", "attn.weight", 2048, []int64{500_000_000, 1_250_000_000, -2_000_000_000, 125_000_000}, 1000, 100, 2),
		halfShard("r1.mlp.fp16", "mlp.weight", 768, []uint16{0x6400, 0x6800, 0xE400}, 2000, 10, 1),
		fp32Shard("r1.mlp.fp32", "mlp.weight", 1536, []int64{1_000_000_000, 2_000_000_000, -1_000_000_000}, 2000, 100, 2),
	}
	linkMasters(shards)
	return model.IncrementRequest{SubmissionID: "sub-round-1", BaseVersion: 1, Round: 1, Shards: shards}
}

func round2() model.IncrementRequest {
	shards := []model.ShardInput{
		halfShard("r2.attn.fp16", "attn.weight", 1200, []uint16{0x6000, 0x6500}, 3000, 10, 1),
		fp32Shard("r2.attn.fp32", "attn.weight", 2400, []int64{500_000_000, 1_250_000_000}, 3000, 100, 2),
		halfShard("r2.mlp.fp16", "mlp.weight", 800, []uint16{0x6400, 0x6800}, 4000, 10, 1),
		fp32Shard("r2.mlp.fp32", "mlp.weight", 1600, []int64{1_000_000_000, 2_000_000_000}, 4000, 100, 2),
	}
	linkMasters(shards)
	return model.IncrementRequest{SubmissionID: "sub-round-2", BaseVersion: 2, Round: 2, Shards: shards}
}

func round3Drift() model.IncrementRequest {
	shards := []model.ShardInput{
		halfShard("r3.attn.fp16", "attn.weight", 1200, []uint16{0x6000, 0x6500}, 5000, 10, 1),
		fp32Shard("r3.attn.fp32", "attn.weight", 2400, []int64{510_000_000, 1_260_000_000}, 5000, 100, 2),
	}
	linkMasters(shards)
	return model.IncrementRequest{SubmissionID: "sub-round-3-drift", BaseVersion: 3, Round: 3, Shards: shards}
}

func round4Complete() model.IncrementRequest {
	shards := []model.ShardInput{
		halfShard("r4.attn.fp16", "attn.weight", 400, []uint16{0x6000}, 6000, 10, 1),
		fp32Shard("r4.attn.fp32", "attn.weight", 800, []int64{500_000_000}, 6000, 100, 1),
	}
	linkMasters(shards)
	return model.IncrementRequest{SubmissionID: "sub-round-4-complete", BaseVersion: 3, Round: 4, Shards: shards}
}

func halfShard(id, parameter string, bytes int64, bits []uint16, access int64, priority int, writeMS int64) model.ShardInput {
	parts := make([]string, len(bits))
	for i, b := range bits {
		parts[i] = strconv.FormatUint(uint64(b), 10)
	}
	return model.ShardInput{
		ShardID: id, ParameterID: parameter, Precision: model.PrecisionFP16, Bytes: bytes,
		Digest:   model.NewDigest("FP16_BITS_V1", strings.Join(parts, ",")),
		FP16Bits: bits, LastAccessMS: int64Ptr(access), Priority: priority, WriteMS: writeMS,
		Quantization: model.RoundingRNE,
	}
}

func fp32Shard(id, parameter string, bytes int64, nanos []int64, access int64, priority int, writeMS int64) model.ShardInput {
	parts := make([]string, len(nanos))
	for i, n := range nanos {
		parts[i] = strconv.FormatInt(n, 10)
	}
	return model.ShardInput{
		ShardID: id, ParameterID: parameter, Precision: model.PrecisionFP32, Bytes: bytes,
		Digest:    model.NewDigest("FP32_NANOS_V1", strings.Join(parts, ",")),
		FP32Nanos: nanos, LastAccessMS: int64Ptr(access), Priority: priority, WriteMS: writeMS,
		MomentumDigest: model.NewDigest("MOMENTUM_V1", id),
	}
}

func linkMasters(shards []model.ShardInput) {
	halfByParam := make(map[string]string)
	for _, s := range shards {
		if s.Precision == model.PrecisionFP16 {
			halfByParam[s.ParameterID] = s.Digest
		}
	}
	for i := range shards {
		if shards[i].Precision == model.PrecisionFP32 {
			shards[i].RestoredFromDigest = halfByParam[shards[i].ParameterID]
		}
	}
}

func writeJSON(path string, value any) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		panic(err)
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		panic(err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		panic(err)
	}
}

func rawInt(v int64) json.RawMessage     { return json.RawMessage(strconv.FormatInt(v, 10)) }
func rawString(v string) json.RawMessage { return json.RawMessage(strconv.Quote(v)) }
func intPtr(v int) *int                  { return &v }
func int64Ptr(v int64) *int64            { return &v }
