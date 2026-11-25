package sample

import (
	"encoding/json"
	"fmt"

	"mpckpt/internal/format"
	"mpckpt/internal/model"
	"mpckpt/internal/numeric"
)

const SeedLabel = "mpckpt-fixed-seed-v1"

type Batch struct {
	Shards         []model.Shard
	Baseline       []string
	ExtraResident  model.Shard
	IncrementPatch model.IncrementalPatch
}

type masterSpec struct {
	ID     string
	Round  int64
	Layer  int
	Source bool
}

var masters = []masterSpec{
	{ID: "r1-embed-master", Round: 1, Layer: 0},
	{ID: "r2-attn-master", Round: 2, Layer: 1},
	{ID: "r3-ffn-master", Round: 3, Layer: 2},
	{ID: "r4-head-master", Round: 4, Layer: 3},
}

func MasterDigest(id string, round int64, layer int) string {
	params := numeric.FixedParams(model.SHA256Hex([]byte(fmt.Sprintf("%s:%s:%d:%d", SeedLabel, id, round, layer))), numeric.DefaultElementsPerShard)
	raw, _ := json.Marshal(params)
	return model.SHA256Hex(append([]byte(SeedLabel+":master:"), raw...))
}

func MomentumDigest(masterDigest string) string {
	return model.SHA256Hex([]byte(SeedLabel + ":momentum:" + masterDigest))
}

func FP16Digest(masterDigest string) string {
	return model.SHA256Hex([]byte(SeedLabel + ":fp16:" + masterDigest))
}

func Build() Batch {
	var shards []model.Shard
	var baseline []string
	for index, master := range masters {
		digest := MasterDigest(master.ID, master.Round, master.Layer)
		masterShard := model.Shard{
			ID:             master.ID,
			Round:          master.Round,
			Precision:      model.PrecisionFP32,
			SizeBytes:      int64(numeric.DefaultElementsPerShard * 4),
			Digest:         digest,
			Pool:           model.PoolPersistent,
			LastAccess:     int64(100 + index),
			MomentumDigest: MomentumDigest(digest),
			Priority:       int64(10 + index),
		}
		shards = append(shards, masterShard)
		baseline = append(baseline, masterShard.ID)
	}

	fp16A := model.Shard{
		ID:             "r1-embed-fp16-a",
		Round:          1,
		Precision:      model.PrecisionFP16,
		SizeBytes:      int64(numeric.DefaultElementsPerShard * 2),
		Digest:         FP16Digest(shards[0].Digest),
		Pool:           model.PoolVRAM,
		LastAccess:     120,
		SourceDigest:   shards[0].Digest,
		MomentumDigest: MomentumDigest(shards[0].Digest),
		Priority:       50,
	}
	fp16B := model.Shard{
		ID:             "r2-attn-fp16-b",
		Round:          2,
		Precision:      model.PrecisionFP16,
		SizeBytes:      int64(numeric.DefaultElementsPerShard * 2),
		Digest:         FP16Digest(shards[1].Digest),
		Pool:           model.PoolPersistent,
		LastAccess:     121,
		SourceDigest:   shards[1].Digest,
		MomentumDigest: MomentumDigest(shards[1].Digest),
		Priority:       40,
	}
	shards = append(shards, fp16A, fp16B)

	extra := model.Shard{
		ID:             "r3-ffn-fp16-extra",
		Round:          3,
		Precision:      model.PrecisionFP16,
		SizeBytes:      int64(numeric.DefaultElementsPerShard * 2),
		Digest:         FP16Digest(shards[2].Digest),
		Pool:           model.PoolVRAM,
		LastAccess:     150,
		SourceDigest:   shards[2].Digest,
		MomentumDigest: MomentumDigest(shards[2].Digest),
		Priority:       60,
	}

	return Batch{
		Shards:        shards,
		Baseline:      baseline,
		ExtraResident: extra,
		IncrementPatch: model.IncrementalPatch{
			ShardID:           fp16B.ID,
			Precision:         model.PrecisionFP32,
			SizeBytes:         int64(numeric.DefaultElementsPerShard * 4),
			Pool:              model.PoolPersistent,
			ClearSourceDigest: true,
		},
	}
}

func Manifest(version int) format.Manifest {
	batch := Build()
	shards := model.CopyShards(batch.Shards)
	if version == 1 {
		for index := range shards {
			shards[index].Pool = ""
		}
	}
	lossPrecision := model.LossScalePrecisionFixed64
	if version == 1 {
		lossPrecision = model.LossScalePrecisionFloat32
	}
	return format.Manifest{
		Format:             format.FormatBinary,
		Version:            version,
		LossScaleFixed:     int64(8 * numeric.FixedScale),
		LossScalePrecision: lossPrecision,
		Shards:             shards,
	}
}

func StrategyV1() model.StrategySnapshot {
	return model.StrategySnapshot{
		Version:               1,
		GeneratedAt:           "2026-01-02T03:04:05Z",
		GPUCapBytes:           96,
		WriteBudgetBytes:      4096,
		TimeBudgetMillis:      1000,
		LossScaleInitialFixed: int64(8 * numeric.FixedScale),
		LossScaleGrowth:       2,
		MaxAbsErrorFixed:      50_000,
		MaxRelErrorPPM:        100_000,
	}
}
