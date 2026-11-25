package engine

import (
	"fmt"
	"time"

	"ropechunk/internal/model"
)

const plannerOverhead = int64(64 << 10)

func PlanMemory(shard model.Shard, cfg model.Config, stage string) (model.MemoryPlan, error) {
	full := EstimateBreakdown(shard.Shape, cfg, false)
	batchPeak := EstimateBreakdown(shard.Shape, cfg, true)
	plan := model.MemoryPlan{
		ShardID: shard.ID,
		Stage:   stage,
		Budget:  cfg.PeakMemoryBytes,
		Full:    full,
	}
	switch {
	case full.Total <= cfg.PeakMemoryBytes:
		plan.Mode = "full"
		plan.Steps = []model.MemoryStep{{
			Index:          0,
			Mode:           "full",
			Batch:          -1,
			RowStart:       0,
			RowEnd:         shard.Shape.Seq(),
			EstimatedBytes: full.Total,
			Note:           "all batches fit in one execution step",
		}}
	case batchPeak.Total <= cfg.PeakMemoryBytes:
		plan.Mode = "batch-split"
		for batch := 0; batch < shard.Shape.Batch(); batch++ {
			plan.Steps = append(plan.Steps, model.MemoryStep{
				Index:          len(plan.Steps),
				Mode:           "batch",
				Batch:          batch,
				RowStart:       0,
				RowEnd:         shard.Shape.Seq(),
				EstimatedBytes: batchPeak.Total,
				Note:           fmt.Sprintf("split batch %d to honor peak memory", batch),
			})
		}
	default:
		plan.Mode = "impossible"
		return plan, &model.AppError{
			Code:    model.CodeMemoryBudget,
			ShardID: shard.ID,
			Stage:   stage,
			Message: "even a single-batch execution exceeds the peak-memory budget",
			Details: map[string]any{
				"budget_bytes":                cfg.PeakMemoryBytes,
				"full_estimate_bytes":         full.Total,
				"batch_estimate_bytes":        batchPeak.Total,
				"breakdown":                   full,
				"batch_breakdown":             batchPeak,
				"minimum_row_workspace_bytes": EstimateBreakdown(shard.Shape, cfg, true).RowWorkspace,
			},
		}
	}
	if plan.Mode != "impossible" {
		plan.CreatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	return plan, nil
}

func EstimateBreakdown(shape model.Shape, cfg model.Config, singleBatch bool) model.MemoryBreakdown {
	batchCount := int64(shape.Batch())
	if singleBatch {
		batchCount = 1
	}
	sequences := int64(shape.Seq())
	heads := int64(shape.Heads())
	dim := int64(shape.HeadDim())
	rows := int64(min(cfg.RowBlockSize, shape.Seq()))
	keyBlock := int64(min(cfg.KeyBlockSize, shape.Seq()))
	workers := int64(cfg.MaxConcurrentBlock)
	if workers < 1 {
		workers = 1
	}
	rotated := batchCount * heads * sequences * dim * 8 * 2 // Q and K float64 equivalents
	rowWorkspace := rows * dim * 8 * 6 * workers
	angleTable := rows * ((dim + 1) / 2) * 16 * workers
	attentionWorkspace := (rows*keyBlock*8 + rows*dim*8*3) * workers
	return model.MemoryBreakdown{
		RotatedBytes:       rotated,
		RowWorkspace:       rowWorkspace,
		AngleTable:         angleTable,
		AttentionWorkspace: attentionWorkspace,
		Overhead:           plannerOverhead,
		Total:              rotated + rowWorkspace + angleTable + attentionWorkspace + plannerOverhead,
	}
}

func EstimateTaskUS(stage string, rows, coveredKeys, headDim int) int64 {
	if rows < 1 {
		rows = 1
	}
	if coveredKeys < 1 {
		coveredKeys = 1
	}
	if headDim < 1 {
		headDim = 1
	}
	var ops int64
	if stage == model.StageRotation {
		ops = int64(rows * headDim * 8)
	} else {
		ops = int64(rows * coveredKeys * headDim * 3)
	}
	cost := 20 + ops/200
	if cost < 1 {
		cost = 1
	}
	return cost
}
