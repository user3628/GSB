// Package scheduler 在峰值内存预算内规划分块执行工作区。
package scheduler

import (
	"fmt"

	"flashattention/internal/errs"
	"flashattention/internal/tensor"
)

const (
	Float32Bytes = 4
	Float64Bytes = 8
	HashBytes    = 32
)

type Plan struct {
	ShardID             string           `json:"shard_id"`
	Batch               int              `json:"batch"`
	RequestedRowBlock   int              `json:"requested_row_block"`
	RequestedColBlock   int              `json:"requested_col_block"`
	RowBlock            int              `json:"row_block"`
	ColBlock            int              `json:"col_block"`
	GroupBlocks         int              `json:"group_blocks"`
	ProcessBatchesSplit bool             `json:"process_batches_split"`
	Decisions           []string         `json:"decisions"`
	BudgetBytes         int64            `json:"budget_bytes"`
	EstimatedPeakBytes  int64            `json:"estimated_peak_bytes"`
	Components          map[string]int64 `json:"components"`
}

type Options struct {
	ShardID     string
	Shape       tensor.Shape
	RowBlock    int
	ColBlock    int
	GroupBlocks int
	BudgetBytes int64
}

func PlanWorkload(opts Options) (*Plan, error) {
	if opts.RowBlock <= 0 || opts.ColBlock <= 0 {
		return nil, errs.New(errs.EConfig, "row/col block must be positive")
	}
	if opts.BudgetBytes <= 0 {
		opts.BudgetBytes = 64 << 20
	}
	p := &Plan{
		ShardID: opts.ShardID, RequestedRowBlock: opts.RowBlock, RequestedColBlock: opts.ColBlock,
		RowBlock: opts.RowBlock, ColBlock: opts.ColBlock, GroupBlocks: opts.GroupBlocks,
		BudgetBytes: opts.BudgetBytes, Components: map[string]int64{},
	}
	if p.GroupBlocks <= 0 {
		p.GroupBlocks = 1
	}
	groupKeyCols := opts.ColBlock * p.GroupBlocks

	base := estimate(opts.Shape.D, p.RowBlock, p.ColBlock, p.GroupBlocks)
	p.EstimatedPeakBytes = base.Total
	p.Components = base.Components
	if base.Total <= opts.BudgetBytes {
		return p, nil
	}

	if opts.Shape.B > 1 {
		p.ProcessBatchesSplit = true
		p.Decisions = append(p.Decisions, fmt.Sprintf("split batch dimension: execute %d batches independently", opts.Shape.B))
	}

	for p.RowBlock > 1 {
		p.RowBlock--
		cost := estimate(opts.Shape.D, p.RowBlock, p.ColBlock, p.GroupBlocks)
		if cost.Total <= opts.BudgetBytes {
			p.EstimatedPeakBytes = cost.Total
			p.Components = cost.Components
			p.Decisions = append(p.Decisions, fmt.Sprintf("shrink row_block from %d to %d", opts.RowBlock, p.RowBlock))
			return p, nil
		}
	}
	p.Decisions = append(p.Decisions, fmt.Sprintf("row_block=1 still exceeds budget; shrink col_block from %d", p.ColBlock))
	for p.ColBlock > 1 {
		p.ColBlock--
		p.GroupBlocks = maxInt(1, (groupKeyCols+p.ColBlock-1)/p.ColBlock)
		cost := estimate(opts.Shape.D, p.RowBlock, p.ColBlock, p.GroupBlocks)
		if cost.Total <= opts.BudgetBytes {
			p.EstimatedPeakBytes = cost.Total
			p.Components = cost.Components
			p.Decisions = append(p.Decisions, fmt.Sprintf("shrink col_block to %d and group_blocks=%d", p.ColBlock, p.GroupBlocks))
			return p, nil
		}
	}

	cost := estimate(opts.Shape.D, 1, 1, 1)
	p.EstimatedPeakBytes = cost.Total
	p.Components = cost.Components
	return p, errs.New(errs.EMemoryBudget, "minimum tiled workspace %d bytes exceeds budget %d bytes", cost.Total, opts.BudgetBytes).
		With("shard", opts.ShardID).
		With("budget_bytes", opts.BudgetBytes).
		With("minimum_peak_bytes", cost.Total).
		With("budget_detail", cost.Components).
		With("suggestion", "increase --memory-budget-bytes or reduce head_dim/reserved buffers")
}

type cost struct {
	Total      int64
	Components map[string]int64
}

func estimate(d, rb, cb, group int) cost {
	comps := map[string]int64{}
	add := func(name string, n int64) { comps[name] = n }
	add("q_row_workspace_f64", int64(rb*d)*Float64Bytes)
	add("k_col_chunk_f32_view", int64(cb*d)*Float32Bytes)
	add("v_col_chunk_f32_view", int64(cb*d)*Float32Bytes)
	add("logits_f64", int64(rb*cb)*Float64Bytes)
	add("block_pv_f64", int64(rb*d)*Float64Bytes)
	add("row_states_m_sum_lse", int64(rb*3)*Float64Bytes)
	add("row_acc_f64", int64(rb*d)*Float64Bytes)
	add("checkpoint_snapshot_buffers", int64(group*rb*d)*Float64Bytes)
	add("output_rows_f32", int64(rb*d)*Float32Bytes)
	add("hash_and_metadata", int64(rb*HashBytes*4+cb*8+1024))
	var total int64
	for _, v := range comps {
		total += v
	}
	return cost{Total: total, Components: comps}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
