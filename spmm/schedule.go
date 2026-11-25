package spmm

import "fmt"

type ExecutionColumn struct {
	Index int `json:"index"`
	Start int `json:"start"`
	End   int `json:"end"`
}

type ExecutionRow struct {
	Index        int               `json:"index"`
	PlanRowBlock int               `json:"plan_row_block"`
	RowStart     int               `json:"row_start"`
	RowEnd       int               `json:"row_end"`
	Columns      []ExecutionColumn `json:"columns"`
}

type MemoryStep struct {
	Step               int   `json:"step"`
	ExecutionRow       int   `json:"execution_row"`
	PlanRowBlock       int   `json:"plan_row_block"`
	RowStart           int   `json:"row_start"`
	RowEnd             int   `json:"row_end"`
	ColStart           int   `json:"col_start"`
	ColEnd             int   `json:"col_end"`
	OutputBufferBytes  int64 `json:"output_buffer_bytes"`
	WorkspaceBytes     int64 `json:"workspace_bytes"`
	DenseSliceBytes    int64 `json:"dense_slice_bytes"`
	EstimatedTotalByte int64 `json:"estimated_total_bytes"`
}

type MemoryPlan struct {
	PeakLimitBytes    int64          `json:"peak_limit_bytes"`
	EffectiveRowBlock int            `json:"effective_row_block"`
	EffectiveColChunk int            `json:"effective_col_chunk"`
	RowSplit          bool           `json:"row_split"`
	ColumnSplit       bool           `json:"column_split"`
	SplitDecisions    []string       `json:"split_decisions"`
	MaxEstimateBytes  int64          `json:"max_estimate_bytes"`
	Rows              []ExecutionRow `json:"execution_rows"`
	Steps             []MemoryStep   `json:"steps"`
}

// BuildMemoryPlan derives the actual execution units from the selected plan
// and a hard peak-memory ceiling. It never allocates based on the budget; it
// only computes integer byte estimates and records every split decision.
func BuildMemoryPlan(p *Plan) (MemoryPlan, error) {
	mp := MemoryPlan{PeakLimitBytes: p.PeakMemoryBytes}
	rowBytes := int64(maxInt(1, p.DenseCols)) * 8
	if p.PeakMemoryBytes <= 0 {
		mp.EffectiveRowBlock = p.RowBlockSize
		mp.EffectiveColChunk = p.DenseCols
		return buildUnlimitedMemoryPlan(p, mp), nil
	}

	// At least one row and one column slice must fit. Output and workspace
	// each reserve one full row's dense result, per the explicit budget model.
	maxRows := int(p.PeakMemoryBytes / (2 * rowBytes))
	if maxRows < 1 {
		return mp, memoryBudgetError(p.PeakMemoryBytes, rowBytes*2, 1, p.DenseCols, 1)
	}
	if maxRows > p.RowBlockSize {
		maxRows = p.RowBlockSize
	}
	mp.EffectiveRowBlock = maxRows
	perRow := int64(maxRows) * rowBytes
	remaining := p.PeakMemoryBytes - 2*perRow
	// The dense B slice is consumed one accumulation-column block at a time:
	// its footprint is Kblock x outputColumnChunk.
	kBlock := int64(maxInt(1, minInt(p.ColBlockSize, p.Cols)))
	maxCols := int(remaining / (kBlock * 8))
	if maxCols < 1 {
		return mp, memoryBudgetError(p.PeakMemoryBytes, 2*perRow+kBlock*8, 1, p.DenseCols, 1)
	}
	if maxCols > p.DenseCols {
		maxCols = p.DenseCols
	}
	mp.EffectiveColChunk = maxCols
	mp.RowSplit = maxRows < p.RowBlockSize
	mp.ColumnSplit = maxCols < p.DenseCols
	if mp.RowSplit {
		mp.SplitDecisions = append(mp.SplitDecisions, fmt.Sprintf(
			"row block split from %d to %d rows: output+workspace=%d bytes, peak=%d bytes",
			p.RowBlockSize, maxRows, 2*perRow, p.PeakMemoryBytes))
	}
	if mp.ColumnSplit {
		mp.SplitDecisions = append(mp.SplitDecisions, fmt.Sprintf(
			"output column block split from %d to %d columns: dense slice=%d bytes, remaining=%d bytes",
			p.DenseCols, maxCols, kBlock*int64(maxCols)*8, remaining))
	}
	return buildMemoryRows(p, mp)
}

func buildUnlimitedMemoryPlan(p *Plan, mp MemoryPlan) MemoryPlan {
	// Unlimited is represented as the selected plan dimensions with the same
	// auditable steps; no split decisions are emitted.
	if mp.EffectiveRowBlock <= 0 {
		mp.EffectiveRowBlock = p.RowBlockSize
	}
	if mp.EffectiveColChunk <= 0 {
		mp.EffectiveColChunk = p.DenseCols
	}
	mp, _ = buildMemoryRows(p, mp)
	return mp
}

func buildMemoryRows(p *Plan, mp MemoryPlan) (MemoryPlan, error) {
	rowBytes := int64(maxInt(1, p.DenseCols)) * 8
	step := 0
	executionIndex := 0
	planBlockIdx := 0
	for start := 0; start < p.Rows; {
		// Keep boundaries aligned with the selected plan's row blocks when
		// possible, so persisted unit indexing remains intuitive and stable.
		for planBlockIdx < len(p.RowBlocks) && p.RowBlocks[planBlockIdx].End <= start {
			planBlockIdx++
		}
		planIdx := planBlockIdx
		if planIdx >= len(p.RowBlocks) {
			planIdx = len(p.RowBlocks) - 1
		}
		end := minInt(start+mp.EffectiveRowBlock, p.Rows)
		if planIdx >= 0 && planIdx < len(p.RowBlocks) && p.RowBlocks[planIdx].End < end {
			end = p.RowBlocks[planIdx].End
		}
		er := ExecutionRow{Index: executionIndex, PlanRowBlock: planIdx, RowStart: start, RowEnd: end}
		colIdx := 0
		for cstart := 0; cstart < p.DenseCols; cstart += mp.EffectiveColChunk {
			cend := minInt(cstart+mp.EffectiveColChunk, p.DenseCols)
			er.Columns = append(er.Columns, ExecutionColumn{Index: colIdx, Start: cstart, End: cend})
			outputBytes := int64(end-start) * rowBytes
			workspaceBytes := int64(end-start) * rowBytes
			denseBytes := int64(maxInt(1, minInt(p.ColBlockSize, p.Cols))) * int64(cend-cstart) * 8
			total := outputBytes + workspaceBytes + denseBytes
			if mp.PeakLimitBytes > 0 && total > mp.PeakLimitBytes {
				return mp, &Error{
					Code:    CodeMemoryBudget,
					Message: fmt.Sprintf("execution step requires %d bytes, peak limit is %d", total, mp.PeakLimitBytes),
					Memory: &MemoryBreakdown{
						PeakLimitBytes: mp.PeakLimitBytes, OutputBufferBytes: outputBytes,
						WorkspaceBytes: workspaceBytes, DenseSliceBytes: denseBytes,
						RequiredBytes: total, Rows: end - start, DenseColumns: p.DenseCols, ColumnChunk: cend - cstart,
					},
				}
			}
			mp.Steps = append(mp.Steps, MemoryStep{
				Step: step, ExecutionRow: executionIndex, PlanRowBlock: planIdx,
				RowStart: start, RowEnd: end, ColStart: cstart, ColEnd: cend,
				OutputBufferBytes: outputBytes, WorkspaceBytes: workspaceBytes,
				DenseSliceBytes: denseBytes, EstimatedTotalByte: total,
			})
			if total > mp.MaxEstimateBytes {
				mp.MaxEstimateBytes = total
			}
			step++
			colIdx++
		}
		mp.Rows = append(mp.Rows, er)
		executionIndex++
		start = end
	}
	if mp.PeakLimitBytes > 0 && mp.MaxEstimateBytes > mp.PeakLimitBytes {
		return mp, &Error{Code: CodeMemoryBudget, Message: "planned peak memory exceeds limit"}
	}
	return mp, nil
}

func memoryBudgetError(limit, outputWorkspace int64, rows, denseCols, chunk int) error {
	return &Error{
		Code:    CodeMemoryBudget,
		Message: fmt.Sprintf("peak memory budget %d bytes is smaller than one output/workspace row (%d bytes)", limit, outputWorkspace),
		Memory: &MemoryBreakdown{
			PeakLimitBytes: limit, OutputBufferBytes: outputWorkspace / 2,
			WorkspaceBytes: outputWorkspace / 2, RequiredBytes: outputWorkspace,
			Rows: rows, DenseColumns: denseCols, ColumnChunk: chunk,
		},
	}
}
