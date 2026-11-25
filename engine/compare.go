package engine

import (
	"math"
	"sort"

	"spmmopt/spmm"
)

// ComparePaths verifies that the independent interactive computation and the
// settlement adjudication agree on the requested rows, block summaries, and
// sampled error bounds. The reported first difference is a row-block index.
func ComparePaths(query, settlement *Result) error {
	if query == nil || settlement == nil {
		return spmm.NewError(spmm.CodePathMismatch, "both path results are required")
	}
	if query.PlanKey != settlement.PlanKey || query.PlanVersion != settlement.PlanVersion || query.PlanDigest != settlement.PlanDigest {
		zero := 0
		return &spmm.Error{
			Code: spmm.CodePathMismatch, Message: "paths refer to different plan snapshots",
			PlanKey: query.PlanKey, PlanVersion: query.PlanVersion,
			Expectation: query.PlanDigest, Actual: settlement.PlanDigest, FirstDiff: &zero,
		}
	}
	settleBlocks := make(map[int]ResultBlock, len(settlement.Blocks))
	for _, b := range settlement.Blocks {
		settleBlocks[b.Summary.ExecutionRowIndex] = b
	}
	blocks := append([]ResultBlock(nil), query.Blocks...)
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].Summary.ExecutionRowIndex < blocks[j].Summary.ExecutionRowIndex })
	for _, qb := range blocks {
		sb, ok := settleBlocks[qb.Summary.ExecutionRowIndex]
		if !ok {
			idx := qb.Summary.ExecutionRowIndex
			return pathMismatch(qb.Summary, "settlement is missing the row block", &idx)
		}
		if qb.Summary.OutputDigest != sb.Summary.OutputDigest || qb.Summary.TraceDigest != sb.Summary.TraceDigest ||
			qb.Summary.ErrorDigest != sb.Summary.ErrorDigest || qb.Summary.RowStart != sb.Summary.RowStart || qb.Summary.RowEnd != sb.Summary.RowEnd {
			idx := qb.Summary.ExecutionRowIndex
			return pathMismatch(qb.Summary, "block summary or digest differs", &idx)
		}
		if !sameFloatBits(qb.Error.UpperBoundAbs, sb.Error.UpperBoundAbs) || !sameFloatBits(qb.Error.UpperBoundRel, sb.Error.UpperBoundRel) ||
			qb.Error.ToleranceExceeded != sb.Error.ToleranceExceeded || spmm.HashJSON(qb.Error) != spmm.HashJSON(sb.Error) {
			idx := qb.Summary.ExecutionRowIndex
			return &spmm.Error{
				Code: spmm.CodePathMismatch, Message: "error bound or sampled error report differs",
				PlanKey: query.PlanKey, PlanVersion: query.PlanVersion,
				RowStart: qb.Summary.RowStart, RowEnd: qb.Summary.RowEnd,
				MaxAbsDev: qb.Error.UpperBoundAbs, MaxRelDev: sb.Error.UpperBoundRel,
				FirstDiff: &idx,
			}
		}
	}
	// Values are compared bit-for-bit after aligning the two row origins. The
	// query values contain exactly the requested rows; settlement may contain a
	// larger complete prefix/ranges.
	start := query.RequestedRowStart
	end := query.RequestedRowEnd
	qOffset := (start - query.RowStart) * query.DenseCols
	sOffset := (start - settlement.RowStart) * settlement.DenseCols
	width := (end - start) * query.DenseCols
	if qOffset < 0 || sOffset < 0 || qOffset+width > len(query.Values) || sOffset+width > len(settlement.Values) {
		idx := firstQueryBlock(query, start)
		return pathMismatch(spmm.BlockSummary{ExecutionRowIndex: idx}, "result coverage does not include the requested row range", &idx)
	}
	for i := 0; i < width; i++ {
		if math.Float64bits(query.Values[qOffset+i]) != math.Float64bits(settlement.Values[sOffset+i]) {
			row := start + i/query.DenseCols
			idx := blockForRow(query, row)
			return &spmm.Error{
				Code: spmm.CodePathMismatch, Message: "output value differs between paths",
				PlanKey: query.PlanKey, PlanVersion: query.PlanVersion,
				RowStart: row, RowEnd: row + 1, ColStart: i % query.DenseCols, ColEnd: i%query.DenseCols + 1,
				FirstDiff: &idx,
			}
		}
	}
	return nil
}

func pathMismatch(summary spmm.BlockSummary, detail string, idx *int) *spmm.Error {
	return &spmm.Error{
		Code: spmm.CodePathMismatch, Message: detail,
		RowStart: summary.RowStart, RowEnd: summary.RowEnd, FirstDiff: idx,
	}
}

func firstQueryBlock(r *Result, row int) int {
	if len(r.Blocks) > 0 {
		return r.Blocks[0].Summary.ExecutionRowIndex
	}
	return 0
}

func blockForRow(r *Result, row int) int {
	for _, b := range r.Blocks {
		if row >= b.Summary.RowStart && row < b.Summary.RowEnd {
			return b.Summary.ExecutionRowIndex
		}
	}
	return firstQueryBlock(r, row)
}

func sameFloatBits(a, b float64) bool { return math.Float64bits(a) == math.Float64bits(b) }
