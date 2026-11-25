package spmm

import (
	"fmt"
	"math"
	"sort"
)

type RowTrace struct {
	Row                   int     `json:"row"`
	VisitedPositions      []int64 `json:"visited_positions"`
	VisitedColumns        []int64 `json:"visited_columns"`
	AccumulationPositions []int64 `json:"accumulation_positions"`
	AccumulationColumns   []int64 `json:"accumulation_columns"`
	DuplicateColumns      []int64 `json:"duplicate_columns,omitempty"`
	ExplicitZeroSkipped   int     `json:"explicit_zero_skipped"`
	ExplicitZeroKept      int     `json:"explicit_zero_kept"`
}

type BlockTrace struct {
	RowStart             int               `json:"row_start"`
	RowEnd               int               `json:"row_end"`
	AccumulationOrder    AccumulationOrder `json:"accumulation_order"`
	SumStrategy          SumStrategy       `json:"sum_strategy"`
	SkipExplicitZeros    bool              `json:"skip_explicit_zeros"`
	Rows                 []RowTrace        `json:"rows"`
	NonzerosVisited      int               `json:"nonzeros_visited"`
	ProcessedEntries     int               `json:"processed_entries"`
	ExplicitZerosSkipped int               `json:"explicit_zeros_skipped"`
	ExplicitZerosKept    int               `json:"explicit_zeros_kept"`
	DuplicateEntries     int               `json:"duplicate_entries"`
	EmptyRows            int               `json:"empty_rows"`
	NumericOperations    int64             `json:"numeric_operations"`
}

type BlockSummary struct {
	ExecutionRowIndex int    `json:"execution_row_index"`
	PlanRowBlockIndex int    `json:"plan_row_block_index"`
	RowStart          int    `json:"row_start"`
	RowEnd            int    `json:"row_end"`
	ColumnStart       int    `json:"column_start"`
	ColumnEnd         int    `json:"column_end"`
	OutputDigest      string `json:"output_digest"`
	TraceDigest       string `json:"trace_digest"`
	ErrorDigest       string `json:"error_digest"`
}

type BlockResult struct {
	Summary     BlockSummary `json:"summary"`
	Values      []float64    `json:"values"`
	Trace       BlockTrace   `json:"trace"`
	Error       ErrorReport  `json:"error"`
	InputDigest string       `json:"input_digest"`
}

type rowChunkOrder struct {
	positions []int64
}

// ExecuteRowUnit computes one indivisible execution-row unit. All execution
// column chunks are evaluated, but every sparse entry is assigned to exactly
// one chunk and visited exactly once.
func ExecuteRowUnit(m *CSRMatrix, dense *DenseMatrix, plan *Plan, unit ExecutionRow) (*BlockResult, error) {
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	if m.Rows != plan.Rows || m.Cols != plan.Cols || dense.Rows != plan.DenseRows || dense.Cols != plan.DenseCols {
		return nil, NewError(CodeInput, "input dimensions do not match plan")
	}
	if unit.RowStart < 0 || unit.RowEnd > m.Rows || unit.RowStart >= unit.RowEnd {
		return nil, NewError(CodeInput, "invalid execution row range [%d,%d)", unit.RowStart, unit.RowEnd)
	}
	if len(unit.Columns) == 0 {
		return nil, NewError(CodeState, "execution row has no column chunks")
	}
	for i, c := range unit.Columns {
		if c.Start < 0 || c.End > dense.Cols || c.Start >= c.End {
			return nil, NewError(CodeState, "invalid execution column range [%d,%d)", c.Start, c.End)
		}
		if i > 0 && unit.Columns[i-1].End > c.Start {
			return nil, NewError(CodeState, "overlapping execution column ranges")
		}
	}

	rows := unit.RowEnd - unit.RowStart
	out := make([]float64, rows*dense.Cols)
	trace, ordered, err := buildTraceAndOrder(m, plan, unit, dense.Cols)
	if err != nil {
		return nil, err
	}

	for localRow := 0; localRow < rows; localRow++ {
		outRow := localRow * dense.Cols
		positions := ordered[localRow][0].positions
		for _, chunk := range unit.Columns {
			if len(positions) == 0 {
				continue
			}
			width := chunk.End - chunk.Start
			var comp []float64
			if plan.SumStrategy == SumCompensated {
				comp = make([]float64, width)
			}
			for _, pos := range positions {
				a := m.Data[pos]
				bRow := int(m.Indices[pos]) * dense.Cols
				for j := chunk.Start; j < chunk.End; j++ {
					product := a * dense.Data[bRow+j]
					offset := outRow + j
					if plan.SumStrategy == SumCompensated {
						sum := out[offset]
						t := sum + product
						if math.Abs(sum) >= math.Abs(product) {
							comp[j-chunk.Start] += (sum - t) + product
						} else {
							comp[j-chunk.Start] += (product - t) + sum
						}
						out[offset] = t
					} else {
						out[offset] += product
					}
				}
			}
			if plan.SumStrategy == SumCompensated {
				for j := chunk.Start; j < chunk.End; j++ {
					out[outRow+j] += comp[j-chunk.Start]
				}
			}
		}
	}

	traceDigest := HashJSON(trace)
	outputDigest := DigestValues(unit.RowStart, unit.RowEnd, dense.Cols, out)
	errReport := EstimateBlockError(m, dense, plan, unit, out, trace)
	if errReport.ToleranceExceeded {
		var maxAbs, maxRel float64
		var rowStart, rowEnd, colStart, colEnd int
		if len(errReport.Samples) > 0 {
			maxAbs = errReport.MaxAbsDeviation
			maxRel = errReport.MaxRelativeDeviation
			for _, s := range errReport.Samples {
				if s.AbsDeviation == maxAbs {
					rowStart, rowEnd, colStart, colEnd = s.Row, s.Row+1, s.Col, s.Col+1
					break
				}
			}
		}
		return &BlockResult{
			Summary: BlockSummary{
				ExecutionRowIndex: unit.Index, PlanRowBlockIndex: unit.PlanRowBlock,
				RowStart: unit.RowStart, RowEnd: unit.RowEnd,
				ColumnStart: 0, ColumnEnd: dense.Cols,
				OutputDigest: outputDigest, TraceDigest: traceDigest,
				ErrorDigest: HashJSON(errReport),
			},
			Values: out, Trace: trace, Error: errReport,
			InputDigest: RowBlockFingerprint(m, dense, unit.RowStart, unit.RowEnd),
		}, &Error{
			Code:     CodeNumericMismatch,
			Message:  fmt.Sprintf("sampled high-precision error exceeds tolerance in row block %d", unit.Index),
			RowStart: rowStart, RowEnd: rowEnd, ColStart: colStart, ColEnd: colEnd,
			MaxAbsDev: maxAbs, MaxRelDev: maxRel,
		}
	}
	result := &BlockResult{
		Summary: BlockSummary{
			ExecutionRowIndex: unit.Index, PlanRowBlockIndex: unit.PlanRowBlock,
			RowStart: unit.RowStart, RowEnd: unit.RowEnd,
			ColumnStart: 0, ColumnEnd: dense.Cols,
			OutputDigest: outputDigest, TraceDigest: traceDigest,
			ErrorDigest: HashJSON(errReport),
		},
		Values: out, Trace: trace, Error: errReport,
		InputDigest: RowBlockFingerprint(m, dense, unit.RowStart, unit.RowEnd),
	}
	return result, nil
}

func buildTraceAndOrder(m *CSRMatrix, plan *Plan, unit ExecutionRow, denseCols int) (BlockTrace, [][]rowChunkOrder, error) {
	trace := BlockTrace{
		RowStart: unit.RowStart, RowEnd: unit.RowEnd,
		AccumulationOrder: plan.AccumulationOrder, SumStrategy: plan.SumStrategy,
		SkipExplicitZeros: plan.SkipExplicitZeros,
	}
	rows := unit.RowEnd - unit.RowStart
	ordered := make([][]rowChunkOrder, rows)
	for i := range ordered {
		ordered[i] = make([]rowChunkOrder, 1)
	}

	for r := unit.RowStart; r < unit.RowEnd; r++ {
		rt := RowTrace{Row: r}
		seen := make(map[int64]struct{})
		for p := m.Indptr[r]; p < m.Indptr[r+1]; p++ {
			pos := int64(p)
			col := m.Indices[p]
			rt.VisitedPositions = append(rt.VisitedPositions, pos)
			rt.VisitedColumns = append(rt.VisitedColumns, col)
			if _, ok := seen[col]; ok {
				rt.DuplicateColumns = append(rt.DuplicateColumns, col)
				trace.DuplicateEntries++
			} else {
				seen[col] = struct{}{}
			}
			if m.Data[p] == 0 {
				if plan.SkipExplicitZeros {
					rt.ExplicitZeroSkipped++
					trace.ExplicitZerosSkipped++
					continue
				}
				rt.ExplicitZeroKept++
				trace.ExplicitZerosKept++
			}
			rt.AccumulationPositions = append(rt.AccumulationPositions, pos)
			rt.AccumulationColumns = append(rt.AccumulationColumns, col)
			ordered[r-unit.RowStart][0].positions = append(ordered[r-unit.RowStart][0].positions, pos)
		}
		if len(rt.VisitedPositions) == 0 {
			trace.EmptyRows++
		}
		trace.NonzerosVisited += len(rt.VisitedPositions)
		trace.ProcessedEntries += len(rt.AccumulationPositions)
		if plan.AccumulationOrder == OrderColumnAscending {
			stableSortPositionsByColumn(ordered[r-unit.RowStart], m.Indices)
		} else {
			stableSortPositionsByAccumulationBlock(ordered[r-unit.RowStart], m.Indices, plan.ColBlocks)
		}
		// Keep the trace's explicit accumulation sequence in exactly the same
		// order as the numerical loop. For block order this is execution chunk
		// order, not the CSR storage order.
		rt.AccumulationPositions = rt.AccumulationPositions[:0]
		rt.AccumulationColumns = rt.AccumulationColumns[:0]
		for chunkIdx := range ordered[r-unit.RowStart] {
			for _, p := range ordered[r-unit.RowStart][chunkIdx].positions {
				rt.AccumulationPositions = append(rt.AccumulationPositions, int64(p))
				rt.AccumulationColumns = append(rt.AccumulationColumns, m.Indices[p])
			}
		}
		trace.Rows = append(trace.Rows, rt)
	}
	for r := 0; r < rows; r++ {
		trace.NumericOperations += int64(len(ordered[r][0].positions)) * int64(denseCols)
	}
	return trace, ordered, nil
}

func stableSortPositionsByColumn(chunks []rowChunkOrder, columns []int64) {
	for ci := range chunks {
		sort.SliceStable(chunks[ci].positions, func(i, j int) bool {
			return columns[chunks[ci].positions[i]] < columns[chunks[ci].positions[j]]
		})
	}
}

func stableSortPositionsByAccumulationBlock(chunks []rowChunkOrder, columns []int64, blocks []BlockDescriptor) {
	positions := chunks[0].positions
	if len(positions) < 2 || len(blocks) == 0 {
		return
	}
	buckets := make([][]int64, len(blocks))
	for _, p := range positions {
		col := columns[p]
		idx := 0
		lo, hi := 0, len(blocks)-1
		for lo <= hi {
			mid := (lo + hi) / 2
			if col < int64(blocks[mid].Start) {
				hi = mid - 1
			} else if col >= int64(blocks[mid].End) {
				lo = mid + 1
			} else {
				idx = mid
				break
			}
		}
		buckets[idx] = append(buckets[idx], p)
	}
	out := positions[:0]
	for _, bucket := range buckets {
		out = append(out, bucket...)
	}
	chunks[0].positions = out
}

func findChunk(chunks []ExecutionColumn, col int) int {
	lo, hi := 0, len(chunks)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		if col < chunks[mid].Start {
			hi = mid - 1
		} else if col >= chunks[mid].End {
			lo = mid + 1
		} else {
			return mid
		}
	}
	return -1
}

func DigestValues(rowStart, rowEnd, denseCols int, values []float64) string {
	bits := make([]uint64, len(values))
	for i, v := range values {
		bits[i] = math.Float64bits(v)
	}
	type payload struct {
		RowStart, RowEnd, DenseCols int
		Values                      []uint64
	}
	return HashJSON(payload{RowStart: rowStart, RowEnd: rowEnd, DenseCols: denseCols, Values: bits})
}
