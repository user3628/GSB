package plan

import (
	"fmt"

	"logagg/internal/errs"
)

const DefaultInt64PairRowWidth int64 = 16

type Request struct {
	BlockNo          int
	Partition        int
	RowCount         int
	DeclaredBytes    int64
	MemoryLimitBytes int64
	ReservedBytes    int64
	MinRowWidthBytes int64
}

type Segment struct {
	Index          int   `json:"index"`
	StartRow       int   `json:"start_row"`
	EndRow         int   `json:"end_row"`
	RowCount       int   `json:"row_count"`
	EstimatedBytes int64 `json:"estimated_bytes"`
}

type BlockPlan struct {
	BlockNo              int       `json:"block_no"`
	Partition            int       `json:"partition"`
	RowCount             int       `json:"row_count"`
	DeclaredBytes        int64     `json:"declared_bytes"`
	MemoryLimitBytes     int64     `json:"memory_limit_bytes"`
	ReservedBytes        int64     `json:"reserved_bytes"`
	AvailableBudgetBytes int64     `json:"available_budget_bytes"`
	MinRowWidthBytes     int64     `json:"min_row_width_bytes"`
	EffectiveRowWidth    int64     `json:"effective_row_width_bytes"`
	WholeDecode          bool      `json:"whole_decode"`
	SegmentRows          int       `json:"segment_rows"`
	SegmentCount         int       `json:"segment_count"`
	EstimatedPeakBytes   int64     `json:"estimated_peak_bytes"`
	Formula              string    `json:"formula"`
	Basis                []string  `json:"basis"`
	Segments             []Segment `json:"segments"`
}

func PlanBlock(req Request) (*BlockPlan, error) {
	minWidth := req.MinRowWidthBytes
	if minWidth <= 0 {
		minWidth = DefaultInt64PairRowWidth
	}
	if req.MemoryLimitBytes <= 0 {
		return nil, errs.New(errs.EInvalidRequest, "memory limit must be positive")
	}
	if req.RowCount < 0 {
		return nil, errs.New(errs.EInvalidRequest, "row count must be non-negative")
	}
	if req.DeclaredBytes < 0 || req.ReservedBytes < 0 {
		return nil, errs.New(errs.EInvalidRequest, "byte declarations must be non-negative")
	}

	available := req.MemoryLimitBytes - req.ReservedBytes
	plan := &BlockPlan{
		BlockNo:              req.BlockNo,
		Partition:            req.Partition,
		RowCount:             req.RowCount,
		DeclaredBytes:        req.DeclaredBytes,
		MemoryLimitBytes:     req.MemoryLimitBytes,
		ReservedBytes:        req.ReservedBytes,
		AvailableBudgetBytes: available,
		MinRowWidthBytes:     minWidth,
		Basis:                []string{},
	}

	if req.DeclaredBytes < minWidth {
		return nil, unbounded(plan, "declared_uncompressed_bytes_below_min_row_width")
	}
	if available < minWidth {
		return nil, unbounded(plan, "available_budget_below_min_row_width")
	}
	if req.RowCount > 0 && req.DeclaredBytes < int64(req.RowCount)*minWidth {
		return nil, unbounded(plan, "declared_bytes_below_row_count_times_min_width")
	}

	rowWidth := minWidth
	if req.RowCount > 0 {
		rowWidth = (req.DeclaredBytes + int64(req.RowCount) - 1) / int64(req.RowCount)
		if rowWidth < minWidth {
			rowWidth = minWidth
		}
	}
	plan.EffectiveRowWidth = rowWidth

	if req.RowCount == 0 || req.DeclaredBytes <= available {
		plan.WholeDecode = true
		plan.SegmentRows = req.RowCount
		plan.SegmentCount = 1
		plan.EstimatedPeakBytes = req.ReservedBytes + req.DeclaredBytes
		plan.Formula = "whole: peak=reserved+declared_bytes"
		plan.Basis = append(plan.Basis,
			fmt.Sprintf("declared_bytes=%d <= available_budget=%d", req.DeclaredBytes, available),
			fmt.Sprintf("estimated_peak=%d <= memory_limit=%d", plan.EstimatedPeakBytes, req.MemoryLimitBytes),
		)
		plan.Segments = []Segment{{
			Index:          0,
			StartRow:       0,
			EndRow:         req.RowCount,
			RowCount:       req.RowCount,
			EstimatedBytes: req.DeclaredBytes,
		}}
		return plan, nil
	}

	segmentRows := int(available / rowWidth)
	if segmentRows < 1 {
		return nil, unbounded(plan, "segment_rows_rounds_to_zero")
	}
	if req.RowCount > 0 && segmentRows > req.RowCount {
		segmentRows = req.RowCount
	}
	plan.WholeDecode = false
	plan.SegmentRows = segmentRows
	plan.SegmentCount = (req.RowCount + segmentRows - 1) / segmentRows
	plan.Formula = "segment_rows=floor(available_budget/ceil(declared_bytes/row_count)); peak=reserved+segment_rows*effective_row_width"
	plan.Basis = append(plan.Basis,
		fmt.Sprintf("declared_bytes=%d > available_budget=%d", req.DeclaredBytes, available),
		fmt.Sprintf("effective_row_width=ceil(%d/%d)=%d", req.DeclaredBytes, req.RowCount, rowWidth),
		fmt.Sprintf("segment_rows=floor(%d/%d)=%d", available, rowWidth, segmentRows),
	)

	peak := int64(0)
	for index, start := 0, 0; start < req.RowCount; index, start = index+1, start+segmentRows {
		end := start + segmentRows
		if end > req.RowCount {
			end = req.RowCount
		}
		rows := end - start
		bytes := int64(rows) * rowWidth
		if bytes > req.DeclaredBytes {
			bytes = req.DeclaredBytes
		}
		plan.Segments = append(plan.Segments, Segment{
			Index:          index,
			StartRow:       start,
			EndRow:         end,
			RowCount:       rows,
			EstimatedBytes: bytes,
		})
		if bytes > peak {
			peak = bytes
		}
	}
	plan.EstimatedPeakBytes = req.ReservedBytes + peak
	plan.Basis = append(plan.Basis, fmt.Sprintf("estimated_peak=%d <= memory_limit=%d", plan.EstimatedPeakBytes, req.MemoryLimitBytes))
	return plan, nil
}

func unbounded(plan *BlockPlan, reason string) error {
	coded := errs.New(errs.EBlockUnbounded, "block cannot be bounded by the per-block memory limit: %s", reason)
	coded.WithDetail("reason", reason).
		WithDetail("min_row_width_bytes", plan.MinRowWidthBytes).
		WithDetail("available_budget_bytes", plan.AvailableBudgetBytes).
		WithDetail("memory_limit_bytes", plan.MemoryLimitBytes).
		WithDetail("declared_bytes", plan.DeclaredBytes)
	return coded
}
