package engine

import (
	"fmt"

	"columnar-executor/internal/model"
)

const DefaultLargeVectorThreshold = 128

func DecidePath(requestedBatchSize, requestedThreshold, selectedRows int) (model.PathDecision, error) {
	if requestedBatchSize <= 0 || requestedBatchSize > 1_000_000 {
		return model.PathDecision{}, model.NewBizError(model.CodeInvalidRequest, "batch_size must be in [1,1000000]").
			With("batch_size", requestedBatchSize)
	}
	threshold := requestedThreshold
	if threshold == 0 {
		threshold = DefaultLargeVectorThreshold
	}
	if threshold < 1 || threshold > 1_000_000 {
		return model.PathDecision{}, model.NewBizError(model.CodeInvalidRequest, "large_vector_threshold must be in [1,1000000]").
			With("large_vector_threshold", threshold)
	}

	path := model.PathSmall
	reason := ""
	batchCount := 0
	if selectedRows > 0 {
		batchCount = (selectedRows + requestedBatchSize - 1) / requestedBatchSize
	}
	if requestedBatchSize >= threshold && selectedRows >= threshold {
		path = model.PathLarge
		reason = fmt.Sprintf("batch_size=%d >= threshold=%d and selected_rows=%d >= threshold=%d; merge selected rows into one vector",
			requestedBatchSize, threshold, selectedRows, threshold)
		if selectedRows > 0 {
			batchCount = 1
		}
	} else if requestedBatchSize < threshold {
		reason = fmt.Sprintf("batch_size=%d < threshold=%d; advance in small batches", requestedBatchSize, threshold)
	} else {
		reason = fmt.Sprintf("selected_rows=%d < threshold=%d; advance in small batches", selectedRows, threshold)
	}

	return model.PathDecision{
		Path:           path,
		Reason:         reason,
		SelectedRows:   selectedRows,
		BatchSize:      requestedBatchSize,
		LargeThreshold: threshold,
		BatchCount:     batchCount,
	}, nil
}

func chunkRows(rows []int, decision model.PathDecision) [][]int {
	if len(rows) == 0 {
		return nil
	}
	if decision.Path == model.PathLarge {
		chunk := make([]int, len(rows))
		copy(chunk, rows)
		return [][]int{chunk}
	}
	var out [][]int
	for start := 0; start < len(rows); start += decision.BatchSize {
		end := start + decision.BatchSize
		if end > len(rows) {
			end = len(rows)
		}
		chunk := make([]int, end-start)
		copy(chunk, rows[start:end])
		out = append(out, chunk)
	}
	return out
}
