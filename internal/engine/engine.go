package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"columnar-executor/internal/model"
)

type Persistence interface {
	CreateExecution(ctx context.Context, view model.ExecutionView, batchNumbers []int) error
	AppendIntent(ctx context.Context, executionID, kind string, payload map[string]any) error
	SaveProvisionalBatch(ctx context.Context, executionID string, trace model.BatchTrace, aggregates []model.AggregateValue) error
	FinalizeExecution(ctx context.Context, executionID string, aggregates []model.AggregateValue) error
	FailExecution(ctx context.Context, executionID, finalStatus, code string, errorContext map[string]any) error
}

type Engine struct {
	Store     Persistence
	CrashHook func(executionID string, batchNo int)
}

func New(store Persistence) *Engine {
	return &Engine{Store: store}
}

func (e *Engine) Execute(ctx context.Context, snap model.Snapshot, chain model.Chain, request model.ExecuteRequest, selection model.Bitmap, executionID string) (*model.ExecutionView, error) {
	if e.Store == nil {
		return nil, model.NewBizError(model.CodeInternal, "execution persistence is not configured")
	}
	if strings.TrimSpace(executionID) == "" {
		return nil, model.NewBizError(model.CodeInvalidRequest, "execution_id is required")
	}
	if err := validateOperatorChainForExecution(chain, snap); err != nil {
		return nil, err
	}
	if err := selection.Validate(); err != nil {
		return nil, err
	}
	if selection.Rows != snap.RowCount {
		return nil, model.NewBizError(model.CodeBitmapMismatch, "selection bitmap rows do not match snapshot").
			With("bitmap_rows", selection.Rows).With("snapshot_rows", snap.RowCount)
	}
	if request.CrashAfterBatch > 0 && e.CrashHook == nil {
		return nil, model.NewBizError(model.CodeInvalidRequest, "crash hook is not enabled on this server")
	}

	decision, err := DecidePath(request.BatchSize, request.LargeVectorThreshold, selection.Count())
	if err != nil {
		return nil, err
	}
	if request.CrashAfterBatch > decision.BatchCount {
		return nil, model.NewBizError(model.CodeInvalidRequest, "crash_after_batch exceeds batch count").
			With("crash_after_batch", request.CrashAfterBatch).With("batch_count", decision.BatchCount)
	}
	if err := validateFaultSpecs(request.Injections, decision.BatchCount); err != nil {
		return nil, err
	}

	startedAt := model.NowUTC()
	view := model.ExecutionView{
		ExecutionID:     executionID,
		ChainID:         chain.ID,
		ChainVersion:    chain.Version,
		SnapshotVersion: snap.Version,
		BitmapForm:      request.BitmapForm,
		Bitmap:          selection.Clone(),
		BatchSize:       decision.BatchSize,
		LargeThreshold:  decision.LargeThreshold,
		Path:            decision,
		Status:          model.StatusRunning,
		Published:       false,
		StartedAt:       startedAt,
	}
	batchNumbers := make([]int, decision.BatchCount)
	for i := range batchNumbers {
		batchNumbers[i] = i + 1
	}
	if err := e.Store.CreateExecution(ctx, view, batchNumbers); err != nil {
		return nil, wrapPersistenceError(err, executionID)
	}

	if fault, ok := findFaultForKind(request.Injections, model.FaultBitmapMismatch); ok {
		biz := bitmapFaultError(fault, snap.RowCount, decision.BatchCount).With("execution_id", executionID)
		view.Status = model.StatusFailed
		view.ErrorCode = biz.Code
		view.ErrorContext = biz.Context
		_ = e.failExecution(executionID, model.StatusFailed, biz)
		return nil, biz
	}

	specs := aggregateSpecs(chain)
	finalAccs, err := newAccumulators(snap, specs)
	if err != nil {
		return nil, e.failWithBiz(executionID, model.StatusFailed, err)
	}
	projection := projectionColumns(chain)
	selectedRows := selection.RowIDs()
	chunks := chunkRows(selectedRows, decision)
	traces := make([]model.BatchTrace, 0, len(chunks))

	for index, chunk := range chunks {
		batchNo := index + 1
		if err := e.Store.AppendIntent(ctx, executionID, "batch_prepare", map[string]any{
			"batch": batchNo,
			"rows":  len(chunk),
		}); err != nil {
			return nil, e.failExecution(executionID, model.StatusFailed, wrapPersistenceError(err, executionID))
		}

		if fault, ok := findFaultForBatch(request.Injections, batchNo); ok {
			biz := faultBizError(fault, batchNo, snap.RowCount).With("execution_id", executionID)
			status := model.StatusFailed
			if biz.Code == model.CodeExecAborted {
				status = model.StatusAborted
			}
			view.Status = status
			view.ErrorCode = biz.Code
			view.ErrorContext = biz.Context
			_ = e.failExecution(executionID, status, biz)
			return nil, biz
		}

		batchInput := model.NewNone(snap.RowCount)
		for _, row := range chunk {
			_ = batchInput.Set(row)
		}
		current := batchInput
		stages := make([]model.FilterStage, 0)
		for _, op := range chain.Operators {
			if operatorKind(op) != "filter" {
				continue
			}
			next, stage, err := applyFilter(snap, current, op.Filter.Predicate, batchNo)
			if err != nil {
				return nil, e.failWithBiz(executionID, model.StatusFailed, err)
			}
			current = next
			stages = append(stages, stage)
		}
		filteredRows := current.RowIDs()
		batchAccs, err := newAccumulators(snap, specs)
		if err != nil {
			return nil, e.failWithBiz(executionID, model.StatusFailed, err)
		}
		aggregates, err := aggregateRows(snap, filteredRows, batchNo, batchAccs, finalAccs)
		if err != nil {
			return nil, e.failWithBiz(executionID, model.StatusFailed, err)
		}
		rowStart := 0
		if decision.Path == model.PathSmall {
			rowStart = index * decision.BatchSize
		}
		trace := model.BatchTrace{
			BatchNo:      batchNo,
			RowStart:     rowStart,
			RowEnd:       rowStart + len(chunk),
			InputRows:    len(chunk),
			FilteredRows: len(filteredRows),
			OutputRows:   len(filteredRows),
			RowIDs:       intRows(filteredRows),
			Projection:   append([]string(nil), projection...),
			Rows:         projectRows(snap, filteredRows, projection),
			Aggregates:   aggregates,
			FilterStages: stages,
		}
		if err := e.Store.SaveProvisionalBatch(ctx, executionID, trace, aggregates); err != nil {
			return nil, e.failExecution(executionID, model.StatusFailed, wrapPersistenceError(err, executionID))
		}
		traces = append(traces, trace)

		if request.CrashAfterBatch > 0 && batchNo == request.CrashAfterBatch {
			// The production hook calls os.Exit and never returns. A test/custom
			// hook may return after observing the durable provisional state; in
			// that case the execution continues normally.
			e.CrashHook(executionID, batchNo)
		}
	}

	finalAggregates, err := finalAggregateValues(finalAccs)
	if err != nil {
		return nil, e.failWithBiz(executionID, model.StatusFailed, err)
	}
	if err := e.Store.FinalizeExecution(ctx, executionID, finalAggregates); err != nil {
		return nil, e.failExecution(executionID, model.StatusFailed, wrapPersistenceError(err, executionID))
	}
	view.Status = model.StatusCommitted
	view.Published = true
	view.FinishedAt = model.NowUTC()
	view.Batches = traces
	view.Aggregates = finalAggregates
	return &view, nil
}

func intRows(rows []int) []int64 {
	out := make([]int64, len(rows))
	for i, row := range rows {
		out[i] = int64(row)
	}
	return out
}

func validateFaultSpecs(faults []model.FaultSpec, batchCount int) error {
	for _, fault := range faults {
		if _, ok := model.BusinessCodeForFault(fault.Kind); !ok {
			return model.NewBizError(model.CodeInvalidRequest, "unsupported fault kind").With("kind", fault.Kind)
		}
		if strings.EqualFold(fault.Kind, model.FaultBitmapMismatch) {
			continue
		}
		batch := fault.Batch
		if batch == 0 {
			batch = 1
		}
		if batch < 1 || batch > batchCount {
			return model.NewBizError(model.CodeInvalidRequest, "fault batch is outside execution batch range").
				With("kind", fault.Kind).With("batch", batch).With("batch_count", batchCount)
		}
	}
	return nil
}

func findFaultForKind(faults []model.FaultSpec, kind string) (model.FaultSpec, bool) {
	for _, fault := range faults {
		if strings.EqualFold(strings.TrimSpace(fault.Kind), kind) {
			return fault, true
		}
	}
	return model.FaultSpec{}, false
}

func findFaultForBatch(faults []model.FaultSpec, batchNo int) (model.FaultSpec, bool) {
	for _, fault := range faults {
		if strings.EqualFold(strings.TrimSpace(fault.Kind), model.FaultBitmapMismatch) {
			continue
		}
		batch := fault.Batch
		if batch == 0 {
			batch = 1
		}
		if batch == batchNo {
			return fault, true
		}
	}
	return model.FaultSpec{}, false
}

func bitmapFaultError(fault model.FaultSpec, actualRows, batchCount int) *model.BizError {
	expected := fault.ExpectedRows
	if expected == 0 {
		expected = actualRows
	}
	actual := fault.ActualRows
	if actual == 0 {
		if expected == 0 {
			actual = 1
		} else {
			actual = expected - 1
		}
	}
	return model.NewBizError(model.CodeBitmapMismatch, "injected bitmap length mismatch").
		With("expected_rows", expected).With("actual_rows", actual).
		With("expected_words", model.ExpectedWords(expected)).With("actual_words", model.ExpectedWords(actual)).
		With("batch_count", batchCount)
}

func faultBizError(fault model.FaultSpec, batchNo, rowCount int) *model.BizError {
	code, _ := model.BusinessCodeForFault(fault.Kind)
	column := strings.TrimSpace(fault.Column)
	switch code {
	case model.CodeBitmapMismatch:
		return bitmapFaultError(fault, rowCount, batchNo)
	case model.CodeNumericOverflow:
		if column == "" {
			column = "price"
		}
		return model.NewBizError(code, "injected fixed point overflow").
			With("column", column).With("batch", batchNo).With("operation", "aggregate_sum")
	case model.CodeDictIndex:
		if column == "" {
			column = "category"
		}
		index := fault.DictIndex
		if index == 0 && fault.Row == 0 {
			index = 999
		}
		return model.NewBizError(code, "injected dictionary index out of range").
			With("column", column).With("batch", batchNo).With("dictionary_index", index)
	case model.CodeDivideByZero:
		if column == "" {
			column = "price"
		}
		return model.NewBizError(code, "injected fixed point divide by zero").
			With("column", column).With("batch", batchNo).With("divisor_units", 0)
	case model.CodeExecAborted:
		return model.NewBizError(code, "injected execution abort marker").
			With("batch", batchNo).With("marker", "exec_aborted")
	default:
		return model.NewBizError(model.CodeInvalidRequest, "unsupported fault kind").With("kind", fault.Kind)
	}
}

func (e *Engine) failWithBiz(executionID, status string, err error) error {
	biz, ok := model.AsBizError(err)
	if !ok {
		biz = model.NewBizError(model.CodeInternal, err.Error())
	}
	biz.With("execution_id", executionID)
	_ = e.failExecution(executionID, status, biz)
	return biz
}

func (e *Engine) failExecution(executionID, status string, biz *model.BizError) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.Store.FailExecution(ctx, executionID, status, biz.Code, biz.Context); err != nil {
		return model.NewBizError(model.CodeInternal, "failed to persist execution failure").
			With("execution_id", executionID).With("cause", err.Error()).With("original_code", biz.Code)
	}
	return biz
}

func wrapPersistenceError(err error, executionID string) *model.BizError {
	if err == nil {
		return nil
	}
	var biz *model.BizError
	if errors.As(err, &biz) {
		biz.With("execution_id", executionID)
		return biz
	}
	return model.NewBizError(model.CodeInternal, fmt.Sprintf("persistence failure: %v", err)).With("execution_id", executionID)
}
