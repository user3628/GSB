package engine

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"spmmopt/spmm"
	"spmmopt/store"
)

const (
	PathInteractive = "interactive"
	PathSettlement  = "settlement"
)

type Engine struct {
	Store   *store.Store
	CSR     *spmm.CSRMatrix
	Dense   *spmm.DenseMatrix
	Plan    spmm.Plan
	PlanKey string
	Ref     store.PlanRef
}

type ResultBlock struct {
	Summary spmm.BlockSummary `json:"summary"`
	Error   spmm.ErrorReport  `json:"error"`
}

type Result struct {
	Path              string           `json:"path"`
	PlanKey           string           `json:"plan_key"`
	PlanVersion       int64            `json:"plan_version"`
	PlanDigest        string           `json:"plan_digest"`
	QueryID           string           `json:"query_id,omitempty"`
	RequestedRowStart int              `json:"requested_row_start"`
	RequestedRowEnd   int              `json:"requested_row_end"`
	RowStart          int              `json:"row_start"`
	RowEnd            int              `json:"row_end"`
	DenseCols         int              `json:"dense_cols"`
	Values            []float64        `json:"values"`
	Blocks            []ResultBlock    `json:"blocks"`
	AggregateError    spmm.ErrorReport `json:"aggregate_error"`
	ElapsedMS         float64          `json:"elapsed_ms"`
	Stopped           bool             `json:"stopped,omitempty"`
	CompletedUnits    int              `json:"completed_units"`
	TotalUnits        int              `json:"total_units"`
	CanonicalDigest   string           `json:"canonical_digest"`
}

type ExecutionEventKind string

const (
	EventBeforeCommit ExecutionEventKind = "before_commit"
	EventAfterCommit  ExecutionEventKind = "after_commit"
	EventAfterCompute ExecutionEventKind = "after_compute"
)

type ExecutionEvent struct {
	Kind        ExecutionEventKind `json:"kind"`
	UnitIndex   int                `json:"unit_index"`
	RowStart    int                `json:"row_start"`
	RowEnd      int                `json:"row_end"`
	PlanVersion int64              `json:"plan_version"`
}

type RunOptions struct {
	Range           *RowRange
	Persist         bool
	Stop            <-chan struct{}
	FaultHook       func(ExecutionEvent) error
	CompletionState string
}

type RowRange struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

type QueryOptions struct {
	RowStart      int
	RowEnd        int
	QueryID       string
	LatencyBudget time.Duration
}

func Open(ctx context.Context, st *store.Store, csr *spmm.CSRMatrix, dense *spmm.DenseMatrix, planKey string) (*Engine, error) {
	if st == nil || csr == nil || dense == nil {
		return nil, spmm.NewError(spmm.CodeInput, "store, sparse matrix, and dense matrix are required")
	}
	rec, err := st.Head(ctx, planKey)
	if err != nil {
		return nil, err
	}
	expected := &store.PlanRef{
		PlanKey: planKey, Version: rec.Version, Digest: rec.Digest,
		ShapeFingerprint:     rec.Plan.ShapeFingerprint,
		StructureFingerprint: rec.Plan.StructureFingerprint,
		DenseFingerprint:     rec.Plan.DenseFingerprint,
	}
	integrity, err := st.ReconcilePlan(ctx, planKey, expected)
	if err != nil {
		return nil, err
	}
	if integrity.Status != store.IntegrityOK && integrity.Status != store.IntegrityAdvance {
		return nil, &spmm.Error{
			Code: spmm.CodeStalePublish, Message: "plan snapshot failed integrity check",
			PlanKey: planKey, PlanVersion: rec.Version, Actual: string(integrity.Status),
			Detail: integrity.Reason,
		}
	}
	stats := csr.Stats(dense)
	if stats.ShapeFingerprint != rec.Plan.ShapeFingerprint ||
		stats.StructureFingerprint != rec.Plan.StructureFingerprint ||
		stats.DenseFingerprint != rec.Plan.DenseFingerprint {
		return nil, spmm.NewError(spmm.CodeInput, "input matrices do not match the published plan snapshot")
	}
	return &Engine{
		Store: st, CSR: csr, Dense: dense, Plan: rec.Plan, PlanKey: planKey,
		Ref: store.PlanRef{PlanKey: planKey, Version: rec.Version, Digest: rec.Digest},
	}, nil
}

func (e *Engine) Settle(ctx context.Context, opts RunOptions) (*Result, error) {
	if opts.Range != nil {
		return e.runRange(ctx, PathSettlement, *opts.Range, opts)
	}
	return e.runRange(ctx, PathSettlement, RowRange{Start: 0, End: e.CSR.Rows}, opts)
}

func (e *Engine) SettleRange(ctx context.Context, start, end int, opts RunOptions) (*Result, error) {
	opts.Range = &RowRange{Start: start, End: end}
	return e.runRange(ctx, PathSettlement, RowRange{Start: start, End: end}, opts)
}

func (e *Engine) Query(ctx context.Context, opts QueryOptions) (*Result, error) {
	start := time.Now()
	if opts.RowStart < 0 || opts.RowEnd > e.CSR.Rows || opts.RowStart >= opts.RowEnd {
		return nil, spmm.NewError(spmm.CodeInput, "invalid query row range [%d,%d)", opts.RowStart, opts.RowEnd)
	}
	budget := opts.LatencyBudget
	if budget == 0 {
		budget = time.Duration(e.Plan.InteractiveBudgetMS * float64(time.Millisecond))
	}
	queryID := opts.QueryID
	if queryID == "" {
		type qid struct {
			Key        string
			Version    int64
			Start, End int
			Budget     time.Duration
		}
		queryID = "qry-" + spmm.HashJSON(qid{e.PlanKey, e.PlanKeyVersion(), opts.RowStart, opts.RowEnd, budget})[:16]
	}
	units := e.unitsForRange(opts.RowStart, opts.RowEnd)
	if budget > 0 {
		estimated := e.estimateQueryDuration(units)
		if estimated > budget {
			elapsed := time.Since(start)
			return nil, latencyError(queryID, opts, budget, elapsed, "predicted latency exceeds budget")
		}
	}
	blocks := make([]ResultBlock, 0, len(units))
	fullValues := make([]float64, 0)
	for _, unit := range units {
		computed, err := spmm.ExecuteRowUnit(e.CSR, e.Dense, &e.Plan, unit)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, ResultBlock{Summary: computed.Summary, Error: computed.Error})
		fullValues = append(fullValues, computed.Values...)
		if budget > 0 {
			elapsed := time.Since(start)
			if elapsed > budget {
				return nil, latencyError(queryID, opts, budget, elapsed, "latency budget exhausted while computing query")
			}
		}
	}
	if len(units) == 0 {
		return nil, spmm.NewError(spmm.CodeState, "query selected no execution rows")
	}
	result := e.buildResult(PathInteractive, opts.RowStart, opts.RowEnd, units, blocks, fullValues)
	result.QueryID = queryID
	result.ElapsedMS = durationMS(time.Since(start))
	result.CanonicalDigest = canonicalDigest(result)
	return result, nil
}

func (e *Engine) PlanKeyVersion() int64 { return e.Ref.Version }

func (e *Engine) runRange(ctx context.Context, path string, requested RowRange, opts RunOptions) (*Result, error) {
	startTime := time.Now()
	if requested.Start < 0 || requested.End > e.CSR.Rows || requested.Start >= requested.End {
		return nil, spmm.NewError(spmm.CodeInput, "invalid row range [%d,%d)", requested.Start, requested.End)
	}
	units := e.unitsForRange(requested.Start, requested.End)
	if len(units) == 0 {
		return nil, spmm.NewError(spmm.CodeState, "range selected no execution rows")
	}
	persist := opts.Persist
	var committed []store.BlockRecord
	if persist {
		var err error
		committed, err = e.Store.LoadBlocks(ctx, e.Ref)
		if err != nil {
			return nil, err
		}
	}
	committedByIndex := make(map[int]store.BlockRecord, len(committed))
	for _, b := range committed {
		committedByIndex[b.ExecutionRowIndex] = b
	}
	blocks := make([]ResultBlock, 0, len(units))
	valuesByUnit := make(map[int][]float64, len(units))
	completed := 0
	stopped := false
	for _, unit := range units {
		if opts.Stop != nil {
			select {
			case <-opts.Stop:
				stopped = true
			default:
			}
		}
		if stopped {
			break
		}
		if rec, ok := committedByIndex[unit.Index]; ok {
			if rec.RowStart != unit.RowStart || rec.RowEnd != unit.RowEnd || rec.PlanRowBlockIndex != unit.PlanRowBlock {
				return nil, spmm.NewError(spmm.CodeState, "persisted row block %d does not match execution plan", unit.Index)
			}
			blocks = append(blocks, ResultBlock{Summary: summaryFromRecord(rec), Error: rec.Error})
			valuesByUnit[unit.Index] = append([]float64(nil), rec.Values...)
			completed++
			continue
		}
		computed, err := spmm.ExecuteRowUnit(e.CSR, e.Dense, &e.Plan, unit)
		if err != nil {
			return nil, err
		}
		if opts.FaultHook != nil {
			if err := opts.FaultHook(ExecutionEvent{Kind: EventAfterCompute, UnitIndex: unit.Index, RowStart: unit.RowStart, RowEnd: unit.RowEnd, PlanVersion: e.Ref.Version}); err != nil {
				return nil, err
			}
		}
		if persist {
			if opts.FaultHook != nil {
				if err := opts.FaultHook(ExecutionEvent{Kind: EventBeforeCommit, UnitIndex: unit.Index, RowStart: unit.RowStart, RowEnd: unit.RowEnd, PlanVersion: e.Ref.Version}); err != nil {
					return nil, err
				}
			}
			if _, err := e.Store.PublishBlock(ctx, computed, store.BlockExpectation{PlanKey: e.PlanKey, Version: e.Ref.Version, Digest: e.Ref.Digest}); err != nil {
				return nil, err
			}
			if opts.FaultHook != nil {
				if err := opts.FaultHook(ExecutionEvent{Kind: EventAfterCommit, UnitIndex: unit.Index, RowStart: unit.RowStart, RowEnd: unit.RowEnd, PlanVersion: e.Ref.Version}); err != nil {
					return nil, err
				}
			}
		}
		blocks = append(blocks, ResultBlock{Summary: computed.Summary, Error: computed.Error})
		valuesByUnit[unit.Index] = computed.Values
		completed++
	}
	// Assemble values in global row order. A graceful stop may leave a
	// contiguous prefix; a range query normally covers every selected unit.
	firstRow := units[0].RowStart
	lastRow := firstRow
	for _, unit := range units {
		if values, ok := valuesByUnit[unit.Index]; ok {
			if unit.RowStart < firstRow {
				firstRow = unit.RowStart
			}
			if unit.RowEnd > lastRow {
				lastRow = unit.RowEnd
			}
			_ = values
		}
	}
	if completed == 0 {
		firstRow, lastRow = requested.Start, requested.Start
	}
	values := make([]float64, 0, maxInt(0, lastRow-firstRow)*e.Dense.Cols)
	for _, unit := range units {
		v, ok := valuesByUnit[unit.Index]
		if !ok {
			break
		}
		values = append(values, v...)
	}
	result := e.buildResult(path, requested.Start, requested.End, units, blocks, values)
	result.RowStart, result.RowEnd = firstRow, firstRow+len(values)/e.Dense.Cols
	result.Stopped = stopped
	result.CompletedUnits = completed
	result.ElapsedMS = durationMS(time.Since(startTime))
	if persist {
		state := opts.CompletionState
		if state == "" {
			if stopped {
				state = "stopped"
			} else if completed == len(units) {
				state = "completed"
			} else {
				state = "running"
			}
		}
		if completed > 0 || state != "stopped" {
			if err := e.Store.SetState(ctx, e.Ref, state); err != nil {
				return nil, err
			}
		}
	}
	result.CanonicalDigest = canonicalDigest(result)
	return result, nil
}

func (e *Engine) unitsForRange(start, end int) []spmm.ExecutionRow {
	var out []spmm.ExecutionRow
	for _, u := range e.Plan.Memory.Rows {
		if u.RowEnd <= start {
			continue
		}
		if u.RowStart >= end {
			break
		}
		out = append(out, u)
	}
	return out
}

func (e *Engine) estimateQueryDuration(units []spmm.ExecutionRow) time.Duration {
	if e.CSR.Rows == 0 {
		return 0
	}
	rows := 0
	for _, u := range units {
		rows += u.RowEnd - u.RowStart
	}
	fraction := float64(rows) / float64(e.CSR.Rows)
	us := e.Plan.Selection.EstimatedLatency * fraction
	return time.Duration(us * float64(time.Microsecond))
}

func latencyError(queryID string, opts QueryOptions, budget time.Duration, elapsed time.Duration, detail string) *spmm.Error {
	ms := durationMS(elapsed)
	if ms <= 0 {
		ms = 0.001
	}
	return &spmm.Error{
		Code: spmm.CodeLatencyBudget, Message: "interactive query exceeded its latency budget",
		QueryID: queryID, ElapsedMS: ms, BudgetMS: durationMS(budget),
		RowStart: opts.RowStart, RowEnd: opts.RowEnd, Detail: detail,
	}
}

func durationMS(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func (e *Engine) buildResult(path string, requestedStart, requestedEnd int, units []spmm.ExecutionRow, blocks []ResultBlock, values []float64) *Result {
	fullStart := 0
	fullEnd := 0
	if len(values) > 0 && len(units) > 0 {
		fullStart = units[0].RowStart
		fullEnd = fullStart + len(values)/e.Dense.Cols
	}
	// Trim to the requested interval only when the caller asked for a subset.
	trimmed := values
	if fullStart < requestedStart || fullEnd > requestedEnd {
		startOffset := maxInt(0, requestedStart-fullStart) * e.Dense.Cols
		endOffset := minInt(len(values), (requestedEnd-fullStart)*e.Dense.Cols)
		if startOffset <= endOffset && endOffset <= len(values) {
			trimmed = values[startOffset:endOffset]
			fullStart, fullEnd = requestedStart, requestedEnd
		}
	}
	r := &Result{
		Path: path, PlanKey: e.PlanKey, PlanVersion: e.Ref.Version, PlanDigest: e.Ref.Digest,
		RequestedRowStart: requestedStart, RequestedRowEnd: requestedEnd,
		RowStart: fullStart, RowEnd: fullEnd, DenseCols: e.Dense.Cols,
		Values: trimmed, Blocks: blocks, TotalUnits: len(units), CompletedUnits: len(blocks),
	}
	r.AggregateError = aggregateErrors(blocks, requestedStart, requestedEnd, e.Dense.Cols)
	return r
}

func summaryFromRecord(rec store.BlockRecord) spmm.BlockSummary {
	return spmm.BlockSummary{
		ExecutionRowIndex: rec.ExecutionRowIndex, PlanRowBlockIndex: rec.PlanRowBlockIndex,
		RowStart: rec.RowStart, RowEnd: rec.RowEnd, ColumnStart: rec.ColStart, ColumnEnd: rec.ColEnd,
		OutputDigest: rec.OutputDigest, TraceDigest: rec.TraceDigest, ErrorDigest: rec.ErrorDigest,
	}
}

func aggregateErrors(blocks []ResultBlock, rowStart, rowEnd, denseCols int) spmm.ErrorReport {
	agg := spmm.ErrorReport{
		ReferenceMethod: "aggregate_of_explicit_local_block_references",
		PrecisionBits:   256, RowStart: rowStart, RowEnd: rowEnd, ColStart: 0, ColEnd: denseCols,
	}
	for _, b := range blocks {
		if b.Summary.RowEnd <= rowStart || b.Summary.RowStart >= rowEnd {
			continue
		}
		agg.AbsTolerance = b.Error.AbsTolerance
		agg.RelTolerance = b.Error.RelTolerance
		if b.Error.MaxAbsDeviation > agg.MaxAbsDeviation {
			agg.MaxAbsDeviation = b.Error.MaxAbsDeviation
		}
		if b.Error.MaxRelativeDeviation > agg.MaxRelativeDeviation {
			agg.MaxRelativeDeviation = b.Error.MaxRelativeDeviation
		}
		if b.Error.ToleranceExceeded {
			agg.ToleranceExceeded = true
		}
		for _, sample := range b.Error.Samples {
			if sample.Row >= rowStart && sample.Row < rowEnd {
				agg.Samples = append(agg.Samples, sample)
			}
		}
	}
	sort.SliceStable(agg.Samples, func(i, j int) bool {
		if agg.Samples[i].Row != agg.Samples[j].Row {
			return agg.Samples[i].Row < agg.Samples[j].Row
		}
		return agg.Samples[i].Col < agg.Samples[j].Col
	})
	agg.SampleCount = len(agg.Samples)
	agg.TotalOutputElements = maxInt(0, rowEnd-rowStart) * denseCols
	if agg.TotalOutputElements > 0 {
		agg.SampleCoverage = float64(agg.SampleCount) / float64(agg.TotalOutputElements)
	}
	agg.UpperBoundAbs = agg.MaxAbsDeviation
	agg.UpperBoundRel = agg.MaxRelativeDeviation
	return agg
}

func canonicalDigest(r *Result) string {
	if r == nil {
		return ""
	}
	type block struct {
		Summary spmm.BlockSummary `json:"summary"`
		Error   string            `json:"error"`
	}
	b := make([]block, len(r.Blocks))
	for i := range r.Blocks {
		b[i] = block{Summary: r.Blocks[i].Summary, Error: spmm.HashJSON(r.Blocks[i].Error)}
	}
	bits := make([]uint64, len(r.Values))
	for i, v := range r.Values {
		bits[i] = math.Float64bits(v)
	}
	type payload struct {
		PlanKey, PlanDigest         string
		PlanVersion                 int64
		RowStart, RowEnd, DenseCols int
		Values                      []uint64
		Blocks                      []block
		Aggregate                   string
		Stopped                     bool
		CompletedUnits, TotalUnits  int
	}
	return spmm.HashJSON(payload{
		PlanKey: r.PlanKey, PlanDigest: r.PlanDigest, PlanVersion: r.PlanVersion,
		RowStart: r.RowStart, RowEnd: r.RowEnd, DenseCols: r.DenseCols,
		Values: bits, Blocks: b, Aggregate: spmm.HashJSON(r.AggregateError),
		Stopped: r.Stopped, CompletedUnits: r.CompletedUnits, TotalUnits: r.TotalUnits,
	})
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (e *Engine) String() string {
	return fmt.Sprintf("engine(plan=%s version=%d blocks=%d)", e.PlanKey, e.Ref.Version, len(e.Plan.Memory.Rows))
}
