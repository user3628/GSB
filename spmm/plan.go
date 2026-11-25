package spmm

import (
	"fmt"
	"math"
	"sort"
)

type SumStrategy string

const (
	SumSequential  SumStrategy = "sequential"
	SumCompensated SumStrategy = "compensated"
)

type AccumulationOrder string

const (
	OrderColumnAscending AccumulationOrder = "column_ascending"
	OrderBlockOrder      AccumulationOrder = "block_order"
)

const (
	DefaultCacheBudgetBytes = 256 << 10
	DefaultPeakMemoryBytes  = 16 << 20
	DefaultErrorSeed        = uint64(0xc0ffee5eed)
	DefaultErrorSamples     = 12
	DefaultAbsTolerance     = 1e-9
	DefaultRelTolerance     = 1e-9
)

type PlanRequest struct {
	Matrix              *CSRMatrix
	Dense               *DenseMatrix
	CacheBudgetBytes    int64
	PeakMemoryBytes     int64
	SumStrategy         SumStrategy
	AccumulationOrder   AccumulationOrder
	SkipExplicitZeros   bool
	RowBlockSize        int
	ColBlockSize        int
	ErrorSeed           uint64
	ErrorSampleCount    int
	AbsTolerance        float64
	RelTolerance        float64
	InteractiveBudgetMS float64
}

type ScoreBreakdown struct {
	TrafficScore       float64 `json:"traffic_score"`
	LoadBalanceScore   float64 `json:"load_balance_score"`
	LatencyScore       float64 `json:"latency_score"`
	OccupancyScore     float64 `json:"occupancy_score"`
	EmptyBlockPenalty  float64 `json:"empty_block_penalty"`
	OversizePenalty    float64 `json:"oversize_penalty"`
	CacheBudgetPenalty float64 `json:"cache_budget_penalty"`
	TotalScore         float64 `json:"total_score"`
}

type CandidateScore struct {
	RowBlockSize        int            `json:"row_block_size"`
	ColBlockSize        int            `json:"col_block_size"`
	PredictedTraffic    int64          `json:"predicted_traffic_bytes"`
	LoadImbalance       float64        `json:"load_imbalance"`
	EmptyBlockRatio     float64        `json:"empty_block_ratio"`
	AverageRowNNZ       float64        `json:"average_row_nnz"`
	GlobalEmptyRowRatio float64        `json:"global_empty_row_ratio"`
	EstimatedLatencyUS  float64        `json:"estimated_latency_us"`
	WorkingSetBytes     int64          `json:"working_set_bytes"`
	CacheBudgetExceeded bool           `json:"cache_budget_exceeded"`
	Score               ScoreBreakdown `json:"score"`
	Selected            bool           `json:"selected"`
	RejectionReason     string         `json:"rejection_reason,omitempty"`
}

type BlockDescriptor struct {
	Index int  `json:"index"`
	Start int  `json:"start"`
	End   int  `json:"end"`
	NNZ   int  `json:"nnz"`
	Empty bool `json:"empty"`
}

type SelectionReport struct {
	Mode             string           `json:"mode"`
	Reason           string           `json:"reason"`
	Candidates       []CandidateScore `json:"candidates"`
	Rejected         []CandidateScore `json:"rejected"`
	RowBlockSize     int              `json:"row_block_size"`
	ColBlockSize     int              `json:"col_block_size"`
	PredictedTraffic int64            `json:"predicted_traffic_bytes"`
	LoadImbalance    float64          `json:"load_imbalance"`
	EstimatedLatency float64          `json:"estimated_latency_us"`
}

type Plan struct {
	Rows                 int               `json:"rows"`
	Cols                 int               `json:"cols"`
	DenseRows            int               `json:"dense_rows"`
	DenseCols            int               `json:"dense_cols"`
	ShapeFingerprint     string            `json:"shape_fingerprint"`
	StructureFingerprint string            `json:"structure_fingerprint"`
	DenseFingerprint     string            `json:"dense_fingerprint"`
	Stats                MatrixStats       `json:"stats"`
	RowBlockSize         int               `json:"row_block_size"`
	ColBlockSize         int               `json:"col_block_size"`
	RowBlocks            []BlockDescriptor `json:"row_blocks"`
	ColBlocks            []BlockDescriptor `json:"col_blocks"`
	SumStrategy          SumStrategy       `json:"sum_strategy"`
	AccumulationOrder    AccumulationOrder `json:"accumulation_order"`
	SkipExplicitZeros    bool              `json:"skip_explicit_zeros"`
	CacheBudgetBytes     int64             `json:"cache_budget_bytes"`
	PeakMemoryBytes      int64             `json:"peak_memory_bytes"`
	ErrorSeed            uint64            `json:"error_seed"`
	ErrorSampleCount     int               `json:"error_sample_count"`
	AbsTolerance         float64           `json:"abs_tolerance"`
	RelTolerance         float64           `json:"rel_tolerance"`
	InteractiveBudgetMS  float64           `json:"interactive_budget_ms"`
	Selection            SelectionReport   `json:"selection"`
	Memory               MemoryPlan        `json:"memory_plan"`
}

type planIdentity struct {
	ShapeFingerprint     string            `json:"shape_fingerprint"`
	StructureFingerprint string            `json:"structure_fingerprint"`
	DenseFingerprint     string            `json:"dense_fingerprint"`
	SumStrategy          SumStrategy       `json:"sum_strategy"`
	AccumulationOrder    AccumulationOrder `json:"accumulation_order"`
	SkipExplicitZeros    bool              `json:"skip_explicit_zeros"`
	CacheBudgetBytes     int64             `json:"cache_budget_bytes"`
	PeakMemoryBytes      int64             `json:"peak_memory_bytes"`
	ErrorSeed            uint64            `json:"error_seed"`
	ErrorSampleCount     int               `json:"error_sample_count"`
	AbsTolerance         float64           `json:"abs_tolerance"`
	RelTolerance         float64           `json:"rel_tolerance"`
	InteractiveBudgetMS  float64           `json:"interactive_budget_ms"`
}

func (p *Plan) Identity() planIdentity {
	return planIdentity{
		ShapeFingerprint: p.ShapeFingerprint, StructureFingerprint: p.StructureFingerprint,
		DenseFingerprint: p.DenseFingerprint, SumStrategy: p.SumStrategy,
		AccumulationOrder: p.AccumulationOrder, SkipExplicitZeros: p.SkipExplicitZeros,
		CacheBudgetBytes: p.CacheBudgetBytes, PeakMemoryBytes: p.PeakMemoryBytes,
		ErrorSeed: p.ErrorSeed, ErrorSampleCount: p.ErrorSampleCount,
		AbsTolerance: p.AbsTolerance, RelTolerance: p.RelTolerance,
		InteractiveBudgetMS: p.InteractiveBudgetMS,
	}
}

// PlanKey identifies one logical version chain. Row/column block sizes are
// intentionally excluded: changing them publishes a new version in the same
// chain, protected by a monotonic sequence number.
func PlanKey(p *Plan) string { return HashJSON(p.Identity()) }

// PlanPayloadHash covers the complete selected plan, including its rationale.
func PlanPayloadHash(p *Plan) string { return HashJSON(p) }

func ChoosePlan(req PlanRequest) (*Plan, error) {
	if req.Matrix == nil || req.Dense == nil {
		return nil, NewError(CodeInput, "matrix and dense right-hand side are required")
	}
	if err := req.Matrix.Validate(); err != nil {
		return nil, err
	}
	if err := req.Dense.Validate(); err != nil {
		return nil, err
	}
	if req.Dense.Rows != req.Matrix.Cols {
		return nil, NewError(CodeInput, "dense rows %d must equal sparse cols %d", req.Dense.Rows, req.Matrix.Cols)
	}
	if req.CacheBudgetBytes <= 0 {
		req.CacheBudgetBytes = DefaultCacheBudgetBytes
	}
	if req.PeakMemoryBytes <= 0 {
		req.PeakMemoryBytes = DefaultPeakMemoryBytes
	}
	if req.SumStrategy == "" {
		req.SumStrategy = SumSequential
	}
	if req.SumStrategy != SumSequential && req.SumStrategy != SumCompensated {
		return nil, NewError(CodeInput, "unsupported sum strategy %q", req.SumStrategy)
	}
	if req.AccumulationOrder == "" {
		req.AccumulationOrder = OrderBlockOrder
	}
	if req.AccumulationOrder != OrderColumnAscending && req.AccumulationOrder != OrderBlockOrder {
		return nil, NewError(CodeInput, "unsupported accumulation order %q", req.AccumulationOrder)
	}
	if req.ErrorSeed == 0 {
		req.ErrorSeed = DefaultErrorSeed
	}
	if req.ErrorSampleCount <= 0 {
		req.ErrorSampleCount = DefaultErrorSamples
	}
	if req.AbsTolerance <= 0 {
		req.AbsTolerance = DefaultAbsTolerance
	}
	if req.RelTolerance <= 0 {
		req.RelTolerance = DefaultRelTolerance
	}
	if req.InteractiveBudgetMS <= 0 {
		req.InteractiveBudgetMS = 50
	}

	stats := req.Matrix.Stats(req.Dense)
	rowSizes, colSizes := candidateSizes(req, stats)
	if req.RowBlockSize > 0 {
		if req.RowBlockSize > req.Matrix.Rows {
			return nil, NewError(CodeInput, "row block size %d exceeds row count %d", req.RowBlockSize, req.Matrix.Rows)
		}
		rowSizes = []int{req.RowBlockSize}
	}
	if req.ColBlockSize > 0 {
		if req.ColBlockSize > req.Matrix.Cols {
			return nil, NewError(CodeInput, "column block size %d exceeds column count %d", req.ColBlockSize, req.Matrix.Cols)
		}
		colSizes = []int{req.ColBlockSize}
	}

	candidates := make([]CandidateScore, 0, len(rowSizes)*len(colSizes))
	for _, rb := range rowSizes {
		for _, cb := range colSizes {
			candidates = append(candidates, scoreCandidate(req, stats, rb, cb))
		}
	}
	normalizeCandidateScores(candidates)
	selected := -1
	for i := range candidates {
		if candidates[i].CacheBudgetExceeded && req.RowBlockSize == 0 && req.ColBlockSize == 0 {
			continue
		}
		if selected == -1 || betterCandidate(candidates[i], candidates[selected]) {
			selected = i
		}
	}
	if selected == -1 {
		// All candidates exceed the cache working-set budget. The smallest
		// working set is still explicitly selected and tagged, while the peak
		// memory scheduler remains the hard safety boundary.
		for i := range candidates {
			if selected == -1 || candidates[i].WorkingSetBytes < candidates[selected].WorkingSetBytes ||
				(candidates[i].WorkingSetBytes == candidates[selected].WorkingSetBytes && betterCandidate(candidates[i], candidates[selected])) {
				selected = i
			}
		}
		if selected == -1 {
			return nil, NewError(CodeMemoryBudget, "no viable block candidate")
		}
	}
	candidates[selected].Selected = true
	rejected := make([]CandidateScore, 0, len(candidates)-1)
	for i := range candidates {
		if i == selected {
			continue
		}
		if candidates[i].RejectionReason == "" {
			candidates[i].RejectionReason = "lower_weighted_score"
		}
		rejected = append(rejected, candidates[i])
	}
	chosen := candidates[selected]
	mode := "automatic"
	reason := fmt.Sprintf("selected %dx%d row/column blocks: traffic=%d bytes, load imbalance=%.4f, predicted block latency=%.2f us",
		chosen.RowBlockSize, chosen.ColBlockSize, chosen.PredictedTraffic, chosen.LoadImbalance, chosen.EstimatedLatencyUS)
	if req.RowBlockSize > 0 || req.ColBlockSize > 0 {
		mode = "manual"
		reason = fmt.Sprintf("manual block override selected %dx%d rows/columns", chosen.RowBlockSize, chosen.ColBlockSize)
	}

	p := &Plan{
		Rows: req.Matrix.Rows, Cols: req.Matrix.Cols,
		DenseRows: req.Dense.Rows, DenseCols: req.Dense.Cols,
		ShapeFingerprint: stats.ShapeFingerprint, StructureFingerprint: stats.StructureFingerprint,
		DenseFingerprint: stats.DenseFingerprint, Stats: stats,
		RowBlockSize: chosen.RowBlockSize, ColBlockSize: chosen.ColBlockSize,
		RowBlocks:   makeRowBlocks(req.Matrix, chosen.RowBlockSize),
		ColBlocks:   makeColBlocks(req.Matrix.Cols, chosen.ColBlockSize),
		SumStrategy: req.SumStrategy, AccumulationOrder: req.AccumulationOrder,
		SkipExplicitZeros: req.SkipExplicitZeros, CacheBudgetBytes: req.CacheBudgetBytes,
		PeakMemoryBytes: req.PeakMemoryBytes, ErrorSeed: req.ErrorSeed,
		ErrorSampleCount: req.ErrorSampleCount, AbsTolerance: req.AbsTolerance,
		RelTolerance: req.RelTolerance, InteractiveBudgetMS: req.InteractiveBudgetMS,
		Selection: SelectionReport{
			Mode: mode, Reason: reason, Candidates: candidates, Rejected: rejected,
			RowBlockSize: chosen.RowBlockSize, ColBlockSize: chosen.ColBlockSize,
			PredictedTraffic: chosen.PredictedTraffic, LoadImbalance: chosen.LoadImbalance,
			EstimatedLatency: chosen.EstimatedLatencyUS,
		},
	}
	mem, err := BuildMemoryPlan(p)
	if err != nil {
		return nil, err
	}
	p.Memory = mem
	return p, nil
}

func candidateSizes(req PlanRequest, stats MatrixStats) ([]int, []int) {
	rowSizes := blockSizes(req.Matrix.Rows, []int{4, 6, 8, 12, 16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512})
	colSizes := blockSizes(req.Matrix.Cols, []int{4, 6, 8, 12, 16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512})
	// Guard against selecting a clearly oversized block on a small matrix. A
	// block candidate must retain at least two row blocks unless the matrix is
	// genuinely tiny; this keeps empty-block ratio and occupancy observable.
	if req.Matrix.Rows >= 16 {
		maxRows := maxInt(8, req.Matrix.Rows/2)
		filtered := rowSizes[:0]
		for _, n := range rowSizes {
			if n <= maxRows {
				filtered = append(filtered, n)
			}
		}
		if len(filtered) > 0 {
			rowSizes = filtered
		}
	}
	if len(rowSizes) == 0 {
		rowSizes = []int{maxInt(1, req.Matrix.Rows)}
	}
	if len(colSizes) == 0 {
		colSizes = []int{maxInt(1, req.Matrix.Cols)}
	}
	_ = stats
	return rowSizes, colSizes
}

func blockSizes(total int, preferred []int) []int {
	if total <= 0 {
		return []int{1}
	}
	seen := make(map[int]bool)
	out := make([]int, 0, len(preferred)+4)
	add := func(n int) {
		if n < 1 {
			return
		}
		if n > total {
			n = total
		}
		if !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	for _, n := range preferred {
		if n <= total {
			add(n)
		}
	}
	add(maxInt(1, total/4))
	add(maxInt(1, total/3))
	add(maxInt(1, total/2))
	add(total)
	sort.Ints(out)
	return out
}

func scoreCandidate(req PlanRequest, stats MatrixStats, rb, cb int) CandidateScore {
	rowBlocks := makeRowBlocks(req.Matrix, rb)
	var nnzCounts []float64
	empty := 0
	for _, b := range rowBlocks {
		nnzCounts = append(nnzCounts, float64(b.NNZ))
		if b.Empty {
			empty++
		}
	}
	mean, cv := loadStats(nnzCounts)
	emptyRatio := 0.0
	if len(rowBlocks) > 0 {
		emptyRatio = float64(empty) / float64(len(rowBlocks))
	}
	colBlockCount := (req.Matrix.Cols + cb - 1) / cb
	traffic := int64(stats.NNZ)*int64(stats.DenseCols)*8 +
		int64(stats.Rows)*int64(stats.DenseCols)*8 +
		int64(len(rowBlocks))*int64(colBlockCount)*int64(cb)*int64(stats.DenseCols)*8
	// A simple portable latency model: 2 GiB/s effective scalar traffic plus
	// fixed per (row-block, column-block) dispatch overhead.
	latencyUS := float64(traffic)/2e9*1e6 + float64(len(rowBlocks)*colBlockCount)*35.0 +
		float64(stats.Rows)*stats.AverageRowNNZ*0.0002
	working := int64(rb)*int64(stats.DenseCols)*8 + int64(cb)*int64(stats.DenseCols)*8 + int64(rb)*8
	oversize := 0.0
	if stats.Rows >= 24 && rb > maxInt(8, stats.Rows/4) {
		oversize = 18
	}
	if len(rowBlocks) < 2 && stats.Rows >= 24 {
		oversize += 22
	}
	if mean == 0 {
		_ = mean
	}
	return CandidateScore{
		RowBlockSize: rb, ColBlockSize: cb, PredictedTraffic: traffic,
		LoadImbalance: cv, EmptyBlockRatio: emptyRatio, AverageRowNNZ: stats.AverageRowNNZ,
		GlobalEmptyRowRatio: stats.EmptyRowRatio, EstimatedLatencyUS: latencyUS,
		WorkingSetBytes: working, CacheBudgetExceeded: working > req.CacheBudgetBytes,
		Score: ScoreBreakdown{OversizePenalty: oversize},
	}
}

func loadStats(values []float64) (float64, float64) {
	if len(values) == 0 {
		return 0, 0
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(len(values))
	if mean == 0 {
		return 0, 0
	}
	var variance float64
	for _, v := range values {
		d := v - mean
		variance += d * d
	}
	variance /= float64(len(values))
	return mean, math.Sqrt(variance) / mean
}

func normalizeCandidateScores(candidates []CandidateScore) {
	if len(candidates) == 0 {
		return
	}
	minTraffic, maxTraffic := candidates[0].PredictedTraffic, candidates[0].PredictedTraffic
	minLatency, maxLatency := candidates[0].EstimatedLatencyUS, candidates[0].EstimatedLatencyUS
	minCV, maxCV := candidates[0].LoadImbalance, candidates[0].LoadImbalance
	maxBlocks := 1
	for _, c := range candidates {
		if c.PredictedTraffic < minTraffic {
			minTraffic = c.PredictedTraffic
		}
		if c.PredictedTraffic > maxTraffic {
			maxTraffic = c.PredictedTraffic
		}
		if c.EstimatedLatencyUS < minLatency {
			minLatency = c.EstimatedLatencyUS
		}
		if c.EstimatedLatencyUS > maxLatency {
			maxLatency = c.EstimatedLatencyUS
		}
		if c.LoadImbalance < minCV {
			minCV = c.LoadImbalance
		}
		if c.LoadImbalance > maxCV {
			maxCV = c.LoadImbalance
		}
		blocks := (c.RowBlockSize + 1) * (c.ColBlockSize + 1)
		if blocks > maxBlocks {
			maxBlocks = blocks
		}
	}
	for i := range candidates {
		c := &candidates[i]
		c.Score.TrafficScore = inverseRangeScore(c.PredictedTraffic, minTraffic, maxTraffic)
		c.Score.LoadBalanceScore = inverseFloatScore(c.LoadImbalance, minCV, maxCV)
		c.Score.LatencyScore = inverseFloatScore(c.EstimatedLatencyUS, minLatency, maxLatency)
		blocks := (c.RowBlockSize + 1) * (c.ColBlockSize + 1)
		c.Score.OccupancyScore = 100 * float64(blocks) / float64(maxBlocks)
		c.Score.EmptyBlockPenalty = 45*c.EmptyBlockRatio + 10*c.GlobalEmptyRowRatio
		if c.CacheBudgetExceeded {
			c.Score.CacheBudgetPenalty = 35
		}
		c.Score.TotalScore = 0.38*c.Score.TrafficScore + 0.27*c.Score.LoadBalanceScore +
			0.25*c.Score.LatencyScore + 0.10*c.Score.OccupancyScore -
			c.Score.EmptyBlockPenalty - c.Score.OversizePenalty - c.Score.CacheBudgetPenalty
		if c.CacheBudgetExceeded && c.RejectionReason == "" {
			c.RejectionReason = "working_set_exceeds_cache_budget"
		}
	}
}

func inverseRangeScore(v, min, max int64) float64 {
	if max == min {
		return 100
	}
	return 100 * float64(max-v) / float64(max-min)
}

func inverseFloatScore(v, min, max float64) float64 {
	if math.Abs(max-min) < 1e-15 {
		return 100
	}
	return 100 * (max - v) / (max - min)
}

func betterCandidate(a, b CandidateScore) bool {
	if math.Abs(a.Score.TotalScore-b.Score.TotalScore) > 1e-9 {
		return a.Score.TotalScore > b.Score.TotalScore
	}
	if a.PredictedTraffic != b.PredictedTraffic {
		return a.PredictedTraffic < b.PredictedTraffic
	}
	if a.EstimatedLatencyUS != b.EstimatedLatencyUS {
		return a.EstimatedLatencyUS < b.EstimatedLatencyUS
	}
	if a.RowBlockSize != b.RowBlockSize {
		return a.RowBlockSize < b.RowBlockSize
	}
	return a.ColBlockSize < b.ColBlockSize
}

func makeRowBlocks(m *CSRMatrix, size int) []BlockDescriptor {
	if size <= 0 {
		size = maxInt(1, m.Rows)
	}
	blocks := make([]BlockDescriptor, 0, (m.Rows+size-1)/size)
	for start, idx := 0, 0; start < m.Rows; start, idx = start+size, idx+1 {
		end := minInt(start+size, m.Rows)
		nnz := int(m.Indptr[end] - m.Indptr[start])
		blocks = append(blocks, BlockDescriptor{Index: idx, Start: start, End: end, NNZ: nnz, Empty: nnz == 0})
	}
	return blocks
}

func makeColBlocks(cols, size int) []BlockDescriptor {
	if size <= 0 {
		size = maxInt(1, cols)
	}
	blocks := make([]BlockDescriptor, 0, (cols+size-1)/size)
	for start, idx := 0, 0; start < cols; start, idx = start+size, idx+1 {
		end := minInt(start+size, cols)
		blocks = append(blocks, BlockDescriptor{Index: idx, Start: start, End: end})
	}
	return blocks
}

func (p *Plan) Validate() error {
	if p == nil {
		return NewError(CodeState, "nil plan")
	}
	if p.Rows < 0 || p.Cols < 0 || p.DenseRows != p.Cols || p.DenseCols < 0 {
		return NewError(CodeState, "plan has inconsistent matrix dimensions")
	}
	if p.RowBlockSize <= 0 || p.ColBlockSize <= 0 || p.DenseCols == 0 {
		return NewError(CodeState, "plan has invalid block dimensions")
	}
	if p.SumStrategy != SumSequential && p.SumStrategy != SumCompensated {
		return NewError(CodeState, "plan has invalid sum strategy")
	}
	if p.AccumulationOrder != OrderColumnAscending && p.AccumulationOrder != OrderBlockOrder {
		return NewError(CodeState, "plan has invalid accumulation order")
	}
	if p.Memory.PeakLimitBytes > 0 && p.Memory.MaxEstimateBytes > p.Memory.PeakLimitBytes {
		return NewError(CodeMemoryBudget, "plan exceeds configured peak memory").withMemory(&MemoryBreakdown{
			PeakLimitBytes: p.Memory.PeakLimitBytes, RequiredBytes: p.Memory.MaxEstimateBytes,
		})
	}
	return nil
}

func (e *Error) withMemory(m *MemoryBreakdown) *Error {
	e.Memory = m
	return e
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
