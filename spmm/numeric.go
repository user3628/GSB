package spmm

import (
	"math"
	"math/big"
	"math/rand"
	"sort"
)

type ErrorSample struct {
	Row          int     `json:"row"`
	Col          int     `json:"col"`
	Reference    string  `json:"reference"`
	Actual       float64 `json:"actual"`
	AbsDeviation float64 `json:"abs_deviation"`
	RelDeviation float64 `json:"rel_deviation"`
}

type ErrorReport struct {
	ReferenceMethod      string        `json:"reference_method"`
	PrecisionBits        int           `json:"precision_bits"`
	SampleSeed           uint64        `json:"sample_seed"`
	SampleCount          int           `json:"sample_count"`
	TotalOutputElements  int           `json:"total_output_elements"`
	SampleCoverage       float64       `json:"sample_coverage"`
	RowStart             int           `json:"row_start"`
	RowEnd               int           `json:"row_end"`
	ColStart             int           `json:"col_start"`
	ColEnd               int           `json:"col_end"`
	Samples              []ErrorSample `json:"samples"`
	MaxAbsDeviation      float64       `json:"max_abs_deviation"`
	MaxRelativeDeviation float64       `json:"max_relative_deviation"`
	UpperBoundAbs        float64       `json:"upper_bound_abs"`
	UpperBoundRel        float64       `json:"upper_bound_rel"`
	AbsTolerance         float64       `json:"abs_tolerance"`
	RelTolerance         float64       `json:"rel_tolerance"`
	ToleranceExceeded    bool          `json:"tolerance_exceeded"`
}

// EstimateBlockError compares deterministic sampled positions with a local
// 256-bit big.Float reference. The positions and reference values are stored
// explicitly in the report; no missing block information is extrapolated.
func EstimateBlockError(m *CSRMatrix, dense *DenseMatrix, plan *Plan, unit ExecutionRow, out []float64, trace BlockTrace) ErrorReport {
	rows := unit.RowEnd - unit.RowStart
	total := rows * dense.Cols
	idx := []int(nil)
	take := plan.ErrorSampleCount
	if take > total {
		take = total
	}
	// Random positions plus the largest-magnitude actual element make the
	// report deterministic while still targeting the most cancellation-prone
	// output.
	peak := 0
	for i := 1; i < len(out); i++ {
		if math.Abs(out[i]) > math.Abs(out[peak]) {
			peak = i
		}
	}
	if take > 0 {
		idx = append(idx, peak)
	}
	seed := sampleSeed(plan.ErrorSeed, unit.Index, unit.RowStart, unit.RowEnd)
	rng := rand.New(rand.NewSource(int64(seed)))
	seen := map[int]bool{peak: take > 0}
	for len(idx) < take {
		pos := rng.Intn(total)
		if !seen[pos] {
			seen[pos] = true
			idx = append(idx, pos)
		}
	}
	sort.Ints(idx)
	report := ErrorReport{
		ReferenceMethod: "local_big_float_256_sampled_positions",
		PrecisionBits:   256, SampleSeed: seed, SampleCount: len(idx),
		TotalOutputElements: total, RowStart: unit.RowStart, RowEnd: unit.RowEnd,
		ColStart: 0, ColEnd: dense.Cols, Samples: make([]ErrorSample, 0, len(idx)),
		AbsTolerance: plan.AbsTolerance, RelTolerance: plan.RelTolerance,
	}
	if total > 0 {
		report.SampleCoverage = float64(len(idx)) / float64(total)
	}
	rowByLocal := make(map[int]int, rows)
	for i := range trace.Rows {
		rowByLocal[trace.Rows[i].Row-unit.RowStart] = i
	}
	for _, flat := range idx {
		localRow := flat / dense.Cols
		col := flat % dense.Cols
		actual := out[flat]
		ref, refText := highPrecisionReference(m, dense, plan, unit.RowStart+localRow, col, trace.Rows[rowByLocal[localRow]].AccumulationPositions)
		absDev, relDev := referenceDeviation(actual, ref)
		report.Samples = append(report.Samples, ErrorSample{
			Row: unit.RowStart + localRow, Col: col, Reference: refText,
			Actual: actual, AbsDeviation: absDev, RelDeviation: relDev,
		})
		if absDev > report.MaxAbsDeviation {
			report.MaxAbsDeviation = absDev
		}
		if relDev > report.MaxRelativeDeviation {
			report.MaxRelativeDeviation = relDev
		}
		if absDev > plan.AbsTolerance && relDev > plan.RelTolerance {
			report.ToleranceExceeded = true
		}
	}
	report.UpperBoundAbs = report.MaxAbsDeviation
	report.UpperBoundRel = report.MaxRelativeDeviation
	return report
}

func highPrecisionReference(m *CSRMatrix, dense *DenseMatrix, plan *Plan, row, col int, positions []int64) (*big.Float, string) {
	sum := new(big.Float).SetPrec(256).SetInt64(0)
	for _, pos := range positions {
		a := new(big.Float).SetPrec(256).SetFloat64(m.Data[pos])
		b := new(big.Float).SetPrec(256).SetFloat64(dense.Data[int(m.Indices[pos])*dense.Cols+col])
		product := new(big.Float).SetPrec(256).Mul(a, b)
		sum.Add(sum, product)
	}
	return sum, sum.Text('g', 50)
}

func referenceDeviation(actual float64, ref *big.Float) (float64, float64) {
	actualBig := new(big.Float).SetPrec(256).SetFloat64(actual)
	diff := new(big.Float).SetPrec(256).Sub(actualBig, ref)
	abs := new(big.Float).SetPrec(256).Abs(diff)
	absF, _ := abs.Float64()
	refAbs := new(big.Float).SetPrec(256).Abs(ref)
	refF, _ := refAbs.Float64()
	if refF == 0 {
		return absF, absF
	}
	rel := new(big.Float).SetPrec(256).Quo(abs, refAbs)
	relF, _ := rel.Float64()
	return absF, relF
}

func sampleSeed(base uint64, unitIndex, rowStart, rowEnd int) uint64 {
	x := base
	x ^= uint64(unitIndex+1) * 0x9e3779b97f4a7c15
	x ^= uint64(rowStart+1) * 0xbf58476d1ce4e5b9
	x ^= uint64(rowEnd+1) * 0x94d049bb133111eb
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
