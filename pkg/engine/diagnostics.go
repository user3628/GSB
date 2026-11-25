package engine

import (
	"encoding/json"
	"math"
	"sort"

	"local/quantops/pkg/dataset"
	"local/quantops/pkg/qerr"
	"local/quantops/pkg/quant"
)

type LayerDiagnostic struct {
	LayerIndex           int                 `json:"layer_index"`
	LayerName            string              `json:"layer_name"`
	SampleCount          int                 `json:"sample_count"`
	MaxAbsError          float64             `json:"max_abs_error"`
	MaxRelativeError     float64             `json:"max_relative_error"`
	RelativeUpperBound   float64             `json:"relative_upper_bound_estimate"`
	ErrorContribution    float64             `json:"error_contribution"`
	CumulativeErrorBound float64             `json:"cumulative_error_bound_estimate"`
	ToleranceMargin      float64             `json:"tolerance_margin"`
	WorstPoint           dataset.SamplePoint `json:"worst_sample"`
	Passed               bool                `json:"passed"`
}

type DiagnosticReport struct {
	Passed              bool              `json:"passed"`
	SampleCount         int               `json:"sample_count"`
	MaxAbsError         float64           `json:"max_abs_error"`
	MaxRelativeError    float64           `json:"max_relative_error"`
	RelativeUpperBound  float64           `json:"relative_upper_bound_estimate"`
	ClosestLayer        string            `json:"closest_to_tolerance_layer"`
	ClosestMargin       float64           `json:"closest_tolerance_margin"`
	Layers              []LayerDiagnostic `json:"layers"`
	NonFiniteInputs     []NonFiniteEvent  `json:"non_finite_inputs,omitempty"`
	SanitizedInputCount int               `json:"sanitized_non_finite_count"`
}

func buildDiagnostics(sh dataset.Shard, input quant.Tensor, weights []quant.Tensor,
	dequant DequantArtifact, nonFinite []NonFiniteEvent) (DiagnosticReport, error) {
	refs, err := quant.ReferenceChain(input, weights)
	if err != nil {
		return DiagnosticReport{}, err
	}
	if len(refs) != len(dequant.Layers) {
		return DiagnosticReport{}, qerr.New(qerr.CodeInvalidInput, sh.ID, "verification", "",
			"reference and dequantized layer counts differ", nil)
	}
	report := DiagnosticReport{
		Passed: true, NonFiniteInputs: append([]NonFiniteEvent(nil), nonFinite...),
		SanitizedInputCount: len(nonFinite),
	}
	byLayer := make(map[int][]dataset.SamplePoint)
	for _, p := range sh.Verification.SamplePoints {
		byLayer[p.Layer] = append(byLayer[p.Layer], p)
	}
	prevBound := 0.0
	maxRelBound := 0.0
	closestMargin := math.Inf(1)
	closestLayer := ""
	for i, ref := range refs {
		points := byLayer[i]
		if len(points) == 0 {
			points = fallbackPoints(ref.OutputShape)
		}
		layer := LayerDiagnostic{
			LayerIndex: i, LayerName: sh.Chain[i].Name, Passed: true,
			ToleranceMargin: sh.Verification.AbsoluteTolerance,
		}
		layerPassed := true
		minAbsRef := math.Inf(1)
		maxL1 := 0.0
		for _, l1 := range ref.WeightL1 {
			if l1 > maxL1 {
				maxL1 = l1
			}
		}
		for _, p := range points {
			if len(ref.OutputShape) != 2 || p.Row < 0 || p.Col < 0 || p.Row >= ref.OutputShape[0] || p.Col >= ref.OutputShape[1] {
				return DiagnosticReport{}, qerr.New(qerr.CodeInvalidInput, sh.ID, "verification", sh.Chain[i].Name,
					"sample point out of bounds", nil)
			}
			if i >= len(dequant.Layers) {
				return DiagnosticReport{}, qerr.New(qerr.CodeInvalidInput, sh.ID, "verification", sh.Chain[i].Name,
					"missing dequantized layer", nil)
			}
			idx := p.Row*ref.OutputShape[1] + p.Col
			if idx < 0 || idx >= len(ref.Output) || idx >= len(dequant.Layers[i].Values) {
				return DiagnosticReport{}, qerr.New(qerr.CodeInvalidInput, sh.ID, "verification", sh.Chain[i].Name,
					"sample index out of bounds", nil)
			}
			want := ref.Output[idx]
			got := dequant.Layers[i].Values[idx]
			absErr := math.Abs(got - want)
			relErr := 0.0
			if math.Abs(want) > 1e-12 {
				relErr = absErr / math.Abs(want)
			}
			layer.SampleCount++
			if absErr > layer.MaxAbsError {
				layer.MaxAbsError = absErr
				layer.WorstPoint = p
			}
			if relErr > layer.MaxRelativeError {
				layer.MaxRelativeError = relErr
			}
			pointPassed := absErr <= sh.Verification.AbsoluteTolerance &&
				(math.Abs(want) <= sh.Verification.AbsoluteTolerance || relErr <= sh.Verification.RelativeTolerance)
			if !pointPassed {
				layerPassed = false
			}
			if math.Abs(want) < minAbsRef {
				minAbsRef = math.Abs(want)
			}
		}
		if math.IsInf(minAbsRef, 1) {
			minAbsRef = 1e-12
		}
		layer.ErrorContribution = finiteFloat(layer.MaxAbsError)
		layer.CumulativeErrorBound = finiteFloat(prevBound*math.Max(0, maxL1) + layer.MaxAbsError)
		layer.RelativeUpperBound = finiteFloat(layer.CumulativeErrorBound / math.Max(minAbsRef, 1e-12))
		if layer.RelativeUpperBound > maxRelBound {
			maxRelBound = layer.RelativeUpperBound
		}
		prevBound = layer.CumulativeErrorBound
		layer.ToleranceMargin = finiteFloat(sh.Verification.AbsoluteTolerance - layer.MaxAbsError)
		layer.Passed = layerPassed
		if !layer.Passed {
			report.Passed = false
		}
		if layer.ToleranceMargin < closestMargin {
			closestMargin = layer.ToleranceMargin
			closestLayer = layer.LayerName
		}
		if layer.MaxAbsError > report.MaxAbsError {
			report.MaxAbsError = layer.MaxAbsError
		}
		if layer.MaxRelativeError > report.MaxRelativeError {
			report.MaxRelativeError = layer.MaxRelativeError
		}
		report.SampleCount += layer.SampleCount
		report.Layers = append(report.Layers, layer)
	}
	report.RelativeUpperBound = finiteFloat(maxRelBound)
	report.ClosestLayer = closestLayer
	report.ClosestMargin = finiteFloat(closestMargin)
	sort.SliceStable(report.Layers, func(i, j int) bool { return report.Layers[i].LayerIndex < report.Layers[j].LayerIndex })
	return report, nil
}

func fallbackPoints(shape []int) []dataset.SamplePoint {
	if len(shape) != 2 || shape[0] <= 0 || shape[1] <= 0 {
		return nil
	}
	candidates := []dataset.SamplePoint{
		{Layer: 0, Row: 0, Col: 0},
		{Layer: 0, Row: shape[0] - 1, Col: shape[1] - 1},
		{Layer: 0, Row: shape[0] / 2, Col: shape[1] / 2},
	}
	seen := map[[2]int]bool{}
	var out []dataset.SamplePoint
	for _, p := range candidates {
		key := [2]int{p.Row, p.Col}
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, p)
	}
	return out
}

// numericMismatchError retains the full report as JSON diagnostics.
func numericMismatchError(shardID string, report DiagnosticReport) *qerr.Error {
	worstLayer := report.ClosestLayer
	var worst float64 = -1
	for _, layer := range report.Layers {
		if layer.MaxAbsError > worst {
			worst = layer.MaxAbsError
			worstLayer = layer.LayerName
		}
	}
	code := qerr.New(qerr.CodeNumericMismatch, shardID, "verification", worstLayer,
		"quantization error exceeds configured tolerance", nil)
	detail := map[string]any{
		"layer":              worstLayer,
		"max_abs_error":      report.MaxAbsError,
		"max_relative_error": report.MaxRelativeError,
		"tolerance_margin":   report.ClosestMargin,
		"report":             report,
	}
	if b, err := json.Marshal(detail); err == nil {
		code.Details = b
	}
	return code
}

func finiteFloat(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return math.MaxFloat64
	}
	return v
}
