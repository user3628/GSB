package quant

import (
	"math"

	"qaudit/internal/model"
)

// CalibrationInput carries everything needed to calibrate one layer.
type CalibrationInput struct {
	Layer        model.Layer
	Policy       model.LayerPolicy
	CalibInputs  [][]float64 // normalized real-valued activations entering the layer
	CalibOutputs [][]float64 // real-valued activations leaving the layer (calibration range)
	InputFP      string
	PolicyFP     string
}

// CalibrateLayer performs stage-2 fixed-point calibration for one layer and
// returns the persisted artifact plus an executable quantized engine.
func CalibrateLayer(in CalibrationInput) (*model.LayerCalibration, *LayerEngine, error) {
	if !SupportedBitWidths[in.Policy.BitWidth] {
		return nil, nil, model.NewBizError(model.CodeQuantParam, "unsupported bit width",
			map[string]string{
				"layer":     in.Layer.Name,
				"bit_width": intToStr(in.Policy.BitWidth),
				"channel":   "-1",
			})
	}
	if len(in.Layer.Weights) == 0 || len(in.Layer.Weights[0]) == 0 {
		return nil, nil, model.NewBizError(model.CodeQuantParam, "empty weight matrix",
			map[string]string{"layer": in.Layer.Name, "channel": "-1"})
	}
	if hasNonFinite(in.Layer.Weights) || hasNonFiniteVec(in.Layer.Bias) {
		return nil, nil, model.NewBizError(model.CodeQuantParam, "non-finite weight or bias",
			map[string]string{"layer": in.Layer.Name, "channel": "-1", "reason": "nan_or_inf"})
	}
	if allZeroMatrix(in.Layer.Weights) {
		return nil, nil, model.NewBizError(model.CodeQuantParam, "all-zero weight matrix",
			map[string]string{"layer": in.Layer.Name, "channel": "-1", "reason": "all_zero_weight"})
	}
	// Explicit per-channel all-zero detection applies in both granularity modes:
	// a dead channel must never be silently calibrated into a usable scale.
	for ch, row := range in.Layer.Weights {
		rowZero := true
		for _, v := range row {
			if v != 0 {
				rowZero = false
				break
			}
		}
		if rowZero {
			return nil, nil, model.NewBizError(model.CodeQuantParam, "all-zero weight channel",
				map[string]string{
					"layer":       in.Layer.Name,
					"channel":     intToStr(ch),
					"reason":      "all_zero_channel",
					"per_channel": boolToStr(in.Policy.PerChannel),
				})
		}
	}

	weightResults, err := CalibrateRows(in.Layer.Weights, in.Policy.BitWidth, in.Policy.Symmetric, in.Policy.PerChannel)
	if err != nil {
		// Ensure layer name is present on every E_QUANT_PARAM diagnostic.
		be := err.(*model.BizError)
		be.Context["layer"] = in.Layer.Name
		return nil, nil, be
	}
	for ch, wr := range weightResults {
		if wr.Params.Scale == 0 {
			return nil, nil, model.NewBizError(model.CodeQuantParam, "zero scale produced",
				map[string]string{"layer": in.Layer.Name, "channel": intToStr(channelOf(weightResults, ch))})
		}
	}

	// Activations are always calibrated per tensor over the calibration mini-batch.
	flat := make([]float64, 0, len(in.CalibInputs)*in.Layer.InDim)
	allZero := true
	for _, row := range in.CalibInputs {
		for _, v := range row {
			flat = append(flat, v)
			if v != 0 {
				allZero = false
			}
		}
	}
	if hasNonFiniteVec(flat) {
		return nil, nil, model.NewBizError(model.CodeQuantParam, "non-finite calibration activation",
			map[string]string{"layer": in.Layer.Name, "channel": "-1", "reason": "non_finite_activation"})
	}
	if allZero {
		return nil, nil, model.NewBizError(model.CodeQuantParam, "all-zero calibration activation",
			map[string]string{"layer": in.Layer.Name, "channel": "-1", "reason": "all_zero_activation"})
	}
	actRes, ok := CalibrateTensor(flat, in.Policy.BitWidth, in.Policy.Symmetric)
	if !ok || actRes.Params.Scale == 0 {
		return nil, nil, model.NewBizError(model.CodeQuantParam, "activation calibration failed",
			map[string]string{"layer": in.Layer.Name, "channel": "-1"})
	}

	// Output activations use the same bit width/mode and cover the calibration
	// extrema of this layer's own outputs. ReLU layers are naturally non-negative.
	outFlat := make([]float64, 0, len(in.CalibOutputs)*in.Layer.OutDim)
	outAllZero := true
	for _, row := range in.CalibOutputs {
		for _, v := range row {
			outFlat = append(outFlat, v)
			if v != 0 {
				outAllZero = false
			}
		}
	}
	if hasNonFiniteVec(outFlat) {
		return nil, nil, model.NewBizError(model.CodeQuantParam, "non-finite output activation",
			map[string]string{"layer": in.Layer.Name, "channel": "-1", "reason": "non_finite_output"})
	}
	if outAllZero {
		return nil, nil, model.NewBizError(model.CodeQuantParam, "all-zero output activation",
			map[string]string{"layer": in.Layer.Name, "channel": "-1", "reason": "all_zero_output"})
	}
	outRes, ok := CalibrateTensor(outFlat, in.Policy.BitWidth, in.Policy.Symmetric)
	if !ok || outRes.Params.Scale == 0 {
		return nil, nil, model.NewBizError(model.CodeQuantParam, "output activation calibration failed",
			map[string]string{"layer": in.Layer.Name, "channel": "-1"})
	}

	// Bias scale is the product of input and weight scales (standard linear bias rule).
	firstWeightScale := weightResults[0].Params.Scale
	biasScale := actRes.Params.Scale * firstWeightScale
	if biasScale <= 0 {
		return nil, nil, model.NewBizError(model.CodeQuantParam, "invalid bias scale",
			map[string]string{"layer": in.Layer.Name, "channel": "-1"})
	}

	engine := buildEngine(in.Layer, in.Policy, weightResults, actRes)
	engine.SetOutputScale(outRes)
	artifact := &model.LayerCalibration{
		LayerName:         in.Layer.Name,
		BitWidth:          in.Policy.BitWidth,
		Symmetric:         in.Policy.Symmetric,
		PerChannel:        in.Policy.PerChannel,
		WeightParams:      ToModelParams(weightResults),
		ActivationParam:   toModel(actRes, -1),
		OutputParam:       toModel(outRes, -1),
		BiasScale:         biasScale,
		InputFingerprint:  in.InputFP,
		PolicyFingerprint: in.PolicyFP,
		AccumBits:         engine.AccumBits,
	}
	dg, err := model.Digest(artifact)
	if err != nil {
		return nil, nil, err
	}
	artifact.ArtifactDigest = dg
	engine.ArtifactDigest = dg
	return artifact, engine, nil
}

func channelOf(results []TensorResult, i int) int {
	if len(results) == 1 {
		return -1
	}
	return i
}

func hasNonFinite(m [][]float64) bool {
	for _, row := range m {
		for _, v := range row {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return true
			}
		}
	}
	return false
}

func hasNonFiniteVec(v []float64) bool {
	for _, x := range v {
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return true
		}
	}
	return false
}

func allZeroMatrix(m [][]float64) bool {
	for _, row := range m {
		for _, v := range row {
			if v != 0 {
				return false
			}
		}
	}
	return true
}

func boolToStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
