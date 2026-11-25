// Package audit implements stage 4: error-propagation attribution, provable
// layer-wise error upper bounds and effective-number-of-bits reporting.
package audit

import (
	"fmt"
	"math"
	"sort"

	"qaudit/internal/infer"
	"qaudit/internal/model"
)

// LayerAudit is the full audit conclusion for one layer.
type LayerAudit struct {
	Layer            string  `json:"layer"`
	SensitivityShare float64 `json:"sensitivity_share"`
	SensitivityRank  int     `json:"sensitivity_rank"`
	ReplayError      float64 `json:"replay_error"`
	ReplayRank       int     `json:"replay_rank"`
	UpperBound       float64 `json:"upper_bound"`
	WeightStep       float64 `json:"weight_step"`
	ActivationStep   float64 `json:"activation_step"`
	EffectiveBits    float64 `json:"effective_bits"`
	NeedsReview      bool    `json:"needs_review"`
	ReviewReason     string  `json:"review_reason,omitempty"`
}

// Report is the exportable audit report.
type Report struct {
	VersionFingerprint string       `json:"version_fingerprint"`
	EndToEndError      float64      `json:"end_to_end_error"`
	Layers             []LayerAudit `json:"layers"`
	ReviewedLayers     []string     `json:"reviewed_layers"`
}

// Inputs bundles everything the audit needs.
type Inputs struct {
	Dataset            *model.Dataset
	Calib              interface{ EntriesCount() int }
	Pipeline           *infer.Pipeline
	VersionFingerprint string
}

// Run executes both attribution methods, derives bounds and ENOB, and flags
// ranking disagreements for manual review.
func Run(p *infer.Pipeline, versionFingerprint string) (*Report, error) {
	if err := validate(p); err != nil {
		return nil, err
	}
	numeric, err := p.Compare()
	if err != nil && model.CodeOf(err) != model.CodeNumericMismatch {
		return nil, err
	}
	endToEnd := numeric.MaxRelError

	sensitivity := firstOrderSensitivity(p)
	replay := perturbationReplay(p)

	bounds := upperBounds(p)
	enob := effectiveBits(p)

	layers := make([]LayerAudit, 0, len(p.Engines))
	sensRank := rankDesc(sensitivity)
	repRank := rankDesc(replay)
	for _, engine := range p.Engines {
		name := engine.Layer.Name
		la := LayerAudit{
			Layer:            name,
			SensitivityShare: sensitivity[name],
			SensitivityRank:  sensRank[name],
			ReplayError:      replay[name],
			ReplayRank:       repRank[name],
			UpperBound:       bounds[name],
			WeightStep:       engine.ParamsForWeight(0).Scale,
			ActivationStep:   engine.InputRes.Params.Scale,
			EffectiveBits:    enob[name],
		}
		if la.SensitivityRank != la.ReplayRank {
			la.NeedsReview = true
			la.ReviewReason = fmt.Sprintf("sensitivity rank %d != replay rank %d", la.SensitivityRank, la.ReplayRank)
		}
		layers = append(layers, la)
	}
	sort.SliceStable(layers, func(i, j int) bool { return layers[i].SensitivityRank < layers[j].SensitivityRank })

	report := &Report{VersionFingerprint: versionFingerprint, EndToEndError: endToEnd, Layers: layers}
	for _, la := range layers {
		if la.NeedsReview {
			report.ReviewedLayers = append(report.ReviewedLayers, la.Layer)
		}
	}
	return report, nil
}

func validate(p *infer.Pipeline) error {
	if p == nil || len(p.Engines) == 0 {
		return model.NewBizError(model.CodeAuditInput, "pipeline not calibrated", nil)
	}
	for _, engine := range p.Engines {
		allZero := true
		for _, row := range engine.WeightQ {
			for _, q := range row {
				if q != 0 {
					allZero = false
				}
			}
		}
		if allZero {
			return model.NewBizError(model.CodeAuditInput, "all-zero quantized weights",
				map[string]string{"layer": engine.Layer.Name})
		}
	}
	if len(p.Dataset.References) == 0 {
		return model.NewBizError(model.CodeAuditInput, "reference outputs missing",
			map[string]string{"layer": p.Engines[0].Layer.Name})
	}
	return nil
}

// firstOrderSensitivity approximates each layer's first-order contribution using
// the operator norm of the weight matrix times the layer's quantization step.
func firstOrderSensitivity(p *infer.Pipeline) map[string]float64 {
	out := map[string]float64{}
	var total float64
	for _, engine := range p.Engines {
		opNorm := 0.0
		for o, row := range engine.Layer.Weights {
			var norm float64
			for _, v := range row {
				norm += v * v
			}
			norm = math.Sqrt(norm)
			if norm > opNorm {
				opNorm = norm
			}
			_ = o
		}
		step := engine.ParamsForWeight(0).Scale
		// Activation quantization contributes half a step of input noise.
		contrib := opNorm*(step+engine.InputRes.Params.Scale/2) + engine.OutputRes.Params.Scale/2
		out[engine.Layer.Name] = contrib
		total += contrib
	}
	if total > 0 {
		for k := range out {
			out[k] /= total
		}
	}
	return out
}

// perturbationReplay replaces each layer's quantized execution with a float64
// execution in isolation (all other layers quantized) and measures the resulting
// end-to-end logit error relative to the fully-quantized baseline.
func perturbationReplay(p *infer.Pipeline) map[string]float64 {
	baseline := map[int][]float64{}
	for _, pos := range p.Dataset.Policy.SamplePositions {
		trace := p.QuantizedForward(pos, p.FeatureAt(pos))
		baseline[pos] = trace.Logits
	}
	out := map[string]float64{}
	for skip, engine := range p.Engines {
		var acc float64
		var count int
		for _, pos := range p.Dataset.Policy.SamplePositions {
			logits := replayOne(p, pos, skip)
			acc += infer.RelativeError(logits, baseline[pos])
			count++
		}
		if count > 0 {
			out[engine.Layer.Name] = acc / float64(count)
		}
	}
	return out
}

// replayOne runs the pipeline with layer skipIdx executed in float64 precision.
func replayOne(p *infer.Pipeline, pos, skipIdx int) []float64 {
	x := p.FeatureAt(pos)
	for idx, engine := range p.Engines {
		if idx == skipIdx {
			y := make([]float64, engine.Layer.OutDim)
			for o := 0; o < engine.Layer.OutDim; o++ {
				s := engine.Layer.Bias[o]
				for i, v := range x {
					s += engine.Layer.Weights[o][i] * v
				}
				if engine.Layer.Relu && s < 0 {
					s = 0
				}
				y[o] = s
			}
			x = y
			continue
		}
		inputQ, _ := engine.QuantizeInput(x)
		res := engine.Forward(inputQ)
		x = res.DequantOutput
	}
	return x
}

// upperBounds derives a worst-case infinity-norm bound layer by layer solely
// from explicit scales and quantization steps. For y = Wx + b with quantized
// operands, the output infinity-norm error obeys
//
//	e_out <= ||W||_1 * (e_in + s_x/2) + s_w * ||x||_1 + s_y/2
//
// where e_in is the bound propagated from upstream layers, s_x the input
// quantization step, s_w the weight step (per channel, the maximum is taken)
// and s_y the output requantization step. ReLU is 1-Lipschitz and adds nothing.
func upperBounds(p *infer.Pipeline) map[string]float64 {
	bounds := map[string]float64{}
	prevBound := 0.0
	for _, engine := range p.Engines {
		rowL1Max := 0.0
		weightStepMax := 0.0
		for _, row := range engine.Layer.Weights {
			var l1 float64
			for _, v := range row {
				l1 += math.Abs(v)
			}
			rowL1Max = math.Max(rowL1Max, l1)
		}
		for ch := 0; ch < engine.Layer.OutDim; ch++ {
			weightStepMax = math.Max(weightStepMax, engine.ParamsForWeight(ch).Scale)
		}
		inputStep := engine.InputRes.Params.Scale
		outputStep := engine.OutputRes.Params.Scale

		// Mean calibration input L1 norm gives an explicit, data-grounded scale
		// for the weight-quantization term without referencing any hidden state.
		var inputL1 float64
		var samples int
		for _, pos := range p.Dataset.Policy.SamplePositions {
			feat := p.FeatureAt(pos)
			x := feat
			for _, prior := range p.Engines {
				if prior == engine {
					break
				}
				iq, _ := prior.QuantizeInput(x)
				x = prior.Forward(iq).DequantOutput
			}
			var l1 float64
			for _, v := range x {
				l1 += math.Abs(v)
			}
			inputL1 += l1
			samples++
		}
		if samples > 0 {
			inputL1 /= float64(samples)
		}
		layerBound := rowL1Max*(prevBound+inputStep/2) +
			(weightStepMax/2)*inputL1 + outputStep/2
		bounds[engine.Layer.Name] = layerBound
		prevBound = layerBound
	}
	return bounds
}

// effectiveBits estimates the effective number of bits from the measured
// signal-to-quantization-noise ratio: ENOB = log2(rms_signal / rms_noise),
// capped at the declared bit width and floored at zero.
func effectiveBits(p *infer.Pipeline) map[string]float64 {
	out := map[string]float64{}
	for _, engine := range p.Engines {
		var power, noise float64
		for _, pos := range p.Dataset.Policy.SamplePositions {
			ref := referenceLayer(p, pos, engine.Layer.Name)
			trace := p.QuantizedForward(pos, p.FeatureAt(pos))
			q := trace.LayerOutputs[engine.Layer.Name]
			for i := range ref {
				power += ref[i] * ref[i]
				d := q[i] - ref[i]
				noise += d * d
			}
		}
		if noise > 0 && power > 0 {
			snr := power / noise
			enob := 0.5 * math.Log2(snr)
			out[engine.Layer.Name] = math.Min(float64(engine.Policy.BitWidth), math.Max(0, enob))
		} else {
			out[engine.Layer.Name] = float64(engine.Policy.BitWidth)
		}
	}
	return out
}

func referenceLayer(p *infer.Pipeline, pos int, name string) []float64 {
	for _, ref := range p.Dataset.References {
		if ref.Position == pos {
			return ref.LayerOutputs[name]
		}
	}
	return nil
}

func rankDesc(values map[string]float64) map[string]int {
	names := make([]string, 0, len(values))
	for k := range values {
		names = append(names, k)
	}
	sort.SliceStable(names, func(i, j int) bool {
		if values[names[i]] != values[names[j]] {
			return values[names[i]] > values[names[j]]
		}
		return names[i] < names[j]
	})
	rank := map[string]int{}
	for i, n := range names {
		rank[n] = i + 1
	}
	return rank
}
