package infer

import (
	"fmt"
	"math"

	"qaudit/internal/calib"
	"qaudit/internal/model"
	"qaudit/internal/quant"
)

// InferenceTrace is the per-position quantized execution trace.
type InferenceTrace struct {
	Position     int                  `json:"position"`
	Logits       []float64            `json:"logits"`
	Probs        []float64            `json:"probs"`
	PredLabel    int                  `json:"pred_label"`
	RefLabel     int                  `json:"ref_label"`
	LayerOutputs map[string][]float64 `json:"layer_outputs"`
	AccumBits    map[string]int       `json:"accum_bits"`
	AccumClamps  map[string]int       `json:"accum_clamps"`
	OutputClamps map[string]int       `json:"output_clamps"`
	InputClamps  int                  `json:"input_clamps"`
}

// Pipeline bundles calibrated engines with the dataset and policy.
type Pipeline struct {
	Dataset   *model.Dataset
	CalibSet  *calib.CalibSet
	Engines   []*quant.LayerEngine
	Artifacts []*model.LayerCalibration
	Tolerance float64
	DataDir   string
}

// CalibrationActivations runs the float64 network on the calibration set and
// collects the input activations seen by every layer.
func CalibrationActivations(net model.Network, cs *calib.CalibSet) map[string][][]float64 {
	acts := map[string][][]float64{}
	for _, layer := range net.Layers {
		acts[layer.Name] = make([][]float64, 0, len(cs.Entries))
	}
	for _, e := range cs.Entries {
		x := append([]float64(nil), e.Feature...)
		for _, layer := range net.Layers {
			acts[layer.Name] = append(acts[layer.Name], append([]float64(nil), x...))
			y := make([]float64, layer.OutDim)
			for o := 0; o < layer.OutDim; o++ {
				s := layer.Bias[o]
				row := layer.Weights[o]
				for i, v := range x {
					s += row[i] * v
				}
				if layer.Relu && s < 0 {
					s = 0
				}
				y[o] = s
			}
			x = y
		}
	}
	return acts
}

// CalibrationOutputs collects the float64 output activation of every layer over
// the calibration mini-batch, including ReLU application.
func CalibrationOutputs(net model.Network, cs *calib.CalibSet) map[string][][]float64 {
	out := map[string][][]float64{}
	for _, layer := range net.Layers {
		out[layer.Name] = make([][]float64, 0, len(cs.Entries))
	}
	for _, e := range cs.Entries {
		x := append([]float64(nil), e.Feature...)
		for _, layer := range net.Layers {
			y := make([]float64, layer.OutDim)
			for o := 0; o < layer.OutDim; o++ {
				s := layer.Bias[o]
				row := layer.Weights[o]
				for i, v := range x {
					s += row[i] * v
				}
				if layer.Relu && s < 0 {
					s = 0
				}
				y[o] = s
			}
			out[layer.Name] = append(out[layer.Name], y)
			x = y
		}
	}
	return out
}

// CalibrateAll runs stage 2 across every layer in order.
func CalibrateAll(ds *model.Dataset, cs *calib.CalibSet, inputFP, policyFP string) ([]*model.LayerCalibration, []*quant.LayerEngine, error) {
	net := ds.Network
	acts := CalibrationActivations(net, cs)
	outputs := CalibrationOutputs(net, cs)
	artifacts := make([]*model.LayerCalibration, 0, len(net.Layers))
	engines := make([]*quant.LayerEngine, 0, len(net.Layers))
	for _, layer := range net.Layers {
		pol := ds.Policy.LayerPolicy(layer.Name)
		artifact, engine, err := quant.CalibrateLayer(quant.CalibrationInput{
			Layer:        layer,
			Policy:       pol,
			CalibInputs:  acts[layer.Name],
			CalibOutputs: outputs[layer.Name],
			InputFP:      inputFP,
			PolicyFP:     policyFP,
		})
		if err != nil {
			return nil, nil, err
		}
		artifacts = append(artifacts, artifact)
		engines = append(engines, engine)
	}
	return artifacts, engines, nil
}

// QuantizedForward runs one sample through the integer pipeline.
func (p *Pipeline) QuantizedForward(position int, input []float64) InferenceTrace {
	trace := InferenceTrace{
		Position:     position,
		LayerOutputs: map[string][]float64{},
		AccumBits:    map[string]int{},
		AccumClamps:  map[string]int{},
		OutputClamps: map[string]int{},
	}
	inputQ, inputClamps := p.Engines[0].QuantizeInput(input)
	trace.InputClamps = inputClamps
	cur := inputQ
	for _, engine := range p.Engines {
		engine.ResetCounters()
		res := engine.Forward(cur)
		trace.LayerOutputs[engine.Layer.Name] = res.DequantOutput
		trace.AccumBits[engine.Layer.Name] = res.AccumBits
		trace.AccumClamps[engine.Layer.Name] = res.AccumClamps
		trace.OutputClamps[engine.Layer.Name] = res.OutputClamps
		cur = res.OutputQ
	}
	logits := trace.LayerOutputs[p.Engines[len(p.Engines)-1].Layer.Name]
	trace.Logits = logits
	trace.Probs = Softmax(logits)
	trace.PredLabel = Argmax(logits)
	return trace
}

// NumericReport is the stage-3 comparison result.
type NumericReport struct {
	Traces      []InferenceTrace `json:"traces"`
	Deviations  []LayerDeviation `json:"deviations"`
	RelErrors   map[int]float64  `json:"rel_errors"`
	Tolerance   float64          `json:"tolerance"`
	MaxRelError float64          `json:"max_rel_error"`
}

// Compare runs quantized inference at every declared sample position and
// compares layer outputs and end-to-end logits against reference truth.
func (p *Pipeline) Compare() (*NumericReport, error) {
	report := &NumericReport{
		Tolerance: p.Tolerance,
		RelErrors: map[int]float64{},
	}
	refByPos := map[int]model.ReferenceOutput{}
	for _, r := range p.Dataset.References {
		refByPos[r.Position] = r
	}
	for _, pos := range p.Dataset.Policy.SamplePositions {
		ref, ok := refByPos[pos]
		if !ok {
			return nil, model.NewBizError(model.CodeAuditInput, "missing reference output",
				map[string]string{"position": fmt.Sprintf("%d", pos)})
		}
		shard := p.Dataset.Shards[pos]
		_ = shard
		feat := p.featureAt(pos)
		trace := p.QuantizedForward(pos, feat)
		trace.RefLabel = ref.Label
		report.Traces = append(report.Traces, trace)
		rel := RelativeError(trace.Logits, ref.Logits)
		report.RelErrors[pos] = rel
		if rel > report.MaxRelError {
			report.MaxRelError = rel
		}
		for _, engine := range p.Engines {
			name := engine.Layer.Name
			refOut, ok := ref.LayerOutputs[name]
			if !ok {
				return nil, model.NewBizError(model.CodeAuditInput, "missing reference layer output",
					map[string]string{"layer": name, "position": fmt.Sprintf("%d", pos)})
			}
			qOut := trace.LayerOutputs[name]
			dev := LayerDeviation{Layer: name, Position: pos, QuantOutput: qOut, RefOutput: refOut}
			var maxAbs, refNorm float64
			for i := range refOut {
				d := math.Abs(qOut[i] - refOut[i])
				if d > maxAbs {
					maxAbs = d
				}
				refNorm += refOut[i] * refOut[i]
			}
			var diffNorm float64
			for i := range refOut {
				diffNorm += (qOut[i] - refOut[i]) * (qOut[i] - refOut[i])
			}
			dev.MaxAbsDev = maxAbs
			if refNorm > 0 {
				dev.RelDev = math.Sqrt(diffNorm / refNorm)
			} else {
				dev.RelDev = math.Sqrt(diffNorm)
			}
			report.Deviations = append(report.Deviations, dev)
		}
	}
	var worst *LayerDeviation
	for i := range report.Deviations {
		d := &report.Deviations[i]
		if d.MaxAbsDev > p.Tolerance {
			if worst == nil || d.MaxAbsDev > worst.MaxAbsDev {
				worst = d
			}
		}
	}
	if worst != nil {
		return report, model.NewBizError(model.CodeNumericMismatch, "layer deviation exceeds tolerance",
			map[string]string{
				"layer":     worst.Layer,
				"position":  fmt.Sprintf("%d", worst.Position),
				"max_dev":   fmt.Sprintf("%g", worst.MaxAbsDev),
				"abs_dev":   fmt.Sprintf("%g", worst.MaxAbsDev),
				"tolerance": fmt.Sprintf("%g", p.Tolerance),
			})
	}
	return report, nil
}

func (p *Pipeline) featureAt(pos int) []float64 {
	for _, e := range p.CalibSet.Entries {
		if e.Index == pos {
			return e.Feature
		}
	}
	// Fallback for positions outside the calibration set: rebuild from shard PNG.
	feat, err := loadShardFeature(p.Dataset, p.DataDir, pos)
	if err != nil {
		panic(err)
	}
	return feat
}

// FeatureAt exposes the normalized feature for a shard position to the auditor.
func (p *Pipeline) FeatureAt(pos int) []float64 { return p.featureAt(pos) }

func (p *Pipeline) featureAtPublic(pos int) []float64 { return p.featureAt(pos) }
