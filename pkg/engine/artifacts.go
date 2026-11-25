package engine

import (
	"fmt"
	"sort"

	"local/quantops/pkg/canon"
	"local/quantops/pkg/dataset"
	"local/quantops/pkg/quant"
	"local/quantops/pkg/store"
)

type NonFiniteEvent struct {
	Tensor string `json:"tensor"`
	Index  int    `json:"index"`
	Kind   string `json:"kind"`
}

type CalibrationArtifact struct {
	ContentFingerprint string           `json:"content_fingerprint"`
	InputParams        []quant.Params   `json:"input_params"`
	WeightParams       [][]quant.Params `json:"weight_params"`
	NonFiniteInputs    []NonFiniteEvent `json:"non_finite_inputs,omitempty"`
}

type ForwardLayerArtifact struct {
	Index            int                `json:"index"`
	Name             string             `json:"name"`
	InputShape       []int              `json:"input_shape"`
	WeightShape      []int              `json:"weight_shape"`
	OutputShape      []int              `json:"output_shape"`
	Accum            []int64            `json:"accum"`
	AScales          []float64          `json:"a_scales"`
	AZero            []int32            `json:"a_zero_points"`
	WScales          []float64          `json:"w_scales"`
	WZero            []int32            `json:"w_zero_points"`
	InputParams      []quant.Params     `json:"input_params"`
	WeightParams     []quant.Params     `json:"weight_params"`
	InputClamps      []quant.ClampEvent `json:"input_clamps,omitempty"`
	WeightClamps     []quant.ClampEvent `json:"weight_clamps,omitempty"`
	InputClampCount  int                `json:"input_clamp_count"`
	WeightClampCount int                `json:"weight_clamp_count"`
}

type ForwardArtifact struct {
	Layers []ForwardLayerArtifact `json:"layers"`
}

type DequantLayerArtifact struct {
	Index  int       `json:"index"`
	Name   string    `json:"name"`
	Shape  []int     `json:"shape"`
	Values []float64 `json:"values"`
}

type DequantArtifact struct {
	Layers []DequantLayerArtifact `json:"layers"`
}

type VerifyArtifact struct {
	Report DiagnosticReport `json:"report"`
}

type shardRuntime struct {
	spec         dataset.Shard
	activation   quant.Tensor
	weights      []quant.Tensor
	nonFinite    []NonFiniteEvent
	contentFP    string
	stageConfigs [4]string
}

func prepareRuntime(ds *dataset.Loaded, sh dataset.Shard) (*shardRuntime, error) {
	rt := &shardRuntime{spec: sh}
	rawAct := ds.Tensor(sh.Activation)
	activation, events := quant.Sanitize(rawAct)
	rt.activation = activation
	appendEvents := func(name string, evs []quant.NonFinite) {
		for _, e := range evs {
			rt.nonFinite = append(rt.nonFinite, NonFiniteEvent{Tensor: name, Index: e.Index, Kind: e.Kind})
		}
	}
	appendEvents(sh.Activation.Name, events)
	for _, ref := range sh.Weights {
		t, evs := quant.Sanitize(ds.Tensor(ref))
		rt.weights = append(rt.weights, t)
		appendEvents(ref.Name, evs)
	}
	sort.Slice(rt.nonFinite, func(i, j int) bool {
		if rt.nonFinite[i].Tensor != rt.nonFinite[j].Tensor {
			return rt.nonFinite[i].Tensor < rt.nonFinite[j].Tensor
		}
		return rt.nonFinite[i].Index < rt.nonFinite[j].Index
	})

	contentFP, err := shardContentFingerprint(rt)
	if err != nil {
		return nil, err
	}
	rt.contentFP = contentFP
	calConfig, err := calibrationConfigVersion(rt)
	if err != nil {
		return nil, err
	}
	fwdConfig, err := forwardConfigVersion(rt, calConfig)
	if err != nil {
		return nil, err
	}
	deqConfig := canon.HashString("dequantization\n" + fwdConfig)
	verifyConfig, err := verifyConfigVersion(rt, deqConfig)
	if err != nil {
		return nil, err
	}
	rt.stageConfigs = [4]string{calConfig, fwdConfig, deqConfig, verifyConfig}
	return rt, nil
}

func shardContentFingerprint(rt *shardRuntime) (string, error) {
	type tensorView struct {
		Name      string    `json:"name"`
		Shape     []int     `json:"shape"`
		Values    []float64 `json:"values"`
		NonFinite []int     `json:"non_finite_positions,omitempty"`
	}
	actNonFinite := make([]int, 0)
	for _, e := range rt.nonFinite {
		if e.Tensor == rt.spec.Activation.Name {
			actNonFinite = append(actNonFinite, e.Index)
		}
	}
	view := struct {
		ShardID    string       `json:"shard_id"`
		Ordinal    int          `json:"ordinal"`
		Activation tensorView   `json:"activation"`
		Weights    []tensorView `json:"weights"`
	}{
		ShardID: rt.spec.ID, Ordinal: rt.spec.Ordinal,
		Activation: tensorView{Name: rt.spec.Activation.Name, Shape: rt.activation.Shape,
			Values: rt.activation.Values, NonFinite: actNonFinite},
	}
	for i, ref := range rt.spec.Weights {
		var nf []int
		for _, e := range rt.nonFinite {
			if e.Tensor == ref.Name {
				nf = append(nf, e.Index)
			}
		}
		view.Weights = append(view.Weights, tensorView{Name: ref.Name, Shape: rt.weights[i].Shape,
			Values: rt.weights[i].Values, NonFinite: nf})
	}
	return canon.Hash(view)
}

func calibrationConfigVersion(rt *shardRuntime) (string, error) {
	view := struct {
		Content       string         `json:"content"`
		InputConfig   quant.Config   `json:"input_config"`
		WeightConfigs []quant.Config `json:"weight_configs"`
	}{
		Content: rt.contentFP, InputConfig: rt.spec.Chain[0].Activation,
	}
	for _, layer := range rt.spec.Chain {
		view.WeightConfigs = append(view.WeightConfigs, layer.Weight)
	}
	return canon.Hash(view)
}

func forwardConfigVersion(rt *shardRuntime, calVersion string) (string, error) {
	type layerView struct {
		Name        string       `json:"name"`
		Activation  quant.Config `json:"activation"`
		Weight      quant.Config `json:"weight"`
		WeightName  string       `json:"weight_name"`
		WeightShape []int        `json:"weight_shape"`
	}
	view := struct {
		Calibration string      `json:"calibration"`
		Layers      []layerView `json:"layers"`
		OutputShape []int       `json:"output_shape"`
	}{Calibration: calVersion, OutputShape: rt.spec.ExpectedOutputShape}
	for i, layer := range rt.spec.Chain {
		view.Layers = append(view.Layers, layerView{
			Name: layer.Name, Activation: layer.Activation, Weight: layer.Weight,
			WeightName: rt.spec.Weights[i].Name, WeightShape: rt.spec.Weights[i].Shape,
		})
	}
	return canon.Hash(view)
}

func verifyConfigVersion(rt *shardRuntime, deqVersion string) (string, error) {
	view := struct {
		Dequantization string                `json:"dequantization"`
		Absolute       float64               `json:"absolute_tolerance"`
		Relative       float64               `json:"relative_tolerance"`
		Samples        []dataset.SamplePoint `json:"sample_points"`
		OutputShape    []int                 `json:"output_shape"`
	}{
		Dequantization: deqVersion, Absolute: rt.spec.Verification.AbsoluteTolerance,
		Relative: rt.spec.Verification.RelativeTolerance, Samples: rt.spec.Verification.SamplePoints,
		OutputShape: rt.spec.ExpectedOutputShape,
	}
	return canon.Hash(view)
}

func expectedStageConfigs(rt *shardRuntime) []store.ExpectedStage {
	out := make([]store.ExpectedStage, 0, 4)
	for i, st := range store.Stages() {
		out = append(out, store.ExpectedStage{Stage: st, StageSeq: i + 1, ConfigVersion: rt.stageConfigs[i]})
	}
	return out
}

func encodeArtifact(v any) ([]byte, error) {
	b, err := canon.JSON(v)
	if err != nil {
		return nil, fmt.Errorf("encode stage artifact: %w", err)
	}
	return b, nil
}
