package engine

import "qaudit/internal/model"

// deepCopyDataset duplicates every slice/map so injected faults cannot alias
// the engine's baseline dataset. NaN/Inf float64 values are preserved bit-for-bit.
func deepCopyDataset(src *model.Dataset) *model.Dataset {
	ds := &model.Dataset{
		InputDim:   src.InputDim,
		NumClasses: src.NumClasses,
		Shards:     append([]model.Shard(nil), src.Shards...),
		References: deepCopyRefs(src.References),
		Policy:     deepCopyPolicy(src.Policy),
	}
	ds.Network.Name = src.Network.Name
	ds.Network.Layers = make([]model.Layer, len(src.Network.Layers))
	for i, l := range src.Network.Layers {
		cp := model.Layer{
			Name: l.Name, InDim: l.InDim, OutDim: l.OutDim, Relu: l.Relu,
			Bias: append([]float64(nil), l.Bias...),
		}
		cp.Weights = make([][]float64, len(l.Weights))
		for r, row := range l.Weights {
			cp.Weights[r] = append([]float64(nil), row...)
		}
		ds.Network.Layers[i] = cp
	}
	return ds
}

func deepCopyPolicy(p model.Policy) model.Policy {
	cp := model.Policy{
		Name:            p.Name,
		CalibRatio:      p.CalibRatio,
		Seed:            p.Seed,
		Tolerance:       p.Tolerance,
		SamplePositions: append([]int(nil), p.SamplePositions...),
		BitWidths:       mapCopy(p.BitWidths),
		Symmetric:       mapCopyBool(p.Symmetric),
		PerChannel:      mapCopyBool(p.PerChannel),
	}
	return cp
}

func mapCopy(m map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func mapCopyBool(m map[string]bool) map[string]bool {
	out := make(map[string]bool, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func deepCopyRefs(refs []model.ReferenceOutput) []model.ReferenceOutput {
	out := make([]model.ReferenceOutput, len(refs))
	for i, r := range refs {
		cp := model.ReferenceOutput{
			Position:     r.Position,
			Label:        r.Label,
			Logits:       append([]float64(nil), r.Logits...),
			LayerOutputs: map[string][]float64{},
		}
		for k, v := range r.LayerOutputs {
			cp.LayerOutputs[k] = append([]float64(nil), v...)
		}
		out[i] = cp
	}
	return out
}
