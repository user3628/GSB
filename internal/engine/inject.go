package engine

import (
	"math"

	"qaudit/internal/model"
)

// Injection describes an externally injected fault used to verify the numeric
// exception chain. Every fault is detected at its owning stage with a stable
// business code and full diagnostic context.
type Injection struct {
	Kind       string  `json:"kind"` // bad_bitwidth | zero_channel | nan_weight | calib_ratio
	Layer      string  `json:"layer"`
	Channel    int     `json:"channel"`
	BitWidth   int     `json:"bit_width"`
	CalibRatio float64 `json:"calib_ratio"`
}

// Apply mutates an in-memory dataset to trigger the requested fault. The
// generated on-disk artifacts are untouched so deterministic reruns stay intact.
func Apply(ds *model.Dataset, inj Injection) error {
	switch inj.Kind {
	case "bad_bitwidth":
		if _, ok := ds.Policy.BitWidths[inj.Layer]; !ok {
			return model.NewBizError(model.CodeState, "unknown layer for injection", map[string]string{"layer": inj.Layer})
		}
		ds.Policy.BitWidths[inj.Layer] = inj.BitWidth
	case "zero_channel":
		layer := findLayer(ds, inj.Layer)
		if layer == nil {
			return model.NewBizError(model.CodeState, "unknown layer for injection", map[string]string{"layer": inj.Layer})
		}
		for i := range layer.Weights[inj.Channel] {
			layer.Weights[inj.Channel][i] = 0
		}
	case "nan_weight":
		layer := findLayer(ds, inj.Layer)
		if layer == nil {
			return model.NewBizError(model.CodeState, "unknown layer for injection", map[string]string{"layer": inj.Layer})
		}
		layer.Weights[inj.Channel][0] = math.NaN()
	case "calib_ratio":
		ds.Policy.CalibRatio = inj.CalibRatio
	default:
		return model.NewBizError(model.CodeState, "unknown injection kind", map[string]string{"kind": inj.Kind})
	}
	return nil
}

func findLayer(ds *model.Dataset, name string) *model.Layer {
	for i := range ds.Network.Layers {
		if ds.Network.Layers[i].Name == name {
			return &ds.Network.Layers[i]
		}
	}
	return nil
}
