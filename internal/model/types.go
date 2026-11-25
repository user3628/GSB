package model

// Layer is a single fully-connected (linear) layer of the lightweight classifier.
// Weights are stored row-major: Weights[out][in]; Bias has one entry per output.
type Layer struct {
	Name    string      `json:"name"`
	InDim   int         `json:"in_dim"`
	OutDim  int         `json:"out_dim"`
	Weights [][]float64 `json:"weights"`
	Bias    []float64   `json:"bias"`
	Relu    bool        `json:"relu"`
}

// Network is the ordered stack of linear layers.
type Network struct {
	Name   string  `json:"name"`
	Layers []Layer `json:"layers"`
}

// LayerPolicy declares the fixed-point strategy for one layer.
type LayerPolicy struct {
	BitWidth   int  `json:"bit_width"`
	Symmetric  bool `json:"symmetric"`
	PerChannel bool `json:"per_channel"`
}

// Policy is the full quantization strategy declaration.
type Policy struct {
	Name            string          `json:"name"`
	BitWidths       map[string]int  `json:"bit_widths"`
	Symmetric       map[string]bool `json:"symmetric"`
	PerChannel      map[string]bool `json:"per_channel"`
	CalibRatio      float64         `json:"calib_ratio"`
	Seed            int64           `json:"seed"`
	Tolerance       float64         `json:"tolerance"`
	SamplePositions []int           `json:"sample_positions"`
}

// LayerPolicy resolves the per-layer policy and validates the bit width set.
func (p *Policy) LayerPolicy(layerName string) LayerPolicy {
	return LayerPolicy{
		BitWidth:   p.BitWidths[layerName],
		Symmetric:  p.Symmetric[layerName],
		PerChannel: p.PerChannel[layerName],
	}
}

// Shard is one small grayscale image patch with its declared size and ground-truth label.
type Shard struct {
	ID     string `json:"id"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
	Label  int    `json:"label"`
	Path   string `json:"path"`
}

// ReferenceOutput is the high-precision inference truth for one sample position.
type ReferenceOutput struct {
	Position int       `json:"position"`
	Logits   []float64 `json:"logits"`
	Label    int       `json:"label"`
	// LayerOutputs holds the high-precision activation at each layer boundary.
	LayerOutputs map[string][]float64 `json:"layer_outputs"`
}

// Dataset bundles every generated/imported input artifact.
type Dataset struct {
	Network    Network           `json:"network"`
	Shards     []Shard           `json:"shards"`
	Policy     Policy            `json:"policy"`
	References []ReferenceOutput `json:"references"`
	InputDim   int               `json:"input_dim"`
	NumClasses int               `json:"num_classes"`
}

// TensorParams is the fixed-point calibration of one tensor (a whole layer or one channel).
type TensorParams struct {
	Scale     float64 `json:"scale"`
	ZeroPoint int     `json:"zero_point"`
	QMin      int     `json:"q_min"`
	QMax      int     `json:"q_max"`
	Clamps    int     `json:"clamps"`
	Step      float64 `json:"step"`    // quantization step == scale
	Channel   int     `json:"channel"` // -1 for layer-wise tensors
}

// LayerCalibration is the persisted calibration artifact of one layer.
type LayerCalibration struct {
	LayerName         string         `json:"layer_name"`
	BitWidth          int            `json:"bit_width"`
	Symmetric         bool           `json:"symmetric"`
	PerChannel        bool           `json:"per_channel"`
	WeightParams      []TensorParams `json:"weight_params"`
	ActivationParam   TensorParams   `json:"activation_param"`
	OutputParam       TensorParams   `json:"output_param"`
	BiasScale         float64        `json:"bias_scale"`
	InputFingerprint  string         `json:"input_fingerprint"`
	PolicyFingerprint string         `json:"policy_fingerprint"`
	ArtifactDigest    string         `json:"artifact_digest"`
	AccumBits         int            `json:"accum_bits"`
}

// StageFingerprints groups the content digests that define a pipeline version.
type StageFingerprints struct {
	WeightsDigest      string `json:"weights_digest"`
	CalibDigest        string `json:"calib_digest"`
	PolicyDigest       string `json:"policy_digest"`
	PositionsDigest    string `json:"positions_digest"`
	VersionFingerprint string `json:"version_fingerprint"`
}

// LayerStatus is the per-layer view served through HTTP/CLI.
type LayerStatus struct {
	Layer          string `json:"layer"`
	Calibrated     bool   `json:"calibrated"`
	Version        int64  `json:"version"`
	Seq            int64  `json:"seq"`
	BitWidth       int    `json:"bit_width"`
	Symmetric      bool   `json:"symmetric"`
	PerChannel     bool   `json:"per_channel"`
	ArtifactDigest string `json:"artifact_digest"`
	Invalidated    bool   `json:"invalidated"`
	Reason         string `json:"reason,omitempty"`
}
