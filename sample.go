package grayrouter

import (
	"encoding/json"
	"os"
	"path/filepath"
)

const SampleSeed int64 = 20261125

type TrailRequest struct {
	RequestID string `json:"request_id"`
	RouteKey  string `json:"route_key"`
	ModelKey  string `json:"model_key,omitempty"`
	Note      string `json:"note,omitempty"`
}

type RequestTrail struct {
	Seed     int64          `json:"seed"`
	Requests []TrailRequest `json:"requests"`
}

type SampleBundle struct {
	Baseline Snapshot     `json:"baseline"`
	Delta    Delta        `json:"delta"`
	Invalid  ModelVersion `json:"invalid"`
	Trail    RequestTrail `json:"trail"`
}

func SampleBundleFixed() SampleBundle {
	baseline := Snapshot{Version: 1, Models: []ModelVersion{
		sampleShapeModel("1.0.0", 4),
		sampleDTypeModel("1.0.0", "float32"),
		sampleQuantModel("1.0.0", 32),
		sampleOpModel("1.0.0", false),
		samplePrePostModel("1.0.0", "v1"),
		sampleStableModel(),
	}}
	delta := Delta{Version: 2, BaseVersion: 1, Models: []ModelVersion{
		sampleShapeModel("1.1.0", 6),
		sampleDTypeModel("1.1.0", "float64"),
		sampleQuantModel("1.1.0", 8),
		sampleOpModel("1.1.0", true),
		samplePrePostModel("1.1.0", "v2"),
	}}
	for i := range delta.Models {
		if delta.Models[i].Name == "demo-op" {
			delta.Models[i].Gray = GrayConfig{
				RouteKey: "demo-op-gray", Baseline: "demo-op@1.0.0", Canary: "demo-op@1.1.0",
				CanaryRate: 0.25, Salt: "fixed-canary-salt",
			}
		}
	}
	invalid := ModelVersion{
		Name: "demo-invalid", Version: "0.0.1",
		Graph: Graph{Nodes: []Node{{
			ID: "", Op: "mystery_op", QuantBits: 7,
			Outputs: []TensorDesc{{Name: "bad", Shape: []int{1, 0, -4}, DType: "bfloat16"}},
		}}},
	}
	trail := RequestTrail{Seed: SampleSeed}
	for i := 1; i <= 24; i++ {
		trail.Requests = append(trail.Requests, TrailRequest{
			RequestID: formatRequestID(i), RouteKey: "demo-op-gray",
			Note: "fixed deterministic request trajectory",
		})
	}
	trail.Requests = append(trail.Requests,
		TrailRequest{RequestID: "req-duplicate-0001", RouteKey: "demo-op-gray"},
		TrailRequest{RequestID: "req-duplicate-0001", RouteKey: "demo-op-gray", Note: "duplicate idempotency submission"},
	)
	return SampleBundle{Baseline: baseline, Delta: delta, Invalid: invalid, Trail: trail}
}

func formatRequestID(i int) string {
	const digits = "0000"
	n := ""
	value := i
	for value > 0 {
		n = string(rune('0'+value%10)) + n
		value /= 10
	}
	if len(n) < 4 {
		n = digits[len(n):] + n
	}
	return "req-" + n
}

func sampleShapeModel(version string, features int) ModelVersion {
	return ModelVersion{
		Name: "demo-shape", Version: version,
		Route: RouteConfig{Path: "/infer/demo-shape", Enabled: true},
		Graph: Graph{
			Nodes: []Node{
				{ID: "n_input", Op: "input", QuantBits: 32, PreSteps: []string{"pre_normalize_v1"},
					Outputs: []TensorDesc{{Name: "x", Shape: []int{1, features}, DType: "float32"}}},
				{ID: "n_linear", Op: "linear", QuantBits: 8,
					Inputs:  []TensorDesc{{Name: "x", Shape: []int{1, features}, DType: "float32"}},
					Outputs: []TensorDesc{{Name: "y", Shape: []int{1, 3}, DType: "float32"}}},
				{ID: "n_relu", Op: "relu", QuantBits: 8,
					Inputs:  []TensorDesc{{Name: "y", Shape: []int{1, 3}, DType: "float32"}},
					Outputs: []TensorDesc{{Name: "z", Shape: []int{1, 3}, DType: "float32"}}},
				sampleOutputNode("z", "out", []int{1, 3}, "float32"),
			},
			Outputs: []string{"out"},
		},
	}
}

func sampleDTypeModel(version, dtype string) ModelVersion {
	return ModelVersion{
		Name: "demo-dtype", Version: version,
		Route: RouteConfig{Path: "/infer/demo-dtype", Enabled: true},
		Graph: Graph{Nodes: []Node{
			{ID: "n_input", Op: "input", QuantBits: 32, Outputs: []TensorDesc{{Name: "x", Shape: []int{2, 4}, DType: "float32"}}},
			{ID: "n_scale", Op: "scale", QuantBits: 16, Attrs: map[string]any{"factor": 0.5},
				Inputs:  []TensorDesc{{Name: "x", Shape: []int{2, 4}, DType: "float32"}},
				Outputs: []TensorDesc{{Name: "y", Shape: []int{2, 4}, DType: dtype}}},
			{ID: "n_relu", Op: "relu", QuantBits: 16,
				Inputs:  []TensorDesc{{Name: "y", Shape: []int{2, 4}, DType: dtype}},
				Outputs: []TensorDesc{{Name: "z", Shape: []int{2, 4}, DType: dtype}}},
			{ID: "n_output", Op: "output", QuantBits: 32,
				Inputs:  []TensorDesc{{Name: "z", Shape: []int{2, 4}, DType: dtype}},
				Outputs: []TensorDesc{{Name: "out", Shape: []int{2, 4}, DType: dtype}}},
		}, Outputs: []string{"out"}},
	}
}

func sampleQuantModel(version string, bits int) ModelVersion {
	return ModelVersion{
		Name: "demo-quant", Version: version,
		Route: RouteConfig{Path: "/infer/demo-quant", Enabled: true},
		Graph: Graph{Nodes: []Node{
			{ID: "n_input", Op: "input", QuantBits: 32, Outputs: []TensorDesc{{Name: "x", Shape: []int{1, 6}, DType: "float32"}}},
			{ID: "n_linear", Op: "linear", QuantBits: bits,
				Inputs:  []TensorDesc{{Name: "x", Shape: []int{1, 6}, DType: "float32"}},
				Outputs: []TensorDesc{{Name: "y", Shape: []int{1, 4}, DType: "float32"}}},
			{ID: "n_sigmoid", Op: "sigmoid", QuantBits: bits,
				Inputs:  []TensorDesc{{Name: "y", Shape: []int{1, 4}, DType: "float32"}},
				Outputs: []TensorDesc{{Name: "z", Shape: []int{1, 4}, DType: "float32"}}},
			sampleOutputNode("z", "out", []int{1, 4}, "float32"),
		}, Outputs: []string{"out"}},
	}
}

func sampleOpModel(version string, withScale bool) ModelVersion {
	nodes := []Node{
		{ID: "n_input", Op: "input", QuantBits: 32, Outputs: []TensorDesc{{Name: "x", Shape: []int{1, 4}, DType: "float32"}}},
		{ID: "n_linear", Op: "linear", QuantBits: 8,
			Inputs:  []TensorDesc{{Name: "x", Shape: []int{1, 4}, DType: "float32"}},
			Outputs: []TensorDesc{{Name: "y", Shape: []int{1, 3}, DType: "float32"}}},
		{ID: "n_relu", Op: "relu", QuantBits: 8,
			Inputs:  []TensorDesc{{Name: "y", Shape: []int{1, 3}, DType: "float32"}},
			Outputs: []TensorDesc{{Name: "z", Shape: []int{1, 3}, DType: "float32"}}},
	}
	if withScale {
		nodes = append(nodes, Node{
			ID: "n_scale", Op: "scale", QuantBits: 8, Attrs: map[string]any{"factor": 0.75},
			Inputs:  []TensorDesc{{Name: "z", Shape: []int{1, 3}, DType: "float32"}},
			Outputs: []TensorDesc{{Name: "s", Shape: []int{1, 3}, DType: "float32"}},
		})
		nodes = append(nodes, Node{
			ID: "n_output", Op: "output", QuantBits: 32, PostSteps: []string{"post_clip_v2"},
			Inputs:  []TensorDesc{{Name: "s", Shape: []int{1, 3}, DType: "float32"}},
			Outputs: []TensorDesc{{Name: "out", Shape: []int{1, 3}, DType: "float32"}},
		})
	} else {
		nodes = append(nodes, sampleOutputNode("z", "out", []int{1, 3}, "float32"))
	}
	return ModelVersion{
		Name: "demo-op", Version: version, Route: RouteConfig{Path: "/infer/demo-op", Enabled: true},
		Graph: Graph{Nodes: nodes, Outputs: []string{"out"}},
	}
}

func samplePrePostModel(version, tag string) ModelVersion {
	return ModelVersion{
		Name: "demo-prepost", Version: version, Route: RouteConfig{Path: "/infer/demo-prepost", Enabled: true},
		Graph: Graph{Nodes: []Node{
			{ID: "n_input", Op: "input", QuantBits: 32, PreSteps: []string{"pre_normalize_" + tag, "pre_mask_" + tag},
				Outputs: []TensorDesc{{Name: "x", Shape: []int{1, 4}, DType: "float32"}}},
			{ID: "n_scale", Op: "scale", QuantBits: 32, Attrs: map[string]any{"factor": 1.25},
				Inputs:  []TensorDesc{{Name: "x", Shape: []int{1, 4}, DType: "float32"}},
				Outputs: []TensorDesc{{Name: "y", Shape: []int{1, 4}, DType: "float32"}}},
			{ID: "n_output", Op: "output", QuantBits: 32, PostSteps: []string{"post_summary_" + tag},
				Inputs:  []TensorDesc{{Name: "y", Shape: []int{1, 4}, DType: "float32"}},
				Outputs: []TensorDesc{{Name: "out", Shape: []int{1, 4}, DType: "float32"}}},
		}, Outputs: []string{"out"}},
	}
}

func sampleStableModel() ModelVersion {
	return ModelVersion{
		Name: "demo-stable", Version: "1.0.0", Route: RouteConfig{Path: "/infer/demo-stable", Enabled: true},
		Gray: GrayConfig{RouteKey: "stable-gray", Baseline: "demo-stable@1.0.0", Canary: "demo-stable@1.0.0", CanaryRate: 0},
		Graph: Graph{Nodes: []Node{
			{ID: "n_input", Op: "input", QuantBits: 32, Outputs: []TensorDesc{{Name: "x", Shape: []int{1, 8}, DType: "float32"}}},
			{ID: "n_relu", Op: "relu", QuantBits: 32,
				Inputs:  []TensorDesc{{Name: "x", Shape: []int{1, 8}, DType: "float32"}},
				Outputs: []TensorDesc{{Name: "z", Shape: []int{1, 8}, DType: "float32"}}},
			sampleOutputNode("z", "out", []int{1, 8}, "float32"),
		}, Outputs: []string{"out"}},
	}
}

func sampleOutputNode(inputName, outputName string, shape []int, dtype string) Node {
	return Node{
		ID: "n_output", Op: "output", QuantBits: 32,
		Inputs:  []TensorDesc{{Name: inputName, Shape: shape, DType: dtype}},
		Outputs: []TensorDesc{{Name: outputName, Shape: append([]int(nil), shape...), DType: dtype}},
	}
}

func WriteSampleFiles(dir string) error {
	bundle := SampleBundleFixed()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	files := map[string]any{
		"snapshot-v1.json":   bundle.Baseline,
		"delta-v2.json":      bundle.Delta,
		"invalid-graph.json": bundle.Invalid,
		"request-trail.json": bundle.Trail,
	}
	for name, value := range files {
		data, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return err
		}
		data = append(data, '\n')
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			return err
		}
	}
	return nil
}
