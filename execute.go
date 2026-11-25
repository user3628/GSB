package grayrouter

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sort"
)

const DefaultSeed int64 = 20261125

type Tensor struct {
	Desc   TensorDesc `json:"desc"`
	Values []float64  `json:"values"`
}

type TensorSummary struct {
	NodeID       string    `json:"node_id,omitempty"`
	Name         string    `json:"name"`
	Shape        []int     `json:"shape"`
	DType        string    `json:"dtype"`
	ElementCount int       `json:"element_count"`
	Digest       string    `json:"digest"`
	Min          float64   `json:"min"`
	Max          float64   `json:"max"`
	Mean         float64   `json:"mean"`
	FirstValues  []float64 `json:"first_values,omitempty"`
}

type RoundingStep struct {
	NodeID                 string  `json:"node_id"`
	Stage                  string  `json:"stage"`
	Bits                   int     `json:"bits"`
	RoundingMode           string  `json:"rounding_mode"`
	Elements               int     `json:"elements"`
	Scale                  float64 `json:"scale,omitempty"`
	StepAbsErrorSum        float64 `json:"step_abs_error_sum"`
	MaxAbsError            float64 `json:"max_abs_error"`
	MeanAbsError           float64 `json:"mean_abs_error"`
	CumulativeAbsErrorSum  float64 `json:"cumulative_abs_error_sum"`
	CumulativeCompensation float64 `json:"cumulative_compensation"`
}

type NodeTrace struct {
	NodeID    string            `json:"node_id"`
	Op        string            `json:"op"`
	Inputs    []string          `json:"inputs"`
	Outputs   []TensorSummary   `json:"outputs"`
	PreSteps  []string          `json:"pre_steps,omitempty"`
	PostSteps []string          `json:"post_steps,omitempty"`
	CacheKeys map[string]string `json:"cache_keys,omitempty"`
}

type ExecutionResult struct {
	ModelKey            string          `json:"model_key"`
	ModelName           string          `json:"model_name"`
	ModelVersion        string          `json:"model_version"`
	Signature           string          `json:"signature"`
	StructuralSignature string          `json:"structural_signature"`
	Seed                int64           `json:"seed"`
	Outputs             []TensorSummary `json:"outputs"`
	NodeTraces          []NodeTrace     `json:"node_traces"`
	RoundingSteps       []RoundingStep  `json:"rounding_steps"`
}

func NumElements(shape []int) int {
	count := 1
	for _, dim := range shape {
		count *= dim
	}
	return count
}

func GenerateTensor(desc TensorDesc, seed int64, context string) Tensor {
	count := NumElements(desc.Shape)
	rng := rand.New(rand.NewSource(seed ^ int64(stableUint64(context))))
	values := make([]float64, count)
	for i := range values {
		values[i] = (rng.Float64()*2 - 1) * 0.75
	}
	values = castValues(values, desc.DType)
	return Tensor{Desc: desc, Values: values}
}

func ExecuteGraph(model ModelVersion, seed int64) (*ExecutionResult, error) {
	if seed == 0 {
		seed = DefaultSeed
	}
	if report := ValidateGraph(model.Graph); !report.Valid {
		return nil, NewAPIError(CodeGraphInvalid, "refusing to execute invalid graph", map[string]any{"errors": report.Errors})
	}
	valuesByName := make(map[string][]float64)
	descByName := make(map[string]TensorDesc)
	result := &ExecutionResult{
		ModelKey: model.Key(), ModelName: model.Name, ModelVersion: model.Version,
		Signature: GraphSignature(model.Graph), StructuralSignature: StructuralSignature(model.Graph), Seed: seed,
	}
	var cumulative KahanSum
	for _, node := range model.Graph.Nodes {
		inputNames := make([]string, 0, len(node.Inputs))
		inputValues := make([][]float64, 0, len(node.Inputs))
		for _, inputDesc := range node.Inputs {
			inputNames = append(inputNames, inputDesc.Name)
			values, ok := valuesByName[inputDesc.Name]
			if !ok {
				generated := GenerateTensor(inputDesc, seed, model.Key()+"/"+node.ID+"/input/"+inputDesc.Name)
				values = generated.Values
				valuesByName[inputDesc.Name] = values
				descByName[inputDesc.Name] = inputDesc
			}
			inputValues = append(inputValues, values)
		}
		if len(node.Outputs) == 0 {
			return nil, NewAPIError(CodeGraphInvalid, "node has no outputs", map[string]any{"node_id": node.ID, "field": "outputs"})
		}
		outputDesc := node.Outputs[0]
		expected := NumElements(outputDesc.Shape)
		var outputValues []float64
		var err error
		switch node.Op {
		case "input":
			tensor := GenerateTensor(outputDesc, seed, model.Key()+"/"+node.ID+"/output/"+outputDesc.Name)
			outputValues = tensor.Values
		case "output":
			outputValues, err = copyInput(inputValues, expected, node.ID)
		case "relu":
			outputValues, err = applyUnary(inputValues, expected, node.ID, math.Max)
			if err == nil {
				for i := range outputValues {
					outputValues[i] = math.Max(0, outputValues[i])
				}
			}
		case "sigmoid":
			outputValues, err = applyUnary(inputValues, expected, node.ID, nil)
			if err == nil {
				for i, value := range outputValues {
					outputValues[i] = 1 / (1 + math.Exp(-value))
				}
			}
		case "scale":
			factor := attrFloat(node.Attrs, "factor", 1)
			outputValues, err = applyUnary(inputValues, expected, node.ID, nil)
			if err == nil {
				for i := range outputValues {
					outputValues[i] *= factor
				}
			}
		case "add":
			outputValues, err = applyAdd(inputValues, expected, node.ID)
		case "linear":
			outputValues, err = applyLinear(node, outputDesc, inputValues, inputNames, model.Key(), seed)
		default:
			err = NewAPIError(CodeGraphInvalid, "execution does not support op", map[string]any{"node_id": node.ID, "op": node.Op})
		}
		if err != nil {
			return nil, err
		}
		if len(outputValues) != expected {
			return nil, NewAPIError(CodeGraphInvalid, "executed element count does not match output shape", map[string]any{"node_id": node.ID, "expected": expected, "actual": len(outputValues)})
		}
		if node.QuantBits < 32 {
			step := fakeQuantize(node.ID, node.QuantBits, outputValues, &cumulative)
			result.RoundingSteps = append(result.RoundingSteps, step)
		}
		beforeCast := append([]float64(nil), outputValues...)
		outputValues = castValues(outputValues, outputDesc.DType)
		if outputDesc.DType == "float32" || isIntegerDType(outputDesc.DType) {
			var local KahanSum
			var maxAbs float64
			for i := range outputValues {
				roundingError := math.Abs(outputValues[i] - beforeCast[i])
				local.Add(roundingError)
				cumulative.Add(roundingError)
				maxAbs = math.Max(maxAbs, roundingError)
			}
			result.RoundingSteps = append(result.RoundingSteps, RoundingStep{
				NodeID: node.ID, Stage: "dtype_cast", Bits: node.QuantBits,
				RoundingMode: castRoundingMode(outputDesc.DType), Elements: len(outputValues),
				StepAbsErrorSum: local.Sum, MaxAbsError: maxAbs, MeanAbsError: local.Sum / float64(max(1, len(outputValues))),
				CumulativeAbsErrorSum: cumulative.Sum, CumulativeCompensation: cumulative.Compensation,
			})
		}
		valuesByName[outputDesc.Name] = outputValues
		descByName[outputDesc.Name] = outputDesc
		summary := summarizeTensor(node.ID, outputDesc, outputValues)
		result.NodeTraces = append(result.NodeTraces, NodeTrace{
			NodeID: node.ID, Op: node.Op, Inputs: inputNames, Outputs: []TensorSummary{summary},
			PreSteps: node.PreSteps, PostSteps: node.PostSteps, CacheKeys: stepCacheKeys(model.Key(), node),
		})
	}
	outputNames := model.Graph.Outputs
	if len(outputNames) == 0 {
		for i := len(model.Graph.Nodes) - 1; i >= 0; i-- {
			if model.Graph.Nodes[i].Op == "output" {
				for _, output := range model.Graph.Nodes[i].Outputs {
					outputNames = append(outputNames, output.Name)
				}
				break
			}
		}
	}
	if len(outputNames) == 0 && len(model.Graph.Nodes) > 0 {
		for _, output := range model.Graph.Nodes[len(model.Graph.Nodes)-1].Outputs {
			outputNames = append(outputNames, output.Name)
		}
	}
	for _, name := range outputNames {
		values, ok := valuesByName[name]
		if !ok {
			return nil, NewAPIError(CodeGraphInvalid, "execution output missing", map[string]any{"output": name})
		}
		desc := descByName[name]
		var nodeID string
		for _, trace := range result.NodeTraces {
			for _, summary := range trace.Outputs {
				if summary.Name == name {
					nodeID = trace.NodeID
				}
			}
		}
		result.Outputs = append(result.Outputs, summarizeTensor(nodeID, desc, values))
	}
	sort.SliceStable(result.Outputs, func(i, j int) bool { return result.Outputs[i].Name < result.Outputs[j].Name })
	return result, nil
}

func copyInput(inputValues [][]float64, expected int, nodeID string) ([]float64, error) {
	if len(inputValues) == 0 {
		return nil, NewAPIError(CodeGraphInvalid, "output node requires an input", map[string]any{"node_id": nodeID})
	}
	if len(inputValues[0]) != expected {
		return nil, NewAPIError(CodeGraphInvalid, "input/output element count mismatch", map[string]any{"node_id": nodeID, "expected": expected, "actual": len(inputValues[0])})
	}
	return append([]float64(nil), inputValues[0]...), nil
}

func applyUnary(inputValues [][]float64, expected int, nodeID string, _ func(float64, float64) float64) ([]float64, error) {
	if len(inputValues) == 0 {
		return nil, NewAPIError(CodeGraphInvalid, "node requires one input", map[string]any{"node_id": nodeID})
	}
	if len(inputValues[0]) != expected {
		return nil, NewAPIError(CodeGraphInvalid, "input/output element count mismatch", map[string]any{"node_id": nodeID, "expected": expected, "actual": len(inputValues[0])})
	}
	return append([]float64(nil), inputValues[0]...), nil
}

func applyAdd(inputValues [][]float64, expected int, nodeID string) ([]float64, error) {
	if len(inputValues) < 2 {
		return nil, NewAPIError(CodeGraphInvalid, "add node requires two inputs", map[string]any{"node_id": nodeID})
	}
	if len(inputValues[0]) != expected || len(inputValues[1]) != expected {
		return nil, NewAPIError(CodeGraphInvalid, "add inputs must match output shape", map[string]any{"node_id": nodeID, "left": len(inputValues[0]), "right": len(inputValues[1]), "expected": expected})
	}
	out := make([]float64, expected)
	for i := range out {
		out[i] = inputValues[0][i] + inputValues[1][i]
	}
	return out, nil
}

func applyLinear(node Node, outputDesc TensorDesc, inputValues [][]float64, inputNames []string, modelKey string, seed int64) ([]float64, error) {
	if len(outputDesc.Shape) != 2 {
		return nil, NewAPIError(CodeGraphInvalid, "built-in linear op requires rank-2 output", map[string]any{"node_id": node.ID, "shape": outputDesc.Shape})
	}
	batch, outFeatures := outputDesc.Shape[0], outputDesc.Shape[1]
	inFeatures := 0
	if len(node.Inputs) > 0 && len(node.Inputs[0].Shape) == 2 {
		inFeatures = node.Inputs[0].Shape[1]
	}
	if inFeatures == 0 && len(inputValues) > 0 {
		inFeatures = len(inputValues[0]) / max(1, batch)
	}
	if inFeatures <= 0 {
		return nil, NewAPIError(CodeGraphInvalid, "cannot infer linear input features", map[string]any{"node_id": node.ID})
	}
	var x []float64
	if len(inputValues) > 0 {
		x = inputValues[0]
	} else {
		desc := node.Inputs[0]
		x = GenerateTensor(desc, seed, modelKey+"/"+node.ID+"/x").Values
	}
	if len(x) != batch*inFeatures {
		return nil, NewAPIError(CodeGraphInvalid, "linear input shape does not match declared dimensions", map[string]any{"node_id": node.ID, "expected": batch * inFeatures, "actual": len(x)})
	}
	weights := make([]float64, inFeatures*outFeatures)
	if len(inputValues) >= 2 && len(inputValues[1]) == inFeatures*outFeatures {
		weights = inputValues[1]
	} else {
		weightDesc := TensorDesc{Name: "kernel", Shape: []int{inFeatures, outFeatures}, DType: "float64"}
		weights = GenerateTensor(weightDesc, seed, modelKey+"/"+node.ID+"/kernel").Values
		for i := range weights {
			weights[i] *= math.Sqrt(2.0 / float64(inFeatures))
		}
	}
	bias := make([]float64, outFeatures)
	if len(inputValues) >= 3 && len(inputValues[2]) == outFeatures {
		bias = inputValues[2]
	} else {
		biasTensor := GenerateTensor(TensorDesc{Name: "bias", Shape: []int{outFeatures}, DType: "float64"}, seed, modelKey+"/"+node.ID+"/bias")
		copy(bias, biasTensor.Values)
	}
	out := make([]float64, batch*outFeatures)
	for b := 0; b < batch; b++ {
		for i := 0; i < inFeatures; i++ {
			xv := x[b*inFeatures+i]
			for j := 0; j < outFeatures; j++ {
				out[b*outFeatures+j] += xv * weights[i*outFeatures+j]
			}
		}
		for j := 0; j < outFeatures; j++ {
			out[b*outFeatures+j] += bias[j]
		}
	}
	return out, nil
}

func fakeQuantize(nodeID string, bits int, values []float64, cumulative *KahanSum) RoundingStep {
	qMax := math.Pow(2, float64(bits-1)) - 1
	qMin := -math.Pow(2, float64(bits-1))
	maxAbs := 1.0
	for _, value := range values {
		maxAbs = math.Max(maxAbs, math.Abs(value))
	}
	scale := maxAbs / qMax
	if scale == 0 {
		scale = 1
	}
	var local KahanSum
	var maxError float64
	for i, value := range values {
		q := math.Round(value / scale)
		q = math.Max(qMin, math.Min(qMax, q))
		dequantized := q * scale
		err := math.Abs(dequantized - value)
		local.Add(err)
		cumulative.Add(err)
		maxError = math.Max(maxError, err)
		values[i] = dequantized
	}
	return RoundingStep{
		NodeID: nodeID, Stage: "quantize_dequantize", Bits: bits,
		RoundingMode: "round-half-away-from-zero", Elements: len(values), Scale: scale,
		StepAbsErrorSum: local.Sum, MaxAbsError: maxError, MeanAbsError: local.Sum / float64(max(1, len(values))),
		CumulativeAbsErrorSum: cumulative.Sum, CumulativeCompensation: cumulative.Compensation,
	}
}

func castValues(values []float64, dtype string) []float64 {
	out := make([]float64, len(values))
	for i, value := range values {
		out[i] = castOne(value, dtype)
	}
	return out
}

func castOne(value float64, dtype string) float64 {
	switch dtype {
	case "float32":
		return float64(float32(value))
	case "int8":
		rounded := math.RoundToEven(value)
		return math.Max(-128, math.Min(127, rounded))
	case "uint8":
		rounded := math.RoundToEven(value)
		return math.Max(0, math.Min(255, rounded))
	case "int32":
		rounded := math.RoundToEven(value)
		return math.Max(-2147483648, math.Min(2147483647, rounded))
	default:
		return value
	}
}

func isIntegerDType(dtype string) bool {
	return dtype == "int8" || dtype == "uint8" || dtype == "int32"
}

func castRoundingMode(dtype string) string {
	if dtype == "float32" {
		return "ieee754-round-to-nearest-even"
	}
	if isIntegerDType(dtype) {
		return "round-to-nearest-even-with-saturation"
	}
	return "none"
}

func summarizeTensor(nodeID string, desc TensorDesc, values []float64) TensorSummary {
	summary := TensorSummary{NodeID: nodeID, Name: desc.Name, Shape: append([]int(nil), desc.Shape...), DType: desc.DType, ElementCount: len(values)}
	if len(values) > 0 {
		summary.Min, summary.Max = values[0], values[0]
		var sum KahanSum
		previewCount := min(8, len(values))
		summary.FirstValues = append([]float64(nil), values[:previewCount]...)
		for _, value := range values {
			summary.Min = math.Min(summary.Min, value)
			summary.Max = math.Max(summary.Max, value)
			sum.Add(value)
		}
		summary.Mean = sum.Sum / float64(len(values))
	}
	h := sha256.New()
	_, _ = h.Write([]byte(desc.DType))
	_ = binary.Write(h, binary.BigEndian, desc.Shape)
	var bytes [8]byte
	for _, value := range values {
		switch desc.DType {
		case "float32":
			binary.BigEndian.PutUint32(bytes[:4], math.Float32bits(float32(value)))
			_, _ = h.Write(bytes[:4])
		case "int8":
			var b byte
			if value < 0 {
				b = byte(int8(value))
			} else {
				b = byte(uint8(value))
			}
			_, _ = h.Write([]byte{b})
		case "uint8":
			_, _ = h.Write([]byte{byte(uint8(value))})
		case "int32":
			binary.BigEndian.PutUint32(bytes[:4], uint32(int32(value)))
			_, _ = h.Write(bytes[:4])
		default:
			binary.BigEndian.PutUint64(bytes[:], math.Float64bits(value))
			_, _ = h.Write(bytes[:])
		}
	}
	summary.Digest = fmt.Sprintf("sha256:%x", h.Sum(nil))
	return summary
}

func stepCacheKeys(modelKey string, node Node) map[string]string {
	if len(node.PreSteps)+len(node.PostSteps) == 0 {
		return nil
	}
	out := map[string]string{}
	for _, step := range append(append([]string{}, node.PreSteps...), node.PostSteps...) {
		out[step] = sha256Hex([]byte(modelKey), []byte(node.ID), []byte(step))[:24]
	}
	return out
}

func attrFloat(attrs map[string]any, key string, fallback float64) float64 {
	if attrs == nil {
		return fallback
	}
	switch value := attrs[key].(type) {
	case float64:
		return value
	case float32:
		return float64(value)
	case int:
		return float64(value)
	case int64:
		return float64(value)
	default:
		return fallback
	}
}
