package grayrouter

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

const (
	CategoryShape    = "shape_change"
	CategoryDType    = "dtype_change"
	CategoryQuant    = "quant_bits_change"
	CategoryOperator = "operator_change"
	CategoryPrePost  = "prepost_change"
	ActionRebuild    = "rebuild"
	ActionMigrate    = "migrate"
	ActionRetain     = "retain"
	ArtifactRouting  = "routing_table"
	ArtifactPrePost  = "prepost_cache"
	ArtifactInFlight = "inflight_binding_index"
	ArtifactGrayPlan = "gray_plan"
)

var ArtifactKinds = []string{
	ArtifactRouting,
	ArtifactPrePost,
	ArtifactInFlight,
	ArtifactGrayPlan,
}

func ParseGraph(data []byte) (Graph, error) {
	var graph Graph
	if err := json.Unmarshal(data, &graph); err != nil {
		return Graph{}, NewAPIError(CodeGraphInvalid, "graph JSON parse failed", map[string]any{
			"field": "$", "reason": err.Error(),
		})
	}
	if report := ValidateGraph(graph); !report.Valid {
		return Graph{}, NewAPIError(CodeGraphInvalid, "operator graph validation failed", map[string]any{
			"errors": report.Errors,
		})
	}
	return graph, nil
}

func ValidateGraph(graph Graph) ValidationReport {
	var errs []FieldError
	if len(graph.Nodes) == 0 {
		errs = append(errs, FieldError{Field: "nodes", Reason: "at least one node is required"})
	}
	seenIDs := make(map[string]int, len(graph.Nodes))
	outputNames := make(map[string]string)
	for i := range graph.Nodes {
		node := &graph.Nodes[i]
		prefix := fmt.Sprintf("nodes[%d]", i)
		if strings.TrimSpace(node.ID) == "" {
			errs = append(errs, FieldError{Field: prefix + ".id", Reason: "node id is required"})
		} else if prior, exists := seenIDs[node.ID]; exists {
			errs = append(errs, FieldError{NodeID: node.ID, Field: prefix + ".id", Reason: fmt.Sprintf("duplicate node id, previously at nodes[%d]", prior)})
		} else {
			seenIDs[node.ID] = i
		}
		if !AllowedOps[node.Op] {
			errs = append(errs, FieldError{NodeID: node.ID, Field: prefix + ".op", Reason: "unsupported operator type: " + node.Op})
		}
		if len(node.Outputs) == 0 {
			errs = append(errs, FieldError{NodeID: node.ID, Field: prefix + ".outputs", Reason: "at least one output tensor is required"})
		}
		for j := range node.Inputs {
			validateTensor(node.Inputs[j], fmt.Sprintf("%s.inputs[%d]", prefix, j), node.ID, &errs)
		}
		for j := range node.Outputs {
			tensorPrefix := fmt.Sprintf("%s.outputs[%d]", prefix, j)
			validateTensor(node.Outputs[j], tensorPrefix, node.ID, &errs)
			if node.Outputs[j].Name != "" {
				if prior, ok := outputNames[node.Outputs[j].Name]; ok {
					errs = append(errs, FieldError{NodeID: node.ID, Field: tensorPrefix + ".name", Reason: "output tensor already declared by node " + prior})
				} else {
					outputNames[node.Outputs[j].Name] = node.ID
				}
			}
		}
		if !AllowedQuantBits[node.QuantBits] {
			errs = append(errs, FieldError{NodeID: node.ID, Field: prefix + ".quant_bits", Reason: fmt.Sprintf("quant_bits must be one of 2,4,8,16,32, got %d", node.QuantBits)})
		}
		for j, step := range node.PreSteps {
			if strings.TrimSpace(step) == "" {
				errs = append(errs, FieldError{NodeID: node.ID, Field: fmt.Sprintf("%s.pre_steps[%d]", prefix, j), Reason: "preprocessing step id must not be empty"})
			}
		}
		for j, step := range node.PostSteps {
			if strings.TrimSpace(step) == "" {
				errs = append(errs, FieldError{NodeID: node.ID, Field: fmt.Sprintf("%s.post_steps[%d]", prefix, j), Reason: "postprocessing step id must not be empty"})
			}
		}
	}
	for i, name := range graph.Outputs {
		if strings.TrimSpace(name) == "" {
			errs = append(errs, FieldError{Field: fmt.Sprintf("outputs[%d]", i), Reason: "output tensor name must not be empty"})
		} else if _, ok := outputNames[name]; !ok {
			errs = append(errs, FieldError{Field: fmt.Sprintf("outputs[%d]", i), Reason: "output tensor is not produced by any node: " + name})
		}
	}
	return ValidationReport{Valid: len(errs) == 0, Errors: errs}
}

func validateTensor(tensor TensorDesc, field, nodeID string, errs *[]FieldError) {
	if strings.TrimSpace(tensor.Name) == "" {
		*errs = append(*errs, FieldError{NodeID: nodeID, Field: field + ".name", Reason: "tensor name is required"})
	}
	if len(tensor.Shape) == 0 {
		*errs = append(*errs, FieldError{NodeID: nodeID, Field: field + ".shape", Reason: "shape must contain at least one positive dimension"})
	}
	for d, dim := range tensor.Shape {
		if dim <= 0 {
			*errs = append(*errs, FieldError{NodeID: nodeID, Field: fmt.Sprintf("%s.shape[%d]", field, d), Reason: fmt.Sprintf("dimension must be a positive integer, got %d", dim)})
		}
	}
	if !AllowedDTypes[tensor.DType] {
		*errs = append(*errs, FieldError{NodeID: nodeID, Field: field + ".dtype", Reason: "dtype not allowed: " + tensor.DType})
	}
}

type signatureNode struct {
	Order     int          `json:"order"`
	ID        string       `json:"id"`
	Op        string       `json:"op"`
	Inputs    []TensorDesc `json:"inputs"`
	Outputs   []TensorDesc `json:"outputs"`
	QuantBits int          `json:"quant_bits"`
}

type fullSignatureNode struct {
	signatureNode
	PreSteps  []string       `json:"pre_steps"`
	PostSteps []string       `json:"post_steps"`
	Attrs     map[string]any `json:"attrs"`
}

func StructuralSignature(graph Graph) string {
	nodes := make([]signatureNode, 0, len(graph.Nodes))
	for i, node := range graph.Nodes {
		nodes = append(nodes, signatureNode{
			Order: i, ID: node.ID, Op: node.Op, Inputs: node.Inputs,
			Outputs: node.Outputs, QuantBits: node.QuantBits,
		})
	}
	return sha256Hex([]byte("grayrouter-structural-graph-v1"), canonicalJSON(nodes), canonicalJSON(graph.Outputs))
}

func GraphSignature(graph Graph) string {
	nodes := make([]fullSignatureNode, 0, len(graph.Nodes))
	for i, node := range graph.Nodes {
		nodes = append(nodes, fullSignatureNode{
			signatureNode: signatureNode{
				Order: i, ID: node.ID, Op: node.Op, Inputs: node.Inputs,
				Outputs: node.Outputs, QuantBits: node.QuantBits,
			},
			PreSteps: sortedStrings(node.PreSteps), PostSteps: sortedStrings(node.PostSteps),
			Attrs: node.Attrs,
		})
	}
	return sha256Hex([]byte("grayrouter-full-graph-v1"), canonicalJSON(nodes), canonicalJSON(graph.Outputs))
}

type Difference struct {
	Category string `json:"category"`
	NodeID   string `json:"node_id,omitempty"`
	Field    string `json:"field"`
	Old      any    `json:"old,omitempty"`
	New      any    `json:"new,omitempty"`
	Message  string `json:"message"`
}

type GraphDiff struct {
	ModelKey           string       `json:"model_key"`
	FromSignature      string       `json:"from_signature"`
	ToSignature        string       `json:"to_signature"`
	FromStructuralHash string       `json:"from_structural_hash"`
	ToStructuralHash   string       `json:"to_structural_hash"`
	Changed            bool         `json:"changed"`
	Categories         []string     `json:"categories"`
	Differences        []Difference `json:"differences"`
}

func DiffModelVersions(oldModel, newModel ModelVersion) GraphDiff {
	diff := DiffGraph(oldModel.Graph, newModel.Graph)
	diff.ModelKey = newModel.Key()
	return diff
}

func DiffGraph(oldGraph, newGraph Graph) GraphDiff {
	result := GraphDiff{
		FromSignature:      GraphSignature(oldGraph),
		ToSignature:        GraphSignature(newGraph),
		FromStructuralHash: StructuralSignature(oldGraph),
		ToStructuralHash:   StructuralSignature(newGraph),
	}
	oldByID := make(map[string]Node, len(oldGraph.Nodes))
	oldIndex := make(map[string]int, len(oldGraph.Nodes))
	for i, node := range oldGraph.Nodes {
		oldByID[node.ID] = node
		oldIndex[node.ID] = i
	}
	newIDs := make(map[string]bool, len(newGraph.Nodes))
	categorySet := make(map[string]bool)
	add := func(d Difference) {
		result.Differences = append(result.Differences, d)
		categorySet[d.Category] = true
	}
	for newIndex, newNode := range newGraph.Nodes {
		newIDs[newNode.ID] = true
		oldNode, exists := oldByID[newNode.ID]
		if !exists {
			add(Difference{Category: CategoryOperator, NodeID: newNode.ID, Field: "nodes", New: newNode.Op, Message: "operator node added"})
			continue
		}
		if oldIndex[newNode.ID] != newIndex {
			add(Difference{Category: CategoryOperator, NodeID: newNode.ID, Field: "node_order", Old: oldIndex[newNode.ID], New: newIndex, Message: "node order changed"})
		}
		if oldNode.Op != newNode.Op {
			add(Difference{Category: CategoryOperator, NodeID: newNode.ID, Field: "op", Old: oldNode.Op, New: newNode.Op, Message: "operator type changed"})
		}
		diffTensorLists(oldNode.Inputs, newNode.Inputs, "inputs", newNode.ID, add)
		diffTensorLists(oldNode.Outputs, newNode.Outputs, "outputs", newNode.ID, add)
		if oldNode.QuantBits != newNode.QuantBits {
			add(Difference{Category: CategoryQuant, NodeID: newNode.ID, Field: "quant_bits", Old: oldNode.QuantBits, New: newNode.QuantBits, Message: "quantization bit width changed"})
		}
		if !reflect.DeepEqual(sortedStrings(oldNode.PreSteps), sortedStrings(newNode.PreSteps)) ||
			!reflect.DeepEqual(sortedStrings(oldNode.PostSteps), sortedStrings(newNode.PostSteps)) {
			add(Difference{Category: CategoryPrePost, NodeID: newNode.ID, Field: "pre_post_steps", Old: map[string][]string{"pre": oldNode.PreSteps, "post": oldNode.PostSteps}, New: map[string][]string{"pre": newNode.PreSteps, "post": newNode.PostSteps}, Message: "pre/post processing steps changed"})
		}
		if stableMapDigest(oldNode.Attrs) != stableMapDigest(newNode.Attrs) {
			add(Difference{Category: CategoryOperator, NodeID: newNode.ID, Field: "attrs", Old: oldNode.Attrs, New: newNode.Attrs, Message: "operator attributes changed"})
		}
	}
	for _, oldNode := range oldGraph.Nodes {
		if !newIDs[oldNode.ID] {
			add(Difference{Category: CategoryOperator, NodeID: oldNode.ID, Field: "nodes", Old: oldNode.Op, Message: "operator node removed"})
		}
	}
	if !reflect.DeepEqual(oldGraph.Outputs, newGraph.Outputs) {
		add(Difference{Category: CategoryOperator, Field: "outputs", Old: oldGraph.Outputs, New: newGraph.Outputs, Message: "graph output set changed"})
	}
	result.Changed = len(result.Differences) > 0 || result.FromSignature != result.ToSignature
	for category := range categorySet {
		result.Categories = append(result.Categories, category)
	}
	if result.Changed && len(result.Categories) == 0 {
		result.Categories = append(result.Categories, CategoryOperator)
	}
	return result
}

func diffTensorLists(oldTensors, newTensors []TensorDesc, field, nodeID string, add func(Difference)) {
	count := len(oldTensors)
	if len(newTensors) > count {
		count = len(newTensors)
	}
	for i := 0; i < count; i++ {
		if i >= len(oldTensors) {
			add(Difference{Category: CategoryShape, NodeID: nodeID, Field: fmt.Sprintf("%s[%d]", field, i), New: newTensors[i], Message: "tensor declaration added"})
			continue
		}
		if i >= len(newTensors) {
			add(Difference{Category: CategoryShape, NodeID: nodeID, Field: fmt.Sprintf("%s[%d]", field, i), Old: oldTensors[i], Message: "tensor declaration removed"})
			continue
		}
		oldTensor, newTensor := oldTensors[i], newTensors[i]
		if !reflect.DeepEqual(oldTensor.Shape, newTensor.Shape) {
			add(Difference{Category: CategoryShape, NodeID: nodeID, Field: fmt.Sprintf("%s[%d].shape", field, i), Old: oldTensor.Shape, New: newTensor.Shape, Message: "tensor shape changed"})
		}
		if oldTensor.DType != newTensor.DType {
			add(Difference{Category: CategoryDType, NodeID: nodeID, Field: fmt.Sprintf("%s[%d].dtype", field, i), Old: oldTensor.DType, New: newTensor.DType, Message: "tensor dtype changed"})
		}
		if oldTensor.Name != newTensor.Name {
			add(Difference{Category: CategoryOperator, NodeID: nodeID, Field: fmt.Sprintf("%s[%d].name", field, i), Old: oldTensor.Name, New: newTensor.Name, Message: "tensor wiring changed"})
		}
	}
}

type ArtifactDecision struct {
	Artifact string   `json:"artifact"`
	Action   string   `json:"action"`
	Reason   string   `json:"reason"`
	Triggers []string `json:"triggers,omitempty"`
}

type MigrationPlan struct {
	ModelKey      string             `json:"model_key"`
	FromVersion   string             `json:"from_version,omitempty"`
	ToVersion     string             `json:"to_version"`
	FromSignature string             `json:"from_signature"`
	ToSignature   string             `json:"to_signature"`
	Categories    []string           `json:"categories"`
	Decisions     []ArtifactDecision `json:"decisions"`
	Migrate       []string           `json:"migrate"`
	Rebuild       []string           `json:"rebuild"`
	Retain        []string           `json:"retain"`
}

func PlanMigration(oldModel *ModelVersion, newModel ModelVersion, diff GraphDiff) MigrationPlan {
	plan := MigrationPlan{ModelKey: newModel.Key(), ToVersion: newModel.Version, FromSignature: diff.FromSignature, ToSignature: diff.ToSignature, Categories: diff.Categories}
	if oldModel != nil {
		plan.FromVersion = oldModel.Version
	}
	rules := map[string]map[string]string{
		CategoryShape:    {ArtifactRouting: ActionRebuild, ArtifactPrePost: ActionRebuild, ArtifactInFlight: ActionMigrate, ArtifactGrayPlan: ActionMigrate},
		CategoryDType:    {ArtifactRouting: ActionRebuild, ArtifactPrePost: ActionRebuild, ArtifactInFlight: ActionMigrate, ArtifactGrayPlan: ActionMigrate},
		CategoryQuant:    {ArtifactRouting: ActionMigrate, ArtifactPrePost: ActionRebuild, ArtifactInFlight: ActionMigrate, ArtifactGrayPlan: ActionMigrate},
		CategoryOperator: {ArtifactRouting: ActionRebuild, ArtifactPrePost: ActionRebuild, ArtifactInFlight: ActionMigrate, ArtifactGrayPlan: ActionRebuild},
		CategoryPrePost:  {ArtifactRouting: ActionMigrate, ArtifactPrePost: ActionRebuild, ArtifactInFlight: ActionMigrate, ArtifactGrayPlan: ActionMigrate},
	}
	reasons := map[string]string{
		CategoryShape:    "shape contract changes invalidate shape-specific routing metadata and transforms",
		CategoryDType:    "dtype changes invalidate codecs and numeric caches",
		CategoryQuant:    "quantization changes require new scales and quantize/dequantize cache entries",
		CategoryOperator: "operator topology changes require graph-derived artifacts and gray gates to be rebuilt",
		CategoryPrePost:  "pre/post step identifiers changed, so transform cache entries must be rebuilt",
	}
	for _, artifact := range ArtifactKinds {
		action := ActionRetain
		triggers := []string{}
		reason := "no graph change affects this artifact"
		for _, category := range diff.Categories {
			if categoryRules, ok := rules[category]; ok {
				if candidate := categoryRules[artifact]; candidate == ActionRebuild {
					action = ActionRebuild
					triggers = append(triggers, category)
					reason = reasons[category]
				} else if candidate == ActionMigrate && action != ActionRebuild {
					action = ActionMigrate
					triggers = append(triggers, category)
					reason = "stable identity/percentage state can be carried forward"
				}
			}
		}
		decision := ArtifactDecision{Artifact: artifact, Action: action, Reason: reason, Triggers: sortedStrings(uniqueStrings(triggers))}
		plan.Decisions = append(plan.Decisions, decision)
		switch action {
		case ActionRebuild:
			plan.Rebuild = append(plan.Rebuild, artifact)
		case ActionMigrate:
			plan.Migrate = append(plan.Migrate, artifact)
		default:
			plan.Retain = append(plan.Retain, artifact)
		}
	}
	return plan
}

func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, value := range values {
		if !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	return out
}
