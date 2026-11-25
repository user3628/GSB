package feedbackstream

import (
	"fmt"
	"sort"
)

func PipelineInvalid(message string) *Error {
	return NewError(CodePipelineInvalid, message)
}

// ValidatePipeline verifies that the pipeline is one connected linear chain
// and that each operator accepts the previous operator's output signature.
func ValidatePipeline(p PipelineSpec) ([]OperatorSpec, error) {
	byID := map[string]OperatorSpec{}
	for _, op := range p.Operators {
		if op.ID == "" {
			return nil, PipelineInvalid("operator id is empty")
		}
		if _, exists := byID[op.ID]; exists {
			return nil, PipelineInvalid("duplicate operator id: " + op.ID)
		}
		byID[op.ID] = op
	}

	order := make([]OperatorSpec, 0, len(p.Operators))
	if len(p.Edges) == 0 {
		order = append(order, p.Operators...)
	} else {
		next := map[string]string{}
		prev := map[string]string{}
		for _, edge := range p.Edges {
			if _, ok := byID[edge.From]; !ok {
				return nil, PipelineInvalid("edge references missing source operator: " + edge.From)
			}
			if _, ok := byID[edge.To]; !ok {
				return nil, PipelineInvalid("edge references missing target operator: " + edge.To)
			}
			if _, duplicated := next[edge.From]; duplicated {
				return nil, PipelineInvalid("operator has multiple outgoing edges: " + edge.From)
			}
			if _, duplicated := prev[edge.To]; duplicated {
				return nil, PipelineInvalid("operator has multiple incoming edges: " + edge.To)
			}
			next[edge.From] = edge.To
			prev[edge.To] = edge.From
		}
		roots := make([]string, 0)
		for _, op := range p.Operators {
			if _, hasPrev := prev[op.ID]; !hasPrev {
				roots = append(roots, op.ID)
			}
		}
		if len(roots) != 1 {
			return nil, PipelineInvalid(fmt.Sprintf("pipeline must have exactly one root, found %d", len(roots)))
		}
		current := roots[0]
		seen := map[string]bool{}
		for current != "" {
			if seen[current] {
				return nil, PipelineInvalid("pipeline contains a cycle at operator: " + current)
			}
			seen[current] = true
			order = append(order, byID[current])
			current = next[current]
		}
		if len(order) != len(p.Operators) {
			return nil, PipelineInvalid("pipeline edges do not connect every operator into one linear chain")
		}
	}

	for i := 1; i < len(order); i++ {
		previous := order[i-1]
		current := order[i]
		if current.InputSignature == nil {
			err := PipelineInvalid("operator input signature is missing").
				With("operator_id", current.ID).
				With("operator_index", i)
			return nil, err
		}
		if !signaturesCompatible(previous.OutputSignature, *current.InputSignature) {
			return nil, PipelineInvalid("first incompatible operator in linear chain").
				With("operator_id", current.ID).
				With("operator_index", i).
				With("previous_operator_id", previous.ID).
				With("expected_schema", previous.OutputSignature.Schema).
				With("actual_input_schema", current.InputSignature.Schema).
				With("expected_fields", previous.OutputSignature.Fields).
				With("actual_input_fields", current.InputSignature.Fields)
		}
	}
	return order, nil
}

func signaturesCompatible(output Signature, input Signature) bool {
	if input.Schema != output.Schema {
		return false
	}
	outputFields := map[string]string{}
	for _, field := range output.Fields {
		outputFields[field.Name] = field.Type
	}
	for _, field := range input.Fields {
		actualType, ok := outputFields[field.Name]
		if !ok || actualType != field.Type {
			return false
		}
	}
	return true
}

func (p *PolicySnapshot) edgeBetween(from, to string) *EdgeSpec {
	for i := range p.Pipeline.Edges {
		edge := &p.Pipeline.Edges[i]
		if edge.From == from && edge.To == to {
			return edge
		}
	}
	return nil
}

func containsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func pairDeclared(rules []FusionPair, left, right OperatorSpec) bool {
	for _, pair := range rules {
		leftOK := pair.Left == left.Kind || pair.Left == left.ID
		rightOK := pair.Right == right.Kind || pair.Right == right.ID
		if leftOK && rightOK {
			return true
		}
	}
	return false
}

// PlanFusion validates the chain and deterministically forms maximal fusion
// groups according to the versioned policy declarations.
func PlanFusion(policy *PolicySnapshot) (*FusionPlan, error) {
	if policy == nil {
		return nil, SpecInvalid("nil policy")
	}
	ordered, err := ValidatePipeline(policy.Pipeline)
	if err != nil {
		return nil, err
	}

	plan := &FusionPlan{}
	groupStart := 0
	groupIndex := 0

	flushGroup := func(end int) {
		if end <= groupStart {
			return
		}
		groupIndex++
		ops := ordered[groupStart:end]
		ids := make([]string, 0, len(ops))
		kinds := make([]string, 0, len(ops))
		for _, op := range ops {
			ids = append(ids, op.ID)
			kinds = append(kinds, op.Kind)
		}
		input := ops[0].OutputSignature
		if ops[0].InputSignature != nil {
			input = *ops[0].InputSignature
		}
		output := ops[len(ops)-1].OutputSignature
		hashPayload := struct {
			PolicyVersion int64          `json:"policy_version"`
			GroupIndex    int            `json:"group_index"`
			OperatorIDs   []string       `json:"operator_ids"`
			Operators     []OperatorSpec `json:"operators"`
			Input         Signature      `json:"input"`
			Output        Signature      `json:"output"`
		}{policy.Version, groupIndex, ids, ops, input, output}
		hash, _ := DigestValue(hashPayload)
		group := FusionGroup{
			Index: groupIndex - 1, ID: fmt.Sprintf("fg-%d", groupIndex),
			OperatorIDs: ids, Kinds: kinds,
			InputSignature: input, OutputSignature: output,
			SignatureHash: hash[:16],
		}
		plan.Groups = append(plan.Groups, group)
		groupID := group.ID
		for j, op := range ops {
			plan.OperatorDecisions = append(plan.OperatorDecisions, OperatorDecision{
				OperatorID: op.ID, Kind: op.Kind, Fused: len(ops) > 1,
				GroupID: groupID, BoundaryAfter: j == len(ops)-1,
				Reasons: fusionOperatorReasons(op, len(ops) > 1),
			})
		}
		groupStart = end
	}

	for i := 0; i+1 < len(ordered); i++ {
		left := ordered[i]
		right := ordered[i+1]
		reasons := make([]string, 0, 4)
		fuse := true

		if !containsString(policy.Fusion.FusibleKinds, left.Kind) || !containsString(policy.Fusion.FusibleKinds, right.Kind) {
			fuse = false
			reasons = append(reasons, "KIND_NOT_IN_FUSIBLE_SET")
		}
		if !pairDeclared(policy.Fusion.Pairs, left, right) {
			fuse = false
			reasons = append(reasons, "PAIR_NOT_DECLARED")
		}
		edge := policy.edgeBetween(left.ID, right.ID)
		if left.Repartition || right.Repartition || (edge != nil && edge.Repartition) {
			fuse = false
			reasons = append(reasons, "CROSS_PARTITION_REDISTRIBUTION")
		}
		currentSize := i - groupStart + 2
		if policy.Fusion.MaxGroupSize > 0 && currentSize > policy.Fusion.MaxGroupSize {
			fuse = false
			reasons = append(reasons, "MAX_GROUP_SIZE")
		}
		if fuse {
			reasons = append(reasons, "DECLARED_FUSIBLE_PAIR", "PARTITION_LOCAL_SIGNATURES_COMPATIBLE")
		}
		decision := EdgeDecision{From: left.ID, To: right.ID, Fused: fuse, Reasons: reasons}
		plan.EdgeDecisions = append(plan.EdgeDecisions, decision)
		if !fuse {
			flushGroup(i + 1)
		}
	}
	flushGroup(len(ordered))

	opGroup := map[string]string{}
	for _, group := range plan.Groups {
		for _, id := range group.OperatorIDs {
			opGroup[id] = group.ID
		}
	}
	for i := range plan.EdgeDecisions {
		if plan.EdgeDecisions[i].Fused {
			if id, ok := opGroup[plan.EdgeDecisions[i].From]; ok {
				plan.EdgeDecisions[i].GroupID = id
			}
		}
	}
	sort.SliceStable(plan.OperatorDecisions, func(i, j int) bool {
		return indexOfOperatorID(ordered, plan.OperatorDecisions[i].OperatorID) <
			indexOfOperatorID(ordered, plan.OperatorDecisions[j].OperatorID)
	})
	return plan, nil
}

func fusionOperatorReasons(op OperatorSpec, fused bool) []string {
	if fused {
		return []string{"PART_OF_FUSED_CHAIN", "SIGNATURE_BOUNDARY_RETAINED_ON_GROUP"}
	}
	return []string{"FUSION_GROUP_BOUNDARY", "STANDALONE_OR_GROUP_EDGE"}
}

func indexOfOperator(ops []OperatorSpec, id string) int {
	return indexOfOperatorID(ops, id)
}

func indexOfOperatorID(ops []OperatorSpec, id string) int {
	for i, op := range ops {
		if op.ID == id {
			return i
		}
	}
	return -1
}

func (p *FusionPlan) GroupByID(id string) *FusionGroup {
	for i := range p.Groups {
		if p.Groups[i].ID == id {
			return &p.Groups[i]
		}
	}
	return nil
}
