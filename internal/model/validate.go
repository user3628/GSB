package model

import (
	"fmt"

	"example.com/logicplan/editor/internal/bizerr"
)

// Analyze validates the complete operator tree and derives its output columns.
// The returned plan contains maps and slices that callers must treat as immutable.
func Analyze(nodes []*Node) (*Plan, error) {
	byID := make(map[string]*Node, len(nodes))
	for i, n := range nodes {
		if n == nil {
			return nil, planErr("nil operator at index %d", i)
		}
		if n.ID == "" {
			return nil, planErr("operator at index %d has an empty id", i)
		}
		if !n.Type.Valid() {
			return nil, planErr("operator %q has unknown type %q", n.ID, n.Type)
		}
		if _, exists := byID[n.ID]; exists {
			return nil, planErr("duplicate operator id %q", n.ID)
		}
		byID[n.ID] = n
	}
	if len(byID) == 0 {
		return nil, planErr("operator tree is empty")
	}

	parent := make(map[string]string, len(byID))
	for _, n := range byID {
		for _, col := range n.Columns {
			if col.Name == "" {
				return nil, planErr("operator %q has an empty column name", n.ID)
			}
		}
		if n.Predicate != nil {
			for _, col := range n.Predicate.Columns {
				if col.Name == "" {
					return nil, planErr("operator %q has an empty predicate column name", n.ID)
				}
			}
		}
		if n.Stats != nil {
			if n.Stats.OperatorID == "" || n.Stats.OperatorID != n.ID {
				return nil, planErr("operator %q has stats bound to %q", n.ID, n.Stats.OperatorID)
			}
			if n.Stats.RowCount < 0 {
				return nil, planErr("operator %q has a negative row count", n.ID)
			}
			for key, value := range n.Stats.Distinct {
				if value < 0 {
					return nil, planErr("operator %q has a negative distinct count for %q", n.ID, key)
				}
			}
		}
		seenChildren := make(map[string]struct{}, len(n.Children))
		for _, child := range n.Children {
			if child == "" {
				return nil, planErr("operator %q has an empty child reference", n.ID)
			}
			if child == n.ID {
				return nil, planErr("operator %q references itself as a child", n.ID)
			}
			if _, ok := byID[child]; !ok {
				return nil, planErr("operator %q references missing child %q", n.ID, child)
			}
			if _, dup := seenChildren[child]; dup {
				return nil, planErr("operator %q references child %q more than once", n.ID, child)
			}
			seenChildren[child] = struct{}{}
			if prior, exists := parent[child]; exists {
				return nil, planErr("operator %q has multiple parents %q and %q", child, prior, n.ID)
			}
			parent[child] = n.ID
		}
		if err := validateArity(n); err != nil {
			return nil, err
		}
	}

	root := ""
	for id := range byID {
		if _, hasParent := parent[id]; !hasParent {
			if root != "" {
				return nil, planErr("operator tree has multiple roots %q and %q", root, id)
			}
			root = id
		}
	}
	if root == "" {
		return nil, planErr("operator tree has no root")
	}

	visiting := make(map[string]bool, len(byID))
	visited := make(map[string]bool, len(byID))
	order := make([]string, 0, len(byID))
	var visit func(string) error
	visit = func(id string) error {
		if visiting[id] {
			return planErr("operator tree contains a cycle at %q", id)
		}
		if visited[id] {
			return nil
		}
		visiting[id] = true
		for _, child := range byID[id].Children {
			if err := visit(child); err != nil {
				return err
			}
		}
		visiting[id] = false
		visited[id] = true
		order = append(order, id)
		return nil
	}
	if err := visit(root); err != nil {
		return nil, err
	}
	if len(visited) != len(byID) {
		for id := range byID {
			if !visited[id] {
				return nil, planErr("operator %q is not reachable from root %q", id, root)
			}
		}
	}

	outputs := make(map[string][]ColumnRef, len(byID))
	for _, id := range order {
		n := byID[id]
		upstream := upstreamOutputs(n, outputs)
		if n.Type != TypeScan {
			if err := validateRefs(n.ID, "columns", n.Columns, upstream); err != nil {
				return nil, err
			}
		}
		if n.Predicate != nil {
			if err := validateRefs(n.ID, "predicate.columns", n.Predicate.Columns, upstream); err != nil {
				return nil, err
			}
		}
		if n.Type == TypeFilter && (n.Predicate == nil || NormalizeExpression(n.Predicate.Expr) == "") {
			return nil, planErr("filter operator %q requires a non-empty predicate", n.ID)
		}
		outputs[id] = deriveOutputs(n, outputs)
	}

	return &Plan{Nodes: byID, Root: root, Outputs: outputs}, nil
}

func validateArity(n *Node) error {
	switch n.Type {
	case TypeScan:
		if len(n.Children) != 0 {
			return planErr("scan operator %q must not have children", n.ID)
		}
	case TypeFilter, TypeProject, TypeAggregate, TypeSort:
		if len(n.Children) != 1 {
			return planErr("%s operator %q must have exactly one child", n.Type, n.ID)
		}
	case TypeJoin:
		if len(n.Children) < 2 {
			return planErr("join operator %q must have at least two children", n.ID)
		}
	}
	return nil
}

func upstreamOutputs(n *Node, outputs map[string][]ColumnRef) []ColumnRef {
	switch n.Type {
	case TypeScan:
		return nil
	case TypeFilter, TypeProject, TypeSort:
		return outputs[n.Children[0]]
	case TypeAggregate:
		return outputs[n.Children[0]]
	case TypeJoin:
		var out []ColumnRef
		for _, child := range n.Children {
			out = append(out, outputs[child]...)
		}
		return out
	default:
		return nil
	}
}

func validateRefs(nodeID, field string, refs, upstream []ColumnRef) error {
	if len(refs) == 0 {
		return nil
	}
	available := make(map[string]struct{}, len(upstream))
	for _, c := range upstream {
		available[c.Key()] = struct{}{}
	}
	for _, c := range refs {
		if c.Name == "" {
			return planErr("operator %q has an empty column name in %s", nodeID, field)
		}
		if _, ok := available[c.Key()]; !ok {
			return planErr("operator %q %s references upstream column %q that is not produced", nodeID, field, c.Key())
		}
	}
	return nil
}

func deriveOutputs(n *Node, outputs map[string][]ColumnRef) []ColumnRef {
	switch n.Type {
	case TypeScan:
		return dedupeColumns(n.Columns)
	case TypeProject, TypeAggregate:
		return dedupeColumns(n.Columns)
	case TypeFilter, TypeSort:
		return append([]ColumnRef(nil), outputs[n.Children[0]]...)
	case TypeJoin:
		var out []ColumnRef
		for _, child := range n.Children {
			out = append(out, outputs[child]...)
		}
		return dedupeColumns(out)
	default:
		return nil
	}
}

func dedupeColumns(cols []ColumnRef) []ColumnRef {
	seen := make(map[string]struct{}, len(cols))
	out := make([]ColumnRef, 0, len(cols))
	for _, c := range cols {
		key := c.Key()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, c)
	}
	return out
}

func planErr(format string, args ...any) *bizerr.Error {
	return bizerr.New(bizerr.CodePlanInvalid, fmt.Sprintf(format, args...), nil)
}
