package planstate

import (
	"sort"
	"time"

	"example.com/logicplan/editor/internal/bizerr"
	"example.com/logicplan/editor/internal/model"
)

type Operation struct {
	Op        string              `json:"op"`
	NodeID    string              `json:"node_id,omitempty"`
	Node      *model.Node         `json:"node,omitempty"`
	Columns   []model.ColumnRef   `json:"columns,omitempty"`
	Predicate *model.Predicate    `json:"predicate,omitempty"`
	Children  []string            `json:"children,omitempty"`
	Stats     *model.StatsBinding `json:"stats,omitempty"`
	ParentID  string              `json:"parent_id,omitempty"`
	Position  *int                `json:"position,omitempty"`
}

type PreparedEdit struct {
	Nodes    map[string]*model.Node
	Plan     *model.Plan
	Affected []string
	Deleted  []string
	Kinds    RebuildKinds
}

func PrepareEdit(state *State, ops []Operation) (*PreparedEdit, error) {
	if state == nil {
		return nil, bizerr.New(bizerr.CodeSnapshotNotFound, "no committed snapshot is loaded", nil)
	}
	if len(ops) == 0 {
		return nil, bizerr.New(bizerr.CodeBadRequest, "incremental fragment has no operations", nil)
	}
	nodes := make(map[string]*model.Node, len(state.Nodes)+len(ops))
	for id, node := range state.Nodes {
		nodes[id] = node
	}
	changed := make(map[string]struct{})
	deleted := make(map[string]struct{})
	kinds := RebuildKinds{TreeSummary: true}
	cloneForEdit := func(id string) (*model.Node, error) {
		node, ok := nodes[id]
		if !ok {
			return nil, bizerr.New(bizerr.CodePlanInvalid, "operation references missing operator",
				map[string]any{"operator_id": id})
		}
		cp := node.Clone()
		nodes[id] = cp
		changed[id] = struct{}{}
		return cp, nil
	}

	for opIndex, op := range ops {
		ctx := map[string]any{"operation_index": opIndex, "operation": op.Op, "node_id": op.NodeID}
		switch op.Op {
		case "add_node":
			kinds = AllRebuildKinds()
			if op.Node == nil {
				return nil, bizerr.New(bizerr.CodeBadRequest, "add_node requires a node", ctx)
			}
			node := op.Node.Clone()
			if node.ID == "" {
				node.ID = op.NodeID
			}
			if op.NodeID != "" && node.ID != op.NodeID {
				return nil, bizerr.New(bizerr.CodeBadRequest, "add_node node_id does not match node.id", ctx)
			}
			if node.ID == "" {
				return nil, bizerr.New(bizerr.CodeBadRequest, "add_node requires an operator id", ctx)
			}
			if _, exists := nodes[node.ID]; exists {
				return nil, bizerr.New(bizerr.CodePlanInvalid, "add_node references an existing operator id",
					map[string]any{"operator_id": node.ID, "operation_index": opIndex})
			}
			nodes[node.ID] = node
			changed[node.ID] = struct{}{}
			delete(deleted, node.ID)
			if op.ParentID != "" {
				parent, err := cloneForEdit(op.ParentID)
				if err != nil {
					return nil, err
				}
				if containsID(parent.Children, node.ID) {
					return nil, bizerr.New(bizerr.CodePlanInvalid, "new operator is already attached to its parent",
						map[string]any{"operator_id": node.ID, "parent_id": op.ParentID})
				}
				parent.Children = insertChild(parent.Children, op.Position, node.ID)
			}
		case "delete_node":
			kinds = AllRebuildKinds()
			if op.NodeID == "" {
				return nil, bizerr.New(bizerr.CodeBadRequest, "delete_node requires node_id", ctx)
			}
			node, ok := nodes[op.NodeID]
			if !ok {
				return nil, bizerr.New(bizerr.CodePlanInvalid, "delete_node references missing operator", ctx)
			}
			replacement := append([]string(nil), node.Children...)
			delete(nodes, op.NodeID)
			deleted[op.NodeID] = struct{}{}
			delete(changed, op.NodeID)
			for parentID, parent := range nodes {
				if !containsID(parent.Children, op.NodeID) {
					continue
				}
				cp, err := cloneForEdit(parentID)
				if err != nil {
					return nil, err
				}
				cp.Children = replaceChild(cp.Children, op.NodeID, replacement)
			}
		case "set_predicate":
			kinds.PredicateIndex = true
			node, err := cloneForEdit(op.NodeID)
			if err != nil {
				return nil, err
			}
			node.Predicate = op.Predicate.Clone()
		case "set_columns":
			kinds.ColumnIndex = true
			node, err := cloneForEdit(op.NodeID)
			if err != nil {
				return nil, err
			}
			if node.Columns == nil {
				node.Columns = []model.ColumnRef{}
			}
			node.Columns = append([]model.ColumnRef(nil), op.Columns...)
		case "set_children":
			kinds = AllRebuildKinds()
			node, err := cloneForEdit(op.NodeID)
			if err != nil {
				return nil, err
			}
			node.Children = append([]string(nil), op.Children...)
		case "set_stats":
			kinds.CostEstimate = true
			node, err := cloneForEdit(op.NodeID)
			if err != nil {
				return nil, err
			}
			node.Stats = op.Stats.Clone()
			if node.Stats != nil && node.Stats.OperatorID == "" {
				node.Stats.OperatorID = node.ID
			}
		default:
			return nil, bizerr.New(bizerr.CodeBadRequest, "unknown edit operation", ctx)
		}
	}

	ids := make([]string, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	list := make([]*model.Node, 0, len(ids))
	for _, id := range ids {
		list = append(list, nodes[id])
	}
	plan, err := model.Analyze(list)
	if err != nil {
		return nil, err
	}

	affected := affectedNodes(state.Nodes, plan, changed, deleted)
	deletedList := make([]string, 0, len(deleted))
	for id := range deleted {
		deletedList = append(deletedList, id)
	}
	sort.Strings(deletedList)
	return &PreparedEdit{Nodes: plan.Nodes, Plan: plan, Affected: affected, Deleted: deletedList, Kinds: kinds}, nil
}

func (p *PreparedEdit) BuildState(prev *State, version uint64, committedAt time.Time) (*State, error) {
	derived, err := RebuildIncremental(&prev.Derived, p.Plan, p.Affected, p.Deleted, p.Kinds)
	if err != nil {
		return nil, err
	}
	return &State{
		Version: version, Format: prev.Format, FormatVersion: prev.FormatVersion,
		Decision: prev.Decision, GeneratedAt: prev.GeneratedAt, CommittedAt: committedAt,
		Root: p.Plan.Root, Nodes: p.Nodes, Outputs: p.Plan.Outputs,
		Derived: derived, Degradations: append([]model.Degradation(nil), prev.Degradations...),
	}, nil
}

func affectedNodes(oldNodes map[string]*model.Node, plan *model.Plan, changed, deleted map[string]struct{}) []string {
	out := make(map[string]struct{})
	newParent := parentMap(plan.Nodes, plan.Root)
	oldParent := parentMap(oldNodes, "")
	addAncestors := func(id string, parents map[string]string) {
		cur := id
		for cur != "" {
			out[cur] = struct{}{}
			cur = parents[cur]
		}
	}
	for id := range changed {
		if _, exists := plan.Nodes[id]; exists {
			addAncestors(id, newParent)
		}
	}
	for id := range deleted {
		addAncestors(id, oldParent)
	}
	addAncestors(plan.Root, newParent)
	result := make([]string, 0, len(out))
	for id := range out {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

func parentMap(nodes map[string]*model.Node, root string) map[string]string {
	parents := make(map[string]string, len(nodes))
	for id, node := range nodes {
		for _, child := range node.Children {
			parents[child] = id
		}
	}
	if root != "" {
		delete(parents, root)
	}
	return parents
}

func insertChild(children []string, position *int, child string) []string {
	at := len(children)
	if position != nil {
		at = *position
		if at < 0 {
			at = 0
		}
		if at > len(children) {
			at = len(children)
		}
	}
	out := make([]string, 0, len(children)+1)
	out = append(out, children[:at]...)
	out = append(out, child)
	out = append(out, children[at:]...)
	return out
}

func replaceChild(children []string, oldChild string, replacements []string) []string {
	out := make([]string, 0, len(children)-1+len(replacements))
	for _, child := range children {
		if child == oldChild {
			out = append(out, replacements...)
		} else {
			out = append(out, child)
		}
	}
	return out
}

func containsID(ids []string, id string) bool {
	for _, candidate := range ids {
		if candidate == id {
			return true
		}
	}
	return false
}
