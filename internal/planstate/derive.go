package planstate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"example.com/logicplan/editor/internal/bizerr"
	"example.com/logicplan/editor/internal/model"
)

func BuildFull(plan *model.Plan) Derived {
	all := make([]string, 0, len(plan.Nodes))
	for id := range plan.Nodes {
		all = append(all, id)
	}
	sortStrings(all)
	derived, _ := rebuildDerived(nil, plan, all, nil, AllRebuildKinds())
	return derived
}

type RebuildKinds struct {
	TreeSummary    bool
	ColumnIndex    bool
	PredicateIndex bool
	CostEstimate   bool
}

func AllRebuildKinds() RebuildKinds {
	return RebuildKinds{TreeSummary: true, ColumnIndex: true, PredicateIndex: true, CostEstimate: true}
}

func (k RebuildKinds) Names() []string {
	out := make([]string, 0, 4)
	if k.TreeSummary {
		out = append(out, "tree_summary")
	}
	if k.ColumnIndex {
		out = append(out, "column_index")
	}
	if k.PredicateIndex {
		out = append(out, "predicate_index")
	}
	if k.CostEstimate {
		out = append(out, "cost_estimate")
	}
	return out
}

func RebuildIncremental(old *Derived, plan *model.Plan, affected, deleted []string, kinds RebuildKinds) (Derived, error) {
	return rebuildDerived(old, plan, affected, deleted, kinds)
}

func rebuildDerived(old *Derived, plan *model.Plan, affected, deleted []string, kinds RebuildKinds) (Derived, error) {
	affectedSet := make(map[string]struct{}, len(affected))
	for _, id := range affected {
		affectedSet[id] = struct{}{}
	}
	deletedSet := make(map[string]struct{}, len(deleted))
	for _, id := range deleted {
		deletedSet[id] = struct{}{}
	}
	if old != nil {
		for id := range old.TreeSummary {
			if _, alive := plan.Nodes[id]; !alive {
				deletedSet[id] = struct{}{}
			}
		}
	}
	order := postorder(plan)

	summary := make(map[string]string, len(plan.Nodes))
	if old != nil {
		for id, hash := range old.TreeSummary {
			if _, keep := plan.Nodes[id]; keep {
				summary[id] = hash
			}
		}
	}
	for _, id := range order {
		if old != nil {
			if _, ok := affectedSet[id]; !ok {
				if _, exists := summary[id]; exists {
					continue
				}
			}
		}
		node := plan.Nodes[id]
		childHashes := make([]childHash, 0, len(node.Children))
		for _, child := range node.Children {
			h, ok := summary[child]
			if !ok {
				return Derived{}, bizerr.New(bizerr.CodeInternal, "cannot rebuild tree summary because a child hash is missing",
					map[string]any{"operator_id": id, "child_id": child})
			}
			childHashes = append(childHashes, childHash{ID: child, Hash: h})
		}
		summary[id] = nodeSummary(node, childHashes)
	}
	for id := range deletedSet {
		delete(summary, id)
	}

	columnIndex := emptyColumnIndex()
	predicateIndex := emptyPredicateIndex()
	cost := emptyCost()
	if old != nil {
		columnIndex = cloneColumnIndex(old.ColumnIndex)
		predicateIndex = clonePredicateIndex(old.PredicateIndex)
		cost = cloneCost(old.CostEstimate)
	}
	if kinds.ColumnIndex || kinds.PredicateIndex || kinds.CostEstimate {
		touched := make([]string, 0, len(affectedSet)+len(deletedSet))
		for id := range affectedSet {
			touched = append(touched, id)
		}
		for id := range deletedSet {
			touched = append(touched, id)
		}
		for _, id := range touched {
			if kinds.ColumnIndex {
				removeNodeFromColumnIndex(&columnIndex, id)
			}
			if kinds.PredicateIndex {
				removeNodeFromPredicateIndex(&predicateIndex, id)
			}
			if kinds.CostEstimate {
				delete(cost.ByNode, id)
			}
		}
	}
	for _, id := range affected {
		node, exists := plan.Nodes[id]
		if !exists {
			continue
		}
		if kinds.ColumnIndex {
			for _, col := range plan.Outputs[id] {
				addIndexEntry(columnIndex.Outputs, col.Key(), id)
			}
			if node.Type != model.TypeScan {
				for _, col := range node.Columns {
					addIndexEntry(columnIndex.References, col.Key(), id)
				}
			}
		}
		if kinds.PredicateIndex && node.Predicate != nil {
			keys := make([]string, 0, len(node.Predicate.Columns))
			for _, col := range node.Predicate.Columns {
				key := col.Key()
				keys = append(keys, key)
				addIndexEntry(predicateIndex.ByColumn, key, id)
			}
			keys = uniqueSorted(keys)
			if len(keys) > 0 {
				predicateIndex.ByOperator[id] = keys
			}
		}
	}

	if kinds.CostEstimate {
		for _, id := range order {
			if old != nil {
				if _, isAffected := affectedSet[id]; !isAffected {
					if _, exists := cost.ByNode[id]; exists {
						continue
					}
				}
			}
			node := plan.Nodes[id]
			childCosts := make([]NodeCost, 0, len(node.Children))
			for _, child := range node.Children {
				cc, ok := cost.ByNode[child]
				if !ok {
					return Derived{}, bizerr.New(bizerr.CodeInternal, "cannot rebuild cost estimate because a child cost is missing",
						map[string]any{"operator_id": id, "child_id": child})
				}
				childCosts = append(childCosts, cc)
			}
			cost.ByNode[id] = estimateNode(node, childCosts)
		}
		for id := range deletedSet {
			delete(cost.ByNode, id)
		}
	}
	rootCost, ok := cost.ByNode[plan.Root]
	if !ok {
		return Derived{}, bizerr.New(bizerr.CodeInternal, "root cost estimate is missing", map[string]any{"root": plan.Root})
	}
	cost.Total = rootCost.Cost
	cost.Rows = rootCost.Rows

	return Derived{
		TreeSummary: summary, RootHash: summary[plan.Root],
		ColumnIndex: columnIndex, PredicateIndex: predicateIndex, CostEstimate: cost,
	}, nil
}

type childHash struct {
	ID   string `json:"id"`
	Hash string `json:"hash"`
}

type nodeDigest struct {
	ID        string              `json:"id"`
	Type      model.OperatorType  `json:"type"`
	Children  []childHash         `json:"children"`
	Columns   []model.ColumnRef   `json:"columns"`
	Predicate *model.Predicate    `json:"predicate,omitempty"`
	Stats     *model.StatsBinding `json:"stats,omitempty"`
}

func nodeSummary(node *model.Node, children []childHash) string {
	payload := nodeDigest{
		ID: node.ID, Type: node.Type, Children: children, Columns: node.Columns,
		Predicate: node.Predicate, Stats: node.Stats,
	}
	b, _ := json.Marshal(payload)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func postorder(plan *model.Plan) []string {
	out := make([]string, 0, len(plan.Nodes))
	seen := make(map[string]bool, len(plan.Nodes))
	var visit func(string)
	visit = func(id string) {
		if seen[id] {
			return
		}
		seen[id] = true
		for _, child := range plan.Nodes[id].Children {
			visit(child)
		}
		out = append(out, id)
	}
	visit(plan.Root)
	return out
}

func estimateNode(node *model.Node, children []NodeCost) NodeCost {
	rows := 1.0
	cost := 1.0
	statsRows := 0.0
	if node.Stats != nil && node.Stats.RowCount > 0 {
		statsRows = float64(node.Stats.RowCount)
		rows = statsRows
	}
	switch node.Type {
	case model.TypeScan:
		cost = rows
	case model.TypeFilter:
		child := children[0]
		rows = math.Max(1, child.Rows*0.5)
		cost = child.Cost + child.Rows
	case model.TypeProject:
		child := children[0]
		rows = child.Rows
		cost = child.Cost + child.Rows*0.1
	case model.TypeJoin:
		left, right := children[0], children[1]
		rows = math.Max(1, left.Rows*right.Rows*0.1)
		cost = left.Cost + right.Cost + left.Rows*right.Rows
		for i := 2; i < len(children); i++ {
			next := children[i]
			rows = math.Max(1, rows*next.Rows*0.1)
			cost += next.Cost + rows*next.Rows
		}
	case model.TypeAggregate:
		child := children[0]
		rows = math.Max(1, child.Rows*0.1)
		cost = child.Cost + child.Rows*1.5
	case model.TypeSort:
		child := children[0]
		rows = child.Rows
		cost = child.Cost + child.Rows*math.Log2(child.Rows+1)
	}
	if statsRows > 0 {
		rows = statsRows
	}
	return NodeCost{Cost: cost, Rows: rows}
}

func emptyColumnIndex() ColumnIndex {
	return ColumnIndex{References: map[string][]string{}, Outputs: map[string][]string{}}
}

func emptyPredicateIndex() PredicateIndex {
	return PredicateIndex{ByOperator: map[string][]string{}, ByColumn: map[string][]string{}}
}

func emptyCost() CostEstimate {
	return CostEstimate{ByNode: map[string]NodeCost{}}
}

func cloneColumnIndex(in ColumnIndex) ColumnIndex {
	return ColumnIndex{References: cloneStringSliceMap(in.References), Outputs: cloneStringSliceMap(in.Outputs)}
}

func clonePredicateIndex(in PredicateIndex) PredicateIndex {
	return PredicateIndex{ByOperator: cloneStringSliceMap(in.ByOperator), ByColumn: cloneStringSliceMap(in.ByColumn)}
}

func cloneCost(in CostEstimate) CostEstimate {
	out := emptyCost()
	out.Total, out.Rows = in.Total, in.Rows
	for k, v := range in.ByNode {
		out.ByNode[k] = v
	}
	return out
}

func cloneStringSliceMap(in map[string][]string) map[string][]string {
	out := make(map[string][]string, len(in))
	for k, v := range in {
		out[k] = append([]string(nil), v...)
	}
	return out
}

func addIndexEntry(m map[string][]string, key, nodeID string) {
	if key == "" {
		return
	}
	m[key] = uniqueSorted(append(m[key], nodeID))
}

func removeNodeFromColumnIndex(index *ColumnIndex, nodeID string) {
	removeNodeFromMap(index.References, nodeID)
	removeNodeFromMap(index.Outputs, nodeID)
}

func removeNodeFromPredicateIndex(index *PredicateIndex, nodeID string) {
	delete(index.ByOperator, nodeID)
	removeNodeFromMap(index.ByColumn, nodeID)
}

func removeNodeFromMap(m map[string][]string, nodeID string) {
	for key, ids := range m {
		filtered := ids[:0]
		for _, id := range ids {
			if id != nodeID {
				filtered = append(filtered, id)
			}
		}
		if len(filtered) == 0 {
			delete(m, key)
			continue
		}
		m[key] = filtered
	}
}

func uniqueSorted(in []string) []string {
	if len(in) == 0 {
		return []string{}
	}
	out := append([]string(nil), in...)
	sort.Strings(out)
	j := 0
	for i := 0; i < len(out); i++ {
		if i == 0 || out[i] != out[j-1] {
			out[j] = out[i]
			j++
		}
	}
	return out[:j]
}

func sortStrings(in []string) { sort.Strings(in) }

func DerivedInvariantError(state *State) error {
	if state == nil {
		return nil
	}
	if state.Derived.RootHash == "" {
		return fmt.Errorf("root tree hash is empty")
	}
	if state.Derived.TreeSummary[state.Root] != state.Derived.RootHash {
		return fmt.Errorf("root hash does not match tree summary")
	}
	for id, node := range state.Nodes {
		if _, ok := state.Derived.TreeSummary[id]; !ok {
			return fmt.Errorf("operator %q has no tree summary", id)
		}
		if _, ok := state.Derived.CostEstimate.ByNode[id]; !ok {
			return fmt.Errorf("operator %q has no cost estimate", id)
		}
		for _, col := range state.Outputs[id] {
			if !indexContains(state.Derived.ColumnIndex.Outputs[col.Key()], id) {
				return fmt.Errorf("output index %q is missing operator %q", col.Key(), id)
			}
		}
		if node.Type != model.TypeScan {
			for _, col := range node.Columns {
				if !indexContains(state.Derived.ColumnIndex.References[col.Key()], id) {
					return fmt.Errorf("column index %q is missing operator %q", col.Key(), id)
				}
			}
		}
		if node.Predicate != nil {
			for _, col := range node.Predicate.Columns {
				if !indexContains(state.Derived.PredicateIndex.ByColumn[col.Key()], id) {
					return fmt.Errorf("predicate index %q is missing operator %q", col.Key(), id)
				}
			}
		}
	}
	for key, ids := range state.Derived.ColumnIndex.References {
		for _, id := range ids {
			node, ok := state.Nodes[id]
			if !ok {
				return fmt.Errorf("column index %q references missing operator %q", key, id)
			}
			if !hasColumn(node.Columns, key) {
				return fmt.Errorf("column index %q references operator %q which no longer has that column", key, id)
			}
		}
	}
	for key, ids := range state.Derived.ColumnIndex.Outputs {
		for _, id := range ids {
			if _, ok := state.Nodes[id]; !ok {
				return fmt.Errorf("output index %q references missing operator %q", key, id)
			}
			if !hasColumn(state.Outputs[id], key) {
				return fmt.Errorf("output index %q references operator %q which no longer outputs that column", key, id)
			}
		}
	}
	for key, ids := range state.Derived.PredicateIndex.ByColumn {
		for _, id := range ids {
			node, ok := state.Nodes[id]
			if !ok {
				return fmt.Errorf("predicate index %q references missing operator %q", key, id)
			}
			if node.Predicate == nil || !hasColumn(node.Predicate.Columns, key) {
				return fmt.Errorf("predicate index %q references operator %q without that predicate column", key, id)
			}
		}
	}
	for id := range state.Derived.PredicateIndex.ByOperator {
		if _, ok := state.Nodes[id]; !ok {
			return fmt.Errorf("predicate operator index references missing operator %q", id)
		}
	}
	rootCost, ok := state.Derived.CostEstimate.ByNode[state.Root]
	if !ok || state.Derived.CostEstimate.Total != rootCost.Cost || state.Derived.CostEstimate.Rows != rootCost.Rows {
		return fmt.Errorf("root cost summary does not match per-node estimate")
	}
	return nil
}

func indexContains(ids []string, want string) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func hasColumn(cols []model.ColumnRef, key string) bool {
	for _, col := range cols {
		if col.Key() == key {
			return true
		}
	}
	return false
}
