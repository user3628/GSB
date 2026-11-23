package deps

import (
	"fmt"
	"sort"
	"strings"

	"secverify/internal/model"
)

type Graph struct {
	Items map[string]model.Item
	Out   map[string][]string
	Rev   map[string][]string
	Edges []model.Edge
}

func Build(items []model.Item) *Graph {
	g := &Graph{
		Items: make(map[string]model.Item, len(items)),
		Out:   make(map[string][]string, len(items)),
		Rev:   make(map[string][]string, len(items)),
	}
	for _, item := range items {
		g.Items[item.ID] = model.CloneItem(item)
	}
	for _, id := range model.SortedIDs(g.Items) {
		item := g.Items[id]
		for _, ref := range model.DependencyIDs(item) {
			edge := model.Edge{From: id, To: ref}
			g.Edges = append(g.Edges, edge)
			g.Out[id] = append(g.Out[id], ref)
			g.Rev[ref] = append(g.Rev[ref], id)
		}
	}
	g.Edges = model.DedupEdges(g.Edges)
	for id := range g.Out {
		sort.Strings(g.Out[id])
	}
	for id := range g.Rev {
		sort.Strings(g.Rev[id])
	}
	return g
}

// Validate checks missing references first, then cycles. Cycle output is the
// globally shortest cycle, with lexical tie-breaking, which yields a minimal
// participating item set and deterministic edge sequence.
func (g *Graph) Validate() *model.BizError {
	missing := make([]model.Edge, 0)
	missingItems := make(map[string]struct{})
	firstMissing := ""
	firstOffset := 0
	for _, edge := range g.Edges {
		if _, ok := g.Items[edge.To]; ok {
			continue
		}
		missing = append(missing, edge)
		missingItems[edge.From] = struct{}{}
		missingItems[edge.To] = struct{}{}
		if firstMissing == "" || edge.From < firstMissing {
			firstMissing = edge.From
			firstOffset = g.Items[edge.From].SourceOffset
		}
	}
	if len(missing) > 0 {
		items := make([]string, 0, len(missingItems))
		for id := range missingItems {
			items = append(items, id)
		}
		sort.Strings(items)
		return &model.BizError{
			Code:            model.CodeDepMissing,
			ItemID:          firstMissing,
			SourceOffset:    firstOffset,
			Message:         "referenced item does not exist",
			Items:           items,
			Edges:           model.DedupEdges(missing),
			FallbackVersion: -1,
		}
	}
	if cycle, ok := g.minimalCycle(); ok {
		nodes := append([]string(nil), cycle...)
		sort.Strings(nodes)
		edges := make([]model.Edge, 0, len(cycle))
		for i, from := range cycle {
			to := cycle[(i+1)%len(cycle)]
			edges = append(edges, model.Edge{From: from, To: to})
		}
		return &model.BizError{
			Code:            model.CodeDepCycle,
			ItemID:          cycle[0],
			SourceOffset:    g.Items[cycle[0]].SourceOffset,
			Message:         "dependency cycle detected",
			Items:           nodes,
			Edges:           edges,
			FallbackVersion: -1,
		}
	}
	return nil
}

type cycleCandidate struct {
	nodes []string
	edges []model.Edge
	key   string
}

func (g *Graph) minimalCycle() ([]string, bool) {
	comps := g.stronglyConnected()
	var best *cycleCandidate
	for _, comp := range comps {
		member := make(map[string]struct{}, len(comp))
		for _, id := range comp {
			member[id] = struct{}{}
		}
		if len(comp) == 1 {
			id := comp[0]
			if !contains(g.Out[id], id) {
				continue
			}
		}
		for _, start := range comp {
			candidate, ok := g.shortestCycleThrough(start, member)
			if !ok {
				continue
			}
			if best == nil || betterCycle(candidate, *best) {
				copyCandidate := candidate
				best = &copyCandidate
			}
		}
	}
	if best == nil {
		return nil, false
	}
	return best.nodes, true
}

func (g *Graph) shortestCycleThrough(start string, member map[string]struct{}) (cycleCandidate, bool) {
	if contains(g.Out[start], start) {
		return cycleCandidate{
			nodes: []string{start},
			edges: []model.Edge{{From: start, To: start}},
			key:   start,
		}, true
	}
	type queueNode struct {
		id       string
		distance int
		path     []string
	}
	queue := []queueNode{{id: start, distance: 0, path: []string{start}}}
	visited := map[string]int{start: 0}
	bestEnd := ""
	bestPath := []string(nil)
	bestLen := int(^uint(0) >> 1)
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, next := range g.Out[cur.id] {
			if _, ok := member[next]; !ok {
				continue
			}
			if next == start {
				length := len(cur.path)
				if length < bestLen || (length == bestLen && lexPathLess(cur.path, bestPath)) {
					bestLen = length
					bestPath = append([]string(nil), cur.path...)
					bestEnd = cur.id
				}
				continue
			}
			distance := cur.distance + 1
			if old, seen := visited[next]; seen && old <= distance {
				continue
			}
			visited[next] = distance
			path := append(append([]string(nil), cur.path...), next)
			queue = append(queue, queueNode{id: next, distance: distance, path: path})
		}
	}
	if bestEnd == "" {
		return cycleCandidate{}, false
	}
	edges := make([]model.Edge, 0, len(bestPath))
	for i := 0; i < len(bestPath); i++ {
		to := bestPath[(i+1)%len(bestPath)]
		edges = append(edges, model.Edge{From: bestPath[i], To: to})
	}
	return cycleCandidate{nodes: bestPath, edges: edges, key: strings.Join(bestPath, "\x00")}, true
}

func betterCycle(a, b cycleCandidate) bool {
	if len(a.nodes) != len(b.nodes) {
		return len(a.nodes) < len(b.nodes)
	}
	return a.key < b.key
}

func lexPathLess(a, b []string) bool {
	if b == nil {
		return true
	}
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func (g *Graph) stronglyConnected() [][]string {
	ids := model.SortedIDs(g.Items)
	index := 0
	indices := make(map[string]int, len(ids))
	low := make(map[string]int, len(ids))
	onStack := make(map[string]bool, len(ids))
	stack := make([]string, 0, len(ids))
	components := make([][]string, 0)
	var visit func(string)
	visit = func(v string) {
		indices[v] = index
		low[v] = index
		index++
		stack = append(stack, v)
		onStack[v] = true
		for _, w := range g.Out[v] {
			if _, ok := g.Items[w]; !ok {
				continue
			}
			if _, seen := indices[w]; !seen {
				visit(w)
				if low[w] < low[v] {
					low[v] = low[w]
				}
			} else if onStack[w] && indices[w] < low[v] {
				low[v] = indices[w]
			}
		}
		if low[v] == indices[v] {
			comp := make([]string, 0)
			for {
				w := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[w] = false
				comp = append(comp, w)
				if w == v {
					break
				}
			}
			sort.Strings(comp)
			components = append(components, comp)
		}
	}
	for _, id := range ids {
		if _, seen := indices[id]; !seen {
			visit(id)
		}
	}
	return components
}

func (g *Graph) String() string {
	parts := make([]string, 0, len(g.Edges))
	for _, edge := range g.Edges {
		parts = append(parts, fmt.Sprintf("%s->%s", edge.From, edge.To))
	}
	return strings.Join(parts, ",")
}
