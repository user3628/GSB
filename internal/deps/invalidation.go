package deps

import (
	"sort"
	"strings"

	"secverify/internal/model"
)

type TriggerSource struct {
	ID    string
	Graph *Graph
}

// ComputeInvalidation walks reverse dependency edges from each trigger. The
// trace chain is stored in affected-to-trigger edge orientation so every
// affected item can be explained back to the original change.
func ComputeInvalidation(sources []TriggerSource) model.Invalidation {
	triggerSet := make(map[string]struct{}, len(sources))
	for _, source := range sources {
		if source.Graph == nil {
			continue
		}
		triggerSet[source.ID] = struct{}{}
	}
	result := model.Invalidation{}
	result.Triggers = make([]string, 0, len(triggerSet))
	for id := range triggerSet {
		result.Triggers = append(result.Triggers, id)
	}
	sort.Strings(result.Triggers)

	items := make(map[string]struct{})
	edges := make([]model.Edge, 0)
	traces := make([]model.TraceEntry, 0)
	for _, source := range sources {
		if source.Graph == nil {
			continue
		}
		_, triggerExists := source.Graph.Items[source.ID]
		if !triggerExists {
			continue
		}
		items[source.ID] = struct{}{}
		traces = append(traces, model.TraceEntry{Trigger: source.ID, ItemID: source.ID, Depth: 0, Chain: []model.Edge{}})
		type queueEntry struct {
			id    string
			depth int
			chain []model.Edge
		}
		queue := []queueEntry{{id: source.ID}}
		visited := map[string]int{source.ID: 0}
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			for _, dependent := range source.Graph.Rev[cur.id] {
				depth := cur.depth + 1
				if old, ok := visited[dependent]; ok && old <= depth {
					continue
				}
				edge := model.Edge{From: dependent, To: cur.id}
				chain := make([]model.Edge, 0, len(cur.chain)+1)
				chain = append(chain, edge)
				chain = append(chain, cur.chain...)
				visited[dependent] = depth
				queue = append(queue, queueEntry{id: dependent, depth: depth, chain: chain})
				items[dependent] = struct{}{}
				edges = append(edges, edge)
				traces = append(traces, model.TraceEntry{
					Trigger: source.ID,
					ItemID:  dependent,
					Depth:   depth,
					Chain:   append([]model.Edge(nil), chain...),
				})
			}
		}
	}
	result.Items = make([]string, 0, len(items))
	for id := range items {
		result.Items = append(result.Items, id)
	}
	sort.Strings(result.Items)
	result.Edges = model.DedupEdges(edges)
	sort.Slice(traces, func(i, j int) bool {
		if traces[i].Trigger != traces[j].Trigger {
			return traces[i].Trigger < traces[j].Trigger
		}
		if traces[i].Depth != traces[j].Depth {
			return traces[i].Depth < traces[j].Depth
		}
		if traces[i].ItemID != traces[j].ItemID {
			return traces[i].ItemID < traces[j].ItemID
		}
		return chainKey(traces[i].Chain) < chainKey(traces[j].Chain)
	})
	result.Traces = traces
	return result
}

func chainKey(chain []model.Edge) string {
	parts := make([]string, 0, len(chain))
	for _, edge := range chain {
		parts = append(parts, edge.From+"->"+edge.To)
	}
	return strings.Join(parts, ",")
}
