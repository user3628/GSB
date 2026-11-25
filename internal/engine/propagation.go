package engine

import (
	"sort"

	"configtree/internal/pathtree"
	"configtree/internal/spec"
)

// downstreamMap inverts dependency edges: for each upstream component id it
// lists the components that directly depend on it.
func downstreamMap(components map[string]spec.Component) map[string][]string {
	down := make(map[string][]string, len(components))
	for id, c := range components {
		for _, dep := range c.DependsOn {
			down[dep] = append(down[dep], id)
		}
	}
	for id := range down {
		sort.Strings(down[id])
	}
	return down
}

// directlySubscribed returns component ids whose own subscription (or
// wildcard) matches any of the changed keys.
func directlySubscribed(changedKeys []string, components map[string]spec.Component, tries map[string]*pathtree.Trie) []string {
	hit := map[string]bool{}
	keys := append([]string(nil), changedKeys...)
	sort.Strings(keys)
	for id, c := range components {
		for _, key := range keys {
			if c.Wildcard {
				hit[id] = true
				break
			}
			if _, _, ok := tries[id].Match(key); ok {
				hit[id] = true
				break
			}
		}
	}
	out := make([]string, 0, len(hit))
	for id := range hit {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// computeAffected expands the directly-hit components across all downstream
// dependency edges, recording trigger keys, the propagation path and hop count.
func computeAffected(st *materializedState, changedKeys []string, unconverged bool) *AffectedSet {
	tries := buildTries(st.components)
	direct := directlySubscribed(changedKeys, st.components, tries)
	down := downstreamMap(st.components)

	keysSorted := append([]string(nil), changedKeys...)
	sort.Strings(keysSorted)

	nodesByID := map[string]*AffectedNode{}
	queue := make([]AffectedNode, 0, len(direct))
	for _, id := range direct {
		node := AffectedNode{
			ComponentID: id,
			TriggerKeys: append([]string(nil), keysSorted...),
			PropPath:    []string{id},
			PropHops:    0,
			Direct:      true,
		}
		queue = append(queue, node)
		nodesByID[id] = &node
	}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, next := range down[cur.ComponentID] {
			path := append(append([]string(nil), cur.PropPath...), next)
			if existing, ok := nodesByID[next]; ok {
				// Keep the shortest propagation path and merge trigger keys.
				if cur.PropHops+1 < existing.PropHops || !existing.Direct {
					existing.PropPath = path
					existing.PropHops = cur.PropHops + 1
				}
				existing.TriggerKeys = mergeStrings(existing.TriggerKeys, cur.TriggerKeys)
				continue
			}
			node := AffectedNode{
				ComponentID: next,
				TriggerKeys: append([]string(nil), cur.TriggerKeys...),
				PropPath:    path,
				PropHops:    cur.PropHops + 1,
				Direct:      false,
			}
			nodesByID[next] = &node
			queue = append(queue, node)
		}
	}

	ids := make([]string, 0, len(nodesByID))
	for id := range nodesByID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	nodes := make([]AffectedNode, 0, len(ids))
	for _, id := range ids {
		node := nodesByID[id]
		sort.Strings(node.TriggerKeys)
		node.TriggerKeys = dedupeStrings(node.TriggerKeys)
		nodes = append(nodes, *node)
	}
	return &AffectedSet{Version: st.version, Nodes: nodes, Unconverged: unconverged}
}

func mergeStrings(a, b []string) []string {
	return dedupeStrings(append(append([]string(nil), a...), b...))
}

func dedupeStrings(in []string) []string {
	seen := map[string]bool{}
	out := in[:0:0]
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}
