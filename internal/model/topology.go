package model

import "sort"

// Topology is an immutable directed dependency graph. An edge s -> u means s
// depends on upstream u. Traversal is deterministic: neighbors are always
// visited in lexicographic order.
type Topology struct {
	upstream   map[string][]string
	downstream map[string][]string
	zones      map[string]string
}

// NewTopology builds a Topology from normalized services. Upstream lists are
// copied and sorted so traversal order never depends on input ordering.
func NewTopology(services []Service) *Topology {
	t := &Topology{
		upstream:   make(map[string][]string, len(services)),
		downstream: make(map[string][]string),
		zones:      make(map[string]string, len(services)),
	}
	for _, s := range services {
		ups := append([]string(nil), s.Upstreams...)
		sort.Strings(ups)
		t.upstream[s.ID] = ups
		t.zones[s.ID] = s.Zone
	}
	for id, ups := range t.upstream {
		for _, u := range ups {
			t.downstream[u] = append(t.downstream[u], id)
		}
	}
	for id := range t.downstream {
		sort.Strings(t.downstream[id])
	}
	return t
}

// Zone returns the zone of a service ("" if unknown).
func (t *Topology) Zone(id string) string { return t.zones[id] }

// Upstreams returns the sorted upstream list.
func (t *Topology) Upstreams(id string) []string { return append([]string(nil), t.upstream[id]...) }

// HopsFrom returns the minimum dependency-hop distance from src to every
// reachable service. src itself maps to 0. Edges follow upstream dependencies.
// BFS expansion is lexicographic, giving reproducible traversal.
func (t *Topology) HopsFrom(src string) map[string]int {
	dist := map[string]int{src: 0}
	queue := []string{src}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, up := range t.upstream[cur] {
			if _, ok := dist[up]; !ok {
				dist[up] = dist[cur] + 1
				queue = append(queue, up)
			}
		}
	}
	return dist
}

// DownstreamOf returns services that (transitively) depend on id.
func (t *Topology) DownstreamOf(id string) []string {
	seen := map[string]bool{id: true}
	var out []string
	queue := []string{id}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, d := range t.downstream[cur] {
			if !seen[d] {
				seen[d] = true
				out = append(out, d)
				queue = append(queue, d)
			}
		}
	}
	sort.Strings(out)
	return out
}

// ServicesAtHop returns reachable services exactly n hops from src, sorted.
func (t *Topology) ServicesAtHop(src string, hop int) []string {
	dist := t.HopsFrom(src)
	var out []string
	for id, h := range dist {
		if h == hop {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}
