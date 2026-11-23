package engine

import "sort"

type Chain struct {
	Number  int      `json:"number"`
	Nodes   []string `json:"nodes"`
	Virtual bool     `json:"virtual,omitempty"`
}

type Correlation struct {
	WindowID   string   `json:"window_id"`
	Chains     []Chain  `json:"chains"`
	Orphans    []string `json:"orphans,omitempty"`
	Complexity int      `json:"complexity"`
}

// CanonicalEvents returns the events sorted by (time offset, weight desc, id).
// This order is a pure function of the input and never depends on traversal
// order or concurrency.
func CanonicalEvents(w Window) []Event {
	evs := make([]Event, len(w.Events))
	copy(evs, w.Events)
	sort.Slice(evs, func(i, j int) bool {
		if evs[i].TimeOffset != evs[j].TimeOffset {
			return evs[i].TimeOffset < evs[j].TimeOffset
		}
		if evs[i].Weight != evs[j].Weight {
			return evs[i].Weight > evs[j].Weight
		}
		return evs[i].ID < evs[j].ID
	})
	return evs
}

// Correlate builds deterministic reference chains inside one window.
// Entities come into existence through create ops; events pointing at
// non-existent subjects or objects are marked E_ORPHAN_REF and attached to
// the virtual root chain (number 0).
func Correlate(w Window) Correlation {
	evs := CanonicalEvents(w)

	exists := map[string]bool{}
	for _, e := range evs {
		if e.Op == OpCreate {
			exists[e.Subject] = true
			exists[e.Object] = true
		}
	}

	orphan := map[string]bool{}
	for _, e := range evs {
		if e.Op == OpCreate {
			continue
		}
		if !exists[e.Subject] || !exists[e.Object] {
			orphan[e.ID] = true
		}
	}

	parent := map[string]string{}
	var find func(string) string
	find = func(x string) string {
		for parent[x] != x {
			parent[x] = parent[parent[x]]
			x = parent[x]
		}
		return x
	}
	union := func(a, b string) {
		ra, rb := find(a), find(b)
		if ra == rb {
			return
		}
		if ra < rb {
			parent[rb] = ra
		} else {
			parent[ra] = rb
		}
	}

	entityOwner := map[string]string{}
	entities := map[string]bool{}
	nonOrphan := 0
	for _, e := range evs {
		if orphan[e.ID] {
			continue
		}
		nonOrphan++
		parent[e.ID] = e.ID
	}
	for _, e := range evs {
		if orphan[e.ID] {
			continue
		}
		for _, ent := range [2]string{e.Subject, e.Object} {
			entities[ent] = true
			if prev, ok := entityOwner[ent]; ok {
				union(prev, e.ID)
			} else {
				entityOwner[ent] = e.ID
			}
		}
	}

	groups := map[string][]string{}
	for _, e := range evs {
		if orphan[e.ID] {
			continue
		}
		root := find(e.ID)
		groups[root] = append(groups[root], e.ID)
	}

	roots := make([]string, 0, len(groups))
	for root := range groups {
		roots = append(roots, root)
	}
	// Canonical event order defines chain numbering: the chain whose first
	// canonical event comes first gets number 1, and so on.
	indexOf := map[string]int{}
	for i, e := range evs {
		indexOf[e.ID] = i
	}
	sort.Slice(roots, func(i, j int) bool {
		return indexOf[groups[roots[i]][0]] < indexOf[groups[roots[j]][0]]
	})

	chains := make([]Chain, 0, len(roots)+1)
	orphanIDs := make([]string, 0)
	for _, e := range evs {
		if orphan[e.ID] {
			orphanIDs = append(orphanIDs, e.ID)
		}
	}
	if len(orphanIDs) > 0 {
		chains = append(chains, Chain{Number: 0, Nodes: orphanIDs, Virtual: true})
	}
	for i, root := range roots {
		chains = append(chains, Chain{Number: i + 1, Nodes: groups[root]})
	}

	return Correlation{
		WindowID:   w.ID,
		Chains:     chains,
		Orphans:    orphanIDs,
		Complexity: nonOrphan + len(entities),
	}
}
