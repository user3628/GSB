package engine

import (
	"encoding/json"
	"sort"

	"configtree/internal/pathtree"
	"configtree/internal/spec"
)

// Source describes where an effective config entry originates.
type Source struct {
	Kind        string `json:"kind"` // self | upstream
	ComponentID string `json:"component_id"`
	Order       int64  `json:"order"`
	Priority    int    `json:"priority"`
	Prefix      string `json:"prefix"`
	Hops        int    `json:"hops"`
}

// Conflict records one losing candidate for a key in a component view.
type Conflict struct {
	KeyPath    string `json:"key_path"`
	Winner     Source `json:"winner"`
	Loser      Source `json:"loser"`
	Resolution string `json:"resolution"`
}

// ViewEntry is one adjudicated effective configuration entry.
type ViewEntry struct {
	KeyPath string          `json:"key_path"`
	Type    string          `json:"type"`
	Value   json.RawMessage `json:"value"`
	Source  Source          `json:"source"`
}

// ComponentView is the effective configuration view of one component.
type ComponentView struct {
	ComponentID string      `json:"component_id"`
	Version     int64       `json:"version"`
	Entries     []ViewEntry `json:"entries"`
	Conflicts   []Conflict  `json:"conflicts"`
	Digest      string      `json:"digest"`
}

// AffectedNode describes one component in an affected-component set.
type AffectedNode struct {
	ComponentID string   `json:"component_id"`
	TriggerKeys []string `json:"trigger_keys"`
	PropPath    []string `json:"prop_path"`
	PropHops    int      `json:"prop_hops"`
	Direct      bool     `json:"direct"`
}

// AffectedSet is the per-commit affected component collection.
type AffectedSet struct {
	Version     int64          `json:"version"`
	Nodes       []AffectedNode `json:"nodes"`
	Unconverged bool           `json:"unconverged"`
}

// materializedState is the in-memory committed state used for derivation.
type materializedState struct {
	version    int64
	items      map[string]spec.ConfigItem
	components map[string]spec.Component
}

// beats reports whether candidate a wins over b under the adjudication order:
// effective order number, then explicit source priority, then deterministic
// source identity.
func beats(a, b Source) (bool, string) {
	if a.Order != b.Order {
		return a.Order > b.Order, "order"
	}
	if a.Priority != b.Priority {
		return a.Priority > b.Priority, "source_priority"
	}
	if a.Kind != b.Kind {
		return a.Kind < b.Kind, "source_kind"
	}
	return a.ComponentID < b.ComponentID, "source_identity"
}

// buildTries constructs one prefix trie per component.
func buildTries(components map[string]spec.Component) map[string]*pathtree.Trie {
	tries := make(map[string]*pathtree.Trie, len(components))
	for id, c := range components {
		t := pathtree.New()
		for _, p := range c.SubPrefixes {
			t.Insert(p)
		}
		tries[id] = t
	}
	return tries
}

// computeViews runs prefix matching and source adjudication for every
// component. Results are deterministic regardless of map iteration order.
func computeViews(st *materializedState) map[string]*ComponentView {
	tries := buildTries(st.components)
	keys := make([]string, 0, len(st.items))
	for k := range st.items {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	views := make(map[string]*ComponentView, len(st.components))
	ids := make([]string, 0, len(st.components))
	for id := range st.components {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		c := st.components[id]
		type candidate struct {
			item   spec.ConfigItem
			source Source
		}
		byKey := map[string][]candidate{}

		addCandidates := func(sourceComp spec.Component, kind string, priority int) {
			trie := tries[sourceComp.ID]
			for _, key := range keys {
				item := st.items[key]
				prefix, hops, matched := trie.Match(key)
				if !matched {
					if !sourceComp.Wildcard {
						continue
					}
					prefix = "*"
					hops = segmentCount(key)
				}
				byKey[key] = append(byKey[key], candidate{
					item: item,
					source: Source{
						Kind:        kind,
						ComponentID: sourceComp.ID,
						Order:       item.Order,
						Priority:    priority,
						Prefix:      prefix,
						Hops:        hops,
					},
				})
			}
		}

		// Self subscription.
		addCandidates(c, "self", sourcePrioritySelf)
		// Upstream components' subscriptions, pulled through dependency edges.
		for _, depID := range c.DependsOn {
			up, ok := st.components[depID]
			if !ok {
				continue
			}
			addCandidates(up, "upstream", sourcePriorityUpstream)
		}

		view := &ComponentView{ComponentID: id, Version: st.version}
		matchedKeys := make([]string, 0, len(byKey))
		for k := range byKey {
			matchedKeys = append(matchedKeys, k)
		}
		sort.Strings(matchedKeys)
		for _, key := range matchedKeys {
			cands := byKey[key]
			winner := cands[0]
			var losers []candidate
			for _, cand := range cands[1:] {
				ok, resolution := beats(cand.source, winner.source)
				if ok {
					losers = append(losers, winner)
					view.Conflicts = append(view.Conflicts, Conflict{
						KeyPath:    key,
						Winner:     cand.source,
						Loser:      winner.source,
						Resolution: resolution,
					})
					winner = cand
					continue
				}
				losers = append(losers, cand)
				view.Conflicts = append(view.Conflicts, Conflict{
					KeyPath:    key,
					Winner:     winner.source,
					Loser:      cand.source,
					Resolution: resolution,
				})
			}
			view.Entries = append(view.Entries, ViewEntry{
				KeyPath: winner.item.KeyPath,
				Type:    winner.item.Type,
				Value:   winner.item.Value,
				Source:  winner.source,
			})
		}
		sort.Slice(view.Conflicts, func(i, j int) bool {
			if view.Conflicts[i].KeyPath != view.Conflicts[j].KeyPath {
				return view.Conflicts[i].KeyPath < view.Conflicts[j].KeyPath
			}
			return view.Conflicts[i].Loser.ComponentID < view.Conflicts[j].Loser.ComponentID
		})
		view.Digest = viewDigest(view)
		views[id] = view
	}
	return views
}

func segmentCount(key string) int {
	n := 1
	for _, r := range key {
		if r == '.' {
			n++
		}
	}
	return n
}
