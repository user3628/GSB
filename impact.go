package configimpact

import (
	"bytes"
	"sort"
)

func computeImpact(oldDoc, newDoc *Document, changes []PatchChange) ImpactReport {
	changedKeys := sortedChangedKeys(changes)
	directDependents := buildDirectDependents(newDoc)

	seeds := make(map[string]map[string]struct{})
	for _, key := range changedKeys {
		for _, component := range newDoc.Components {
			if containsString(component.Watches, key) {
				addSeed(seeds, component.ID, key)
			}
		}
		for _, group := range newDoc.Groups {
			oldGroup := findGroup(oldDoc, group.ID)
			if declarationChanged(oldGroupDefaults(oldGroup), group.Defaults, key) {
				for _, component := range newDoc.Components {
					if component.Group == group.ID {
						addSeed(seeds, component.ID, key)
					}
				}
			}
		}
		for _, component := range newDoc.Components {
			oldComponent := findComponent(oldDoc, component.ID)
			if declarationChanged(oldComponentConfig(oldComponent), component.Config, key) {
				addSeed(seeds, component.ID, key)
			}
		}
	}

	seedIDs := make([]string, 0, len(seeds))
	for id := range seeds {
		seedIDs = append(seedIDs, id)
	}
	sort.Strings(seedIDs)

	downstream := make(map[string][]string, len(seedIDs))
	allReachable := make(map[string][]string, len(seedIDs))
	for _, seed := range seedIDs {
		reachable := reachableDependents(directDependents, seed)
		allReachable[seed] = reachable
		if len(reachable) > 0 {
			downstream[seed] = append([]string(nil), reachable...)
		} else {
			downstream[seed] = []string{}
		}
	}

	type frontierState struct {
		hops     int
		triggers map[string]struct{}
		causes   map[string]struct{}
	}
	states := make(map[string]*frontierState, len(seeds))
	queue := make([]string, 0, len(seeds))
	for _, seed := range seedIDs {
		state := &frontierState{hops: 0, triggers: map[string]struct{}{}, causes: map[string]struct{}{seed: {}}}
		for key := range seeds[seed] {
			state.triggers[key] = struct{}{}
		}
		states[seed] = state
		queue = append(queue, seed)
	}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		currentState := states[current]
		for _, dependent := range directDependents[current] {
			nextHops := currentState.hops + 1
			state := states[dependent]
			changed := false
			if state == nil {
				state = &frontierState{hops: nextHops, triggers: map[string]struct{}{}, causes: map[string]struct{}{}}
				states[dependent] = state
				changed = true
			} else if nextHops < state.hops {
				state.hops = nextHops
				changed = true
			}
			for key := range currentState.triggers {
				if _, exists := state.triggers[key]; !exists {
					state.triggers[key] = struct{}{}
					changed = true
				}
			}
			for cause := range currentState.causes {
				if _, exists := state.causes[cause]; !exists {
					state.causes[cause] = struct{}{}
					changed = true
				}
			}
			if changed {
				queue = append(queue, dependent)
			}
		}
	}

	// Every affected component exposes its own reachable downstream set, not
	// only the original seed components.
	for componentID := range states {
		allReachable[componentID] = reachableDependents(directDependents, componentID)
	}
	affected := make([]AffectedComponent, 0, len(states))
	for componentID, state := range states {
		affected = append(affected, AffectedComponent{
			ComponentID: componentID,
			TriggerKeys: sortedSet(state.triggers),
			CauseSeeds:  sortedSet(state.causes),
			Hops:        state.hops,
			Downstream:  append([]string{}, allReachable[componentID]...),
		})
	}
	sort.Slice(affected, func(i, j int) bool { return affected[i].ComponentID < affected[j].ComponentID })
	return ImpactReport{AffectedComponents: affected, Downstream: downstream}
}

func sortedChangedKeys(changes []PatchChange) []string {
	set := make(map[string]struct{}, len(changes))
	for _, change := range changes {
		set[change.Key] = struct{}{}
	}
	return sortedSet(set)
}

func addSeed(seeds map[string]map[string]struct{}, componentID, key string) {
	keys := seeds[componentID]
	if keys == nil {
		keys = make(map[string]struct{})
		seeds[componentID] = keys
	}
	keys[key] = struct{}{}
}

func buildDirectDependents(doc *Document) map[string][]string {
	dependents := make(map[string][]string, len(doc.Components))
	for _, component := range doc.Components {
		dependents[component.ID] = []string{}
	}
	for _, component := range doc.Components {
		for _, dependency := range component.DependsOn {
			dependents[dependency] = append(dependents[dependency], component.ID)
		}
	}
	for id := range dependents {
		sort.Strings(dependents[id])
		dependents[id] = deduplicateSorted(dependents[id])
	}
	return dependents
}

func reachableDependents(direct map[string][]string, seed string) []string {
	seen := make(map[string]struct{})
	queue := append([]string(nil), direct[seed]...)
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if current == seed {
			continue
		}
		if _, exists := seen[current]; exists {
			continue
		}
		seen[current] = struct{}{}
		queue = append(queue, direct[current]...)
	}
	result := make([]string, 0, len(seen))
	for id := range seen {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

func declarationChanged(oldKeys, newKeys []ConfigKey, key string) bool {
	oldKey, newKey := findKey(oldKeys, key), findKey(newKeys, key)
	if oldKey == nil || newKey == nil {
		return oldKey != nil || newKey != nil
	}
	return oldKey.Type != newKey.Type || oldKey.Order != newKey.Order || !bytes.Equal(oldKey.Value, newKey.Value)
}

func oldGroupDefaults(group *Group) []ConfigKey {
	if group == nil {
		return nil
	}
	return group.Defaults
}

func oldComponentConfig(component *Component) []ConfigKey {
	if component == nil {
		return nil
	}
	return component.Config
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func sortedSet(set map[string]struct{}) []string {
	values := make([]string, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

func deduplicateSorted(values []string) []string {
	if len(values) < 2 {
		return values
	}
	out := values[:1]
	for _, value := range values[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}
