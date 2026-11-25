package engine

import (
	"encoding/json"
	"sort"

	"configtree/internal/spec"
)

// validateDelta validates a delta against the materialized base state.
func validateDelta(st *materializedState, d *spec.Delta) error {
	if d.BaseVersion != st.version {
		return spec.NewError(spec.CodeStaleBase, "delta base version is not the current version",
			map[string]any{"base_version": d.BaseVersion, "current_version": st.version})
	}
	if d.TargetVersion <= st.version {
		return spec.NewError(spec.CodeStaleBase, "delta target version must be greater than the base version",
			map[string]any{"target_version": d.TargetVersion, "current_version": st.version})
	}
	changedComps := map[string]bool{}
	for _, ch := range d.Subscriptions {
		if ch.Op != "replace" {
			return invalid("unsupported subscription op", map[string]any{"op": ch.Op})
		}
		if _, ok := st.components[ch.Component.ID]; !ok {
			return invalid("subscription change references unknown component",
				map[string]any{"component": ch.Component.ID})
		}
		changedComps[ch.Component.ID] = true
	}
	for _, ch := range d.Items {
		if ch.Op != "upsert" && ch.Op != "delete" {
			return invalid("unsupported item op", map[string]any{"op": ch.Op})
		}
		if err := spec.ValidateKeyPath(ch.Item.KeyPath); err != nil {
			return invalid(err.Error(), map[string]any{"key_path": ch.Item.KeyPath})
		}
		if ch.Op == "delete" {
			continue
		}
		if !mapAllowedType(ch.Item.Type) {
			return invalid("unsupported config item type", map[string]any{
				"key_path": ch.Item.KeyPath, "type": ch.Item.Type,
			})
		}
		if !json.Valid(ch.Item.Value) {
			return invalid("config item value is not valid JSON",
				map[string]any{"key_path": ch.Item.KeyPath})
		}
	}
	// Apply tentatively on a copy and run the full component validation so a
	// delta cannot leave the batch in an invalid state.
	next := cloneState(st)
	applyDeltaToState(next, d)
	components := make([]spec.Component, 0, len(next.components))
	for _, c := range next.components {
		components = append(components, c)
	}
	declared := make(map[string]bool, len(next.items))
	for k := range next.items {
		declared[k] = true
	}
	if err := validateComponentSet(components, declared); err != nil {
		return err
	}
	return nil
}

// changedKeyPaths returns the key paths touched by a delta.
func changedKeyPaths(d *spec.Delta) []string {
	seen := map[string]bool{}
	for _, ch := range d.Items {
		seen[ch.Item.KeyPath] = true
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// applyDeltaToState mutates a materialized state with the delta.
func applyDeltaToState(st *materializedState, d *spec.Delta) {
	for _, ch := range d.Items {
		switch ch.Op {
		case "upsert":
			st.items[ch.Item.KeyPath] = ch.Item
		case "delete":
			delete(st.items, ch.Item.KeyPath)
		}
	}
	for _, ch := range d.Subscriptions {
		// Preserve map identity semantics with a fresh copy.
		c := ch.Component
		c.SubPrefixes = append([]string(nil), c.SubPrefixes...)
		c.DependsOn = append([]string(nil), c.DependsOn...)
		st.components[c.ID] = c
	}
	st.version = d.TargetVersion
}

func cloneState(st *materializedState) *materializedState {
	next := &materializedState{
		version:    st.version,
		items:      make(map[string]spec.ConfigItem, len(st.items)),
		components: make(map[string]spec.Component, len(st.components)),
	}
	for k, v := range st.items {
		next.items[k] = v
	}
	for k, v := range st.components {
		v.SubPrefixes = append([]string(nil), v.SubPrefixes...)
		v.DependsOn = append([]string(nil), v.DependsOn...)
		next.components[k] = v
	}
	return next
}

func invalid(message string, ctx map[string]any) error {
	return spec.NewError(spec.CodeSpecInvalid, message, ctx)
}

func mapAllowedType(t string) bool {
	switch t {
	case "string", "int", "float", "bool", "json":
		return true
	}
	return false
}

// validateComponentSet mirrors spec.ValidateSnapshot component checks without
// requiring a full snapshot value.
func validateComponentSet(components []spec.Component, declared map[string]bool) error {
	ids := map[string]bool{}
	for _, c := range components {
		if c.ID == "" {
			return invalid("component id is empty", nil)
		}
		if ids[c.ID] {
			return invalid("duplicate component id", map[string]any{"component": c.ID})
		}
		ids[c.ID] = true
		seen := map[string]bool{}
		for _, p := range c.SubPrefixes {
			if err := spec.ValidateKeyPath(p); err != nil {
				return invalid(err.Error(), map[string]any{"component": c.ID})
			}
			if seen[p] {
				return invalid("duplicate subscription prefix",
					map[string]any{"component": c.ID, "prefix": p})
			}
			seen[p] = true
			covered := false
			for k := range declared {
				if spec.IsPathPrefix(p, k) {
					covered = true
					break
				}
			}
			if !covered {
				return invalid("subscription prefix is not a prefix of any declared key path",
					map[string]any{"component": c.ID, "prefix": p})
			}
		}
		if c.Wildcard && len(c.SubPrefixes) != 0 {
			return invalid("wildcard component must not declare explicit prefixes",
				map[string]any{"component": c.ID})
		}
		if !c.Wildcard && len(c.SubPrefixes) == 0 {
			return invalid("component has neither subscription prefixes nor wildcard flag",
				map[string]any{"component": c.ID})
		}
	}
	for _, c := range components {
		for _, dep := range c.DependsOn {
			if dep == c.ID {
				return invalid("component depends on itself", map[string]any{"component": c.ID})
			}
			if !ids[dep] {
				return invalid("dependency references unknown component",
					map[string]any{"component": c.ID, "depends_on": dep})
			}
		}
	}
	return detectComponentCycle(components)
}

func detectComponentCycle(components []spec.Component) error {
	deps := map[string][]string{}
	for _, c := range components {
		deps[c.ID] = append([]string(nil), c.DependsOn...)
	}
	color := map[string]int{}
	var visit func(id string, stack []string) error
	visit = func(id string, stack []string) error {
		color[id] = 1
		for _, dep := range deps[id] {
			switch color[dep] {
			case 1:
				return invalid("dependency cycle detected",
					map[string]any{"cycle": append(append([]string(nil), stack...), id, dep)})
			case 0:
				if err := visit(dep, append(stack, id)); err != nil {
					return err
				}
			}
		}
		color[id] = 2
		return nil
	}
	ids := make([]string, 0, len(deps))
	for id := range deps {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if color[id] == 0 {
			if err := visit(id, nil); err != nil {
				return err
			}
		}
	}
	return nil
}
