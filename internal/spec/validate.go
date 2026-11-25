package spec

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const (
	CodeSpecInvalid        = "E_SPEC_INVALID"
	CodeStaleSnapshot      = "E_STALE_SNAPSHOT"
	CodeStaleBase          = "E_STALE_BASE"
	CodeStaleRead          = "E_STALE_READ"
	CodeConflictingRequest = "E_CONFLICTING_REQUEST"
	CodeNotFound           = "E_NOT_FOUND"
)

var allowedTypes = map[string]bool{
	"string": true, "int": true, "float": true, "bool": true, "json": true,
}

// ValidateKeyPath enforces dot-separated non-empty segments.
func ValidateKeyPath(p string) error {
	if p == "" {
		return fmt.Errorf("key path is empty")
	}
	for i, seg := range strings.Split(p, ".") {
		if seg == "" {
			return fmt.Errorf("key path %q has an empty segment at index %d", p, i)
		}
	}
	return nil
}

// IsPrefix reports whether prefix is a path-wise prefix of key.
// Equal paths count as a prefix (hop distance 0).
func IsPathPrefix(prefix, key string) bool {
	if prefix == key {
		return true
	}
	return strings.HasPrefix(key, prefix+".")
}

// HopDistance returns the number of key-path segments between prefix and key.
// Returns -1 when prefix is not a path-wise prefix of key.
func HopDistance(prefix, key string) int {
	if prefix == key {
		return 0
	}
	if !strings.HasPrefix(key, prefix+".") {
		return -1
	}
	return strings.Count(key[len(prefix)+1:], ".") + 1
}

// ValidateSnapshot validates a full snapshot and verifies its checksum.
// On any invalid input it returns E_SPEC_INVALID; the whole batch must be
// rejected by the caller (no partial application).
func ValidateSnapshot(s *Snapshot) error {
	if s.Version <= 0 {
		return invalidErr("snapshot version must be positive", map[string]any{"version": s.Version})
	}
	seenKeys := make(map[string]bool, len(s.Items))
	declared := make(map[string]bool, len(s.Items))
	for _, it := range s.Items {
		if err := ValidateKeyPath(it.KeyPath); err != nil {
			return invalidErr(err.Error(), map[string]any{"key_path": it.KeyPath})
		}
		if seenKeys[it.KeyPath] {
			return invalidErr("duplicate config item key path", map[string]any{"key_path": it.KeyPath})
		}
		seenKeys[it.KeyPath] = true
		declared[it.KeyPath] = true
		if !allowedTypes[it.Type] {
			return invalidErr("unsupported config item type", map[string]any{
				"key_path": it.KeyPath, "type": it.Type,
			})
		}
		if !json.Valid(it.Value) {
			return invalidErr("config item value is not valid JSON", map[string]any{"key_path": it.KeyPath})
		}
	}
	if err := validateComponents(s.Components, declared); err != nil {
		return err
	}
	want := Checksum(s.Items, s.Components)
	if s.Checksum != want {
		return invalidErr("snapshot checksum mismatch", map[string]any{
			"expected": want, "actual": s.Checksum, "version": s.Version,
		})
	}
	return nil
}

func validateComponents(components []Component, declaredKeys map[string]bool) error {
	ids := make(map[string]bool, len(components))
	for _, c := range components {
		if c.ID == "" {
			return invalidErr("component id is empty", nil)
		}
		if ids[c.ID] {
			return invalidErr("duplicate component id", map[string]any{"component": c.ID})
		}
		ids[c.ID] = true
		seenPrefix := make(map[string]bool, len(c.SubPrefixes))
		for _, p := range c.SubPrefixes {
			if err := ValidateKeyPath(p); err != nil {
				return invalidErr(err.Error(), map[string]any{"component": c.ID})
			}
			if seenPrefix[p] {
				return invalidErr("duplicate subscription prefix", map[string]any{
					"component": c.ID, "prefix": p,
				})
			}
			seenPrefix[p] = true
			if !prefixCovered(p, declaredKeys) {
				return invalidErr("subscription prefix is not a prefix of any declared key path",
					map[string]any{"component": c.ID, "prefix": p})
			}
		}
		if c.Wildcard && len(c.SubPrefixes) != 0 {
			return invalidErr("wildcard component must not declare explicit prefixes",
				map[string]any{"component": c.ID})
		}
		if !c.Wildcard && len(c.SubPrefixes) == 0 {
			return invalidErr("component has neither subscription prefixes nor wildcard flag",
				map[string]any{"component": c.ID})
		}
		seenDep := make(map[string]bool, len(c.DependsOn))
		for _, dep := range c.DependsOn {
			if dep == c.ID {
				return invalidErr("component depends on itself", map[string]any{"component": c.ID})
			}
			if seenDep[dep] {
				return invalidErr("duplicate dependency edge", map[string]any{
					"component": c.ID, "depends_on": dep,
				})
			}
			seenDep[dep] = true
		}
	}
	for _, c := range components {
		for _, dep := range c.DependsOn {
			if !ids[dep] {
				return invalidErr("dependency references unknown component", map[string]any{
					"component": c.ID, "depends_on": dep,
				})
			}
		}
	}
	if err := detectCycle(components); err != nil {
		return err
	}
	return nil
}

func prefixCovered(prefix string, declaredKeys map[string]bool) bool {
	for k := range declaredKeys {
		if IsPathPrefix(prefix, k) {
			return true
		}
	}
	return false
}

func detectCycle(components []Component) error {
	deps := make(map[string][]string, len(components))
	for _, c := range components {
		deps[c.ID] = append([]string(nil), c.DependsOn...)
	}
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(components))
	var visit func(id string, stack []string) error
	visit = func(id string, stack []string) error {
		color[id] = gray
		for _, dep := range deps[id] {
			switch color[dep] {
			case gray:
				path := append(append([]string(nil), stack...), id, dep)
				return invalidErr("dependency cycle detected", map[string]any{"cycle": path})
			case white:
				if err := visit(dep, append(stack, id)); err != nil {
					return err
				}
			}
		}
		color[id] = black
		return nil
	}
	ids := make([]string, 0, len(components))
	for id := range deps {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if color[id] == white {
			if err := visit(id, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func invalidErr(message string, ctx map[string]any) error {
	return NewError(CodeSpecInvalid, message, ctx)
}
