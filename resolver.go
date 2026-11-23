package configimpact

import (
	"sort"
	"strings"
)

type resolver struct {
	doc        *Document
	groups     map[string]*Group
	components map[string]*Component
	filter     map[string]struct{}
	memo       map[string]map[string]EffectiveValue
}

func newResolver(doc *Document, filter map[string]struct{}) *resolver {
	groups := make(map[string]*Group, len(doc.Groups))
	for i := range doc.Groups {
		groups[doc.Groups[i].ID] = &doc.Groups[i]
	}
	components := make(map[string]*Component, len(doc.Components))
	for i := range doc.Components {
		components[doc.Components[i].ID] = &doc.Components[i]
	}
	return &resolver{
		doc:        doc,
		groups:     groups,
		components: components,
		filter:     filter,
		memo:       make(map[string]map[string]EffectiveValue, len(doc.Components)),
	}
}

func (r *resolver) allows(key string) bool {
	if r.filter == nil {
		return true
	}
	_, ok := r.filter[key]
	return ok
}

func (r *resolver) resolve(componentID string) (map[string]EffectiveValue, error) {
	if values, ok := r.memo[componentID]; ok {
		return values, nil
	}
	component := r.components[componentID]
	if component == nil {
		return nil, businessError(ErrNotFound, "component %q not found", componentID)
	}

	candidates := make(map[string][]Source)
	addLocalCandidates(candidates, component, r.groups[component.Group])

	dependencyIDs := append([]string(nil), component.DependsOn...)
	sort.Strings(dependencyIDs)
	for _, dependencyID := range dependencyIDs {
		dependencyValues, err := r.resolve(dependencyID)
		if err != nil {
			return nil, err
		}
		for key, value := range dependencyValues {
			if !r.allows(key) {
				continue
			}
			for _, source := range value.Sources {
				candidates[key] = append(candidates[key], promoteSource(source, componentID))
			}
		}
	}

	values := make(map[string]EffectiveValue, len(candidates))
	for key, sources := range candidates {
		if !r.allows(key) {
			continue
		}
		values[key] = chooseEffectiveValue(key, sources)
	}
	r.memo[componentID] = values
	return values, nil
}

func addLocalCandidates(candidates map[string][]Source, component *Component, group *Group) {
	if group != nil {
		for _, key := range group.Defaults {
			candidates[key.Key] = append(candidates[key.Key], Source{
				Kind:       "group",
				Component:  component.ID,
				Owner:      group.ID,
				Key:        key.Key,
				Type:       key.Type,
				Value:      append([]byte(nil), key.Value...),
				Order:      key.Order,
				Distance:   0,
				Precedence: 10,
				Via:        []string{component.ID, "group:" + group.ID},
			})
		}
	}
	for _, key := range component.Config {
		candidates[key.Key] = append(candidates[key.Key], Source{
			Kind:       "component",
			Component:  component.ID,
			Owner:      component.ID,
			Key:        key.Key,
			Type:       key.Type,
			Value:      append([]byte(nil), key.Value...),
			Order:      key.Order,
			Distance:   0,
			Precedence: 30,
			Via:        []string{component.ID},
		})
	}
}

func promoteSource(source Source, targetComponent string) Source {
	distance := source.Distance + 1
	promoted := source
	promoted.Component = targetComponent
	promoted.Distance = distance
	promoted.Value = append([]byte(nil), source.Value...)
	if source.Kind == "component" {
		promoted.Precedence = 30 - distance
	} else {
		promoted.Precedence = 10 - distance
	}
	if promoted.Precedence < 1 {
		promoted.Precedence = 1
	}
	promoted.Via = append([]string{targetComponent}, source.Via...)
	return promoted
}

func chooseEffectiveValue(key string, sources []Source) EffectiveValue {
	sort.SliceStable(sources, func(i, j int) bool {
		left, right := sources[i], sources[j]
		if left.Order != right.Order {
			return left.Order > right.Order
		}
		if left.Precedence != right.Precedence {
			return left.Precedence > right.Precedence
		}
		if left.Distance != right.Distance {
			return left.Distance < right.Distance
		}
		if left.Owner != right.Owner {
			return left.Owner < right.Owner
		}
		if left.Kind != right.Kind {
			return left.Kind > right.Kind
		}
		return strings.Join(left.Via, "/") < strings.Join(right.Via, "/")
	})
	winner := sources[0]
	winner.Value = append([]byte(nil), winner.Value...)
	winner.Winner = true
	sources[0] = winner
	for i := 1; i < len(sources); i++ {
		sources[i].Winner = false
	}
	return EffectiveValue{
		Key:        key,
		Type:       winner.Type,
		Value:      append([]byte(nil), winner.Value...),
		Order:      winner.Order,
		Winner:     winner,
		Sources:    sources,
		Resolution: resolutionReason(sources),
	}
}

func resolutionReason(sources []Source) string {
	if len(sources) == 1 {
		return "single source"
	}
	first, second := sources[0], sources[1]
	switch {
	case first.Order != second.Order:
		return "highest effective order"
	case first.Precedence != second.Precedence:
		if first.Kind == "component" && second.Kind == "group" {
			return "explicit declaration over inherited group default"
		}
		if first.Distance < second.Distance {
			return "nearer dependency declaration"
		}
		return "explicit declaration precedence"
	case first.Distance != second.Distance:
		return "nearest source chain"
	default:
		return "deterministic tie-break by owner and route"
	}
}

func undefinedWatchedKeys(component *Component, values map[string]EffectiveValue) []string {
	undefined := make([]string, 0)
	for _, key := range component.Watches {
		if _, exists := values[key]; !exists {
			undefined = append(undefined, key)
		}
	}
	sort.Strings(undefined)
	return undefined
}

// EffectiveConfig resolves a component from scratch. It is exported for
// deterministic sample inspection and for equivalence checks.
func EffectiveConfig(doc *Document, componentID string) (MaterializedConfig, error) {
	validated, err := validateDocument(doc)
	if err != nil {
		return MaterializedConfig{}, err
	}
	component := findComponent(validated, componentID)
	if component == nil {
		return MaterializedConfig{}, businessError(ErrNotFound, "component %q not found", componentID)
	}
	values, err := newResolver(validated, nil).resolve(componentID)
	if err != nil {
		return MaterializedConfig{}, err
	}
	return MaterializedConfig{
		SnapshotVersion:     validated.Version,
		ComponentID:         componentID,
		MaterializedVersion: validated.Version,
		Values:              values,
		Undefined:           undefinedWatchedKeys(component, values),
		Decision: MaterializationDecision{
			Path:        "full",
			TouchedKeys: len(values),
			WatchKeys:   len(component.Watches),
		},
	}, nil
}
