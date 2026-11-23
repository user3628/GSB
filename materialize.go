package configimpact

import (
	"sort"
	"time"
)

const (
	DefaultIncrementalThreshold = 0.50
	defaultEstimatedBase        = 200 * time.Microsecond
	defaultEstimatedPerKey      = 100 * time.Microsecond
)

// Options controls the explicit full-versus-incremental materialization
// budget. A component uses the incremental path when touched/watch is strictly
// below Threshold; equality and higher ratios choose the full path.
type Options struct {
	IncrementalThreshold    float64
	EstimatedBaseDuration   time.Duration
	EstimatedPerKeyDuration time.Duration
}

func normalizeOptions(options Options) Options {
	if options.IncrementalThreshold <= 0 || options.IncrementalThreshold > 1 {
		options.IncrementalThreshold = DefaultIncrementalThreshold
	}
	if options.EstimatedBaseDuration <= 0 {
		options.EstimatedBaseDuration = defaultEstimatedBase
	}
	if options.EstimatedPerKeyDuration <= 0 {
		options.EstimatedPerKeyDuration = defaultEstimatedPerKey
	}
	return options
}

func materializationInputs(doc *Document, component *Component) (watchKeys int, totalKeys int) {
	watchKeys = len(component.Watches)
	totalKeys = len(component.Config)
	if group := findGroup(doc, component.Group); group != nil {
		totalKeys += len(group.Defaults)
	}
	if totalKeys == 0 {
		totalKeys = 1
	}
	return watchKeys, totalKeys
}

func chooseMaterializationPath(options Options, previous *MaterializedConfig, touchedKeys, watchKeys int) (string, float64) {
	denominator := watchKeys
	if denominator <= 0 {
		denominator = 1
	}
	ratio := float64(touchedKeys) / float64(denominator)
	if previous == nil {
		return "full", ratio
	}
	if ratio < options.IncrementalThreshold {
		return "incremental", ratio
	}
	return "full", ratio
}

func materializeComponent(doc *Document, componentID string, previous *MaterializedConfig, triggerKeys []string, options Options, forceFull bool) (MaterializedConfig, error) {
	options = normalizeOptions(options)
	component := findComponent(doc, componentID)
	if component == nil {
		return MaterializedConfig{}, businessError(ErrNotFound, "component %q not found", componentID)
	}
	watchKeys, totalKeys := materializationInputs(doc, component)
	touchedKeys := len(uniqueStrings(triggerKeys))
	path, ratio := chooseMaterializationPath(options, previous, touchedKeys, watchKeys)
	if forceFull {
		path = "full"
	}

	var filter map[string]struct{}
	if path == "incremental" {
		filter = make(map[string]struct{}, touchedKeys)
		for _, key := range triggerKeys {
			filter[key] = struct{}{}
		}
	}

	estimatedKeys := totalKeys
	if path == "incremental" {
		estimatedKeys = len(filter)
	}
	estimated := options.EstimatedBaseDuration + time.Duration(estimatedKeys)*options.EstimatedPerKeyDuration
	started := time.Now()
	resolved, err := newResolver(doc, filter).resolve(componentID)
	if err != nil {
		return MaterializedConfig{}, err
	}
	actual := time.Since(started)

	values := resolved
	if path == "incremental" && previous != nil {
		values = cloneEffectiveValues(previous.Values)
		for key := range filter {
			delete(values, key)
			if value, exists := resolved[key]; exists {
				values[key] = value
			}
		}
	}
	if values == nil {
		values = map[string]EffectiveValue{}
	}
	return MaterializedConfig{
		SnapshotVersion:     doc.Version,
		ComponentID:         componentID,
		MaterializedVersion: doc.Version,
		Values:              values,
		Undefined:           undefinedWatchedKeys(component, values),
		Decision: MaterializationDecision{
			Path:                path,
			Threshold:           options.IncrementalThreshold,
			Ratio:               ratio,
			TouchedKeys:         touchedKeys,
			WatchKeys:           watchKeys,
			EstimatedDurationNS: estimated.Nanoseconds(),
			ActualDurationNS:    actual.Nanoseconds(),
		},
	}, nil
}

func reuseMaterialization(previous MaterializedConfig, snapshotVersion int64, options Options) MaterializedConfig {
	options = normalizeOptions(options)
	reused := cloneMaterialization(previous)
	reused.SnapshotVersion = snapshotVersion
	reused.Decision = MaterializationDecision{
		Path:      "reuse",
		Threshold: options.IncrementalThreshold,
	}
	return reused
}

func cloneEffectiveValues(values map[string]EffectiveValue) map[string]EffectiveValue {
	cloned := make(map[string]EffectiveValue, len(values))
	for key, value := range values {
		copied := value
		copied.Value = append([]byte(nil), value.Value...)
		copied.Winner = cloneSource(value.Winner)
		copied.Sources = make([]Source, len(value.Sources))
		for i := range value.Sources {
			copied.Sources[i] = cloneSource(value.Sources[i])
		}
		cloned[key] = copied
	}
	return cloned
}

func cloneSource(source Source) Source {
	cloned := source
	cloned.Value = append([]byte(nil), source.Value...)
	cloned.Via = append([]string(nil), source.Via...)
	return cloned
}

func cloneMaterialization(config MaterializedConfig) MaterializedConfig {
	cloned := config
	cloned.Values = cloneEffectiveValues(config.Values)
	cloned.Undefined = append([]string(nil), config.Undefined...)
	return cloned
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func buildReferences(doc *Document, materializations map[string]MaterializedConfig) []PatchRef {
	seen := make(map[string]PatchRef)
	add := func(ref PatchRef) {
		key := ref.ComponentID + "\x00" + ref.Kind + "\x00" + ref.TargetID + "\x00" + ref.Key
		seen[key] = ref
	}
	for _, component := range doc.Components {
		for _, key := range component.Watches {
			add(PatchRef{ComponentID: component.ID, Kind: "watch", Key: key})
		}
		for _, key := range component.Config {
			add(PatchRef{ComponentID: component.ID, Kind: "config_key", TargetID: component.ID, Key: key.Key})
		}
		if group := findGroup(doc, component.Group); group != nil {
			for _, key := range group.Defaults {
				add(PatchRef{ComponentID: component.ID, Kind: "group_default", TargetID: group.ID, Key: key.Key})
			}
		}
		for _, dependency := range component.DependsOn {
			add(PatchRef{ComponentID: component.ID, Kind: "upstream_component", TargetID: dependency})
		}
		if config, ok := materializations[component.ID]; ok {
			for _, value := range config.Values {
				for _, source := range value.Sources {
					if source.Kind == "component" {
						add(PatchRef{ComponentID: component.ID, Kind: "upstream_component", TargetID: source.Owner})
					} else {
						add(PatchRef{ComponentID: component.ID, Kind: "group_default", TargetID: source.Owner, Key: source.Key})
					}
				}
			}
		}
	}
	refs := make([]PatchRef, 0, len(seen))
	for _, ref := range seen {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		left, right := refs[i], refs[j]
		if left.ComponentID != right.ComponentID {
			return left.ComponentID < right.ComponentID
		}
		if left.Kind != right.Kind {
			return left.Kind < right.Kind
		}
		if left.TargetID != right.TargetID {
			return left.TargetID < right.TargetID
		}
		return left.Key < right.Key
	})
	return refs
}
