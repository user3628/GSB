package spec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"recoveryengine/internal/model"
)

const (
	maxIDLength       = 96
	maxProbeTimeoutMs = int64(60_000)
	maxExpectedStatus = 599
	maxTolerancePPM   = int64(1_000_000)
)

type Ref struct {
	Kind  string `json:"kind"`
	Key   string `json:"key"`
	Field string `json:"field"`
}

func (r Ref) ID() string {
	return r.Kind + ":" + r.Key + ":" + r.Field
}

type checksumContent struct {
	Version           int64                        `json:"version"`
	GeneratedAt       string                       `json:"generated_at"`
	Components        []model.Component            `json:"components"`
	Edges             []model.Edge                 `json:"edges"`
	Observations      map[string]model.Observation `json:"observations"`
	ConsistencyConfig model.ConsistencyConfig      `json:"consistency_config"`
}

func DecodeSnapshot(data []byte) (model.Snapshot, *model.BusinessError) {
	var snapshot model.Snapshot
	if err := decodeStrict(data, &snapshot); err != nil {
		return snapshot, invalid("snapshot JSON is not valid", map[string]any{"cause": err.Error()})
	}
	return snapshot, nil
}

func DecodeIncrement(data []byte) (model.IncrementRequest, *model.BusinessError) {
	var request model.IncrementRequest
	if err := decodeStrict(data, &request); err != nil {
		return request, invalid("increment JSON is not valid", map[string]any{"cause": err.Error()})
	}
	return request, nil
}

func DecodeDrill(data []byte) (model.DrillRequest, *model.BusinessError) {
	var request model.DrillRequest
	if err := decodeStrict(data, &request); err != nil {
		return request, invalid("drill JSON is not valid", map[string]any{"cause": err.Error()})
	}
	return request, nil
}

func decodeStrict(data []byte, dst any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("unexpected trailing JSON value")
	}
	return nil
}

// NormalizeSnapshot validates a source snapshot and returns the canonical form used by the engine.
// Duplicate component IDs are resolved by preserving the first occurrence and reporting the rest.
func NormalizeSnapshot(raw model.Snapshot, requireChecksum bool) (model.NormalizedSnapshot, *model.BusinessError) {
	result := model.NormalizedSnapshot{Snapshot: raw.Clone()}
	if raw.Version < 1 {
		return result, invalid("snapshot version must be at least 1", map[string]any{"version": raw.Version})
	}
	if _, err := time.Parse(time.RFC3339Nano, raw.GeneratedAt); err != nil {
		return result, invalid("generated_at must be RFC3339", map[string]any{"generated_at": raw.GeneratedAt})
	}
	if err := validateConfig(raw.ConsistencyConfig); err != nil {
		return result, err
	}

	kept := make([]model.Component, 0, len(raw.Components))
	firstIndex := make(map[string]int, len(raw.Components))
	for index, component := range raw.Components {
		if previous, ok := firstIndex[component.ID]; ok {
			result.Duplicates = append(result.Duplicates, model.DuplicateComponent{
				ID:             component.ID,
				FirstIndex:     previous,
				DuplicateIndex: index,
			})
			continue
		}
		firstIndex[component.ID] = index
		kept = append(kept, component.Clone())
	}
	sort.Slice(kept, func(i, j int) bool { return kept[i].ID < kept[j].ID })

	byID := make(map[string]*model.Component, len(kept))
	for i := range kept {
		byID[kept[i].ID] = &kept[i]
	}
	for _, edge := range raw.Edges {
		downstream, ok := byID[edge.To]
		if !ok {
			return result, invalid("dependency edge references unknown component", map[string]any{
				"from": edge.From, "to": edge.To,
			})
		}
		if _, ok := byID[edge.From]; !ok {
			return result, invalid("dependency edge references unknown component", map[string]any{
				"from": edge.From, "to": edge.To,
			})
		}
		downstream.DependsOn = append(downstream.DependsOn, edge.From)
	}

	var cpuTotal, memoryTotal int64
	for i := range kept {
		component := &kept[i]
		if err := validateComponent(*component, byID); err != nil {
			return result, err
		}
		var ok bool
		cpuTotal, ok = checkedAdd(cpuTotal, component.Resources.CPUMillicores)
		if !ok {
			return result, invalid("aggregate CPU requirement overflows int64", map[string]any{"component": component.ID})
		}
		memoryTotal, ok = checkedAdd(memoryTotal, component.Resources.MemoryBytes)
		if !ok {
			return result, invalid("aggregate memory requirement overflows int64", map[string]any{"component": component.ID})
		}
	}
	for id, observation := range raw.Observations {
		if _, ok := byID[id]; !ok {
			return result, invalid("observation references unknown component", map[string]any{"component": id})
		}
		if err := validateObservation(observation); err != nil {
			return result, err
		}
	}

	edges := edgesFromComponents(kept)
	canonical := model.Snapshot{
		Version:           raw.Version,
		GeneratedAt:       raw.GeneratedAt,
		Components:        kept,
		Edges:             edges,
		Observations:      cloneObservations(raw.Observations),
		ConsistencyConfig: raw.ConsistencyConfig,
	}
	canonical.ChecksumSHA256 = ComputeChecksum(canonical)
	if requireChecksum {
		if strings.TrimSpace(raw.ChecksumSHA256) == "" {
			return result, invalid("snapshot checksum_sha256 is required", nil)
		}
		if !strings.EqualFold(strings.TrimSpace(raw.ChecksumSHA256), canonical.ChecksumSHA256) {
			return result, invalid("snapshot checksum does not match canonical content", map[string]any{
				"supplied": raw.ChecksumSHA256, "expected": canonical.ChecksumSHA256,
			})
		}
	}
	result.Snapshot = canonical
	return result, nil
}

func ComputeChecksum(snapshot model.Snapshot) string {
	content := checksumContent{
		Version:           snapshot.Version,
		GeneratedAt:       snapshot.GeneratedAt,
		Components:        snapshot.SortedComponents(),
		Edges:             snapshot.SortedEdges(),
		Observations:      cloneObservations(snapshot.Observations),
		ConsistencyConfig: snapshot.ConsistencyConfig,
	}
	for i := range content.Components {
		content.Components[i].DependsOn = append([]string(nil), content.Components[i].DependsOn...)
		sort.Strings(content.Components[i].DependsOn)
	}
	payload, _ := json.Marshal(content)
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func ApplyIncrement(base model.Snapshot, request model.IncrementRequest, targetVersion int64) (model.Snapshot, []Ref, *model.BusinessError) {
	if targetVersion <= base.Version {
		return model.Snapshot{}, nil, invalid("target version must be greater than base version", map[string]any{
			"base": base.Version, "target": targetVersion,
		})
	}
	if strings.TrimSpace(request.GeneratedAt) == "" {
		return model.Snapshot{}, nil, invalid("generated_at is required", nil)
	}
	if _, err := time.Parse(time.RFC3339Nano, request.GeneratedAt); err != nil {
		return model.Snapshot{}, nil, invalid("generated_at must be RFC3339", map[string]any{"generated_at": request.GeneratedAt})
	}
	if len(request.Operations) == 0 {
		return model.Snapshot{}, nil, invalid("increment must contain at least one operation", nil)
	}

	components := base.ComponentMap()
	observations := cloneObservations(base.Observations)
	changed := make(map[string]Ref)
	addChanged := func(ref Ref) { changed[ref.ID()] = ref }

	requireComponent := func(id string) (model.Component, *model.BusinessError) {
		component, ok := components[id]
		if !ok {
			return model.Component{}, invalid("operation references unknown component", map[string]any{"component": id})
		}
		return component, nil
	}

	for operationIndex, operation := range request.Operations {
		switch operation.Type {
		case "upsert_component":
			if operation.Component == nil {
				return model.Snapshot{}, nil, invalid("upsert_component requires component", map[string]any{"operation_index": operationIndex})
			}
			component := operation.Component.Clone()
			if component.ID == "" {
				return model.Snapshot{}, nil, invalid("component.id is required", map[string]any{"operation_index": operationIndex})
			}
			component.DependsOn = append([]string(nil), component.DependsOn...)
			if _, existed := components[component.ID]; existed {
				component.DependsOn = mergeStrings(component.DependsOn, components[component.ID].DependsOn)
			}
			components[component.ID] = component
			addChanged(Ref{Kind: "component", Key: component.ID, Field: "all"})
		case "patch_resource":
			component, err := requireComponent(operation.ComponentID)
			if err != nil {
				return model.Snapshot{}, nil, err
			}
			if operation.Resources == nil && operation.Capacity == nil {
				return model.Snapshot{}, nil, invalid("patch_resource requires resources or capacity", map[string]any{"component": component.ID})
			}
			if operation.Resources != nil {
				component.Resources = *operation.Resources
				addChanged(Ref{Kind: "component", Key: component.ID, Field: "resources"})
			}
			if operation.Capacity != nil {
				component.Capacity = *operation.Capacity
				addChanged(Ref{Kind: "component", Key: component.ID, Field: "capacity"})
			}
			components[component.ID] = component
		case "patch_probe":
			component, err := requireComponent(operation.ComponentID)
			if err != nil {
				return model.Snapshot{}, nil, err
			}
			if operation.HealthProbe == nil && operation.RecoveryRequest == nil {
				return model.Snapshot{}, nil, invalid("patch_probe requires health_probe or recovery_request", map[string]any{"component": component.ID})
			}
			if operation.HealthProbe != nil {
				component.HealthProbe = *operation.HealthProbe
				addChanged(Ref{Kind: "component", Key: component.ID, Field: "health_probe"})
			}
			if operation.RecoveryRequest != nil {
				component.RecoveryRequest = *operation.RecoveryRequest
				addChanged(Ref{Kind: "component", Key: component.ID, Field: "recovery_request"})
			}
			components[component.ID] = component
		case "patch_observation":
			component, err := requireComponent(operation.ComponentID)
			if err != nil {
				return model.Snapshot{}, nil, err
			}
			if operation.Observation == nil {
				return model.Snapshot{}, nil, invalid("patch_observation requires observation", map[string]any{"component": component.ID})
			}
			observations[component.ID] = *operation.Observation
			addChanged(Ref{Kind: "component", Key: component.ID, Field: "observation"})
		case "add_edge":
			if operation.From == "" || operation.To == "" {
				return model.Snapshot{}, nil, invalid("add_edge requires from and to", map[string]any{"operation_index": operationIndex})
			}
			downstream, ok := components[operation.To]
			if !ok {
				return model.Snapshot{}, nil, invalid("edge references unknown component", map[string]any{"component": operation.To})
			}
			if _, ok := components[operation.From]; !ok {
				return model.Snapshot{}, nil, invalid("edge references unknown component", map[string]any{"component": operation.From})
			}
			downstream.DependsOn = append(downstream.DependsOn, operation.From)
			components[operation.To] = downstream
			edge := model.Edge{From: operation.From, To: operation.To}
			addChanged(Ref{Kind: "edge", Key: edge.Key(), Field: "dependency"})
			addChanged(Ref{Kind: "component", Key: operation.To, Field: "depends_on"})
		case "remove_edge":
			if operation.From == "" || operation.To == "" {
				return model.Snapshot{}, nil, invalid("remove_edge requires from and to", map[string]any{"operation_index": operationIndex})
			}
			downstream, ok := components[operation.To]
			if !ok {
				return model.Snapshot{}, nil, invalid("edge references unknown component", map[string]any{"component": operation.To})
			}
			next := make([]string, 0, len(downstream.DependsOn))
			found := false
			for _, dependency := range downstream.DependsOn {
				if dependency == operation.From {
					found = true
					continue
				}
				next = append(next, dependency)
			}
			if !found {
				return model.Snapshot{}, nil, invalid("dependency edge does not exist", map[string]any{"from": operation.From, "to": operation.To})
			}
			downstream.DependsOn = next
			components[operation.To] = downstream
			edge := model.Edge{From: operation.From, To: operation.To}
			addChanged(Ref{Kind: "edge", Key: edge.Key(), Field: "dependency"})
			addChanged(Ref{Kind: "component", Key: operation.To, Field: "depends_on"})
		default:
			return model.Snapshot{}, nil, invalid("unsupported increment operation", map[string]any{"type": operation.Type, "operation_index": operationIndex})
		}
	}

	list := make([]model.Component, 0, len(components))
	for _, component := range components {
		list = append(list, component.Clone())
	}
	raw := model.Snapshot{
		Version:           targetVersion,
		GeneratedAt:       request.GeneratedAt,
		Components:        list,
		Observations:      observations,
		ConsistencyConfig: base.ConsistencyConfig,
	}
	normalized, err := NormalizeSnapshot(raw, false)
	if err != nil {
		return model.Snapshot{}, nil, err
	}
	refs := make([]Ref, 0, len(changed))
	for _, ref := range changed {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].ID() < refs[j].ID() })
	return normalized.Snapshot, refs, nil
}

func validateComponent(component model.Component, byID map[string]*model.Component) *model.BusinessError {
	if err := validateID(component.ID, "component.id"); err != nil {
		return err
	}
	if err := validateResource(component.Resources, component.ID, "resources"); err != nil {
		return err
	}
	if err := validateResource(component.Capacity, component.ID, "capacity"); err != nil {
		return err
	}
	if component.HealthWeightPermille < 0 || component.ResourceWeightPermille < 0 ||
		component.HealthWeightPermille+component.ResourceWeightPermille != 1000 {
		return invalid("health and resource weights must be non-negative and sum to 1000", map[string]any{
			"component": component.ID, "health": component.HealthWeightPermille, "resource": component.ResourceWeightPermille,
		})
	}
	if component.ConsistencyWeightPermille <= 0 || component.ConsistencyWeightPermille > 1000 {
		return invalid("consistency weight must be between 1 and 1000", map[string]any{
			"component": component.ID, "weight": component.ConsistencyWeightPermille,
		})
	}
	if component.HealthProbe.Type != "http" && component.HealthProbe.Type != "https" {
		return invalid("health_probe.type must be http or https", map[string]any{"component": component.ID, "type": component.HealthProbe.Type})
	}
	if !model.IsHTTPURL(component.HealthProbe.URL) {
		return invalid("health_probe.url must be an absolute HTTP URL", map[string]any{"component": component.ID})
	}
	if component.HealthProbe.TimeoutMs < 1 || component.HealthProbe.TimeoutMs > maxProbeTimeoutMs {
		return invalid("health_probe.timeout_ms is outside the supported range", map[string]any{"component": component.ID})
	}
	if component.HealthProbe.ExpectedStatus < 0 || component.HealthProbe.ExpectedStatus > maxExpectedStatus {
		return invalid("health_probe.expected_status is invalid", map[string]any{"component": component.ID})
	}
	if !model.IsHTTPURL(component.RecoveryRequest.URL) {
		return invalid("recovery_request.url must be an absolute HTTP URL", map[string]any{"component": component.ID})
	}
	if component.RecoveryRequest.TimeoutMs < 1 || component.RecoveryRequest.TimeoutMs > maxProbeTimeoutMs {
		return invalid("recovery_request.timeout_ms is outside the supported range", map[string]any{"component": component.ID})
	}
	seen := make(map[string]struct{}, len(component.DependsOn))
	canonicalDeps := make([]string, 0, len(component.DependsOn))
	for _, dependency := range component.DependsOn {
		if err := validateID(dependency, "depends_on"); err != nil {
			return err
		}
		if _, exists := byID[dependency]; !exists {
			return invalid("component depends on an unknown component", map[string]any{
				"component": component.ID, "dependency": dependency,
			})
		}
		if _, duplicate := seen[dependency]; duplicate {
			continue
		}
		seen[dependency] = struct{}{}
		canonicalDeps = append(canonicalDeps, dependency)
	}
	sort.Strings(canonicalDeps)
	if canonical, ok := byID[component.ID]; ok {
		canonical.DependsOn = canonicalDeps
	}
	return nil
}

func validateResource(resource model.Resource, componentID, field string) *model.BusinessError {
	if resource.CPUMillicores < 0 || resource.MemoryBytes < 0 {
		return invalid("resource values must be non-negative integers", map[string]any{
			"component": componentID, "field": field,
		})
	}
	if resource.CPUMillicores == math.MaxInt64 || resource.MemoryBytes == math.MaxInt64 {
		return invalid("resource value is too large", map[string]any{"component": componentID, "field": field})
	}
	return nil
}

func validateID(id, field string) *model.BusinessError {
	if id == "" {
		return invalid(field+" is required", nil)
	}
	if len(id) > maxIDLength {
		return invalid(field+" is too long", map[string]any{"value": id})
	}
	for _, r := range id {
		valid := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' || r == ':'
		if !valid {
			return invalid(field+" contains an unsupported character", map[string]any{"value": id})
		}
	}
	return nil
}

func validateObservation(observation model.Observation) *model.BusinessError {
	switch observation.State {
	case "healthy", "degraded", "unhealthy", "unknown":
	default:
		return invalid("observation state is invalid", map[string]any{"state": observation.State})
	}
	if observation.ObservedAt != "" {
		if _, err := time.Parse(time.RFC3339Nano, observation.ObservedAt); err != nil {
			return invalid("observation observed_at must be RFC3339", map[string]any{"observed_at": observation.ObservedAt})
		}
	}
	return nil
}

func validateConfig(config model.ConsistencyConfig) *model.BusinessError {
	if config.ThresholdPermille < 0 || config.ThresholdPermille > 1000 {
		return invalid("consistency threshold_permille must be in [0,1000]", map[string]any{"threshold": config.ThresholdPermille})
	}
	if config.RelativeTolerancePPM < 0 || config.RelativeTolerancePPM > maxTolerancePPM {
		return invalid("consistency relative_tolerance_ppm must be in [0,1000000]", map[string]any{"tolerance": config.RelativeTolerancePPM})
	}
	return nil
}

func edgesFromComponents(components []model.Component) []model.Edge {
	edges := make([]model.Edge, 0)
	for _, component := range components {
		for _, dependency := range component.DependsOn {
			edges = append(edges, model.Edge{From: dependency, To: component.ID})
		}
	}
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].From == edges[j].From {
			return edges[i].To < edges[j].To
		}
		return edges[i].From < edges[j].From
	})
	return edges
}

func checkedAdd(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, false
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, false
	}
	return a + b, true
}

func cloneObservations(in map[string]model.Observation) map[string]model.Observation {
	out := make(map[string]model.Observation, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func mergeStrings(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, list := range [][]string{a, b} {
		for _, item := range list {
			if _, ok := seen[item]; ok {
				continue
			}
			seen[item] = struct{}{}
			out = append(out, item)
		}
	}
	sort.Strings(out)
	return out
}

func invalid(message string, context map[string]any) *model.BusinessError {
	err := model.NewError(model.CodeSpecInvalid, message, 422)
	if context != nil {
		err.Context = context
	}
	return err
}
