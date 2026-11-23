package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"

	"recoveryengine/internal/fixed"
	"recoveryengine/internal/graph"
	"recoveryengine/internal/model"
	"recoveryengine/internal/spec"
	"recoveryengine/internal/store"
)

func buildDerivedRefs(snapshot model.Snapshot, analysis *graph.Analysis, consistency fixed.Result) []store.RefRecord {
	records := make(map[string]store.RefRecord)
	add := func(derivedKind, derivedKey, refKind, refKey, field string) {
		record := store.RefRecord{
			DerivedKind: derivedKind, DerivedKey: derivedKey,
			RefKind: refKind, RefKey: refKey, Field: field,
		}
		key := strings.Join([]string{record.DerivedKind, record.DerivedKey, record.RefKind, record.RefKey, record.Field}, "\x00")
		records[key] = record
	}

	edges := snapshot.SortedEdges()
	components := snapshot.SortedComponents()
	for _, component := range components {
		add("resource_summary", component.ID, "component", component.ID, "resources")
		add("resource_summary", component.ID, "component", component.ID, "capacity")
		add("resource_summary", component.ID, "component", component.ID, "observation")
		add("resource_summary", component.ID, "component", component.ID, "weights")

		add("batch", component.ID, "component", component.ID, "depends_on")
		for _, edge := range edges {
			add("batch", component.ID, "edge", edge.Key(), "dependency")
		}

		add("consistency", component.ID, "component", component.ID, "observation")
		add("consistency", component.ID, "component", component.ID, "resources")
		add("consistency", component.ID, "component", component.ID, "capacity")
		add("consistency", component.ID, "component", component.ID, "health_probe")
		add("consistency", component.ID, "component", component.ID, "recovery_request")
		add("consistency", component.ID, "component", component.ID, "weights")

		for _, field := range []string{"id", "depends_on", "observation", "resources", "capacity", "health_probe", "recovery_request", "weights"} {
			add("consistency", "aggregate", "component", component.ID, field)
		}
	}
	for _, edge := range edges {
		add("consistency", "aggregate", "edge", edge.Key(), "dependency")
		add("diagnostics", "graph", "edge", edge.Key(), "dependency")
	}
	for _, component := range components {
		add("diagnostics", "graph", "component", component.ID, "depends_on")
	}
	for origin, impact := range analysis.Impacts {
		for _, row := range impact.Affected {
			key := origin + "/" + row.Component
			add("impact", key, "component", origin, "id")
			add("impact", key, "component", row.Component, "depends_on")
			for _, edge := range edges {
				add("impact", key, "edge", edge.Key(), "dependency")
			}
		}
	}

	out := make([]store.RefRecord, 0, len(records))
	for _, record := range records {
		out = append(out, record)
	}
	sort.Slice(out, func(i, j int) bool {
		left, right := out[i], out[j]
		if left.DerivedKind != right.DerivedKind {
			return left.DerivedKind < right.DerivedKind
		}
		if left.DerivedKey != right.DerivedKey {
			return left.DerivedKey < right.DerivedKey
		}
		if left.RefKind != right.RefKind {
			return left.RefKind < right.RefKind
		}
		if left.RefKey != right.RefKey {
			return left.RefKey < right.RefKey
		}
		return left.Field < right.Field
	})
	return out
}

func derivedViews(refs []store.RefRecord) []string {
	keys := make(map[string]struct{})
	for _, ref := range refs {
		keys[ref.DerivedKind+"/"+ref.DerivedKey] = struct{}{}
	}
	out := make([]string, 0, len(keys))
	for key := range keys {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func invalidatedViews(previous []store.RefRecord, changed []spec.Ref) []string {
	if len(previous) == 0 || len(changed) == 0 {
		return []string{}
	}
	all := derivedViews(previous)
	changedComponents := map[string]struct{}{}
	changedEdges := false
	changedSnapshot := false
	for _, ref := range changed {
		switch ref.Kind {
		case "component":
			changedComponents[ref.Key] = struct{}{}
		case "edge":
			changedEdges = true
		case "snapshot":
			changedSnapshot = true
		}
	}
	if changedEdges || changedSnapshot {
		return all
	}
	invalidated := map[string]struct{}{}
	for _, ref := range previous {
		if _, ok := changedComponents[ref.RefKey]; ok && (ref.RefKind == "component" || ref.RefKind == "edge") {
			invalidated[ref.DerivedKind+"/"+ref.DerivedKey] = struct{}{}
		}
	}
	for _, component := range sortedKeys(changedComponents) {
		for _, view := range all {
			if keyContainsSegment(view, component) {
				invalidated[view] = struct{}{}
			}
		}
		if containsString(all, "consistency/aggregate") {
			invalidated["consistency/aggregate"] = struct{}{}
		}
	}
	out := make([]string, 0, len(invalidated))
	for key := range invalidated {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func derivedDigest(snapshot model.Snapshot, analysis *graph.Analysis, consistency fixed.Result, refs []store.RefRecord) string {
	refDigest := refsDigest(refs)
	payload, _ := json.Marshal(struct {
		Version           int64  `json:"version"`
		Checksum          string `json:"checksum"`
		GraphDigest       string `json:"graph_digest"`
		ConsistencyDigest string `json:"consistency_digest"`
		RefsDigest        string `json:"refs_digest"`
	}{
		Version: snapshot.Version, Checksum: snapshot.ChecksumSHA256,
		GraphDigest: analysis.GraphDigest, ConsistencyDigest: consistency.Digest, RefsDigest: refDigest,
	})
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func refsDigest(refs []store.RefRecord) string {
	payload, _ := json.Marshal(refs)
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func keyContainsSegment(key, segment string) bool {
	parts := strings.Split(key, "/")
	for _, part := range parts {
		if part == segment {
			return true
		}
	}
	return false
}

func containsString(list []string, target string) bool {
	for _, value := range list {
		if value == target {
			return true
		}
	}
	return false
}

func sortedKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
