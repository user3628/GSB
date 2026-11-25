// Package seed produces the deterministic local sample set used by the
// validation script and integration tests. No external data is required.
package seed

import (
	"encoding/json"
	"os"
	"path/filepath"

	"configtree/internal/spec"
)

const GeneratedAt = "2026-01-01T00:00:00Z"

// File names of the deterministic samples.
const (
	FileSnapshotV1        = "snapshot_v1.json"
	FileSnapshotV1Invalid = "snapshot_v1_invalid.json"
	FileSnapshotStale     = "snapshot_stale_v1.json"
	FileDeltaV2           = "delta_v2_cache.json"
	FileDeltaV2Concurrent = "delta_v2_concurrent.json"
	FileDeltaV3Redirect   = "delta_v3_redirect.json"
	FileDeltaWrongBase    = "delta_wrong_base.json"
)

func item(key, typ string, value any, order int64) spec.ConfigItem {
	raw, _ := json.Marshal(value)
	return spec.ConfigItem{KeyPath: key, Type: typ, Value: raw, Order: order}
}

// V1 builds the fixed base snapshot.
func V1() *spec.Snapshot {
	items := []spec.ConfigItem{
		item("service.http.host", "string", "gateway.local", 10),
		item("service.http.port", "int", 8080, 10),
		item("service.http.tls.enabled", "bool", true, 10),
		item("service.http.tls.cert", "string", "/etc/tls/cert.pem", 10),
		item("service.db.dsn", "string", "postgres://cfg/db", 20),
		item("service.db.pool.max", "int", 16, 10),
		item("service.cache.ttl", "int", 30, 10),
		item("service.cache.backend", "string", "redis", 10),
		item("feature.flag.new_ui", "bool", false, 10),
		item("feature.flag.beta", "bool", true, 10),
	}
	components := []spec.Component{
		{ID: "gateway", SubPrefixes: []string{"service.http"}},
		{ID: "auth", SubPrefixes: []string{"service.http.tls", "service.db"},
			DependsOn: []string{"gateway"}},
		{ID: "worker", SubPrefixes: []string{"service.db", "service.cache"},
			DependsOn: []string{"auth"}},
		{ID: "ops-wildcard", Wildcard: true},
	}
	s := &spec.Snapshot{
		Version: 1, GeneratedAt: GeneratedAt, Items: items, Components: components,
	}
	s.Checksum = spec.Checksum(items, components)
	return s
}

// InvalidV1 is a batch that must be rejected wholesale: duplicate component
// id and a prefix matching no declared key.
func InvalidV1() *spec.Snapshot {
	good := V1()
	bad := *good
	bad.Components = append([]spec.Component{}, good.Components...)
	bad.Components = append(bad.Components, spec.Component{
		ID:          "gateway",
		SubPrefixes: []string{"missing.branch"},
	})
	bad.Checksum = "deadbeef"
	return &bad
}

// StaleSnapshot reuses version 1, which must fail once V1 is committed.
func StaleSnapshot() *spec.Snapshot {
	s := *V1()
	return &s
}

// DeltaV2 changes cache entries on base version 1.
func DeltaV2() *spec.Delta {
	return &spec.Delta{
		BaseVersion:   1,
		TargetVersion: 2,
		Items: []spec.ItemChange{
			{Op: "upsert", Item: item("service.cache.ttl", "int", 90, 30)},
			{Op: "upsert", Item: item("service.cache.region", "string", "cn-east", 10)},
			{Op: "delete", Item: spec.ConfigItem{KeyPath: "feature.flag.beta"}},
		},
	}
}

// DeltaV2Concurrent shares base version 1 with DeltaV2: only one may win.
func DeltaV2Concurrent() *spec.Delta {
	return &spec.Delta{
		BaseVersion:   1,
		TargetVersion: 2,
		Items: []spec.ItemChange{
			{Op: "upsert", Item: item("service.cache.backend", "string", "memcached", 40)},
		},
	}
}

// DeltaV3Redirect re-points the worker's dependency edge (gateway instead of
// auth) and trims a subscription prefix.
func DeltaV3Redirect() *spec.Delta {
	return &spec.Delta{
		BaseVersion:   2,
		TargetVersion: 3,
		Subscriptions: []spec.SubscriptionChange{
			{Op: "replace", Component: spec.Component{
				ID:          "worker",
				SubPrefixes: []string{"service.cache"},
				DependsOn:   []string{"gateway"},
			}},
		},
	}
}

// DeltaWrongBase claims a base version that does not exist.
func DeltaWrongBase() *spec.Delta {
	d := DeltaV2()
	d.BaseVersion = 99
	return d
}

// WriteAll emits every sample JSON file into dir.
func WriteAll(dir string) ([]string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	files := map[string]any{
		FileSnapshotV1:        V1(),
		FileSnapshotV1Invalid: InvalidV1(),
		FileSnapshotStale:     StaleSnapshot(),
		FileDeltaV2:           DeltaV2(),
		FileDeltaV2Concurrent: DeltaV2Concurrent(),
		FileDeltaV3Redirect:   DeltaV3Redirect(),
		FileDeltaWrongBase:    DeltaWrongBase(),
	}
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sortStrings(names)
	for _, name := range names {
		path := filepath.Join(dir, name)
		raw, err := json.MarshalIndent(files[name], "", "  ")
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(path, append(raw, '\n'), 0o644); err != nil {
			return nil, err
		}
	}
	return names, nil
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
