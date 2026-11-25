package demo

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"deployverify/internal/model"
)

func Digest(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func BaseTime() time.Time { return time.Date(2026, 11, 25, 2, 0, 0, 0, time.UTC) }

func SnapshotV1() *model.Snapshot {
	generated := BaseTime()
	snapshot := &model.Snapshot{
		Version:     1,
		GeneratedAt: generated,
		Environments: []model.Environment{
			{
				Name: "prod", Priority: 10,
				Units: []model.Unit{
					{Service: "identity", ImageDigest: Digest("identity:1.4.0"), DesiredVersion: "1.4.0", Config: map[string]string{"LOG_LEVEL": "info", "SESSION_TTL": "12h"}, DependsOn: []string{}},
					{Service: "catalog", ImageDigest: Digest("catalog:2.1.0"), DesiredVersion: "2.1.0", Config: map[string]string{"CACHE_TTL": "5m"}, DependsOn: []string{}},
					{Service: "gateway", ImageDigest: Digest("gateway:3.0.1"), DesiredVersion: "3.0.1", Config: map[string]string{"LOG_LEVEL": "info", "TIMEOUT_MS": "3000"}, DependsOn: []string{"identity", "catalog"}},
					{Service: "billing", ImageDigest: Digest("billing:1.9.2"), DesiredVersion: "1.9.2", Config: map[string]string{"LOG_LEVEL": "info"}, DependsOn: []string{"identity"}},
				},
			},
			{
				Name: "staging", Priority: 20,
				Units: []model.Unit{
					{Service: "identity", ImageDigest: Digest("identity:1.4.0"), DesiredVersion: "1.4.0", Config: map[string]string{"LOG_LEVEL": "debug", "SESSION_TTL": "12h"}, DependsOn: []string{}},
					{Service: "catalog", ImageDigest: Digest("catalog:2.1.0"), DesiredVersion: "2.1.0", Config: map[string]string{"CACHE_TTL": "5m"}, DependsOn: []string{}},
					{Service: "gateway", ImageDigest: Digest("gateway:3.0.0-staging"), DesiredVersion: "3.0.0", Config: map[string]string{"LOG_LEVEL": "debug", "TIMEOUT_MS": "3000"}, DependsOn: []string{"identity", "catalog"}},
					{Service: "billing", ImageDigest: Digest("billing:1.9.0-staging"), DesiredVersion: "1.9.0", Config: map[string]string{"LOG_LEVEL": "debug"}, DependsOn: []string{"identity"}},
				},
			},
			{
				Name: "dev", Priority: 30,
				Units: []model.Unit{
					{Service: "identity", ImageDigest: Digest("identity:1.5.0-dev"), DesiredVersion: "1.5.0", Config: map[string]string{"LOG_LEVEL": "trace", "SESSION_TTL": "13h"}, DependsOn: []string{}},
					{Service: "catalog", ImageDigest: Digest("catalog:2.2.0-dev"), DesiredVersion: "2.2.0", Config: map[string]string{"CACHE_TTL": "1m"}, DependsOn: []string{}},
					{Service: "gateway", ImageDigest: Digest("gateway:3.1.0-dev"), DesiredVersion: "3.1.0", Config: map[string]string{"LOG_LEVEL": "trace", "TIMEOUT_MS": "3000"}, DependsOn: []string{"identity", "catalog"}},
					{Service: "billing", ImageDigest: Digest("billing:1.10.0-dev"), DesiredVersion: "1.10.0", Config: map[string]string{"LOG_LEVEL": "trace"}, DependsOn: []string{"identity"}},
					{
						Service: "slowprobe", ImageDigest: Digest("slowprobe:0.1.0"), DesiredVersion: "0.1.0",
						Config: map[string]string{}, DependsOn: []string{},
						Probe: &model.ProbeSpec{Path: "/probe/slow?delay=5s", TimeoutMS: 80},
					},
				},
			},
		},
	}
	checksum, _ := model.Checksum(*snapshot)
	snapshot.Checksum = checksum
	return snapshot
}

func ConcurrentIncrement(variant string) *model.Increment {
	image := Digest("billing:1.9.3-" + variant)
	increment := &model.Increment{
		BaseVersion: 1, GeneratedAt: BaseTime().Add(10 * time.Minute),
		Changes: []model.UnitChange{{
			Environment: "prod",
			Unit: model.Unit{
				Service: "billing", ImageDigest: image, DesiredVersion: "1.9.3",
				Config: map[string]string{"LOG_LEVEL": "info", "REGION": variant}, DependsOn: []string{"identity"},
			},
		}},
	}
	checksum, _ := model.Checksum(*increment)
	increment.Checksum = checksum
	return increment
}

func CascadeIncrement() *model.Increment {
	increment := &model.Increment{
		BaseVersion: 2, GeneratedAt: BaseTime().Add(20 * time.Minute),
		Changes: []model.UnitChange{{
			Environment: "prod",
			Unit: model.Unit{
				Service: "identity", ImageDigest: Digest("identity:1.4.1-cascade"), DesiredVersion: "1.4.1",
				Config:    map[string]string{"LOG_LEVEL": "info", "SESSION_TTL": "8h", "TOKEN_AUDIENCE": "deployverify"},
				DependsOn: []string{},
			},
		}},
	}
	checksum, _ := model.Checksum(*increment)
	increment.Checksum = checksum
	return increment
}

func CycleIncrement() *model.Increment {
	increment := &model.Increment{
		BaseVersion: 3, GeneratedAt: BaseTime().Add(30 * time.Minute),
		Changes: []model.UnitChange{
			{
				Environment: "staging",
				Unit: model.Unit{
					Service: "identity", ImageDigest: Digest("identity:1.4.0"), DesiredVersion: "1.4.0",
					Config:    map[string]string{"LOG_LEVEL": "debug", "SESSION_TTL": "12h"},
					DependsOn: []string{"catalog"},
				},
			},
			{
				Environment: "staging",
				Unit: model.Unit{
					Service: "catalog", ImageDigest: Digest("catalog:2.1.0"), DesiredVersion: "2.1.0",
					Config: map[string]string{"CACHE_TTL": "5m"}, DependsOn: []string{"identity"},
				},
			},
		},
	}
	checksum, _ := model.Checksum(*increment)
	increment.Checksum = checksum
	return increment
}

func InvalidSnapshot() *model.Snapshot {
	snapshot := &model.Snapshot{
		Version: 1, GeneratedAt: BaseTime(),
		Environments: []model.Environment{{
			Name: "prod", Priority: 1,
			Units: []model.Unit{
				{Service: "auth", ImageDigest: "not-a-digest", DesiredVersion: "1.0.0", DependsOn: []string{"ghost"}},
				{Service: "auth", ImageDigest: Digest("auth:1"), DesiredVersion: "1.0.0"},
			},
		}},
	}
	checksum, _ := model.Checksum(*snapshot)
	snapshot.Checksum = checksum
	return snapshot
}

type Manifest struct {
	Workdir string            `json:"workdir"`
	Files   map[string]string `json:"files"`
}

func WriteSamples(workdir string) (*Manifest, error) {
	dir := filepath.Join(workdir, "samples")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	files := map[string]any{
		"snapshot-v1.json":            SnapshotV1(),
		"increment-concurrent-a.json": ConcurrentIncrement("a"),
		"increment-concurrent-b.json": ConcurrentIncrement("b"),
		"increment-cascade.json":      CascadeIncrement(),
		"increment-cycle.json":        CycleIncrement(),
		"snapshot-invalid.json":       InvalidSnapshot(),
	}
	manifest := &Manifest{Workdir: dir, Files: map[string]string{}}
	for name, value := range files {
		data, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return nil, err
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return nil, err
		}
		manifest.Files[name] = path
	}
	return manifest, nil
}
