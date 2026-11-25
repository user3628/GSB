package sample

import (
	"embed"
	"fmt"
	"os"
	"path/filepath"

	"driftengine/internal/spec"
)

//go:embed assets/*.json
var assetsFS embed.FS

const FixedSeed = 245245

func Asset(name string) ([]byte, error) {
	return assetsFS.ReadFile("assets/" + name)
}

func MustAsset(name string) []byte {
	data, err := Asset(name)
	if err != nil {
		panic(err)
	}
	return data
}

func StandardSnapshot() (*spec.Snapshot, error) {
	return spec.ParseSnapshot(MustAsset("snapshot-v1.json"))
}

func CycleSnapshot() (*spec.Snapshot, error) {
	return spec.ParseSnapshot(MustAsset("snapshot-cycle.json"))
}

func LargeSnapshot() (*spec.Snapshot, error) {
	return spec.ParseSnapshot(MustAsset("snapshot-large-2000.json"))
}

func Increment() (*spec.Increment, error) {
	return spec.ParseIncrement(MustAsset("increment-base-v1.json"))
}

func EndpointState() (map[string]any, error) {
	var state map[string]any
	if err := spec.DecodeJSON(MustAsset("endpoint-state.json"), &state); err != nil {
		return nil, err
	}
	return state, nil
}

var fixedFiles = []string{
	"snapshot-v1.json",
	"snapshot-cycle.json",
	"snapshot-large-2000.json",
	"increment-base-v1.json",
	"endpoint-state.json",
}

func GenerateTo(dir string, seed int64) ([]string, error) {
	if seed != FixedSeed {
		return nil, fmt.Errorf("the checked-in deterministic generator currently publishes fixed seed %d; got %d", FixedSeed, seed)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	written := []string{}
	for _, name := range fixedFiles {
		data, err := Asset(name)
		if err != nil {
			return nil, err
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return nil, err
		}
		written = append(written, path)
	}
	return written, nil
}
