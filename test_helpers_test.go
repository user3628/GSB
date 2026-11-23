package dnsdetect

import (
	"context"
	"testing"
)

func requireCode(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected code %s, got nil error", want)
	}
	if got := CodeOf(err); got != want {
		t.Fatalf("expected code %s, got %s (%v)", want, got, err)
	}
}

func testDataset(t *testing.T) *Dataset {
	t.Helper()
	dataset, err := GenerateDataset(t.TempDir())
	if err != nil {
		t.Fatalf("GenerateDataset: %v", err)
	}
	return dataset
}

func testConfig(dataset *Dataset) Config {
	config := DefaultConfig()
	config.WindowAnchorTicks = dataset.ObservationStartTicks
	return config
}

func testStore(t *testing.T, name string) *SnapshotStore {
	t.Helper()
	store, err := OpenSnapshotStore(t.TempDir() + "/" + name + ".db")
	if err != nil {
		t.Fatalf("OpenSnapshotStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func processGood(t *testing.T, dataset *Dataset, config Config, store *SnapshotStore) *RunResult {
	t.Helper()
	engine := NewEngine(config, store)
	result, err := engine.Process(context.Background(), dataset.Sources)
	if err != nil {
		t.Fatalf("Process good dataset: %v", err)
	}
	return result
}
