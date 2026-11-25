package ttsflow

import (
	"context"
	"path/filepath"
	"testing"
)

func testDataset(t *testing.T) (Config, []byte, []TextSample) {
	t.Helper()
	samples := GeneratedSamples(DefaultSeed)
	cfg := GeneratedConfig(DefaultSeed, samples)
	raw, err := cfg.EncodeYAML()
	if err != nil {
		t.Fatal(err)
	}
	return cfg, raw, samples
}

func openTestStore(t *testing.T, name string) *Store {
	t.Helper()
	store, err := OpenStore(filepath.Join(t.TempDir(), name+".db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func newTestEngine(t *testing.T, store *Store, cfg Config, raw []byte, samples []TextSample) *Engine {
	t.Helper()
	engine, err := NewEngine(cfg, raw, store, samples)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func runEngine(t *testing.T, engine *Engine, opts RunOptions) SynthesisResult {
	t.Helper()
	result, err := engine.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	return result
}
