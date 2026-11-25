package engine

import (
	"path/filepath"
	"testing"

	"alarmengine/internal/model"
	"alarmengine/internal/sample"
	"alarmengine/internal/storage"
)

func newTestEngine(t *testing.T, cfg Config) (*Engine, *storage.Store) {
	t.Helper()
	dir := t.TempDir()
	st, err := storage.Open(filepath.Join(dir, "engine.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if cfg.Now == nil {
		cfg.Now = func() int64 { return 1_700_000_000_000 }
	}
	if cfg.PatrolQueueCapacity == 0 {
		cfg.PatrolQueueCapacity = 64
	}
	eng, err := New(st, cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() {
		eng.Close()
		_ = st.Close()
	})
	return eng, st
}

func bootstrappedEngine(t *testing.T) (*Engine, sample.Dataset) {
	eng, _ := newTestEngine(t, Config{})
	data := sample.Generate()
	if err := eng.BootstrapServices(data.Services); err != nil {
		t.Fatalf("bootstrap services: %v", err)
	}
	if err := eng.BootstrapPolicy(data.Policy); err != nil {
		t.Fatalf("bootstrap policy: %v", err)
	}
	return eng, data
}

func ingestSeed(t *testing.T, eng *Engine, data sample.Dataset) IngestResult {
	t.Helper()
	res, err := eng.IngestObservations(IngestRequest{
		RequestID: "req-seed", BaseVersion: 1, Observations: data.Observations,
	})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	return res
}

func obs(ts int64, svc, metric string, value int64, labels map[string]string) model.Observation {
	if labels == nil {
		labels = map[string]string{"tier": "core"}
	}
	return model.Observation{TimestampMS: ts, ServiceID: svc, Metric: metric, Value: value, Labels: labels}
}
