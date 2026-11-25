package engine

import (
	"path/filepath"
	"testing"

	"qaudit/internal/generator"
)

func TestGracefulStopCompletesCalibrationOnly(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	ds, err := generator.Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng, err := New(dir, filepath.Join(dir, "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if err := eng.SaveDataset(ds); err != nil {
		t.Fatal(err)
	}
	if err := eng.RequestGracefulStop(); err != nil {
		t.Fatal(err)
	}
	if err := eng.Run(); err != nil {
		t.Fatalf("graceful stop run should not error: %v", err)
	}
	// Stop marker means later stages are skipped; state stays at calibration.
	state, err := eng.store.LoadState()
	if err != nil {
		t.Fatal(err)
	}
	if state == nil {
		t.Fatal("state must be committed even on graceful stop")
	}
	rows, err := eng.store.ListSnapshots(state.Version)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(ds.Network.Layers) {
		t.Fatalf("calibration snapshots should all commit, got %d", len(rows))
	}
	if eng.Audit != nil {
		t.Fatal("audit stage must be skipped after graceful stop")
	}
}
