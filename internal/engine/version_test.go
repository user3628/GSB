package engine

import (
	"path/filepath"
	"testing"

	"qaudit/internal/generator"
	"qaudit/internal/model"
)

func setupEngine(t *testing.T, tag string) (*Engine, *model.Dataset) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), tag)
	ds, err := generator.Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng, err := New(dir, filepath.Join(dir, "snapshots.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.SaveDataset(ds); err != nil {
		t.Fatal(err)
	}
	return eng, ds
}

func TestNormalThenReplacementThenRollback(t *testing.T) {
	eng, ds := setupEngine(t, "v")
	defer eng.Close()
	if err := eng.Run(); err != nil {
		t.Fatalf("initial run: %v", err)
	}
	firstFP := eng.Fingerprints.VersionFingerprint
	if eng.Version != 1 {
		t.Fatalf("want version 1, got %d", eng.Version)
	}

	// Full replacement: change the calibration seed, producing a new fingerprint.
	ds2 := deepCopyDataset(ds)
	ds2.Policy.Seed = ds.Policy.Seed + 999
	if err := eng.RunWith(ds2); err != nil {
		t.Fatalf("replacement run: %v", err)
	}
	if eng.Version != 2 {
		t.Fatalf("want version 2 after replacement, got %d", eng.Version)
	}
	if eng.Fingerprints.VersionFingerprint == firstFP {
		t.Fatal("fingerprint must change after replacement")
	}
	if len(eng.InvalidationLog) != len(ds.Network.Layers) {
		t.Fatalf("invalidations must enumerate every layer, got %d", len(eng.InvalidationLog))
	}
	rows, _ := eng.store.ListSnapshots(2)
	if len(rows) != len(ds.Network.Layers) {
		t.Fatalf("replacement must rebuild all layers, got %d", len(rows))
	}

	// Rollback: replay the original fingerprint against the newer latest version.
	ds3 := deepCopyDataset(ds)
	err := eng.RunWith(ds3)
	if err == nil || model.CodeOf(err) != model.CodeStaleVersion {
		t.Fatalf("expected E_STALE_VERSION rollback, got %v", err)
	}
}

func TestUnchangedInputIsNormalProgression(t *testing.T) {
	eng, ds := setupEngine(t, "n")
	defer eng.Close()
	if err := eng.Run(); err != nil {
		t.Fatal(err)
	}
	again := deepCopyDataset(ds)
	if err := eng.RunWith(again); err != nil {
		t.Fatalf("rerun unchanged input: %v", err)
	}
	if eng.Version != 1 {
		t.Fatalf("unchanged fingerprint must stay at version 1, got %d", eng.Version)
	}
}
