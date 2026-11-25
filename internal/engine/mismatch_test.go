package engine

import (
	"path/filepath"
	"testing"

	"qaudit/internal/generator"
	"qaudit/internal/model"
)

func TestNumericMismatchReportedButAuditProduced(t *testing.T) {
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
	// A deliberately tiny absolute tolerance forces the 4-bit layers to exceed it.
	ds.Policy.Tolerance = 1e-9
	err = eng.RunWith(ds)
	if err == nil || model.CodeOf(err) != model.CodeNumericMismatch {
		t.Fatalf("expected E_NUMERIC_MISMATCH, got %v", err)
	}
	if eng.Numeric == nil || len(eng.Numeric.Deviations) == 0 {
		t.Fatal("numeric report must retain layer/sample diagnostics")
	}
	if eng.Audit == nil || len(eng.Audit.Layers) != len(ds.Network.Layers) {
		t.Fatal("audit attribution must still be produced after mismatch")
	}
	be := err.(*model.BizError)
	if be.Context["layer"] == "" || be.Context["position"] == "" {
		t.Fatalf("mismatch diagnostic must name layer and sample position: %v", be.Context)
	}
}
