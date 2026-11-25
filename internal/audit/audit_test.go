package audit_test

import (
	"path/filepath"
	"testing"

	"qaudit/internal/audit"
	"qaudit/internal/engine"
	"qaudit/internal/generator"
	"qaudit/internal/infer"
	"qaudit/internal/model"
)

func buildPipeline(t *testing.T) *engine.Engine {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "d")
	ds, err := generator.Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng, err := engine.New(dir, filepath.Join(dir, "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.SaveDataset(ds); err != nil {
		t.Fatal(err)
	}
	if err := eng.Run(); err != nil {
		t.Fatalf("run: %v", err)
	}
	return eng
}

func TestAuditBoundsCoverAndRanks(t *testing.T) {
	eng := buildPipeline(t)
	defer eng.Close()
	view := eng.AuditView()
	rep := view["audit"].(*audit.Report)
	num := view["numeric"].(*infer.NumericReport)
	if len(rep.Layers) != 4 {
		t.Fatalf("want 4 layer audits, got %d", len(rep.Layers))
	}
	worst := map[string]float64{}
	for _, d := range num.Deviations {
		if d.MaxAbsDev > worst[d.Layer] {
			worst[d.Layer] = d.MaxAbsDev
		}
	}
	for _, l := range rep.Layers {
		if l.UpperBound <= 0 || l.WeightStep <= 0 || l.ActivationStep <= 0 {
			t.Fatalf("layer %s has non-positive bound/step: %+v", l.Layer, l)
		}
		if l.UpperBound < worst[l.Layer] {
			t.Fatalf("layer %s bound %g does not cover observed %g", l.Layer, l.UpperBound, worst[l.Layer])
		}
		if l.SensitivityRank < 1 || l.ReplayRank < 1 {
			t.Fatalf("layer %s missing ranking basis", l.Layer)
		}
		if l.NeedsReview && l.ReviewReason == "" {
			t.Fatalf("review flag without reason on %s", l.Layer)
		}
	}
}

func TestAuditRejectsMissingReferences(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	ds, err := generator.Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	ds.References = nil
	eng, err := engine.New(dir, filepath.Join(dir, "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if err := eng.SaveDataset(ds); err != nil {
		t.Fatal(err)
	}
	err = eng.Run()
	if err == nil || model.CodeOf(err) != model.CodeAuditInput {
		t.Fatalf("expected E_AUDIT_INPUT for missing references, got %v", err)
	}
}
