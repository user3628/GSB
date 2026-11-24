package engine_test

import (
	"context"
	"encoding/json"
	"testing"

	"motionlab/internal/engine"
	"motionlab/internal/model"
	"motionlab/internal/motion"
	"motionlab/internal/store"
)

func newHarness(t *testing.T) (*motion.Catalog, *store.Store, *engine.Engine) {
	t.Helper()
	dir := t.TempDir()
	if err := motion.Initialize(dir, true); err != nil {
		t.Fatal(err)
	}
	cat, err := motion.LoadCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return cat, st, engine.New(cat, st, engine.Options{})
}

func TestDefaultRunProducesPruningAndErrors(t *testing.T) {
	cat, _, eng := newHarness(t)
	ctx := context.Background()
	row, err := eng.EnsureRun(ctx, "harbor_slide", cat.Policy, model.RunConfig{SequenceID: "harbor_slide"}, true)
	if err != nil {
		t.Fatal(err)
	}
	report, err := eng.Run(ctx, row.RunID)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.MarshalIndent(report.Summary, "", "  ")
	t.Logf("summary=%s", b)
	for _, br := range report.Branches {
		t.Logf("branch=%d status=%s steps=%d mean=%.4f curve=%v reason=%s", br.BranchID, br.Status, len(br.Steps), br.MeanMAE, br.ErrorCurve, br.Reason)
	}
	if report.Branches[0].MeanMAE != 0 {
		t.Fatalf("baseline branch must match truth exactly, got %f", report.Branches[0].MeanMAE)
	}
	if report.Summary.Pruned == 0 {
		t.Fatalf("expected at least one pruned branch")
	}
	if report.DeterministicSHA == "" {
		t.Fatal("missing deterministic digest")
	}
}
