package pipeline

import (
	"testing"

	"headprune/internal/generate"
	"headprune/internal/model"
)

func testDataset(t *testing.T) model.Dataset {
	t.Helper()
	return generate.Build().Dataset
}

func assertCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error %s, got nil", code)
	}
	be, ok := model.AsBizError(err)
	if !ok {
		t.Fatalf("error is not a BizError: %v", err)
	}
	if be.Code != code {
		t.Fatalf("expected code %s, got %s (%v)", code, be.Code, err)
	}
}

func TestStage1DeterministicAndCoverage(t *testing.T) {
	ds := testDataset(t)
	quant, err := QuantizeHeads(ds.Heads, ds.Policy.QuantBits)
	if err != nil {
		t.Fatal(err)
	}
	r1, err := Stage1(ds, quant)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := Stage1(ds, quant)
	if err != nil {
		t.Fatal(err)
	}
	b1, _ := model.CanonicalJSON(r1)
	b2, _ := model.CanonicalJSON(r2)
	if string(b1) != string(b2) {
		t.Fatal("per-head error table is not byte-identical across runs")
	}
	if len(r1.Probes) != ds.Policy.ProbeLimit {
		t.Fatalf("probe count = %d, want %d", len(r1.Probes), ds.Policy.ProbeLimit)
	}
	if len(r1.HeadErrors) != len(ds.Heads) {
		t.Fatalf("head error rows = %d, want %d", len(r1.HeadErrors), len(ds.Heads))
	}
	for _, row := range r1.HeadErrors {
		if len(row.ProbeCoverage) != len(r1.Probes) {
			t.Fatalf("coverage mismatch for %s", row.Head)
		}
	}
}

func TestInsufficientProbeCarriesHead(t *testing.T) {
	ds := testDataset(t)
	ds.Policy.MinProbes = 100
	quant, err := QuantizeHeads(ds.Heads, ds.Policy.QuantBits)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Stage1(ds, quant)
	assertCode(t, err, model.CodeInsufficientProbe)
	be, _ := model.AsBizError(err)
	if be.Layer == nil || be.Head == nil {
		t.Fatal("E_INSUFFICIENT_PROBE must carry layer and head coordinates")
	}
}

func TestProbeLimitZero(t *testing.T) {
	ds := testDataset(t)
	ds.Policy.ProbeLimit = 0
	if err := ValidatePolicy(ds.Policy); err == nil {
		t.Fatal("expected policy validation failure for zero probe limit")
	} else {
		assertCode(t, err, model.CodePolicyInvalid)
	}
}

func TestNegativeTolerance(t *testing.T) {
	ds := testDataset(t)
	ds.Policy.Tolerance = -1
	assertCode(t, ValidatePolicy(ds.Policy), model.CodePolicyInvalid)
}

func TestHeadDimMismatch(t *testing.T) {
	ds := testDataset(t)
	ds.Heads[0].Weight = ds.Heads[0].Weight[:2]
	assertCode(t, ValidateDataset(ds), model.CodeHeadInvalid)
}

func TestMissingTruthCandidate(t *testing.T) {
	ds := testDataset(t)
	for i := range ds.Sentences[0].Candidates {
		ds.Sentences[0].Candidates[i].IsTrue = false
	}
	assertCode(t, ValidateDataset(ds), model.CodeSentenceInvalid)
}

func TestQuantizationZeroScaleAndNaN(t *testing.T) {
	ds := testDataset(t)
	for i := range ds.Heads[2].Activations {
		ds.Heads[2].Activations[i] = 0
	}
	_, err := QuantizeHeads(ds.Heads, 4)
	assertCode(t, err, model.CodeQuantization)
	be, _ := model.AsBizError(err)
	if be.Layer == nil || be.Head == nil {
		t.Fatal("quantization error must carry layer and head")
	}

	ds2 := testDataset(t)
	ds2.Heads[1].Activations[0] = nan()
	_, err = QuantizeHeads(ds2.Heads, 4)
	assertCode(t, err, model.CodeQuantization)
}

func TestCouplingIsReplayedNotPredicted(t *testing.T) {
	ds := testDataset(t)
	quant, err := QuantizeHeads(ds.Heads, ds.Policy.QuantBits)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := Stage1(ds, quant)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Stage2(ds, probe.Probes, quant, probe.HeadErrors)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Coupled) == 0 {
		t.Fatal("generator must produce at least one strongly coupled combo")
	}
	for _, c := range report.Coupled {
		if c.Amplification <= 1 {
			t.Fatalf("coupled combo amplification must exceed 1: %v", c.Amplification)
		}
		if len(c.Contributions) != c.Size {
			t.Fatal("contribution log must cover every combo head")
		}
	}
}

func TestEmptyDecisionWhenToleranceTooTight(t *testing.T) {
	ds := testDataset(t)
	ds.Policy.Tolerance = 1e-9
	quant, err := QuantizeHeads(ds.Heads, ds.Policy.QuantBits)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := Stage1(ds, quant)
	if err != nil {
		t.Fatal(err)
	}
	combos, err := Stage2(ds, probe.Probes, quant, probe.HeadErrors)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := Stage3(ds, probe.HeadErrors, combos)
	if err != nil {
		t.Fatal(err)
	}
	if !decision.Empty {
		t.Fatal("expected explicit empty decision when no combo fits tolerance")
	}
	if len(decision.Selected) != 0 {
		t.Fatal("empty decision must select zero heads")
	}
	if decision.ClosestCombo == nil || len(decision.ClosestCombo) == 0 {
		t.Fatal("empty decision must name the closest combo")
	}
}

func TestDecisionDeterministic(t *testing.T) {
	ds := testDataset(t)
	quant, _ := QuantizeHeads(ds.Heads, ds.Policy.QuantBits)
	probe, _ := Stage1(ds, quant)
	combos, _ := Stage2(ds, probe.Probes, quant, probe.HeadErrors)
	d1, _ := Stage3(ds, probe.HeadErrors, combos)
	d2, _ := Stage3(ds, probe.HeadErrors, combos)
	if d1.Fingerprint() != d2.Fingerprint() {
		t.Fatal("decision digest must be deterministic")
	}
}

func TestLineageClassifications(t *testing.T) {
	ds := testDataset(t)
	// Head append: new head id only.
	dsApp := testDataset(t)
	extra := dsApp.Heads[0]
	extra.Head = 9
	extra.Layer = 9
	dsApp.Heads = append(dsApp.Heads, extra)
	v, err := ClassifyHeads(ds, dsApp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v.Relation != RelationAppend || v.FullInvalidation {
		t.Fatalf("expected append without full invalidation, got %+v", v)
	}
	// Head replacement: existing head changed.
	dsRep := testDataset(t)
	dsRep.Heads[0].Weight[0] += 1
	v, err = ClassifyHeads(ds, dsRep, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v.Relation != RelationReplace || !v.FullInvalidation {
		t.Fatalf("expected replacement with full invalidation, got %+v", v)
	}
	// Policy rollback via sequence decrease.
	dsPol := testDataset(t)
	dsPol.Policy.Version = 0
	v, err = ClassifyPolicy(ds, dsPol, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v.Relation != RelationRollback {
		t.Fatalf("expected policy rollback, got %s", v.Relation)
	}
}
