package convergence

import (
	"math"
	"strings"
	"testing"
)

func TestDriftAllFourCategoriesAndStablePrimary(t *testing.T) {
	object := Object{
		ID: "mixed", ClusterID: "c", Kind: "ConfigMap", Namespace: "ns",
		Expected: FieldSet{
			"missing":  field("v1", "git"),
			"shift":    field(3, "git"),
			"source":   field("same-value", "git"),
			"balanced": field(D("2.00"), "git"),
		},
		Actual: FieldSet{
			"extra":    field("x", "live"),
			"shift":    field(9, "git"),
			"source":   field("same-value", "manual"),
			"balanced": field(D("2.01"), "git"),
		},
	}
	report, err := DriftObject(object, SamplePolicy())
	if err != nil {
		t.Fatalf("DriftObject returned error: %v", err)
	}
	if report.PrimaryCategory != CategorySourceConflict {
		t.Fatalf("primary = %q, want %q; categories=%v", report.PrimaryCategory, CategorySourceConflict, report.Categories)
	}
	seen := map[string]bool{}
	for _, detail := range report.Details {
		seen[detail.Category] = true
		if detail.Basis == "" || detail.ReasonCode == "" {
			t.Fatalf("detail for %s lacks basis/reason: %+v", detail.Category, detail)
		}
	}
	for _, category := range []string{CategoryMissing, CategoryExtra, CategoryValueShift, CategorySourceConflict} {
		if !seen[category] {
			t.Fatalf("category %s not represented in details: %+v", category, report.Details)
		}
	}
	if containsString(report.AffectedFields, "balanced") {
		t.Fatalf("within-tolerance field should not be drift: %+v", report.Details)
	}
}

func TestNumericComparisonIsExactFixedPointAndNotFloat(t *testing.T) {
	policy := SamplePolicy()
	policy.DriftTolerance = D("0.0001")

	within, basis, extra := compareFieldValue(D("1.000000000000"), D("1.00005"), policy.DriftTolerance)
	if !within {
		t.Fatalf("expected relative delta 5e-5 to be within 1e-4: %s %s", basis, extra.relativeDelta)
	}
	outside, _, _ := compareFieldValue(D("1"), D("1.0002"), policy.DriftTolerance)
	if outside {
		t.Fatal("expected relative delta 2e-4 to be outside 1e-4")
	}

	// A naive float64 implementation can lose this distinction at large magnitudes.
	largeWithin, _, _ := compareFieldValue(D("1000000000000"), D("1000000000000.05"), D("0.000001"))
	if !largeWithin {
		t.Fatal("expected exact decimal comparison to accept tiny relative delta")
	}
	if math.IsNaN(0) {
		t.Fatal("unreachable")
	}
}

func TestIncompleteFieldSetsReturnBusinessErrorInsteadOfSilentConvergence(t *testing.T) {
	policy := SamplePolicy()
	for name, object := range map[string]Object{
		"nil expected": {ID: "x", Expected: nil, Actual: FieldSet{}},
		"nil actual":   {ID: "x", Expected: FieldSet{}, Actual: nil},
	} {
		t.Run(name, func(t *testing.T) {
			report, err := DriftObject(object, policy)
			if err == nil {
				t.Fatalf("expected error, got report converged=%v", report != nil && report.Converged)
			}
			if !HasCode(err, CodeSpecInvalid) {
				t.Fatalf("code = %v, want E_SPEC_INVALID", AsError(err))
			}
			if report != nil {
				t.Fatalf("report must be nil on incomplete input")
			}
		})
	}

	report, err := DriftObject(Object{ID: "empty", Expected: FieldSet{}, Actual: FieldSet{}}, policy)
	if err != nil || !report.Converged {
		t.Fatalf("explicit empty sets should be valid and converged: report=%+v err=%v", report, err)
	}
}

func TestSampleDriftContainsStableCategories(t *testing.T) {
	snapshot := SampleSnapshotV1()
	reports, byID, err := JudgeSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != len(snapshot.Objects) {
		t.Fatalf("reports=%d objects=%d", len(reports), len(snapshot.Objects))
	}
	mixed := byID["foxtrot-cache"]
	if mixed.PrimaryCategory != CategorySourceConflict || len(mixed.Categories) < 4 {
		t.Fatalf("foxtrot should hit all categories with source primary: %+v", mixed)
	}
	if !strings.Contains(mixed.Details[0].CategoryZh, "冲突") && mixed.Details[0].Category != CategorySourceConflict {
		t.Fatalf("expected Chinese category label, got %q", mixed.Details[0].CategoryZh)
	}
}
