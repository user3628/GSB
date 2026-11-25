package drift

import (
	"testing"

	"driftengine/internal/fixednum"
	"driftengine/internal/spec"
)

func TestInclusiveBoundaryIsDeterministic(t *testing.T) {
	tolerance, err := fixednum.Parse("1/100")
	if err != nil {
		t.Fatal(err)
	}
	inside := fixednum.RelativeWithin(fixednum.Rat(202, 1), fixednum.Rat(200, 1), tolerance, true)
	if !inside.Within || inside.DifferenceExact != "2" || inside.AllowedExact != "2" {
		t.Fatalf("unexpected inclusive boundary: %+v", inside)
	}
	outside := fixednum.RelativeWithin(fixednum.Rat(203, 1), fixednum.Rat(200, 1), tolerance, true)
	if outside.Within {
		t.Fatal("203/200 should exceed 1% inclusive tolerance")
	}
	exclusive := fixednum.RelativeWithin(fixednum.Rat(202, 1), fixednum.Rat(200, 1), tolerance, false)
	if exclusive.Within {
		t.Fatal("equality must be outside for exclusive boundary")
	}
}

func TestAllCategoriesAndStablePrimary(t *testing.T) {
	object := spec.Object{
		ID:      "multi",
		Desired: spec.Fields{"missing": "x", "same": "v", "replicas": 4},
		Actual:  spec.Fields{"extra": "y", "same": "v", "replicas": 5},
		SourceMarkers: map[string]spec.SourceMarker{
			"replicas": {Desired: "git", Actual: "hotfix"},
		},
	}
	policy := spec.Policy{
		NumericRelativeTolerance: "0",
		ToleranceBoundary:        "inclusive",
		CategoryPriority:         spec.DefaultPriority(),
	}
	report := Evaluate(object, policy)
	if report.PrimaryCategory != spec.CategorySourceConflict {
		t.Fatalf("primary=%s", report.PrimaryCategory)
	}
	got := map[string]bool{}
	for _, category := range report.Categories {
		got[category] = true
	}
	for _, category := range spec.DefaultPriority() {
		if !got[category] {
			t.Fatalf("missing category %s in %v", category, report.Categories)
		}
	}
}
