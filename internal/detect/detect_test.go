package detect

import (
	"reflect"
	"testing"

	"driftconverge/internal/model"
)

func TestSampleDriftConclusions(t *testing.T) {
	samples, err := model.LoadSamples("../../samples/cluster-objects.json", "../../samples/policy-snapshot.json", "../../samples/drift-field-values.json")
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(samples.Cluster, samples.Policy, samples.Values)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Reconciled || report.ObjectsCompared != len(samples.Cluster.Objects) {
		t.Fatalf("paths not reconciled: %+v", report)
	}
	if report.BucketMethod == "" {
		t.Fatal("bucket method must be documented")
	}
	web := objectResult(t, report.Results, "prod/deployment/web")
	if web.PrimaryCategory != SourceConflict {
		t.Fatalf("source conflict must be primary: %+v", web.Categories)
	}
	want := []Category{SourceConflict, ValueOffset, FieldExtra}
	if !reflect.DeepEqual(web.Categories, want) {
		t.Fatalf("categories=%v want %v", web.Categories, want)
	}
	if hasField(web, "ratio") {
		t.Fatal("0.7502 versus 0.75 at 1% must stay inside tolerance")
	}
	service := objectResult(t, report.Results, "prod/service/web")
	if !hasCategory(service, FieldMissing) {
		t.Fatalf("service missing port not classified: %+v", service)
	}
	if len(report.ValueCaseChecks) != len(samples.Values.Cases) {
		t.Fatalf("value cases checked=%d want %d", len(report.ValueCaseChecks), len(samples.Values.Cases))
	}
	for _, check := range report.ValueCaseChecks {
		if !check.ExpectedPass {
			t.Fatalf("value case failed: %+v", check)
		}
	}
}

func TestAdaptivePathSelection(t *testing.T) {
	samples, err := model.LoadSamples("../../samples/cluster-objects.json", "../../samples/policy-snapshot.json", "../../samples/drift-field-values.json")
	if err != nil {
		t.Fatal(err)
	}
	exact, err := Analyze(samples.Cluster, samples.Policy, samples.Values)
	if err != nil {
		t.Fatal(err)
	}
	samples.Policy.ExactObjectLimit = 1
	bucketed, err := Analyze(samples.Cluster, samples.Policy, samples.Values)
	if err != nil {
		t.Fatal(err)
	}
	if exact.SelectedPath != "exact" || bucketed.SelectedPath != "bucketed" {
		t.Fatalf("selection paths exact=%s bucketed=%s", exact.SelectedPath, bucketed.SelectedPath)
	}
	if !sameConclusions(exact.Results, bucketed.Results) {
		t.Fatal("adaptive paths changed conclusions")
	}
}

func TestRepeatDeterminism(t *testing.T) {
	samples, err := model.LoadSamples("../../samples/cluster-objects.json", "../../samples/policy-snapshot.json", "../../samples/drift-field-values.json")
	if err != nil {
		t.Fatal(err)
	}
	first, err := Analyze(samples.Cluster, samples.Policy, samples.Values)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Analyze(samples.Cluster, samples.Policy, samples.Values)
	if err != nil {
		t.Fatal(err)
	}
	if !sameConclusions(first.Results, second.Results) {
		t.Fatal("repeated drift decisions are not identical")
	}
}

func TestReconcileDetectsDivergence(t *testing.T) {
	left := []ObjectDrift{{ObjectID: "a", HasDrift: true, Categories: []Category{ValueOffset}, PrimaryCategory: ValueOffset}}
	right := []ObjectDrift{{ObjectID: "a", HasDrift: true, Categories: []Category{FieldMissing}, PrimaryCategory: FieldMissing}}
	divergences := reconcile(left, right)
	if len(divergences) != 1 || divergences[0].ObjectID != "a" {
		t.Fatalf("expected divergence: %+v", divergences)
	}
}

func objectResult(t *testing.T, results []ObjectDrift, id string) ObjectDrift {
	t.Helper()
	for _, result := range results {
		if result.ObjectID == id {
			return result
		}
	}
	t.Fatalf("object %s not found", id)
	return ObjectDrift{}
}

func hasField(result ObjectDrift, field string) bool {
	for _, drift := range result.FieldDrifts {
		if drift.Field == field {
			return true
		}
	}
	return false
}

func hasCategory(result ObjectDrift, category Category) bool {
	for _, got := range result.Categories {
		if got == category {
			return true
		}
	}
	return false
}

func sameConclusions(left, right []ObjectDrift) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		a, b := left[i], right[i]
		if a.ObjectID != b.ObjectID || a.HasDrift != b.HasDrift || a.PrimaryCategory != b.PrimaryCategory || !reflect.DeepEqual(a.Categories, b.Categories) || !reflect.DeepEqual(a.FieldDrifts, b.FieldDrifts) {
			return false
		}
	}
	return true
}
