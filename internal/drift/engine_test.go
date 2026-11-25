package drift_test

import (
	"testing"

	"auditdrift/internal/drift"
	"auditdrift/internal/model"
)

func TestIntervalEndpointInclusionAndOffset(t *testing.T) {
	b := model.Baseline{
		ID: "AUDIT_OBJECT_ACCESS", Category: model.CategoryAuditPolicy, SeverityWeight: 6,
		Expected: model.Expectation{Interval: &model.Interval{Min: 2, Max: 4, MinInclusive: true, MaxInclusive: false}},
	}
	c, err := drift.EvaluateTarget("s", "h", b, []model.Observation{{Actual: model.Value{Kind: model.KindNumber, Number: 4}}}, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if c.DriftCategory != model.DriftValueOffset || c.OwnLevel == 0 {
		t.Fatalf("boundary value should drift: %+v", c)
	}

	c, err = drift.EvaluateTarget("s", "h", b, []model.Observation{{Actual: model.Value{Kind: model.KindNumber, Number: 3}}}, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if c.DriftCategory != model.DriftNone || c.DriftLevel != 0 {
		t.Fatalf("3 should be inside [2,4): %+v", c)
	}
}

func TestTypeMismatchReturnsCodedError(t *testing.T) {
	b := model.Baseline{ID: "acct", Category: model.CategoryAccountPolicy, SeverityWeight: 2, Expected: model.Expectation{Interval: &model.Interval{Min: 1, Max: 2, MinInclusive: true, MaxInclusive: true}}}
	_, err := drift.EvaluateTarget("s", "h", b, []model.Observation{{Actual: model.Value{Kind: model.KindSet, Set: []string{"x"}}}}, nil, 1)
	if model.CodeOf(err) != model.ErrValueTypeMismatch {
		t.Fatalf("err=%v code=%q", err, model.CodeOf(err))
	}
}

func TestServiceAndRegistryCategories(t *testing.T) {
	svc := model.Baseline{ID: "svc", Category: model.CategoryServiceStartup, SeverityWeight: 8, Expected: model.Expectation{ExpectedText: "Automatic", AllowedEnum: []string{"Automatic", "Manual", "Disabled"}}}
	c, err := drift.EvaluateTarget("s", "h", svc, nil, &drift.SourceValue{Source: "probe", Value: model.Value{Kind: model.KindString, Text: "Manual"}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if c.DriftCategory != model.DriftValueOffset || c.DriftLevel < 3 {
		t.Fatalf("service drift=%+v", c)
	}

	reg := model.Baseline{ID: "reg", Category: model.CategoryRegistryACL, SeverityWeight: 9, Expected: model.Expectation{RequiredSet: []string{"SYSTEM:Full", "Admins:Read"}}}
	c, err = drift.EvaluateTarget("s", "h", reg, []model.Observation{{Actual: model.Value{Kind: model.KindSet, Set: []string{"SYSTEM:Full", "Everyone:Read"}}}}, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if c.DriftCategory != model.DriftValueOffset || len(c.MissingElements) != 1 || len(c.ExtraElements) != 1 {
		t.Fatalf("registry mismatch=%+v", c)
	}

	regExtraOnly := reg
	c, err = drift.EvaluateTarget("s", "h", regExtraOnly, []model.Observation{{Actual: model.Value{Kind: model.KindSet, Set: []string{"SYSTEM:Full", "Admins:Read", "Everyone:Read"}}}}, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if c.DriftCategory != model.DriftExtra {
		t.Fatalf("expected extra, got %s", c.DriftCategory)
	}
}

func TestSourceConflict(t *testing.T) {
	b := model.Baseline{ID: "svc", Category: model.CategoryServiceStartup, SeverityWeight: 5, Expected: model.Expectation{ExpectedText: "Automatic", AllowedEnum: []string{"Automatic", "Manual", "Disabled"}}}
	obs := []model.Observation{
		{Source: "agent", Actual: model.Value{Kind: model.KindString, Text: "Manual"}},
		{Source: "old-tool", Actual: model.Value{Kind: model.KindString, Text: "Disabled"}},
	}
	c, err := drift.EvaluateTarget("s", "h", b, obs, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if c.DriftCategory != model.DriftSourceConflict {
		t.Fatalf("category=%s details=%v", c.DriftCategory, c.Details)
	}
}
