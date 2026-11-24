package model

import (
	"strings"
	"testing"

	"driftconverge/internal/apperr"
)

func TestLoadSamplesAndChecksum(t *testing.T) {
	samples, err := LoadSamples("../../samples/cluster-objects.json", "../../samples/policy-snapshot.json", "../../samples/drift-field-values.json")
	if err != nil {
		t.Fatal(err)
	}
	if samples.Policy.Version != 7 || samples.Cluster.SnapshotVersion != 7 {
		t.Fatalf("unexpected versions: policy=%d cluster=%d", samples.Policy.Version, samples.Cluster.SnapshotVersion)
	}
	copy := *samples.Policy
	copy.Checksum = "sha256:bad"
	if err := copy.Validate(); apperr.CodeOf(err) != apperr.SpecInvalid {
		t.Fatalf("checksum mismatch must be E_SPEC_INVALID: %v", err)
	}
}

func TestClusterValidationFailures(t *testing.T) {
	tests := []struct {
		name   string
		object []Object
	}{
		{"duplicate id", []Object{baseObject("a"), baseObject("a")}},
		{"missing dependency", []Object{{ID: "a", Kind: "K", Namespace: "n", DependsOn: []string{"missing"}, ResourceBudget: 1}}},
		{"negative budget", []Object{{ID: "a", Kind: "K", Namespace: "n", ResourceBudget: -1}}},
		{"budget overflow", []Object{{ID: "a", Kind: "K", Namespace: "n", ResourceBudget: 1<<63 - 1}, {ID: "b", Kind: "K", Namespace: "n", ResourceBudget: 1}}},
		{"empty expected field", []Object{{ID: "a", Kind: "K", Namespace: "n", Expected: map[string]FieldValue{" ": {Type: TypeString, Value: "x"}}, ResourceBudget: 1}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cluster := &ClusterSpec{SnapshotVersion: 1, Objects: tc.object}
			err := cluster.NormalizeAndValidate()
			if apperr.CodeOf(err) != apperr.SpecInvalid {
				t.Fatalf("want E_SPEC_INVALID, got %v", err)
			}
		})
	}
}

func TestDecodeStrictRejectsTrailingJSON(t *testing.T) {
	payload := `{"snapshotVersion":1,"objects":[]} {"extra":true}`
	if _, err := DecodeCluster([]byte(payload)); apperr.CodeOf(err) != apperr.SpecInvalid {
		t.Fatalf("trailing JSON must fail validation: %v", err)
	}
}

func TestNormalizationPreservesFloatPrecision(t *testing.T) {
	cluster := &ClusterSpec{SnapshotVersion: 1, Objects: []Object{{
		ID: "a", Kind: "K", Namespace: "n", ResourceBudget: 1,
		Expected: map[string]FieldValue{"x": {Type: TypeFloat, Value: "0.123456789012345678901"}},
		Actual:   map[string]FieldValue{"x": {Type: TypeFloat, Value: "0.123456789012345678901"}},
		Sources:  map[string]FieldSource{},
	}}}
	if err := cluster.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if got := cluster.Objects[0].Expected["x"].Value; got != "0.123456789012345678901" {
		t.Fatalf("float text lost precision: %s", got)
	}
}

func TestIntegerNormalization(t *testing.T) {
	cluster := &ClusterSpec{SnapshotVersion: 1, Objects: []Object{{
		ID: "a", Kind: "K", Namespace: "n", ResourceBudget: 1,
		Expected: map[string]FieldValue{"x": {Type: TypeInteger, Value: "0007"}},
		Actual:   map[string]FieldValue{"x": {Type: TypeInteger, Value: "+7"}},
		Sources:  map[string]FieldSource{},
	}}}
	if err := cluster.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if cluster.Objects[0].Expected["x"].Value != "7" || cluster.Objects[0].Actual["x"].Value != "7" {
		t.Fatalf("integers not canonicalized: %+v", cluster.Objects[0])
	}
}

func TestDecodePolicyUnknownField(t *testing.T) {
	payload := `{"version":1,"generatedAt":"2026-11-20T08:00:00Z","unknown":true}`
	if _, err := DecodePolicy([]byte(payload)); !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("expected strict unknown-field error, got %v", err)
	}
}

func baseObject(id string) Object {
	return Object{ID: id, Kind: "K", Namespace: "n", ResourceBudget: 1}
}
