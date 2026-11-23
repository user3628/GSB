package compat

import (
	"reflect"
	"testing"

	"meshmigrate/internal/model"
)

func TestExplicitMatrixThreeDecisionsAreDeterministic(t *testing.T) {
	m, err := LoadMatrix()
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name     string
		parsed   model.ParsedSnapshot
		decision string
		unknown  []string
	}{
		{"binary legacy", model.ParsedSnapshot{SourceFormat: model.FormatBinary, FormatVersion: 1}, DecisionDowngrade, nil},
		{"binary current", model.ParsedSnapshot{SourceFormat: model.FormatBinary, FormatVersion: 2}, DecisionAccept, nil},
		{"binary future", model.ParsedSnapshot{SourceFormat: model.FormatBinary, FormatVersion: 3}, DecisionReject, nil},
		{"json legacy", model.ParsedSnapshot{SourceFormat: model.FormatJSONL, FormatVersion: 1}, DecisionDowngrade, nil},
		{"json current", model.ParsedSnapshot{SourceFormat: model.FormatJSONL, FormatVersion: 2}, DecisionAccept, nil},
		{"json unknown required", model.ParsedSnapshot{SourceFormat: model.FormatJSONL, FormatVersion: 3, RequiredFeatures: []string{"geoAffinity"}}, DecisionReject, []string{"geoAffinity"}},
		{"undeclared version", model.ParsedSnapshot{SourceFormat: model.FormatJSONL, FormatVersion: 9}, DecisionReject, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			first := m.Decide(&tc.parsed)
			second := m.Decide(&tc.parsed)
			if first.Decision != tc.decision {
				t.Fatalf("got %s want %s (%s)", first.Decision, tc.decision, first.Reason)
			}
			if !reflect.DeepEqual(first, second) {
				t.Fatalf("decision is not deterministic:\n%+v\n%+v", first, second)
			}
			if !reflect.DeepEqual(first.UnknownFields, tc.unknown) {
				t.Fatalf("unknown fields got %v want %v", first.UnknownFields, tc.unknown)
			}
		})
	}
}
