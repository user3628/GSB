package report

import (
	"bytes"
	"encoding/json"
	"testing"

	"secverify/internal/engine"
)

func TestMarshalIsDeterministicAndSorted(t *testing.T) {
	r := VerificationReport{
		Scenario: "unit",
		Checks: []CheckAssertion{
			{Name: "z", Expected: "1", Actual: "1", Passed: true},
			{Name: "a", Expected: "1", Actual: "1", Passed: true},
		},
		Namespaces: []NamespaceReport{
			{Namespace: "z"},
			{Namespace: "a"},
		},
		Injections: []InjectionAssertion{
			{Scenario: "z", ExpectedCode: "Z", Passed: true},
			{Scenario: "a", ExpectedCode: "A", Passed: true},
		},
	}
	first, err := Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("report encoding is not deterministic")
	}
	var decoded VerificationReport
	if err := json.Unmarshal(first, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Checks[0].Name != "a" || decoded.Namespaces[0].Namespace != "a" || decoded.Injections[0].Scenario != "a" {
		t.Fatalf("report was not stably sorted: %+v", decoded)
	}
	if decoded.Result != "PASS" {
		t.Fatalf("result = %s", decoded.Result)
	}
}

func TestFailureCheckFlipsResult(t *testing.T) {
	r := VerificationReport{Scenario: "unit", Checks: []CheckAssertion{{Name: "bad", Expected: "x", Actual: "y", Passed: false}}}
	data, err := Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var decoded VerificationReport
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Result != "FAIL" {
		t.Fatalf("result = %s, want FAIL", decoded.Result)
	}
}

func TestEmptySlicesEncodeAsArrays(t *testing.T) {
	r := VerificationReport{Scenario: "unit"}
	data, err := Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("null")) {
		t.Fatalf("report contains null collection: %s", data)
	}
}

var _ = engine.RoundResult{}
