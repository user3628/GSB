package capver

import (
	"strings"
	"testing"
)

func TestPolicyValidate(t *testing.T) {
	p := GenerateSamplePolicy()
	if err := p.Validate(); err != nil {
		t.Fatalf("sample policy invalid: %v", err)
	}

	bad := &Policy{Name: "x", Processes: []ProcessDecl{{ID: "a"}}}
	if CodeOf(bad.Validate()) != CodePolicyInvalid {
		t.Fatal("empty command should be rejected")
	}

	dup := &Policy{Name: "x", Processes: []ProcessDecl{
		{ID: "a", Command: []string{"c"}},
		{ID: "a", Command: []string{"c"}},
	}}
	if CodeOf(dup.Validate()) != CodePolicyInvalid {
		t.Fatal("duplicate id should be rejected")
	}

	unknownCap := &Policy{Name: "x", Processes: []ProcessDecl{
		{ID: "a", Command: []string{"c"}, Capabilities: []Capability{"root"}},
	}}
	if CodeOf(unknownCap.Validate()) != CodePolicyInvalid {
		t.Fatal("unknown capability should be rejected")
	}
}

func TestPolicyHashDeterministic(t *testing.T) {
	p1 := GenerateSamplePolicy()
	p2 := GenerateSamplePolicy()
	if p1.Hash() != p2.Hash() {
		t.Fatal("sample generation must be deterministic")
	}
}

func TestParsePolicy(t *testing.T) {
	if _, err := ParsePolicy([]byte("{")); CodeOf(err) != CodePolicyInvalid {
		t.Fatal("invalid json should yield E_POLICY_INVALID")
	}
	if _, err := ParsePolicy([]byte(`{"name":"","processes":[]}`)); CodeOf(err) != CodePolicyInvalid {
		t.Fatal("empty name should yield E_POLICY_INVALID")
	}
}

func TestInjectKinds(t *testing.T) {
	for _, k := range AllInjectKinds() {
		if !k.Valid() {
			t.Fatalf("kind %s should be valid", k)
		}
		if k.ExpectedCode() == CodeOK {
			t.Fatalf("kind %s should map to a non-OK code", k)
		}
	}
	if InjectNone.Valid() != true {
		t.Fatal("empty inject kind should be valid")
	}
	if InjectKind("bogus").Valid() {
		t.Fatal("bogus kind should be invalid")
	}
	if !strings.Contains(string(InjectSnapshotGap.ExpectedCode()), "SNAPSHOT") {
		t.Fatal("snapshot-gap should map to E_SNAPSHOT_VERSION")
	}
}
