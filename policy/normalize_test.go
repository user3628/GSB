package policy

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"sandboxpolicy/business"
)

func TestNormalizeIsStableAcrossInputOrder(t *testing.T) {
	baseline := loadSamplePolicy(t)
	shuffled := baseline.Clone()
	reverseDirectories(shuffled.Directories)
	reverseGroups(shuffled.ProcessGroups)
	reverseRegistry(shuffled.RegistryPaths)
	reverseRules(shuffled.Rules)
	for i := range shuffled.Directories {
		reverseStrings(shuffled.Directories[i].Access)
	}
	for i := range shuffled.ProcessGroups {
		reverseStrings(shuffled.ProcessGroups[i].Executables)
	}
	for i := range shuffled.Rules {
		shuffled.Rules[i].Effect = stringsUpper(shuffled.Rules[i].Effect)
		shuffled.Rules[i].Action = stringsUpper(shuffled.Rules[i].Action)
		reverseStrings(shuffled.Rules[i].DependsOn)
	}
	if err := NormalizeStrict(shuffled); err != nil {
		t.Fatalf("normalize reordered policy: %v", err)
	}
	want, err := DigestPolicy(baseline)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DigestPolicy(shuffled)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("canonical digest changed with input order: %s != %s", got, want)
	}
	first, _ := CanonicalJSON(baseline)
	second, _ := CanonicalJSON(shuffled)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("canonical bytes differ:\n%s\n%s", first, second)
	}
}

func TestMalformedPoliciesReturnSpecificBusinessCodes(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Policy)
		code   business.Code
		reason string
	}{
		{
			name: "duplicate",
			mutate: func(p *Policy) {
				p.Directories[1].ID = p.Directories[0].ID
			},
			code:   business.CodePolicyMalformed,
			reason: business.ReasonDuplicateIdentifier,
		},
		{
			name: "missing-ref",
			mutate: func(p *Policy) {
				p.Rules[0].Ref = "dir.missing"
			},
			code:   business.CodePolicyMalformed,
			reason: business.ReasonReferenceMissing,
		},
		{
			name: "rule-cycle",
			mutate: func(p *Policy) {
				p.Rules[0].DependsOn = []string{p.Rules[1].ID}
				p.Rules[1].DependsOn = []string{p.Rules[0].ID}
			},
			code:   business.CodePolicyMalformed,
			reason: business.ReasonDependencyCycle,
		},
		{
			name: "cross-namespace",
			mutate: func(p *Policy) {
				p.Rules[0].Ref = "other/ns::dir.data"
			},
			code: business.CodeNamespaceScope,
		},
		{
			name: "layer-mismatch",
			mutate: func(p *Policy) {
				p.Rules[0].Action = ActionProcessStart
			},
			code:   business.CodePolicyMalformed,
			reason: business.ReasonLayerMismatch,
		},
		{
			name: "hierarchy",
			mutate: func(p *Policy) {
				p.Directories[1].Path = "work\\outside"
			},
			code:   business.CodePolicyMalformed,
			reason: business.ReasonHierarchyInvalid,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := loadSamplePolicy(t)
			test.mutate(p)
			err := NormalizeStrict(p)
			if err == nil {
				t.Fatal("expected malformed policy")
			}
			be := business.From(err)
			if be.Code != test.code {
				t.Fatalf("code = %s, want %s: %v", be.Code, test.code, err)
			}
			if test.reason != "" && be.Reason != test.reason {
				t.Fatalf("reason = %s, want %s", be.Reason, test.reason)
			}
			if be.Entry == "" {
				t.Fatal("malformed error must identify a specific entry")
			}
		})
	}
}

func TestParsePolicyRejectsUnknownFields(t *testing.T) {
	_, err := ParsePolicy([]byte(`{"schema_version":1,"namespace":"n","directories":[],"process_groups":[],"registry_paths":[],"rules":[],"surprise":true}`))
	if err == nil {
		t.Fatal("expected unknown-field error")
	}
	var be *business.Error
	if !errors.As(err, &be) || be.Code != business.CodePolicyMalformed {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNormalizeWindowsDriveRootAndOperationID(t *testing.T) {
	for input, want := range map[string]string{
		`C:\`:          `C:\`,
		`c:/`:          `c:\`,
		`C:\work\..\`:  `C:\`,
		`C:\work\data`: `C:\work\data`,
	} {
		got, err := normalizePath(input)
		if err != nil {
			t.Fatalf("normalize path %q: %v", input, err)
		}
		if got != want {
			t.Fatalf("normalize path %q = %q, want %q", input, got, want)
		}
	}
	cs := &ChangeSet{Namespace: "ns", Operations: []Operation{{Op: "delete", Kind: "directory", ID: " dir.root "}}}
	if err := NormalizeChangeSet(cs); err != nil {
		t.Fatal(err)
	}
	if cs.Operations[0].ID != "dir.root" {
		t.Fatalf("operation id was not normalized: %q", cs.Operations[0].ID)
	}
}

func loadSamplePolicy(t *testing.T) *Policy {
	t.Helper()
	path := filepath.Join("..", "samples", "policies", "baseline.json")
	p, err := ParsePolicyFile(path)
	if err != nil {
		t.Fatalf("parse baseline: %v", err)
	}
	return p
}

func reverseDirectories(values []Directory) {
	for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
		values[i], values[j] = values[j], values[i]
	}
}

func reverseGroups(values []ProcessGroup) {
	for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
		values[i], values[j] = values[j], values[i]
	}
}

func reverseRegistry(values []RegistryPath) {
	for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
		values[i], values[j] = values[j], values[i]
	}
}

func reverseRules(values []Rule) {
	for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
		values[i], values[j] = values[j], values[i]
	}
}

func reverseStrings(values []string) {
	for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
		values[i], values[j] = values[j], values[i]
	}
}

func stringsUpper(value string) string {
	out := []byte(value)
	for i := range out {
		if out[i] >= 'a' && out[i] <= 'z' {
			out[i] -= 'a' - 'A'
		}
	}
	return string(out)
}

func TestLoadSamplePolicy(t *testing.T) {
	p := loadSamplePolicy(t)
	if len(p.Rules) != 10 {
		t.Fatalf("rules = %d, want 10", len(p.Rules))
	}
	if _, err := os.Stat(filepath.Join("..", "samples", "changes", "01-add-cache.json")); err != nil {
		t.Fatal(err)
	}
}
