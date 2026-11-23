package policy

import (
	"path/filepath"
	"slices"
	"testing"

	"sandboxpolicy/business"
)

func TestApplyDeleteComputesDirectAndTransitiveClosure(t *testing.T) {
	baseline := loadSamplePolicy(t)
	p := baseline
	for _, change := range []string{"01-add-cache.json", "02-delete-directory.json"} {
		cs := loadSampleChange(t, change)
		result, err := Apply(p, cs, cs.BaseVersion+1)
		if err != nil {
			t.Fatalf("apply %s: %v", change, err)
		}
		p = result.Policy
		if change == "02-delete-directory.json" {
			if got := ruleIDs(result.Closure.Direct); !slices.Equal(got, []string{"rule.file.data.read", "rule.file.data.write"}) {
				t.Fatalf("direct closure = %v", got)
			}
			if got := ruleIDs(result.Closure.Transitive); !slices.Equal(got, []string{"rule.file.audit.read", "rule.file.cache.read"}) {
				t.Fatalf("transitive closure = %v", got)
			}
			for _, code := range result.Closure.BusinessCodes {
				if code != business.CodeDepInvalidated {
					t.Fatalf("unexpected closure code %s", code)
				}
			}
			state := StateIssues(result.Policy)
			if got := affectedIDs(state); !slices.Equal(got, []string{"rule.file.audit.read", "rule.file.cache.read", "rule.file.data.read", "rule.file.data.write"}) {
				t.Fatalf("state issues did not propagate transitively: %v", got)
			}
			if len(result.Closure.Direct) > 0 && result.Closure.Direct[0].Via[len(result.Closure.Direct[0].Via)-1] == result.Closure.Direct[0].Via[0] {
				t.Fatalf("via chain contains duplicate endpoints: %v", result.Closure.Direct[0].Via)
			}
		}
	}
}

func TestApplyProcessTightening(t *testing.T) {
	baseline := loadSamplePolicy(t)
	p := baseline
	for _, change := range []string{"01-add-cache.json", "02-delete-directory.json", "03-tighten-process.json"} {
		cs := loadSampleChange(t, change)
		result, err := Apply(p, cs, cs.BaseVersion+1)
		if err != nil {
			t.Fatalf("apply %s: %v", change, err)
		}
		p = result.Policy
		if change == "03-tighten-process.json" {
			if len(result.Closure.Triggers) != 1 || result.Closure.Triggers[0].Change != "tightened" {
				t.Fatalf("triggers = %+v", result.Closure.Triggers)
			}
			if got := ruleIDs(result.Closure.Direct); !slices.Equal(got, []string{"rule.proc.trusted"}) {
				t.Fatalf("direct = %v", got)
			}
			if got := ruleIDs(result.Closure.Transitive); !slices.Equal(got, []string{"rule.proc.audit"}) {
				t.Fatalf("transitive = %v", got)
			}
			if len(result.Closure.BusinessCodes) != 1 || result.Closure.BusinessCodes[0] != business.CodeDepRestricted {
				t.Fatalf("codes = %v", result.Closure.BusinessCodes)
			}
		}
	}
}

func TestApplyRejectsMissingReferenceAndCycles(t *testing.T) {
	for _, name := range []string{"missing-reference.json", "dependency-cycle.json"} {
		t.Run(name, func(t *testing.T) {
			baseline := loadSamplePolicy(t)
			cs, err := ParseChangeSetFile(filepath.Join("..", "samples", "invalid", name))
			if err != nil {
				t.Fatal(err)
			}
			_, err = Apply(baseline, cs, 2)
			if err == nil {
				t.Fatal("expected apply error")
			}
			be := business.From(err)
			if be.Code != business.CodePolicyMalformed {
				t.Fatalf("code = %s: %v", be.Code, err)
			}
		})
	}
}

func TestTraceRuleIncludesDependenciesAndHierarchy(t *testing.T) {
	p := loadSamplePolicy(t)
	trace := TraceRule(p, "rule.file.audit.read")
	if !trace.Found || len(trace.Chain) < 4 {
		t.Fatalf("trace = %+v", trace)
	}
	ids := make([]string, 0, len(trace.Chain))
	for _, node := range trace.Chain {
		ids = append(ids, node.ID)
	}
	for _, want := range []string{"rule.file.audit.read", "rule.file.data.read", "dir.audit", "dir.root"} {
		if !slices.Contains(ids, want) {
			t.Fatalf("trace missing %s: %v", want, ids)
		}
	}
}

func loadSampleChange(t *testing.T, name string) *ChangeSet {
	t.Helper()
	cs, err := ParseChangeSetFile(filepath.Join("..", "samples", "changes", name))
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return cs
}

func ruleIDs(values []AffectedRule) []string {
	out := make([]string, len(values))
	for i, value := range values {
		out[i] = value.RuleID
	}
	return out
}

func affectedIDs(values []AffectedRule) []string {
	return ruleIDs(values)
}
