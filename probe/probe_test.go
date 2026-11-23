package probe

import (
	"context"
	"os"
	"os/user"
	"path/filepath"
	"testing"

	"sandboxpolicy/policy"
)

func TestProbeMatchesBaselineAndCleansWorkspace(t *testing.T) {
	if current, err := user.Current(); err == nil && current.Uid == "0" {
		t.Skip("permission-denial probes are not enforceable for root")
	}
	baseline := loadProbePolicy(t)
	parent := t.TempDir()
	result, err := Run(context.Background(), baseline, 1, Options{TempParent: parent, TimeoutMS: 3000})
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary.Total != 8 || result.Summary.Matched != 8 || result.Summary.Failed != 0 || result.Summary.Mismatched != 0 {
		t.Fatalf("probe summary = %+v results=%+v", result.Summary, result.Results)
	}
	if len(result.BusinessCodes) != 0 {
		t.Fatalf("unexpected probe codes: %v", result.BusinessCodes)
	}
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("probe workspace was not cleaned: %v", entries)
	}
	for i := 1; i < len(result.Results); i++ {
		if result.Results[i-1].RuleID > result.Results[i].RuleID {
			t.Fatalf("probe results are not sorted: %s > %s", result.Results[i-1].RuleID, result.Results[i].RuleID)
		}
	}
}

func TestProbeSkipsDependencyInvalidRulesWithoutProbeFailure(t *testing.T) {
	baseline := loadProbePolicy(t)
	baseline.Directories = removeDirectory(baseline.Directories, "dir.data")
	// Keep the dependent rules to model a committed snapshot after an upstream delete.
	result, err := Run(context.Background(), baseline, 3, Options{TempParent: t.TempDir(), TimeoutMS: 3000})
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, item := range result.Results {
		if item.Status == "dependency_invalid" {
			seen++
			if item.Code != "E_DEP_INVALIDATED" {
				t.Fatalf("dependency skip code = %s", item.Code)
			}
			if item.Code == "E_PROBE_FAILED" {
				t.Fatal("dependency invalidation was misclassified as probe failure")
			}
		}
	}
	if seen < 3 {
		t.Fatalf("expected dependent skips, got %d: %+v", seen, result.Results)
	}
}

func TestCanceledProcessProbeIsProbeFailure(t *testing.T) {
	baseline := loadProbePolicy(t)
	baseline.Rules = []policy.Rule{{
		ID:     "rule.proc.trusted",
		Effect: policy.EffectAllow,
		Action: policy.ActionProcessStart,
		Ref:    "pg.trusted",
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := Run(ctx, baseline, 1, Options{TempParent: t.TempDir(), TimeoutMS: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Results) != 1 {
		t.Fatalf("results = %+v", result.Results)
	}
	item := result.Results[0]
	if item.Status != "probe_failed" || item.Code != "E_PROBE_FAILED" {
		t.Fatalf("canceled probe classification = %+v", item)
	}
}

func loadProbePolicy(t *testing.T) *policy.Policy {
	t.Helper()
	p, err := policy.ParsePolicyFile(filepath.Join("..", "samples", "policies", "baseline.json"))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func removeDirectory(values []policy.Directory, id string) []policy.Directory {
	out := make([]policy.Directory, 0, len(values))
	for _, value := range values {
		if value.ID != id {
			out = append(out, value)
		}
	}
	return out
}
