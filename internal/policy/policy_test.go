package policy

import (
	"testing"

	"queryrewrite/internal/spec"
)

func basePolicy() spec.Policy {
	return spec.Policy{
		Version: 1, ParentVersion: 0, CandidateDepth: 6,
		StrategyPerMille: map[string]int{"history": 600, "synonym": 400},
		CacheCapacity:    4, MinPrefixRunes: 6, PrefixBlockRunes: 3,
		HitGainThreshold: 0.20, FailureUpperLimit: 0.85, EvalBudgetPerTurn: 6,
		Weights: spec.DecisionWeights{Overlap: 1.6, Hit: 2.0, Age: 0.25, FailPenalty: 1},
	}
}

func TestDigestStable(t *testing.T) {
	p := basePolicy()
	d1 := Digest(p)
	d2 := Digest(p)
	if d1 != d2 || d1 == "" {
		t.Fatal("policy digest must be stable and non-empty")
	}
}

func TestApplyPatchIncrementalEarliestStage(t *testing.T) {
	p := basePolicy()
	depth := p.CandidateDepth + 2
	patch := spec.PolicyPatch{Version: p.Version + 1, CandidateDepth: &depth}
	next, earliest, err := ApplyPatch(p, patch)
	if err != nil {
		t.Fatal(err)
	}
	if next.Version != p.Version+1 || next.ParentVersion != p.Version {
		t.Fatal("lineage not set")
	}
	if earliest != spec.StageGenerate {
		t.Fatalf("candidate_depth must invalidate from stage1, got %s", earliest)
	}

	minP := p.MinPrefixRunes + 1
	patch2 := spec.PolicyPatch{Version: next.Version + 1, MinPrefixRunes: &minP}
	next2, earliest2, err := ApplyPatch(next, patch2)
	if err != nil {
		t.Fatal(err)
	}
	if earliest2 != spec.StagePrefix {
		t.Fatalf("min prefix change earliest stage = %s", earliest2)
	}
	_ = next2
}

func TestPatchWrongVersionRejected(t *testing.T) {
	p := basePolicy()
	v := 5
	patch := spec.PolicyPatch{Version: p.Version, CacheCapacity: &v}
	if _, _, err := ApplyPatch(p, patch); err == nil {
		t.Fatal("non-consecutive patch version must be rejected")
	}
}

func TestClassifyReplacementAndRollback(t *testing.T) {
	cur := basePolicy()
	incoming := cur
	incoming.Version = cur.Version + 1
	incoming.ParentVersion = cur.Version - 1 // lineage break
	incoming.Kind = "replacement"
	incoming.CacheCapacity = cur.CacheCapacity + 3
	incoming.ContentDigest = Digest(incoming)
	kind, berr := Classify(cur, incoming)
	if berr != nil || kind != "replacement" {
		t.Fatalf("want replacement, got kind=%s err=%v", kind, berr)
	}

	rollback := cur
	rollback.Version = cur.Version - 1
	kind, berr = Classify(cur, rollback)
	if berr == nil || berr.Code != spec.CodeStaleVersion {
		t.Fatalf("want E_STALE_VERSION, got kind=%s err=%v", kind, berr)
	}
	if berr.Details["current_version"] == "" {
		t.Fatal("rollback error must report current effective version")
	}
}

func TestMilleValidation(t *testing.T) {
	p := basePolicy()
	p.CacheCapacity = 0
	if err := Validate(p); err == nil {
		t.Fatal("non-positive capacity must fail validation")
	}
	p2 := basePolicy()
	p2.Weights.Hit = 0
	if err := Validate(p2); err == nil {
		t.Fatal("non-positive weight must fail validation")
	}
}
