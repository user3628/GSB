package versioning_test

import (
	"testing"

	"flashattention/internal/attn"
	"flashattention/internal/versioning"
)

func TestCausalPrefixInvalidationStartsAtKeyRowBlock(t *testing.T) {
	cfg := attn.Config{RowBlock: 4, ColBlock: 8, Causal: true, KeyLen: 32}
	old := versioning.Compute(versioning.Stages{Config: cfg, KFP: "k1"})
	newCfg := cfg
	newFP := versioning.Compute(versioning.Stages{Config: newCfg, KFP: "k2"})
	rs := versioning.Diff("s", old, newFP, 32, versioning.Change{KeyStart: 10, KeyEnd: 18})
	if len(rs) != 1 {
		t.Fatalf("expected one range, got %+v", rs)
	}
	r := rs[0]
	if r.Trigger != versioning.TriggerPrefix || r.FirstRowBlock != 2 || r.FirstColBlock != 1 || r.LastColBlock != 2 {
		t.Fatalf("unexpected range: %+v", r)
	}
}

func TestStrategyChangeInvalidatesAll(t *testing.T) {
	old := versioning.Compute(versioning.Stages{Config: attn.Config{RowBlock: 1, ColBlock: 1, Strategy: attn.Scaled, KeyLen: 4}})
	new := versioning.Compute(versioning.Stages{Config: attn.Config{RowBlock: 1, ColBlock: 1, Strategy: attn.LogSum, KeyLen: 4}})
	rs := versioning.Diff("s", old, new, 4, versioning.Change{})
	if len(rs) == 0 || rs[0].Trigger != versioning.TriggerStrategy || !rs[0].AllRowBlocks {
		t.Fatalf("expected strategy all invalidation, got %+v", rs)
	}
}
