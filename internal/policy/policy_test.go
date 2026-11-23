package policy

import (
	"testing"

	"cdn-edge-consistency/internal/cache"
	"cdn-edge-consistency/internal/model"
)

func validPolicy() *Snapshot {
	return &Snapshot{
		Version: 1, GeneratedAt: "2026-11-23T00:00:00Z", Nodes: []string{"edge-b", "edge-a"},
		TTLMs: 1000, SWRMs: 500, RevalidateWindowMs: 500, PurgeWindowMs: 1000,
		OriginTimeoutMs: 100, RevalidateTimeoutMs: 200,
		Vary: []string{"X-Tenant", "accept-language"}, KeyRules: cache.KeyRules{CollapseSlashes: true, MergeDotSegments: true, UppercasePercent: true, SortQuery: true},
		Scheduler: SchedulerPolicy{GlobalOriginConcurrency: 2, PerOriginConcurrency: 1, QueueCapacity: 4, RoundBudgetMs: 5, DefaultTaskCostMs: 1},
	}
}

func TestChecksumDetectsTamperingAndSealIsStable(t *testing.T) {
	p := validPolicy()
	allowed := map[string]model.NodeConfig{"edge-a": {ID: "edge-a"}, "edge-b": {ID: "edge-b"}}
	if err := p.NormalizeAndValidate(allowed); err != nil {
		t.Fatal(err)
	}
	if err := p.Seal(); err != nil {
		t.Fatal(err)
	}
	sum1 := p.Checksum
	p.Nodes = []string{"edge-a"}
	if err := p.Seal(); model.AsBiz(err).Code != model.CodeSnapshotChecksum {
		t.Fatalf("expected checksum mismatch, got %v", err)
	}
	p.Nodes = []string{"edge-a", "edge-b"}
	p.Checksum = ""
	if err := p.Seal(); err != nil {
		t.Fatal(err)
	}
	if p.Checksum != sum1 {
		t.Fatalf("checksum not stable: %s vs %s", p.Checksum, sum1)
	}
}

func TestPolicyValidationRejectsBadInputs(t *testing.T) {
	allowed := map[string]model.NodeConfig{"edge-a": {ID: "edge-a"}, "edge-b": {ID: "edge-b"}}
	cases := []func(*Snapshot){
		func(p *Snapshot) { p.Version = 0 },
		func(p *Snapshot) { p.GeneratedAt = "not-a-time" },
		func(p *Snapshot) { p.Nodes = nil },
		func(p *Snapshot) { p.Nodes = []string{"edge-z"} },
		func(p *Snapshot) { p.TTLMs = -1 },
		func(p *Snapshot) { p.Scheduler.GlobalOriginConcurrency = 0 },
		func(p *Snapshot) { p.Scheduler.PerOriginConcurrency = 3 },
		func(p *Snapshot) { p.Scheduler.QueueCapacity = -1 },
	}
	for i, mutate := range cases {
		p := validPolicy()
		mutate(p)
		if err := p.NormalizeAndValidate(allowed); err == nil {
			t.Errorf("case %d unexpectedly passed", i)
		}
	}
}
