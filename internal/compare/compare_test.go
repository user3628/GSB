package compare

import (
	"sync"
	"testing"
	"time"

	"fwadjudge/internal/codes"
	"fwadjudge/internal/model"
)

func mkRule(idx int) model.Rule {
	r := model.Rule{
		SnapshotID: "s", Index: idx, Direction: model.DirAny, Protocol: model.ProtoAny,
		SrcCIDR: "0.0.0.0/0", DstCIDR: "0.0.0.0/0", PortLo: 0, PortHi: 65535,
		Action: model.ActionDeny, Priority: idx,
	}
	if err := r.Normalize(); err != nil {
		panic(err)
	}
	return r
}

var probes = []model.Probe{{
	ID: "p1", Direction: model.DirIn, Protocol: model.ProtoTCP,
	SrcIP: "10.0.0.1", DstIP: "10.0.0.2", DstPort: 80, Weight: "1", Count: "1",
}}

func TestRuleBudget(t *testing.T) {
	m := NewManager(Config{MaxConcurrent: 2, RuleBudget: 2, WaitThreshold: time.Second})
	rules := []model.Rule{mkRule(1), mkRule(2), mkRule(3), mkRule(4)}
	res := m.Run([]Task{{SnapshotID: "s", Rules: rules, Probes: probes}})
	if len(res) != 1 || !res[0].BudgetCut {
		t.Fatalf("budget cut expected: %+v", res[0])
	}
	found := false
	for _, e := range res[0].Errors {
		if e.Code == codes.ERuleBudget && e.SnapshotID == "s" && e.RuleIndex == 3 {
			found = true
		}
	}
	if !found {
		t.Fatalf("E_RULE_BUDGET with traceability missing: %+v", res[0].Errors)
	}
	// The parsed prefix is preserved and still adjudicates.
	if len(res[0].Verdicts) != 1 {
		t.Fatalf("verdicts %+v", res[0].Verdicts)
	}
}

func TestContentionAndInflight(t *testing.T) {
	m := NewManager(Config{MaxConcurrent: 1, RuleBudget: 10, WaitThreshold: 50 * time.Millisecond})
	if err := m.Acquire(); err != nil {
		t.Fatal(err)
	}
	if m.Inflight() != 1 {
		t.Fatalf("inflight %d, want 1", m.Inflight())
	}
	// Second acquire exceeds the wait threshold -> E_COMPARE_CONTENTION.
	if err := m.Acquire(); err == nil || err.Code != codes.ECompareContention {
		t.Fatalf("want E_COMPARE_CONTENTION, got %v", err)
	}
	m.Release()
	if m.Inflight() != 0 {
		t.Fatalf("inflight %d, want 0", m.Inflight())
	}
}

func TestConcurrentSessionsIsolated(t *testing.T) {
	m := NewManager(Config{MaxConcurrent: 4, RuleBudget: 10, WaitThreshold: 5 * time.Second})
	var tasks []Task
	for _, id := range []string{"a", "b", "c", "d"} {
		tasks = append(tasks, Task{SnapshotID: id, Rules: []model.Rule{mkRule(1)}, Probes: probes})
	}
	var maxSeen int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if n := m.Inflight(); n > maxSeen {
				maxSeen = n
			}
			time.Sleep(time.Millisecond)
		}
	}()
	res := m.Run(tasks)
	wg.Wait()
	if len(res) != 4 {
		t.Fatalf("results %d", len(res))
	}
	for _, r := range res {
		if r.Session == nil || r.Session.Ledger == nil {
			t.Fatal("session missing")
		}
	}
	if m.Inflight() != 0 {
		t.Fatalf("inflight %d after run", m.Inflight())
	}
}
