package trace

import (
	"testing"

	"flowtrace/internal/codes"
	"flowtrace/internal/model"
	"flowtrace/internal/policy"
)

func testPolicy() *policy.Snapshot {
	return &policy.Snapshot{
		Version: 1,
		SensitiveFields: map[string][]string{
			"users.id_card": {"ID_CARD"},
			"users.bank":    {"BANK_CARD"},
		},
		AuthorizedTargets: map[string]bool{
			"users.id_card": true, "users.bank": true,
			"dw.copy": true, "risk.f": true, "stats.kpi": true,
			"a": true, "b": true, "c": true,
		},
		AggregateSafe: map[string]bool{"ID_CARD": true},
	}
}

func ev(seq uint64, src, dst string, op model.Op) model.Event {
	return model.Event{Seq: seq, Subject: "s", SrcField: src, DstField: dst, Op: op, LogicalTime: seq * 10}
}

func TestPropagationSemantics(t *testing.T) {
	e := New(testPolicy())
	// copy propagates fully, authorized
	if j := e.Apply(ev(1, "users.id_card", "dw.copy", model.OpCrossTableCopy)); j.Code != codes.OK {
		t.Fatalf("copy: %s", j.Code)
	}
	// aggregate de-identifies aggregate-safe marks
	if j := e.Apply(ev(2, "users.id_card", "stats.kpi", model.OpAggregateWrite)); j.Code != codes.OK {
		t.Fatalf("agg safe: %s", j.Code)
	}
	if len(e.Marks()["stats.kpi"]) != 0 {
		t.Fatalf("aggregate-safe mark must not propagate: %v", e.Marks()["stats.kpi"])
	}
	// aggregate keeps non-safe marks
	e.Apply(ev(3, "users.bank", "risk.f", model.OpAggregateWrite))
	if got := e.Marks()["risk.f"]; len(got) != 1 || got[0] != "BANK_CARD" {
		t.Fatalf("agg non-safe: %v", got)
	}
}

func TestEscapeJudgmentAndMinimalPath(t *testing.T) {
	e := New(testPolicy())
	e.Apply(ev(1, "users.id_card", "dw.copy", model.OpCrossTableCopy))
	j := e.Apply(ev(2, "dw.copy", "ext.feed", model.OpCrossServiceExport))
	if j.Code != codes.EFlowEscaped {
		t.Fatalf("want escape, got %s", j.Code)
	}
	want := []string{"users.id_card", "dw.copy", "ext.feed"}
	if len(j.Path) != len(want) {
		t.Fatalf("path %v", j.Path)
	}
	for i := range want {
		if j.Path[i] != want[i] {
			t.Fatalf("path %v, want %v", j.Path, want)
		}
	}
	if len(j.Marks) != 1 || j.Marks[0] != "ID_CARD" {
		t.Fatalf("marks %v", j.Marks)
	}
}

func TestCycleTerminates(t *testing.T) {
	e := New(testPolicy())
	e.Apply(ev(1, "users.id_card", "a", model.OpCrossTableCopy))
	e.Apply(ev(2, "a", "b", model.OpCrossTableCopy))
	e.Apply(ev(3, "b", "c", model.OpCrossTableCopy))
	e.Apply(ev(4, "c", "a", model.OpCrossTableCopy)) // cycle back
	e.Apply(ev(5, "a", "b", model.OpCrossTableCopy)) // re-arrival
	marks := e.Marks()
	if len(marks["a"]) != 1 || len(marks["b"]) != 1 || len(marks["c"]) != 1 {
		t.Fatalf("cycle diffusion wrong: %v", marks)
	}
	// path must stay the minimal one, not grow around the loop
	if got := len(e.lineage["b"]["ID_CARD"]); got != 3 {
		t.Fatalf("path length grew around cycle: %d", got)
	}
}

func TestCanonicalOrder(t *testing.T) {
	in := []model.Event{ev(3, "a", "b", 0), ev(1, "a", "b", 0), ev(2, "a", "b", 0)}
	out := CanonicalOrder(in)
	for i, want := range []uint64{1, 2, 3} {
		if out[i].Seq != want {
			t.Fatalf("order %v", out)
		}
	}
}
