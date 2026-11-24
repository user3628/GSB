package engine

import (
	"testing"

	"fwadjudge/internal/codes"
	"fwadjudge/internal/model"
)

func mkRule(index, priority int, action model.Action, dst string, portLo, portHi int) model.Rule {
	r := model.Rule{
		SnapshotID: "s", Index: index, Direction: model.DirIn, Protocol: model.ProtoTCP,
		SrcCIDR: "10.0.0.0/8", DstCIDR: dst, PortLo: portLo, PortHi: portHi,
		Action: action, Priority: priority,
	}
	if err := r.Normalize(); err != nil {
		panic(err)
	}
	return r
}

func probe(id, dst string, port int, weight, count string) model.Probe {
	return model.Probe{
		ID: id, Direction: model.DirIn, Protocol: model.ProtoTCP,
		SrcIP: "10.0.0.5", DstIP: dst, DstPort: port, Weight: weight, Count: count,
	}
}

func TestVerdictPriority(t *testing.T) {
	rules := []model.Rule{
		mkRule(1, 100, model.ActionAllow, "10.0.1.0/24", 8080, 8080),
		mkRule(2, 50, model.ActionDeny, "10.0.1.0/24", 8000, 9000),
	}
	v, err := VerdictFor(rules, &model.Probe{
		ID: "p", Direction: model.DirIn, Protocol: model.ProtoTCP,
		SrcIP: "10.0.0.5", DstIP: "10.0.1.9", DstPort: 8080,
	})
	if err != nil {
		t.Fatal(err)
	}
	if v.Action != model.ActionAllow || v.HitRule != 1 {
		t.Fatalf("verdict %+v", v)
	}
}

func TestImplicitConflict(t *testing.T) {
	rules := []model.Rule{
		mkRule(1, 100, model.ActionAllow, "10.0.1.0/24", 8080, 8080),
		mkRule(2, 100, model.ActionDeny, "10.0.1.0/24", 8080, 8080),
	}
	s := NewSession("conflict", rules)
	v, err := s.Observe(probe("p1", "10.0.1.9", 8080, "1", "1"))
	if err != nil {
		t.Fatal(err)
	}
	if !v.Implicit || len(v.Conflicts) != 2 {
		t.Fatalf("implicit %+v", v)
	}
	// Deterministic pick: lowest index wins.
	if v.Action != model.ActionAllow || v.HitRule != 1 {
		t.Fatalf("pick %+v", v)
	}
}

func TestOverflowLeavesNoPartialLedger(t *testing.T) {
	rules := []model.Rule{mkRule(1, 10, model.ActionAllow, "0.0.0.0/0", 0, 65535)}
	s := NewSession("ovf", rules)
	_, err := s.Observe(probe("o1", "10.0.1.9", 8080, "5000000000", "5000000000"))
	ce := err
	if ce == nil || ce.Code != codes.EFixedOverflow {
		t.Fatalf("want E_FIXED_OVERFLOW, got %v", err)
	}
	if ce.SnapshotID != "ovf" || ce.RuleIndex != 1 {
		t.Fatalf("traceability %+v", ce)
	}
	if got := s.Ledger.BoundUnits(); got != 0 {
		t.Fatalf("ledger polluted: %d units", got)
	}
	if len(s.Stats) != 0 {
		t.Fatalf("stats polluted: %+v", s.Stats)
	}
}

func TestDivergenceLocalizationAndClassification(t *testing.T) {
	hq := []model.Rule{
		mkRule(1, 100, model.ActionAllow, "10.0.1.0/24", 8080, 8080),
		mkRule(2, 50, model.ActionDeny, "10.0.1.0/24", 8000, 9000),
		mkRule(3, 1, model.ActionDeny, "0.0.0.0/0", 0, 65535),
	}
	branch := []model.Rule{
		mkRule(1, 100, model.ActionAllow, "10.0.2.0/24", 8080, 8080),
		mkRule(2, 50, model.ActionDeny, "10.0.1.0/24", 9000, 9100),
		mkRule(3, 1, model.ActionDeny, "0.0.0.0/0", 0, 65535),
	}
	sA, sB := NewSession("hq", hq), NewSession("br", branch)
	probes := []model.Probe{
		probe("p1", "10.0.1.9", 8080, "2.5", "4"),        // hq: r1 allow; br: r3 deny
		probe("p2", "10.0.1.20", 8500, "1.0000005", "2"), // both deny (r2/r3)
		probe("p3", "10.0.2.9", 8080, "3", "1"),          // hq: r3 deny; br: r1 allow
	}
	// Observe the full probe list first; classification uses the final
	// accumulated statistics and ledger bounds of each session.
	type pair struct {
		vA, vB Verdict
	}
	verdicts := map[string]pair{}
	for _, p := range probes {
		vA, err := sA.Observe(p)
		if err != nil {
			t.Fatal(err)
		}
		vB, err := sB.Observe(p)
		if err != nil {
			t.Fatal(err)
		}
		verdicts[p.ID] = pair{vA, vB}
	}
	var divs []*Divergence
	for _, p := range probes {
		vp := verdicts[p.ID]
		if d := ComparePair(sA, sB, vp.vA, vp.vB); d != nil {
			divs = append(divs, d)
		}
	}
	if len(divs) != 2 {
		t.Fatalf("divergences %d, want 2", len(divs))
	}
	// p1: hit rules differ -> field "index"; stats 10 vs 12.000001 with
	// 1-unit bounds -> intervals do not overlap -> deterministic.
	if divs[0].ProbeID != "p1" || divs[0].Field != "index" || divs[0].Class != "deterministic" {
		t.Fatalf("p1 %+v", divs[0])
	}
	// p3: stats 3 vs 3 -> intervals overlap -> uncertain.
	if divs[1].ProbeID != "p3" || divs[1].Class != "uncertain" {
		t.Fatalf("p3 %+v", divs[1])
	}
	// Byte-identical classification on repeat with the same input.
	sA2, sB2 := NewSession("hq", hq), NewSession("br", branch)
	var divs2 []*Divergence
	for _, p := range probes {
		if _, err := sA2.Observe(p); err != nil {
			t.Fatal(err)
		}
		if _, err := sB2.Observe(p); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range probes {
		vp := verdicts[p.ID]
		if d := ComparePair(sA2, sB2, vp.vA, vp.vB); d != nil {
			divs2 = append(divs2, d)
		}
	}
	if len(divs2) != len(divs) {
		t.Fatal("repeat divergence count mismatch")
	}
	for i := range divs {
		if *divs[i] != *divs2[i] {
			t.Fatalf("repeat mismatch at %d: %+v vs %+v", i, divs[i], divs2[i])
		}
	}
}

func TestFieldLevelLocalization(t *testing.T) {
	a := mkRule(1, 100, model.ActionAllow, "10.0.1.0/24", 8080, 8080)
	b := mkRule(1, 100, model.ActionDeny, "10.0.1.0/24", 8080, 8080)
	if f := FirstDivergentField(&a, &b); f != "action" {
		t.Fatalf("field %s", f)
	}
	b.Priority = 90
	if f := FirstDivergentField(&a, &b); f != "priority" {
		t.Fatalf("field %s", f)
	}
}
