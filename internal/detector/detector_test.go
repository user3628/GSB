package detector

import (
	"testing"

	"privescworkbench/internal/domain"
	"privescworkbench/internal/sample"
	"privescworkbench/internal/scoring"
)

func parseGenerated(t *testing.T, sc sample.Scenario, seed int64) *domain.Bundle {
	t.Helper()
	g := sample.Generate(seed, sc)
	b, err := sample.ParseBundle(g.Bundle.Version, g.ManifestBytes, g.StreamBytes)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestBaselineProducesAllThreeVerdictClasses(t *testing.T) {
	b := parseGenerated(t, sample.ScenarioBaseline, 1)
	res, err := Run(b, nil, Options{SessionID: "t", ThresholdMicro: 56_000_000})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[domain.BizCode]bool{}
	for _, c := range res.Round.Conclusions {
		seen[c.Code] = true
		// interval consistency: low = score-bound, high = score+bound
		if c.Range.Low != c.ScoreMicro-c.BoundMicro ||
			c.Range.High != c.ScoreMicro+c.BoundMicro {
			t.Fatalf("pid %d inconsistent range %+v", c.PID, c.Range)
		}
		if c.BoundMicro < 0 {
			t.Fatal("bound must be non-negative")
		}
	}
	for _, want := range []domain.BizCode{
		domain.CodePrivEscalation, domain.CodeIndeterminateRange, domain.CodeOK,
	} {
		if !seen[want] {
			t.Fatalf("verdict class %s missing, got=%v (threshold=%d)",
				want, seen, scoring.ThresholdMicro)
		}
	}
}

func TestThresholdCrossingIsIndeterminate(t *testing.T) {
	b := parseGenerated(t, sample.ScenarioBaseline, 1)
	res, err := Run(b, nil, Options{SessionID: "t"})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range res.Round.Conclusions {
		if c.Code == domain.CodeIndeterminateRange {
			if !(c.Range.Low <= 56_000_000 && c.Range.High > 56_000_000) {
				t.Fatalf("indeterminate range does not straddle threshold: %+v", c.Range)
			}
			return
		}
	}
}

func TestOrphanAttachedToVirtualRoot(t *testing.T) {
	b := parseGenerated(t, sample.ScenarioOrphan, 1)
	res, err := Run(b, nil, Options{SessionID: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Round.Orphans) != 1 || res.Round.Orphans[0] != 4001 {
		t.Fatalf("orphans=%v", res.Round.Orphans)
	}
	var orphan *domain.ProcConclusion
	for i := range res.Round.Conclusions {
		if res.Round.Conclusions[i].PID == 4001 {
			orphan = &res.Round.Conclusions[i]
		}
	}
	if orphan == nil || orphan.Code != domain.CodeOrphanRef {
		t.Fatalf("orphan verdict=%v", orphan)
	}
	if len(orphan.Trace) == 0 || orphan.Trace[0].FromPID != domain.VirtualRootPID {
		t.Fatalf("trace must start at virtual root: %+v", orphan.Trace)
	}
}

func TestHandleAbuseAttributionChain(t *testing.T) {
	b := parseGenerated(t, sample.ScenarioHandleAbuse, 1)
	res, err := Run(b, nil, Options{SessionID: "t"})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range res.Round.Conclusions {
		if c.PID == 3002 {
			if c.Code != domain.CodePrivEscalation {
				t.Fatalf("pid 3002 code=%s", c.Code)
			}
			foundHandle := false
			for _, hop := range c.Trace {
				if hop.Edge == "handle" && hop.FromPID == 3001 {
					foundHandle = true
				}
			}
			if !foundHandle {
				t.Fatalf("handle attribution hop missing: %+v", c.Trace)
			}
		}
	}
}

func TestOrderIndependence(t *testing.T) {
	b1 := parseGenerated(t, sample.ScenarioHandleAbuse, 1)
	b2 := parseGenerated(t, sample.ScenarioHandleAbuse, 1)
	// reverse processes and events
	for i, j := 0, len(b2.Processes)-1; i < j; i, j = i+1, j-1 {
		b2.Processes[i], b2.Processes[j] = b2.Processes[j], b2.Processes[i]
	}
	for i, j := 0, len(b2.Events)-1; i < j; i, j = i+1, j-1 {
		b2.Events[i], b2.Events[j] = b2.Events[j], b2.Events[i]
	}
	r1, err := Run(b1, nil, Options{SessionID: "a"})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := Run(b2, nil, Options{SessionID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	if len(r1.Round.Conclusions) != len(r2.Round.Conclusions) {
		t.Fatal("conclusion count differs")
	}
	for i := range r1.Round.Conclusions {
		a, c := r1.Round.Conclusions[i], r2.Round.Conclusions[i]
		if a.PID != c.PID || a.Code != c.Code ||
			a.ScoreMicro != c.ScoreMicro || a.BoundMicro != c.BoundMicro {
			t.Fatalf("non-deterministic result at %d: %+v vs %+v", i, a, c)
		}
	}
}

func TestNoFloatingPointInScores(t *testing.T) {
	b := parseGenerated(t, sample.ScenarioBaseline, 1)
	res, err := Run(b, nil, Options{SessionID: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ledger) == 0 {
		t.Fatal("expected truncation ledger entries from time/multiplication")
	}
	for _, item := range res.Ledger {
		if item.Remainder < 0 {
			t.Fatal("negative ledger remainder")
		}
	}
}
