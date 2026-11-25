package engine

import (
	"encoding/json"
	"strings"
	"testing"

	"queryrewrite/internal/gen"
	"queryrewrite/internal/spec"
)

func TestStage1ByteStableAndBusinessCodes(t *testing.T) {
	p := gen.DemoPolicy()
	qt := gen.DemoSessions()[0]
	_, c1, err := Stage1(qt, p)
	if err != nil {
		t.Fatal(err)
	}
	_, c2, err := Stage1(qt, p)
	if err != nil {
		t.Fatal(err)
	}
	b1, _ := json.Marshal(c1)
	b2, _ := json.Marshal(c2)
	if string(b1) != string(b2) {
		t.Fatal("candidate list not byte-stable")
	}
	if len(c1) > p.CandidateDepth {
		t.Fatal("candidate depth not enforced")
	}

	// Empty query stable code.
	empty := spec.QueryTurn{SessionID: "s", Turn: 9, RawQuery: "   "}
	_, _, berr := Stage1(empty, p)
	if berr == nil || berr.Code != spec.CodeEmptyQuery || berr.SessionID != "s" ||
		berr.Turn != 9 || berr.Stage != spec.StageGenerate {
		t.Fatalf("want E_EMPTY_QUERY with context, got %+v", berr)
	}

	// No candidates: query with no history and no synonym head.
	noCand := spec.QueryTurn{SessionID: "s", Turn: 1, RawQuery: "xyz qqq"}
	_, _, berr = Stage1(noCand, p)
	if berr == nil || berr.Code != spec.CodeNoCandidates {
		t.Fatalf("want E_NO_CANDIDATES, got %+v", berr)
	}
}

func TestStage2OutcomesAndContention(t *testing.T) {
	p := gen.DemoPolicy()
	qt := gen.DemoSessions()[0]
	_, cands, berr := Stage1(qt, p)
	if berr != nil {
		t.Fatal(berr)
	}
	cache := NewCache(p.CacheCapacity)
	hits := Stage2(cands, p, cache, 1)
	if len(hits) != len(cands) {
		t.Fatal("one hit result per candidate required")
	}
	// First turn against an empty cache: nothing is an exact hit.
	for _, h := range hits {
		if h.Outcome == spec.HitExact {
			t.Fatal("empty cache cannot produce exact hit")
		}
		if len(h.Blocks) == 0 && h.Cacheable {
			t.Fatal("cacheable candidate must carry blocks")
		}
	}
	// Deterministic ordering: repeated call yields identical bytes.
	cache2 := NewCache(p.CacheCapacity)
	hits2 := Stage2(cands, p, cache2, 1)
	a, _ := json.Marshal(hits)
	b, _ := json.Marshal(hits2)
	if string(a) != string(b) {
		t.Fatal("stage2 not deterministic")
	}
}

func TestStage3NumericErrorOnZeroWeight(t *testing.T) {
	p := gen.DemoPolicy()
	p.Weights.Hit = 0
	qt := gen.DemoSessions()[0]
	_, cands, _ := Stage1(qt, p)
	cache := NewCache(p.CacheCapacity)
	hits := Stage2(cands, p, cache, 1)
	adjs := Stage3(cands, hits, p, cache, 1)
	for _, a := range adjs {
		if a.NumericError == nil || a.NumericError.Code != spec.CodeNumeric {
			t.Fatalf("want E_NUMERIC for zero weight, got %+v", a.NumericError)
		}
		if a.NumericError.Candidate == "" {
			t.Fatal("E_NUMERIC must carry candidate id")
		}
	}
}

func TestStage3FailureUpperInRangeAndDerived(t *testing.T) {
	p := gen.DemoPolicy()
	qt := gen.DemoSessions()[0]
	_, cands, _ := Stage1(qt, p)
	cache := NewCache(p.CacheCapacity)
	hits := Stage2(cands, p, cache, 1)
	adjs := Stage3(cands, hits, p, cache, 1)
	for _, a := range adjs {
		if a.NumericError != nil {
			t.Fatalf("unexpected numeric error: %+v", a.NumericError)
		}
		if a.FailureUpper < 0 || a.FailureUpper > 1 {
			t.Fatalf("failure upper outside [0,1]: %v", a.FailureUpper)
		}
		if a.Derivation["slot_age_turns"] == "" || a.Derivation["prefix_block_count"] == "" ||
			a.Derivation["upper_formula"] == "" {
			t.Fatalf("missing explicit age/block/formula derivation: %+v", a.Derivation)
		}
		if a.Decision != "ACCEPT" && a.Decision != "REJECT" {
			t.Fatalf("decision must be ACCEPT/REJECT, got %s", a.Decision)
		}
	}
}

func TestStage4BudgetMilleDegradation(t *testing.T) {
	p := gen.DemoPolicy()
	p.StrategyPerMille = map[string]int{"history": 700, "synonym": 400} // sum 1100
	qt := gen.DemoSessions()[0]
	_, cands, _ := Stage1(qt, p)
	cache := NewCache(p.CacheCapacity)
	hits := Stage2(cands, p, cache, 1)
	adjs := Stage3(cands, hits, p, cache, 1)
	ledger := Stage4(qt.SessionID, 1, cands, hits, adjs, p, cache)
	if !ledger.Degraded {
		t.Fatal("per-mille sum != 1000 must set degraded")
	}
	found := false
	for _, r := range ledger.DegradeReasons {
		if strings.Contains(r, string(spec.CodeBudgetMille)) {
			found = true
		}
	}
	if !found {
		t.Fatal("must report E_BUDGET_MILLE rebalance reason")
	}
	if got := sumMille(ledger.PerMilleBudget); got != 1000 {
		t.Fatalf("rebalanced budget must sum 1000, got %d", got)
	}
}

func TestStage4EvalBudgetSkips(t *testing.T) {
	p := gen.DemoPolicy()
	p.EvalBudgetPerTurn = 2
	qt := gen.DemoSessions()[0]
	_, cands, _ := Stage1(qt, p)
	cache := NewCache(p.CacheCapacity)
	hits := Stage2(cands, p, cache, 1)
	adjs := Stage3(cands, hits, p, cache, 1)
	ledger := Stage4(qt.SessionID, 1, cands, hits, adjs, p, cache)
	if ledger.EvalUsed != 2 {
		t.Fatalf("eval used = %d want 2", ledger.EvalUsed)
	}
	if len(ledger.Skips) == 0 {
		t.Fatal("budget skips must be recorded")
	}
}
