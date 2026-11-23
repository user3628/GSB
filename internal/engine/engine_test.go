package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"baselineworkbench/internal/codes"
	"baselineworkbench/internal/parse"
	"baselineworkbench/internal/policy"
)

type testItem struct {
	ID     string
	Unit   string
	Value  string
	Weight string
}

func baselineBytes(t *testing.T, items []testItem) []byte {
	t.Helper()
	type wireItem struct {
		ID     string `json:"id"`
		Unit   string `json:"unit"`
		Value  string `json:"value"`
		Weight string `json:"weight,omitempty"`
	}
	wireItems := make([]wireItem, 0, len(items))
	for _, item := range items {
		wireItems = append(wireItems, wireItem{ID: item.ID, Unit: item.Unit, Value: item.Value, Weight: item.Weight})
	}
	document := struct {
		Schema string     `json:"schema"`
		Items  []wireItem `json:"items"`
	}{Schema: "baseline.workbench.snapshot/v1", Items: wireItems}
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func repairBytes(items []testItem) []byte {
	var out []byte
	for _, item := range items {
		out = append(out, parse.RepairBlock(policy.RawRecord{ID: item.ID, Unit: item.Unit, Value: item.Value, Weight: item.Weight})...)
	}
	return out
}

func makeInput(t *testing.T, path string, baseline, repairs []testItem) Input {
	t.Helper()
	return Input{PackagePath: path, Baseline: baselineBytes(t, baseline), Repairs: repairBytes(repairs)}
}

func preparedReference(t *testing.T, items []testItem) Prepared {
	t.Helper()
	input := makeInput(t, "packages/reference", items, items)
	prepared := Prepare(input)
	if len(prepared.Issues) > 0 {
		t.Fatalf("reference issues: %+v", prepared.Issues)
	}
	return prepared
}

func runOne(t *testing.T, cfg Config, reference Prepared, candidate Input) Result {
	t.Helper()
	results := Run(context.Background(), []Input{candidate}, reference, cfg)
	if len(results) != 1 {
		t.Fatalf("expected one result, got %d", len(results))
	}
	return results[0]
}

func TestMixedUnitsNormalizeToSameValue(t *testing.T) {
	referenceItems := []testItem{{ID: "lockout", Unit: "second", Value: "900", Weight: "1.0"}}
	reference := preparedReference(t, referenceItems)
	candidate := makeInput(t, "packages/candidate", []testItem{{ID: "lockout", Unit: "minute", Value: "15", Weight: "1.0"}}, nil)
	result := runOne(t, Config{MaxConcurrency: 1, MaxItems: 10}, reference, candidate)
	if !result.Complete || result.Code != codes.None {
		t.Fatalf("unexpected result: %+v", result)
	}
	if result.ScoreMicros != 0 || result.ErrorUpperMicros != 0 {
		t.Fatalf("expected exact normalization, got score=%d error=%d", result.ScoreMicros, result.ErrorUpperMicros)
	}
	if len(result.Items) != 1 || result.Items[0].Normalized != 900 || result.Items[0].Unit != "minute" {
		t.Fatalf("unexpected normalized item: %+v", result.Items)
	}
}

func TestDeterministicAndUncertainIntervals(t *testing.T) {
	reference := preparedReference(t, []testItem{{ID: "timeout", Unit: "second", Value: "100", Weight: "1.0"}})
	deterministic := runOne(t, Config{MaxConcurrency: 1, MaxItems: 10}, reference,
		makeInput(t, "packages/deterministic", []testItem{{ID: "timeout", Unit: "second", Value: "90", Weight: "1.0"}}, nil))
	if deterministic.Code != codes.None || deterministic.Deterministic != 1 || deterministic.Uncertain != 0 {
		t.Fatalf("expected deterministic deviation, got %+v", deterministic)
	}
	if deterministic.ScoreMicros != -10_000_000 {
		t.Fatalf("unexpected score %d", deterministic.ScoreMicros)
	}
	if deterministic.Comparisons[0].Classification != ClassificationDeterministic {
		t.Fatalf("unexpected classification %s", deterministic.Comparisons[0].Classification)
	}

	filetimeReference := preparedReference(t, []testItem{{ID: "timestamp", Unit: "filetime", Value: "133485408000000000", Weight: "1.0"}})
	uncertain := runOne(t, Config{MaxConcurrency: 1, MaxItems: 10}, filetimeReference,
		makeInput(t, "packages/uncertain", []testItem{{ID: "timestamp", Unit: "filetime", Value: "133485408009999999", Weight: "1.0"}}, nil))
	if uncertain.Code != codes.None || uncertain.Deterministic != 0 || uncertain.Uncertain != 1 {
		t.Fatalf("expected uncertain overlap, got %+v", uncertain)
	}
	if uncertain.ErrorUpperMicros != 1_000_000 {
		t.Fatalf("unexpected propagated error %d", uncertain.ErrorUpperMicros)
	}
	comparison := uncertain.Comparisons[0]
	if comparison.Classification != ClassificationUncertain || comparison.DeltaInterval.Lower != -1_000_000 || comparison.DeltaInterval.Upper != 1_000_000 {
		t.Fatalf("unexpected uncertainty interval: %+v", comparison)
	}
}

func TestNumericOverflowRollsBackLedger(t *testing.T) {
	reference := preparedReference(t, []testItem{{ID: "score", Unit: "score", Value: "1.000000", Weight: "1.25"}})
	candidate := makeInput(t, "packages/overflow", []testItem{{ID: "score", Unit: "score", Value: "9000000000000.000000", Weight: "1.25"}}, nil)
	result := runOne(t, Config{MaxConcurrency: 1, MaxItems: 10}, reference, candidate)
	if result.Code != codes.NumericOverflow {
		t.Fatalf("expected E_NUMERIC_OVERFLOW, got %+v", result)
	}
	if result.Ledger.Terminal != "rolled_back" || len(result.Ledger.Entries) != 0 {
		t.Fatalf("ledger was polluted: %+v", result.Ledger)
	}
	if result.Complete {
		t.Fatal("overflow result must not be complete")
	}
}

func TestItemBudgetKeepsPartialResultAndIsolatesSessions(t *testing.T) {
	referenceItems := []testItem{
		{ID: "a.item", Unit: "second", Value: "100", Weight: "1.0"},
		{ID: "b.item", Unit: "second", Value: "100", Weight: "1.0"},
		{ID: "c.item", Unit: "second", Value: "100", Weight: "1.0"},
	}
	reference := preparedReference(t, referenceItems)
	candidateItems := []testItem{
		{ID: "a.item", Unit: "second", Value: "90", Weight: "1.0"},
		{ID: "b.item", Unit: "second", Value: "90", Weight: "1.0"},
		{ID: "c.item", Unit: "second", Value: "90", Weight: "1.0"},
	}
	results := Run(context.Background(), []Input{
		makeInput(t, "packages/budget", candidateItems, nil),
		makeInput(t, "packages/valid", candidateItems[:1], nil),
	}, reference, Config{MaxConcurrency: 1, MaxItems: 2})
	if len(results) != 2 {
		t.Fatalf("expected two results, got %d", len(results))
	}
	budget, valid := results[0], results[1]
	if budget.Code != codes.ItemBudget || budget.ProcessedItems != 2 || budget.Complete {
		t.Fatalf("unexpected budget result: %+v", budget)
	}
	if budget.ScoreMicros != -20_000_000 || budget.Ledger.Terminal != "partial" || len(budget.Ledger.Entries) == 0 {
		t.Fatalf("partial score/ledger missing: score=%d ledger=%+v", budget.ScoreMicros, budget.Ledger)
	}
	if !valid.Complete || valid.Code != codes.None || valid.ScoreMicros != -10_000_000 {
		t.Fatalf("budget result polluted another session: %+v", valid)
	}
}

func TestStrictAndTolerantErrorBound(t *testing.T) {
	reference := preparedReference(t, []testItem{{ID: "timestamp", Unit: "filetime", Value: "133485408000000000", Weight: "1.0"}})
	candidate := makeInput(t, "packages/error-bound", []testItem{{ID: "timestamp", Unit: "filetime", Value: "133485408009999999", Weight: "1.0"}}, nil)
	strict := runOne(t, Config{Mode: ModeStrict, MaxConcurrency: 1, MaxItems: 10, MaxErrorBoundMicros: 100}, reference, candidate)
	if strict.Code != codes.ErrorBound || strict.Ledger.Terminal != "rolled_back" || len(strict.Ledger.Entries) != 0 {
		t.Fatalf("strict mode did not reject error bound: %+v", strict)
	}
	tolerant := runOne(t, Config{Mode: ModeTolerant, MaxConcurrency: 1, MaxItems: 10, MaxErrorBoundMicros: 100}, reference, candidate)
	if tolerant.Code != codes.None || !tolerant.Complete || tolerant.Ledger.Terminal != "committed" {
		t.Fatalf("tolerant mode did not accept error bound: %+v", tolerant)
	}
}

func TestConcurrencyLimitQueuesAllSessions(t *testing.T) {
	reference := preparedReference(t, []testItem{{ID: "item", Unit: "second", Value: "100", Weight: "1.0"}})
	inputs := make([]Input, 0, 6)
	for i := 0; i < 6; i++ {
		inputs = append(inputs, makeInput(t, fmt.Sprintf("packages/p%d", i),
			[]testItem{{ID: "item", Unit: "second", Value: "90", Weight: "1.0"}}, nil))
	}
	results := Run(context.Background(), inputs, reference, Config{MaxConcurrency: 1, MaxItems: 10})
	if len(results) != len(inputs) {
		t.Fatalf("expected all %d sessions, got %d", len(inputs), len(results))
	}
	seen := make(map[string]bool)
	for _, result := range results {
		if !result.Complete {
			t.Fatalf("session %s was rejected: %+v", result.PackagePath, result)
		}
		if seen[result.SessionID] {
			t.Fatalf("duplicate session id %s", result.SessionID)
		}
		seen[result.SessionID] = true
	}
}
