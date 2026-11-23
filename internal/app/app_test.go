package app

import (
	"path/filepath"
	"testing"

	"injectctl/internal/model"
	"injectctl/internal/report"
)

func TestPipelineDeterministicByteIdentical(t *testing.T) {
	dir1 := filepath.Join(t.TempDir(), "run1")
	dir2 := filepath.Join(t.TempDir(), "run2")

	res1, err := Run(RunConfig{WorkDir: dir1, MaxConcurrent: 2, MaxQueueDepth: 16})
	if err != nil {
		t.Fatal(err)
	}
	res2, err := Run(RunConfig{WorkDir: dir2, MaxConcurrent: 2, MaxQueueDepth: 16})
	if err != nil {
		t.Fatal(err)
	}

	doc1 := report.Build(res1.ContextVer, res1.Results, res1.ParseErrors, res1.Chain)
	doc2 := report.Build(res2.ContextVer, res2.Results, res2.ParseErrors, res2.Chain)
	raw1, err := report.Render(doc1)
	if err != nil {
		t.Fatal(err)
	}
	raw2, err := report.Render(doc2)
	if err != nil {
		t.Fatal(err)
	}
	if !report.EqualBytes(raw1, raw2) {
		t.Fatal("repeated runs must be byte-identical")
	}

	// Core adjudication expectations.
	if doc1.SnapshotChainStatus != model.CodeOK || len(doc1.SnapshotChain) != 3 {
		t.Fatalf("snapshot chain wrong: %s %d", doc1.SnapshotChainStatus, len(doc1.SnapshotChain))
	}
	codes := map[string]bool{}
	for _, c := range doc1.BusinessCodes {
		codes[c] = true
	}
	if !codes[model.CodeCtxEscape] || !codes[model.CodeOK] {
		t.Fatalf("missing expected business codes: %v", doc1.BusinessCodes)
	}

	var escapeCount, okCount int
	for _, b := range doc1.Batches {
		for _, sm := range b.Samples {
			switch sm.Code {
			case model.CodeCtxEscape:
				escapeCount++
				if sm.RawOffset < 0 || sm.LandedOffset < 0 || sm.RuleID == "" {
					t.Fatalf("escape verdict must carry offsets and rule id: %+v", sm)
				}
			case model.CodeOK:
				okCount++
			}
		}
	}
	if escapeCount != 5 || okCount != 7 {
		t.Fatalf("verdict distribution wrong: escape=%d ok=%d", escapeCount, okCount)
	}
}

func TestAllFiveAnomalyChecks(t *testing.T) {
	rep, err := RunSelfCheck(filepath.Join(t.TempDir(), "sc"))
	if err != nil {
		t.Fatal(err)
	}
	if !rep.AllPassed || len(rep.Checks) != 5 {
		t.Fatalf("self-check failure: %+v", rep)
	}
	want := map[string]string{
		"empty_payload":               model.CodeEmptyPayload,
		"context_out_of_bounds":       model.CodeContextOutOfBounds,
		"snapshot_version_gap":        model.CodeSnapshotVersion,
		"concurrent_batch_contention": model.CodeOK,
		"encode_byte_limit":           model.CodeEncodeLimit,
	}
	for _, c := range rep.Checks {
		if want[c.Name] != c.Code {
			t.Fatalf("check %s expected code %s got %s", c.Name, want[c.Name], c.Code)
		}
	}
}

func TestBudgetCheckStandalone(t *testing.T) {
	res := BudgetCheck(filepath.Join(t.TempDir(), "budget"))
	if !res.Passed {
		t.Fatalf("budget check failed: %+v", res)
	}
}

func TestInputLoadingAndParseErrors(t *testing.T) {
	dir := t.TempDir()
	res, err := Run(RunConfig{WorkDir: filepath.Join(dir, "p"), MaxConcurrent: 1, MaxQueueDepth: 4})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Results) == 0 {
		t.Fatal("pipeline produced no batches")
	}
	// Standard generator is valid: no parse errors expected.
	if len(res.ParseErrors) != 0 {
		t.Fatalf("unexpected parse errors: %+v", res.ParseErrors)
	}
}
