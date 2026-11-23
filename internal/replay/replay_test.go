package replay_test

import (
	"context"
	"path/filepath"
	"testing"

	"protocolfuzzlab/internal/codes"
	"protocolfuzzlab/internal/demo"
	"protocolfuzzlab/internal/model"
	"protocolfuzzlab/internal/protocol"
	"protocolfuzzlab/internal/replay"
	"protocolfuzzlab/internal/store"
)

func openDemo(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "lab.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if _, _, err := st.Initialize(demo.BaseCaseSet(), "replay test baseline"); err != nil {
		t.Fatal(err)
	}
	return st
}

func TestReplayIsDeterministicIdempotentAndObservable(t *testing.T) {
	ctx := context.Background()
	st := openDemo(t)
	opts := replay.Options{CaseSetVersion: 1, BatchSize: 1, MaxConcurrency: 2, CaseBudget: 1000}
	first, err := replay.Run(ctx, st, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Batches) != 3 || len(first.Snapshots) != 3 {
		t.Fatalf("batches=%d snapshots=%d", len(first.Batches), len(first.Snapshots))
	}
	if first.Stats.PeakInFlight > 2 || first.Stats.PeakQueued < 1 {
		t.Fatalf("unexpected scheduler stats: %#v", first.Stats)
	}
	for _, batch := range first.Batches {
		if batch.Execution.Code != codes.OK || !batch.NewSnapshot {
			t.Fatalf("batch %s code=%s new=%t", batch.BatchID, batch.Execution.Code, batch.NewSnapshot)
		}
	}
	second, err := replay.Run(ctx, st, opts)
	if err != nil {
		t.Fatal(err)
	}
	if second.ContentDigest != first.ContentDigest {
		t.Fatalf("content digest changed: %s vs %s", first.ContentDigest, second.ContentDigest)
	}
	for _, batch := range second.Batches {
		if batch.NewSnapshot {
			t.Fatalf("idempotent replay committed duplicate snapshot for %s", batch.BatchID)
		}
	}
	count, err := st.SnapshotCount()
	if err != nil || count != 3 {
		t.Fatalf("snapshot count=%d err=%v", count, err)
	}

	other := openDemo(t)
	third, err := replay.Run(ctx, other, opts)
	if err != nil {
		t.Fatal(err)
	}
	if third.ContentDigest != first.ContentDigest {
		t.Fatalf("independent database produced different bytes: %s vs %s", third.ContentDigest, first.ContentDigest)
	}
}

func TestCaseBudgetPreservesPartialResults(t *testing.T) {
	st := openDemo(t)
	result, err := replay.Run(context.Background(), st, replay.Options{
		CaseSetVersion: 1, BatchSize: 3, MaxConcurrency: 1, CaseBudget: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Batches) != 1 {
		t.Fatalf("batches=%d", len(result.Batches))
	}
	batch := result.Batches[0]
	if batch.Execution.Code != codes.CaseBudget {
		t.Fatalf("batch code=%s", batch.Execution.Code)
	}
	if len(batch.Execution.Cases) != 1 || len(batch.Execution.Unprocessed) != 2 {
		t.Fatalf("cases=%d unprocessed=%d", len(batch.Execution.Cases), len(batch.Execution.Unprocessed))
	}
	if batch.Execution.Cases[0].Code != codes.OK {
		t.Fatalf("partial case was not preserved: %#v", batch.Execution.Cases[0])
	}
	if batch.Snapshot.Version == 0 {
		t.Fatal("budgeted partial session was not snapshotted")
	}
}

func TestChecksumInjectionAndDependencyInvalidation(t *testing.T) {
	st := openDemo(t)
	tampered, err := replay.Run(context.Background(), st, replay.Options{
		CaseSetVersion: 1, BatchSize: 100, MaxConcurrency: 1, CaseBudget: 1000,
		NoCommit: true, TamperCaseID: "case-basic", TamperStep: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := caseCode(tampered, "case-basic"); got != codes.BadChecksum {
		t.Fatalf("tamper code=%s", got)
	}
	if count, _ := st.SnapshotCount(); count != 0 {
		t.Fatalf("NoCommit wrote %d snapshots", count)
	}

	if _, err := st.ApplyChangeSet(demo.BreakChange()); err != nil {
		t.Fatal(err)
	}
	broken, err := replay.Run(context.Background(), st, replay.Options{
		CaseSetVersion: 2, BatchSize: 100, MaxConcurrency: 1, CaseBudget: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := caseCode(broken, "case-ping"); got != codes.DepInvalidated {
		t.Fatalf("invalid case code=%s", got)
	}
	for _, key := range broken.Coverage {
		if key == (model.TransitionKey{From: protocol.StateReady, To: protocol.StateReady}) {
			t.Fatal("invalidated case still contributed READY->READY coverage")
		}
	}
}

func TestWholeReplacementDetectedFromContentIdentity(t *testing.T) {
	st := openDemo(t)
	ctx := context.Background()
	first, err := replay.Run(ctx, st, replay.Options{CaseSetVersion: 1, BatchSize: 3, MaxConcurrency: 1, CaseBudget: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Batches) != 1 {
		t.Fatalf("expected one logical slot, got %d", len(first.Batches))
	}
	if _, err := st.ApplyChangeSet(demo.ReplaceAllCasesChange()); err != nil {
		t.Fatal(err)
	}
	second, err := replay.Run(ctx, st, replay.Options{CaseSetVersion: 2, BatchSize: 3, MaxConcurrency: 1, CaseBudget: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Snapshots) != 2 {
		t.Fatalf("snapshots=%d", len(second.Snapshots))
	}
	latest := second.Snapshots[len(second.Snapshots)-1]
	if !latest.WholeReplacement {
		t.Fatalf("disjoint replacement case IDs were not identified: %#v", latest)
	}
	if !latest.ContentChanged {
		t.Fatal("content change flag not set")
	}
}

func caseCode(result replay.RunResult, caseID string) string {
	for _, batch := range result.Batches {
		for _, c := range batch.Execution.Cases {
			if c.CaseID == caseID {
				return c.Code
			}
		}
	}
	return ""
}
