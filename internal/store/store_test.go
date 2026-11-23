package store_test

import (
	"path/filepath"
	"testing"

	"protocolfuzzlab/internal/codes"
	"protocolfuzzlab/internal/demo"
	"protocolfuzzlab/internal/model"
	"protocolfuzzlab/internal/store"
)

func openDemo(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "lab.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if _, _, err := st.Initialize(demo.BaseCaseSet(), "test baseline"); err != nil {
		t.Fatal(err)
	}
	return st
}

func TestIncrementalVersionsAndHistoryAreRetained(t *testing.T) {
	st := openDemo(t)
	v1, err := st.LoadCaseSet(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(v1.CaseSet.Cases) != 3 {
		t.Fatalf("v1 case count %d", len(v1.CaseSet.Cases))
	}
	result, err := st.ApplyChangeSet(demo.IncrementalChange())
	if err != nil {
		t.Fatal(err)
	}
	if result.Current.Version != 2 || len(result.Current.CaseSet.Cases) != 4 {
		t.Fatalf("unexpected v2: %#v", result.Current)
	}
	if len(result.Summary.Added) != 8 || len(result.Summary.AffectedCases) != 1 || result.Summary.AffectedCases[0] != "case-bulk" {
		t.Fatalf("unexpected diff: %#v", result.Summary)
	}
	againV1, err := st.LoadCaseSet(1)
	if err != nil {
		t.Fatal(err)
	}
	if againV1.Digest != v1.Digest || len(againV1.CaseSet.Cases) != 3 {
		t.Fatal("old version was not retained intact")
	}

	breakResult, err := st.ApplyChangeSet(demo.BreakChange())
	if err != nil {
		t.Fatal(err)
	}
	if breakResult.Current.Version != 3 {
		t.Fatalf("head %d", breakResult.Current.Version)
	}
	diag := diagnosticForCase(breakResult.Current.CaseSet.Invalidations, "case-ping")
	if diag == nil || diag.Code != codes.DepInvalidated {
		t.Fatalf("case-ping invalidation missing: %#v", breakResult.Current.CaseSet.Invalidations)
	}
	v2, err := st.LoadCaseSet(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(v2.CaseSet.Cases) != 4 || len(v2.CaseSet.Invalidations) != 0 {
		t.Fatal("v2 history changed after v3")
	}
}

func TestInvalidAddedReferencesAndCyclesDoNotAdvanceVersion(t *testing.T) {
	st := openDemo(t)
	for _, tc := range []struct {
		name string
		cs   model.ChangeSet
		code string
	}{
		{"missing-seed", demo.MissingSeedChange(), codes.DepInvalidated},
		{"operator-cycle", demo.OperatorCycleChange(), codes.DepCycle},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := st.ApplyChangeSet(tc.cs)
			if err == nil {
				t.Fatal("expected rejection")
			}
			if got := codes.From(err).Code; got != tc.code {
				t.Fatalf("code=%s want=%s", got, tc.code)
			}
			head, err := st.Head()
			if err != nil {
				t.Fatal(err)
			}
			if head.Version != 1 {
				t.Fatalf("version advanced to %d", head.Version)
			}
			versions, err := st.CaseSetVersions()
			if err != nil {
				t.Fatal(err)
			}
			if len(versions) != 1 {
				t.Fatalf("partial version rows persisted: %d", len(versions))
			}
		})
	}
}

func TestSnapshotChainGapFallbackAndFailedCommit(t *testing.T) {
	st := openDemo(t)
	for i := 1; i <= 3; i++ {
		_, created, err := st.CommitSnapshot(store.SnapshotCommit{
			CaseSetVersion: 1,
			BatchID:        "batch-00" + string(rune('0'+i)),
			LogicalBatch:   "slot-00" + string(rune('0'+i)),
			ReplayKey:      "replay-" + string(rune('0'+i)),
			Content:        []byte(`{"schema":"test","batch":` + string(rune('0'+i)) + `}`),
		})
		if err != nil || !created {
			t.Fatalf("commit %d: created=%t err=%v", i, created, err)
		}
	}
	chain, err := st.SnapshotChain()
	if err != nil || len(chain) != 3 {
		t.Fatalf("chain len=%d err=%v", len(chain), err)
	}
	if err := st.InjectSnapshotGap(2); err != nil {
		t.Fatal(err)
	}
	chain, err = st.SnapshotChain()
	if got := codes.From(err).Code; got != codes.SnapshotVersion {
		t.Fatalf("gap code=%s", got)
	}
	coded := codes.From(err)
	if coded.FallbackVersion != 1 || len(chain) != 1 {
		t.Fatalf("fallback=%d chain=%d", coded.FallbackVersion, len(chain))
	}
	countBefore, _ := st.SnapshotCount()
	maxBefore, _ := st.MaxSnapshotVersion()
	_, _, err = st.CommitSnapshot(store.SnapshotCommit{
		CaseSetVersion: 1, BatchID: "batch-new", LogicalBatch: "slot-new",
		ReplayKey: "replay-new", Content: []byte(`{"schema":"test","batch":4}`),
	})
	if got := codes.From(err).Code; got != codes.SnapshotVersion {
		t.Fatalf("commit across gap code=%s", got)
	}
	countAfter, _ := st.SnapshotCount()
	maxAfter, _ := st.MaxSnapshotVersion()
	if countAfter != countBefore || maxAfter != maxBefore {
		t.Fatalf("failed commit advanced snapshot state: count %d->%d max %d->%d", countBefore, countAfter, maxBefore, maxAfter)
	}
}

func diagnosticForCase(diags []model.Diagnostic, id string) *model.Diagnostic {
	for i := range diags {
		if diags[i].Entity == model.EntityCase && diags[i].EntityID == id {
			return &diags[i]
		}
	}
	return nil
}
