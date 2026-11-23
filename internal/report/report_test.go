package report_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"protocolfuzzlab/internal/codes"
	"protocolfuzzlab/internal/demo"
	"protocolfuzzlab/internal/model"
	"protocolfuzzlab/internal/protocol"
	"protocolfuzzlab/internal/replay"
	"protocolfuzzlab/internal/report"
	"protocolfuzzlab/internal/store"
)

func TestReportCoverageLossAndDeterministicBytes(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "lab.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, _, err := st.Initialize(demo.BaseCaseSet(), "report baseline"); err != nil {
		t.Fatal(err)
	}
	opts := replay.Options{CaseSetVersion: 1, BatchSize: 1, MaxConcurrency: 2, CaseBudget: 1000}
	if _, err := replay.Run(ctx, st, opts); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ApplyChangeSet(demo.IncrementalChange()); err != nil {
		t.Fatal(err)
	}
	opts.CaseSetVersion = 2
	if _, err := replay.Run(ctx, st, opts); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ApplyChangeSet(demo.BreakChange()); err != nil {
		t.Fatal(err)
	}
	opts.CaseSetVersion = 3
	runResult, err := replay.Run(ctx, st, opts)
	if err != nil {
		t.Fatal(err)
	}
	first, err := report.Build(st, runResult)
	if err != nil {
		t.Fatal(err)
	}
	if first.SampledAt != store.SampleMarker {
		t.Fatalf("unstable sampled_at %q", first.SampledAt)
	}
	if first.SnapshotChainState != "CONTIGUOUS" || len(first.SnapshotChain) != 11 {
		t.Fatalf("snapshot chain state=%s len=%d", first.SnapshotChainState, len(first.SnapshotChain))
	}
	if !containsTransition(first.CoverageLost, protocol.StateReady, protocol.StateReady) {
		t.Fatalf("READY->READY loss missing: %#v", first.CoverageLost)
	}
	if !containsTransition(first.CoverageLost, protocol.StateReady, protocol.StateClosed) {
		t.Fatalf("READY->CLOSED loss missing: %#v", first.CoverageLost)
	}
	foundPing := false
	for _, affected := range first.AffectedCases {
		if affected.CaseID == "case-ping" {
			foundPing = true
			if affected.Code != codes.DepInvalidated || len(affected.RootChain) == 0 {
				t.Fatalf("bad affected case: %#v", affected)
			}
		}
	}
	if !foundPing {
		t.Fatal("case-ping missing from affected cases")
	}
	for i := 1; i < len(first.Replay.Cases); i++ {
		if first.Replay.Cases[i-1].CaseID > first.Replay.Cases[i].CaseID {
			t.Fatal("cases are not sorted by case id")
		}
	}
	firstBytes, err := report.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}

	reused, err := replay.Run(ctx, st, opts)
	if err != nil {
		t.Fatal(err)
	}
	second, err := report.Build(st, reused)
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := report.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstBytes, secondBytes) {
		t.Fatalf("report bytes differ across repeated runs\nfirst=%s\nsecond=%s", firstBytes, secondBytes)
	}
	if bytes.Contains(firstBytes, []byte("127.0.0.1:")) {
		t.Fatal("ephemeral target port leaked into deterministic report")
	}
}

func containsTransition(list []model.TransitionKey, from, to protocol.State) bool {
	for _, key := range list {
		if key.From == from && key.To == to {
			return true
		}
	}
	return false
}
