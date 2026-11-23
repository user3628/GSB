package store

import (
	"path/filepath"
	"testing"

	"injectctl/internal/model"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func seedSnapshots(t *testing.T, st *Store, n int) {
	t.Helper()
	for v := 1; v <= n; v++ {
		chain, _ := st.LoadChain()
		running := ""
		for _, sp := range chain.Snapshots {
			running = ChainSummaryHash(running, sp.ContentHash)
		}
		verdicts := []model.Verdict{{SampleID: "s", Seq: 1, Code: model.CodeOK}}
		snap := NewSnapshot(v, "b", "ctx-1", verdicts, nil, running)
		if v == 1 {
			if err := st.SaveSnapshot(snap); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, err := st.SaveSnapshotAtomic(snap); err != nil {
			t.Fatal(err)
		}
	}
}

func TestContinuousChain(t *testing.T) {
	st := openTestStore(t)
	seedSnapshots(t, st, 4)
	chain, err := st.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if chain.LastContinuousVersion != 4 || len(chain.Snapshots) != 4 {
		t.Fatalf("chain wrong: %+v", chain)
	}
}

func TestVersionGapRejectedWithRollback(t *testing.T) {
	st := openTestStore(t)
	seedSnapshots(t, st, 3)
	if err := st.ForceGap(2); err != nil {
		t.Fatal(err)
	}
	chain, err := st.LoadChain()
	if err == nil {
		t.Fatal("expected E_SNAPSHOT_VERSION")
	}
	ve, ok := err.(*VersionError)
	if !ok || ve.Code != model.CodeSnapshotVersion || ve.LastContinuous != 1 {
		t.Fatalf("wrong error: %v", err)
	}
	if chain.ErrorCode != model.CodeSnapshotVersion || len(chain.Snapshots) != 1 {
		t.Fatalf("rollback prefix wrong: %+v", chain)
	}
}

func TestWholeReplacementDetected(t *testing.T) {
	st := openTestStore(t)
	seedSnapshots(t, st, 2)

	// Tamper with version 2 content while keeping its version number.
	tampered := Snapshot{
		Version: 2, BatchID: "b", RuleSetVersion: model.RuleSetVersion,
		ContextVersion: "ctx-1", ContentHash: "deadbeef", SummaryHash: "tampered",
		Verdicts: []model.Verdict{{SampleID: "x", Seq: 1, Code: model.CodeCtxEscape}},
	}
	if _, err := st.db.Exec(`UPDATE snapshots SET payload_json=? WHERE version=2`,
		mustJSON(t, tampered)); err != nil {
		t.Fatal(err)
	}
	_, err := st.LoadChain()
	if err == nil {
		t.Fatal("whole-replacement must be detected even with preserved version number")
	}
	if ve, ok := err.(*VersionError); !ok || ve.Code != model.CodeSnapshotVersion {
		t.Fatalf("wrong error: %v", err)
	}
}

func TestSettlementRoundTrip(t *testing.T) {
	st := openTestStore(t)
	verdicts := []model.Verdict{
		{SampleID: "s1", Seq: 1, Code: model.CodeOK, RawOffset: -1, LandedOffset: -1},
		{SampleID: "s2", Seq: 2, Code: model.CodeCtxEscape, RuleID: "R1", RawOffset: 3, LandedOffset: 3},
	}
	if err := st.SaveSettlement("b1", StatusSettled, verdicts, nil); err != nil {
		t.Fatal(err)
	}
	exists, status, err := st.BatchExists("b1")
	if err != nil || !exists || status != StatusSettled {
		t.Fatalf("batch persistence wrong: %v %s", exists, status)
	}
	loaded, err := st.LoadVerdicts("b1")
	if err != nil || len(loaded) != 2 || loaded[1].RuleID != "R1" || loaded[1].RawOffset != 3 {
		t.Fatalf("verdict round trip wrong: %+v err=%v", loaded, err)
	}
}
