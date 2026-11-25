package store

import (
	"context"
	"path/filepath"
	"testing"

	"configaudit/internal/fixedpoint"
	"configaudit/internal/report"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "audit.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func sampleReport() *report.Report {
	return &report.Report{
		NodeCount: 1, KeyCount: 1, CombinationCnt: 1,
		Keys: []*report.KeyResult{{
			NodeID: "n1", KeyPath: "k1", WinnerChannel: "file",
			Combinations: []report.CombinationResult{{
				ItemID: "n1|k1|file", NodeID: "n1", KeyPath: "k1", Channel: "file",
				AbsDeviation: "0", RelDeviation: "0", ErrorBound: "1", WithinBound: true,
			}},
		}},
	}
}

func TestSnapshotMonotonicAndChecksum(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	s1 := &Snapshot{Version: 1, CreatedAt: "t1", Report: sampleReport()}
	if err := st.SaveSnapshot(ctx, s1); err != nil {
		t.Fatal(err)
	}
	// Replaying an older/equal version must fail while the old snapshot survives.
	old := &Snapshot{Version: 1, CreatedAt: "t1b", Report: sampleReport()}
	if err := st.SaveSnapshot(ctx, old); fixedpoint.CodeOf(err) != fixedpoint.ESnapshotInvalid {
		t.Fatalf("code = %v", fixedpoint.CodeOf(err))
	}
	loaded, err := st.LoadSnapshot(ctx, 1)
	if err != nil {
		t.Fatalf("old snapshot must remain usable: %v", err)
	}
	if loaded.Checksum != s1.Checksum {
		t.Fatalf("checksum mismatch")
	}
	// Bad declared checksum is rejected.
	bad := &Snapshot{Version: 2, CreatedAt: "t2", Checksum: "sha256:deadbeef", Report: sampleReport()}
	if err := st.SaveSnapshot(ctx, bad); fixedpoint.CodeOf(err) != fixedpoint.ESnapshotInvalid {
		t.Fatalf("code = %v", fixedpoint.CodeOf(err))
	}
}

func TestRecoveryAbortsRunningAudit(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	snap := &Snapshot{Version: 1, CreatedAt: "t", Report: sampleReport()}
	if err := st.SaveSnapshot(ctx, snap); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash between intent write and completion.
	if err := st.CreateAuditIntent(ctx, "req-x", "hash", "full", 1, 2); err != nil {
		t.Fatal(err)
	}
	rep, err := st.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.RecoveredVersion != 1 {
		t.Fatalf("recovered version = %d", rep.RecoveredVersion)
	}
	if len(rep.AbortedAudits) != 1 {
		t.Fatalf("aborted audits = %v", rep.AbortedAudits)
	}
	row, err := st.GetAudit(ctx, "req-x")
	if err != nil || row.Status != StatusAborted {
		t.Fatalf("row = %+v err = %v", row, err)
	}
	// The complete snapshot remains queryable.
	if _, err := st.LoadSnapshot(ctx, 1); err != nil {
		t.Fatal(err)
	}
}

func TestIncrementalStaleBase(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	if err := st.SaveSnapshot(ctx, &Snapshot{Version: 1, CreatedAt: "t", Report: sampleReport()}); err != nil {
		t.Fatal(err)
	}
	_, err := st.SaveIncremental(ctx, "inc-1", "hash", 99, sampleReport())
	if fixedpoint.CodeOf(err) != fixedpoint.EStaleBase {
		t.Fatalf("code = %v", fixedpoint.CodeOf(err))
	}
	// State unchanged: still only version 1.
	v, err := st.LatestSnapshotVersion(ctx)
	if err != nil || v != 1 {
		t.Fatalf("version = %d err = %v", v, err)
	}
}

func TestLoadSnapshotRejectsTamperedPayload(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	snap := &Snapshot{Version: 1, CreatedAt: "t", Report: sampleReport()}
	if err := st.SaveSnapshot(ctx, snap); err != nil {
		t.Fatal(err)
	}
	// Tamper with the stored payload while keeping the checksum row intact.
	if _, err := st.db.ExecContext(ctx,
		`UPDATE snapshots SET payload=? WHERE version=1`, []byte("{\"tampered\":true}")); err != nil {
		t.Fatal(err)
	}
	_, err := st.LoadSnapshot(ctx, 1)
	if fixedpoint.CodeOf(err) != fixedpoint.ESnapshotInvalid {
		t.Fatalf("code = %v", fixedpoint.CodeOf(err))
	}
}
