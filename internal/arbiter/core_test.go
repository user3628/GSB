package arbiter

import (
	"context"
	"testing"
	"time"
)

func TestReadSnapshotMatrix(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	if _, err := h.eng.Read(ctx, ReadRequest{RowID: "r1", SnapshotVer: 99}); err == nil {
		t.Fatal("expected E_SNAPSHOT_AHEAD")
	} else if errCode(t, err) != CodeSnapshotAhead {
		t.Fatalf("got %v", err)
	}

	view, err := h.eng.Read(ctx, ReadRequest{RowID: "r1", SnapshotVer: 1})
	if err != nil {
		t.Fatalf("read snapshot 1: %v", err)
	}
	if view.Content != "v1-content" || view.RowVersion != 1 {
		t.Fatalf("unexpected seed view: %+v", view)
	}

	lease := mustAcquire(t, h, "r1", "alice", time.Minute)
	commit, err := h.eng.Commit(ctx, CommitRequest{
		RowID: "r1", Holder: "alice", LeaseVersion: lease.LeaseVersion,
		BaseRowVersion: 1, Content: "v2-content",
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if commit.CommitVer != 3 || commit.RowVersion != 2 {
		t.Fatalf("unexpected commit %+v", commit)
	}

	old, err := h.eng.Read(ctx, ReadRequest{RowID: "r1", SnapshotVer: 1})
	if err != nil || old.Content != "v1-content" {
		t.Fatalf("old snapshot changed: %+v err=%v", old, err)
	}
	later, err := h.eng.Read(ctx, ReadRequest{RowID: "r1", SnapshotVer: commit.CommitVer})
	if err != nil || later.Content != "v2-content" {
		t.Fatalf("new snapshot wrong: %+v err=%v", later, err)
	}
}

func TestLeaseHeldStaleAndExpiry(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	first := mustAcquire(t, h, "r1", "alice", 100*time.Millisecond)

	_, err := h.eng.Acquire(ctx, AcquireRequest{RowID: "r1", Holder: "bob", TTLMs: 50})
	if errCode(t, err) != CodeLeaseHeld {
		t.Fatalf("expected held, got %v", err)
	}

	// Expire via the deterministic clock; renewal must then be impossible.
	h.clk.Advance(150 * time.Millisecond)
	if !h.eng.expireIfDue("r1") {
		t.Fatal("expected lease to be expired by sweep")
	}
	_, err = h.eng.Renew(ctx, RenewRequest{RowID: "r1", Holder: "alice", LeaseVersion: first.LeaseVersion, TTLMs: 100})
	if errCode(t, err) != CodeLeaseLost {
		t.Fatalf("expected lease lost after expiry, got %v", err)
	}

	// Row becomes grantable again and lease_version advances (preemption proven).
	second := mustAcquire(t, h, "r1", "bob", 100*time.Millisecond)
	if second.LeaseVersion != first.LeaseVersion+1 {
		t.Fatalf("lease version did not advance: %d vs %d", second.LeaseVersion, first.LeaseVersion)
	}

	// Presenting the old (stale) lease version now yields E_LEASE_STALE with
	// the current version attached.
	_, err = h.eng.Renew(ctx, RenewRequest{RowID: "r1", Holder: "bob", LeaseVersion: first.LeaseVersion, TTLMs: 100})
	ae := requireAPI(t, err)
	if ae.Code != CodeLeaseStale {
		t.Fatalf("got %v", err)
	}
	if ae.Details["current_lease_ver"] != second.LeaseVersion {
		t.Fatalf("missing current version detail: %+v", ae.Details)
	}
}

func TestCommitChecksShareTransactionAndFailClosed(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	lease := mustAcquire(t, h, "r1", "alice", time.Minute)

	// Stale base row version.
	_, err := h.eng.Commit(ctx, CommitRequest{
		RowID: "r1", Holder: "alice", LeaseVersion: lease.LeaseVersion,
		BaseRowVersion: 99, Content: "x",
	})
	ae := requireAPI(t, err)
	if ae.Code != CodeEditStale || ae.Details["current_row_version"] != int64(1) {
		t.Fatalf("unexpected stale response: %+v", ae)
	}

	// Force lease expiry, then attempt a commit whose base version is still
	// correct. Lease loss must dominate and no row version may be produced.
	h.clk.Advance(time.Minute + time.Second)
	h.eng.expireIfDue("r1")
	_, err = h.eng.Commit(ctx, CommitRequest{
		RowID: "r1", Holder: "alice", LeaseVersion: lease.LeaseVersion,
		BaseRowVersion: 1, Content: "must-not-commit",
	})
	if errCode(t, err) != CodeLeaseLost {
		t.Fatalf("expected lease lost, got %v", err)
	}
	latest, err := h.eng.latestCommitted("r1")
	if err != nil || latest.RowVersion != 1 || latest.Content != "v1-content" {
		t.Fatalf("partial commit observed: %+v err=%v", latest, err)
	}
	ver, _ := h.st.CommitVer()
	if ver != 2 {
		t.Fatalf("commit_ver advanced on failed commit: %d", ver)
	}
}

func TestDeleteVisibilityAndCommitMonotonicity(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	lease := mustAcquire(t, h, "r2", "alice", time.Minute)
	resp, err := h.eng.Commit(ctx, CommitRequest{
		RowID: "r2", Holder: "alice", LeaseVersion: lease.LeaseVersion,
		BaseRowVersion: 1, Deleted: true,
	})
	if err != nil {
		t.Fatalf("delete commit: %v", err)
	}
	if !resp.Deleted {
		t.Fatal("expected deleted marker")
	}
	_, err = h.eng.Read(ctx, ReadRequest{RowID: "r2", SnapshotVer: resp.CommitVer})
	if errCode(t, err) != CodeRowDeleted {
		t.Fatalf("expected deleted row error, got %v", err)
	}
	// Snapshot before the delete still returns the original content.
	old, err := h.eng.Read(ctx, ReadRequest{RowID: "r2", SnapshotVer: 2})
	if err != nil || old.Deleted {
		t.Fatalf("old snapshot should remain visible: %+v err=%v", old, err)
	}
}

func requireAPI(t *testing.T, err error) *APIError {
	t.Helper()
	var ae *APIError
	if !asAPIError(err, &ae) {
		t.Fatalf("expected APIError, got %v", err)
	}
	return ae
}
