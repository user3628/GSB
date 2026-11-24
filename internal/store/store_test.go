package store

import (
	"path/filepath"
	"testing"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.UpsertTenant("tn", "n", "secret"); err != nil {
		t.Fatal(err)
	}
	return st
}

func sampleTicket(id string) Ticket {
	return Ticket{
		ID: id, TenantID: "tn", SPID: "sp", UserID: "u",
		Raw: "raw", IssuedAt: 1, ExpiresAt: 999, Status: StatusValid,
	}
}

func TestSnapshotVersionGapDetected(t *testing.T) {
	st := openTestStore(t)
	for i, id := range []string{"ST-1", "ST-2", "ST-3"} {
		if _, err := st.Issue(sampleTicket(id), int64(i+1)); err != nil {
			t.Fatal(err)
		}
	}
	// Delete a middle version to create a real gap (1,3).
	if _, err := st.db.Exec(`DELETE FROM snapshots WHERE tenant_id='tn' AND version=2`); err != nil {
		t.Fatal(err)
	}
	chain, err := st.VerifyChain("tn")
	if err != nil {
		t.Fatal(err)
	}
	if chain.Contiguous || chain.GapAt != 2 {
		t.Fatalf("expected gap at version 2, got %+v", chain)
	}

	// Rollback to last contiguous version (v1) and rebuild a healthy chain.
	if _, err := st.Rollback("tn", chain.LastContig, 42); err != nil {
		t.Fatal(err)
	}
	after, err := st.VerifyChain("tn")
	if err != nil {
		t.Fatal(err)
	}
	if !after.Contiguous || !after.HeadMatches {
		t.Fatalf("chain not restored: %+v", after)
	}
}

func TestRedeemAtomicAndIdempotent(t *testing.T) {
	st := openTestStore(t)
	tk := sampleTicket("ST-ONCE")
	if _, err := st.Issue(tk, 1); err != nil {
		t.Fatal(err)
	}
	out, err := st.Redeem(&tk, 7, 11)
	if err != nil {
		t.Fatal(err)
	}
	if out.Version < 0 {
		t.Fatal("first redeem must succeed")
	}
	got, _ := st.GetTicket("ST-ONCE")
	if got.Status != StatusRedeemed {
		t.Fatalf("status=%s", got.Status)
	}
	// Second redeem must be a no-op signalled by Version == -1.
	out2, err := st.Redeem(got, 8, 12)
	if err != nil {
		t.Fatal(err)
	}
	if out2.Version != -1 {
		t.Fatalf("second redeem must not mutate, got version %d", out2.Version)
	}
	reds, _ := st.ListRedemptions("tn")
	if len(reds) != 1 {
		t.Fatalf("redemptions=%d want 1", len(reds))
	}
}
