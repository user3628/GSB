package engine

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"casengine/internal/clock"
	"casengine/internal/codes"
	"casengine/internal/metrics"
	"casengine/internal/store"
)

func newTestEngine(t *testing.T) (*Engine, *store.Store, *clock.ScriptedClock, *metrics.Collector) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.UpsertTenant("tn", "name", "secret"); err != nil {
		t.Fatal(err)
	}
	sc := clock.NewScripted()
	mc := metrics.NewCollector(128)
	cfg := DefaultConfig()
	cfg.CleanupInterval = 0
	e := New(st, sc, mc, cfg)
	t.Cleanup(e.Close)
	return e, st, sc, mc
}

// TestConcurrentOneTimeRedemption hammers one ticket with real goroutines and
// asserts exactly one success regardless of scheduling.
func TestConcurrentOneTimeRedemption(t *testing.T) {
	e, _, _, _ := newTestEngine(t)
	issued, err := e.Issue(IssueRequest{
		ID: "ST-RACE", TenantID: "tn", SPID: "sp", UserID: "u", TTLSeconds: 300,
	})
	if err != nil {
		t.Fatal(err)
	}
	const n = 64
	var wg sync.WaitGroup
	results := make([]Result, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = e.Validate(context.Background(), ValidateRequest{
				Seq: int64(i + 1), Ticket: issued.Ticket, PresentSP: "sp",
			})
		}(i)
	}
	close(start)
	wg.Wait()

	ok, consumed := 0, 0
	for _, r := range results {
		switch r.Code {
		case codes.OK:
			ok++
		case codes.Consumed:
			consumed++
		default:
			t.Fatalf("unexpected code %s", r.Code)
		}
		if r.LatencyNS <= 0 {
			t.Fatalf("non-positive latency: %d", r.LatencyNS)
		}
	}
	if ok != 1 {
		t.Fatalf("want exactly 1 success, got %d", ok)
	}
	if consumed != n-1 {
		t.Fatalf("want %d consumed, got %d", n-1, consumed)
	}
	ticket, err := e.store.GetTicket("ST-RACE")
	if err != nil {
		t.Fatal(err)
	}
	if ticket.Status != store.StatusRedeemed {
		t.Fatalf("ticket status = %s, want REDEEMED", ticket.Status)
	}
	reds, err := e.store.ListRedemptions("tn")
	if err != nil {
		t.Fatal(err)
	}
	if len(reds) != 1 {
		t.Fatalf("redemptions = %d, want 1", len(reds))
	}
}

func TestValidationCodes(t *testing.T) {
	e, st, sc, _ := newTestEngine(t)

	// Signature tamper.
	bad, err := e.Issue(IssueRequest{ID: "ST-SIG", TenantID: "tn", SPID: "sp", UserID: "u", TTLSeconds: 300})
	if err != nil {
		t.Fatal(err)
	}
	tampered := tamperRaw(t, bad.Ticket)
	if got := e.Validate(context.Background(), ValidateRequest{Seq: 1, Ticket: tampered, PresentSP: "sp"}).Code; got != codes.Signature {
		t.Fatalf("tamper code = %s", got)
	}
	if still, _ := st.GetTicket("ST-SIG"); still.Status != store.StatusValid {
		t.Fatalf("failed validation must not consume ticket, status=%s", still.Status)
	}

	// Audience mismatch.
	aud, err := e.Issue(IssueRequest{ID: "ST-AUD", TenantID: "tn", SPID: "sp", UserID: "u", TTLSeconds: 300})
	if err != nil {
		t.Fatal(err)
	}
	if got := e.Validate(context.Background(), ValidateRequest{Seq: 2, Ticket: aud.Ticket, PresentSP: "other-sp"}).Code; got != codes.Audience {
		t.Fatalf("audience code = %s", got)
	}
	if still, _ := st.GetTicket("ST-AUD"); still.Status != store.StatusValid {
		t.Fatalf("audience failure must not consume ticket, status=%s", still.Status)
	}

	// Expired ticket.
	exp, err := e.Issue(IssueRequest{ID: "ST-EXP", TenantID: "tn", SPID: "sp", UserID: "u", TTLSeconds: 1})
	if err != nil {
		t.Fatal(err)
	}
	sc.Advance(2_000_000_000) // 2s in virtual time
	if got := e.Validate(context.Background(), ValidateRequest{Seq: 3, Ticket: exp.Ticket, PresentSP: "sp"}).Code; got != codes.Expired {
		t.Fatalf("expired code = %s", got)
	}
}

func TestSnapshotChainAndContentDigest(t *testing.T) {
	e, st, _, _ := newTestEngine(t)
	for i := 0; i < 3; i++ {
		if _, err := e.Issue(IssueRequest{
			ID: "ST-CHAIN-" + string(rune('A'+i)), TenantID: "tn", SPID: "sp",
			UserID: "u", TTLSeconds: 300, Seq: int64(i + 1),
		}); err != nil {
			t.Fatal(err)
		}
	}
	chain, err := e.VerifyTenantChain("tn")
	if err != nil {
		t.Fatal(err)
	}
	if !chain.Contiguous || chain.HeadVersion != 3 || !chain.HeadMatches {
		t.Fatalf("unexpected chain: %+v", chain)
	}

	// Inject corruption by deleting the head snapshot. The remaining versions
	// stay contiguous but the head digest no longer matches live state, which
	// must be detected and the tenant faulted, then rolled back.
	if _, err := st.InjectGap("tn"); err != nil {
		t.Fatal(err)
	}
	gapped, err := e.VerifyTenantChain("tn")
	if err != nil {
		t.Fatal(err)
	}
	if gapped.HeadMatches || e.TenantFault("tn") == nil {
		t.Fatalf("expected gap fault, got %+v", gapped)
	}
	rec, err := e.RollbackTenant("tn")
	if err != nil {
		t.Fatal(err)
	}
	if !rec.Contiguous || !rec.HeadMatches || !rec.Recovered || e.TenantFault("tn") != nil {
		t.Fatalf("rollback failed: %+v", rec)
	}
}

func TestContentionLimit(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.UpsertTenant("tn", "n", "s"); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.MaxConcurrency = 1
	cfg.AcquireTimeout = 1_000_000 // 1ms
	cfg.CleanupInterval = 0
	mc := metrics.NewCollector(8)
	e := New(st, clock.NewScripted(), mc, cfg)
	defer e.Close()

	issued, err := e.Issue(IssueRequest{ID: "ST-C1", TenantID: "tn", SPID: "sp", UserID: "u", TTLSeconds: 300})
	if err != nil {
		t.Fatal(err)
	}
	// Hold the only slot, then a second request must time out into CONTENTION.
	e.sem <- struct{}{}
	res := e.Validate(context.Background(), ValidateRequest{Seq: 1, Ticket: issued.Ticket, PresentSP: "sp"})
	<-e.sem
	if res.Code != codes.Contention {
		t.Fatalf("code = %s, want E_TICKET_CONTENTION", res.Code)
	}
}
