package arbiter

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"leasearbiter/internal/clock"
	"leasearbiter/internal/store"
)

type harness struct {
	st  *store.Store
	eng *Engine
	clk *clock.Manual
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir := t.TempDir()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewManual(start)
	st, err := store.Open(filepath.Join(dir, "arbiter.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	eng := New(Config{Store: st, Clock: clk, Sleeper: clk})
	t.Cleanup(eng.Close)
	if _, err := eng.Seed(context.Background(), []SeedRow{
		{RowID: "r1", Content: "v1-content"},
		{RowID: "r2", Content: "r2-content"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return &harness{st: st, eng: eng, clk: clk}
}

func mustAcquire(t *testing.T, h *harness, rowID, holder string, ttl time.Duration) *AcquireResponse {
	t.Helper()
	resp, err := h.eng.Acquire(context.Background(), AcquireRequest{
		RowID: rowID, Holder: holder, TTLMs: ttl.Milliseconds(),
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	return resp
}

func errCode(t *testing.T, err error) ErrorCode {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var ae *APIError
	if !asAPIError(err, &ae) {
		t.Fatalf("not an APIError: %v", err)
	}
	return ae.Code
}

func asAPIError(err error, target **APIError) bool {
	for err != nil {
		if ae, ok := err.(*APIError); ok {
			*target = ae
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
