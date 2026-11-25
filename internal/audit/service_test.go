package audit

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"configaudit/internal/fixedpoint"
	"configaudit/internal/store"
)

func newService(t *testing.T) (*Service, context.Context) {
	t.Helper()
	ctx := context.Background()
	ds := loadDataset(t)
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "audit.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return NewService(st, ds), ctx
}

func TestIdempotentSubmit(t *testing.T) {
	svc, ctx := newService(t)
	payload := map[string]any{"k": "v"}
	s1, err := svc.FullAudit(ctx, "req-1", payload)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := svc.FullAudit(ctx, "req-1", payload)
	if err != nil {
		t.Fatal(err)
	}
	if s1.Version != s2.Version || s1.Checksum != s2.Checksum {
		t.Fatalf("repeated submit changed result: %+v vs %+v", s1, s2)
	}
}

func TestConflictingRequest(t *testing.T) {
	svc, ctx := newService(t)
	if _, err := svc.FullAudit(ctx, "req-2", map[string]any{"k": 1}); err != nil {
		t.Fatal(err)
	}
	_, err := svc.FullAudit(ctx, "req-2", map[string]any{"k": 2})
	if fixedpoint.CodeOf(err) != fixedpoint.EConflictingRequest {
		t.Fatalf("code = %v", fixedpoint.CodeOf(err))
	}
}

func TestConcurrentSameRequestCoalesces(t *testing.T) {
	svc, ctx := newService(t)
	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	versions := make([]int64, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			snap, err := svc.FullAudit(ctx, "hot-req", map[string]any{"seed": 7})
			if err == nil {
				versions[i] = snap.Version
			}
			errs[i] = err
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("concurrent submit error: %v", err)
		}
	}
	for _, v := range versions {
		if v != versions[0] {
			t.Fatalf("versions diverged: %v", versions)
		}
	}
}

func TestIncrementalBaseCheck(t *testing.T) {
	svc, ctx := newService(t)
	snap, err := svc.FullAudit(ctx, "base-req", map[string]any{"seed": 1})
	if err != nil {
		t.Fatal(err)
	}
	// Stale base must be rejected without mutating state.
	if _, err := svc.Incremental(ctx, "inc-stale", snap.Version-1,
		[]string{"runtime.timeout_ms"}, map[string]any{"x": 1}); fixedpoint.CodeOf(err) != fixedpoint.EStaleBase {
		t.Fatalf("code = %v", fixedpoint.CodeOf(err))
	}
	inc, err := svc.Incremental(ctx, "inc-ok", snap.Version,
		[]string{"runtime.timeout_ms"}, map[string]any{"x": 1})
	if err != nil {
		t.Fatal(err)
	}
	if inc.Version != snap.Version+1 {
		t.Fatalf("incremental version = %d", inc.Version)
	}
}
