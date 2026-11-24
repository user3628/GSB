package pool

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"local/sqlitepool/internal/dbconn"
	"local/sqlitepool/internal/domain"
	"local/sqlitepool/internal/store"
)

type fakeConn struct {
	id     int64
	closed atomic.Bool
}

func (c *fakeConn) Ping(context.Context) error {
	if c.closed.Load() {
		return dbconn.ErrConnClosed
	}
	return nil
}

func (c *fakeConn) Query(context.Context, string, ...any) (domain.QueryResult, error) {
	if c.closed.Load() {
		return domain.QueryResult{}, dbconn.ErrConnClosed
	}
	return domain.QueryResult{Columns: []string{"value"}, Rows: [][]any{{c.id}}}, nil
}

func (c *fakeConn) Close() error {
	c.closed.Store(true)
	return nil
}

type fakeOpener struct {
	next atomic.Int64
}

func (o *fakeOpener) Open(context.Context) (dbconn.Conn, error) {
	return &fakeConn{id: o.next.Add(1)}, nil
}

func newTestPool(t *testing.T, cfg domain.Config) (*Pool, *store.Store) {
	t.Helper()
	if cfg.Version == 0 {
		cfg.Version = 1
	}
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "meta.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	p, err := New(context.Background(), cfg, st, &fakeOpener{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = p.Shutdown(ctx)
	})
	return p, st
}

func waitFor(t *testing.T, p *Pool, predicate func(domain.PoolSnapshot) bool) domain.PoolSnapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap := p.Snapshot()
		if predicate(snap) {
			return snap
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not reached; snapshot=%+v", p.Snapshot())
	return domain.PoolSnapshot{}
}

func TestPoolFIFOWakeAndSingleOwner(t *testing.T) {
	cfg := domain.Config{MaxOpen: 1, MaxIdle: 1, AcquireTimeoutMS: 3000, MaxWaiters: 3, Version: 1}
	p, _ := newTestPool(t, cfg)
	ctx := context.Background()
	first, err := p.Acquire(ctx, AcquireOptions{RequestID: "hold"})
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		index int
		lease domain.Lease
		err   error
	}
	results := make(chan result, 3)
	for i := 0; i < 3; i++ {
		i := i
		go func() {
			lease, err := p.Acquire(ctx, AcquireOptions{RequestID: "wait-" + string(rune('a'+i))})
			results <- result{index: i, lease: lease, err: err}
		}()
		waitFor(t, p, func(s domain.PoolSnapshot) bool { return s.Stats.Waiting == i+1 })
	}

	if _, err := p.Release(ctx, first.ConnectionID, "release-hold"); err != nil {
		t.Fatal(err)
	}
	var got []result
	for len(got) < 1 {
		select {
		case r := <-results:
			got = append(got, r)
		case <-time.After(2 * time.Second):
			t.Fatal("first waiter was not woken")
		}
	}
	if got[0].index != 0 || got[0].err != nil {
		t.Fatalf("FIFO head should receive connection first: %+v", got[0])
	}
	if got[0].lease.ConnectionID != first.ConnectionID {
		t.Fatalf("expected released connection %s, got %s", first.ConnectionID, got[0].lease.ConnectionID)
	}
	if _, err := p.Release(ctx, got[0].lease.ConnectionID, "release-w0"); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-results:
		got = append(got, r)
	case <-time.After(2 * time.Second):
		t.Fatal("second waiter was not woken")
	}
	if got[1].index != 1 || got[1].err != nil {
		t.Fatalf("FIFO second waiter mismatch: %+v", got[1])
	}
	if _, err := p.Release(ctx, got[1].lease.ConnectionID, "release-w1"); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-results:
		got = append(got, r)
	case <-time.After(2 * time.Second):
		t.Fatal("third waiter was not woken")
	}
	if got[2].index != 2 || got[2].err != nil {
		t.Fatalf("FIFO third waiter mismatch: %+v", got[2])
	}
	if _, err := p.Release(ctx, got[2].lease.ConnectionID, "release-w2"); err != nil {
		t.Fatal(err)
	}

	snap := p.Snapshot()
	if snap.Stats.Waiting != 0 || snap.Stats.InUse != 0 || snap.Stats.Idle != 1 {
		t.Fatalf("unexpected final pool state: %+v", snap.Stats)
	}
	wakeEvents := 0
	for _, event := range snap.Events {
		if event.Type == "acquire_wake" {
			wakeEvents++
		}
	}
	if wakeEvents < 3 {
		t.Fatalf("expected three observable wake events, got %d", wakeEvents)
	}
}

func TestAcquireTimeoutRemovesWaiter(t *testing.T) {
	cfg := domain.Config{MaxOpen: 1, MaxIdle: 1, AcquireTimeoutMS: 3000, MaxWaiters: 2, Version: 1}
	p, _ := newTestPool(t, cfg)
	ctx := context.Background()
	held, err := p.Acquire(ctx, AcquireOptions{RequestID: "hold"})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = p.Acquire(ctx, AcquireOptions{RequestID: "timeout", Timeout: 80 * time.Millisecond})
	var be *domain.BusinessError
	if !errors.As(err, &be) || be.Code != domain.CodeAcquireTimeout {
		t.Fatalf("expected E_ACQUIRE_TIMEOUT, got %v", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatalf("timeout took too long: %v", time.Since(start))
	}
	waitFor(t, p, func(s domain.PoolSnapshot) bool { return s.Stats.Waiting == 0 })
	released, err := p.Release(ctx, held.ConnectionID, "release-hold")
	if err != nil {
		t.Fatal(err)
	}
	if released.Disposition != domain.DispositionIdle {
		t.Fatalf("timed-out waiter must not be woken later; disposition=%s", released.Disposition)
	}
	next, err := p.Acquire(ctx, AcquireOptions{RequestID: "after-timeout", Timeout: time.Second})
	if err != nil || next.ConnectionID != held.ConnectionID {
		t.Fatalf("connection should remain reusable after timeout: lease=%+v err=%v", next, err)
	}
	_, _ = p.Release(ctx, next.ConnectionID, "release-after")
}

func TestQueueOverflowLeavesNoEntry(t *testing.T) {
	cfg := domain.Config{MaxOpen: 1, MaxIdle: 1, AcquireTimeoutMS: 3000, MaxWaiters: 1, Version: 1}
	p, _ := newTestPool(t, cfg)
	ctx := context.Background()
	held, err := p.Acquire(ctx, AcquireOptions{RequestID: "hold"})
	if err != nil {
		t.Fatal(err)
	}
	waitDone := make(chan error, 1)
	go func() {
		lease, err := p.Acquire(ctx, AcquireOptions{RequestID: "queued", Timeout: 2 * time.Second})
		if err == nil {
			_, _ = p.Release(context.Background(), lease.ConnectionID, "queued-release")
		}
		waitDone <- err
	}()
	waitFor(t, p, func(s domain.PoolSnapshot) bool { return s.Stats.Waiting == 1 })
	_, err = p.Acquire(ctx, AcquireOptions{RequestID: "overflow", Timeout: 2 * time.Second})
	var be *domain.BusinessError
	if !errors.As(err, &be) || be.Code != domain.CodePoolExhausted {
		t.Fatalf("expected E_POOL_EXHAUSTED, got %v", err)
	}
	snap := p.Snapshot()
	if snap.Stats.Waiting != 1 {
		t.Fatalf("overflow request left a residual waiter: %+v", snap.Stats)
	}
	if _, err := p.Release(ctx, held.ConnectionID, "release-hold"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("queued waiter should have been woken: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queued waiter did not receive released connection")
	}
	waitFor(t, p, func(s domain.PoolSnapshot) bool { return s.Stats.Waiting == 0 })
}

func TestDialFailureLeavesNoSlot(t *testing.T) {
	cfg := domain.Config{MaxOpen: 1, MaxIdle: 1, AcquireTimeoutMS: 500, MaxWaiters: 1, Version: 1}
	p, _ := newTestPool(t, cfg)
	_, err := p.Acquire(context.Background(), AcquireOptions{RequestID: "dial-fail", ForceDialFailure: true})
	var be *domain.BusinessError
	if !errors.As(err, &be) || be.Code != domain.CodeDialFailed {
		t.Fatalf("expected E_DIAL_FAILED, got %v", err)
	}
	snap := p.Snapshot()
	if snap.Stats.Active != 0 || snap.Stats.Dialing != 0 || snap.Stats.Waiting != 0 {
		t.Fatalf("dial failure leaked capacity: %+v", snap.Stats)
	}
	lease, err := p.Acquire(context.Background(), AcquireOptions{RequestID: "dial-good", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = p.Release(context.Background(), lease.ConnectionID, "release-good")
}

func TestForceResetDetectionAndReplacement(t *testing.T) {
	cfg := domain.Config{MaxOpen: 1, MaxIdle: 1, AcquireTimeoutMS: 1000, MaxWaiters: 1, Version: 1}
	p, st := newTestPool(t, cfg)
	ctx := context.Background()
	old, err := p.Acquire(ctx, AcquireOptions{RequestID: "old"})
	if err != nil {
		t.Fatal(err)
	}
	closed, err := p.ForceClose(ctx, old.ConnectionID, "loopback_test_reset")
	if err != nil || !closed.WasInUse {
		t.Fatalf("force close failed: %+v %v", closed, err)
	}
	_, err = p.Query(ctx, old.ConnectionID, "SELECT 1", QueryOptions{})
	var be *domain.BusinessError
	if !errors.As(err, &be) || be.Code != domain.CodeConnReset {
		t.Fatalf("expected E_CONN_RESET from invalidated lease, got %v", err)
	}
	released, err := p.Release(ctx, old.ConnectionID, "release-old")
	if err != nil || released.Disposition != domain.DispositionInvalid {
		t.Fatalf("invalid lease release should be idempotently invalidated: %+v %v", released, err)
	}
	replacement, err := p.Acquire(ctx, AcquireOptions{RequestID: "replacement", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if replacement.ConnectionID == old.ConnectionID {
		t.Fatal("force-closed connection was reused")
	}
	rec, err := st.GetConnection(ctx, replacement.ConnectionID)
	if err != nil {
		t.Fatal(err)
	}
	if rec.ReplacedConnID != old.ConnectionID || rec.ReplacementReason != "loopback_test_reset" {
		t.Fatalf("replacement reason not persisted: %+v", rec)
	}
	oldRec, err := st.GetConnection(ctx, old.ConnectionID)
	if err != nil {
		t.Fatal(err)
	}
	if oldRec.State != domain.StateInvalid || oldRec.ReplacedBy != replacement.ConnectionID {
		t.Fatalf("invalid connection replacement link not persisted: %+v", oldRec)
	}
	var snapshotOld *domain.ConnectionRecord
	snapshot := p.Snapshot()
	for i := range snapshot.Connections {
		rec := &snapshot.Connections[i]
		if rec.ConnectionID == old.ConnectionID {
			snapshotOld = rec
			break
		}
	}
	if snapshotOld == nil || snapshotOld.ReplacedBy != replacement.ConnectionID ||
		snapshotOld.ReplacementReason != "loopback_test_reset" {
		t.Fatalf("replacement link missing from atomic pool snapshot: %+v", snapshotOld)
	}
	_, _ = p.Release(ctx, replacement.ConnectionID, "release-replacement")
}

func TestIdleTimeExpiryDoesNotReuseConnection(t *testing.T) {
	cfg := domain.Config{MaxOpen: 1, MaxIdle: 1, MaxIdleTimeMS: 50, AcquireTimeoutMS: 1000, MaxWaiters: 1, Version: 1}
	p, _ := newTestPool(t, cfg)
	ctx := context.Background()
	first, err := p.Acquire(ctx, AcquireOptions{RequestID: "first"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Release(ctx, first.ConnectionID, "first-release"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond)
	second, err := p.Acquire(ctx, AcquireOptions{RequestID: "second", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if second.ConnectionID == first.ConnectionID {
		t.Fatal("idle-expired connection was reused")
	}
	snap := p.Snapshot()
	foundClosed := false
	for _, rec := range snap.Connections {
		if rec.ConnectionID == first.ConnectionID && rec.State == domain.StateClosed && rec.CloseReason == "max_idle_time" {
			foundClosed = true
		}
	}
	if !foundClosed {
		t.Fatalf("expired connection was not recorded closed: %+v", snap.Connections)
	}
	_, _ = p.Release(ctx, second.ConnectionID, "second-release")
}

func TestConcurrentOwnershipNeverDuplicates(t *testing.T) {
	cfg := domain.Config{MaxOpen: 2, MaxIdle: 2, AcquireTimeoutMS: 3000, MaxWaiters: 8, Version: 1}
	p, _ := newTestPool(t, cfg)
	var owners sync.Map
	var duplicate atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, err := p.Acquire(context.Background(), AcquireOptions{
				RequestID: "owner-" + string(rune('a'+i)),
				Timeout:   2 * time.Second,
			})
			if err != nil {
				return
			}
			if _, loaded := owners.LoadOrStore(lease.ConnectionID, i); loaded {
				duplicate.Add(1)
			}
			time.Sleep(2 * time.Millisecond)
			owners.Delete(lease.ConnectionID)
			_, _ = p.Release(context.Background(), lease.ConnectionID, "release-owner")
		}()
	}
	wg.Wait()
	if duplicate.Load() != 0 {
		t.Fatalf("%d duplicate concurrent ownership observations", duplicate.Load())
	}
}
