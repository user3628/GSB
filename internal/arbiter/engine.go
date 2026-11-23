package arbiter

import (
	"context"
	"sync"
	"time"

	"leasearbiter/internal/clock"
	"leasearbiter/internal/store"
)

// Engine is the arbitration service. One Engine owns one SQLite file; multiple
// OS processes may open the same file and rely on SQLite file locking plus the
// durable intent log for crash-safe convergence.
type Engine struct {
	st    *store.Store
	clk   clock.Clock
	sleep clock.Sleeper
	gates *gateRegistry

	cacheMu sync.RWMutex
	cache   map[string]*panelCache

	reaperStop    chan struct{}
	reaperDone    chan struct{}
	reaperRunning bool
	closeOnce     sync.Once

	now func() time.Time
}

// Config wires an engine.
type Config struct {
	Store        *store.Store
	Clock        clock.Clock
	Sleeper      clock.Sleeper
	StartReaper  bool // background expiry sweeper; tests can disable it
	ReaperPeriod time.Duration
}

// New constructs an engine. Recovery must be run separately via Recover so the
// caller can surface the fact-based recovery report.
func New(cfg Config) *Engine {
	clk := cfg.Clock
	if clk == nil {
		sys := clock.NewSystem()
		clk = sys
		if cfg.Sleeper == nil {
			cfg.Sleeper = sys
		}
	}
	if cfg.Sleeper == nil {
		cfg.Sleeper = noopSleeper{}
	}
	e := &Engine{
		st:         cfg.Store,
		clk:        clk,
		sleep:      cfg.Sleeper,
		gates:      newGateRegistry(),
		cache:      make(map[string]*panelCache),
		reaperStop: make(chan struct{}),
		reaperDone: make(chan struct{}),
		now:        clk.Now,
	}
	if cfg.StartReaper {
		period := cfg.ReaperPeriod
		if period <= 0 {
			period = 100 * time.Millisecond
		}
		e.reaperRunning = true
		go e.reaperLoop(period)
	}
	return e
}

func (e *Engine) Close() {
	e.closeOnce.Do(func() {
		close(e.reaperStop)
		if e.reaperRunning {
			<-e.reaperDone
		}
	})
}

type noopSleeper struct{}

func (noopSleeper) Sleep(ctx context.Context, d time.Duration) error {
	return ctx.Err()
}

// reaperLoop periodically sweeps expired leases. The timer is a real
// time.Timer channel; on shutdown it stops cleanly rather than leaking.
func (e *Engine) reaperLoop(period time.Duration) {
	defer close(e.reaperDone)
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-e.reaperStop:
			return
		case <-t.C:
			_ = e.SweepExpired(context.Background())
		}
	}
}

// SweepExpired transitions every granted lease whose expires_at has passed to
// the expired state and wakes any waiters on those rows. Each row is handled
// under its own gate lock, so readers and other-row writers stay unblocked.
func (e *Engine) SweepExpired(ctx context.Context) error {
	ids, err := e.st.AllRowIDs()
	if err != nil {
		return err
	}
	// Also sweep leases for rows that have a lease but no committed version
	// (an acquired row whose first commit was killed).
	leaseRows, err := e.allLeaseRows()
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		seen[id] = struct{}{}
	}
	for _, id := range leaseRows {
		if _, ok := seen[id]; !ok {
			ids = append(ids, id)
		}
	}
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		e.expireIfDue(id)
	}
	return nil
}

func (e *Engine) allLeaseRows() ([]string, error) {
	rows, err := e.st.DB.Query(`SELECT row_id FROM write_leases WHERE state=?`, string(store.LeaseGranted))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// expireIfDue performs the durable expiry transition for one row if needed.
func (e *Engine) expireIfDue(rowID string) bool {
	g := e.gates.get(rowID)
	g.mu.Lock()
	defer g.mu.Unlock()
	nowMs := store.Milli(e.now())
	l, err := store.ScanLease(e.st.DB, rowID)
	if err != nil || l.State != store.LeaseGranted || l.ExpiresAtMs > nowMs {
		return false
	}
	if err := e.persistExpiry(rowID, l.LeaseVersion, nowMs); err != nil {
		return false
	}
	e.invalidateCache(rowID)
	g.wakeAll()
	return true
}
