package pool

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"local/sqlitepool/internal/dbconn"
	"local/sqlitepool/internal/domain"
	"local/sqlitepool/internal/store"
)

const eventLimit = 256

type AcquireOptions struct {
	Timeout          time.Duration
	ForceDialFailure bool
	RequestID        string
}

type QueryOptions struct {
	Timeout     time.Duration
	InjectDelay time.Duration
	RequestID   string
}

type ForceCloseResult struct {
	ConnectionID string    `json:"connection_id"`
	Reason       string    `json:"reason"`
	ClosedAt     time.Time `json:"closed_at"`
	WasInUse     bool      `json:"was_in_use"`
}

type Pool struct {
	opener dbconn.Opener
	store  *store.Store
	cfg    domain.Config

	cmdCh chan any
	done  chan struct{}

	snapshot atomic.Pointer[domain.PoolSnapshot]
	connSeq  atomic.Int64
	waitSeq  atomic.Int64
	version  atomic.Int64

	closeOnce sync.Once
	wg        sync.WaitGroup
}

type managedConn struct {
	rec       domain.ConnectionRecord
	raw       dbconn.Conn
	idleSince time.Time
	closed    atomic.Bool
	queryMu   sync.Mutex
}

type waiter struct {
	id        string
	requestID string
	deadline  time.Time
	arrived   time.Time
	queued    bool
	finished  bool
	done      chan struct{}
	reply     chan acquireOutcome
}

type replacementCandidate struct {
	oldID  string
	reason string
}

type dialCall struct {
	id          string
	waiter      *waiter
	forceFail   bool
	started     time.Time
	replacement *replacementCandidate
}

type acquireOutcome struct {
	lease domain.Lease
	err   error
}

type releaseOutcome struct {
	result domain.ReleaseResult
	err    error
}

type handleOutcome struct {
	conn *managedConn
	rec  domain.ConnectionRecord
	err  error
}

type poolState struct {
	cfg      domain.Config
	idle     []*managedConn
	inUse    map[string]*managedConn
	known    map[string]*domain.ConnectionRecord
	waiters  []*waiter
	dials    map[string]*dialCall
	replace  []replacementCandidate
	events   []domain.Event
	metrics  domain.Metrics
	eventSeq int64
}

type acquireCmd struct {
	ctx   context.Context
	opts  AcquireOptions
	reply chan acquireOutcome
}

type releaseCmd struct {
	ctx       context.Context
	connID    string
	requestID string
	reply     chan releaseOutcome
}

type getHandleCmd struct {
	connID string
	reply  chan handleOutcome
}

type touchCmd struct {
	connID string
	reply  chan error
}

type forceCloseCmd struct {
	ctx    context.Context
	connID string
	reason string
	reply  chan forceCloseOutcome
}

type forceCloseOutcome struct {
	result ForceCloseResult
	err    error
}

type reconfigureCmd struct {
	ctx   context.Context
	cfg   domain.Config
	reply chan reconfigureOutcome
}

type reconfigureOutcome struct {
	cfg domain.Config
	err error
}

type waiterExpiredCmd struct {
	waiter *waiter
	reason string
}

type dialResultCmd struct {
	call *dialCall
	conn *managedConn
	err  error
}

type shutdownCmd struct{ reply chan struct{} }

type sweepCmd struct{}

func New(ctx context.Context, cfg domain.Config, st *store.Store, opener dbconn.Opener) (*Pool, error) {
	if err := cfg.Validate(); err != nil {
		return nil, domain.WrapError(domain.CodeConfigInvalid, "invalid pool configuration", map[string]any{"config": cfg}, err)
	}
	p := &Pool{
		opener: opener,
		store:  st,
		cfg:    cfg,
		cmdCh:  make(chan any, 1024),
		done:   make(chan struct{}),
	}
	p.version.Store(1)
	state, err := p.buildInitialState(ctx)
	if err != nil {
		return nil, err
	}
	p.publish(state)
	p.wg.Add(1)
	go p.actor(state)
	p.wg.Add(1)
	go p.janitor()
	return p, nil
}

func (p *Pool) buildInitialState(ctx context.Context) (*poolState, error) {
	state := &poolState{
		cfg:     p.cfg,
		inUse:   make(map[string]*managedConn),
		known:   make(map[string]*domain.ConnectionRecord),
		dials:   make(map[string]*dialCall),
		metrics: domain.Metrics{},
	}
	all, err := p.store.LoadAllConnections(ctx)
	if err != nil {
		return nil, err
	}
	for _, rec := range all {
		cp := rec
		state.known[rec.ConnectionID] = &cp
	}
	available, err := p.store.LoadAvailableConnections(ctx)
	if err != nil {
		return nil, err
	}
	// FIFO order itself is not persisted. This ordering is only a deterministic
	// reconstruction heuristic, and recovery reports state that explicitly.
	now := time.Now()
	for _, rec := range available {
		if len(state.idle) >= state.cfg.MaxIdle {
			_ = p.store.MarkConnectionState(ctx, rec.ConnectionID, domain.StateClosed, "recovery_max_idle_exceeded")
			cp := rec
			cp.State = domain.StateClosed
			cp.CloseReason = "recovery_max_idle_exceeded"
			state.known[rec.ConnectionID] = &cp
			continue
		}
		if state.cfg.MaxLifetime() > 0 && now.Sub(rec.CreatedAt) >= state.cfg.MaxLifetime() {
			_ = p.store.MarkConnectionState(ctx, rec.ConnectionID, domain.StateClosed, "recovery_lifetime_expired")
			cp := rec
			cp.State = domain.StateClosed
			cp.CloseReason = "recovery_lifetime_expired"
			state.known[rec.ConnectionID] = &cp
			continue
		}
		raw, openErr := p.opener.Open(ctx)
		if openErr != nil {
			_ = p.store.MarkConnectionState(ctx, rec.ConnectionID, domain.StateClosed, "recovery_dial_failed")
			cp := rec
			cp.State = domain.StateClosed
			cp.CloseReason = "recovery_dial_failed"
			state.known[rec.ConnectionID] = &cp
			continue
		}
		cp := rec
		cp.State = domain.StateAvailable
		mc := &managedConn{rec: cp, raw: raw, idleSince: rec.LastUsedAt}
		state.idle = append(state.idle, mc)
		state.known[rec.ConnectionID] = &cp
	}
	return state, nil
}

func (p *Pool) actor(state *poolState) {
	defer p.wg.Done()
	for command := range p.cmdCh {
		switch cmd := command.(type) {
		case acquireCmd:
			p.handleAcquire(state, cmd)
		case releaseCmd:
			p.handleRelease(state, cmd)
		case getHandleCmd:
			p.handleGetHandle(state, cmd)
		case touchCmd:
			p.handleTouch(state, cmd)
		case forceCloseCmd:
			p.handleForceClose(state, cmd)
		case reconfigureCmd:
			p.handleReconfigure(state, cmd)
		case waiterExpiredCmd:
			p.handleWaiterExpired(state, cmd)
		case dialResultCmd:
			p.handleDialResult(state, cmd)
		case shutdownCmd:
			p.handleShutdown(state)
			close(cmd.reply)
			return
		case sweepCmd:
			if p.cleanupExpiredIdle(state, time.Now()) {
				p.ensureDials(state)
			}
		}
	}
}

func (p *Pool) janitor() {
	defer p.wg.Done()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if !p.post(sweepCmd{}) {
				return
			}
		case <-p.done:
			return
		}
	}
}

func (p *Pool) Acquire(ctx context.Context, opts AcquireOptions) (domain.Lease, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	reply := make(chan acquireOutcome, 1)
	select {
	case p.cmdCh <- acquireCmd{ctx: ctx, opts: opts, reply: reply}:
	case <-p.done:
		return domain.Lease{}, domain.NewError(domain.CodeShutdown, "pool is shut down", nil)
	case <-ctx.Done():
		return domain.Lease{}, domain.WrapError(domain.CodeRequestCancelled, "request cancelled before reaching pool actor", nil, ctx.Err())
	}
	select {
	case out := <-reply:
		return out.lease, out.err
	case <-p.done:
		return domain.Lease{}, domain.NewError(domain.CodeShutdown, "pool is shut down", nil)
	}
}

func (p *Pool) Release(ctx context.Context, connID, requestID string) (domain.ReleaseResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	reply := make(chan releaseOutcome, 1)
	select {
	case p.cmdCh <- releaseCmd{ctx: ctx, connID: connID, requestID: requestID, reply: reply}:
	case <-p.done:
		return domain.ReleaseResult{}, domain.NewError(domain.CodeShutdown, "pool is shut down", nil)
	case <-ctx.Done():
		return domain.ReleaseResult{}, domain.WrapError(domain.CodeRequestCancelled, "request cancelled before reaching pool actor", nil, ctx.Err())
	}
	select {
	case out := <-reply:
		return out.result, out.err
	case <-p.done:
		return domain.ReleaseResult{}, domain.NewError(domain.CodeShutdown, "pool is shut down", nil)
	}
}

func (p *Pool) Query(ctx context.Context, connID, query string, opts QueryOptions) (domain.QueryResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	reply := make(chan handleOutcome, 1)
	select {
	case p.cmdCh <- getHandleCmd{connID: connID, reply: reply}:
	case <-p.done:
		return domain.QueryResult{}, domain.NewError(domain.CodeShutdown, "pool is shut down", nil)
	case <-ctx.Done():
		return domain.QueryResult{}, domain.WrapError(domain.CodeRequestCancelled, "request cancelled before reaching pool actor", nil, ctx.Err())
	}
	var out handleOutcome
	select {
	case out = <-reply:
	case <-p.done:
		return domain.QueryResult{}, domain.NewError(domain.CodeShutdown, "pool is shut down", nil)
	}
	if out.err != nil {
		return domain.QueryResult{}, out.err
	}

	queryCtx := ctx
	var cancel context.CancelFunc
	if opts.Timeout > 0 {
		queryCtx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}
	out.conn.queryMu.Lock()
	defer out.conn.queryMu.Unlock()

	if opts.InjectDelay > 0 {
		timer := time.NewTimer(opts.InjectDelay)
		defer timer.Stop()
		select {
		case <-timer.C:
			return domain.QueryResult{}, domain.NewError(domain.CodeQueryTimeout,
				"fault-injected query execution timeout", map[string]any{
					"connection_id": connID,
					"delay_ms":      opts.InjectDelay.Milliseconds(),
					"timeout_ms":    opts.Timeout.Milliseconds(),
					"simulated":     true,
				})
		case <-queryCtx.Done():
			return domain.QueryResult{}, domain.WrapError(domain.CodeQueryTimeout,
				"fault-injected query execution timeout", map[string]any{
					"connection_id": connID,
					"delay_ms":      opts.InjectDelay.Milliseconds(),
					"timeout_ms":    opts.Timeout.Milliseconds(),
					"simulated":     true,
				}, queryCtx.Err())
		}
	}

	if out.conn.closed.Load() {
		return domain.QueryResult{}, domain.NewError(domain.CodeConnReset, "underlying connection was force-closed",
			map[string]any{"connection_id": connID})
	}
	result, err := out.conn.raw.Query(queryCtx, query)
	if err == nil {
		_ = p.touch(context.Background(), connID)
		return result, nil
	}
	if errors.Is(err, dbconn.ErrConnClosed) {
		_, _ = p.ForceClose(ctx, connID, "driver_detected_connection_reset")
		return domain.QueryResult{}, domain.WrapError(domain.CodeConnReset, "underlying connection was reset",
			map[string]any{"connection_id": connID, "detected_by": "query_driver"}, err)
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return domain.QueryResult{}, domain.WrapError(domain.CodeQueryTimeout, "query execution exceeded its deadline",
			map[string]any{"connection_id": connID}, err)
	}
	return domain.QueryResult{}, domain.WrapError(domain.CodeQueryFailed, "query execution failed",
		map[string]any{"connection_id": connID, "query": query}, err)
}

func (p *Pool) touch(ctx context.Context, connID string) error {
	reply := make(chan error, 1)
	select {
	case p.cmdCh <- touchCmd{connID: connID, reply: reply}:
	case <-p.done:
		return domain.NewError(domain.CodeShutdown, "pool is shut down", nil)
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-reply:
		return err
	case <-p.done:
		return domain.NewError(domain.CodeShutdown, "pool is shut down", nil)
	}
}

func (p *Pool) ForceClose(ctx context.Context, connID, reason string) (ForceCloseResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	reply := make(chan forceCloseOutcome, 1)
	select {
	case p.cmdCh <- forceCloseCmd{ctx: ctx, connID: connID, reason: reason, reply: reply}:
	case <-p.done:
		return ForceCloseResult{}, domain.NewError(domain.CodeShutdown, "pool is shut down", nil)
	case <-ctx.Done():
		return ForceCloseResult{}, domain.WrapError(domain.CodeRequestCancelled, "request cancelled before reaching pool actor", nil, ctx.Err())
	}
	select {
	case out := <-reply:
		return out.result, out.err
	case <-p.done:
		return ForceCloseResult{}, domain.NewError(domain.CodeShutdown, "pool is shut down", nil)
	}
}

func (p *Pool) ProbeDial(ctx context.Context, forceFailure bool, reason string) error {
	if forceFailure {
		return domain.NewError(domain.CodeDialFailed, "fault-injected connection establishment failure",
			map[string]any{"reason": reason, "simulated": true})
	}
	started := time.Now()
	raw, err := p.opener.Open(ctx)
	if err != nil {
		return domain.WrapError(domain.CodeDialFailed, "connection establishment failed",
			map[string]any{"elapsed_ms": time.Since(started).Milliseconds()}, err)
	}
	_ = raw.Close()
	return nil
}

func (p *Pool) Reconfigure(ctx context.Context, cfg domain.Config) (domain.Config, error) {
	if err := cfg.Validate(); err != nil {
		return domain.Config{}, domain.WrapError(domain.CodeConfigInvalid, "invalid pool configuration",
			map[string]any{"config": cfg}, err)
	}
	reply := make(chan reconfigureOutcome, 1)
	select {
	case p.cmdCh <- reconfigureCmd{ctx: ctx, cfg: cfg, reply: reply}:
	case <-p.done:
		return domain.Config{}, domain.NewError(domain.CodeShutdown, "pool is shut down", nil)
	case <-ctx.Done():
		return domain.Config{}, domain.WrapError(domain.CodeRequestCancelled, "request cancelled before reaching pool actor", nil, ctx.Err())
	}
	select {
	case out := <-reply:
		return out.cfg, out.err
	case <-p.done:
		return domain.Config{}, domain.NewError(domain.CodeShutdown, "pool is shut down", nil)
	}
}

func (p *Pool) Shutdown(ctx context.Context) error {
	var err error
	p.closeOnce.Do(func() {
		reply := make(chan struct{})
		select {
		case p.cmdCh <- shutdownCmd{reply: reply}:
			select {
			case <-reply:
			case <-ctx.Done():
				err = ctx.Err()
			}
		case <-p.done:
		case <-ctx.Done():
			err = ctx.Err()
		}
	})
	return err
}

func (p *Pool) Snapshot() domain.PoolSnapshot {
	snap := p.snapshot.Load()
	if snap == nil {
		return domain.PoolSnapshot{}
	}
	clone := *snap
	clone.Stats.IdleOrder = append([]string(nil), snap.Stats.IdleOrder...)
	clone.Connections = append([]domain.ConnectionRecord(nil), snap.Connections...)
	clone.Events = append([]domain.Event(nil), snap.Events...)
	return clone
}

func (p *Pool) Config() domain.Config {
	snap := p.snapshot.Load()
	if snap == nil {
		return p.cfg
	}
	return snap.Stats.Config
}

func (p *Pool) publish(state *poolState) {
	version := p.version.Add(1)
	connections := make([]domain.ConnectionRecord, 0, len(state.known))
	for _, rec := range state.known {
		connections = append(connections, *rec)
	}
	sort.Slice(connections, func(i, j int) bool {
		if connections[i].CreatedAt.Equal(connections[j].CreatedAt) {
			return connections[i].ConnectionID < connections[j].ConnectionID
		}
		return connections[i].CreatedAt.Before(connections[j].CreatedAt)
	})
	idleOrder := make([]string, 0, len(state.idle))
	for _, conn := range state.idle {
		idleOrder = append(idleOrder, conn.rec.ConnectionID)
	}
	if idleOrder == nil {
		idleOrder = []string{}
	}
	start := 0
	if len(state.events) > 60 {
		start = len(state.events) - 60
	}
	events := append([]domain.Event(nil), state.events[start:]...)
	snap := &domain.PoolSnapshot{
		Stats: domain.PoolStats{
			SnapshotVersion:  version,
			Config:           state.cfg,
			Active:           len(state.inUse) + len(state.idle) + len(state.dials),
			InUse:            len(state.inUse),
			Idle:             len(state.idle),
			Dialing:          len(state.dials),
			Waiting:          len(state.waiters),
			WaitingCapacity:  state.cfg.MaxWaiters,
			KnownConnections: len(state.known),
			IdleOrder:        idleOrder,
			Metrics:          state.metrics,
		},
		Connections: connections,
		Events:      events,
	}
	p.snapshot.Store(snap)
}

func (p *Pool) event(state *poolState, eventType, connID, waiterID string, detail map[string]any) {
	state.eventSeq++
	state.events = append(state.events, domain.Event{
		Seq:          state.eventSeq,
		Type:         eventType,
		At:           time.Now(),
		ConnectionID: connID,
		WaiterID:     waiterID,
		Detail:       detail,
	})
	if len(state.events) > eventLimit {
		state.events = append([]domain.Event(nil), state.events[len(state.events)-eventLimit:]...)
	}
}

func (p *Pool) nextConnID() string {
	n := p.connSeq.Add(1)
	buf := make([]byte, 4)
	_, _ = rand.Read(buf)
	return fmt.Sprintf("conn-%d-%d-%s", time.Now().UnixNano(), n, hex.EncodeToString(buf))
}

func (p *Pool) nextWaiterID() string {
	n := p.waitSeq.Add(1)
	return fmt.Sprintf("waiter-%d-%d", time.Now().UnixNano(), n)
}

func (p *Pool) total(state *poolState) int {
	return len(state.inUse) + len(state.idle) + len(state.dials)
}

func (p *Pool) idleExpired(state *poolState, conn *managedConn, now time.Time) string {
	if connState := conn.rec.State; connState == domain.StateInvalid || connState == domain.StateClosed {
		return "already_unusable"
	}
	if limit := state.cfg.MaxLifetime(); limit > 0 && now.Sub(conn.rec.CreatedAt) >= limit {
		return "max_lifetime"
	}
	if limit := state.cfg.MaxIdleTime(); limit > 0 && now.Sub(conn.rec.LastUsedAt) >= limit {
		return "max_idle_time"
	}
	return ""
}

func (p *Pool) releaseExpired(state *poolState, conn *managedConn, now time.Time) string {
	if conn.closed.Load() {
		return "connection_closed"
	}
	if limit := state.cfg.MaxLifetime(); limit > 0 && now.Sub(conn.rec.CreatedAt) >= limit {
		return "max_lifetime_on_release"
	}
	if limit := state.cfg.MaxIdleTime(); limit > 0 && now.Sub(conn.rec.LastUsedAt) >= limit {
		return "max_idle_time_on_release"
	}
	return ""
}
