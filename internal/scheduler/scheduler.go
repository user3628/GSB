// Package scheduler coordinates upstream DNS attempts. It enforces a global
// concurrency cap, a per-authoritative-server cap, a bounded priority queue and
// a per-round deadline. Identical in-flight keys are coalesced.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"example.com/gsb/dnscache/internal/errs"
)

type Config struct {
	GlobalLimit    int
	PerServerLimit int
	QueueLimit     int
	RoundBudget    time.Duration
	// DetachAfter is the deterministic long-tail threshold. A duplicate waits
	// for an existing leader unless the leader has run at least this long, the
	// caller has enough remaining round budget, and an extra concurrency slot is
	// currently available. In that case the duplicate is launched independently.
	DetachAfter time.Duration
}

type Request struct {
	Key      string
	Server   string
	Priority int // lower value is served first
}

type Func func(context.Context) (any, error)

type Stats struct {
	GlobalActive int            `json:"global_active"`
	ServerActive map[string]int `json:"server_active"`
	QueueDepth   int            `json:"queue_depth"`
	Joined       uint64         `json:"joined"`
	Detached     uint64         `json:"detached"`
	QueueFull    uint64         `json:"queue_full"`
	TimedOut     uint64         `json:"round_timed_out"`
	Completed    uint64         `json:"completed"`
	Failed       uint64         `json:"failed"`
}

type Scheduler struct {
	cfg Config

	mu             sync.Mutex
	queue          []*task
	activeGlobal   int
	activeByServer map[string]int
	flights        map[string]*flight
	seq            uint64

	joined    atomic.Uint64
	detached  atomic.Uint64
	queueFull atomic.Uint64
	timedOut  atomic.Uint64
	completed atomic.Uint64
	failed    atomic.Uint64
}

type task struct {
	seq       uint64
	req       Request
	deadline  time.Time
	fn        Func
	ctx       context.Context
	cancel    context.CancelFunc
	started   bool
	queued    bool
	leader    *flight
	result    chan outcome
	startedAt time.Time
}

type outcome struct {
	value any
	err   error
}

type flight struct {
	key       string
	leader    *task
	done      chan struct{}
	result    any
	err       error
	finished  bool
	startedAt time.Time
}

func New(cfg Config) *Scheduler {
	if cfg.GlobalLimit <= 0 {
		cfg.GlobalLimit = 1
	}
	if cfg.PerServerLimit <= 0 {
		cfg.PerServerLimit = 1
	}
	if cfg.QueueLimit < 0 {
		cfg.QueueLimit = 0
	}
	if cfg.RoundBudget <= 0 {
		cfg.RoundBudget = 2 * time.Second
	}
	if cfg.DetachAfter <= 0 {
		cfg.DetachAfter = cfg.RoundBudget / 2
	}
	return &Scheduler{
		cfg:            cfg,
		activeByServer: make(map[string]int),
		flights:        make(map[string]*flight),
	}
}

func (s *Scheduler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	servers := make(map[string]int, len(s.activeByServer))
	for k, v := range s.activeByServer {
		servers[k] = v
	}
	return Stats{
		GlobalActive: s.activeGlobal,
		ServerActive: servers,
		QueueDepth:   len(s.queue),
		Joined:       s.joined.Load(),
		Detached:     s.detached.Load(),
		QueueFull:    s.queueFull.Load(),
		TimedOut:     s.timedOut.Load(),
		Completed:    s.completed.Load(),
		Failed:       s.failed.Load(),
	}
}

func (s *Scheduler) Do(parent context.Context, req Request, fn Func) (any, error) {
	now := time.Now()
	deadline := now.Add(s.cfg.RoundBudget)
	if d, ok := parent.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if !deadline.After(now) {
		return nil, errs.New(errs.ERoundBudgetExceeded, "round budget is already exhausted")
	}
	taskCtx, cancel := context.WithDeadline(parent, deadline)

	s.mu.Lock()
	if f, ok := s.flights[req.Key]; ok && !f.finished {
		if s.shouldDetachLocked(f, req.Server, deadline, now) {
			s.detached.Add(1)
			t := s.newTaskLocked(req, fn, taskCtx, cancel, deadline, now)
			if err := s.admitLocked(t); err != nil {
				s.mu.Unlock()
				// A duplicate can still be useful by reusing the leader.
				value, waitErr := s.waitJoined(f, parent, deadline)
				cancel()
				return value, waitErr
			}
			s.mu.Unlock()
			value, waitErr := s.waitDetached(t, parent, deadline)
			cancel()
			return value, waitErr
		}
		s.joined.Add(1)
		s.mu.Unlock()
		value, waitErr := s.waitJoined(f, parent, deadline)
		cancel()
		return value, waitErr
	}

	f := &flight{key: req.Key, done: make(chan struct{})}
	t := s.newTaskLocked(req, fn, taskCtx, cancel, deadline, now)
	t.leader = f
	f.leader = t
	s.flights[req.Key] = f
	if err := s.admitLocked(t); err != nil {
		delete(s.flights, req.Key)
		s.mu.Unlock()
		cancel()
		return nil, err
	}
	s.mu.Unlock()
	value, waitErr := s.waitLeader(f, parent, deadline)
	cancel()
	return value, waitErr
}

func (s *Scheduler) newTaskLocked(req Request, fn Func, ctx context.Context, cancel context.CancelFunc, deadline, now time.Time) *task {
	s.seq++
	return &task{
		seq: s.seq, req: req, deadline: deadline, fn: fn,
		ctx: ctx, cancel: cancel, result: make(chan outcome, 1),
	}
}

func (s *Scheduler) shouldDetachLocked(f *flight, server string, ownDeadline, now time.Time) bool {
	if f.startedAt.IsZero() || now.Sub(f.startedAt) < s.cfg.DetachAfter {
		return false
	}
	remaining := ownDeadline.Sub(now)
	if remaining < s.cfg.DetachAfter {
		return false
	}
	return s.canStartLocked(server)
}

func (s *Scheduler) admitLocked(t *task) error {
	if !s.canStartLocked(t.req.Server) && len(s.queue) >= s.cfg.QueueLimit {
		s.queueFull.Add(1)
		return errs.New(errs.EUpstreamQueueFull, "upstream wait queue is full (%d entries)", len(s.queue))
	}
	t.queued = true
	s.queue = append(s.queue, t)
	s.dispatchLocked()
	return nil
}

func (s *Scheduler) canStartLocked(server string) bool {
	return s.activeGlobal < s.cfg.GlobalLimit && s.activeByServer[server] < s.cfg.PerServerLimit
}

func (s *Scheduler) dispatchLocked() {
	for {
		idx := -1
		for i, t := range s.queue {
			if !s.canStartLocked(t.req.Server) {
				continue
			}
			if idx == -1 || t.req.Priority < s.queue[idx].req.Priority ||
				(t.req.Priority == s.queue[idx].req.Priority && t.seq < s.queue[idx].seq) {
				idx = i
			}
		}
		if idx == -1 {
			return
		}
		t := s.queue[idx]
		s.queue = append(s.queue[:idx], s.queue[idx+1:]...)
		t.queued = false
		t.started = true
		t.startedAt = time.Now()
		s.activeGlobal++
		s.activeByServer[t.req.Server]++
		if t.leader != nil {
			t.leader.startedAt = t.startedAt
		}
		go s.run(t)
	}
}

func (s *Scheduler) run(t *task) {
	// The task context carries the round deadline only. Per-hop upstream
	// timeouts are applied independently by the resolver, so a CNAME chain's
	// second hop still receives its full per-hop budget.
	value, err := t.fn(t.ctx)
	if err == nil && t.ctx.Err() != nil {
		if errors.Is(t.ctx.Err(), context.DeadlineExceeded) {
			err = errs.New(errs.ERoundBudgetExceeded, "upstream attempt exceeded its round deadline")
		} else {
			err = t.ctx.Err()
		}
	}
	t.cancel()

	s.mu.Lock()
	s.activeGlobal--
	s.activeByServer[t.req.Server]--
	if s.activeByServer[t.req.Server] == 0 {
		delete(s.activeByServer, t.req.Server)
	}
	if err != nil {
		s.failed.Add(1)
	} else {
		s.completed.Add(1)
	}
	if t.leader != nil {
		s.finishFlightLocked(t.leader, value, err)
	} else {
		t.result <- outcome{value: value, err: err}
		close(t.result)
	}
	s.dispatchLocked()
	s.mu.Unlock()
}

func (s *Scheduler) finishFlightLocked(f *flight, value any, err error) {
	if f.finished {
		return
	}
	f.finished = true
	f.result = value
	f.err = err
	delete(s.flights, f.key)
	close(f.done)
}

func (s *Scheduler) waitLeader(f *flight, parent context.Context, deadline time.Time) (any, error) {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-f.done:
		return f.result, f.err
	case <-parent.Done():
		// A result that became ready at the same instant wins over cancellation.
		select {
		case <-f.done:
			return f.result, f.err
		default:
		}
		s.timeoutTask(f.leader)
		return nil, s.roundError(parent)
	case <-timer.C:
		select {
		case <-f.done:
			return f.result, f.err
		default:
		}
		s.timeoutTask(f.leader)
		// Once started, the upstream function is responsible for closing its
		// socket. Give that cleanup a short bounded grace period so a real
		// E_UPSTREAM_TIMEOUT produced by the function wins over a synthetic
		// round-budget error.
		grace := time.NewTimer(100 * time.Millisecond)
		defer grace.Stop()
		select {
		case <-f.done:
			return f.result, f.err
		case <-grace.C:
			return nil, errs.New(errs.ERoundBudgetExceeded, "query round time budget exhausted")
		}
	}
}

func (s *Scheduler) waitJoined(f *flight, parent context.Context, deadline time.Time) (any, error) {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-f.done:
		return f.result, f.err
	case <-parent.Done():
		select {
		case <-f.done:
			return f.result, f.err
		default:
		}
		return nil, s.roundError(parent)
	case <-timer.C:
		select {
		case <-f.done:
			return f.result, f.err
		default:
		}
		s.timedOut.Add(1)
		return nil, errs.New(errs.ERoundBudgetExceeded, "query round time budget exhausted while joining an in-flight query")
	}
}

func (s *Scheduler) waitDetached(t *task, parent context.Context, deadline time.Time) (any, error) {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case out := <-t.result:
		return out.value, out.err
	case <-parent.Done():
		select {
		case out := <-t.result:
			return out.value, out.err
		default:
		}
		s.timeoutTask(t)
		return nil, s.roundError(parent)
	case <-timer.C:
		select {
		case out := <-t.result:
			return out.value, out.err
		default:
		}
		s.timeoutTask(t)
		grace := time.NewTimer(100 * time.Millisecond)
		defer grace.Stop()
		select {
		case out := <-t.result:
			return out.value, out.err
		case <-grace.C:
			return nil, errs.New(errs.ERoundBudgetExceeded, "query round time budget exhausted")
		}
	}
}

func (s *Scheduler) timeoutTask(t *task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.started {
		t.cancel()
		return
	}
	if t.queued {
		for i, queued := range s.queue {
			if queued == t {
				s.queue = append(s.queue[:i], s.queue[i+1:]...)
				break
			}
		}
		t.queued = false
	}
	s.timedOut.Add(1)
	err := errs.New(errs.ERoundBudgetExceeded, "queued upstream task exceeded round budget")
	if t.leader != nil {
		s.finishFlightLocked(t.leader, nil, err)
	} else {
		select {
		case t.result <- outcome{err: err}:
		default:
		}
		close(t.result)
	}
}

func (s *Scheduler) roundError(ctx context.Context) error {
	s.timedOut.Add(1)
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return errs.New(errs.ERoundBudgetExceeded, "query round time budget exhausted")
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return fmt.Errorf("query cancelled: %w", ctx.Err())
	}
	return errs.New(errs.ERoundBudgetExceeded, "query round time budget exhausted")
}
