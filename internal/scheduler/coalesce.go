package scheduler

import (
	"context"
	"sync"
	"time"

	"cdn-edge-consistency/internal/model"
)

type FlightMeta struct {
	FlightKey string
	TaskID    string
	OriginID  string
	NodeID    string
	Key       string
	Priority  int
	Heat      int64
	CostMs    int64
	Kind      TaskKind
}

type flight struct {
	done   chan struct{}
	result any
	err    error
	closed bool
}

type Coalescer struct {
	sched *Scheduler

	mu      sync.Mutex
	flights map[string]*flight
}

func NewCoalescer(sched *Scheduler) *Coalescer {
	return &Coalescer{sched: sched, flights: map[string]*flight{}}
}

// Do joins an existing in-flight origin task or schedules the first one.
// coalesced is true for waiters. A waiter timeout never cancels the shared
// origin task; it only abandons this waiter.
func (c *Coalescer) Do(ctx context.Context, meta FlightMeta, timeout time.Duration, fn func(context.Context) (any, error)) (any, bool, error) {
	f, coalesced, err := c.join(meta, fn)
	if err != nil {
		return nil, false, err
	}
	waitCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	select {
	case <-f.done:
		return f.result, coalesced, f.err
	case <-waitCtx.Done():
		if waitCtx.Err() == context.DeadlineExceeded {
			return nil, coalesced, model.WithDetails(model.NewBiz(model.CodeRevalidateTimeout, "timed out waiting for coalesced origin result"), map[string]any{
				"flight_key": meta.FlightKey,
				"timeout_ms": timeout.Milliseconds(),
				"coalesced":  coalesced,
			})
		}
		return nil, coalesced, waitCtx.Err()
	}
}

func (c *Coalescer) DoAsync(meta FlightMeta, fn func(context.Context) (any, error)) (bool, error) {
	_, coalesced, err := c.join(meta, fn)
	if err != nil {
		return false, err
	}
	return coalesced, nil
}

func (c *Coalescer) join(meta FlightMeta, fn func(context.Context) (any, error)) (*flight, bool, error) {
	c.mu.Lock()
	if existing, ok := c.flights[meta.FlightKey]; ok {
		c.mu.Unlock()
		return existing, true, nil
	}
	f := &flight{done: make(chan struct{})}
	c.flights[meta.FlightKey] = f
	c.mu.Unlock()

	task := Task{
		ID:       meta.TaskID,
		Kind:     meta.Kind,
		OriginID: meta.OriginID,
		NodeID:   meta.NodeID,
		Key:      meta.Key,
		Priority: meta.Priority,
		Heat:     meta.Heat,
		CostMs:   meta.CostMs,
		Run: func(taskCtx context.Context) error {
			result, err := fn(taskCtx)
			c.complete(meta.FlightKey, f, result, err)
			return err
		},
	}
	resultCh, err := c.sched.SubmitTask(context.Background(), task)
	if err != nil {
		c.complete(meta.FlightKey, f, nil, err)
		return nil, false, err
	}
	go func() {
		// A scheduler can reject or skip a task before Run executes. In that
		// case the result channel is the only error signal, so complete the
		// shared flight here. Successful runs are idempotently ignored because
		// Run has already closed the flight.
		if err := <-resultCh; err != nil {
			c.complete(meta.FlightKey, f, nil, err)
		}
	}()
	return f, false, nil
}

func (c *Coalescer) complete(key string, f *flight, result any, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if f.closed {
		return
	}
	f.closed = true
	f.result = result
	f.err = err
	if c.flights[key] == f {
		delete(c.flights, key)
	}
	close(f.done)
}

func (c *Coalescer) InFlight() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.flights)
}
