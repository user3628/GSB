// Package clock defines the time abstraction used by the arbitration engine.
//
// All lease and intent timestamps are produced through this interface so that
// deterministic tests (including forced-termination recovery) can control the
// observed time. The production implementation wraps time.Now().
package clock

import (
	"context"
	"sync"
	"time"
)

// Clock is the source of wall-clock time used by the engine.
type Clock interface {
	Now() time.Time
}

// Sleeper performs a real, interruptible wait. It is split from Clock because
// bounded lease waits must be cancellable via context instead of busy loops.
type Sleeper interface {
	Sleep(ctx context.Context, d time.Duration) error
}

// System is the production clock: monotonic wall time plus real interruptible
// sleeps backed by time.Timer (no spinning).
type System struct{}

func NewSystem() *System { return &System{} }

func (System) Now() time.Time { return time.Now() }

func (System) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Manual is a deterministic clock for tests. Waiters park on channels outside
// the mutex, so Advance never blocks against a sleeping goroutine.
type Manual struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*manualWaiter
}

type manualWaiter struct {
	deadline time.Time
	ch       chan struct{}
	done     bool
}

func NewManual(start time.Time) *Manual {
	return &Manual{now: start}
}

func (m *Manual) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

// Sleep registers a waiter under the lock, releases it, then parks on the
// waiter channel. Cancellation removes the waiter again. No CPU is spun.
func (m *Manual) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	m.mu.Lock()
	w := &manualWaiter{deadline: m.now.Add(d), ch: make(chan struct{})}
	m.waiters = append(m.waiters, w)
	m.mu.Unlock()

	select {
	case <-ctx.Done():
		m.mu.Lock()
		if !w.done {
			m.removeWaiter(w)
		}
		m.mu.Unlock()
		return ctx.Err()
	case <-w.ch:
		return nil
	}
}

func (m *Manual) removeWaiter(w *manualWaiter) {
	for i, cand := range m.waiters {
		if cand == w {
			m.waiters = append(m.waiters[:i], m.waiters[i+1:]...)
			return
		}
	}
}

// Advance moves the clock forward and wakes every waiter whose deadline has
// been reached. Channel closes happen after the mutex is released.
func (m *Manual) Advance(d time.Duration) {
	m.mu.Lock()
	m.now = m.now.Add(d)
	var due []*manualWaiter
	remaining := m.waiters[:0]
	for _, w := range m.waiters {
		if !m.now.Before(w.deadline) {
			w.done = true
			due = append(due, w)
		} else {
			remaining = append(remaining, w)
		}
	}
	m.waiters = remaining
	m.mu.Unlock()
	for _, w := range due {
		close(w.ch)
	}
}
