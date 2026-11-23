package arbiter

import "sync"

// rowGate serializes every mutating operation for one business row. Reads
// never take this lock, so snapshot readers stay fully concurrent.
type rowGate struct {
	mu      sync.Mutex
	waiters []*leaseWaiter
}

// leaseWaiter is one blocked Acquire call. It parks on a channel that is
// closed when the lease is released or expires; there is no polling loop.
type leaseWaiter struct {
	ch     chan struct{}
	holder string
}

// gateRegistry hands out one gate per row id and lets recycle enumerate rows.
type gateRegistry struct {
	mu    sync.Mutex
	gates map[string]*rowGate
}

func newGateRegistry() *gateRegistry {
	return &gateRegistry{gates: make(map[string]*rowGate)}
}

func (r *gateRegistry) get(rowID string) *rowGate {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.gates[rowID]
	if !ok {
		g = &rowGate{}
		r.gates[rowID] = g
	}
	return g
}

// enqueue registers a waiter while the gate lock is held.
func (g *rowGate) enqueue(holder string) *leaseWaiter {
	w := &leaseWaiter{ch: make(chan struct{}), holder: holder}
	g.waiters = append(g.waiters, w)
	return w
}

// remove pulls a timed-out or cancelled waiter out of the queue.
func (g *rowGate) remove(w *leaseWaiter) {
	for i, cand := range g.waiters {
		if cand == w {
			g.waiters = append(g.waiters[:i], g.waiters[i+1:]...)
			return
		}
	}
}

// wakeAll releases every blocked waiter. Each one re-checks the durable lease
// under the gate lock, so exactly one caller can observe a free lease.
func (g *rowGate) wakeAll() {
	for _, w := range g.waiters {
		close(w.ch)
	}
	g.waiters = nil
}
