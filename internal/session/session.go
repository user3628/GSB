// Package session runs concurrent detection sessions with bounded
// concurrency, per-session process/event budgets and full state isolation.
// Over-limit batches queue instead of being rejected; budget breaches abort
// only the offending session while committed snapshots stay intact.
package session

import (
	"fmt"
	"sync"

	"privescworkbench/internal/detector"
	"privescworkbench/internal/domain"
	"privescworkbench/internal/snapshot"
)

// Limits configures concurrency and per-session budgets.
type Limits struct {
	MaxConcurrent int
	MaxProcesses  int
	MaxEvents     int
}

// DefaultLimits returns conservative local-workbench limits.
func DefaultLimits() Limits {
	return Limits{MaxConcurrent: 2, MaxProcesses: 1000, MaxEvents: 10000}
}

// Manager schedules sessions over a shared snapshot store.
type Manager struct {
	limits Limits
	sem    chan struct{}
	mu     sync.Mutex
	states map[string]*State
}

// State is the isolated per-session view, never shared across sessions.
type State struct {
	ID        string
	Processes int
	Events    int
	Status    string
	Code      domain.BizCode
	Queued    bool
	result    *detector.Result
	doneCh    chan struct{}
}

// NewManager builds a session manager.
func NewManager(limits Limits) *Manager {
	if limits.MaxConcurrent <= 0 {
		limits = DefaultLimits()
	}
	return &Manager{
		limits: limits,
		sem:    make(chan struct{}, limits.MaxConcurrent),
		states: map[string]*State{},
	}
}

// Submit queues and runs one batch. It blocks until a concurrency slot is
// available, so excess batches wait in queue rather than failing. Budget
// validation happens before any scoring work starts.
func (m *Manager) Submit(id string, b *domain.Bundle, store *snapshot.Store, threshold int64) (*detector.Result, *State, error) {
	st := &State{ID: id, Processes: len(b.Processes), Events: len(b.Events), Status: "queued", Code: domain.CodeOK, Queued: true, doneCh: make(chan struct{})}
	m.mu.Lock()
	m.states[id] = st
	m.mu.Unlock()

	m.sem <- struct{}{}
	st.Queued = false
	st.Status = "running"
	defer func() {
		<-m.sem
		close(st.doneCh)
	}()

	if len(b.Processes) > m.limits.MaxProcesses || len(b.Events) > m.limits.MaxEvents {
		st.Status = "aborted"
		st.Code = domain.CodeSessionBudget
		return nil, st, domain.NewError(domain.CodeSessionBudget,
			fmt.Sprintf("session %s exceeded budget: procs=%d events=%d limits=%+v",
				id, len(b.Processes), len(b.Events), m.limits))
	}

	res, err := detector.Run(b, store, detector.Options{
		SessionID:      id,
		ThresholdMicro: threshold,
		CommitSnapshot: store != nil,
	})
	if err != nil {
		st.Status = "failed"
		if ce, ok := err.(*domain.CodeError); ok {
			st.Code = ce.Code
		} else {
			st.Code = domain.CodeInternal
		}
		return nil, st, err
	}
	st.Status = "done"
	st.result = res
	return res, st, nil
}

// State returns an isolated snapshot of one session's status.
func (m *Manager) State(id string) (*State, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.states[id]
	if !ok {
		return nil, false
	}
	copy := *st
	return &copy, true
}

// WaitFor blocks until the named session reaches a terminal state.
func (m *Manager) WaitFor(id string) bool {
	m.mu.Lock()
	st, ok := m.states[id]
	m.mu.Unlock()
	if !ok {
		return false
	}
	<-st.doneCh
	return true
}
