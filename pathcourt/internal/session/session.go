// Package session runs concurrent, isolated manifest resolution sessions
// with bounded concurrency, a waiting queue and a per-session path budget.
// Sessions share only the snapshot store and resolver; all in-progress state
// is session-local.
package session

import (
	"context"
	"sync"

	"pathcourt/internal/bizcode"
	"pathcourt/internal/resolver"
	"pathcourt/internal/snapshot"
)

// Config controls the manager.
type Config struct {
	// MaxConcurrent is the number of sessions resolved at once; overflow
	// sessions wait in a FIFO queue rather than being rejected.
	MaxConcurrent int
	// MaxPathsPerSession aborts a manifest with E_PATH_BUDGET when exceeded.
	MaxPathsPerSession int
}

// Stats is the observable resource state of the manager.
type Stats struct {
	Inflight   int `json:"inflight"`
	Waiting    int `json:"waiting"`
	Completed  int `json:"completed"`
	BudgetHits int `json:"budget_hits"`
}

// SessionOutcome is one manifest's adjudication result.
type SessionOutcome struct {
	ManifestID  string             `json:"manifest_id"`
	Code        bizcode.Code       `json:"code"`
	Rows        []snapshot.Row     `json:"rows"`
	Version     *snapshot.Version `json:"version,omitempty"`
	PathCount   int                `json:"path_count"`
	Committed   int                `json:"committed"`
	BudgetLimit int                `json:"budget_limit,omitempty"`
}

// Manager schedules sessions.
type Manager struct {
	cfg      Config
	resolver *resolver.Resolver
	store    *snapshot.Store
	sem      chan struct{}

	mu       sync.Mutex
	waiting  int
	complete int
	budget   int
}

// NewManager builds a Manager.
func NewManager(cfg Config, r *resolver.Resolver, store *snapshot.Store) *Manager {
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 1
	}
	if cfg.MaxPathsPerSession <= 0 {
		cfg.MaxPathsPerSession = 1 << 30
	}
	return &Manager{
		cfg:      cfg,
		resolver: r,
		store:    store,
		sem:      make(chan struct{}, cfg.MaxConcurrent),
	}
}

// Stats returns a consistent snapshot of resource counters.
func (m *Manager) Stats() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Stats{
		Inflight:   int(m.store.Inflight()),
		Waiting:    m.waiting,
		Completed:  m.complete,
		BudgetHits: m.budget,
	}
}

// Submit queues a manifest and blocks until a concurrency slot is free. The
// returned outcome always contains the completed rows even when the budget
// is hit; those rows are committed in one snapshot transaction.
func (m *Manager) Submit(ctx context.Context, manifestID string, paths []string) (*SessionOutcome, error) {
	// Queue phase: observable waiting count while blocked on the semaphore.
	m.mu.Lock()
	m.waiting++
	m.mu.Unlock()

	select {
	case m.sem <- struct{}{}:
	case <-ctx.Done():
		m.decWaiting()
		return nil, ctx.Err()
	}
	m.decWaiting()
	defer func() { <-m.sem }()

	m.store.EnterInflight()
	defer m.store.LeaveInflight()

	out := &SessionOutcome{ManifestID: manifestID, Code: bizcode.OK, PathCount: len(paths), BudgetLimit: m.cfg.MaxPathsPerSession}
	if len(paths) > m.cfg.MaxPathsPerSession {
		out.Code = bizcode.E_PATH_BUDGET
		m.mu.Lock()
		m.budget++
		m.mu.Unlock()
		paths = paths[:m.cfg.MaxPathsPerSession]
	}

	for idx, input := range paths {
		res, err := m.resolver.Resolve(manifestID, idx, input)
		if err != nil {
			return nil, err
		}
		out.Rows = append(out.Rows, snapshot.Row{
			ManifestID: manifestID,
			PathIndex:  idx,
			Input:      input,
			Result:     res,
		})
	}

	version, err := m.store.Commit(ctx, m.resolver.Root(), manifestID, out.Rows)
	if err != nil {
		return nil, err
	}
	out.Version = version
	out.Committed = len(out.Rows)

	m.mu.Lock()
	m.complete++
	m.mu.Unlock()
	return out, nil
}

func (m *Manager) decWaiting() {
	m.mu.Lock()
	m.waiting--
	m.mu.Unlock()
}
