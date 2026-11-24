// Package compare runs multi-snapshot comparison sessions with
// configurable concurrency, per-session rule budgets and observable
// in-flight session counts.
package compare

import (
	"sync"
	"sync/atomic"
	"time"

	"fwadjudge/internal/codes"
	"fwadjudge/internal/engine"
	"fwadjudge/internal/fixed"
	"fwadjudge/internal/ledger"
	"fwadjudge/internal/model"
)

// Config controls resource limits of the compare manager.
type Config struct {
	MaxConcurrent int           // max simultaneous sessions
	RuleBudget    int           // max rules per snapshot session
	WaitThreshold time.Duration // max queue wait before E_COMPARE_CONTENTION
}

type Manager struct {
	sem      chan struct{}
	inflight atomic.Int64
	cfg      Config
}

func NewManager(cfg Config) *Manager {
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 1
	}
	return &Manager{sem: make(chan struct{}, cfg.MaxConcurrent), cfg: cfg}
}

// Inflight reports the current number of running sessions.
func (m *Manager) Inflight() int64 { return m.inflight.Load() }

// Acquire takes a session slot, waiting up to WaitThreshold. Requests
// beyond the concurrency limit queue instead of being rejected; only
// when the wait exceeds the threshold is E_COMPARE_CONTENTION returned.
func (m *Manager) Acquire() *codes.Error {
	t := time.NewTimer(m.cfg.WaitThreshold)
	defer t.Stop()
	select {
	case m.sem <- struct{}{}:
		m.inflight.Add(1)
		return nil
	case <-t.C:
		return codes.New(codes.ECompareContention, "compare session wait threshold exceeded")
	}
}

func (m *Manager) Release() {
	<-m.sem
	m.inflight.Add(-1)
}

// Task is one snapshot comparison session.
type Task struct {
	SnapshotID string
	Rules      []model.Rule
	Probes     []model.Probe
}

// Result is the outcome of one session.
type Result struct {
	SnapshotID string
	Session    *engine.Session
	Verdicts   []engine.Verdict
	Errors     []*codes.Error
	BudgetCut  bool // rule budget exceeded; snapshot truncated
}

// Run executes all tasks in isolated sessions. Sessions that exceed
// the rule budget are terminated with E_RULE_BUDGET but keep the
// already-parsed prefix of their rules.
func (m *Manager) Run(tasks []Task) []Result {
	results := make([]Result, len(tasks))
	var wg sync.WaitGroup
	for i := range tasks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = m.runOne(tasks[i])
		}(i)
	}
	wg.Wait()
	return results
}

func (m *Manager) runOne(t Task) Result {
	res := Result{SnapshotID: t.SnapshotID}
	if err := m.Acquire(); err != nil {
		err.SnapshotID = t.SnapshotID
		res.Errors = append(res.Errors, err)
		return res
	}
	defer m.Release()

	rules := t.Rules
	if m.cfg.RuleBudget > 0 && len(rules) > m.cfg.RuleBudget {
		rules = rules[:m.cfg.RuleBudget]
		res.BudgetCut = true
		res.Errors = append(res.Errors, codes.At(codes.ERuleBudget,
			"rule budget exceeded, snapshot truncated", t.SnapshotID,
			t.Rules[m.cfg.RuleBudget].Index, -1))
	}
	sess := engine.NewSession(t.SnapshotID, rules)
	for _, p := range t.Probes {
		v, err := sess.Observe(p)
		if err != nil {
			res.Errors = append(res.Errors, err)
			continue
		}
		res.Verdicts = append(res.Verdicts, v)
	}
	res.Session = sess
	return res
}

// BoundOf returns the fixed-point error bound of a session ledger.
func BoundOf(l *ledger.Ledger) fixed.Fixed {
	return fixed.Fixed(ledger.BoundFixed(l.BoundUnits()))
}
