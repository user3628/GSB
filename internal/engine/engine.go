// Package engine implements alert candidate generation, deterministic
// fingerprinting, suppression, root-cause scoring, versioned incremental
// commits, atomic derived rebuilds, the sampling/full noise-reduction paths,
// patrol budget scheduling and crash recovery on top of the SQLite store.
package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"
	"time"

	"alarmengine/internal/model"
	"alarmengine/internal/storage"
)

// CrashHook names externally injectable crash points. Injected via
// Engine.Config.CrashPoint (external parameter), never by editing source.
type CrashPoint string

const (
	CrashAfterIntentIngest   CrashPoint = "CRASH_AFTER_INTENT_INGEST"
	CrashAfterIntentSnapshot CrashPoint = "CRASH_AFTER_INTENT_SNAPSHOT"
	CrashAfterIntentRebuild  CrashPoint = "CRASH_AFTER_INTENT_REBUILD"
	CrashMidRebuild          CrashPoint = "CRASH_MID_REBUILD"
)

// Config holds externally supplied runtime knobs.
type Config struct {
	// CrashPoint forces os.Exit after the durable intent was flushed but
	// before the transaction commits, simulating SIGKILL.
	CrashPoint CrashPoint
	// CrashIntentID limits the crash to one specific intent (empty = first).
	CrashIntentID string
	// PatrolQueueCapacity bounds the waiting queue.
	PatrolQueueCapacity int
	// Now overrides the clock for deterministic tests.
	Now func() int64
}

// Engine is the single-writer, multi-reader facade.
type Engine struct {
	store *storage.Store
	cfg   Config

	writeMu sync.Mutex // serializes every mutating operation (single writer)

	mu       sync.RWMutex
	services map[string]model.Service
	topology *model.Topology
	policy   model.PolicySnapshot
	version  int64 // current committed version
	lastGood int64 // last fully rebuilt version

	patrolMu       sync.Mutex
	patrolQ        chan patrolEnvelope
	patrolWg       sync.WaitGroup
	recoveryReport RecoveryReport
}

type patrolEnvelope struct {
	ctx  context.Context
	task model.PatrolTask
	ch   chan patrolResult
}

type patrolResult struct {
	task model.PatrolTask
	cost int64
}

// New opens an engine over an existing store and recovers unfinished intents.
func New(store *storage.Store, cfg Config) (*Engine, error) {
	if cfg.Now == nil {
		cfg.Now = func() int64 { return time.Now().UnixMilli() }
	}
	e := &Engine{
		store:    store,
		cfg:      cfg,
		services: map[string]model.Service{},
		patrolQ:  make(chan patrolEnvelope, maxInt(cfg.PatrolQueueCapacity, 1)),
	}
	if err := e.loadState(); err != nil {
		return nil, err
	}
	report, err := e.Recover()
	if err != nil {
		return nil, err
	}
	e.recoveryReport = report
	e.startPatrolWorkers()
	return e, nil
}

func (e *Engine) loadState() error {
	rows, err := storage.LoadServices(e.store.ReadDB())
	if err != nil {
		return err
	}
	svcs := make([]model.Service, 0, len(rows))
	for _, r := range rows {
		e.services[r.Service.ID] = r.Service
		svcs = append(svcs, r.Service)
	}
	e.topology = model.NewTopology(svcs)

	maxVer, err := storage.MaxPolicyVersion(e.store.ReadDB())
	if err != nil {
		return err
	}
	e.version, _ = storage.GetMetaInt(e.store.ReadDB(), storage.MetaCommittedVersion, maxVer)
	e.lastGood, _ = storage.GetMetaInt(e.store.ReadDB(), storage.MetaLastCompleteVer, e.version)
	if maxVer > 0 {
		row, err := storage.GetPolicyRow(e.store.ReadDB(), maxVer)
		if err != nil {
			return err
		}
		var p model.PolicySnapshot
		if err := json.Unmarshal([]byte(row.PayloadJSON), &p); err != nil {
			return err
		}
		e.policy = p
	}
	return nil
}

// Version returns the current committed policy/data version.
func (e *Engine) Version() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.version
}

// LastGoodVersion returns the last fully rebuilt version.
func (e *Engine) LastGoodVersion() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.lastGood
}

// Snapshot returns a defensive copy of the active policy.
func (e *Engine) Snapshot() model.PolicySnapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return clonePolicy(e.policy)
}

// Services returns a defensive copy of active services.
func (e *Engine) Services() []model.Service {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]model.Service, 0, len(e.services))
	for _, s := range e.services {
		out = append(out, s)
	}
	return out
}

// DB exposes the store for tooling/tests.
func (e *Engine) DB() *sql.DB { return e.store.DB() }

// Store exposes the underlying store.
func (e *Engine) Store() *storage.Store { return e.store }
