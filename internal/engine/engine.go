package engine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"cdn-edge-consistency/internal/cache"
	"cdn-edge-consistency/internal/model"
	"cdn-edge-consistency/internal/policy"
	"cdn-edge-consistency/internal/scheduler"
	"cdn-edge-consistency/internal/store"
)

type Response struct {
	Status  int
	Body    []byte
	Headers map[string]string
}

type Engine struct {
	cfg   model.Config
	clock model.Clock
	store *store.Store

	visibility sync.RWMutex

	policyMu sync.RWMutex
	current  *policy.Snapshot
	versions map[int64]*policy.Snapshot

	sched      *scheduler.Scheduler
	coalescer  *scheduler.Coalescer
	originMu   sync.RWMutex
	origin     *OriginClient
	originBase string
	refusedURL string

	heatMu sync.RWMutex
	heat   map[string]int64

	idemMu sync.Mutex
	idem   map[string]*idemCall

	recoveryMu sync.RWMutex
	recovery   *store.RecoveryReport

	counters struct {
		originFetches atomic.Int64
		originErrors  atomic.Int64
		cacheHits     atomic.Int64
		cacheMisses   atomic.Int64
		coalesced     atomic.Int64
		staleServed   atomic.Int64
		purges        atomic.Int64
	}
}

type idemCall struct {
	done     chan struct{}
	response Response
	trace    string
	owner    bool
}

func New(ctx context.Context, cfg model.Config, initial *policy.Snapshot, clock model.Clock) (*Engine, error) {
	if clock == nil {
		clock = model.RealClock{}
	}
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return nil, err
	}
	e := &Engine{
		cfg:      cfg,
		clock:    clock,
		store:    st,
		versions: map[int64]*policy.Snapshot{},
		heat:     map[string]int64{},
		idem:     map[string]*idemCall{},
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = st.Close()
		}
	}()
	nowMs := clock.Now().UnixMilli()
	report, err := st.Recover(ctx, nowMs)
	if err != nil {
		return nil, fmt.Errorf("recover store: %w", err)
	}
	e.recovery = report

	latest, err := st.CurrentPolicy(ctx)
	if err != nil {
		return nil, err
	}
	if latest == nil {
		if initial == nil {
			return nil, errors.New("database has no policy snapshot and no initial policy was supplied")
		}
		cp := initial.Clone()
		allowed := map[string]model.NodeConfig{}
		for _, n := range cfg.Nodes {
			allowed[n.ID] = n
		}
		if err := cp.NormalizeAndValidate(allowed); err != nil {
			return nil, err
		}
		if err := cp.Seal(); err != nil {
			return nil, err
		}
		cp.CommittedAtMs = nowMs
		if err := st.Do(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return saveInitialPolicy(ctx, tx, cp)
		}); err != nil {
			return nil, err
		}
		latest = cp.Clone()
	}
	e.current = latest
	e.versions[latest.Version] = latest
	e.sched = scheduler.New(schedulerConfig(latest))
	e.coalescer = scheduler.NewCoalescer(e.sched)
	closedAddr, err := reserveClosedLoopbackAddr()
	if err != nil {
		return nil, err
	}
	e.refusedURL = "http://" + closedAddr
	cleanup = false
	return e, nil
}

func validateConfig(cfg model.Config) error {
	if cfg.DBPath == "" {
		return errors.New("db_path is required")
	}
	if cfg.OriginRoot == "" {
		return errors.New("origin_root is required")
	}
	if len(cfg.Nodes) == 0 {
		return errors.New("at least one edge node is required")
	}
	seen := map[string]bool{}
	for _, n := range cfg.Nodes {
		if n.ID == "" || seen[n.ID] {
			return fmt.Errorf("edge node ids must be unique and non-empty")
		}
		seen[n.ID] = true
		if n.OriginID == "" {
			return fmt.Errorf("node %s has empty origin_id", n.ID)
		}
	}
	return nil
}

func schedulerConfig(p *policy.Snapshot) scheduler.Config {
	return scheduler.Config{
		GlobalLimit:     p.Scheduler.GlobalOriginConcurrency,
		PerOriginLimit:  p.Scheduler.PerOriginConcurrency,
		QueueCapacity:   p.Scheduler.QueueCapacity,
		RoundBudgetMs:   p.Scheduler.RoundBudgetMs,
		DefaultTaskCost: p.Scheduler.DefaultTaskCostMs,
	}
}

func saveInitialPolicy(ctx context.Context, tx *sql.Tx, p *policy.Snapshot) error {
	if err := store.ValidateNewPolicyTx(ctx, tx, p.Version); err != nil {
		return err
	}
	return store.SavePolicyTx(ctx, tx, p)
}

func reserveClosedLoopbackAddr() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		return "", err
	}
	return addr, nil
}

// SetOriginBaseURL must be called after the local HTTP listener is active.
func (e *Engine) SetOriginBaseURL(base string) {
	e.originMu.Lock()
	defer e.originMu.Unlock()
	if e.origin != nil {
		e.origin.Close()
	}
	e.originBase = base
	e.origin = NewOriginClient(base, e.refusedURL, e.clock)
}

func (e *Engine) OriginBaseURL() string {
	e.originMu.RLock()
	defer e.originMu.RUnlock()
	return e.originBase
}

func (e *Engine) Store() *store.Store  { return e.store }
func (e *Engine) Config() model.Config { return e.cfg }
func (e *Engine) Clock() model.Clock   { return e.clock }

func (e *Engine) CurrentPolicy() *policy.Snapshot {
	e.policyMu.RLock()
	defer e.policyMu.RUnlock()
	return e.current.Clone()
}

func (e *Engine) SnapshotFor(ctx context.Context, version int64) (*policy.Snapshot, error) {
	e.policyMu.RLock()
	if version == 0 && e.current != nil {
		p := e.current
		e.policyMu.RUnlock()
		return p, nil
	}
	if p, ok := e.versions[version]; ok {
		e.policyMu.RUnlock()
		return p, nil
	}
	e.policyMu.RUnlock()
	p, err := e.store.PolicyByVersion(ctx, version)
	if err != nil {
		return nil, err
	}
	if p == nil {
		return nil, model.WithDetails(model.NewBiz(model.CodeVersionRetired, "requested policy snapshot is not retained"), map[string]any{"snapshot_version": version})
	}
	e.policyMu.Lock()
	e.versions[version] = p
	e.policyMu.Unlock()
	return p, nil
}

func (e *Engine) SubmitPolicy(ctx context.Context, incoming *policy.Snapshot, requestID string) (*policy.Snapshot, error) {
	cp := incoming.Clone()
	allowed := map[string]model.NodeConfig{}
	for _, n := range e.cfg.Nodes {
		allowed[n.ID] = n
	}
	if err := cp.NormalizeAndValidate(allowed); err != nil {
		e.audit(ctx, requestID, err)
		return nil, err
	}
	if err := cp.Seal(); err != nil {
		e.audit(ctx, requestID, err)
		return nil, err
	}
	nowMs := e.clock.Now().UnixMilli()
	cp.CommittedAtMs = nowMs
	if err := e.store.Do(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := store.ValidateNewPolicyTx(ctx, tx, cp.Version); err != nil {
			return err
		}
		return store.SavePolicyTx(ctx, tx, cp)
	}); err != nil {
		e.audit(ctx, requestID, err)
		return nil, err
	}
	e.policyMu.Lock()
	e.current = cp
	e.versions[cp.Version] = cp
	e.policyMu.Unlock()
	e.sched.UpdateConfig(schedulerConfig(cp))
	return cp.Clone(), nil
}

func (e *Engine) audit(ctx context.Context, requestID string, err error) {
	if err == nil {
		return
	}
	be := model.AsBiz(err)
	_ = e.store.AddAudit(context.Background(), requestID, be.Code, be.Message, be.Details, e.clock.Now().UnixMilli())
}

func (e *Engine) bumpHeat(key string) int64 {
	e.heatMu.Lock()
	defer e.heatMu.Unlock()
	e.heat[key]++
	return e.heat[key]
}

func (e *Engine) Heat(key string) int64 {
	e.heatMu.RLock()
	defer e.heatMu.RUnlock()
	return e.heat[key]
}

func (e *Engine) OriginClient() (*OriginClient, error) {
	e.originMu.RLock()
	defer e.originMu.RUnlock()
	if e.origin == nil {
		return nil, errors.New("origin base URL has not been configured")
	}
	return e.origin, nil
}

func (e *Engine) fetchOrigin(ctx context.Context, identity cache.Identity, headers http.Header, fault FaultSpec, timeoutMs int64) (*OriginResult, int, error) {
	client, err := e.OriginClient()
	if err != nil {
		return nil, 0, err
	}
	e.counters.originFetches.Add(1)
	result, err := client.Fetch(ctx, identity, headers, fault, time.Duration(timeoutMs)*time.Millisecond)
	if err != nil {
		e.counters.originErrors.Add(1)
	}
	return result, 1, err
}

func (e *Engine) newRequestID(explicit string) string {
	if explicit != "" {
		return explicit
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err == nil {
		return "req-" + hex.EncodeToString(b[:])
	}
	return fmt.Sprintf("req-%d", e.clock.Now().UnixNano())
}

func (e *Engine) LatestRecoveryReport() *store.RecoveryReport {
	e.recoveryMu.RLock()
	defer e.recoveryMu.RUnlock()
	if e.recovery == nil {
		return nil
	}
	cp := *e.recovery
	return &cp
}

func (e *Engine) WriteRecoveryReport(path string) error {
	report := e.LatestRecoveryReport()
	if report == nil {
		return nil
	}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o644)
}

func (e *Engine) Metrics(ctx context.Context) (map[string]int64, error) {
	base, err := e.store.Metrics(ctx)
	if err != nil {
		return nil, err
	}
	base["runtime_origin_fetches"] = e.counters.originFetches.Load()
	base["runtime_origin_errors"] = e.counters.originErrors.Load()
	base["runtime_cache_hits"] = e.counters.cacheHits.Load()
	base["runtime_cache_misses"] = e.counters.cacheMisses.Load()
	base["runtime_coalesced_waiters"] = e.counters.coalesced.Load()
	base["runtime_stale_served"] = e.counters.staleServed.Load()
	base["runtime_purges"] = e.counters.purges.Load()
	base["scheduler_queue_depth"] = int64(e.sched.QueueDepth())
	base["scheduler_active"] = int64(e.sched.Active())
	base["coalescer_inflight"] = int64(e.coalescer.InFlight())
	return base, nil
}

func (e *Engine) SchedulerReport() scheduler.RoundReport {
	return e.sched.LastReport()
}

func (e *Engine) Close(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		_ = e.sched.WaitIdle(ctx)
		e.sched.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		e.sched.Close()
	}
	e.originMu.Lock()
	if e.origin != nil {
		e.origin.Close()
	}
	e.originMu.Unlock()
	return e.store.Close()
}
