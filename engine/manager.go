// Package engine coordinates atomic incremental applications, namespace
// serialization, global in-flight limits and queue-timeout behavior.
package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"sandboxpolicy/business"
	"sandboxpolicy/policy"
	"sandboxpolicy/snapshot"
)

type Config struct {
	MaxInflight  int
	QueueTimeout time.Duration
}

type Manager struct {
	store *snapshot.Store
	cfg   Config
	sem   chan struct{}
	gates sync.Map
}

type Result struct {
	Namespace     string         `json:"namespace"`
	BaseVersion   int            `json:"base_version"`
	Version       int            `json:"version"`
	ParentVersion int            `json:"parent_version"`
	Digest        string         `json:"digest"`
	Closure       policy.Closure `json:"closure"`
}

func NewManager(store *snapshot.Store, cfg Config) *Manager {
	if cfg.MaxInflight <= 0 {
		cfg.MaxInflight = 4
	}
	if cfg.QueueTimeout <= 0 {
		cfg.QueueTimeout = 2 * time.Second
	}
	return &Manager{store: store, cfg: cfg, sem: make(chan struct{}, cfg.MaxInflight)}
}

type gate struct {
	ch chan struct{}
}

func newGate() *gate {
	g := &gate{ch: make(chan struct{}, 1)}
	g.ch <- struct{}{}
	return g
}

func (g *gate) tryAcquire() bool {
	select {
	case <-g.ch:
		return true
	default:
		return false
	}
}

func (g *gate) acquire(ctx context.Context) bool {
	select {
	case <-g.ch:
		return true
	case <-ctx.Done():
		return false
	}
}

func (g *gate) release() {
	g.ch <- struct{}{}
}

// Apply commits one change set and returns its version-bound closure.
func (m *Manager) Apply(ctx context.Context, cs *policy.ChangeSet) (*Result, error) {
	if cs == nil {
		return nil, business.New(business.CodePolicyMalformed, "changeset is required")
	}
	working := cs.Clone()
	if err := policy.NormalizeChangeSet(working); err != nil {
		return nil, err
	}
	namespace := working.Namespace
	requestVersion := working.BaseVersion

	value, _ := m.gates.LoadOrStore(namespace, newGate())
	nsGate := value.(*gate)
	waitCtx, cancel := context.WithTimeout(ctx, m.cfg.QueueTimeout)
	defer cancel()
	queuedDetail := ""
	if !nsGate.tryAcquire() {
		queuedDetail = "waiting for namespace gate"
		if !nsGate.acquire(waitCtx) {
			return nil, m.contention(ctx, namespace, requestVersion, "namespace gate wait exceeded threshold")
		}
	}
	defer nsGate.release()

	select {
	case m.sem <- struct{}{}:
	default:
		if queuedDetail == "" {
			queuedDetail = "waiting for global in-flight slot"
		}
		select {
		case m.sem <- struct{}{}:
		case <-waitCtx.Done():
			return nil, m.contention(ctx, namespace, requestVersion, "global in-flight wait exceeded threshold")
		}
	}
	defer func() { <-m.sem }()
	if queuedDetail != "" {
		m.recordBestEffort(namespace, requestVersion, "queued", "", queuedDetail)
	}

	txCtx, cancelTx := context.WithTimeout(ctx, 30*time.Second)
	defer cancelTx()
	var tx *sql.Tx
	var err error
	for {
		tx, err = m.store.Begin(txCtx)
		if err == nil {
			break
		}
		if !isBusy(err) {
			return nil, err
		}
		select {
		case <-waitCtx.Done():
			return nil, m.contention(ctx, namespace, requestVersion, "sqlite writer busy")
		case <-time.After(20 * time.Millisecond):
		}
	}
	txClosed := false
	defer func() {
		if !txClosed {
			_ = tx.Rollback()
		}
	}()
	latest, chain, err := m.store.LatestForUpdate(txCtx, tx, namespace)
	if err != nil {
		return nil, err
	}
	if chain != nil && chain.Warning != nil {
		return nil, &business.Error{Code: business.CodeSnapshotVersion, Version: chain.Warning.InvalidVersion, Message: chain.Warning.Message}
	}
	if latest == nil {
		return nil, business.New(business.CodeBaselineMissing, fmt.Sprintf("namespace %q has no committed baseline", namespace))
	}
	abort := func(be *business.Error) error {
		_ = tx.Rollback()
		txClosed = true
		m.recordFailure(context.WithoutCancel(ctx), namespace, latest.Version, be)
		return be
	}
	if working.BaseVersion > 0 && working.BaseVersion != latest.Version {
		err := &business.Error{Code: business.CodeChangeConflict, Entry: "changeset.base_version", Version: latest.Version, Message: fmt.Sprintf("committed version is %d, request expected %d", latest.Version, working.BaseVersion)}
		return nil, abort(err)
	}
	applied, err := policy.Apply(latest.Policy, working, latest.Version+1)
	if err != nil {
		return nil, abort(business.From(err))
	}
	stored, err := m.store.InsertSnapshotTx(txCtx, tx, applied.Policy, applied.Closure, latest.Version)
	if err != nil {
		return nil, abort(business.From(err))
	}
	if err := m.store.RecordEventTx(txCtx, tx, snapshot.Event{Namespace: namespace, Version: stored.Version, Operation: "apply", EventType: "applied", Code: "OK", Detail: fmt.Sprintf("committed version %d", stored.Version)}); err != nil {
		return nil, abort(business.From(err))
	}
	if err := tx.Commit(); err != nil {
		txClosed = true
		_ = tx.Rollback()
		if isBusy(err) {
			return nil, m.contention(ctx, namespace, requestVersion, "sqlite commit busy")
		}
		m.recordFailure(context.WithoutCancel(ctx), namespace, latest.Version, business.From(err))
		return nil, err
	}
	txClosed = true
	return &Result{
		Namespace:     stored.Namespace,
		BaseVersion:   latest.Version,
		Version:       stored.Version,
		ParentVersion: stored.ParentVersion,
		Digest:        stored.Digest,
		Closure:       stored.Closure,
	}, nil
}

func (m *Manager) contention(ctx context.Context, namespace string, version int, detail string) error {
	err := &business.Error{Code: business.CodeChangeContention, Version: version, Message: detail}
	m.recordBestEffort(namespace, version, "contention", string(err.Code), detail)
	return err
}

func (m *Manager) record(ctx context.Context, event snapshot.Event) error {
	if err := m.store.RecordEvent(ctx, event); err != nil {
		return business.New(business.CodeInternal, err.Error())
	}
	return nil
}

func (m *Manager) recordBestEffort(namespace string, version int, eventType, code, detail string) {
	eventCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_ = m.store.RecordEvent(eventCtx, snapshot.Event{Namespace: namespace, Version: version, Operation: "apply", EventType: eventType, Code: code, Detail: detail})
}

func (m *Manager) recordFailure(ctx context.Context, namespace string, version int, be *business.Error) {
	if be == nil {
		return
	}
	eventType := "failed"
	if be.Code == business.CodeChangeConflict {
		eventType = "conflict"
	}
	_ = m.record(ctx, snapshot.Event{Namespace: namespace, Version: version, Operation: "apply", EventType: eventType, Code: string(be.Code), Detail: be.Message})
}

func isBusy(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	return strings.Contains(text, "database is locked") || strings.Contains(text, "sqlite_busy") || strings.Contains(text, "busy")
}
