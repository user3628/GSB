package registry

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// Heartbeat records a real lease observation. The effective heartbeat is
// monotonic: an out-of-order observation never moves it backwards.
func (e *Engine) Heartbeat(ctx context.Context, service, id string, req HeartbeatRequest) (HeartbeatResult, *Error) {
	var out HeartbeatResult
	normalized := req
	normalized.RequestID = ""
	hash := requestHash(struct {
		Service string           `json:"service"`
		ID      string           `json:"id"`
		Request HeartbeatRequest `json:"request"`
	}{service, id, normalized})
	operation := "heartbeat:" + service + "/" + id
	body, opErr := e.idempotent(ctx, req.RequestID, operation, hash, func() ([]byte, *Error) {
		e.writeMu.Lock()
		defer e.writeMu.Unlock()
		return e.doHeartbeat(ctx, service, id, req, operation, hash)
	})
	if opErr != nil {
		return out, opErr
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return out, newError(ErrInternal, "decode heartbeat response", "detail", err.Error())
	}
	return out, nil
}

func (e *Engine) doHeartbeat(ctx context.Context, service, id string, req HeartbeatRequest, operation, hash string) ([]byte, *Error) {
	e.stateMu.RLock()
	meta, instances, loadErr := e.loadState(ctx)
	e.stateMu.RUnlock()
	if loadErr != nil {
		return nil, loadErr
	}
	byKey := runtimeMap(instances)
	key := service + "\x00" + id
	current, ok := byKey[key]
	if !ok {
		return nil, newError(ErrNotFound, "instance not found", "serviceName", service, "instanceId", id)
	}
	if current.Probe.Kind != ProbeLease {
		return nil, validationError(fmt.Errorf("heartbeats are only accepted for LEASE probe instances"))
	}
	observedMS := time.Now().UnixMilli()
	if req.ObservedAt != "" {
		t, err := time.Parse(time.RFC3339Nano, req.ObservedAt)
		if err != nil {
			return nil, validationError(fmt.Errorf("observedAt must be RFC3339: %w", err))
		}
		observedMS = t.UnixMilli()
	}
	if observedMS < current.LastHeartbeatMS {
		observedMS = current.LastHeartbeatMS
	}
	if req.ObservedAt == "" {
		observedMS = time.Now().UnixMilli()
	}
	revision := meta.DerivedRevision + 1
	current.LastHeartbeatMS = observedMS
	current.UpdatedRevision = revision
	byKey[key] = current
	updated := sortedRuntimeMap(byKey)
	view := buildDerived(revision, updated, time.Now().UnixMilli())
	view.SnapshotVersion = meta.Version
	expires := int64(0)
	if observedMS > 0 {
		expires = observedMS + current.LeaseTTLMS
	}
	result := HeartbeatResult{ServiceName: service, InstanceID: id, LastHeartbeatMS: observedMS,
		ExpiresAtMS: expires, DerivedRevision: revision}
	body, _ := json.Marshal(result)
	return e.commitRuntimeUpdate(ctx, runtimeCommit{
		Kind: "heartbeat", RequestID: req.RequestID, Operation: operation, RequestHash: hash,
		Revision: revision, View: view, Upserts: []RuntimeInstance{current}, Body: body,
	})
}

// SetState validates and commits an explicit state transition.
func (e *Engine) SetState(ctx context.Context, service, id string, req SetStateRequest) (StateResult, *Error) {
	var out StateResult
	normalized := req
	normalized.RequestID = ""
	hash := requestHash(struct {
		Service string          `json:"service"`
		ID      string          `json:"id"`
		Request SetStateRequest `json:"request"`
	}{service, id, normalized})
	operation := "state:" + service + "/" + id
	body, opErr := e.idempotent(ctx, req.RequestID, operation, hash, func() ([]byte, *Error) {
		e.writeMu.Lock()
		defer e.writeMu.Unlock()
		return e.doSetState(ctx, service, id, req, operation, hash)
	})
	if opErr != nil {
		return out, opErr
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return out, newError(ErrInternal, "decode state response", "detail", err.Error())
	}
	return out, nil
}

func (e *Engine) doSetState(ctx context.Context, service, id string, req SetStateRequest, operation, hash string) ([]byte, *Error) {
	switch req.State {
	case StateUnknown, StateHealthy, StateSuspect, StateUnhealthy, StateDraining:
	default:
		return nil, validationError(fmt.Errorf("state must be UNKNOWN, HEALTHY, SUSPECT, UNHEALTHY, or DRAINING"))
	}
	e.stateMu.RLock()
	meta, instances, loadErr := e.loadState(ctx)
	e.stateMu.RUnlock()
	if loadErr != nil {
		return nil, loadErr
	}
	byKey := runtimeMap(instances)
	key := service + "\x00" + id
	current, ok := byKey[key]
	if !ok {
		return nil, newError(ErrNotFound, "instance not found", "serviceName", service, "instanceId", id)
	}
	from := current.State
	if !CanTransition(from, req.State) {
		return nil, newError(ErrInvalidTransition, "illegal health state transition",
			"serviceName", service, "instanceId", id, "from", from, "to", req.State)
	}
	if from == req.State {
		result := StateResult{ServiceName: service, InstanceID: id, From: from, To: req.State,
			DerivedRevision: meta.DerivedRevision}
		body, _ := json.Marshal(result)
		return body, nil
	}
	revision := meta.DerivedRevision + 1
	current.State = req.State
	current.ConsecutiveSuccess = 0
	current.ConsecutiveFailure = 0
	current.UpdatedRevision = revision
	byKey[key] = current
	updated := sortedRuntimeMap(byKey)
	view := buildDerived(revision, updated, time.Now().UnixMilli())
	view.SnapshotVersion = meta.Version
	result := StateResult{ServiceName: service, InstanceID: id, From: from, To: req.State, DerivedRevision: revision}
	body, _ := json.Marshal(result)
	return e.commitRuntimeUpdate(ctx, runtimeCommit{
		Kind: "state", RequestID: req.RequestID, Operation: operation, RequestHash: hash,
		Revision: revision, View: view, Upserts: []RuntimeInstance{current}, Body: body,
	})
}

type runtimeCommit struct {
	Kind        string
	RequestID   string
	Operation   string
	RequestHash string
	Revision    int64
	View        DerivedView
	Upserts     []RuntimeInstance
	Deletes     []InstanceRef
	Body        []byte
	RoundID     string
}

func (e *Engine) commitRuntimeUpdate(ctx context.Context, p runtimeCommit) ([]byte, *Error) {
	intentID := newID()
	err := writeTx(ctx, e.db.sql, func(tx *sql.Tx) error {
		if err := insertIntent(ctx, tx, intentID, p.Kind, p.RequestID, p.RequestHash, 0, p.Revision, p.RoundID, nil); err != nil {
			return err
		}
		if p.RequestID != "" {
			return putRequestRunning(ctx, tx, p.RequestID, p.Operation, p.RequestHash)
		}
		return nil
	})
	if err != nil {
		return nil, newError(ErrInternal, "write runtime intent failed", "detail", err.Error())
	}
	e.crash("after_intent")

	err = writeTx(ctx, e.db.sql, func(tx *sql.Tx) error {
		if err := stageDerived(ctx, tx, p.View, p.View.SnapshotVersion, p.Kind); err != nil {
			return err
		}
		e.crash("mid_cascade")
		return nil
	})
	if err != nil {
		e.abortRuntimeIntent(ctx, intentID, p.Revision)
		return nil, newError(ErrInternal, "stage runtime derived view failed", "detail", err.Error())
	}
	e.crash("after_stage")

	err = writeTx(ctx, e.db.sql, func(tx *sql.Tx) error {
		for _, ref := range p.Deletes {
			if _, err := tx.ExecContext(ctx, `DELETE FROM instances WHERE service_name=? AND instance_id=?`, ref.ServiceName, ref.InstanceID); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM leases WHERE service_name=? AND instance_id=?`, ref.ServiceName, ref.InstanceID); err != nil {
				return err
			}
		}
		for _, in := range p.Upserts {
			if err := upsertRuntimeInstance(ctx, tx, in); err != nil {
				return err
			}
			if err := upsertLease(ctx, tx, in, 0); err != nil {
				return err
			}
		}
		if err := activateDerived(ctx, tx, p.View, -1); err != nil {
			return err
		}
		if p.RoundID != "" {
			if err := setMeta(ctx, tx, "latest_round_id", p.RoundID); err != nil {
				return err
			}
		}
		if err := completeIntent(ctx, tx, intentID, "COMPLETED"); err != nil {
			return err
		}
		if p.RequestID != "" {
			return finishRequest(ctx, tx, p.RequestID, "COMPLETED", p.Body, 200, 0, "")
		}
		return nil
	})
	if err != nil {
		e.abortRuntimeIntent(ctx, intentID, p.Revision)
		return nil, newError(ErrInternal, "activate runtime derived view failed", "detail", err.Error())
	}
	e.crash("after_activation")
	return p.Body, nil
}

func (e *Engine) abortRuntimeIntent(ctx context.Context, intentID string, revision int64) {
	_ = writeTx(context.Background(), e.db.sql, func(tx *sql.Tx) error {
		_, _ = tx.ExecContext(ctx, `DELETE FROM health_summary WHERE revision=?`, revision)
		_, _ = tx.ExecContext(ctx, `DELETE FROM weight_allocations WHERE revision=?`, revision)
		_, _ = tx.ExecContext(ctx, `DELETE FROM probe_plans WHERE revision=?`, revision)
		_, _ = tx.ExecContext(ctx, `DELETE FROM derived_revisions WHERE revision=? AND status='STAGING'`, revision)
		_, _ = tx.ExecContext(ctx, `UPDATE intents SET status='ABORTED',updated_at=? WHERE id=?`, nowText(), intentID)
		return nil
	})
}

func sortedRuntimeMap(byKey map[string]RuntimeInstance) []RuntimeInstance {
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]RuntimeInstance, 0, len(keys))
	for _, key := range keys {
		out = append(out, byKey[key])
	}
	return out
}
