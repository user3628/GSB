package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"sort"

	"configtree/internal/spec"
)

// CommitRequest is a snapshot or delta submission envelope carrying the
// idempotency key (commit_id) and explicit source priority policy.
type CommitRequest struct {
	CommitID string          `json:"commit_id"`
	Kind     string          `json:"kind"` // snapshot | delta
	Snapshot *spec.Snapshot  `json:"snapshot,omitempty"`
	Delta    *spec.Delta     `json:"delta,omitempty"`
	RawBody  json.RawMessage `json:"-"`
}

// Commit serializes one submission through the single-writer pipeline.
// Duplicate commit_ids are executed once; concurrent duplicates wait and
// reuse the result. A reused id with a different body is E_CONFLICTING_REQUEST.
func (e *Engine) Commit(ctx context.Context, req *CommitRequest) (*commitResult, error) {
	if req.Kind != "snapshot" && req.Kind != "delta" {
		return nil, spec.NewError(spec.CodeSpecInvalid, "unknown commit kind",
			map[string]any{"kind": req.Kind})
	}
	if req.CommitID == "" {
		return nil, spec.NewError(spec.CodeSpecInvalid, "commit_id is required", nil)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	// Fast path: an already completed commit with the same id is reused.
	if existingReq, existingRes, err := e.lookupIdempotent(ctx, req.CommitID); err != nil {
		return nil, err
	} else if existingRes != nil {
		if string(existingReq) != string(body) {
			return nil, spec.NewError(spec.CodeConflictingRequest,
				"commit_id reused with a different request body",
				map[string]any{"commit_id": req.CommitID})
		}
		res := *existingRes
		res.Reused = true
		return &res, nil
	}

	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	// In-process single-flight for concurrent duplicate submissions.
	e.mu.Lock()
	if op, ok := e.inflight[req.CommitID]; ok {
		e.mu.Unlock()
		<-op.done
		if string(op.resultPayload) != string(body) {
			return nil, spec.NewError(spec.CodeConflictingRequest,
				"commit_id reused with a different request body",
				map[string]any{"commit_id": req.CommitID})
		}
		return &op.result, op.err
	}
	op := &inflightOp{done: make(chan struct{})}
	e.inflight[req.CommitID] = op
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.inflight, req.CommitID)
		e.mu.Unlock()
		close(op.done)
	}()

	res, err := e.executeCommit(ctx, req, body)
	op.err = err
	if err == nil {
		op.result = *res
		payload, _ := json.Marshal(res)
		op.resultPayload = payload
	}
	return res, err
}

func (e *Engine) executeCommit(ctx context.Context, req *CommitRequest, body []byte) (*commitResult, error) {
	current, err := e.currentVersion(ctx)
	if err != nil {
		return nil, err
	}

	var (
		next        *materializedState
		baseVersion int64
		targetVer   int64
		triggerKeys []string
		checksum    string
	)
	switch req.Kind {
	case "snapshot":
		s := req.Snapshot
		if s == nil {
			return nil, spec.NewError(spec.CodeSpecInvalid, "snapshot payload missing", nil)
		}
		if err := spec.ValidateSnapshot(s); err != nil {
			return nil, err
		}
		if s.Version <= current {
			return nil, spec.NewError(spec.CodeStaleSnapshot,
				"snapshot version is not greater than the current version",
				map[string]any{"snapshot_version": s.Version, "current_version": current})
		}
		next = stateFromSnapshot(s)
		baseVersion, targetVer = current, s.Version
		checksum = s.Checksum
		// A full snapshot replaces everything: every key is a trigger.
		for k := range next.items {
			triggerKeys = append(triggerKeys, k)
		}
		sort.Strings(triggerKeys)
	case "delta":
		d := req.Delta
		if d == nil {
			return nil, spec.NewError(spec.CodeSpecInvalid, "delta payload missing", nil)
		}
		st, err := e.loadMaterialized(ctx)
		if err != nil {
			return nil, err
		}
		if err := validateDelta(st, d); err != nil {
			return nil, err
		}
		next = cloneState(st)
		applyDeltaToState(next, d)
		baseVersion, targetVer = d.BaseVersion, d.TargetVersion
		triggerKeys = changedKeyPaths(d)
		if len(triggerKeys) == 0 {
			triggerKeys = subscriptionTriggerKeys(st, next)
		}
		checksum = spec.Checksum(itemList(next), componentList(next))
	}

	intentID := "intent-" + req.CommitID
	pendingJSON, _ := json.Marshal(triggerKeys)

	// Phase 1: intent + new facts are written and committed first. Any derived
	// structures still reference the previous complete version at this point.
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO intents(intent_id, commit_id, kind, target_version, base_version,
			status, phase, created_at, updated_at, pending_keys, payload)
		VALUES(?,?,?,?,?, 'prepared', 'data_written', ?, ?, ?, ?)`,
		intentID, req.CommitID, req.Kind, targetVer, baseVersion,
		e.now(), e.now(), string(pendingJSON), string(body)); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if err := e.writeFacts(ctx, tx, req, next, targetVer); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	// Hard-kill window: facts and intent are durable, derived views are stale.
	if e.hook != nil && e.hook("after_facts_before_derived") {
		if e.testSkipExit {
			return nil, spec.NewError(spec.CodeStaleRead,
				"simulated hard kill before derived rebuild (test mode)",
				map[string]any{"phase": "after_facts_before_derived"})
		}
		_ = e.db.Close()
		os.Exit(137)
	}

	// Phase 2: cascade-invalidate and atomically rebuild every derived
	// structure inside one transaction, then close the intent and commit log.
	tx, err = e.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	views := computeViews(next)
	if err := e.replaceDerived(ctx, tx, next, views, triggerKeys, targetVer); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO commits(commit_id, kind, version, base_version, checksum, status, created_at, payload)
		VALUES(?,?,?,?,?, 'committed', ?, ?)`,
		req.CommitID, req.Kind, targetVer, baseVersion, checksum, e.now(), string(body)); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE intents SET status='completed', phase='derived_built', updated_at=?
		WHERE intent_id=?`, e.now(), intentID); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	affected := computeAffected(next, triggerKeys, false)
	res := &commitResult{
		Version:  targetVer,
		CommitID: req.CommitID,
		Affected: affected,
	}
	resultJSON, _ := json.Marshal(res)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO idempotency(commit_id, request, result_json, created_at)
		VALUES(?,?,?,?)`, req.CommitID, string(body), string(resultJSON), e.now()); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return res, nil
}

// writeFacts replaces current facts. For a snapshot the whole fact set is
// swapped; for a delta only touched rows change.
func (e *Engine) writeFacts(ctx context.Context, tx *sql.Tx, req *CommitRequest,
	st *materializedState, version int64) error {
	if req.Kind == "snapshot" {
		for _, table := range []string{
			"config_items", "components", "subscriptions", "dependencies",
		} {
			if _, err := tx.ExecContext(ctx, `DELETE FROM `+table); err != nil {
				return err
			}
		}
	}
	for _, it := range itemList(st) {
		raw, _ := json.Marshal(it.Value)
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO config_items(key_path, type, value, ord, version)
			VALUES(?,?,?,?,?)
			ON CONFLICT(key_path) DO UPDATE SET
			  type=excluded.type, value=excluded.value, ord=excluded.ord,
			  version=excluded.version`,
			it.KeyPath, it.Type, string(raw), it.Order, version); err != nil {
			return err
		}
	}
	// Delta deletions: keys absent from the state but present in the delta.
	if req.Kind == "delta" {
		for _, ch := range req.Delta.Items {
			if ch.Op == "delete" {
				if _, err := tx.ExecContext(ctx,
					`DELETE FROM config_items WHERE key_path=?`, ch.Item.KeyPath); err != nil {
					return err
				}
			}
		}
	}
	changedComps := map[string]bool{}
	if req.Kind == "delta" {
		for _, ch := range req.Delta.Subscriptions {
			changedComps[ch.Component.ID] = true
		}
	}
	for _, c := range componentList(st) {
		if req.Kind == "delta" && !changedComps[c.ID] {
			continue
		}
		specJSON, _ := json.Marshal(c)
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO components(component_id, wildcard, spec_json, version)
			VALUES(?,?,?,?)
			ON CONFLICT(component_id) DO UPDATE SET
			  wildcard=excluded.wildcard, spec_json=excluded.spec_json,
			  version=excluded.version`,
			c.ID, c.Wildcard, string(specJSON), version); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM subscriptions WHERE component_id=?`, c.ID); err != nil {
			return err
		}
		for _, p := range c.SubPrefixes {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO subscriptions(component_id, prefix, version)
				VALUES(?,?,?)`, c.ID, p, version); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM dependencies WHERE component_id=?`, c.ID); err != nil {
			return err
		}
		for _, up := range c.DependsOn {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO dependencies(component_id, upstream_id, version)
				VALUES(?,?,?)`, c.ID, up, version); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Engine) lookupIdempotent(ctx context.Context, id string) ([]byte, *commitResult, error) {
	var request, resultJSON string
	err := e.db.QueryRowContext(ctx,
		`SELECT request, result_json FROM idempotency WHERE commit_id=?`, id).
		Scan(&request, &resultJSON)
	if err == sql.ErrNoRows {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var res commitResult
	if err := json.Unmarshal([]byte(resultJSON), &res); err != nil {
		return nil, nil, err
	}
	return []byte(request), &res, nil
}

func stateFromSnapshot(s *spec.Snapshot) *materializedState {
	st := &materializedState{
		version:    s.Version,
		items:      map[string]spec.ConfigItem{},
		components: map[string]spec.Component{},
	}
	for _, it := range s.Items {
		st.items[it.KeyPath] = it
	}
	for _, c := range s.Components {
		st.components[c.ID] = c
	}
	return st
}

func itemList(st *materializedState) []spec.ConfigItem {
	out := make([]spec.ConfigItem, 0, len(st.items))
	for _, it := range st.items {
		out = append(out, it)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].KeyPath < out[j].KeyPath })
	return out
}

func componentList(st *materializedState) []spec.Component {
	out := make([]spec.Component, 0, len(st.components))
	for _, c := range st.components {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func subscriptionTriggerKeys(before, after *materializedState) []string {
	seen := map[string]bool{}
	for id, bc := range before.components {
		ac, ok := after.components[id]
		if !ok || !sameStringSet(bc.SubPrefixes, ac.SubPrefixes) ||
			!sameStringSet(bc.DependsOn, ac.DependsOn) {
			for k := range after.items {
				seen[k] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[string]int{}
	for _, s := range a {
		seen[s]++
	}
	for _, s := range b {
		seen[s]--
		if seen[s] < 0 {
			return false
		}
	}
	return true
}
