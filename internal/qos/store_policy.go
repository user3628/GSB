package qos

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"strings"
)

type derivedPayload struct {
	Queue       Queue            `json:"queue"`
	Effective   EffectiveRates   `json:"effective_rates"`
	TokenView   TokenBucketView  `json:"token_bucket_view"`
	Consistency ConsistencyScore `json:"consistency"`
}

func loadPolicyTx(ctx context.Context, tx *sql.Tx) (map[string]Queue, error) {
	rows, err := tx.QueryContext(ctx, `SELECT queue_id,cir_bps,pir_bps,cbs_bytes,pbs_bytes,priority,
		src_port_min,src_port_max,dscp_json,parent_id FROM queues`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	policy := map[string]Queue{}
	for rows.Next() {
		var q Queue
		var dscp string
		var parent sql.NullString
		if err := rows.Scan(&q.ID, &q.CIRBPS, &q.PIRBPS, &q.CBSBytes, &q.PBSBytes, &q.Priority,
			&q.Match.SrcPortMin, &q.Match.SrcPortMax, &dscp, &parent); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(dscp), &q.Match.DSCPValues); err != nil {
			return nil, err
		}
		if parent.Valid {
			p := parent.String
			q.ParentID = &p
		}
		policy[q.ID] = q
	}
	return policy, rows.Err()
}

func loadPolicyDB(ctx context.Context, db *sql.DB) (map[string]Queue, error) {
	rows, err := db.QueryContext(ctx, `SELECT queue_id,cir_bps,pir_bps,cbs_bytes,pbs_bytes,priority,
		src_port_min,src_port_max,dscp_json,parent_id FROM queues`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	policy := map[string]Queue{}
	for rows.Next() {
		var q Queue
		var dscp string
		var parent sql.NullString
		if err := rows.Scan(&q.ID, &q.CIRBPS, &q.PIRBPS, &q.CBSBytes, &q.PBSBytes, &q.Priority,
			&q.Match.SrcPortMin, &q.Match.SrcPortMax, &dscp, &parent); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(dscp), &q.Match.DSCPValues); err != nil {
			return nil, err
		}
		if parent.Valid {
			p := parent.String
			q.ParentID = &p
		}
		policy[q.ID] = q
	}
	return policy, rows.Err()
}

func insertQueueTx(ctx context.Context, tx *sql.Tx, q Queue, version int64) error {
	dscp, _ := json.Marshal(q.Match.DSCPValues)
	var parent any
	if q.ParentID != nil {
		parent = *q.ParentID
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO queues(queue_id,cir_bps,pir_bps,cbs_bytes,pbs_bytes,priority,
		src_port_min,src_port_max,dscp_json,parent_id,policy_version) VALUES(?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(queue_id) DO UPDATE SET cir_bps=excluded.cir_bps,pir_bps=excluded.pir_bps,
		cbs_bytes=excluded.cbs_bytes,pbs_bytes=excluded.pbs_bytes,priority=excluded.priority,
		src_port_min=excluded.src_port_min,src_port_max=excluded.src_port_max,dscp_json=excluded.dscp_json,
		parent_id=excluded.parent_id,policy_version=excluded.policy_version`,
		q.ID, q.CIRBPS, q.PIRBPS, q.CBSBytes, q.PBSBytes, q.Priority,
		q.Match.SrcPortMin, q.Match.SrcPortMax, string(dscp), parent, version)
	return err
}

func makeDerived(q Queue, eff EffectiveRates, policy map[string]Queue, calc, valid int64) derivedPayload {
	eff.CalculationVersion = calc
	eff.ValidThroughVersion = valid
	tv := BuildTokenBucketView(q, eff, calc, valid)
	cs := BuildConsistencyScore(q, eff, policy, calc, valid)
	return derivedPayload{Queue: q, Effective: eff, TokenView: tv, Consistency: cs}
}

func insertDerivedTx(ctx context.Context, tx *sql.Tx, version int64, d derivedPayload) error {
	effJSON, _ := json.Marshal(d.Effective)
	cirJSON, _ := json.Marshal(d.Effective.CIRSRC)
	pirJSON, _ := json.Marshal(d.Effective.PIRSRC)
	chainJSON, _ := json.Marshal(d.Effective.ParentChain)
	tokenJSON, _ := json.Marshal(d.TokenView)
	consJSON, _ := json.Marshal(d.Consistency)
	_, err := tx.ExecContext(ctx, `INSERT INTO queue_views(queue_id,effective_cir_bps,effective_pir_bps,effective_json,
		cir_source_json,pir_source_json,parent_chain_json,token_view_json,consistency_json,consistency_score,
		calculation_version,valid_through_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(queue_id) DO UPDATE SET effective_cir_bps=excluded.effective_cir_bps,
		effective_pir_bps=excluded.effective_pir_bps,effective_json=excluded.effective_json,
		cir_source_json=excluded.cir_source_json,pir_source_json=excluded.pir_source_json,
		parent_chain_json=excluded.parent_chain_json,token_view_json=excluded.token_view_json,
		consistency_json=excluded.consistency_json,consistency_score=excluded.consistency_score,
		calculation_version=excluded.calculation_version,valid_through_version=excluded.valid_through_version`,
		d.Queue.ID, d.Effective.EffectiveCIRBPS, d.Effective.EffectivePIRBPS, string(effJSON),
		string(cirJSON), string(pirJSON), string(chainJSON), string(tokenJSON), string(consJSON),
		d.Consistency.Score, version, version)
	if err != nil {
		return err
	}
	full, _ := json.Marshal(d)
	if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO queue_view_history(version,queue_id,view_json)
		VALUES(?,?,?)`, version, d.Queue.ID, string(full)); err != nil {
		return err
	}
	return nil
}

func insertCommitTx(ctx context.Context, tx *sql.Tx, version, parent int64, kind, requestID, digest string,
	affected, recomputed, removed []string) error {
	a, _ := json.Marshal(affected)
	r, _ := json.Marshal(recomputed)
	d, _ := json.Marshal(removed)
	_, err := tx.ExecContext(ctx, `INSERT INTO commits(version,kind,request_id,parent_version,created_at,
		affected_json,recomputed_json,removed_json,policy_digest) VALUES(?,?,?,?,?,?,?,?,?)`,
		version, kind, requestID, parent, nowRFC3339(), string(a), string(r), string(d), digest)
	return err
}

func sortedSet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func (s *Store) SubmitSnapshot(ctx context.Context, req SnapshotRequest) (*MutationResponse, *BusinessError) {
	if be := validateRequestID(req.RequestID); be != nil {
		return nil, be
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if it, err := s.replayIntent(ctx, req.RequestID); err != nil {
		return nil, NewError(CodeInternal, "cannot check request idempotency", 500, map[string]any{"error": err.Error()})
	} else if it != nil {
		if it.Kind != "snapshot" {
			return nil, NewError(CodeRequestConflict, "request_id belongs to another operation kind", 409, map[string]any{
				"request_id": req.RequestID, "existing_kind": it.Kind,
			})
		}
		if be := intentReplayError(it); be != nil {
			return nil, be
		}
		var out MutationResponse
		if err := json.Unmarshal(it.ResponseJSON, &out); err != nil {
			return nil, NewError(CodeInternal, "cannot decode idempotent response", 500, map[string]any{"error": err.Error()})
		}
		return &out, nil
	}
	if err := s.insertIntent(ctx, req.RequestID, "snapshot", 0, req.Version, req); err != nil {
		return nil, NewError(CodeRequestConflict, "request_id already exists", 409, map[string]any{"request_id": req.RequestID})
	}
	current, err := s.CurrentVersion(ctx)
	if err != nil {
		be := NewError(CodeInternal, "cannot read current version", 500, map[string]any{"error": err.Error()})
		s.failIntent(ctx, req.RequestID, be)
		return nil, be
	}
	if req.Version <= current {
		be := staleSnapshot(req.Version, current)
		s.failIntent(ctx, req.RequestID, be)
		return nil, be
	}
	_, policy, biz := NormalizeAndValidate(req.Queues)
	if biz != nil {
		s.failIntent(ctx, req.RequestID, biz)
		return nil, biz
	}
	eff := ComputeEffectiveRates(policy)
	recomputed := SortedQueueIDs(policy)
	affected := append([]string(nil), recomputed...)
	digest := PolicyDigest(policy)
	committedAt := nowRFC3339()
	resp := &MutationResponse{
		Code: CodeOK, Kind: "snapshot", Version: req.Version, AffectedQueueIDs: affected,
		RecomputedQueueIDs: recomputed, RemovedQueueIDs: []string{}, UnchangedQueueIDs: []string{},
		CommittedAt: committedAt, PreviousVersion: current, PolicyDigest: digest,
		DerivedInvalidated: len(recomputed) > 0,
	}
	if err := s.setIntentPhase(ctx, req.RequestID, "snapshot_rebuild"); err != nil {
		be := NewError(CodeInternal, "cannot record intent phase", 500, map[string]any{"error": err.Error()})
		s.failIntent(ctx, req.RequestID, be)
		return nil, be
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		be := NewError(CodeInternal, "cannot begin snapshot transaction", 500, map[string]any{"error": err.Error()})
		s.failIntent(ctx, req.RequestID, be)
		return nil, be
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	if _, err := tx.ExecContext(ctx, `DELETE FROM queues`); err != nil {
		return nil, s.txFailure(ctx, req.RequestID, tx, "cannot replace queues", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM queue_views`); err != nil {
		return nil, s.txFailure(ctx, req.RequestID, tx, "cannot replace queue views", err)
	}
	for _, id := range recomputed {
		q := policy[id]
		if err := insertQueueTx(ctx, tx, q, req.Version); err != nil {
			return nil, s.txFailure(ctx, req.RequestID, tx, "cannot insert queue", err)
		}
		if err := insertDerivedTx(ctx, tx, req.Version, makeDerived(q, eff[id], policy, req.Version, req.Version)); err != nil {
			return nil, s.txFailure(ctx, req.RequestID, tx, "cannot rebuild derived structures", err)
		}
	}
	if err := insertCommitTx(ctx, tx, req.Version, current, "snapshot", req.RequestID, digest, affected, recomputed, nil); err != nil {
		return nil, s.txFailure(ctx, req.RequestID, tx, "cannot insert commit record", err)
	}
	if err := setCurrentVersionTx(ctx, tx, req.Version); err != nil {
		return nil, s.txFailure(ctx, req.RequestID, tx, "cannot advance current version", err)
	}
	if err := completeIntentTx(ctx, tx, req.RequestID, req.Version, resp, 200); err != nil {
		return nil, s.txFailure(ctx, req.RequestID, tx, "cannot complete intent", err)
	}
	s.maybeFailpoint("derived", req.RequestID)
	if err := tx.Commit(); err != nil {
		be := NewError(CodeInternal, "snapshot commit failed", 500, map[string]any{"error": err.Error()})
		s.failIntent(ctx, req.RequestID, be)
		return nil, be
	}
	rollback = false
	return resp, nil
}

func (s *Store) txFailure(ctx context.Context, requestID string, tx *sql.Tx, message string, cause error) *BusinessError {
	_ = tx.Rollback()
	be := NewError(CodeInternal, message, 500, map[string]any{"error": cause.Error()})
	s.failIntent(ctx, requestID, be)
	return be
}

func (s *Store) SubmitIncrement(ctx context.Context, req IncrementRequest) (*MutationResponse, *BusinessError) {
	if be := validateRequestID(req.RequestID); be != nil {
		return nil, be
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if it, err := s.replayIntent(ctx, req.RequestID); err != nil {
		return nil, NewError(CodeInternal, "cannot check request idempotency", 500, map[string]any{"error": err.Error()})
	} else if it != nil {
		if it.Kind != "increment" {
			return nil, NewError(CodeRequestConflict, "request_id belongs to another operation kind", 409, map[string]any{
				"request_id": req.RequestID, "existing_kind": it.Kind,
			})
		}
		if be := intentReplayError(it); be != nil {
			return nil, be
		}
		var out MutationResponse
		if err := json.Unmarshal(it.ResponseJSON, &out); err != nil {
			return nil, NewError(CodeInternal, "cannot decode idempotent response", 500, map[string]any{"error": err.Error()})
		}
		return &out, nil
	}
	if req.BaseVersion < 0 || req.BaseVersion == int64(^uint64(0)>>1) {
		return nil, policyInvalid("base_version is outside the supported range", map[string]any{"base_version": req.BaseVersion})
	}
	target := req.BaseVersion + 1
	if err := s.insertIntent(ctx, req.RequestID, "increment", req.BaseVersion, target, req); err != nil {
		return nil, NewError(CodeRequestConflict, "request_id already exists", 409, map[string]any{"request_id": req.RequestID})
	}
	current, err := s.CurrentVersion(ctx)
	if err != nil {
		be := NewError(CodeInternal, "cannot read current version", 500, map[string]any{"error": err.Error()})
		s.failIntent(ctx, req.RequestID, be)
		return nil, be
	}
	if req.BaseVersion != current {
		be := staleBase(req.BaseVersion, current)
		s.failIntent(ctx, req.RequestID, be)
		return nil, be
	}
	if current == 0 {
		be := NewError(CodeNoPolicy, "no policy has been committed", 404, map[string]any{"base_version": req.BaseVersion})
		s.failIntent(ctx, req.RequestID, be)
		return nil, be
	}
	before, err := loadPolicyDB(ctx, s.db)
	if err != nil {
		be := NewError(CodeInternal, "cannot load current policy", 500, map[string]any{"error": err.Error()})
		s.failIntent(ctx, req.RequestID, be)
		return nil, be
	}
	after, cascadeSeeds, selfSeeds, dbChanged, removed, biz := applyIncrement(before, req)
	if biz != nil {
		s.failIntent(ctx, req.RequestID, biz)
		return nil, biz
	}
	normalized, policy, biz := NormalizeAndValidate(queueSlice(after))
	if biz != nil {
		s.failIntent(ctx, req.RequestID, biz)
		return nil, biz
	}
	_ = normalized
	eff := ComputeEffectiveRates(policy)
	recomputedSet := map[string]bool{}
	for seed := range cascadeSeeds {
		if _, exists := policy[seed]; !exists {
			continue
		}
		recomputedSet[seed] = true
		for _, d := range Descendants(policy, seed) {
			recomputedSet[d] = true
		}
	}
	for id := range selfSeeds {
		if _, exists := policy[id]; exists {
			recomputedSet[id] = true
		}
	}
	recomputed := sortedSet(recomputedSet)
	removedIDs := sortedSet(removed)
	affectedSet := map[string]bool{}
	for _, id := range recomputed {
		affectedSet[id] = true
	}
	for _, id := range removedIDs {
		affectedSet[id] = true
	}
	affected := sortedSet(affectedSet)
	unchanged := make([]string, 0)
	for _, id := range SortedQueueIDs(policy) {
		if !recomputedSet[id] {
			unchanged = append(unchanged, id)
		}
	}
	digest := PolicyDigest(policy)
	resp := &MutationResponse{
		Code: CodeOK, Kind: "increment", Version: target, BaseVersion: req.BaseVersion,
		AffectedQueueIDs: affected, RecomputedQueueIDs: recomputed, RemovedQueueIDs: removedIDs,
		UnchangedQueueIDs: unchanged, CommittedAt: nowRFC3339(), PreviousVersion: current,
		PolicyDigest: digest, DerivedInvalidated: len(affected) > 0,
	}
	if err := s.setIntentPhase(ctx, req.RequestID, "increment_rebuild"); err != nil {
		be := NewError(CodeInternal, "cannot record intent phase", 500, map[string]any{"error": err.Error()})
		s.failIntent(ctx, req.RequestID, be)
		return nil, be
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		be := NewError(CodeInternal, "cannot begin increment transaction", 500, map[string]any{"error": err.Error()})
		s.failIntent(ctx, req.RequestID, be)
		return nil, be
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	txCurrent, err := currentVersionTx(ctx, tx)
	if err != nil {
		return nil, s.txFailure(ctx, req.RequestID, tx, "cannot recheck current version", err)
	}
	if txCurrent != req.BaseVersion {
		_ = tx.Rollback()
		be := staleBase(req.BaseVersion, txCurrent)
		s.failIntent(ctx, req.RequestID, be)
		return nil, be
	}
	for _, id := range removedIDs {
		if _, err := tx.ExecContext(ctx, `DELETE FROM queues WHERE queue_id=?`, id); err != nil {
			return nil, s.txFailure(ctx, req.RequestID, tx, "cannot delete queue", err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM queue_views WHERE queue_id=?`, id); err != nil {
			return nil, s.txFailure(ctx, req.RequestID, tx, "cannot delete queue view", err)
		}
	}
	for _, id := range sortedSet(dbChanged) {
		if q, ok := policy[id]; ok {
			if err := insertQueueTx(ctx, tx, q, target); err != nil {
				return nil, s.txFailure(ctx, req.RequestID, tx, "cannot update queue", err)
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE queue_views SET valid_through_version=?`, target); err != nil {
		return nil, s.txFailure(ctx, req.RequestID, tx, "cannot stamp unaffected views", err)
	}
	for _, id := range recomputed {
		if err := insertDerivedTx(ctx, tx, target, makeDerived(policy[id], eff[id], policy, target, target)); err != nil {
			return nil, s.txFailure(ctx, req.RequestID, tx, "cannot rebuild affected derived structures", err)
		}
	}
	if err := insertCommitTx(ctx, tx, target, req.BaseVersion, "increment", req.RequestID, digest, affected, recomputed, removedIDs); err != nil {
		return nil, s.txFailure(ctx, req.RequestID, tx, "cannot insert commit record", err)
	}
	if err := setCurrentVersionTx(ctx, tx, target); err != nil {
		return nil, s.txFailure(ctx, req.RequestID, tx, "cannot advance current version", err)
	}
	if err := completeIntentTx(ctx, tx, req.RequestID, target, resp, 200); err != nil {
		return nil, s.txFailure(ctx, req.RequestID, tx, "cannot complete intent", err)
	}
	s.maybeFailpoint("derived", req.RequestID)
	if err := tx.Commit(); err != nil {
		be := NewError(CodeInternal, "increment commit failed", 500, map[string]any{"error": err.Error()})
		s.failIntent(ctx, req.RequestID, be)
		return nil, be
	}
	rollback = false
	return resp, nil
}

func queueSlice(policy map[string]Queue) []Queue {
	ids := SortedQueueIDs(policy)
	out := make([]Queue, 0, len(ids))
	for _, id := range ids {
		out = append(out, policy[id])
	}
	return out
}

func applyIncrement(before map[string]Queue, req IncrementRequest) (map[string]Queue, map[string]bool, map[string]bool, map[string]bool, map[string]bool, *BusinessError) {
	after := ClonePolicy(before)
	cascadeSeeds := map[string]bool{}
	selfSeeds := map[string]bool{}
	dbChanged := map[string]bool{}
	removed := map[string]bool{}
	patchSeen := map[string]bool{}
	parentSeen := map[string]bool{}
	deleteSeen := map[string]bool{}

	deletes := make([]string, 0, len(req.DeleteQueues))
	for _, raw := range req.DeleteQueues {
		id := strings.TrimSpace(raw)
		if id == "" || deleteSeen[id] {
			return nil, nil, nil, nil, nil, policyInvalid("delete_queue_ids contains an empty or duplicate id", map[string]any{"queue_id": id})
		}
		deleteSeen[id] = true
		if _, ok := after[id]; !ok {
			return nil, nil, nil, nil, nil, policyInvalid("cannot delete a queue that does not exist", map[string]any{"queue_id": id})
		}
		deletes = append(deletes, id)
	}
	for _, id := range deletes {
		if patchSeen[id] || parentSeen[id] {
			return nil, nil, nil, nil, nil, policyInvalid("a deleted queue cannot also be patched", map[string]any{"queue_id": id})
		}
		removed[id] = true
		for _, d := range Descendants(before, id) {
			removed[d] = true
		}
	}
	for id := range removed {
		delete(after, id)
	}

	for i, p := range req.QueuePatches {
		id := strings.TrimSpace(p.QueueID)
		if id == "" || patchSeen[id] {
			return nil, nil, nil, nil, nil, policyInvalid("queue_patches contains an empty or duplicate queue_id", map[string]any{
				"queue_id": id, "patch_index": i,
			})
		}
		patchSeen[id] = true
		if deleteSeen[id] || removed[id] {
			return nil, nil, nil, nil, nil, policyInvalid("a deleted queue cannot also be patched", map[string]any{"queue_id": id})
		}
		q, exists := after[id]
		if !exists {
			missing := []string{}
			if p.CIRBPS == nil {
				missing = append(missing, "cir_bps")
			}
			if p.PIRBPS == nil {
				missing = append(missing, "pir_bps")
			}
			if p.CBSBytes == nil {
				missing = append(missing, "cbs_bytes")
			}
			if p.PBSBytes == nil {
				missing = append(missing, "pbs_bytes")
			}
			if p.Priority == nil {
				missing = append(missing, "priority")
			}
			if p.Match == nil {
				missing = append(missing, "match")
			}
			if len(missing) > 0 {
				return nil, nil, nil, nil, nil, policyInvalid("new queue patch must include all declared fields", map[string]any{
					"queue_id": id, "missing_fields": missing,
				})
			}
			q = Queue{ID: id}
			exists = false
		}
		old := cloneQueue(q)
		if p.CIRBPS != nil {
			q.CIRBPS = *p.CIRBPS
		}
		if p.PIRBPS != nil {
			q.PIRBPS = *p.PIRBPS
		}
		if p.CBSBytes != nil {
			q.CBSBytes = *p.CBSBytes
		}
		if p.PBSBytes != nil {
			q.PBSBytes = *p.PBSBytes
		}
		if p.Priority != nil {
			q.Priority = *p.Priority
		}
		if p.Match != nil {
			q.Match = *p.Match
			q.Match.DSCPValues = append([]int(nil), p.Match.DSCPValues...)
		}
		after[id] = q
		dbChanged[id] = true
		if !exists {
			cascadeSeeds[id] = true
		} else {
			if q.CIRBPS != old.CIRBPS || q.PIRBPS != old.PIRBPS || q.Priority != old.Priority {
				cascadeSeeds[id] = true
			}
			if q.CBSBytes != old.CBSBytes || q.PBSBytes != old.PBSBytes || !matchEqual(q.Match, old.Match) {
				selfSeeds[id] = true
			}
		}
	}
	for i, p := range req.ParentChanges {
		id := strings.TrimSpace(p.QueueID)
		if id == "" || parentSeen[id] {
			return nil, nil, nil, nil, nil, policyInvalid("parent_changes contains an empty or duplicate queue_id", map[string]any{
				"queue_id": id, "change_index": i,
			})
		}
		parentSeen[id] = true
		if deleteSeen[id] || removed[id] {
			return nil, nil, nil, nil, nil, policyInvalid("a deleted queue cannot change parent", map[string]any{"queue_id": id})
		}
		q, ok := after[id]
		if !ok {
			return nil, nil, nil, nil, nil, policyInvalid("parent_changes references a queue that does not exist", map[string]any{"queue_id": id})
		}
		oldParent := ""
		if q.ParentID != nil {
			oldParent = *q.ParentID
		}
		newParent := ""
		if p.ParentID != nil {
			newParent = strings.TrimSpace(*p.ParentID)
			if newParent == "" {
				return nil, nil, nil, nil, nil, policyInvalid("parent_id must be null or non-empty", map[string]any{"queue_id": id})
			}
			v := newParent
			q.ParentID = &v
		} else {
			q.ParentID = nil
		}
		after[id] = q
		dbChanged[id] = true
		if oldParent != newParent {
			cascadeSeeds[id] = true
		}
	}
	return after, cascadeSeeds, selfSeeds, dbChanged, removed, nil
}

func matchEqual(a, b MatchRule) bool {
	if a.SrcPortMin != b.SrcPortMin || a.SrcPortMax != b.SrcPortMax || len(a.DSCPValues) != len(b.DSCPValues) {
		return false
	}
	for i := range a.DSCPValues {
		if a.DSCPValues[i] != b.DSCPValues[i] {
			return false
		}
	}
	return true
}
