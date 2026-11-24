package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
)

type ErrorPayload struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context"`
}

type APIResponse struct {
	OK            bool          `json:"ok"`
	RequestID     string        `json:"request_id,omitempty"`
	LedgerVersion int64         `json:"ledger_version"`
	Data          any           `json:"data,omitempty"`
	Error         *ErrorPayload `json:"error,omitempty"`
}

type StoredResponse struct {
	Status        int
	Body          []byte
	LedgerVersion int64
}

type WriteOutcome struct {
	Data          any
	LedgerVersion int64
	StateChanged  bool
}

type writeFunc func(tx *sql.Tx, meta Meta, newVersion int64) (WriteOutcome, error)

func (s *Store) ExecuteWrite(ctx context.Context, kind, requestID string, payload any, fn writeFunc) (StoredResponse, error) {
	if !idPattern.MatchString(requestID) {
		return StoredResponse{}, Biz(CodeBadRequest, "request_id is required and must use safe identifier characters", map[string]any{"request_id": requestID})
	}
	payloadDigest, err := CanonicalJSONDigest(payload)
	if err != nil {
		return StoredResponse{}, internalErrorf("digest request payload: %v", err)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	var existingKind, existingDigest, existingBody string
	var existingStatus int
	var existingVersion int64
	err = s.db.QueryRowContext(ctx, `SELECT kind, payload_digest, http_status, response_json, ledger_version FROM requests WHERE request_id=?`, requestID).
		Scan(&existingKind, &existingDigest, &existingStatus, &existingBody, &existingVersion)
	if err == nil {
		if existingKind != kind || existingDigest != payloadDigest {
			be := Biz(CodeConflictingRequest, "request_id was already used with different content", map[string]any{
				"request_id":  requestID,
				"stored_kind": existingKind,
			})
			return s.errorResponse(requestID, 0, be)
		}
		return StoredResponse{Status: existingStatus, Body: []byte(existingBody), LedgerVersion: existingVersion}, nil
	}
	if err != sql.ErrNoRows {
		return StoredResponse{}, err
	}

	meta, err := s.CurrentMeta(ctx, s.db)
	if err != nil {
		return StoredResponse{}, err
	}
	newVersion := meta.Version + 1
	intentID := newIntentID(requestID)
	intentDetail, _ := json.Marshal(map[string]any{"target_ledger_version": newVersion, "kind": kind})
	if _, err := s.db.ExecContext(ctx, `INSERT INTO intents(intent_id, request_id, kind, payload_digest, target_ledger_version, status, phase, detail_json, created_ms, updated_ms)
		VALUES(?,?,?,?,?,'pending','intent_recorded',?,?,?)`, intentID, requestID, kind, payloadDigest, newVersion, string(intentDetail), s.nowMS(), s.nowMS()); err != nil {
		return StoredResponse{}, fmt.Errorf("persist write-ahead intent: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		_ = s.abortIntent(ctx, intentID, "begin transaction failed")
		return StoredResponse{}, err
	}
	outcome, fnErr := fn(tx, meta, newVersion)
	if fnErr != nil {
		_ = tx.Rollback()
		be := AsBiz(fnErr)
		return s.persistErrorResponse(ctx, intentID, requestID, kind, payloadDigest, meta.Version, be)
	}
	version := outcome.LedgerVersion
	if version == 0 {
		version = meta.Version
	}
	success := APIResponse{OK: true, RequestID: requestID, LedgerVersion: version, Data: outcome.Data}
	body, err := json.Marshal(success)
	if err != nil {
		_ = tx.Rollback()
		be := NewBizError(CodeInternal, err.Error(), http.StatusInternalServerError, nil)
		return s.persistErrorResponse(ctx, intentID, requestID, kind, payloadDigest, meta.Version, be)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO requests(request_id, kind, payload_digest, http_status, response_json, ledger_version, created_ms)
		VALUES(?,?,?,?,?,?,?)`, requestID, kind, payloadDigest, http.StatusOK, string(body), version, s.nowMS()); err != nil {
		_ = tx.Rollback()
		return StoredResponse{}, err
	}
	phase := "committed_without_state_change"
	if outcome.StateChanged {
		phase = "committed_projection"
	} else {
		version = meta.Version
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='committed', phase=?, committed_ledger_version=?, detail_json=?, updated_ms=? WHERE intent_id=?`,
		phase, version, string(intentDetail), s.nowMS(), intentID); err != nil {
		_ = tx.Rollback()
		return StoredResponse{}, err
	}
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		if v, ok, findErr := s.findCommittedRequestVersion(ctx, requestID); findErr == nil && ok {
			return StoredResponse{Status: http.StatusOK, Body: body, LedgerVersion: v}, nil
		}
		return StoredResponse{}, fmt.Errorf("commit write transaction: %w", err)
	}
	return StoredResponse{Status: http.StatusOK, Body: body, LedgerVersion: version}, nil
}

func (s *Store) findCommittedRequestVersion(ctx context.Context, requestID string) (int64, bool, error) {
	var v int64
	err := s.db.QueryRowContext(ctx, `SELECT ledger_version FROM ledger_versions WHERE request_id=?`, requestID).Scan(&v)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return v, true, nil
}

func (s *Store) abortIntent(ctx context.Context, intentID, reason string) error {
	detail, _ := json.Marshal(map[string]any{"reason": reason})
	_, err := s.db.ExecContext(ctx, `UPDATE intents SET status='aborted', phase='aborted_before_commit', detail_json=?, updated_ms=? WHERE intent_id=? AND status IN ('pending','processing')`, string(detail), s.nowMS(), intentID)
	return err
}

func (s *Store) persistErrorResponse(ctx context.Context, intentID, requestID, kind, payloadDigest string, version int64, be *BizError) (StoredResponse, error) {
	if be == nil {
		be = NewBizError(CodeInternal, "unknown error", http.StatusInternalServerError, nil)
	}
	status := be.HTTPStatus
	if status == 0 {
		status = statusForBizCode(be.Code)
	}
	envelope := APIResponse{OK: false, RequestID: requestID, LedgerVersion: version, Error: &ErrorPayload{Code: be.Code, Message: be.Message, Context: be.Context}}
	body, err := json.Marshal(envelope)
	if err != nil {
		return StoredResponse{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return StoredResponse{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO requests(request_id, kind, payload_digest, http_status, response_json, ledger_version, created_ms)
		VALUES(?,?,?,?,?,?,?)`, requestID, kind, payloadDigest, status, string(body), version, s.nowMS()); err != nil {
		return StoredResponse{}, err
	}
	detail, _ := json.Marshal(map[string]any{"error_code": be.Code, "http_status": status})
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='aborted', phase='business_error', detail_json=?, updated_ms=? WHERE intent_id=? AND status IN ('pending','processing')`, string(detail), s.nowMS(), intentID); err != nil {
		return StoredResponse{}, err
	}
	if err := tx.Commit(); err != nil {
		return StoredResponse{}, err
	}
	return StoredResponse{Status: status, Body: body, LedgerVersion: version}, nil
}

func (s *Store) errorResponse(requestID string, version int64, be *BizError) (StoredResponse, error) {
	status := be.HTTPStatus
	if status == 0 {
		status = statusForBizCode(be.Code)
	}
	body, err := json.Marshal(APIResponse{OK: false, RequestID: requestID, LedgerVersion: version, Error: &ErrorPayload{Code: be.Code, Message: be.Message, Context: be.Context}})
	if err != nil {
		return StoredResponse{}, err
	}
	return StoredResponse{Status: status, Body: body, LedgerVersion: version}, nil
}

func (s *Store) writeProjection(ctx context.Context, tx *sql.Tx, state *LedgerState, requestID, kind, payloadDigest, reason string, affectedTenants []string) error {
	if state.StateDigest == "" {
		digest, err := StateDigest(state)
		if err != nil {
			return err
		}
		state.StateDigest = digest
	}
	stateBody, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO ledger_versions(ledger_version, request_id, kind, payload_digest, state_json, state_digest, as_of_minute, last_settled_through_ms, created_ms)
		VALUES(?,?,?,?,?,?,?,?,?)`, state.LedgerVersion, requestID, kind, payloadDigest, string(stateBody), state.StateDigest, state.AsOfMinute, state.LastSettledThroughMS, s.nowMS()); err != nil {
		return err
	}
	for _, t := range state.Tenants {
		tb, _ := json.Marshal(t)
		if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_states(ledger_version, tenant_id, pool_id, declaration_snapshot_version, base_shares_milli, credit_limit_milli, rate_tier, repayment_priority, principal_outstanding_milli, unsettled_interest_micros, available_credit_micros, state_json)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, state.LedgerVersion, t.TenantID, t.PoolID, t.DeclarationSnapshotVersion, t.BaseSharesMilli, t.CreditLimitMilli, t.RateTier, t.RepaymentPriority, t.PrincipalOutstandingMilli, t.UnsettledInterestMicros, t.AvailableCreditMicros, string(tb)); err != nil {
			return err
		}
	}
	for _, p := range state.Pools {
		pb, _ := json.Marshal(p)
		risk := 0
		if p.OvercommitRisk {
			risk = 1
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO pool_states(ledger_version, pool_id, used_ratio_fixed, used_ratio_ppm, principal_outstanding_milli, unsettled_interest_micros, cross_tenant_exposure_micros, overcommit_risk, trigger_tenant_id, state_json)
			VALUES(?,?,?,?,?,?,?,?,?,?)`, state.LedgerVersion, p.PoolID, p.UsedRatioFixed, p.UsedRatioPPM, p.PrincipalOutstandingMilli, p.UnsettledInterestMicros, p.CrossTenantExposureMicros, risk, p.TriggerTenantID, string(pb)); err != nil {
			return err
		}
	}
	seen := map[string]struct{}{}
	for _, tenantID := range affectedTenants {
		if _, ok := seen[tenantID]; ok {
			continue
		}
		seen[tenantID] = struct{}{}
		t, ok := FindTenant(state, tenantID)
		if !ok {
			continue
		}
		deps := []string{"tenant_principal_state", "tenant_unsettled_interest", "tenant_available_credit", "pool_used_ratio", "pool_overcommit_risk", "cross_tenant_exposure"}
		for _, dep := range deps {
			key := t.PoolID
			if dep[:6] == "tenant" {
				key = "tenant:" + tenantID
			}
			if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO reconstruction_audit(ledger_version, affected_tenant_id, dependent_kind, dependent_key, reason, resolved_version)
				VALUES(?,?,?,?,?,?)`, state.LedgerVersion, tenantID, dep, key, reason, state.LedgerVersion); err != nil {
				return err
			}
		}
	}
	return nil
}

type SnapshotResult struct {
	SnapshotVersion int64        `json:"snapshot_version"`
	SnapshotDigest  string       `json:"snapshot_digest"`
	EffectiveMinute int64        `json:"effective_minute"`
	State           *LedgerState `json:"state"`
}

func (s *Store) SubmitSnapshot(ctx context.Context, req SnapshotSubmission) (StoredResponse, error) {
	if req.RequestID == "" {
		return s.errorResponse("", 0, Biz(CodeBadRequest, "request_id is required", nil))
	}
	return s.ExecuteWrite(ctx, "snapshot", req.RequestID, req, func(tx *sql.Tx, meta Meta, newVersion int64) (WriteOutcome, error) {
		if req.Snapshot.Version <= 0 {
			return WriteOutcome{}, Biz(CodeLedgerInvalid, "snapshot version must be positive", map[string]any{"field": "version"})
		}
		currentSnapshotVersion, err := s.maxSnapshotVersion(ctx, tx)
		if err != nil {
			return WriteOutcome{}, err
		}
		if req.Snapshot.Version <= currentSnapshotVersion {
			return WriteOutcome{}, Biz(CodeStaleSnapshot, "snapshot version must be greater than the last committed snapshot version", map[string]any{
				"submitted_version": req.Snapshot.Version,
				"committed_version": currentSnapshotVersion,
			})
		}
		snap := req.Snapshot
		calculated, err := SnapshotDigest(snap)
		if err != nil {
			return WriteOutcome{}, Biz(CodeLedgerInvalid, "cannot canonicalize snapshot", map[string]any{"error": err.Error()})
		}
		if snap.Digest == "" {
			snap.Digest = calculated
		} else if !DigestMatches(snap.Digest, calculated) {
			return WriteOutcome{}, Biz(CodeLedgerInvalid, "snapshot digest does not match canonical content", map[string]any{
				"provided_digest":   snap.Digest,
				"calculated_digest": calculated,
			})
		}
		if err := ValidateSnapshot(snap); err != nil {
			return WriteOutcome{}, err
		}
		snaps, events, err := s.loadFacts(ctx, tx)
		if err != nil {
			return WriteOutcome{}, err
		}
		declared := make(map[string]struct{}, len(snap.Tenants))
		for _, t := range snap.Tenants {
			declared[t.ID] = struct{}{}
		}
		for _, fe := range events {
			if _, ok := declared[fe.Event.TenantID]; !ok {
				return WriteOutcome{}, Biz(CodeLedgerInvalid, "snapshot would remove a tenant that has committed events", map[string]any{
					"tenant_id": fe.Event.TenantID,
				})
			}
		}
		candidate := normalizedSnapshot(snap)
		candidate.Digest = snap.Digest
		snaps = append(snaps, FactSnapshot{Snapshot: candidate, CommitVersion: newVersion})
		buildAsOf := meta.AsOfMinute
		if meta.Version == 0 {
			if effective := MinuteOf(candidate.GeneratedAtMS); effective > buildAsOf {
				buildAsOf = effective
			}
		}
		result, err := BuildProjection(snaps, events, buildAsOf, ProjectionMeta{LedgerVersion: newVersion, LastSettledThroughMS: meta.LastSettledThroughMS}, false)
		if err != nil {
			return WriteOutcome{}, err
		}
		affected := make([]string, 0, len(result.State.Tenants))
		for _, t := range result.State.Tenants {
			affected = append(affected, t.TenantID)
		}
		if err := s.writeProjection(ctx, tx, result.State, req.RequestID, "snapshot", "", "snapshot_or_rate_tier_change", affected); err != nil {
			return WriteOutcome{}, err
		}
		body, _ := json.Marshal(candidate)
		if _, err := tx.ExecContext(ctx, `INSERT INTO snapshots(snapshot_version, generated_at_ms, digest, body_json, effective_minute, commit_version)
			VALUES(?,?,?,?,?,?)`, candidate.Version, candidate.GeneratedAtMS, candidate.Digest, string(body), MinuteOf(candidate.GeneratedAtMS), newVersion); err != nil {
			return WriteOutcome{}, err
		}
		data := SnapshotResult{SnapshotVersion: candidate.Version, SnapshotDigest: candidate.Digest, EffectiveMinute: MinuteOf(candidate.GeneratedAtMS), State: result.State}
		return WriteOutcome{Data: data, LedgerVersion: newVersion, StateChanged: true}, nil
	})
}

type EventIngestResult struct {
	NewEventCount       int          `json:"new_event_count"`
	DuplicateSettleSeqs []string     `json:"duplicate_settle_seqs"`
	AsOfMinute          int64        `json:"as_of_minute"`
	State               *LedgerState `json:"state"`
}

func (s *Store) SubmitEvents(ctx context.Context, req EventSubmission) (StoredResponse, error) {
	if req.RequestID == "" {
		return s.errorResponse("", 0, Biz(CodeBadRequest, "request_id is required", nil))
	}
	return s.ExecuteWrite(ctx, "events", req.RequestID, req, func(tx *sql.Tx, meta Meta, newVersion int64) (WriteOutcome, error) {
		if len(req.Events) == 0 {
			return WriteOutcome{}, Biz(CodeEventInvalid, "event batch must not be empty", map[string]any{"request_id": req.RequestID})
		}
		snaps, events, err := s.loadFacts(ctx, tx)
		if err != nil {
			return WriteOutcome{}, err
		}
		existingSeq := make(map[string]string, len(events))
		lastPersisted := make(map[string]int64)
		for _, fe := range events {
			existingSeq[fe.Event.SettleSeq] = fe.ContentDigest
			if ts, ok := lastPersisted[fe.Event.TenantID]; !ok || fe.Event.TimestampMS > ts {
				lastPersisted[fe.Event.TenantID] = fe.Event.TimestampMS
			}
		}
		newFacts := make([]FactEvent, 0, len(req.Events))
		duplicates := []string{}
		incoming := make(map[string]string)
		lastIncoming := make(map[string]int64)
		maxMinute := meta.AsOfMinute
		affectedSet := map[string]struct{}{}
		for i, ev := range req.Events {
			if err := ValidateEventShape(ev); err != nil {
				if be := AsBiz(err); be != nil {
					be.Context = mergeContext(be.Context, map[string]any{"event_index": i})
				}
				return WriteOutcome{}, err
			}
			digest, err := EventDigest(ev)
			if err != nil {
				return WriteOutcome{}, err
			}
			if priorDigest, ok := incoming[ev.SettleSeq]; ok {
				if priorDigest != digest {
					return WriteOutcome{}, Biz(CodeConflictingSettle, "the same settle_seq appears with different content in this batch", map[string]any{"settle_seq": ev.SettleSeq, "event_index": i})
				}
				duplicates = append(duplicates, ev.SettleSeq)
				continue
			}
			incoming[ev.SettleSeq] = digest
			if persistedDigest, ok := existingSeq[ev.SettleSeq]; ok {
				if persistedDigest != digest {
					return WriteOutcome{}, Biz(CodeConflictingSettle, "settle_seq already exists with different content", map[string]any{"settle_seq": ev.SettleSeq, "event_index": i})
				}
				duplicates = append(duplicates, ev.SettleSeq)
				continue
			}
			if lastTS, ok := lastPersisted[ev.TenantID]; ok && ev.TimestampMS < lastTS {
				return WriteOutcome{}, Biz(CodeEventInvalid, "tenant event timestamp regressed relative to committed events", map[string]any{
					"tenant_id": ev.TenantID, "settle_seq": ev.SettleSeq, "timestamp_ms": ev.TimestampMS, "last_committed_timestamp_ms": lastTS,
				})
			}
			if lastTS, ok := lastIncoming[ev.TenantID]; ok && ev.TimestampMS < lastTS {
				return WriteOutcome{}, Biz(CodeEventInvalid, "tenant event timestamps must be monotonic within a batch", map[string]any{
					"tenant_id": ev.TenantID, "settle_seq": ev.SettleSeq, "timestamp_ms": ev.TimestampMS, "previous_timestamp_ms": lastTS,
				})
			}
			lastIncoming[ev.TenantID] = ev.TimestampMS
			candidate := FactEvent{Event: ev, ContentDigest: digest, CommitVersion: newVersion}
			newFacts = append(newFacts, candidate)
			affectedSet[ev.TenantID] = struct{}{}
			if m := MinuteOf(ev.TimestampMS); m > maxMinute {
				maxMinute = m
			}
		}
		sort.Slice(duplicates, func(i, j int) bool { return duplicates[i] < duplicates[j] })
		if len(newFacts) == 0 {
			state, err := currentStateJSON(ctx, tx)
			if err != nil {
				return WriteOutcome{}, err
			}
			return WriteOutcome{Data: EventIngestResult{NewEventCount: 0, DuplicateSettleSeqs: duplicates, AsOfMinute: state.AsOfMinute, State: state}, LedgerVersion: meta.Version, StateChanged: false}, nil
		}
		allEvents := append(events, newFacts...)
		result, err := BuildProjection(snaps, allEvents, maxMinute, ProjectionMeta{LedgerVersion: newVersion, LastSettledThroughMS: meta.LastSettledThroughMS}, false)
		if err != nil {
			return WriteOutcome{}, err
		}
		affected := make([]string, 0, len(affectedSet))
		for id := range affectedSet {
			affected = append(affected, id)
		}
		if result.State.AsOfMinute > meta.AsOfMinute {
			for _, t := range result.State.Tenants {
				affected = append(affected, t.TenantID)
			}
		}
		sort.Strings(affected)
		if err := s.writeProjection(ctx, tx, result.State, req.RequestID, "events", "", "event_changed_tenant_and_pool_ledgers", affected); err != nil {
			return WriteOutcome{}, err
		}
		for _, fe := range newFacts {
			body, _ := json.Marshal(fe.Event)
			if _, err := tx.ExecContext(ctx, `INSERT INTO events(settle_seq, tenant_id, event_type, shares_milli, timestamp_ms, content_digest, body_json, commit_version)
				VALUES(?,?,?,?,?,?,?,?)`, fe.Event.SettleSeq, fe.Event.TenantID, fe.Event.Type, fe.Event.SharesMilli, fe.Event.TimestampMS, fe.ContentDigest, string(body), newVersion); err != nil {
				return WriteOutcome{}, err
			}
		}
		data := EventIngestResult{NewEventCount: len(newFacts), DuplicateSettleSeqs: duplicates, AsOfMinute: result.State.AsOfMinute, State: result.State}
		return WriteOutcome{Data: data, LedgerVersion: newVersion, StateChanged: true}, nil
	})
}

func mergeContext(base map[string]any, extra map[string]any) map[string]any {
	if base == nil {
		base = map[string]any{}
	}
	for k, v := range extra {
		base[k] = v
	}
	return base
}

func currentStateJSON(ctx context.Context, q queryer) (*LedgerState, error) {
	var body string
	err := q.QueryRowContext(ctx, `SELECT state_json FROM ledger_versions ORDER BY ledger_version DESC LIMIT 1`).Scan(&body)
	if err == sql.ErrNoRows {
		return &LedgerState{Tenants: []TenantState{}, Pools: []PoolState{}}, nil
	}
	if err != nil {
		return nil, err
	}
	var state LedgerState
	if err := json.Unmarshal([]byte(body), &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (s *Store) Settle(ctx context.Context, req SettlementRequest) (StoredResponse, error) {
	if req.RequestID == "" {
		return s.errorResponse("", 0, Biz(CodeBadRequest, "request_id is required", nil))
	}
	return s.ExecuteWrite(ctx, "settle", req.RequestID, req, func(tx *sql.Tx, meta Meta, newVersion int64) (WriteOutcome, error) {
		if req.ThroughMS < 0 || req.ThroughMS > MaxTimestampMS {
			return WriteOutcome{}, Biz(CodeBadRequest, "through_ms must be non-negative", map[string]any{"through_ms": req.ThroughMS})
		}
		throughMinute := MinuteOf(req.ThroughMS)
		if throughMinute < meta.AsOfMinute {
			return WriteOutcome{}, Biz(CodeStaleSettlement, "settlement cutoff precedes the current committed as-of minute", map[string]any{
				"through_minute": throughMinute, "current_as_of_minute": meta.AsOfMinute,
			})
		}
		snaps, events, err := s.loadFacts(ctx, tx)
		if err != nil {
			return WriteOutcome{}, err
		}
		result, err := BuildProjection(snaps, events, throughMinute, ProjectionMeta{LedgerVersion: newVersion, LastSettledThroughMS: req.ThroughMS}, true)
		if err != nil {
			return WriteOutcome{}, err
		}
		affected := make([]string, 0, len(result.State.Tenants))
		for _, t := range result.State.Tenants {
			affected = append(affected, t.TenantID)
		}
		if err := s.writeProjection(ctx, tx, result.State, req.RequestID, "settle", "", "interest_settlement_cascade_rebuild", affected); err != nil {
			return WriteOutcome{}, err
		}
		report := BuildSettlementReport(req, result.State, result)
		reportBody, _ := json.Marshal(report)
		if _, err := tx.ExecContext(ctx, `INSERT INTO settlement_reports(request_id, ledger_version, report_json, created_ms) VALUES(?,?,?,?)`, req.RequestID, newVersion, string(reportBody), s.nowMS()); err != nil {
			return WriteOutcome{}, err
		}
		return WriteOutcome{Data: report, LedgerVersion: newVersion, StateChanged: true}, nil
	})
}

func (s *Store) Borrow(ctx context.Context, req BorrowRequest) (StoredResponse, error) {
	if req.RequestID == "" {
		return s.errorResponse("", 0, Biz(CodeBadRequest, "request_id is required", nil))
	}
	return s.ExecuteWrite(ctx, "borrow", req.RequestID, req, func(tx *sql.Tx, meta Meta, newVersion int64) (WriteOutcome, error) {
		if req.SettleSeq == "" {
			req.SettleSeq = req.RequestID
		}
		if !settleSeqPattern.MatchString(req.SettleSeq) {
			return WriteOutcome{}, Biz(CodeEventInvalid, "settle_seq is missing or has invalid characters", map[string]any{"settle_seq": req.SettleSeq})
		}
		candidate := Event{SettleSeq: req.SettleSeq, TenantID: req.TenantID, Type: "borrow", SharesMilli: req.SharesMilli, TimestampMS: req.TimestampMS}
		if err := ValidateEventShape(candidate); err != nil {
			return WriteOutcome{}, err
		}
		snaps, events, err := s.loadFacts(ctx, tx)
		if err != nil {
			return WriteOutcome{}, err
		}
		digest, err := EventDigest(candidate)
		if err != nil {
			return WriteOutcome{}, err
		}
		var existing *FactEvent
		for i := range events {
			if events[i].Event.SettleSeq == req.SettleSeq {
				existing = &events[i]
				break
			}
		}
		if existing != nil {
			if existing.ContentDigest != digest {
				return WriteOutcome{}, Biz(CodeConflictingSettle, "borrow settle_seq already exists with different content", map[string]any{"settle_seq": req.SettleSeq})
			}
			current, err := currentStateJSON(ctx, tx)
			if err != nil {
				return WriteOutcome{}, err
			}
			ts, ok := FindTenant(current, req.TenantID)
			if !ok {
				return WriteOutcome{}, internalErrorf("duplicate borrow tenant state is missing: %s", req.TenantID)
			}
			ps, ok := FindPool(current, ts.PoolID)
			if !ok {
				return WriteOutcome{}, internalErrorf("duplicate borrow pool state is missing: %s", ts.PoolID)
			}
			return WriteOutcome{Data: BorrowDecision{
				Accepted:                  true,
				TenantID:                  req.TenantID,
				RequestedSharesMilli:      req.SharesMilli,
				RemainingCreditMicros:     ts.AvailableCreditMicros,
				ProjectedUsedMicros:       ps.UsedMicros,
				ProjectedRatioFixed:       ps.UsedRatioFixed,
				PoolOvercommit:            ps.OvercommitRisk,
				TriggerTenantID:           ps.TriggerTenantID,
				ExcessOverThresholdMicros: ps.ExcessOverThresholdMicros,
				State:                     current,
			}, LedgerVersion: meta.Version, StateChanged: false}, nil
		}
		var lastTenantTS int64 = -1
		for _, fe := range events {
			if fe.Event.TenantID == req.TenantID && fe.Event.TimestampMS > lastTenantTS {
				lastTenantTS = fe.Event.TimestampMS
			}
		}
		if lastTenantTS >= 0 && req.TimestampMS < lastTenantTS {
			return WriteOutcome{}, Biz(CodeEventInvalid, "borrow timestamp regresses relative to the tenant's committed events", map[string]any{
				"tenant_id": req.TenantID, "timestamp_ms": req.TimestampMS, "last_committed_timestamp_ms": lastTenantTS,
			})
		}
		decisionMinute := MinuteOf(req.TimestampMS)
		decisionEvents := make([]FactEvent, 0, len(events)+1)
		for _, fe := range events {
			if MinuteOf(fe.Event.TimestampMS) <= decisionMinute {
				decisionEvents = append(decisionEvents, fe)
			}
		}
		decisionEvents = append(decisionEvents, FactEvent{Event: candidate, ContentDigest: digest, CommitVersion: newVersion})
		decision, err := BuildProjection(snaps, decisionEvents, decisionMinute, ProjectionMeta{LedgerVersion: meta.Version, LastSettledThroughMS: meta.LastSettledThroughMS}, false)
		if err != nil {
			return WriteOutcome{}, err
		}
		ts, ok := FindTenant(decision.State, req.TenantID)
		if !ok {
			return WriteOutcome{}, Biz(CodeEventInvalid, "borrow tenant is not declared at the request time", map[string]any{"tenant_id": req.TenantID})
		}
		ps, ok := FindPool(decision.State, ts.PoolID)
		if !ok {
			return WriteOutcome{}, Biz(CodeLedgerInvalid, "tenant pool state is missing", map[string]any{"tenant_id": req.TenantID, "pool_id": ts.PoolID})
		}
		baseDecision := BorrowDecision{
			TenantID:                  req.TenantID,
			RequestedSharesMilli:      req.SharesMilli,
			RemainingCreditMicros:     ts.AvailableCreditMicros,
			ProjectedUsedMicros:       ps.UsedMicros,
			ProjectedRatioFixed:       ps.UsedRatioFixed,
			PoolOvercommit:            ps.OvercommitRisk,
			TriggerTenantID:           req.TenantID,
			ExcessOverThresholdMicros: ps.ExcessOverThresholdMicros,
		}
		if ps.OvercommitRisk {
			baseDecision.Code = CodePoolOvercommit
			return WriteOutcome{}, Biz(CodePoolOvercommit, "borrow would cross the pool overcommit threshold", map[string]any{
				"tenant_id":                    req.TenantID,
				"trigger_tenant_id":            req.TenantID,
				"projected_used_micros":        ps.UsedMicros,
				"threshold_limit_micros":       ps.ThresholdLimitMicros,
				"tolerance_limit_micros":       ps.ToleranceLimitMicros,
				"excess_over_threshold_micros": ps.ExcessOverThresholdMicros,
				"projected_used_ratio_fixed":   ps.UsedRatioFixed,
				"requested_shares_milli":       req.SharesMilli,
				"remaining_credit_micros":      ts.AvailableCreditMicros,
			})
		}
		var priorityGuard int64
		for _, other := range decision.State.Tenants {
			if other.PoolID != ts.PoolID || other.TenantID == ts.TenantID {
				continue
			}
			if other.RepaymentPriority < ts.RepaymentPriority {
				priorityGuard, err = checkedAddInt64(priorityGuard, other.AvailableCreditMicros, "priority guard")
				if err != nil {
					return WriteOutcome{}, err
				}
			}
		}
		capacityMicros, err := checkedSharesToMicros(ps.UsableCapacityMilli, "priority decision capacity")
		if err != nil {
			return WriteOutcome{}, err
		}
		priorityAllowed := capacityMicros - priorityGuard
		if ps.UsedMicros > priorityAllowed {
			baseDecision.Code = CodePriorityConflict
			baseDecision.PriorityConflict = true
			return WriteOutcome{}, Biz(CodePriorityConflict, "borrow would consume capacity reserved for higher repayment-priority tenants", map[string]any{
				"tenant_id":                            req.TenantID,
				"tenant_repayment_priority":            ts.RepaymentPriority,
				"higher_priority_unused_credit_micros": priorityGuard,
				"priority_allowed_micros":              priorityAllowed,
				"projected_used_micros":                ps.UsedMicros,
				"requested_shares_milli":               req.SharesMilli,
			})
		}
		fullAsOf := decisionMinute
		if meta.AsOfMinute > fullAsOf {
			fullAsOf = meta.AsOfMinute
		}
		fullEvents := append(events, FactEvent{Event: candidate, ContentDigest: digest, CommitVersion: newVersion})
		full, err := BuildProjection(snaps, fullEvents, fullAsOf, ProjectionMeta{LedgerVersion: newVersion, LastSettledThroughMS: meta.LastSettledThroughMS}, false)
		if err != nil {
			return WriteOutcome{}, err
		}
		affected := []string{req.TenantID}
		if full.State.AsOfMinute > meta.AsOfMinute {
			for _, t := range full.State.Tenants {
				affected = append(affected, t.TenantID)
			}
		}
		if err := s.writeProjection(ctx, tx, full.State, req.RequestID, "borrow", "", "borrow_decision_cascade_rebuild", affected); err != nil {
			return WriteOutcome{}, err
		}
		body, _ := json.Marshal(candidate)
		if _, err := tx.ExecContext(ctx, `INSERT INTO events(settle_seq, tenant_id, event_type, shares_milli, timestamp_ms, content_digest, body_json, commit_version)
			VALUES(?,?,?,?,?,?,?,?)`, candidate.SettleSeq, candidate.TenantID, candidate.Type, candidate.SharesMilli, candidate.TimestampMS, digest, string(body), newVersion); err != nil {
			return WriteOutcome{}, err
		}
		baseDecision.Accepted = true
		baseDecision.Code = ""
		baseDecision.State = full.State
		return WriteOutcome{Data: baseDecision, LedgerVersion: newVersion, StateChanged: true}, nil
	})
}

func (s *Store) LastState(ctx context.Context) (*LedgerState, error) {
	state, err := s.GetState(ctx, nil)
	if err != nil {
		if be := AsBiz(err); be != nil && be.Code == CodeNotFound {
			return &LedgerState{Tenants: []TenantState{}, Pools: []PoolState{}}, nil
		}
		return nil, err
	}
	return state, nil
}
