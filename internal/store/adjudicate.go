package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gatewayfreshness.local/workbench/internal/freshness"
)

type nonceRow struct {
	ClientID          string
	Nonce             string
	FirstOccupiedAtMS int64
	WindowMS          int64
	ExpiresAtMS       int64
	Weight            int64
	FirstBatchID      string
	FirstRequestID    string
	WindowVersion     int64
	ConfigVersion     string
}

func (s *Store) Adjudicate(ctx context.Context, batch *freshness.ValidatedBatch) (*freshness.BatchDecision, bool, error) {
	if batch == nil {
		return nil, false, freshness.NewAppError(freshness.CodeRequestInvalid, "nil batch", nil)
	}
	if !s.IsRecovered() {
		if _, err := s.Recover(ctx); err != nil {
			return nil, false, fmt.Errorf("recover before adjudication: %w", err)
		}
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	existing, err := s.GetDecision(ctx, batch.BatchID)
	if err != nil && !errors.Is(err, ErrBatchNotFound) {
		return nil, false, err
	}
	if existing != nil {
		if existing.ContentHash != batch.ContentHash {
			return nil, false, freshness.NewAppError(freshness.CodeConflictingRequest,
				"batch_id was already committed with different content", map[string]any{
					"batch_id":                 batch.BatchID,
					"persisted_window_version": existing.WindowVersion,
					"persisted_content_hash":   existing.ContentHash,
					"submitted_content_hash":   batch.ContentHash,
				})
		}
		existing.IdempotentReplay = true
		return existing, true, nil
	}

	intentID, plannedVersion, err := s.createIntent(ctx, batch)
	if err != nil {
		return nil, false, err
	}
	crashNow(s.crashPoint)

	decision, err := s.commitDecision(ctx, intentID, plannedVersion, batch)
	if err != nil {
		s.markIntentAborted(intentID, err.Error())
		// A failed COMMIT can be ambiguous. If the decision is present, return the
		// durable result rather than reporting a phantom failure.
		if durable, getErr := s.GetDecision(context.Background(), batch.BatchID); getErr == nil && durable != nil {
			if durable.ContentHash != batch.ContentHash {
				return nil, false, freshness.NewAppError(freshness.CodeConflictingRequest, "batch_id was already committed with different content", nil)
			}
			durable.IdempotentReplay = true
			return durable, true, nil
		}
		return nil, false, err
	}
	return decision, false, nil
}

func (s *Store) createIntent(ctx context.Context, batch *freshness.ValidatedBatch) (int64, int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("begin intent transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var lastCommitted, maxPlanned int64
	if err := tx.QueryRowContext(ctx, `SELECT last_committed_version FROM server_state WHERE id=1`).Scan(&lastCommitted); err != nil {
		return 0, 0, fmt.Errorf("read server version: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(planned_window_version),0) FROM intents`).Scan(&maxPlanned); err != nil {
		return 0, 0, fmt.Errorf("read intent version: %w", err)
	}
	planned := lastCommitted
	if maxPlanned > planned {
		planned = maxPlanned
	}
	planned++
	now := time.Now().UnixMilli()
	detail, _ := json.Marshal(map[string]any{
		"request_count":     len(batch.Requests),
		"reference_time_ms": batch.ReferenceTimeMS,
		"mode":              batch.Mode,
		"total_weight":      batch.TotalWeight,
	})
	res, err := tx.ExecContext(ctx, `INSERT INTO intents(batch_id,planned_window_version,config_version,content_hash,mode,state,detail_json,created_at_ms,updated_at_ms)
		VALUES(?,?,?,?,?,'pending',?,?,?)`,
		batch.BatchID, planned, s.opt.ConfigVersion, batch.ContentHash, batch.Mode, string(detail), now, now)
	if err != nil {
		return 0, 0, fmt.Errorf("insert intent: %w", err)
	}
	intentID, err := res.LastInsertId()
	if err != nil {
		return 0, 0, fmt.Errorf("intent id: %w", err)
	}
	for _, r := range batch.Requests {
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO intent_nonce_refs(intent_id,client_id,nonce) VALUES(?,?,?)`,
			intentID, r.ClientID, r.Nonce); err != nil {
			return 0, 0, fmt.Errorf("insert intent nonce reference: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, fmt.Errorf("commit intent record: %w", err)
	}
	return intentID, planned, nil
}

func (s *Store) commitDecision(ctx context.Context, intentID, plannedVersion int64, batch *freshness.ValidatedBatch) (*freshness.BatchDecision, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin decision transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Cross-process race guard: a second process may have committed this batch.
	if existing, err := s.getDecisionTx(ctx, tx, batch.BatchID); err != nil {
		return nil, err
	} else if existing != nil {
		if existing.ContentHash != batch.ContentHash {
			return nil, freshness.NewAppError(freshness.CodeConflictingRequest, "batch_id was already committed with different content", nil)
		}
		existing.IdempotentReplay = true
		return existing, nil
	}

	if err := s.reclaimExpiredNonces(ctx, tx, intentID, plannedVersion, batch.BatchID, batch.ReferenceTimeMS); err != nil {
		return nil, err
	}

	checkedLimit := len(batch.Requests)
	precheckMethod := "all_fields"
	if batch.Mode == freshness.ModeFast {
		checkedLimit = s.opt.FastPrecheckLimit
		if checkedLimit > len(batch.Requests) {
			checkedLimit = len(batch.Requests)
		}
		precheckMethod = "deterministic_prefix_v1"
	}

	items := make([]freshness.ItemDecision, 0, len(batch.Requests))
	failures := 0
	skipped := 0
	firstFailureCode := ""
	for i, r := range batch.Requests {
		prechecked := i < checkedLimit
		item := freshness.ItemDecision{
			Ordinal:                i,
			RequestID:              r.RequestID,
			TimestampMS:            r.TimestampMS,
			RouteKey:               r.RouteKey,
			ClientID:               r.ClientID,
			Nonce:                  r.Nonce,
			Weight:                 r.Weight,
			ToleranceMS:            r.ToleranceMS,
			WindowMS:               r.WindowMS,
			Prechecked:             prechecked,
			BoundaryInclusive:      true,
			WindowVersion:          plannedVersion,
			ToleranceConfigVersion: s.opt.ConfigVersion,
			TriggeredBoundary:      freshness.BoundaryNone,
		}
		if prechecked {
			item.PrecheckSelection = "fast_prefix"
			if batch.Mode == freshness.ModeFull {
				item.PrecheckSelection = "full_mode_all_fields"
			}
			item.SignatureStage = freshness.StageResult{
				WindowVersion: plannedVersion, ToleranceConfigVersion: s.opt.ConfigVersion,
				Details: map[string]any{"algorithm": "HMAC-SHA256", "key_id": s.opt.SigningKeyID},
			}
			signatureOK := freshness.VerifySignature(s.opt.SigningKey, r)
			if signatureOK {
				item.SignatureStage.Status = freshness.StagePassed
				item.SignatureStage.Code = freshness.CodeOK
			} else {
				item.SignatureStage.Status = freshness.StageFailed
				item.SignatureStage.Code = freshness.CodeSignatureMismatch
			}

			offset, boundary, timestampCode, lower, upper := freshness.JudgeTimestamp(r.TimestampMS, batch.ReferenceTimeMS, r.ToleranceMS)
			item.OffsetMS = offset
			item.TriggeredBoundary = boundary
			item.ToleranceLowerBoundMS = lower
			item.ToleranceUpperBoundMS = upper
			item.TimestampStage = freshness.StageResult{
				Status: freshness.StagePassed, Code: freshness.CodeOK,
				WindowVersion: plannedVersion, ToleranceConfigVersion: s.opt.ConfigVersion,
				Details: map[string]any{
					"offset_ms":      offset,
					"lower_bound_ms": lower,
					"upper_bound_ms": upper,
					"boundary_rule":  freshness.ToleranceBoundaryRule,
				},
			}
			if timestampCode != freshness.CodeOK {
				item.TimestampStage.Status = freshness.StageFailed
				item.TimestampStage.Code = timestampCode
				item.TriggeredBoundary = boundary
			}

			item.NonceStage = freshness.StageResult{
				WindowVersion: plannedVersion, ToleranceConfigVersion: s.opt.ConfigVersion,
			}
			finalCode := freshness.CodeOK
			switch {
			case !signatureOK:
				finalCode = freshness.CodeSignatureMismatch
				item.NonceStage.Status = freshness.StageSkipped
				item.NonceStage.Code = "BLOCKED_BY_SIGNATURE"
				item.NonceStage.Details = map[string]any{"reason": "signature did not pass; nonce was not read or occupied"}
			case timestampCode != freshness.CodeOK:
				finalCode = timestampCode
				item.NonceStage.Status = freshness.StageSkipped
				item.NonceStage.Code = "BLOCKED_BY_TIMESTAMP"
				item.NonceStage.Details = map[string]any{"reason": "timestamp outside inclusive window; nonce was not read or occupied"}
			default:
				row, lookupErr := s.lookupNonce(ctx, tx, r.ClientID, r.Nonce)
				if lookupErr != nil {
					return nil, lookupErr
				}
				if row != nil {
					finalCode = freshness.CodeNonceReplayed
					remaining := row.ExpiresAtMS - batch.ReferenceTimeMS
					if remaining < 0 {
						remaining = 0
					}
					first := row.FirstOccupiedAtMS
					rem := remaining
					item.FirstOccupiedAtMS = &first
					item.RemainingWindowMS = &rem
					item.ReplayBatchID = row.FirstBatchID
					item.ReplayRequestID = row.FirstRequestID
					item.NonceStage.Status = freshness.StageFailed
					item.NonceStage.Code = freshness.CodeNonceReplayed
					item.NonceStage.Details = map[string]any{
						"first_occupied_at_ms": row.FirstOccupiedAtMS,
						"expires_at_ms":        row.ExpiresAtMS,
						"remaining_window_ms":  remaining,
						"first_batch_id":       row.FirstBatchID,
						"first_request_id":     row.FirstRequestID,
					}
				} else {
					first := batch.ReferenceTimeMS
					expires := batch.ReferenceTimeMS + r.WindowMS
					if expires < first {
						return nil, fmt.Errorf("nonce expiry overflow for request %s", r.RequestID)
					}
					if err := s.occupyNonce(ctx, tx, plannedVersion, batch.BatchID, r, first, expires); err != nil {
						return nil, err
					}
					rem := r.WindowMS
					item.FirstOccupiedAtMS = &first
					item.RemainingWindowMS = &rem
					item.NonceStage.Status = freshness.StagePassed
					item.NonceStage.Code = freshness.CodeOK
					item.NonceStage.Details = map[string]any{
						"first_occupied_at_ms": first,
						"expires_at_ms":        expires,
						"remaining_window_ms":  r.WindowMS,
					}
				}
			}
			if finalCode == freshness.CodeOK {
				item.Result = freshness.ResultAccepted
				item.Code = freshness.CodeOK
				item.Message = "request accepted"
			} else {
				item.Result = freshness.ResultRejected
				item.Code = finalCode
				item.Message = itemMessage(finalCode)
				failures++
				if firstFailureCode == "" {
					firstFailureCode = finalCode
				}
			}
		} else {
			skipped++
			item.Result = freshness.ResultNotPrechecked
			item.Code = freshness.CodePrecheckIncomplete
			item.Message = "fast mode did not precheck this request; it is not a pass"
			item.PrecheckSelection = "outside_deterministic_prefix"
			item.SignatureStage = freshness.StageResult{Status: freshness.StageNotPrechecked, Code: freshness.CodePrecheckIncomplete, WindowVersion: plannedVersion, ToleranceConfigVersion: s.opt.ConfigVersion}
			item.TimestampStage = freshness.StageResult{Status: freshness.StageNotPrechecked, Code: freshness.CodePrecheckIncomplete, WindowVersion: plannedVersion, ToleranceConfigVersion: s.opt.ConfigVersion}
			item.NonceStage = freshness.StageResult{Status: freshness.StageNotPrechecked, Code: freshness.CodePrecheckIncomplete, WindowVersion: plannedVersion, ToleranceConfigVersion: s.opt.ConfigVersion}
			// The offset is a pure calculation and is included for explainability,
			// but no signature or nonce conclusion is implied.
			offset, _, _, lower, upper := freshness.JudgeTimestamp(r.TimestampMS, batch.ReferenceTimeMS, r.ToleranceMS)
			item.OffsetMS, item.ToleranceLowerBoundMS, item.ToleranceUpperBoundMS = offset, lower, upper
		}
		items = append(items, item)
		if err := s.insertItemAudit(ctx, tx, batch.BatchID, plannedVersion, item); err != nil {
			return nil, err
		}
	}

	outcome, code, message, confidence, confidenceNote, sampleRate := summarize(batch.Mode, len(batch.Requests), checkedLimit, failures, skipped, firstFailureCode)
	now := time.Now().UnixMilli()
	decision := &freshness.BatchDecision{
		BatchID: batch.BatchID, Mode: batch.Mode, ContentHash: batch.ContentHash,
		ReferenceTimeMS: batch.ReferenceTimeMS, ReferenceSource: batch.ReferenceSource,
		WindowVersion: plannedVersion, ToleranceConfigVersion: s.opt.ConfigVersion,
		Outcome: outcome, Code: code, Message: message,
		RequestCount: len(batch.Requests), PrecheckedCount: checkedLimit,
		TotalWeight: batch.TotalWeight, SampleRate: sampleRate, PrecheckLimit: s.opt.FastPrecheckLimit,
		PrecheckMethod: precheckMethod, Confidence: confidence, ConfidenceNote: confidenceNote,
		BoundaryRule: freshness.ToleranceBoundaryRule, Items: items, CommittedAtMS: now,
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO batches(
		batch_id,content_hash,mode,reference_time_ms,reference_source,window_version,config_version,outcome,code,message,
		request_count,prechecked_count,precheck_limit,total_weight,sample_rate,precheck_method,confidence,confidence_note,boundary_rule,committed_at_ms)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		decision.BatchID, decision.ContentHash, decision.Mode, decision.ReferenceTimeMS, decision.ReferenceSource,
		decision.WindowVersion, decision.ToleranceConfigVersion, decision.Outcome, decision.Code, decision.Message,
		decision.RequestCount, decision.PrecheckedCount, decision.PrecheckLimit, decision.TotalWeight,
		decision.SampleRate, decision.PrecheckMethod, decision.Confidence, decision.ConfidenceNote,
		decision.BoundaryRule, decision.CommittedAtMS); err != nil {
		return nil, fmt.Errorf("insert batch decision: %w", err)
	}
	for i := range items {
		itemJSON, _ := json.Marshal(items[i])
		if _, err := tx.ExecContext(ctx, `INSERT INTO batch_items(batch_id,ordinal,request_id,client_id,nonce,result,code,window_version,config_version,item_json)
			VALUES(?,?,?,?,?,?,?,?,?,?)`,
			batch.BatchID, items[i].Ordinal, items[i].RequestID, items[i].ClientID, items[i].Nonce,
			items[i].Result, items[i].Code, plannedVersion, s.opt.ConfigVersion, string(itemJSON)); err != nil {
			return nil, fmt.Errorf("insert batch item: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE server_state SET last_committed_version=?,last_reference_time_ms=?,last_config_version=?,last_committed_at_ms=? WHERE id=1`,
		plannedVersion, batch.ReferenceTimeMS, s.opt.ConfigVersion, now); err != nil {
		return nil, fmt.Errorf("update server state: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET state='committed',updated_at_ms=? WHERE id=? AND state='pending'`, now, intentID); err != nil {
		return nil, fmt.Errorf("mark intent committed: %w", err)
	}
	crashNow(s.crashPoint)
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit decision transaction: %w", err)
	}
	return decision, nil
}

func (s *Store) reclaimExpiredNonces(ctx context.Context, tx *sql.Tx, intentID, version int64, batchID string, referenceMS int64) error {
	rows, err := tx.QueryContext(ctx, `SELECT client_id,nonce,first_occupied_at_ms,window_ms,expires_at_ms,weight,first_batch_id,first_request_id,window_version,config_version
		FROM nonces WHERE expires_at_ms <= ? ORDER BY client_id,nonce`, referenceMS)
	if err != nil {
		return fmt.Errorf("select expired nonces: %w", err)
	}
	expired := make([]nonceRow, 0)
	for rows.Next() {
		var n nonceRow
		if err := rows.Scan(&n.ClientID, &n.Nonce, &n.FirstOccupiedAtMS, &n.WindowMS, &n.ExpiresAtMS, &n.Weight, &n.FirstBatchID, &n.FirstRequestID, &n.WindowVersion, &n.ConfigVersion); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan expired nonce: %w", err)
		}
		expired = append(expired, n)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, n := range expired {
		var busy int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM intent_nonce_refs r
			JOIN intents i ON i.id=r.intent_id
			WHERE i.state='pending' AND i.id<>? AND r.client_id=? AND r.nonce=?`,
			intentID, n.ClientID, n.Nonce).Scan(&busy); err != nil {
			return fmt.Errorf("check in-flight nonce reference: %w", err)
		}
		if busy > 0 {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO nonce_events(client_id,nonce,event_type,at_ms,first_occupied_at_ms,expires_at_ms,window_ms,weight,batch_id,request_id,window_version,config_version)
			VALUES(?,?,'reclaim',?,?,?,?,?,?,?,?,?)`,
			n.ClientID, n.Nonce, referenceMS, n.FirstOccupiedAtMS, n.ExpiresAtMS, n.WindowMS, n.Weight, batchID, "", version, s.opt.ConfigVersion); err != nil {
			return fmt.Errorf("insert nonce reclaim event: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM nonces WHERE client_id=? AND nonce=?`, n.ClientID, n.Nonce); err != nil {
			return fmt.Errorf("delete reclaimed nonce: %w", err)
		}
	}
	return nil
}

func (s *Store) lookupNonce(ctx context.Context, tx *sql.Tx, clientID, nonce string) (*nonceRow, error) {
	var n nonceRow
	err := tx.QueryRowContext(ctx, `SELECT client_id,nonce,first_occupied_at_ms,window_ms,expires_at_ms,weight,first_batch_id,first_request_id,window_version,config_version
		FROM nonces WHERE client_id=? AND nonce=?`, clientID, nonce).Scan(
		&n.ClientID, &n.Nonce, &n.FirstOccupiedAtMS, &n.WindowMS, &n.ExpiresAtMS, &n.Weight, &n.FirstBatchID, &n.FirstRequestID, &n.WindowVersion, &n.ConfigVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup nonce: %w", err)
	}
	return &n, nil
}

func (s *Store) occupyNonce(ctx context.Context, tx *sql.Tx, version int64, batchID string, r freshness.Request, first, expires int64) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO nonces(client_id,nonce,first_occupied_at_ms,window_ms,expires_at_ms,weight,first_batch_id,first_request_id,window_version,config_version)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		r.ClientID, r.Nonce, first, r.WindowMS, expires, r.Weight, batchID, r.RequestID, version, s.opt.ConfigVersion); err != nil {
		return fmt.Errorf("occupy nonce: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO nonce_events(client_id,nonce,event_type,at_ms,first_occupied_at_ms,expires_at_ms,window_ms,weight,batch_id,request_id,window_version,config_version)
		VALUES(?,?,'occupy',?,?,?,?,?,?,?,?,?)`,
		r.ClientID, r.Nonce, first, first, expires, r.WindowMS, r.Weight, batchID, r.RequestID, version, s.opt.ConfigVersion); err != nil {
		return fmt.Errorf("insert nonce occupy event: %w", err)
	}
	return nil
}

func (s *Store) insertItemAudit(ctx context.Context, tx *sql.Tx, batchID string, version int64, item freshness.ItemDecision) error {
	stages := []struct {
		name  string
		stage freshness.StageResult
	}{
		{"signature", item.SignatureStage},
		{"timestamp", item.TimestampStage},
		{"nonce", item.NonceStage},
		{"result", freshness.StageResult{Status: item.Result, Code: item.Code, WindowVersion: version, ToleranceConfigVersion: s.opt.ConfigVersion}},
	}
	for _, st := range stages {
		details, _ := json.Marshal(st.stage)
		if _, err := tx.ExecContext(ctx, `INSERT INTO audit_trail(batch_id,window_version,config_version,ordinal,request_id,stage,outcome,code,at_ms,details_json)
			VALUES(?,?,?,?,?,?,?,?,?,?)`,
			batchID, version, s.opt.ConfigVersion, item.Ordinal, item.RequestID, st.name,
			st.stage.Status, st.stage.Code, item.TimestampMS, string(details)); err != nil {
			return fmt.Errorf("insert audit trail: %w", err)
		}
	}
	return nil
}

func (s *Store) markIntentAborted(intentID int64, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	detail, _ := json.Marshal(map[string]string{"abort_reason": reason})
	_, _ = s.db.ExecContext(ctx, `UPDATE intents SET state='aborted',updated_at_ms=?,detail_json=? WHERE id=? AND state='pending'`,
		time.Now().UnixMilli(), string(detail), intentID)
}

func summarize(mode string, requestCount, checked, failures, skipped int, firstFailure string) (outcome, code, message, confidence, confidenceNote string, sampleRate float64) {
	if requestCount == 0 {
		return freshness.OutcomeRejected, freshness.CodeRequestInvalid, "empty batch", "invalid", "no requests", 0
	}
	sampleRate = float64(checked) / float64(requestCount)
	switch {
	case failures > 0:
		outcome = freshness.OutcomeRejected
		code = firstFailure
		message = "at least one prechecked request failed"
		if mode == freshness.ModeFast && skipped > 0 {
			confidence = "exact_failure_in_prefix"
			confidenceNote = "a failure was proven inside the deterministic prefix; unchecked suffix requests were not treated as accepted"
		} else {
			confidence = "exact_failure"
			confidenceNote = "all requests were checked and at least one failed"
		}
	case skipped > 0:
		outcome = freshness.OutcomePartial
		code = freshness.CodePrecheckIncomplete
		message = "fast prechecks passed for the prefix; unchecked requests are explicitly indeterminate"
		confidence = "bounded_partial"
		confidenceNote = fmt.Sprintf("%d of %d requests were checked by deterministic prefix; skipped requests are never represented as pass", checked, requestCount)
	default:
		outcome = freshness.OutcomeAccepted
		code = freshness.CodeOK
		message = "all requests accepted under one committed version"
		confidence = "exact"
		confidenceNote = "all signatures, timestamps, and nonces were checked in one write transaction"
	}
	return
}

func itemMessage(code string) string {
	switch code {
	case freshness.CodeSignatureMismatch:
		return "HMAC-SHA256 signature does not match the canonical request fields"
	case freshness.CodeTimestampTooOld:
		return "request timestamp is earlier than the inclusive lower tolerance boundary"
	case freshness.CodeTimestampAhead:
		return "request timestamp is later than the inclusive upper tolerance boundary"
	case freshness.CodeNonceReplayed:
		return "nonce is already occupied for this client in the active deduplication window"
	default:
		return "request rejected"
	}
}
