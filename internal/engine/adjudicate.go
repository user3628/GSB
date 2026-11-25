package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"nonceguard/internal/canonical"
	"nonceguard/internal/model"
)

type AdjudicateOptions struct {
	ConfigVersion int64  `json:"config_version,omitempty"`
	ReferenceMs   int64  `json:"reference_ms,omitempty"`
	RecordDelayMs int64  `json:"record_delay_ms,omitempty"`
	Fault         string `json:"fault,omitempty"`
}

type ItemResult struct {
	Index                  int            `json:"index"`
	RequestID              string         `json:"request_id"`
	IntentID               string         `json:"intent_id,omitempty"`
	BatchID                string         `json:"batch_id,omitempty"`
	Code                   string         `json:"code"`
	Status                 string         `json:"status"`
	Accepted               bool           `json:"accepted"`
	Provisional            bool           `json:"provisional,omitempty"`
	ConfigVersion          int64          `json:"config_version"`
	ReferenceMs            int64          `json:"reference_ms"`
	OffsetMs               *int64         `json:"offset_ms,omitempty"`
	BoundaryMs             *int64         `json:"boundary_ms,omitempty"`
	FirstSeenMs            *int64         `json:"first_seen_ms,omitempty"`
	RemainingWindowMs      *int64         `json:"remaining_window_ms,omitempty"`
	ExpiresAtMs            *int64         `json:"expires_at_ms,omitempty"`
	IdempotentReuse        bool           `json:"idempotent_reuse,omitempty"`
	ReusedFromIndex        int            `json:"reused_from_index,omitempty"`
	Recycled               bool           `json:"recycled,omitempty"`
	OccupancyConfigVersion int64          `json:"occupancy_config_version,omitempty"`
	ContentHash            string         `json:"content_hash"`
	Message                string         `json:"message"`
	CanonicalString        string         `json:"canonical_string,omitempty"`
	Fast                   *FastDetail    `json:"fast,omitempty"`
	Context                map[string]any `json:"context,omitempty"`
}

type BatchResult struct {
	BatchID          string              `json:"batch_id"`
	Mode             string              `json:"mode"`
	ConfigVersion    int64               `json:"config_version"`
	Reference        referenceResolution `json:"reference"`
	Total            int                 `json:"total"`
	Accepted         int                 `json:"accepted"`
	Rejected         int                 `json:"rejected"`
	IdempotentReuses int                 `json:"idempotent_reuses"`
	StartedAtMs      int64               `json:"started_at_ms"`
	FinishedAtMs     int64               `json:"finished_at_ms"`
	BoundaryPolicy   map[string]string   `json:"boundary_policy"`
	Results          []ItemResult        `json:"results"`
	Sampling         *SamplingReport     `json:"sampling,omitempty"`
	Comparison       *ComparisonReport   `json:"comparison,omitempty"`
}

type priorCall struct {
	hash   string
	result ItemResult
}

func (e *Engine) AdjudicateBatch(ctx context.Context, batch model.Batch, opts AdjudicateOptions) (*BatchResult, *Failure) {
	e.mu.Lock()
	defer e.mu.Unlock()

	started := e.Clock()
	cfg, ferr := e.loadConfigForWrite(ctx, opts.ConfigVersion)
	if ferr != nil {
		return nil, ferr
	}
	ref, err := e.resolveReference(ctx, firstNonZero(opts.ReferenceMs, batch.ReferenceMs), false)
	if err != nil {
		return nil, NewFailure(model.CodeInternal, "cannot resolve monotonic reference", map[string]any{"error": err.Error()})
	}

	now := e.Clock()
	e.attemptSeq++
	attemptID := e.attemptSeq
	tx, err := e.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, internalFailure("cannot begin intent transaction", err)
	}
	intentIDs := make([]string, len(batch.Requests))
	optionsJSON, _ := json.Marshal(opts)
	requestJSON, _ := json.Marshal(batch)
	_, err = tx.ExecContext(ctx, `INSERT INTO batch_jobs(batch_id,config_version,reference_ms,mode,status,total,processed,request_json,options_json,created_at_ms,updated_at_ms)
		VALUES(?,?,?, 'full','processing',?,0,?,?,?,?)
		ON CONFLICT(batch_id) DO UPDATE SET config_version=excluded.config_version, reference_ms=excluded.reference_ms, mode='full',status='processing',total=excluded.total,processed=0,request_json=excluded.request_json,options_json=excluded.options_json,updated_at_ms=excluded.updated_at_ms`,
		batch.BatchID, cfg.Version, ref.ReferenceMs, len(batch.Requests), string(requestJSON), string(optionsJSON), now, now)
	if err != nil {
		tx.Rollback()
		return nil, internalFailure("cannot persist batch job", err)
	}
	for i, req := range batch.Requests {
		intentID := fmt.Sprintf("intent:%s:%s-a%d:%d:%s", batch.BatchID, e.attemptPrefix, attemptID, i, req.RequestID)
		intentIDs[i] = intentID
		payload, _ := json.Marshal(map[string]any{"index": i, "request": req})
		_, err := tx.ExecContext(ctx, `INSERT INTO intents(intent_id,intent_type,batch_id,request_id,config_version,reference_ms,request_hash,status,payload_json,created_at_ms,updated_at_ms)
			VALUES(?, 'request_adjudication', ?,?,?,?,?, 'open', ?,?,?)
			ON CONFLICT(intent_id) DO UPDATE SET status='open', updated_at_ms=excluded.updated_at_ms`,
			intentID, batch.BatchID, req.RequestID, cfg.Version, ref.ReferenceMs, req.ContentHash(), string(payload), now, now)
		if err != nil {
			tx.Rollback()
			return nil, internalFailure("cannot write adjudication intent", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, internalFailure("cannot commit intent records", err)
	}

	if opts.Fault == "batch_interrupt" {
		return nil, NewFailure(model.CodeBatchInterrupted, "batch interrupted after durable intent write; no partial adjudication transaction was committed", map[string]any{
			"batch_id":        batch.BatchID,
			"config_version":  cfg.Version,
			"reference_ms":    ref.ReferenceMs,
			"open_intents":    len(intentIDs),
			"recovery_action": "call POST /v1/recovery/run or restart, then resubmit the same fixed batch",
		})
	}

	if opts.RecordDelayMs > 0 {
		if err := sleepCtx(ctx, time.Duration(opts.RecordDelayMs)*time.Millisecond); err != nil {
			return nil, NewFailure(model.CodeBatchInterrupted, "batch cancelled before atomic adjudication", map[string]any{"batch_id": batch.BatchID, "error": err.Error()})
		}
	}

	tx, err = e.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, internalFailure("cannot begin adjudication transaction", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE clock_state SET high_water_ms=?, updated_at_ms=? WHERE id=1`, ref.ReferenceMs, e.Clock()); err != nil {
		return nil, internalFailure("cannot advance monotonic reference", err)
	}

	out := &BatchResult{
		BatchID:        batch.BatchID,
		Mode:           model.ModeFull,
		ConfigVersion:  cfg.Version,
		Reference:      ref,
		Total:          len(batch.Requests),
		StartedAtMs:    started,
		BoundaryPolicy: model.BoundaryDescription(),
		Results:        make([]ItemResult, 0, len(batch.Requests)),
	}
	local := map[string]priorCall{}

	for i, req := range batch.Requests {
		if opts.RecordDelayMs > 0 {
			if err := sleepCtx(ctx, time.Duration(opts.RecordDelayMs)*time.Millisecond); err != nil {
				return nil, NewFailure(model.CodeBatchInterrupted, "batch cancelled during record processing; open intents remain recoverable", map[string]any{"batch_id": batch.BatchID, "after_index": i, "error": err.Error()})
			}
		}
		result := e.adjudicateOne(ctx, tx, cfg, ref.ReferenceMs, batch.BatchID, i, req, intentIDs[i], local)
		if result.Code == model.CodeInternal {
			_ = tx.Rollback()
			return nil, NewFailure(model.CodeInternal, "adjudication transaction failed and was rolled back", map[string]any{
				"batch_id": batch.BatchID, "index": i, "request_id": req.RequestID, "error": result.Message,
			})
		}
		out.Results = append(out.Results, result)
		if result.Accepted {
			out.Accepted++
		}
		if result.Code != model.CodeOK {
			out.Rejected++
		}
		if result.IdempotentReuse {
			out.IdempotentReuses++
		}
		codeJSON, _ := json.Marshal(result)
		if _, err := tx.ExecContext(ctx, `INSERT INTO batch_items(batch_id,batch_index,request_id,code,idempotent_reuse,result_json)
			VALUES(?,?,?,?,?,?) ON CONFLICT(batch_id,batch_index) DO UPDATE SET request_id=excluded.request_id,code=excluded.code,idempotent_reuse=excluded.idempotent_reuse,result_json=excluded.result_json`,
			batch.BatchID, i, req.RequestID, result.Code, boolToInt(result.IdempotentReuse), string(codeJSON)); err != nil {
			return nil, internalFailure("cannot persist batch item", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE batch_jobs SET status='completed',processed=total,updated_at_ms=? WHERE batch_id=?`, e.Clock(), batch.BatchID); err != nil {
		return nil, internalFailure("cannot complete batch job", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, internalFailure("cannot commit atomic batch adjudication", err)
	}
	out.FinishedAtMs = e.Clock()
	return out, nil
}

func (e *Engine) loadConfigForWrite(ctx context.Context, version int64) (model.Snapshot, *Failure) {
	if version > 0 {
		cfg, err := e.GetConfig(ctx, version)
		if err != nil {
			return model.Snapshot{}, NewFailure(model.CodeNotFound, "requested configuration version was not found", map[string]any{"config_version": version})
		}
		return cfg, nil
	}
	cfg, err := e.ActiveConfig(ctx)
	if err != nil {
		return model.Snapshot{}, NewFailure(model.CodeInternal, "active configuration is unavailable", map[string]any{"error": err.Error()})
	}
	return cfg, nil
}

func (e *Engine) adjudicateOne(
	ctx context.Context,
	tx *sql.Tx,
	cfg model.Snapshot,
	referenceMs int64,
	batchID string,
	index int,
	req model.Request,
	intentID string,
	local map[string]priorCall,
) ItemResult {
	base := ItemResult{
		Index:         index,
		RequestID:     req.RequestID,
		IntentID:      intentID,
		BatchID:       batchID,
		ConfigVersion: cfg.Version,
		ReferenceMs:   referenceMs,
		ContentHash:   req.ContentHash(),
	}
	finish := func(code, status, message string, accepted bool) ItemResult {
		base.Code = code
		base.Status = status
		base.Message = message
		base.Accepted = accepted
		return base
	}
	internal := func(err error) ItemResult {
		return finish(model.CodeInternal, "error", err.Error(), false)
	}

	if prior, ok := local[req.RequestID]; ok {
		if prior.hash != base.ContentHash {
			_, err := tx.ExecContext(ctx, `INSERT INTO conflict_attempts(request_id,batch_id,provided_hash,stored_hash,detail_json,created_at_ms)
				VALUES(?,?,?,?,?,?)`, req.RequestID, batchID, base.ContentHash, prior.hash, `{"source":"same_batch"}`, e.Clock())
			if err != nil {
				return internal(err)
			}
			if err := e.completeIntent(ctx, tx, intentID); err != nil {
				return internal(err)
			}
			return finish(model.CodeConflictingRequest, model.StatusRejected, "same request_id was first seen with different content in this batch", false)
		}
		reused := prior.result
		reused.Index = index
		reused.BatchID = batchID
		reused.IntentID = intentID
		reused.IdempotentReuse = true
		reused.ReusedFromIndex = prior.result.Index
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='reused',updated_at_ms=? WHERE intent_id=?`, e.Clock(), intentID); err != nil {
			return internal(err)
		}
		return reused
	}

	var storedHash, storedBatch, storedCode string
	var storedVersion int64
	err := tx.QueryRowContext(ctx, `SELECT content_hash,batch_id,config_version,decision_code FROM request_registry WHERE request_id=?`, req.RequestID).
		Scan(&storedHash, &storedBatch, &storedVersion, &storedCode)
	if err == nil {
		if storedHash != base.ContentHash {
			_, err := tx.ExecContext(ctx, `INSERT INTO conflict_attempts(request_id,batch_id,provided_hash,stored_hash,detail_json,created_at_ms)
				VALUES(?,?,?,?,?,?)`, req.RequestID, batchID, base.ContentHash, storedHash, fmt.Sprintf(`{"stored_batch_id":%q}`, storedBatch), e.Clock())
			if err != nil {
				return internal(err)
			}
			if err := e.completeIntent(ctx, tx, intentID); err != nil {
				return internal(err)
			}
			r := finish(model.CodeConflictingRequest, model.StatusRejected, "request_id already exists with different content", false)
			r.Context = map[string]any{"stored_batch_id": storedBatch, "stored_config_version": storedVersion, "original_code": storedCode}
			return r
		}
		decision, derr := scanDecision(ctx, tx, `SELECT `+decisionColumns+` FROM decisions WHERE request_id=?`, req.RequestID)
		if derr != nil {
			return internal(derr)
		}
		decision.Index = index
		decision.BatchID = batchID
		decision.IntentID = intentID
		decision.IdempotentReuse = true
		decision.Message = "identical request_id/content already adjudicated; reused committed result"
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='reused',updated_at_ms=? WHERE intent_id=?`, e.Clock(), intentID); err != nil {
			return internal(err)
		}
		local[req.RequestID] = priorCall{hash: base.ContentHash, result: decision}
		return decision
	} else if !errors.Is(err, sql.ErrNoRows) {
		return internal(err)
	}

	canonicalString, err := canonical.RequestString(req, cfg.SignatureFields)
	if err != nil {
		return internal(err)
	}
	base.CanonicalString = canonicalString
	signatureValid, err := canonical.VerifyRequest(req, req.SignatureDigest, cfg.SignatureFields)
	if err != nil {
		return internal(err)
	}
	expectedDigest, _ := canonical.SignRequest(req, cfg.SignatureFields)
	if _, err := tx.ExecContext(ctx, `INSERT INTO signature_checks(request_id,intent_id,batch_id,config_version,algorithm,checked,valid,canonical_string,provided_digest,expected_digest,created_at_ms)
		VALUES(?,?,?,?, 'hmac-sha256/length-prefixed-v1',1,?,?,?,?,?)`,
		req.RequestID, intentID, batchID, cfg.Version, boolToInt(signatureValid), canonicalString, req.SignatureDigest, expectedDigest, e.Clock()); err != nil {
		return internal(err)
	}
	if !signatureValid {
		result := finish(model.CodeSignatureMismatch, model.StatusRejected, "HMAC deterministic digest does not match normalized fields", false)
		result.Context = map[string]any{"signature_fields": cfg.SignatureFields, "expected_digest": expectedDigest}
		return e.persistTerminal(ctx, tx, cfg, referenceMs, batchID, intentID, req, result, local)
	}

	offset := req.TimestampMs - referenceMs
	base.OffsetMs = &offset
	if offset < cfg.MinOffsetMs {
		boundary := cfg.MinOffsetMs
		base.BoundaryMs = &boundary
		result := finish(model.CodeTimestampEarly, model.StatusRejected, "request timestamp is earlier than the inclusive lower tolerance boundary", false)
		result.Context = map[string]any{"touched_boundary": "min_offset_ms", "inclusion": "offset >= min_offset_ms is allowed"}
		return e.persistTerminal(ctx, tx, cfg, referenceMs, batchID, intentID, req, result, local)
	}
	if offset > cfg.MaxOffsetMs {
		boundary := cfg.MaxOffsetMs
		base.BoundaryMs = &boundary
		result := finish(model.CodeTimestampLate, model.StatusRejected, "request timestamp is later than the inclusive upper tolerance boundary", false)
		result.Context = map[string]any{"touched_boundary": "max_offset_ms", "inclusion": "offset <= max_offset_ms is allowed"}
		return e.persistTerminal(ctx, tx, cfg, referenceMs, batchID, intentID, req, result, local)
	}

	var occVersion, firstSeen, storedWindow, expiresAt int64
	var occIntent, occBatch string
	err = tx.QueryRowContext(ctx, `SELECT config_version,first_seen_ms,window_ms,expires_at_ms,intent_id,batch_id FROM nonce_occupancy WHERE client_id=? AND nonce=?`, req.ClientID, req.Nonce).
		Scan(&occVersion, &firstSeen, &storedWindow, &expiresAt, &occIntent, &occBatch)
	if err == nil {
		currentExpiry := firstSeen + cfg.NonceWindowMs
		if referenceMs < currentExpiry {
			remaining := currentExpiry - referenceMs
			base.FirstSeenMs = &firstSeen
			base.ExpiresAtMs = &currentExpiry
			base.RemainingWindowMs = &remaining
			base.OccupancyConfigVersion = occVersion
			result := finish(model.CodeNonceReplayed, model.StatusRejected, "client_id+nonce was already occupied inside the half-open window", false)
			result.FirstSeenMs = base.FirstSeenMs
			result.ExpiresAtMs = base.ExpiresAtMs
			result.RemainingWindowMs = base.RemainingWindowMs
			result.OccupancyConfigVersion = occVersion
			result.Context = map[string]any{
				"window_ms": cfg.NonceWindowMs, "occupancy_window_ms_at_first_use": storedWindow,
				"expiry_rule":        "[first_seen_ms, first_seen_ms + config.nonce_window_ms); right endpoint exclusive",
				"occupancy_batch_id": occBatch, "expires_at_persisted_ms": expiresAt, "occupancy_intent_id": occIntent,
			}
			return e.persistTerminal(ctx, tx, cfg, referenceMs, batchID, intentID, req, result, local)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM nonce_occupancy WHERE client_id=? AND nonce=?`, req.ClientID, req.Nonce); err != nil {
			return internal(err)
		}
		base.Recycled = true
		base.Context = map[string]any{"recycled_first_seen_ms": firstSeen, "recycled_expiry_ms": currentExpiry, "previous_occupancy_config_version": occVersion}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return internal(err)
	}

	newExpires := referenceMs + cfg.NonceWindowMs
	if _, err := tx.ExecContext(ctx, `INSERT INTO nonce_occupancy(client_id,nonce,intent_id,batch_id,request_id,config_version,first_seen_ms,window_ms,expires_at_ms,recycled_from_ms,created_at_ms)
		VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		req.ClientID, req.Nonce, intentID, batchID, req.RequestID, cfg.Version, referenceMs, cfg.NonceWindowMs, newExpires, recycledFrom(base.Context), e.Clock()); err != nil {
		return internal(err)
	}
	first := referenceMs
	remaining := cfg.NonceWindowMs
	base.FirstSeenMs = &first
	base.ExpiresAtMs = &newExpires
	base.RemainingWindowMs = &remaining
	result := finish(model.CodeOK, model.StatusAccepted, "signature, inclusive offset interval, and nonce first-use check passed", true)
	result.FirstSeenMs = base.FirstSeenMs
	result.ExpiresAtMs = base.ExpiresAtMs
	result.RemainingWindowMs = base.RemainingWindowMs
	result.Recycled = base.Recycled
	result.Context = base.Context
	return e.persistTerminal(ctx, tx, cfg, referenceMs, batchID, intentID, req, result, local)
}

func (e *Engine) persistTerminal(
	ctx context.Context,
	tx *sql.Tx,
	cfg model.Snapshot,
	referenceMs int64,
	batchID, intentID string,
	req model.Request,
	result ItemResult,
	local map[string]priorCall,
) ItemResult {
	local[req.RequestID] = priorCall{hash: result.ContentHash, result: result}
	detail, _ := json.Marshal(result.Context)
	_, err := tx.ExecContext(ctx, `INSERT INTO decisions(request_id,intent_id,batch_id,batch_index,config_version,reference_ms,mode,code,status,accepted,offset_ms,boundary_ms,first_seen_ms,remaining_window_ms,expires_at_ms,idempotent_reuse,recycled,content_hash,detail_json,decided_at_ms)
		VALUES(?,?,?,?,?,?, 'full',?,?,?,?,?,?,?,?,0,?,?,?,?)`,
		req.RequestID, intentID, batchID, result.Index, cfg.Version, referenceMs, result.Code, result.Status, boolToInt(result.Accepted),
		result.OffsetMs, result.BoundaryMs, result.FirstSeenMs, result.RemainingWindowMs, result.ExpiresAtMs, boolToInt(result.Recycled), result.ContentHash, string(detail), e.Clock())
	if err != nil {
		result.Code = model.CodeInternal
		result.Message = err.Error()
		return result
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO request_registry(request_id,content_hash,batch_id,config_version,decision_code,created_at_ms)
		VALUES(?,?,?,?,?,?) ON CONFLICT(request_id) DO NOTHING`,
		req.RequestID, result.ContentHash, batchID, cfg.Version, result.Code, e.Clock())
	if err != nil {
		result.Code = model.CodeInternal
		result.Message = err.Error()
		return result
	}
	if err := e.completeIntent(ctx, tx, intentID); err != nil {
		result.Code = model.CodeInternal
		result.Message = err.Error()
	}
	return result
}

func (e *Engine) completeIntent(ctx context.Context, tx *sql.Tx, intentID string) error {
	_, err := tx.ExecContext(ctx, `UPDATE intents SET status='committed',updated_at_ms=? WHERE intent_id=?`, e.Clock(), intentID)
	return err
}

func internalFailure(message string, err error) *Failure {
	return NewFailure(model.CodeInternal, message, map[string]any{"error": err.Error()})
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func firstNonZero(values ...int64) int64 {
	for _, v := range values {
		if v != 0 {
			return v
		}
	}
	return 0
}

func recycledFrom(ctx map[string]any) int64 {
	if ctx == nil {
		return 0
	}
	if v, ok := ctx["recycled_first_seen_ms"].(int64); ok {
		return v
	}
	return 0
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

const decisionColumns = `request_id,intent_id,batch_id,batch_index,config_version,reference_ms,mode,code,status,accepted,offset_ms,boundary_ms,first_seen_ms,remaining_window_ms,expires_at_ms,idempotent_reuse,recycled,content_hash,detail_json,decided_at_ms`

type rowScanner interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func scanDecision(ctx context.Context, q rowScanner, query string, args ...any) (ItemResult, error) {
	var d ItemResult
	var mode string
	var accepted, reuse, recycled int
	var offset, boundary, firstSeen, remaining, expires sql.NullInt64
	var detailJSON string
	var decidedAt int64
	err := q.QueryRowContext(ctx, query, args...).Scan(
		&d.RequestID, &d.IntentID, &d.BatchID, &d.Index, &d.ConfigVersion, &d.ReferenceMs, &mode,
		&d.Code, &d.Status, &accepted, &offset, &boundary, &firstSeen, &remaining, &expires,
		&reuse, &recycled, &d.ContentHash, &detailJSON, &decidedAt)
	if err != nil {
		return ItemResult{}, err
	}
	d.Accepted = accepted == 1
	d.IdempotentReuse = reuse == 1
	d.Recycled = recycled == 1
	assignNullInt(&d.OffsetMs, offset)
	assignNullInt(&d.BoundaryMs, boundary)
	assignNullInt(&d.FirstSeenMs, firstSeen)
	assignNullInt(&d.RemainingWindowMs, remaining)
	assignNullInt(&d.ExpiresAtMs, expires)
	if detailJSON != "" && detailJSON != "{}" {
		var m map[string]any
		if json.Unmarshal([]byte(detailJSON), &m) == nil {
			d.Context = m
		}
	}
	return d, nil
}

func assignNullInt(target **int64, v sql.NullInt64) {
	if v.Valid {
		x := v.Int64
		*target = &x
	}
}
