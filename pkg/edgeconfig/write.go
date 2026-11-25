package edgeconfig

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
)

type writeJob[T any] struct {
	service   *Service
	ctx       context.Context
	kind      string
	key       string
	hash      string
	intentID  string
	requestID string
	startedAt int64
}

type idempotentRecord struct {
	RequestHash      string
	Status           string
	HTTPStatus       int
	ResponseJSON     string
	ErrorCode        string
	ErrorMessage     string
	ErrorDetailsJSON sql.NullString
	Version          int64
}

func runWrite[T any](s *Service, ctx context.Context, kind, requestID string, payload any, work func(*writeJob[T]) (*T, *APIError)) (*T, error) {
	if requestID == "" {
		requestID = newRandomID("req")
	}
	key := kind + ":" + requestID
	hash, err := RequestHash(payload)
	if err != nil {
		return nil, NewAPIError(http.StatusBadRequest, CodeInvalidJSON, "cannot canonicalize request", err.Error())
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	record, found, err := findIdempotent(ctx, s.store.db, key)
	if err != nil {
		return nil, err
	}
	if found {
		if record.RequestHash != hash {
			return nil, NewAPIError(http.StatusConflict, CodeConflictRequest, "request id was already used with different content", map[string]any{
				"request_id": requestID,
				"kind":       kind,
			})
		}
		if record.Status == "rejected" {
			return nil, record.asAPIError()
		}
		var result T
		if err := json.Unmarshal([]byte(record.ResponseJSON), &result); err != nil {
			return nil, err
		}
		return &result, nil
	}

	now := s.NowMS()
	intentID := "intent_" + key
	payloadJSON, _ := json.Marshal(payload)
	intent := Intent{
		IntentID:       intentID,
		Kind:           kind,
		IdempotencyKey: key,
		RequestHash:    hash,
		Status:         IntentPending,
		PayloadJSON:    string(payloadJSON),
		CreatedAtMS:    now,
		UpdatedAtMS:    now,
	}
	if err := insertIntent(ctx, s.store.db, intent); err != nil {
		return nil, err
	}

	job := &writeJob[T]{
		service:   s,
		ctx:       ctx,
		kind:      kind,
		key:       key,
		hash:      hash,
		intentID:  intentID,
		requestID: requestID,
		startedAt: now,
	}
	result, apiErr := work(job)
	if apiErr != nil {
		_ = rejectWrite(context.WithoutCancel(ctx), s.store.db, job, apiErr)
		return nil, apiErr
	}
	return result, nil
}

func (j *writeJob[T]) Complete(txWork func(*sql.Tx) error, result *T, version int64) *APIError {
	tx, err := j.service.store.db.BeginTx(j.ctx, nil)
	if err != nil {
		return internalError(err)
	}
	if err := txWork(tx); err != nil {
		_ = tx.Rollback()
		return asAPIErrorOrInternal(err)
	}
	if err := completeIdempotentTx(j.ctx, tx, j, result, version); err != nil {
		_ = tx.Rollback()
		return internalError(err)
	}
	if err := commitIntentTx(j.ctx, tx, j.intentID, version); err != nil {
		_ = tx.Rollback()
		return internalError(err)
	}
	if err := tx.Commit(); err != nil {
		return internalError(err)
	}
	return nil
}

func completeIdempotentTx[T any](ctx context.Context, tx *sql.Tx, job *writeJob[T], result *T, version int64) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	now := job.service.NowMS()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO idempotent_requests(
			idempotency_key, request_hash, kind, version, status, http_status,
			response_json, created_at_ms, updated_at_ms
		) VALUES(?, ?, ?, ?, 'completed', 200, ?, ?, ?)`,
		job.key, job.hash, job.kind, version, string(raw), job.startedAt, now)
	return err
}

func commitIntentTx(ctx context.Context, tx *sql.Tx, intentID string, version int64) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE intents SET status=?, target_version=?, updated_at_ms=? WHERE intent_id=?`,
		IntentCommitted, version, nowMS(), intentID)
	return err
}

func rejectWrite[T any](ctx context.Context, db *sql.DB, job *writeJob[T], apiErr *APIError) error {
	detailsJSON := sql.NullString{}
	if apiErr.Details != nil {
		if raw, err := json.Marshal(apiErr.Details); err == nil {
			detailsJSON = sql.NullString{String: string(raw), Valid: true}
		}
	}
	status := apiErr.HTTPStatus
	if status == 0 {
		status = http.StatusInternalServerError
	}
	_, err := db.ExecContext(ctx, `
		INSERT INTO idempotent_requests(
			idempotency_key, request_hash, kind, version, status, http_status,
			response_json, error_code, error_message, error_details_json,
			created_at_ms, updated_at_ms
		) VALUES(?, ?, ?, 0, 'rejected', ?, '{}', ?, ?, ?, ?, ?)
		ON CONFLICT(idempotency_key) DO UPDATE SET
			request_hash=excluded.request_hash,
			status='rejected',
			http_status=excluded.http_status,
			error_code=excluded.error_code,
			error_message=excluded.error_message,
			error_details_json=excluded.error_details_json,
			updated_at_ms=excluded.updated_at_ms`,
		job.key, job.hash, job.kind, status, apiErr.Code, apiErr.Message, detailsJSON, job.startedAt, nowMS())
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `
		UPDATE intents SET status=?, recovery_action=?, updated_at_ms=? WHERE intent_id=?`,
		IntentAborted, "rejected_before_commit:"+apiErr.Code, nowMS(), job.intentID)
	return err
}

func findIdempotent(ctx context.Context, db *sql.DB, key string) (idempotentRecord, bool, error) {
	var rec idempotentRecord
	var details sql.NullString
	err := db.QueryRowContext(ctx, `
		SELECT request_hash, status, http_status, response_json, error_code,
		       error_message, error_details_json, version
		FROM idempotent_requests WHERE idempotency_key=?`, key).Scan(
		&rec.RequestHash, &rec.Status, &rec.HTTPStatus, &rec.ResponseJSON,
		&rec.ErrorCode, &rec.ErrorMessage, &details, &rec.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return rec, false, nil
	}
	if err != nil {
		return rec, false, err
	}
	rec.ErrorDetailsJSON = details
	return rec, true, nil
}

func (r idempotentRecord) asAPIError() *APIError {
	apiErr := NewAPIError(r.HTTPStatus, r.ErrorCode, r.ErrorMessage, nil)
	if r.ErrorDetailsJSON.Valid && r.ErrorDetailsJSON.String != "" {
		var details any
		if err := json.Unmarshal([]byte(r.ErrorDetailsJSON.String), &details); err == nil {
			apiErr.Details = details
		}
	}
	return apiErr
}

func insertIntent(ctx context.Context, db *sql.DB, intent Intent) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO intents(
			intent_id, kind, idempotency_key, request_hash, target_version,
			status, payload_json, recovery_action, created_at_ms, updated_at_ms
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		intent.IntentID, intent.Kind, intent.IdempotencyKey, intent.RequestHash,
		intent.TargetVersion, intent.Status, intent.PayloadJSON, intent.RecoveryAction,
		intent.CreatedAtMS, intent.UpdatedAtMS)
	return err
}

func internalError(err error) *APIError {
	if apiErr, ok := err.(*APIError); ok {
		return apiErr
	}
	return NewAPIError(http.StatusInternalServerError, CodeInternal, err.Error(), nil)
}

func asAPIErrorOrInternal(err error) *APIError {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr
	}
	return internalError(err)
}
