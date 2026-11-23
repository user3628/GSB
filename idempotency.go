package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type opContext struct {
	RequestID   string
	IntentID    string
	Operation   string
	Target      string
	Fingerprint string
	InjectionID string
}

type opDecision struct {
	Op         *opContext
	Response   json.RawMessage
	StoredErr  *WorkbenchError
	RequestID  string
	InProgress bool
}

func (s *Service) prepareOperation(ctx context.Context, requestID, operation, target string,
	payload any, injection *InjectionRequest) (opDecision, error) {
	if requestID == "" {
		requestID = newID("req")
	}
	fingerprint := fingerprintOf(operation, payload)
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return opDecision{}, err
	}
	defer tx.Rollback()

	var rec requestRecord
	var response, errCode, errMsg sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT request_id, operation, fingerprint, status,
		response_json, error_code, error_message FROM requests WHERE request_id=?`, requestID).Scan(
		&rec.RequestID, &rec.Operation, &rec.Fingerprint, &rec.Status, &response, &errCode, &errMsg)
	switch {
	case err == nil:
		if rec.Operation != operation || rec.Fingerprint != fingerprint {
			return opDecision{}, newErrDetails(CodeConflictingRequest,
				"request_id was already used for a different operation or payload",
				map[string]any{"request_id": requestID, "existing_operation": rec.Operation})
		}
		switch rec.Status {
		case RequestCommitted:
			return opDecision{Response: json.RawMessage(response.String), RequestID: requestID}, nil
		case RequestFailed:
			code := errCode.String
			if code == "" {
				code = CodeInternal
			}
			return opDecision{RequestID: requestID, StoredErr: &WorkbenchError{
				Code: code, Message: errMsg.String, HTTP: statusForCode(code),
			}}, nil
		case RequestRunning:
			return opDecision{RequestID: requestID, InProgress: true}, nil
		default:
			return opDecision{}, fmt.Errorf("unknown request state %q", rec.Status)
		}
	case !errors.Is(err, sql.ErrNoRows):
		return opDecision{}, err
	}

	intentID := newID("intent")
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return opDecision{}, err
	}
	now := nowMS(s.now())
	if _, err = tx.ExecContext(ctx, `INSERT INTO requests
		(request_id, operation, fingerprint, status, created_at_ms) VALUES (?,?,?,?,?)`,
		requestID, operation, fingerprint, RequestRunning, now); err != nil {
		return opDecision{}, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO intents
		(intent_id, request_id, operation, target, payload_json, status, created_at_ms, updated_at_ms)
		VALUES (?,?,?,?,?,?,?,?)`,
		intentID, requestID, operation, target, string(payloadJSON), IntentPrepared, now, now); err != nil {
		return opDecision{}, err
	}
	injectionID := ""
	if injection != nil {
		injectionID = newID("inj")
		active := 0
		if injection.Active != nil && *injection.Active {
			active = 1
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO injections
			(id, kind, target, mode, delay_ms, payload, active, created_at_ms)
			VALUES (?,?,?,?,?,?,?,?)`,
			injectionID, injection.Kind, injection.Target, injection.Mode,
			injection.DelayMS, injection.Payload, active, now); err != nil {
			return opDecision{}, err
		}
	}
	if err = tx.Commit(); err != nil {
		return opDecision{}, err
	}
	return opDecision{Op: &opContext{
		RequestID: requestID, IntentID: intentID, Operation: operation,
		Target: target, Fingerprint: fingerprint, InjectionID: injectionID,
	}, RequestID: requestID}, nil
}

func (s *Service) awaitOperation(ctx context.Context, requestID string) (opDecision, error) {
	deadline := time.Now().Add(s.idemWait)
	for {
		var rec requestRecord
		var response, errCode, errMsg sql.NullString
		err := s.db.QueryRowContext(ctx, `SELECT request_id, operation, fingerprint, status,
			response_json, error_code, error_message FROM requests WHERE request_id=?`, requestID).Scan(
			&rec.RequestID, &rec.Operation, &rec.Fingerprint, &rec.Status, &response, &errCode, &errMsg)
		if err != nil {
			return opDecision{}, err
		}
		switch rec.Status {
		case RequestCommitted:
			return opDecision{Response: json.RawMessage(response.String), RequestID: requestID}, nil
		case RequestFailed:
			code := errCode.String
			if code == "" {
				code = CodeInternal
			}
			return opDecision{RequestID: requestID, StoredErr: &WorkbenchError{
				Code: code, Message: errMsg.String, HTTP: statusForCode(code),
			}}, nil
		}
		if time.Now().After(deadline) {
			return opDecision{}, newErrDetails(CodeRequestInProgress,
				"duplicate request is still running; its result is not yet available",
				map[string]any{"request_id": requestID})
		}
		select {
		case <-ctx.Done():
			return opDecision{}, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// commitOperation executes the domain mutation and records its response, the
// intent completion, and the request idempotency outcome in one SQLite commit.
func (s *Service) commitOperation(ctx context.Context, op *opContext,
	mutate func(*sql.Tx) (any, error)) (json.RawMessage, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var status string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM requests WHERE request_id=?`, op.RequestID).Scan(&status); err != nil {
		return nil, err
	}
	if status != RequestRunning {
		return nil, newErr(CodeRequestInProgress, "request is no longer running")
	}

	result, err := mutate(tx)
	if err != nil {
		we := asWorkbenchError(err)
		if we.Code == CodeInternal || we.Code == CodeDatabase {
			// Roll back domain changes before recording the failure.
			_ = tx.Rollback()
			if failErr := s.markOperationFailedLocked(ctx, op, we); failErr != nil {
				return nil, failErr
			}
			return nil, we
		}
		now := nowMS(s.now())
		if _, updateErr := tx.ExecContext(ctx, `UPDATE requests SET status=?, error_code=?,
			error_message=?, finished_at_ms=? WHERE request_id=?`,
			RequestFailed, we.Code, we.Message, now, op.RequestID); updateErr != nil {
			return nil, updateErr
		}
		if _, updateErr := tx.ExecContext(ctx, `UPDATE intents SET status=?, updated_at_ms=?
			WHERE intent_id=?`, IntentAborted, now, op.IntentID); updateErr != nil {
			return nil, updateErr
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return nil, commitErr
		}
		return nil, we
	}

	raw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	now := nowMS(s.now())
	if _, err := tx.ExecContext(ctx, `UPDATE requests SET status=?, response_json=?,
		finished_at_ms=? WHERE request_id=?`, RequestCommitted, string(raw), now, op.RequestID); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status=?, result_json=?, updated_at_ms=?
		WHERE intent_id=?`, IntentCommitted, string(raw), now, op.IntentID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return raw, nil
}

func (s *Service) markOperationFailed(ctx context.Context, op *opContext, we *WorkbenchError) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.markOperationFailedLocked(ctx, op, we)
}

func (s *Service) markOperationFailedLocked(ctx context.Context, op *opContext, we *WorkbenchError) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := nowMS(s.now())
	if _, err := tx.ExecContext(ctx, `UPDATE requests SET status=?, error_code=?,
		error_message=?, finished_at_ms=? WHERE request_id=? AND status=?`,
		RequestFailed, we.Code, we.Message, now, op.RequestID, RequestRunning); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status=?, updated_at_ms=?
		WHERE intent_id=? AND status=?`, IntentAborted, now, op.IntentID, IntentPrepared); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) recordObservation(ctx context.Context, injectionID, kind, mode, target,
	phase, code, detail, requestID string) (*Observation, error) {
	if injectionID == "" {
		return nil, nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.recordObservationLocked(ctx, s.db, injectionID, kind, mode, target, phase, code, detail, requestID)
}

type execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (s *Service) recordObservationLocked(ctx context.Context, ex execer, injectionID, kind, mode, target,
	phase, code, detail, requestID string) (*Observation, error) {
	if injectionID == "" {
		return nil, nil
	}
	now := nowMS(s.now())
	res, err := ex.ExecContext(ctx, `INSERT INTO injection_observations
		(injection_id, kind, mode, target, phase, observed_code, detail, request_id, observed_at_ms)
		VALUES (?,?,?,?,?,?,?,?,?)`, injectionID, kind, mode, target, phase, code, detail, requestID, now)
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	return &Observation{ID: id, InjectionID: injectionID, Kind: kind, Mode: mode,
		Target: target, Phase: phase, ObservedCode: code, Detail: detail,
		RequestID: requestID, ObservedAtMS: now}, nil
}
