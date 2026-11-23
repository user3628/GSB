package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"local/objectstore/internal/model"
)

type CreateSessionParams struct {
	SessionID         string
	ObjectKey         string
	LowerBound        int64
	TTLMS             int64
	CreationRequestID string
	IntentSeq         int64
	NowMS             int64
}

type CreateSessionResult struct {
	Session     model.Session
	Reused      bool
	ReclaimedID string
}

const sessionSelect = `
SELECT session_id, object_key, generation, lower_bound, ttl_ms, status,
       created_at_ms, expires_at_ms, completed_at_ms, cancelled_at_ms, cancel_reason,
       object_sha256, object_total_size, object_part_count, creation_request_id
FROM sessions`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanSession(scanner rowScanner) (model.Session, error) {
	var session model.Session
	var completed sql.NullInt64
	var cancelled sql.NullInt64
	err := scanner.Scan(
		&session.SessionID, &session.ObjectKey, &session.Generation, &session.LowerBound,
		&session.TTLMS, &session.Status, &session.CreatedAtMS, &session.ExpiresAtMS,
		&completed, &cancelled, &session.CancelReason, &session.ObjectSHA256,
		&session.ObjectTotalSize, &session.ObjectPartCount, &session.CreationRequestID,
	)
	if err != nil {
		return model.Session{}, err
	}
	session.CompletedAtMS = fromNullInt64(completed)
	session.CancelledAtMS = fromNullInt64(cancelled)
	return session, nil
}

func (s *Store) CreateSession(ctx context.Context, params CreateSessionParams) (CreateSessionResult, error) {
	var result CreateSessionResult
	err := s.withWriteTx(ctx, func(conn *sql.Conn) error {
		if params.CreationRequestID != "" {
			existing, err := scanSession(conn.QueryRowContext(ctx, sessionSelect+" WHERE creation_request_id = ?", params.CreationRequestID))
			if err == nil {
				if existing.ObjectKey != params.ObjectKey || existing.LowerBound != params.LowerBound || existing.TTLMS != params.TTLMS {
					return model.NewError(model.CodeConflictingRequest, "create request_id was already used with different session parameters").
						WithContext("request_id", params.CreationRequestID).
						WithContext("existing_session_id", existing.SessionID)
				}
				result.Session = existing
				result.Reused = true
				return markIntentCommitted(ctx, conn, params.IntentSeq, params.NowMS)
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}

		var activeID string
		var activeExpiry int64
		err := conn.QueryRowContext(ctx, `
SELECT session_id, expires_at_ms FROM sessions WHERE object_key = ? AND status = 'active'`,
			params.ObjectKey).Scan(&activeID, &activeExpiry)
		switch {
		case err == nil && activeExpiry > params.NowMS:
			return model.NewError(model.CodeSessionConflict, "an active upload session already exists for this object key").
				WithContext("object_key", params.ObjectKey).
				WithContext("existing_session_id", activeID).
				WithContext("existing_expires_at_ms", activeExpiry)
		case err == nil:
			if _, err := conn.ExecContext(ctx, `
INSERT INTO intents(request_id, action, session_id, object_key, payload_json, status, created_at_ms, committed_at_ms)
VALUES('', ?, ?, ?, ?, 'committed', ?, ?)`,
				model.ActionExpireSession, activeID, params.ObjectKey,
				marshalPayload(map[string]any{"reason": "ttl_expired_before_new_session"}), params.NowMS, params.NowMS); err != nil {
				return err
			}
			if _, err := expireSessionInTx(ctx, conn, activeID, params.NowMS, "ttl_expired_before_new_session", false); err != nil {
				return err
			}
			result.ReclaimedID = activeID
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}

		var generation int64
		err = conn.QueryRowContext(ctx, `SELECT generation FROM key_state WHERE object_key = ?`, params.ObjectKey).Scan(&generation)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var maxGeneration sql.NullInt64
		if err := conn.QueryRowContext(ctx, `SELECT MAX(generation) FROM sessions WHERE object_key = ?`, params.ObjectKey).Scan(&maxGeneration); err != nil {
			return err
		}
		if maxGeneration.Valid && maxGeneration.Int64 > generation {
			generation = maxGeneration.Int64
		}
		generation++

		session := model.Session{
			SessionID:         params.SessionID,
			ObjectKey:         params.ObjectKey,
			Generation:        generation,
			LowerBound:        params.LowerBound,
			TTLMS:             params.TTLMS,
			Status:            model.SessionActive,
			CreatedAtMS:       params.NowMS,
			ExpiresAtMS:       params.NowMS + params.TTLMS,
			CreationRequestID: params.CreationRequestID,
		}
		_, err = conn.ExecContext(ctx, `
INSERT INTO sessions(session_id, object_key, generation, lower_bound, ttl_ms, status,
                     created_at_ms, expires_at_ms, creation_request_id)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			session.SessionID, session.ObjectKey, session.Generation, session.LowerBound,
			session.TTLMS, session.Status, session.CreatedAtMS, session.ExpiresAtMS,
			session.CreationRequestID)
		if err != nil {
			if isConstraintError(err) {
				var existingID string
				_ = conn.QueryRowContext(ctx, `
SELECT session_id FROM sessions WHERE object_key = ? AND status = 'active'`, params.ObjectKey).Scan(&existingID)
				if existingID != "" {
					return model.NewError(model.CodeSessionConflict, "an active upload session already exists for this object key").
						WithContext("object_key", params.ObjectKey).
						WithContext("existing_session_id", existingID)
				}
			}
			return err
		}
		if _, err := conn.ExecContext(ctx, `
INSERT INTO key_state(object_key, generation, active_session_id, updated_at_ms)
VALUES(?, ?, ?, ?)
ON CONFLICT(object_key) DO UPDATE SET
    generation = excluded.generation,
    active_session_id = excluded.active_session_id,
    updated_at_ms = excluded.updated_at_ms`,
			params.ObjectKey, generation, params.SessionID, params.NowMS); err != nil {
			return err
		}
		if err := markIntentCommitted(ctx, conn, params.IntentSeq, params.NowMS); err != nil {
			return err
		}
		result.Session = session
		return nil
	})
	if err != nil {
		return CreateSessionResult{}, err
	}
	return result, nil
}

func expireSessionInTx(ctx context.Context, conn *sql.Conn, sessionID string, nowMS int64, reason string, requireActive bool) (model.Session, error) {
	session, err := scanSession(conn.QueryRowContext(ctx, sessionSelect+" WHERE session_id = ?", sessionID))
	if errors.Is(err, sql.ErrNoRows) {
		return model.Session{}, model.NewError(model.CodeSessionNotFound, "upload session not found").WithContext("session_id", sessionID)
	}
	if err != nil {
		return model.Session{}, err
	}
	if requireActive && (session.Status != model.SessionActive || session.ExpiresAtMS > nowMS) {
		return session, nil
	}
	if session.Status != model.SessionActive {
		return session, nil
	}
	if _, err := conn.ExecContext(ctx, `UPDATE parts SET status = 'void' WHERE session_id = ? AND status = 'active'`, sessionID); err != nil {
		return model.Session{}, err
	}
	if _, err := conn.ExecContext(ctx, `
UPDATE sessions SET status = 'expired', cancelled_at_ms = ?, cancel_reason = ? WHERE session_id = ?`,
		nowMS, reason, sessionID); err != nil {
		return model.Session{}, err
	}
	if _, err := conn.ExecContext(ctx, `
UPDATE key_state SET active_session_id = '', updated_at_ms = ? WHERE object_key = ? AND active_session_id = ?`,
		nowMS, session.ObjectKey, sessionID); err != nil {
		return model.Session{}, err
	}
	return scanSession(conn.QueryRowContext(ctx, sessionSelect+" WHERE session_id = ?", sessionID))
}

func (s *Store) GetSession(ctx context.Context, sessionID string) (model.Session, error) {
	session, err := scanSession(s.db.QueryRowContext(ctx, sessionSelect+" WHERE session_id = ?", sessionID))
	if errors.Is(err, sql.ErrNoRows) {
		return model.Session{}, model.NewError(model.CodeSessionNotFound, "upload session not found").WithContext("session_id", sessionID)
	}
	if err != nil {
		return model.Session{}, err
	}
	return session, nil
}

type ReclaimedSession struct {
	SessionID     string `json:"session_id"`
	ObjectKey     string `json:"object_key"`
	IntentSeq     int64  `json:"intent_seq"`
	ReclaimedAtMS int64  `json:"reclaimed_at_ms"`
}

func (s *Store) ReclaimExpired(ctx context.Context, nowMS int64) ([]ReclaimedSession, error) {
	var reclaimed []ReclaimedSession
	err := s.withWriteTx(ctx, func(conn *sql.Conn) error {
		rows, err := conn.QueryContext(ctx, `
SELECT session_id, object_key FROM sessions WHERE status = 'active' AND expires_at_ms <= ? ORDER BY session_id`, nowMS)
		if err != nil {
			return err
		}
		type expired struct{ id, key string }
		var candidates []expired
		for rows.Next() {
			var candidate expired
			if err := rows.Scan(&candidate.id, &candidate.key); err != nil {
				rows.Close()
				return err
			}
			candidates = append(candidates, candidate)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, candidate := range candidates {
			result, err := conn.ExecContext(ctx, `
INSERT INTO intents(request_id, action, session_id, object_key, payload_json, status, created_at_ms, committed_at_ms)
VALUES('', ?, ?, ?, ?, 'committed', ?, ?)`,
				model.ActionExpireSession, candidate.id, candidate.key,
				marshalPayload(map[string]any{"reason": "ttl_expired"}), nowMS, nowMS)
			if err != nil {
				return err
			}
			intentSeq, _ := result.LastInsertId()
			if _, err := expireSessionInTx(ctx, conn, candidate.id, nowMS, "ttl_expired", true); err != nil {
				return err
			}
			reclaimed = append(reclaimed, ReclaimedSession{
				SessionID: candidate.id, ObjectKey: candidate.key, IntentSeq: intentSeq, ReclaimedAtMS: nowMS,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return reclaimed, nil
}

func (s *Store) ReclaimExpiredForKey(ctx context.Context, objectKey string, nowMS int64) (ReclaimedSession, bool, error) {
	var reclaimed ReclaimedSession
	var didReclaim bool
	err := s.withWriteTx(ctx, func(conn *sql.Conn) error {
		var sessionID string
		err := conn.QueryRowContext(ctx, `
SELECT session_id FROM sessions WHERE object_key = ? AND status = 'active' AND expires_at_ms <= ?`,
			objectKey, nowMS).Scan(&sessionID)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		result, err := conn.ExecContext(ctx, `
INSERT INTO intents(request_id, action, session_id, object_key, payload_json, status, created_at_ms, committed_at_ms)
VALUES('', ?, ?, ?, ?, 'committed', ?, ?)`,
			model.ActionExpireSession, sessionID, objectKey,
			marshalPayload(map[string]any{"reason": "ttl_expired"}), nowMS, nowMS)
		if err != nil {
			return err
		}
		intentSeq, _ := result.LastInsertId()
		if _, err := expireSessionInTx(ctx, conn, sessionID, nowMS, "ttl_expired", true); err != nil {
			return err
		}
		reclaimed = ReclaimedSession{SessionID: sessionID, ObjectKey: objectKey, IntentSeq: intentSeq, ReclaimedAtMS: nowMS}
		didReclaim = true
		return nil
	})
	return reclaimed, didReclaim, err
}

type CancelSessionResult struct {
	Session            model.Session `json:"session"`
	VoidedPartCount    int64         `json:"voided_part_count"`
	ReleasedObjectSlot bool          `json:"released_object_slot"`
}

func (s *Store) CancelSession(ctx context.Context, sessionID string, reason string, intentSeq int64, nowMS int64) (CancelSessionResult, error) {
	var result CancelSessionResult
	err := s.withWriteTx(ctx, func(conn *sql.Conn) error {
		session, err := scanSession(conn.QueryRowContext(ctx, sessionSelect+" WHERE session_id = ?", sessionID))
		if errors.Is(err, sql.ErrNoRows) {
			return model.NewError(model.CodeSessionNotFound, "upload session not found").WithContext("session_id", sessionID)
		}
		if err != nil {
			return err
		}
		switch session.Status {
		case model.SessionCancelled, model.SessionExpired:
			result.Session = session
			return markIntentCommitted(ctx, conn, intentSeq, nowMS)
		case model.SessionCompleted:
			return model.NewError(model.CodeSessionState, "completed upload session cannot be cancelled").
				WithContext("session_id", sessionID).WithContext("status", session.Status)
		}
		update, err := conn.ExecContext(ctx, `
UPDATE parts SET status = 'void' WHERE session_id = ? AND status = 'active'`, sessionID)
		if err != nil {
			return err
		}
		result.VoidedPartCount, _ = update.RowsAffected()
		if _, err := conn.ExecContext(ctx, `
UPDATE sessions SET status = 'cancelled', cancelled_at_ms = ?, cancel_reason = ? WHERE session_id = ?`,
			nowMS, reason, sessionID); err != nil {
			return err
		}
		release, err := conn.ExecContext(ctx, `
UPDATE key_state SET active_session_id = '', updated_at_ms = ? WHERE object_key = ? AND active_session_id = ?`,
			nowMS, session.ObjectKey, sessionID)
		if err != nil {
			return err
		}
		released, _ := release.RowsAffected()
		result.ReleasedObjectSlot = released > 0
		if err := markIntentCommitted(ctx, conn, intentSeq, nowMS); err != nil {
			return err
		}
		result.Session, err = scanSession(conn.QueryRowContext(ctx, sessionSelect+" WHERE session_id = ?", sessionID))
		return err
	})
	if err != nil {
		return CancelSessionResult{}, err
	}
	return result, nil
}

func (s *Store) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("database ping: %w", err)
	}
	return nil
}
