package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"

	"local/objectstore/internal/model"
)

type MergeParams struct {
	SessionID         string
	ExpectedTotalSize *int64
	ExpectedPartCount *int64
	IntentSeq         int64
	NowMS             int64
}

type MergeResult struct {
	Object        model.ObjectMetadata `json:"object"`
	Session       model.Session        `json:"session"`
	AlreadyMerged bool                 `json:"already_merged"`
}

func (s *Store) MergeSession(ctx context.Context, params MergeParams) (MergeResult, error) {
	var result MergeResult
	err := s.withWriteTx(ctx, func(conn *sql.Conn) error {
		session, err := scanSession(conn.QueryRowContext(ctx, sessionSelect+" WHERE session_id = ?", params.SessionID))
		if errors.Is(err, sql.ErrNoRows) {
			return model.NewError(model.CodeSessionNotFound, "upload session not found").WithContext("session_id", params.SessionID)
		}
		if err != nil {
			return err
		}
		if session.Status == model.SessionCompleted {
			if session.ObjectSHA256 == "" {
				return model.NewError(model.CodeInternal, "completed session has no persisted object metadata").
					WithContext("session_id", params.SessionID)
			}
			if err := markIntentCommitted(ctx, conn, params.IntentSeq, params.NowMS); err != nil {
				return err
			}
			result.Session = session
			result.Object = objectFromSession(session)
			result.AlreadyMerged = true
			return nil
		}
		if session.Status != model.SessionActive {
			return model.NewError(model.CodeSessionState, "upload session cannot be merged in its current state").
				WithContext("session_id", params.SessionID).WithContext("status", session.Status)
		}
		if session.ExpiresAtMS <= params.NowMS {
			return model.NewError(model.CodeSessionState, "upload session has expired").
				WithContext("session_id", params.SessionID).WithContext("expires_at_ms", session.ExpiresAtMS)
		}

		parts, err := loadPartsWithContent(ctx, conn, params.SessionID)
		if err != nil {
			return err
		}
		if len(parts) == 0 {
			return model.NewError(model.CodePartMissing, "no parts have been committed").
				WithContext("session_id", params.SessionID).
				WithContext("missing_part_numbers", []int64{session.LowerBound})
		}

		missing := missingPartNumbers(parts, session.LowerBound)
		if len(missing) > 0 {
			return model.NewError(model.CodePartMissing, "part sequence contains gaps").
				WithContext("session_id", params.SessionID).
				WithContext("missing_part_numbers", missing)
		}
		if params.ExpectedPartCount != nil && *params.ExpectedPartCount != int64(len(parts)) {
			extraMissing := make([]int64, 0)
			if *params.ExpectedPartCount > int64(len(parts)) {
				next := session.LowerBound + int64(len(parts))
				for number := next; number < session.LowerBound+*params.ExpectedPartCount; number++ {
					extraMissing = append(extraMissing, number)
				}
			}
			return model.NewError(model.CodePartMissing, "committed part count does not match the merge request").
				WithContext("session_id", params.SessionID).
				WithContext("expected_part_count", *params.ExpectedPartCount).
				WithContext("actual_part_count", len(parts)).
				WithContext("missing_part_numbers", extraMissing)
		}

		hasher := sha256.New()
		var totalSize int64
		for _, part := range parts {
			if int64(len(part.Content)) != part.Size {
				return model.NewError(model.CodeSizeMismatch, "stored part size differs from its content length").
					WithContext("session_id", params.SessionID).
					WithContext("part_number", part.PartNumber).
					WithContext("declared_size", part.Size).
					WithContext("actual_size", len(part.Content))
			}
			if _, err := hasher.Write(part.Content); err != nil {
				return fmt.Errorf("hash part %d: %w", part.PartNumber, err)
			}
			totalSize += part.Size
		}
		if params.ExpectedTotalSize != nil && *params.ExpectedTotalSize != totalSize {
			return model.NewError(model.CodeSizeMismatch, "object size does not match the merge request").
				WithContext("session_id", params.SessionID).
				WithContext("expected_total_size", *params.ExpectedTotalSize).
				WithContext("actual_total_size", totalSize)
		}
		objectSHA := hex.EncodeToString(hasher.Sum(nil))
		partCount := int64(len(parts))

		var currentGeneration sql.NullInt64
		err = conn.QueryRowContext(ctx, `SELECT generation FROM objects WHERE object_key = ?`, session.ObjectKey).Scan(&currentGeneration)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if currentGeneration.Valid && currentGeneration.Int64 > session.Generation {
			return model.NewError(model.CodeSessionState, "session generation is older than the current object generation").
				WithContext("session_generation", session.Generation).
				WithContext("current_generation", currentGeneration.Int64)
		}

		if _, err := conn.ExecContext(ctx, `
INSERT INTO objects(object_key, session_id, generation, total_size, part_count, sha256, updated_at_ms)
VALUES(?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(object_key) DO UPDATE SET
    session_id = excluded.session_id,
    generation = excluded.generation,
    total_size = excluded.total_size,
    part_count = excluded.part_count,
    sha256 = excluded.sha256,
    updated_at_ms = excluded.updated_at_ms`,
			session.ObjectKey, session.SessionID, session.Generation, totalSize, partCount, objectSHA, params.NowMS); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `
UPDATE sessions SET status = 'completed', completed_at_ms = ?, object_sha256 = ?,
                    object_total_size = ?, object_part_count = ?
WHERE session_id = ?`,
			params.NowMS, objectSHA, totalSize, partCount, session.SessionID); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `
UPDATE key_state SET active_session_id = '', updated_at_ms = ? WHERE object_key = ? AND active_session_id = ?`,
			params.NowMS, session.ObjectKey, session.SessionID); err != nil {
			return err
		}
		if err := markIntentCommitted(ctx, conn, params.IntentSeq, params.NowMS); err != nil {
			return err
		}
		result.Session, err = scanSession(conn.QueryRowContext(ctx, sessionSelect+" WHERE session_id = ?", session.SessionID))
		if err != nil {
			return err
		}
		result.Object = model.ObjectMetadata{
			ObjectKey: session.ObjectKey, SessionID: session.SessionID, Generation: session.Generation,
			TotalSize: totalSize, PartCount: partCount, SHA256: objectSHA, UpdatedAtMS: params.NowMS,
		}
		return nil
	})
	if err != nil {
		return MergeResult{}, err
	}
	return result, nil
}

func loadPartsWithContent(ctx context.Context, conn *sql.Conn, sessionID string) ([]model.Part, error) {
	rows, err := conn.QueryContext(ctx, `
SELECT session_id, part_number, size, sha256, arrived_at_ms, duplicate_count, status, created_at_ms, content
FROM parts WHERE session_id = ? AND status = 'active' ORDER BY part_number ASC`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var parts []model.Part
	for rows.Next() {
		var part model.Part
		if err := rows.Scan(&part.SessionID, &part.PartNumber, &part.Size, &part.SHA256,
			&part.ArrivedAtMS, &part.DuplicateCount, &part.Status, &part.CreatedAtMS, &part.Content); err != nil {
			return nil, err
		}
		parts = append(parts, part)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return parts, nil
}

func missingPartNumbers(parts []model.Part, lowerBound int64) []int64 {
	missing := make([]int64, 0)
	next := lowerBound
	for _, part := range parts {
		for next < part.PartNumber {
			missing = append(missing, next)
			next++
		}
		if part.PartNumber >= next {
			next = part.PartNumber + 1
		}
	}
	return missing
}

func objectFromSession(session model.Session) model.ObjectMetadata {
	updatedAt := session.CreatedAtMS
	if session.CompletedAtMS != nil {
		updatedAt = *session.CompletedAtMS
	}
	return model.ObjectMetadata{
		ObjectKey: session.ObjectKey, SessionID: session.SessionID, Generation: session.Generation,
		TotalSize: session.ObjectTotalSize, PartCount: session.ObjectPartCount,
		SHA256: session.ObjectSHA256, UpdatedAtMS: updatedAt,
	}
}

func (s *Store) GetObject(ctx context.Context, objectKey string) (model.ObjectMetadata, error) {
	var object model.ObjectMetadata
	err := s.db.QueryRowContext(ctx, `
SELECT object_key, session_id, generation, total_size, part_count, sha256, updated_at_ms
FROM objects WHERE object_key = ?`, objectKey).Scan(
		&object.ObjectKey, &object.SessionID, &object.Generation,
		&object.TotalSize, &object.PartCount, &object.SHA256, &object.UpdatedAtMS)
	if errors.Is(err, sql.ErrNoRows) {
		return model.ObjectMetadata{}, model.NewError(model.CodeObjectNotFound, "object metadata not found").
			WithContext("object_key", objectKey)
	}
	if err != nil {
		return model.ObjectMetadata{}, err
	}
	return object, nil
}
