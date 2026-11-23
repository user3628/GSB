package store

import (
	"context"
	"database/sql"
	"errors"

	"local/objectstore/internal/model"
)

type SubmitPartParams struct {
	SessionID   string
	PartNumber  int64
	Size        int64
	SHA256      string
	ArrivedAtMS int64
	Content     []byte
	IntentSeq   int64
	NowMS       int64
}

type SubmitPartResult struct {
	Part      model.Part `json:"part"`
	Duplicate bool       `json:"duplicate"`
}

func (s *Store) SubmitPart(ctx context.Context, params SubmitPartParams) (SubmitPartResult, error) {
	var result SubmitPartResult
	err := s.withWriteTx(ctx, func(conn *sql.Conn) error {
		var objectKey, status string
		var lowerBound, expiresAt int64
		err := conn.QueryRowContext(ctx, `
SELECT object_key, lower_bound, status, expires_at_ms FROM sessions WHERE session_id = ?`, params.SessionID).
			Scan(&objectKey, &lowerBound, &status, &expiresAt)
		if errors.Is(err, sql.ErrNoRows) {
			return model.NewError(model.CodeSessionNotFound, "upload session not found").WithContext("session_id", params.SessionID)
		}
		if err != nil {
			return err
		}
		if status != model.SessionActive {
			return model.NewError(model.CodeSessionState, "upload session is not active").
				WithContext("session_id", params.SessionID).WithContext("status", status)
		}
		if expiresAt <= params.NowMS {
			return model.NewError(model.CodeSessionState, "upload session has expired").
				WithContext("session_id", params.SessionID).WithContext("expires_at_ms", expiresAt)
		}
		if params.PartNumber < lowerBound {
			return model.NewError(model.CodeInvalidArgument, "part number is below the session lower bound").
				WithContext("part_number", params.PartNumber).WithContext("lower_bound", lowerBound)
		}

		var existing model.Part
		var existingStatus string
		err = conn.QueryRowContext(ctx, `
SELECT size, sha256, arrived_at_ms, duplicate_count, status, created_at_ms
FROM parts WHERE session_id = ? AND part_number = ?`,
			params.SessionID, params.PartNumber).Scan(
			&existing.Size, &existing.SHA256, &existing.ArrivedAtMS,
			&existing.DuplicateCount, &existingStatus, &existing.CreatedAtMS)
		if err == nil {
			existing.SessionID = params.SessionID
			existing.PartNumber = params.PartNumber
			existing.Status = existingStatus
			if existing.SHA256 != params.SHA256 {
				return model.NewError(model.CodePartMismatch, "part digest differs from the first accepted submission").
					WithContext("session_id", params.SessionID).
					WithContext("part_number", params.PartNumber).
					WithContext("stored_sha256", existing.SHA256).
					WithContext("submitted_sha256", params.SHA256).
					WithContext("preserved", true)
			}
			if _, err := conn.ExecContext(ctx, `
UPDATE parts SET duplicate_count = duplicate_count + 1 WHERE session_id = ? AND part_number = ?`,
				params.SessionID, params.PartNumber); err != nil {
				return err
			}
			existing.DuplicateCount++
			if err := markIntentCommitted(ctx, conn, params.IntentSeq, params.NowMS); err != nil {
				return err
			}
			result.Part = existing
			result.Duplicate = true
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		part := model.Part{
			SessionID:      params.SessionID,
			PartNumber:     params.PartNumber,
			Size:           params.Size,
			SHA256:         params.SHA256,
			ArrivedAtMS:    params.ArrivedAtMS,
			DuplicateCount: 0,
			Status:         model.PartActive,
			CreatedAtMS:    params.NowMS,
			Content:        params.Content,
		}
		if _, err := conn.ExecContext(ctx, `
INSERT INTO parts(session_id, part_number, size, sha256, arrived_at_ms, duplicate_count, status, created_at_ms, content)
VALUES(?, ?, ?, ?, ?, 0, 'active', ?, ?)`,
			part.SessionID, part.PartNumber, part.Size, part.SHA256, part.ArrivedAtMS, part.CreatedAtMS, part.Content); err != nil {
			return err
		}
		if err := markIntentCommitted(ctx, conn, params.IntentSeq, params.NowMS); err != nil {
			return err
		}
		result.Part = part
		return nil
	})
	if err != nil {
		return SubmitPartResult{}, err
	}
	return result, nil
}

func (s *Store) ListParts(ctx context.Context, sessionID string) ([]model.Part, error) {
	var parts []model.Part
	err := s.withReadTx(ctx, func(conn *sql.Conn) error {
		var exists int
		if err := conn.QueryRowContext(ctx, `SELECT 1 FROM sessions WHERE session_id = ?`, sessionID).Scan(&exists); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return model.NewError(model.CodeSessionNotFound, "upload session not found").WithContext("session_id", sessionID)
			}
			return err
		}
		rows, err := conn.QueryContext(ctx, `
SELECT session_id, part_number, size, sha256, arrived_at_ms, duplicate_count, status, created_at_ms
FROM parts WHERE session_id = ? ORDER BY part_number ASC`, sessionID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var part model.Part
			if err := rows.Scan(&part.SessionID, &part.PartNumber, &part.Size, &part.SHA256,
				&part.ArrivedAtMS, &part.DuplicateCount, &part.Status, &part.CreatedAtMS); err != nil {
				return err
			}
			parts = append(parts, part)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	if parts == nil {
		parts = []model.Part{}
	}
	return parts, nil
}

func (s *Store) ActivePartsWithContent(ctx context.Context, sessionID string) ([]model.Part, error) {
	rows, err := s.db.QueryContext(ctx, `
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
