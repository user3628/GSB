package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"local/objectstore/internal/model"
)

func (s *Store) ClaimRequest(ctx context.Context, requestID, operation, fingerprint string, nowMS int64) (model.RequestRecord, error) {
	var record model.RequestRecord
	err := s.withWriteTx(ctx, func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `
INSERT INTO requests(request_id, operation, fingerprint, status, created_at_ms)
VALUES(?, ?, ?, 'processing', ?)
ON CONFLICT(request_id) DO NOTHING`, requestID, operation, fingerprint, nowMS)
		if err != nil {
			return err
		}
		record, err = loadRequest(ctx, conn, requestID)
		if err != nil {
			return err
		}
		if record.Operation != operation || record.Fingerprint != fingerprint {
			return model.NewError(model.CodeConflictingRequest, "request_id was already used with different content").
				WithContext("request_id", requestID).
				WithContext("existing_operation", record.Operation).
				WithContext("submitted_operation", operation)
		}
		if record.Status == "interrupted" {
			if _, err := conn.ExecContext(ctx, `
UPDATE requests SET status = 'processing', completed_at_ms = NULL WHERE request_id = ? AND status = 'interrupted'`,
				requestID); err != nil {
				return err
			}
			record.Status = "processing"
		}
		return nil
	})
	if err != nil {
		return model.RequestRecord{}, err
	}
	return record, nil
}

func (s *Store) CompleteRequest(ctx context.Context, requestID string, status int, body []byte, code string, nowMS int64) error {
	return s.withWriteTx(ctx, func(conn *sql.Conn) error {
		result, err := conn.ExecContext(ctx, `
UPDATE requests SET status = 'completed', http_status = ?, response_body = ?, code = ?, completed_at_ms = ?
WHERE request_id = ?`, status, body, code, nowMS, requestID)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return fmt.Errorf("request %s does not exist", requestID)
		}
		return nil
	})
}

func (s *Store) GetRequest(ctx context.Context, requestID string) (model.RequestRecord, bool, error) {
	record, err := loadRequest(ctx, s.db, requestID)
	if errors.Is(err, sql.ErrNoRows) {
		return model.RequestRecord{}, false, nil
	}
	if err != nil {
		return model.RequestRecord{}, false, err
	}
	return record, true, nil
}

type requestQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadRequest(ctx context.Context, queryer requestQueryer, requestID string) (model.RequestRecord, error) {
	var record model.RequestRecord
	var completed sql.NullInt64
	var body []byte
	err := queryer.QueryRowContext(ctx, `
SELECT request_id, operation, fingerprint, status, http_status, response_body, code, created_at_ms, completed_at_ms
FROM requests WHERE request_id = ?`, requestID).Scan(
		&record.RequestID, &record.Operation, &record.Fingerprint, &record.Status,
		&record.HTTPStatus, &body, &record.Code, &record.CreatedAtMS, &completed)
	if err != nil {
		return model.RequestRecord{}, err
	}
	record.ResponseBody = append([]byte(nil), body...)
	record.CompletedAtMS = fromNullInt64(completed)
	return record, nil
}
