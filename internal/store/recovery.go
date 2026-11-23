package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"local/objectstore/internal/model"
)

type RecoveryChecks struct {
	Passed                         bool  `json:"passed"`
	CompletedSessionsWithoutObject int64 `json:"completed_sessions_without_object"`
	ActivePartsInInactiveSessions  int64 `json:"active_parts_in_inactive_sessions"`
	ExpiredActiveSessions          int64 `json:"expired_active_sessions"`
}

type InterruptedIntent struct {
	Seq            int64  `json:"seq"`
	RequestID      string `json:"request_id,omitempty"`
	Action         string `json:"action"`
	SessionID      string `json:"session_id,omitempty"`
	ObjectKey      string `json:"object_key,omitempty"`
	PartNumber     *int64 `json:"part_number,omitempty"`
	SHA256         string `json:"sha256,omitempty"`
	Size           *int64 `json:"size,omitempty"`
	RecordedStatus string `json:"recorded_status"`
	RecoveryStatus string `json:"recovery_status"`
	Applied        bool   `json:"applied"`
	CreatedAtMS    int64  `json:"created_at_ms"`
}

type RecoveryReport struct {
	ReportID            int64                  `json:"report_id"`
	GeneratedAtMS       int64                  `json:"generated_at_ms"`
	InterruptedIntents  []InterruptedIntent    `json:"interrupted_intents"`
	ExpiredReclaimed    []ReclaimedSession     `json:"expired_reclaimed"`
	Sessions            []model.Session        `json:"sessions"`
	Parts               []model.Part           `json:"parts"`
	Objects             []model.ObjectMetadata `json:"objects"`
	IntentCounts        map[string]int64       `json:"intent_counts"`
	Checks              RecoveryChecks         `json:"checks"`
	InterruptedRequests int64                  `json:"interrupted_requests"`
}

func (s *Store) Recover(ctx context.Context, nowMS int64) (RecoveryReport, error) {
	var report RecoveryReport
	err := s.withWriteTx(ctx, func(conn *sql.Conn) error {
		pending, err := loadPendingIntents(ctx, conn)
		if err != nil {
			return err
		}
		if len(pending) > 0 {
			if _, err := conn.ExecContext(ctx, `
UPDATE intents SET status = 'abandoned', committed_at_ms = ? WHERE status = 'pending'`, nowMS); err != nil {
				return err
			}
		}

		requests, err := conn.ExecContext(ctx, `
UPDATE requests SET status = 'interrupted', completed_at_ms = ? WHERE status = 'processing'`, nowMS)
		if err != nil {
			return err
		}
		report.InterruptedRequests, _ = requests.RowsAffected()

		expired, err := loadExpiredActiveSessions(ctx, conn, nowMS)
		if err != nil {
			return err
		}
		for _, candidate := range expired {
			result, err := conn.ExecContext(ctx, `
INSERT INTO intents(request_id, action, session_id, object_key, payload_json, status, created_at_ms, committed_at_ms)
VALUES('', ?, ?, ?, ?, 'committed', ?, ?)`,
				model.ActionExpireSession, candidate.SessionID, candidate.ObjectKey,
				marshalPayload(map[string]any{"reason": "ttl_expired_during_recovery"}), nowMS, nowMS)
			if err != nil {
				return err
			}
			intentSeq, _ := result.LastInsertId()
			if _, err := expireSessionInTx(ctx, conn, candidate.SessionID, nowMS, "ttl_expired_during_recovery", true); err != nil {
				return err
			}
			report.ExpiredReclaimed = append(report.ExpiredReclaimed, ReclaimedSession{
				SessionID: candidate.SessionID, ObjectKey: candidate.ObjectKey,
				IntentSeq: intentSeq, ReclaimedAtMS: nowMS,
			})
		}

		report.ReportID = 0
		report.GeneratedAtMS = nowMS
		report.InterruptedIntents = pending
		if report.ExpiredReclaimed == nil {
			report.ExpiredReclaimed = []ReclaimedSession{}
		}
		if err := populateRecoverySnapshot(ctx, conn, &report); err != nil {
			return err
		}
		report.Checks = RecoveryChecks{
			Passed: report.Checks.CompletedSessionsWithoutObject == 0 &&
				report.Checks.ActivePartsInInactiveSessions == 0 &&
				report.Checks.ExpiredActiveSessions == 0,
		}
		encoded, err := json.Marshal(report)
		if err != nil {
			return err
		}
		result, err := conn.ExecContext(ctx, `
INSERT INTO recovery_reports(generated_at_ms, report_json) VALUES(?, ?)`, nowMS, encoded)
		if err != nil {
			return err
		}
		report.ReportID, err = result.LastInsertId()
		if err != nil {
			return err
		}
		encoded, err = json.Marshal(report)
		if err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `UPDATE recovery_reports SET report_json = ? WHERE report_id = ?`, encoded, report.ReportID); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return RecoveryReport{}, err
	}
	return report, nil
}

type expiredCandidate struct {
	SessionID string
	ObjectKey string
}

func loadExpiredActiveSessions(ctx context.Context, conn *sql.Conn, nowMS int64) ([]expiredCandidate, error) {
	rows, err := conn.QueryContext(ctx, `
SELECT session_id, object_key FROM sessions WHERE status = 'active' AND expires_at_ms <= ? ORDER BY session_id`, nowMS)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []expiredCandidate
	for rows.Next() {
		var candidate expiredCandidate
		if err := rows.Scan(&candidate.SessionID, &candidate.ObjectKey); err != nil {
			return nil, err
		}
		result = append(result, candidate)
	}
	return result, rows.Err()
}

func loadPendingIntents(ctx context.Context, conn *sql.Conn) ([]InterruptedIntent, error) {
	rows, err := conn.QueryContext(ctx, `
SELECT seq, request_id, action, session_id, object_key, part_number, sha256, size, created_at_ms
FROM intents WHERE status = 'pending' ORDER BY seq ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []InterruptedIntent
	for rows.Next() {
		var item InterruptedIntent
		var partNumber sql.NullInt64
		var size sql.NullInt64
		if err := rows.Scan(&item.Seq, &item.RequestID, &item.Action, &item.SessionID, &item.ObjectKey,
			&partNumber, &item.SHA256, &size, &item.CreatedAtMS); err != nil {
			return nil, err
		}
		item.PartNumber = fromNullInt64(partNumber)
		item.Size = fromNullInt64(size)
		item.RecordedStatus = model.IntentPending
		item.RecoveryStatus = model.IntentAbandoned
		item.Applied = false
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if result == nil {
		result = []InterruptedIntent{}
	}
	return result, nil
}

func populateRecoverySnapshot(ctx context.Context, conn *sql.Conn, report *RecoveryReport) error {
	rows, err := conn.QueryContext(ctx, sessionSelect+" ORDER BY object_key ASC, generation ASC")
	if err != nil {
		return err
	}
	for rows.Next() {
		session, err := scanSession(rows)
		if err != nil {
			rows.Close()
			return err
		}
		report.Sessions = append(report.Sessions, session)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if report.Sessions == nil {
		report.Sessions = []model.Session{}
	}

	partRows, err := conn.QueryContext(ctx, `
SELECT session_id, part_number, size, sha256, arrived_at_ms, duplicate_count, status, created_at_ms
FROM parts ORDER BY session_id ASC, part_number ASC`)
	if err != nil {
		return err
	}
	for partRows.Next() {
		var part model.Part
		if err := partRows.Scan(&part.SessionID, &part.PartNumber, &part.Size, &part.SHA256,
			&part.ArrivedAtMS, &part.DuplicateCount, &part.Status, &part.CreatedAtMS); err != nil {
			partRows.Close()
			return err
		}
		report.Parts = append(report.Parts, part)
	}
	if err := partRows.Close(); err != nil {
		return err
	}
	if report.Parts == nil {
		report.Parts = []model.Part{}
	}

	objectRows, err := conn.QueryContext(ctx, `
SELECT object_key, session_id, generation, total_size, part_count, sha256, updated_at_ms
FROM objects ORDER BY object_key ASC`)
	if err != nil {
		return err
	}
	for objectRows.Next() {
		var object model.ObjectMetadata
		if err := objectRows.Scan(&object.ObjectKey, &object.SessionID, &object.Generation,
			&object.TotalSize, &object.PartCount, &object.SHA256, &object.UpdatedAtMS); err != nil {
			objectRows.Close()
			return err
		}
		report.Objects = append(report.Objects, object)
	}
	if err := objectRows.Close(); err != nil {
		return err
	}
	if report.Objects == nil {
		report.Objects = []model.ObjectMetadata{}
	}

	report.IntentCounts = make(map[string]int64)
	countRows, err := conn.QueryContext(ctx, `SELECT status, COUNT(*) FROM intents GROUP BY status`)
	if err != nil {
		return err
	}
	for countRows.Next() {
		var status string
		var count int64
		if err := countRows.Scan(&status, &count); err != nil {
			countRows.Close()
			return err
		}
		report.IntentCounts[status] = count
	}
	if err := countRows.Close(); err != nil {
		return err
	}

	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*) FROM sessions WHERE status = 'completed' AND object_sha256 = ''`).
		Scan(&report.Checks.CompletedSessionsWithoutObject); err != nil {
		return err
	}
	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*) FROM parts p JOIN sessions s ON s.session_id = p.session_id
WHERE p.status = 'active' AND s.status IN ('cancelled','expired')`).
		Scan(&report.Checks.ActivePartsInInactiveSessions); err != nil {
		return err
	}
	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*) FROM sessions WHERE status = 'active' AND expires_at_ms <= ?`, report.GeneratedAtMS).
		Scan(&report.Checks.ExpiredActiveSessions); err != nil {
		return err
	}
	return nil
}

func (s *Store) GetRecoveryReport(ctx context.Context, reportID int64) (RecoveryReport, error) {
	query := `SELECT report_json FROM recovery_reports ORDER BY report_id DESC LIMIT 1`
	args := []any{}
	if reportID > 0 {
		query = `SELECT report_json FROM recovery_reports WHERE report_id = ?`
		args = append(args, reportID)
	}
	var encoded []byte
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&encoded); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RecoveryReport{}, model.NewError(model.CodeObjectNotFound, "recovery report not found")
		}
		return RecoveryReport{}, err
	}
	var report RecoveryReport
	if err := json.Unmarshal(encoded, &report); err != nil {
		return RecoveryReport{}, fmt.Errorf("decode recovery report: %w", err)
	}
	return report, nil
}
