package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"local/sqlitepool/internal/domain"
)

func (s *Store) Recover(ctx context.Context) (domain.RecoveryReport, error) {
	started := time.Now()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.RecoveryReport{}, err
	}
	defer tx.Rollback()

	pendingRows, err := tx.QueryContext(ctx, `SELECT intent_id, kind, connection_id, request_id,
		payload_json, created_at_ns FROM intents WHERE status=? ORDER BY intent_id ASC`, domain.IntentPending)
	if err != nil {
		return domain.RecoveryReport{}, err
	}
	type pendingIntent struct {
		id                               int64
		kind, connID, requestID, payload string
		created                          int64
	}
	var pending []pendingIntent
	for pendingRows.Next() {
		var p pendingIntent
		if err := pendingRows.Scan(&p.id, &p.kind, &p.connID, &p.requestID, &p.payload, &p.created); err != nil {
			pendingRows.Close()
			return domain.RecoveryReport{}, err
		}
		pending = append(pending, p)
	}
	if err := pendingRows.Close(); err != nil {
		return domain.RecoveryReport{}, err
	}

	before, err := queryAllConnectionsTx(ctx, tx)
	if err != nil {
		return domain.RecoveryReport{}, err
	}
	stateBefore := make(map[string]string, len(before))
	for _, rec := range before {
		stateBefore[rec.ConnectionID] = rec.State
	}

	recovered := make([]domain.RecoveredIntent, 0, len(pending))
	allApplied := true
	for _, p := range pending {
		var payload map[string]any
		_ = json.Unmarshal([]byte(p.payload), &payload)
		from := stateBefore[p.connID]
		if from == "" {
			_ = tx.QueryRowContext(ctx, `SELECT state FROM connections WHERE connection_id=?`, p.connID).Scan(&from)
		}
		action := ""
		to := from
		detail := ""
		switch p.kind {
		case domain.IntentAcquire:
			action = "rollback_uncommitted_acquire"
			to = domain.StateAvailable
			if from == "" {
				allApplied = false
				detail = "connection row is missing; acquisition intent was recorded but target metadata cannot be restored"
			} else {
				_, err = tx.ExecContext(ctx, `UPDATE connections SET state=?, close_reason='' WHERE connection_id=?`,
					domain.StateAvailable, p.connID)
				if err != nil {
					return domain.RecoveryReport{}, err
				}
			}
		case domain.IntentRelease:
			action = "rollback_uncommitted_release"
			to = domain.StateInUse
			if from == "" {
				allApplied = false
				detail = "connection row is missing; release intent was recorded but target metadata cannot be restored"
			} else {
				_, err = tx.ExecContext(ctx, `UPDATE connections SET state=? WHERE connection_id=?`,
					domain.StateInUse, p.connID)
				if err != nil {
					return domain.RecoveryReport{}, err
				}
			}
		case domain.IntentInvalidate:
			action = "rollback_uncommitted_invalidate"
			if prior, ok := payload["previous_state"].(string); ok && prior != "" {
				to = prior
			} else {
				to = domain.StateInUse
			}
			if from == "" {
				allApplied = false
				detail = "connection row is missing; invalidation intent could not be rolled back"
			} else {
				_, err = tx.ExecContext(ctx, `UPDATE connections SET state=? WHERE connection_id=?`, to, p.connID)
				if err != nil {
					return domain.RecoveryReport{}, err
				}
			}
		case domain.IntentConfig:
			action = "config_change_not_applied"
			to = ""
			detail = "config intent was never committed; the previous monotonic config version remains active"
		default:
			action = "unrecognized_intent_recorded"
			allApplied = false
			detail = "no automatic state transition is defined for this intent kind"
		}
		if action != "config_change_not_applied" && to != "" {
			_, err = tx.ExecContext(ctx, `UPDATE intents SET status=?, recovered_at_ns=? WHERE intent_id=?`,
				domain.IntentRecovered, time.Now().UnixNano(), p.id)
			if err != nil {
				return domain.RecoveryReport{}, err
			}
		} else {
			_, err = tx.ExecContext(ctx, `UPDATE intents SET status=?, recovered_at_ns=? WHERE intent_id=?`,
				domain.IntentRecovered, time.Now().UnixNano(), p.id)
			if err != nil {
				return domain.RecoveryReport{}, err
			}
		}
		recovered = append(recovered, domain.RecoveredIntent{
			IntentID:     p.id,
			Kind:         p.kind,
			ConnectionID: p.connID,
			Action:       action,
			FromState:    from,
			ToState:      to,
			Detail:       detail,
		})
	}

	after, err := queryAllConnectionsTx(ctx, tx)
	if err != nil {
		return domain.RecoveryReport{}, err
	}
	stateAfter := make(map[string]string, len(after))
	for _, rec := range after {
		stateAfter[rec.ConnectionID] = rec.State
	}
	deltas := make([]domain.ConnectionStateDelta, 0, len(after))
	for _, rec := range after {
		deltas = append(deltas, domain.ConnectionStateDelta{
			ConnectionID: rec.ConnectionID,
			StateBefore:  stateBefore[rec.ConnectionID],
			StateAfter:   stateAfter[rec.ConnectionID],
		})
	}

	finished := time.Now()
	report := domain.RecoveryReport{
		ReportID:                  newID("recovery"),
		StartedAt:                 started,
		FinishedAt:                finished,
		PendingIntentsFound:       len(pending),
		Intents:                   recovered,
		Connections:               deltas,
		MetadataStatesRestored:    allApplied,
		RuntimeHandlesPreserved:   false,
		IdleQueueOrderRecoverable: false,
		FactsOnly:                 true,
		Notes: []string{
			"报告只基于强制终止前已提交到 SQLite 的连接行、配置行与 pending 意图。",
			"未完成 acquire 意图回滚为 available；未完成 release 意图回滚为 in_use。",
			"底层连接句柄、使用中归属和空闲队列的精确 FIFO 次序均未持久化，无法还原。",
			"本报告由重启进程生成，不是强制终止进程的退出报告。",
		},
	}
	reportJSON, err := json.Marshal(report)
	if err != nil {
		return domain.RecoveryReport{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO recovery_reports (report_id, created_at_ns, report_json)
		VALUES (?, ?, ?)`, report.ReportID, finished.UnixNano(), string(reportJSON))
	if err != nil {
		return domain.RecoveryReport{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.RecoveryReport{}, err
	}
	return report, nil
}

func (s *Store) LatestRecoveryReport(ctx context.Context) (domain.RecoveryReport, error) {
	var reportJSON string
	err := s.db.QueryRowContext(ctx, `SELECT report_json FROM recovery_reports ORDER BY created_at_ns DESC LIMIT 1`).
		Scan(&reportJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.RecoveryReport{}, fmt.Errorf("no recovery report")
	}
	if err != nil {
		return domain.RecoveryReport{}, err
	}
	var report domain.RecoveryReport
	if err := json.Unmarshal([]byte(reportJSON), &report); err != nil {
		return report, err
	}
	return report, nil
}

func queryAllConnectionsTx(ctx context.Context, tx *sql.Tx) ([]domain.ConnectionRecord, error) {
	rows, err := tx.QueryContext(ctx, connectionSelect+` ORDER BY created_at_ns ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanConnections(rows)
}
