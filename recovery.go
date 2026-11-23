package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func parsePartitionTarget(target string) (string, int, bool) {
	idx := strings.LastIndex(target, "/")
	if idx <= 0 || idx == len(target)-1 {
		return "", 0, false
	}
	p, err := strconv.Atoi(target[idx+1:])
	if err != nil {
		return "", 0, false
	}
	return target[:idx], p, true
}

func (s *Service) recoverIntentTx(ctx context.Context, tx *sql.Tx, intent Intent) (RecoveryDetail, error) {
	detail := RecoveryDetail{IntentID: intent.IntentID, RequestID: intent.RequestID,
		Operation: intent.Operation, Target: intent.Target}
	switch intent.Operation {
	case "pull":
		topic, partition, ok := parsePartitionTarget(intent.Target)
		if ok {
			ps, err := s.loadPartitionTx(ctx, tx, topic, partition)
			if err == nil {
				detail.Action = "pull_rolled_back_to_consumable"
				detail.Evidence = fmt.Sprintf("persisted lease_state=%s lease_version=%d consume_cursor=%d; prepared pull had no committed delivery reservation",
					ps.LeaseState, ps.LeaseVersion, ps.ConsumeCursor)
				detail.StateRestored = true
			}
		}
		if detail.Action == "" {
			detail.Action = "pull_aborted_without_delivery"
			detail.Evidence = "prepared pull intent had no committed state mutation"
		}
	case "commit":
		topic, partition, ok := parsePartitionTarget(intent.Target)
		if ok {
			var req CommitRequest
			_ = json.Unmarshal([]byte(intent.Payload), &req)
			ps, err := s.loadPartitionTx(ctx, tx, topic, partition)
			if err == nil {
				detail.Action = "commit_rolled_back_to_old_cursor"
				detail.Evidence = fmt.Sprintf("persisted consume_cursor=%d; through_seq=%d was not committed because intent is PREPARED",
					ps.ConsumeCursor, req.ThroughSeq)
				detail.StateRestored = ps.ConsumeCursor <= req.ThroughSeq
			}
		}
		if detail.Action == "" {
			detail.Action = "commit_aborted_without_cursor_change"
			detail.Evidence = "prepared commit intent had no committed cursor mutation"
		}
	case "replay":
		topic, partition, ok := parsePartitionTarget(intent.Target)
		if ok {
			var req ReplayRequest
			_ = json.Unmarshal([]byte(intent.Payload), &req)
			ps, err := s.loadPartitionTx(ctx, tx, topic, partition)
			if err == nil {
				detail.Action = "replay_aborted"
				if ps.ReplayActive && ps.ReplayFrom != nil && *ps.ReplayFrom == req.FromSeq {
					ps.ReplayActive = false
					if ps.ReplayLimit != nil {
						ps.ConsumeCursor = *ps.ReplayLimit
					}
					ps.ReplayFrom = nil
					ps.ReplayLimit = nil
					if err := s.savePartitionTx(ctx, tx, ps); err != nil {
						return detail, err
					}
					detail.StateRestored = true
					detail.Evidence = fmt.Sprintf("persisted replay was aborted; consume_cursor restored to %d", ps.ConsumeCursor)
				} else {
					detail.StateRestored = !ps.ReplayActive
					detail.Evidence = fmt.Sprintf("persisted replay_active=%t replay_from=%v; prepared replay never committed", ps.ReplayActive, ps.ReplayFrom)
				}
			}
		}
		if detail.Action == "" {
			detail.Action = "replay_aborted_without_state_mutation"
			detail.Evidence = "prepared replay intent had no committed replay state"
		}
	case "txn_resolve":
		var status, messageID, topic string
		var partition int
		err := tx.QueryRowContext(ctx, `SELECT status, message_id, topic, partition_no
			FROM txn_visibility WHERE txn_id=?`, intent.Target).Scan(&status, &messageID, &topic, &partition)
		if err == nil {
			var msgStatus string
			_ = tx.QueryRowContext(ctx, `SELECT status FROM messages WHERE message_id=?`, messageID).Scan(&msgStatus)
			detail.Action = "transaction_left_pending"
			detail.Evidence = fmt.Sprintf("persisted txn_status=%s message_status=%s; no coordinator conclusion was committed", status, msgStatus)
			detail.StateRestored = status == TxnPending || status == TxnCommitted || status == TxnRolledBack
		}
		if detail.Action == "" {
			detail.Action = "transaction_intent_aborted"
			detail.Evidence = "persisted transaction row was not found; no conclusion was committed"
		}
	default:
		detail.Action = "operation_aborted"
		detail.Evidence = "prepared intent had no committed business response"
		detail.StateRestored = true
	}
	return detail, nil
}

// RecoverIntents scans only already-persisted PREPARED intents. The final
// mutation and the intent completion share one SQLite commit, so a PREPARED
// intent means the business transaction did not commit. The recovery report is
// built from the intent rows and the persisted partition/message facts observed
// during this scan.
func (s *Service) RecoverIntents(ctx context.Context) (RecoveryReport, error) {
	start := nowMS(s.now())
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RecoveryReport{}, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT intent_id, request_id, operation, target,
		payload_json, status, COALESCE(result_json,''), created_at_ms, updated_at_ms
		FROM intents WHERE status=? ORDER BY created_at_ms, intent_id`, IntentPrepared)
	if err != nil {
		return RecoveryReport{}, err
	}
	intents := []Intent{}
	for rows.Next() {
		var intent Intent
		if err := rows.Scan(&intent.IntentID, &intent.RequestID, &intent.Operation,
			&intent.Target, &intent.Payload, &intent.Status, &intent.Result,
			&intent.CreatedAtMS, &intent.UpdatedAtMS); err != nil {
			rows.Close()
			return RecoveryReport{}, err
		}
		intents = append(intents, intent)
	}
	if err := rows.Close(); err != nil {
		return RecoveryReport{}, err
	}
	details := make([]RecoveryDetail, 0, len(intents))
	now := nowMS(s.now())
	for _, intent := range intents {
		detail, err := s.recoverIntentTx(ctx, tx, intent)
		if err != nil {
			return RecoveryReport{}, err
		}
		details = append(details, detail)
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET status=?, recovery_action=?,
			recovery_evidence=?, updated_at_ms=? WHERE intent_id=? AND status=?`,
			IntentRecovered, detail.Action, detail.Evidence, now, intent.IntentID, IntentPrepared); err != nil {
			return RecoveryReport{}, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE requests SET status=?, error_code=?,
			error_message=?, finished_at_ms=? WHERE request_id=? AND status=?`,
			RequestFailed, CodeIntentRecovered, "request interrupted by process termination; recovered from prepared intent",
			now, intent.RequestID, RequestRunning); err != nil {
			return RecoveryReport{}, err
		}
	}
	report := RecoveryReport{StartedAtMS: start, FinishedAtMS: now,
		RecoveredIntents: int64(len(intents)), Details: details}
	raw, err := json.Marshal(details)
	if err != nil {
		return RecoveryReport{}, err
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO recovery_reports
		(started_at_ms, finished_at_ms, recovered_intents, details_json) VALUES (?,?,?,?)`,
		report.StartedAtMS, report.FinishedAtMS, report.RecoveredIntents, string(raw))
	if err != nil {
		return RecoveryReport{}, err
	}
	report.ID, _ = res.LastInsertId()
	if err := tx.Commit(); err != nil {
		return RecoveryReport{}, err
	}
	return report, nil
}

func (s *Service) LatestRecoveryReport(ctx context.Context) (*RecoveryReport, error) {
	return latestRecoveryReport(ctx, s.db)
}

type queryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func latestRecoveryReport(ctx context.Context, q queryRower) (*RecoveryReport, error) {
	var report RecoveryReport
	var raw string
	err := q.QueryRowContext(ctx, `SELECT id, started_at_ms, finished_at_ms,
		recovered_intents, details_json FROM recovery_reports ORDER BY id DESC LIMIT 1`).Scan(
		&report.ID, &report.StartedAtMS, &report.FinishedAtMS,
		&report.RecoveredIntents, &raw)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(raw), &report.Details); err != nil {
		return nil, err
	}
	if report.Details == nil {
		report.Details = []RecoveryDetail{}
	}
	return &report, nil
}

func (s *Service) StartLeaseReaper(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.reapExpiredLeases(ctx)
			}
		}
	}()
}

func (s *Service) reapExpiredLeases(ctx context.Context) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT topic, partition_no FROM partitions
		WHERE lease_state='LEASED' AND lease_expires_at_ms IS NOT NULL AND lease_expires_at_ms <= ?`,
		nowMS(s.now()))
	if err != nil {
		return
	}
	type key struct {
		topic string
		p     int
	}
	keys := []key{}
	for rows.Next() {
		var k key
		if err := rows.Scan(&k.topic, &k.p); err == nil {
			keys = append(keys, k)
		}
	}
	rows.Close()
	for _, k := range keys {
		ps, err := s.loadPartitionTx(ctx, tx, k.topic, k.p)
		if err != nil {
			continue
		}
		if _, err := s.expireLeaseIfNeededTx(ctx, tx, ps); err != nil {
			continue
		}
	}
	_ = tx.Commit()
}
