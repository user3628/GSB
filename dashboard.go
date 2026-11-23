package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

func (s *Service) DashboardSnapshot(ctx context.Context, scheduler *Scheduler) (Dashboard, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Dashboard{}, err
	}
	defer tx.Rollback()
	out := Dashboard{GeneratedAtMS: nowMS(s.now()), Scheduler: scheduler.View()}

	topicRows, err := tx.QueryContext(ctx, `SELECT name, partitions, routing_rule, created_at_ms
		FROM topics ORDER BY name`)
	if err != nil {
		return out, err
	}
	out.Topics = []Topic{}
	for topicRows.Next() {
		var topic Topic
		if err := topicRows.Scan(&topic.Name, &topic.Partitions, &topic.RoutingRule, &topic.CreatedAtMS); err != nil {
			topicRows.Close()
			return out, err
		}
		out.Topics = append(out.Topics, topic)
	}
	if err := topicRows.Close(); err != nil {
		return out, err
	}

	partitionRows, err := tx.QueryContext(ctx, `SELECT topic, partition_no FROM partitions
		ORDER BY topic, partition_no`)
	if err != nil {
		return out, err
	}
	type partitionKey struct {
		topic string
		p     int
	}
	partitionKeys := []partitionKey{}
	for partitionRows.Next() {
		var k partitionKey
		if err := partitionRows.Scan(&k.topic, &k.p); err != nil {
			partitionRows.Close()
			return out, err
		}
		partitionKeys = append(partitionKeys, k)
	}
	if err := partitionRows.Close(); err != nil {
		return out, err
	}
	out.Partitions = []PartitionView{}
	for _, k := range partitionKeys {
		ps, err := s.loadPartitionTx(ctx, tx, k.topic, k.p)
		if err != nil {
			return out, err
		}
		view, err := s.partitionViewTx(ctx, tx, ps)
		if err != nil {
			return out, err
		}
		out.Partitions = append(out.Partitions, view)
	}

	txnRows, err := tx.QueryContext(ctx, `SELECT txn_id, message_id, topic, partition_no, seq,
		status, COALESCE(decision,''), query_count, COALESCE(coordinator_result,''),
		COALESCE(queried_at_ms,0), COALESCE(decided_at_ms,0)
		FROM txn_visibility WHERE status='PENDING' ORDER BY topic, partition_no, seq`)
	if err != nil {
		return out, err
	}
	out.PendingTxns = []Transaction{}
	for txnRows.Next() {
		var txn Transaction
		if err := txnRows.Scan(&txn.TxnID, &txn.MessageID, &txn.Topic, &txn.Partition,
			&txn.Seq, &txn.Status, &txn.Decision, &txn.QueryCount,
			&txn.CoordinatorResult, &txn.QueriedAtMS, &txn.DecidedAtMS); err != nil {
			txnRows.Close()
			return out, err
		}
		out.PendingTxns = append(out.PendingTxns, txn)
	}
	if err := txnRows.Close(); err != nil {
		return out, err
	}

	out.RecentMessages, err = s.listRecentMessagesTx(ctx, tx, 80)
	if err != nil {
		return out, err
	}
	out.Recovery, err = latestRecoveryReport(ctx, tx)
	if err != nil {
		return out, err
	}

	roundRows, err := tx.QueryContext(ctx, `SELECT id, started_at_ms, budget_ms, consumed_ms,
		remaining_ms, status, trace_json FROM scheduler_rounds ORDER BY id DESC LIMIT 8`)
	if err != nil {
		return out, err
	}
	out.BudgetRounds = []BudgetRound{}
	for roundRows.Next() {
		var round BudgetRound
		var raw string
		if err := roundRows.Scan(&round.ID, &round.StartedAtMS, &round.BudgetMS,
			&round.ConsumedMS, &round.RemainingMS, &round.Status, &raw); err != nil {
			roundRows.Close()
			return out, err
		}
		_ = json.Unmarshal([]byte(raw), &round)
		if round.Steps == nil {
			round.Steps = []BudgetStep{}
		}
		out.BudgetRounds = append(out.BudgetRounds, round)
	}
	if err := roundRows.Close(); err != nil {
		return out, err
	}

	obsRows, err := tx.QueryContext(ctx, `SELECT id, injection_id, kind, mode, target, phase,
		observed_code, detail, request_id, observed_at_ms FROM injection_observations
		ORDER BY id DESC LIMIT 60`)
	if err != nil {
		return out, err
	}
	out.Observations = []Observation{}
	for obsRows.Next() {
		var o Observation
		if err := obsRows.Scan(&o.ID, &o.InjectionID, &o.Kind, &o.Mode, &o.Target,
			&o.Phase, &o.ObservedCode, &o.Detail, &o.RequestID, &o.ObservedAtMS); err != nil {
			obsRows.Close()
			return out, err
		}
		out.Observations = append(out.Observations, o)
	}
	if err := obsRows.Close(); err != nil {
		return out, err
	}
	if err := tx.Commit(); err != nil {
		return out, err
	}
	return out, nil
}

func (s *Service) ListRecoveryReports(ctx context.Context, limit int) ([]RecoveryReport, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, started_at_ms, finished_at_ms,
		recovered_intents, details_json FROM recovery_reports ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RecoveryReport{}
	for rows.Next() {
		var report RecoveryReport
		var raw string
		if err := rows.Scan(&report.ID, &report.StartedAtMS, &report.FinishedAtMS,
			&report.RecoveredIntents, &raw); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(raw), &report.Details)
		if report.Details == nil {
			report.Details = []RecoveryDetail{}
		}
		out = append(out, report)
	}
	return out, rows.Err()
}

func (s *Service) WaitForRecovery(ctx context.Context, timeout time.Duration) (*RecoveryReport, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	deadline := time.Now().Add(timeout)
	for {
		report, err := s.LatestRecoveryReport(ctx)
		if err != nil || report != nil || time.Now().After(deadline) {
			return report, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}
