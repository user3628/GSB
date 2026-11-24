package engine

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"checkpointengine/internal/model"
)

type roundUsage struct {
	Round         int
	PolicyVersion int64
	BytesLimit    int64
	BytesUsed     int64
	TimeLimitMS   int64
	TimeUsedMS    int64
}

type scheduleResult struct {
	Writes          []model.WriteTask
	Deferred        []model.WriteTask
	QueueDepth      int
	OrderBasis      string
	RemainingBytes  int64
	RemainingTimeMS int64
	Usage           map[int]roundUsage
}

func scheduleWrites(ctx context.Context, tx *sql.Tx, policy model.Snapshot, shards []model.Shard) (scheduleResult, error) {
	usage := make(map[int]roundUsage)
	rows, err := tx.QueryContext(ctx, `SELECT round, policy_version, bytes_limit, bytes_used,
		time_limit_ms, time_used_ms FROM round_budget`)
	if err != nil {
		return scheduleResult{}, err
	}
	for rows.Next() {
		var u roundUsage
		if err := rows.Scan(&u.Round, &u.PolicyVersion, &u.BytesLimit, &u.BytesUsed,
			&u.TimeLimitMS, &u.TimeUsedMS); err != nil {
			rows.Close()
			return scheduleResult{}, err
		}
		usage[u.Round] = u
	}
	if err := rows.Close(); err != nil {
		return scheduleResult{}, err
	}
	if err := rows.Err(); err != nil {
		return scheduleResult{}, err
	}

	var tasks []model.WriteTask
	for _, s := range shards {
		if s.WriteState != model.WritePending {
			continue
		}
		tasks = append(tasks, model.WriteTask{
			Round: s.Round, ShardID: s.ShardID, Precision: s.Precision,
			Digest: s.Digest, Bytes: s.Bytes, Priority: s.Priority,
			WriteMS: s.WriteMS, Status: model.WritePending,
		})
	}
	preferenceRank := func(precision string) int {
		if policy.WritePreference == model.WritePreferenceFP16First {
			if precision == model.PrecisionFP16 {
				return 0
			}
			return 1
		}
		if precision == model.PrecisionFP32 {
			return 0
		}
		return 1
	}
	sort.Slice(tasks, func(i, j int) bool {
		ri, rj := preferenceRank(tasks[i].Precision), preferenceRank(tasks[j].Precision)
		if ri != rj {
			return ri < rj
		}
		if tasks[i].Priority != tasks[j].Priority {
			return tasks[i].Priority > tasks[j].Priority
		}
		if tasks[i].Round != tasks[j].Round {
			return tasks[i].Round < tasks[j].Round
		}
		return tasks[i].ShardID < tasks[j].ShardID
	})

	var writes, deferred []model.WriteTask
	for _, task := range tasks {
		u, ok := usage[task.Round]
		if !ok {
			u = roundUsage{Round: task.Round, PolicyVersion: policy.Version,
				BytesLimit: policy.WriteBudgetBytesPerRound, TimeLimitMS: policy.TimeBudgetMS}
		} else {
			u.PolicyVersion = policy.Version
			u.BytesLimit = policy.WriteBudgetBytesPerRound
			u.TimeLimitMS = policy.TimeBudgetMS
		}
		byteRemaining := u.BytesLimit - u.BytesUsed
		timeRemaining := u.TimeLimitMS - u.TimeUsedMS
		if byteRemaining < 0 {
			byteRemaining = 0
		}
		if timeRemaining < 0 {
			timeRemaining = 0
		}
		switch {
		case task.Bytes > byteRemaining && task.WriteMS > timeRemaining:
			task.QueueReason = "BYTE_AND_TIME_BUDGET_EXHAUSTED"
		case task.Bytes > byteRemaining:
			task.QueueReason = "BYTE_BUDGET_EXHAUSTED"
		case task.WriteMS > timeRemaining:
			task.QueueReason = "TIME_BUDGET_EXHAUSTED"
		}
		if task.QueueReason != "" {
			task.Status = model.WritePending
			deferred = append(deferred, task)
			usage[task.Round] = u
			continue
		}
		task.Status = model.WriteWritten
		writes = append(writes, task)
		u.BytesUsed = saturatingAdd(u.BytesUsed, task.Bytes)
		u.TimeUsedMS = saturatingAdd(u.TimeUsedMS, task.WriteMS)
		usage[task.Round] = u
	}

	if len(deferred) > policy.WriteQueueCapacity {
		return scheduleResult{}, model.WithDetails(model.NewAPIError(model.CodeWriteQueueFull, "bounded write wait queue is full"),
			map[string]any{"queue_size": len(deferred), "queue_capacity": policy.WriteQueueCapacity,
				"deferred_count": len(deferred), "scheduled_count": len(writes)})
	}

	orderBasis := fmt.Sprintf("%s precision class first, then priority descending, round ascending, and shard id; byte and time budgets are consumed per round", policy.WritePreference)
	return scheduleResult{
		Writes: writes, Deferred: deferred, QueueDepth: len(deferred), OrderBasis: orderBasis,
		RemainingBytes:  minRemainingBytes(usage, policy),
		RemainingTimeMS: minRemainingTime(usage, policy),
		Usage:           usage,
	}, nil
}

func minRemainingBytes(usage map[int]roundUsage, policy model.Snapshot) int64 {
	out := policy.WriteBudgetBytesPerRound
	found := false
	for _, u := range usage {
		remaining := u.BytesLimit - u.BytesUsed
		if remaining < 0 {
			remaining = 0
		}
		if !found || remaining < out {
			out = remaining
			found = true
		}
	}
	return out
}

func minRemainingTime(usage map[int]roundUsage, policy model.Snapshot) int64 {
	out := policy.TimeBudgetMS
	found := false
	for _, u := range usage {
		remaining := u.TimeLimitMS - u.TimeUsedMS
		if remaining < 0 {
			remaining = 0
		}
		if !found || remaining < out {
			out = remaining
			found = true
		}
	}
	return out
}

func applyScheduleTx(ctx context.Context, tx *sql.Tx, submissionID string, sched scheduleResult, now string, nowMS int64) error {
	for _, task := range sched.Writes {
		if _, err := tx.ExecContext(ctx, `UPDATE shards SET write_state='WRITTEN' WHERE round=? AND shard_id=?`, task.Round, task.ShardID); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM write_queue`); err != nil {
		return err
	}
	for _, task := range sched.Deferred {
		if _, err := tx.ExecContext(ctx, `INSERT INTO write_queue(round, shard_id, precision, digest, bytes,
			priority, write_ms, submission_id, status, enqueued_ms, reason)
			VALUES(?,?,?,?,?,?,?,?, 'WAITING', ?, ?)`, task.Round, task.ShardID, task.Precision,
			task.Digest, task.Bytes, task.Priority, task.WriteMS, submissionID, nowMS, task.QueueReason); err != nil {
			return err
		}
	}
	for _, u := range sched.Usage {
		if _, err := tx.ExecContext(ctx, `INSERT INTO round_budget(round, policy_version, bytes_limit,
			bytes_used, time_limit_ms, time_used_ms, updated_at) VALUES(?,?,?,?,?,?,?)
			ON CONFLICT(round) DO UPDATE SET policy_version=excluded.policy_version,
			bytes_limit=excluded.bytes_limit, bytes_used=excluded.bytes_used,
			time_limit_ms=excluded.time_limit_ms, time_used_ms=excluded.time_used_ms,
			updated_at=excluded.updated_at`, u.Round, u.PolicyVersion, u.BytesLimit,
			u.BytesUsed, u.TimeLimitMS, u.TimeUsedMS, now); err != nil {
			return err
		}
	}
	return nil
}
