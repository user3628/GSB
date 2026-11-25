package bus

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"local/eventbus/internal/agg"
)

func (s *Service) Rewind(ctx context.Context, request RewindRequest) (RewindResult, error) {
	request.Topic = strings.TrimSpace(request.Topic)
	request.Group = strings.TrimSpace(request.Group)
	if request.Topic == "" || request.Group == "" {
		return RewindResult{}, invalidArgument("topic and group are required")
	}
	hash := contentHash("rewind", request)
	return idempotentJSON(s, ctx, "rewind", request.RequestID, hash, func() (RewindResult, error) {
		lock := s.partitionLock(request.Topic, request.Partition, request.Group)
		lock.Lock()
		defer lock.Unlock()

		if err := s.validateTopicGroup(ctx, s.db, request.Topic, request.Group); err != nil {
			return RewindResult{}, err
		}
		if err := s.validateTopicPartition(ctx, s.db, request.Topic, request.Partition); err != nil {
			return RewindResult{}, err
		}
		offset, err := s.loadOffset(ctx, s.db, request.Topic, request.Group, request.Partition)
		if err != nil {
			return RewindResult{}, err
		}
		algorithmName, err := s.groupAlgorithm(ctx, s.db, request.Topic, request.Group)
		if err != nil {
			return RewindResult{}, err
		}
		algorithm, err := algorithmFromString(algorithmName)
		if err != nil {
			return RewindResult{}, err
		}
		if request.TargetSeq <= 0 || request.TargetSeq > offset.AckedOffset {
			return RewindResult{}, NewError(CodeOffsetInvalid, fmt.Sprintf("target sequence %d is not committed; committed offset is %d", request.TargetSeq, offset.AckedOffset))
		}
		var earliestSeq int64
		if err := s.db.QueryRowContext(ctx,
			`SELECT earliest_seq FROM partition_counters WHERE topic = ? AND partition = ?`,
			request.Topic, request.Partition).Scan(&earliestSeq); err != nil {
			return RewindResult{}, WrapError(CodeInternal, "read retention floor", err)
		}
		if request.TargetSeq < earliestSeq {
			return RewindResult{}, NewError(CodeOffsetExpired, fmt.Sprintf("target sequence %d is older than retained sequence %d", request.TargetSeq, earliestSeq))
		}
		var targetExists int
		if err := s.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM events WHERE topic = ? AND partition = ? AND seq = ?`,
			request.Topic, request.Partition, request.TargetSeq).Scan(&targetExists); err != nil {
			return RewindResult{}, WrapError(CodeInternal, "validate target sequence", err)
		}
		if targetExists != 1 {
			return RewindResult{}, NewError(CodeOffsetExpired, "target sequence is no longer retained")
		}
		if request.TargetSeq == offset.AckedOffset {
			groupState, err := s.loadAggregateGroup(ctx, s.db, request.Topic, request.Group, request.Partition, algorithm)
			if err != nil {
				return RewindResult{}, err
			}
			return RewindResult{
				Topic: request.Topic, Group: request.Group, Partition: request.Partition,
				OldOffset: offset.AckedOffset, NewOffset: offset.AckedOffset,
				ReplayUntil: offset.ReplayUntil, StateDigest: groupState.Digest(),
			}, nil
		}

		prior := map[string]any{
			"acked_offset":     offset.AckedOffset,
			"replay_until":     offset.ReplayUntil,
			"replay_active":    offset.ReplayActive,
			"requested_target": request.TargetSeq,
		}
		it, err := s.beginIntent(ctx, "REWIND", request.RequestID, hash, request.Topic, request.Group, request.Partition, prior, request)
		if err != nil {
			return RewindResult{}, err
		}
		if err := s.injectFault("rewind_after_intent"); err != nil {
			return RewindResult{}, err
		}
		tx, release, err := s.beginWriteTx(ctx)
		if err != nil {
			_ = s.finishIntent(context.WithoutCancel(ctx), it.ID, "ROLLED_BACK")
			return RewindResult{}, WrapError(CodeInternal, "begin rewind transaction", err)
		}
		defer release()
		defer tx.Rollback()

		offset, err = s.loadOffset(ctx, tx, request.Topic, request.Group, request.Partition)
		if err != nil {
			return RewindResult{}, err
		}
		if offset.AckedOffset != prior["acked_offset"].(int64) || request.TargetSeq > offset.AckedOffset {
			return RewindResult{}, NewError(CodeOffsetInvalid, "offset changed before rewind")
		}
		now := s.nowMS()

		// Undo affected aggregate batches newest-first. The final accumulator is
		// rebuilt from the remaining durable acknowledgements below, so a count
		// and its metric sum can never be exposed in different versions.
		rows, err := tx.QueryContext(ctx, `
			SELECT id, start_seq, end_seq FROM agg_batches
			WHERE topic = ? AND group_name = ? AND partition = ? AND active = 1 AND end_seq > ?
			ORDER BY id DESC`, request.Topic, request.Group, request.Partition, request.TargetSeq)
		if err != nil {
			return RewindResult{}, WrapError(CodeInternal, "read affected aggregate batches", err)
		}
		type affectedBatch struct {
			id, start, end int64
		}
		var affected []affectedBatch
		for rows.Next() {
			var batch affectedBatch
			if err := rows.Scan(&batch.id, &batch.start, &batch.end); err != nil {
				rows.Close()
				return RewindResult{}, WrapError(CodeInternal, "scan affected aggregate batch", err)
			}
			affected = append(affected, batch)
		}
		if err := rows.Close(); err != nil {
			return RewindResult{}, WrapError(CodeInternal, "close affected aggregate batches", err)
		}
		for _, batch := range affected {
			if _, err := tx.ExecContext(ctx, `
				UPDATE agg_batches SET active = 0, reverted_at_ms = ?, reverted_by_intent_id = ?
				WHERE id = ?`, now, it.ID, batch.id); err != nil {
				return RewindResult{}, WrapError(CodeInternal, "revert aggregate batch", err)
			}
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE acks SET active = 0
			WHERE topic = ? AND group_name = ? AND partition = ? AND active = 1 AND seq > ?`,
			request.Topic, request.Group, request.Partition, request.TargetSeq); err != nil {
			return RewindResult{}, WrapError(CodeInternal, "revert acknowledgements", err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE agg_snapshots SET active = 0
			WHERE topic = ? AND group_name = ? AND partition = ? AND active = 1 AND offset_value > ?`,
			request.Topic, request.Group, request.Partition, request.TargetSeq); err != nil {
			return RewindResult{}, WrapError(CodeInternal, "revert aggregate snapshots", err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE event_states SET state = 'REDELIVERED', lease_token = '', updated_at_ms = ?,
				transition_reason = 'OFFSET_REWIND', last_intent_id = ?
			WHERE topic = ? AND group_name = ? AND partition = ? AND seq > ?
				AND state IN ('ACKED', 'DISPATCHED', 'DEAD_LETTERED')`,
			now, it.ID, request.Topic, request.Group, request.Partition, request.TargetSeq); err != nil {
			return RewindResult{}, WrapError(CodeInternal, "rewind event states", err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE event_states SET lease_token = '', updated_at_ms = ?, transition_reason = 'OFFSET_REWIND',
				last_intent_id = ?
			WHERE topic = ? AND group_name = ? AND partition = ? AND seq > ? AND state = 'REDELIVERED'`,
			now, it.ID, request.Topic, request.Group, request.Partition, request.TargetSeq); err != nil {
			return RewindResult{}, WrapError(CodeInternal, "clear rewound lease tokens", err)
		}

		groupState, err := s.rebuildAggregateFromAcks(ctx, tx, request.Topic, request.Group, request.Partition, request.TargetSeq, algorithm)
		if err != nil {
			return RewindResult{}, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM agg_states WHERE topic = ? AND group_name = ? AND partition = ?`,
			request.Topic, request.Group, request.Partition); err != nil {
			return RewindResult{}, WrapError(CodeInternal, "clear aggregate state", err)
		}
		if err := s.persistAggregateGroup(ctx, tx, request.Topic, request.Group, request.Partition, &groupState); err != nil {
			return RewindResult{}, err
		}
		version := offset.Version + 1
		digest, err := s.saveAggregateSnapshot(ctx, tx, request.Topic, request.Group, request.Partition,
			request.TargetSeq, version, &groupState, 0)
		if err != nil {
			return RewindResult{}, err
		}
		// If the rewind point cuts through an acknowledged batch, retain an
		// active audit segment for its prefix so the ledger still accounts for
		// every acknowledged event that survived the rollback.
		for _, batch := range affected {
			if batch.start <= request.TargetSeq && request.TargetSeq < batch.end {
				retainedEvents, err := s.loadEventRange(ctx, tx, request.Topic, request.Partition, batch.start, request.TargetSeq)
				if err != nil {
					return RewindResult{}, err
				}
				partials, sumAbs, bound, err := batchPartials(algorithm, retainedEvents)
				if err != nil {
					return RewindResult{}, err
				}
				var partialSum float64
				for _, key := range sortedKeys(partials) {
					partialSum += partials[key].Sum
				}
				partialsJSON, _ := json.Marshal(partials)
				eventBatches := eventBatchIDs(retainedEvents)
				if _, err := tx.ExecContext(ctx, `
					INSERT INTO agg_batches(topic, group_name, partition, ack_batch_id, event_batch_ids_json,
						start_seq, end_seq, algorithm, event_count, partials_json, sum_abs, abs_error_bound,
						relative_error_bound, state_digest, previous_offset, new_offset, version, active,
						intent_id, created_at_ms)
					VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)`,
					request.Topic, request.Group, request.Partition,
					fmt.Sprintf("rewind-retained-%d-%d", batch.id, request.TargetSeq),
					marshalStringSlice(eventBatches), batch.start, request.TargetSeq, algorithm,
					int64(len(retainedEvents)), string(partialsJSON), sumAbs, bound,
					relativeBound(bound, partialSum), digest, batch.start-1, request.TargetSeq,
					version, it.ID, now); err != nil {
					return RewindResult{}, WrapError(CodeInternal, "record retained aggregate segment", err)
				}
				break
			}
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE consumer_offsets SET acked_offset = ?, replay_until = ?, replay_active = 1,
				version = ?, lease_token = '', lease_owner = '', lease_expires_at_ms = 0,
				updated_at_ms = ?, last_intent_id = ?
			WHERE topic = ? AND group_name = ? AND partition = ?`,
			request.TargetSeq, offset.AckedOffset, version, now, it.ID,
			request.Topic, request.Group, request.Partition); err != nil {
			return RewindResult{}, WrapError(CodeInternal, "apply offset rewind", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO offset_history(topic, group_name, partition, old_offset, new_offset, reason, intent_id, created_at_ms)
			VALUES(?, ?, ?, ?, ?, 'REWIND', ?, ?)`,
			request.Topic, request.Group, request.Partition, offset.AckedOffset, request.TargetSeq, it.ID, now); err != nil {
			return RewindResult{}, WrapError(CodeInternal, "record offset rewind", err)
		}
		if err := s.injectFault("rewind_before_commit"); err != nil {
			return RewindResult{}, err
		}
		if err := tx.Commit(); err != nil {
			_ = s.finishIntent(context.WithoutCancel(ctx), it.ID, "ROLLED_BACK")
			return RewindResult{}, WrapError(CodeInternal, "commit rewind", err)
		}
		release()
		if err := s.injectFault("rewind_after_commit"); err != nil {
			return RewindResult{}, err
		}
		if err := s.finishIntent(ctx, it.ID, "COMMITTED"); err != nil {
			return RewindResult{}, err
		}
		return RewindResult{
			Topic: request.Topic, Group: request.Group, Partition: request.Partition,
			OldOffset: offset.AckedOffset, NewOffset: request.TargetSeq,
			ReplayUntil: offset.AckedOffset, RevertedBatches: len(affected), StateDigest: digest,
		}, nil
	})
}

func (s *Service) rebuildAggregateFromAcks(
	ctx context.Context,
	tx *sql.Tx,
	topic, group string,
	partition int,
	target int64,
	algorithm agg.Algorithm,
) (agg.Group, error) {
	result := agg.NewGroup(algorithm)
	rows, err := tx.QueryContext(ctx, `
		SELECT e.event_id, e.topic, e.partition, e.seq, e.partition_key, e.event_time_ms,
			e.body_digest, e.batch_id, e.aggregate_key, e.metric, e.content_hash, e.created_at_ms
		FROM acks a JOIN events e
			ON e.topic = a.topic AND e.partition = a.partition AND e.seq = a.seq
		WHERE a.topic = ? AND a.group_name = ? AND a.partition = ? AND a.active = 1 AND a.seq <= ?
		ORDER BY a.seq`, topic, group, partition, target)
	if err != nil {
		return result, WrapError(CodeInternal, "read acknowledged events", err)
	}
	defer rows.Close()
	for rows.Next() {
		var event storedEvent
		if err := rows.Scan(&event.EventID, &event.Topic, &event.Partition, &event.Seq, &event.PartitionKey,
			&event.EventTimeMS, &event.BodyDigest, &event.BatchID, &event.AggregateKey, &event.Metric,
			&event.ContentHash, &event.CreatedAtMS); err != nil {
			return result, WrapError(CodeInternal, "scan acknowledged event", err)
		}
		result.Add(event.AggregateKey, event.Metric)
	}
	if err := rows.Err(); err != nil {
		return result, WrapError(CodeInternal, "iterate acknowledged events", err)
	}
	result.Finalize()
	return result, nil
}
