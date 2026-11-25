package bus

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

type offsetRow struct {
	AckedOffset      int64
	ReplayUntil      int64
	ReplayActive     bool
	Version          int64
	LeaseToken       string
	LeaseOwner       string
	LeaseExpiresAtMS int64
	UpdatedAtMS      int64
}

func (s *Service) Pull(ctx context.Context, request PullRequest) (PullResult, error) {
	request.Topic = strings.TrimSpace(request.Topic)
	request.Group = strings.TrimSpace(request.Group)
	request.ConsumerID = strings.TrimSpace(request.ConsumerID)
	if request.Topic == "" || request.Group == "" || request.ConsumerID == "" {
		return PullResult{}, invalidArgument("topic, group and consumer_id are required")
	}
	if request.MaxEvents <= 0 {
		request.MaxEvents = 1
	}
	if request.MaxEvents > 256 {
		return PullResult{}, invalidArgument("max_events must be at most 256")
	}
	hash := contentHash("pull", request)
	return idempotentJSON(s, ctx, "pull", request.RequestID, hash, func() (PullResult, error) {
		lock := s.partitionLock(request.Topic, request.Partition, request.Group)
		lock.Lock()
		defer lock.Unlock()

		if err := s.validateTopicGroup(ctx, s.db, request.Topic, request.Group); err != nil {
			return PullResult{}, err
		}
		if err := s.validateTopicPartition(ctx, s.db, request.Topic, request.Partition); err != nil {
			return PullResult{}, err
		}
		offset, err := s.loadOffset(ctx, s.db, request.Topic, request.Group, request.Partition)
		if err != nil {
			return PullResult{}, err
		}
		now := s.nowMS()
		if offset.LeaseToken != "" && offset.LeaseExpiresAtMS > now {
			return PullResult{}, NewError(CodeLeaseHeld, fmt.Sprintf("lease for %s/%s/%d is held by %s until %d",
				request.Topic, request.Group, request.Partition, offset.LeaseOwner, offset.LeaseExpiresAtMS))
		}

		var lastSeq, earliestSeq int64
		if err := s.db.QueryRowContext(ctx,
			`SELECT last_seq, earliest_seq FROM partition_counters WHERE topic = ? AND partition = ?`,
			request.Topic, request.Partition).Scan(&lastSeq, &earliestSeq); err != nil {
			return PullResult{}, WrapError(CodeInternal, "read partition counter", err)
		}
		start := offset.AckedOffset + 1
		if start < earliestSeq {
			return PullResult{}, NewError(CodeOffsetExpired, fmt.Sprintf("consumer offset %d is older than retained sequence %d", offset.AckedOffset, earliestSeq))
		}
		replayActive := offset.ReplayActive
		replayUntil := offset.ReplayUntil
		allowedEnd := lastSeq
		if replayActive {
			if offset.AckedOffset >= replayUntil {
				replayActive = false
				replayUntil = 0
			} else if allowedEnd > replayUntil {
				allowedEnd = replayUntil
			}
		}
		if start > allowedEnd {
			return PullResult{}, NewError(CodeNoEvent, "no dispatchable event after the consumer offset")
		}
		end := start + int64(request.MaxEvents) - 1
		if end > allowedEnd {
			end = allowedEnd
		}
		events, err := s.loadEventRange(ctx, s.db, request.Topic, request.Partition, start, end)
		if err != nil {
			return PullResult{}, err
		}
		if len(events) == 0 {
			return PullResult{}, NewError(CodeNoEvent, "no dispatchable event after the consumer offset")
		}
		if events[0].Seq != start || events[len(events)-1].Seq != end {
			return PullResult{}, NewError(CodeInternal, "event sequence is not gapless")
		}

		prior := map[string]any{
			"acked_offset": offset.AckedOffset,
			"lease_owner":  offset.LeaseOwner,
			"lease_token":  offset.LeaseToken,
		}
		it, err := s.beginIntent(ctx, "PULL", request.RequestID, hash, request.Topic, request.Group, request.Partition, prior, request)
		if err != nil {
			return PullResult{}, err
		}
		if err := s.injectFault("pull_after_intent"); err != nil {
			return PullResult{}, err
		}
		tx, release, err := s.beginWriteTx(ctx)
		if err != nil {
			_ = s.finishIntent(context.WithoutCancel(ctx), it.ID, "ROLLED_BACK")
			return PullResult{}, WrapError(CodeInternal, "begin pull transaction", err)
		}
		defer release()
		defer tx.Rollback()

		// Re-read under the transaction; the partition lock protects this row from
		// other writers in this process.
		offset, err = s.loadOffset(ctx, tx, request.Topic, request.Group, request.Partition)
		if err != nil {
			return PullResult{}, err
		}
		if offset.LeaseToken != "" && offset.LeaseExpiresAtMS > now {
			return PullResult{}, NewError(CodeLeaseHeld, "lease was acquired concurrently")
		}
		if offset.LeaseToken != "" {
			if _, err := tx.ExecContext(ctx, `
				UPDATE event_states
				SET state = 'REDELIVERED', lease_token = '', updated_at_ms = ?, transition_reason = 'LEASE_EXPIRED'
				WHERE topic = ? AND group_name = ? AND partition = ? AND lease_token = ? AND state = 'DISPATCHED'`,
				now, request.Topic, request.Group, request.Partition, offset.LeaseToken); err != nil {
				return PullResult{}, WrapError(CodeInternal, "release expired lease", err)
			}
		}
		if offset.ReplayActive && offset.AckedOffset >= offset.ReplayUntil {
			replayActive = false
			replayUntil = 0
		}
		leaseToken := randomID("lease_")
		expiresAt := now + s.leaseDuration.Milliseconds()
		if expiresAt <= now {
			expiresAt = now + 1
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO consumer_offsets(topic, group_name, partition, acked_offset, replay_until, replay_active,
				version, lease_token, lease_owner, lease_expires_at_ms, updated_at_ms, last_intent_id)
			VALUES(?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
			ON CONFLICT(topic, group_name, partition) DO UPDATE SET
				replay_active = excluded.replay_active,
				replay_until = excluded.replay_until,
				version = consumer_offsets.version + 1,
				lease_token = excluded.lease_token,
				lease_owner = excluded.lease_owner,
				lease_expires_at_ms = excluded.lease_expires_at_ms,
				updated_at_ms = excluded.updated_at_ms,
				last_intent_id = excluded.last_intent_id`,
			request.Topic, request.Group, request.Partition, offset.AckedOffset, replayUntil, boolInt(replayActive),
			leaseToken, request.ConsumerID, expiresAt, now, it.ID); err != nil {
			return PullResult{}, WrapError(CodeInternal, "grant delivery lease", err)
		}

		delivered := make([]DeliveredEvent, 0, len(events))
		for _, event := range events {
			state, deliveryCount, err := s.loadEventState(ctx, tx, request.Topic, request.Group, request.Partition, event.Seq)
			if err != nil {
				return PullResult{}, err
			}
			if state == "" {
				state = StateAccepted
			}
			if state == StateDispatched {
				// A stale DISPATCHED row can remain if a lease record was lost. It
				// must be visibly redelivered before the new grant.
				if !AllowedTransition(state, StateRedelivered) {
					return PullResult{}, NewError(CodeInvalidTransition, fmt.Sprintf("cannot redeliver %s", state))
				}
				state = StateRedelivered
			}
			if !AllowedTransition(state, StateDispatched) {
				return PullResult{}, NewError(CodeInvalidTransition, fmt.Sprintf("cannot dispatch %s at seq %d", state, event.Seq))
			}
			deliveryCount++
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO event_states(topic, group_name, partition, seq, event_id, state, delivery_count,
					lease_token, updated_at_ms, transition_reason, last_intent_id)
				VALUES(?, ?, ?, ?, ?, 'DISPATCHED', ?, ?, ?, 'PULL', ?)
				ON CONFLICT(topic, group_name, partition, seq) DO UPDATE SET
					state = 'DISPATCHED',
					delivery_count = event_states.delivery_count + 1,
					lease_token = excluded.lease_token,
					updated_at_ms = excluded.updated_at_ms,
					transition_reason = 'PULL',
					last_intent_id = excluded.last_intent_id`,
				request.Topic, request.Group, request.Partition, event.Seq, event.EventID, deliveryCount,
				leaseToken, now, it.ID); err != nil {
				return PullResult{}, WrapError(CodeInternal, "persist delivery state", err)
			}
			item := event.delivered()
			item.State = StateDispatched
			item.DeliveryCount = deliveryCount
			delivered = append(delivered, item)
		}
		if err := tx.Commit(); err != nil {
			_ = s.finishIntent(context.WithoutCancel(ctx), it.ID, "ROLLED_BACK")
			return PullResult{}, WrapError(CodeInternal, "commit pull", err)
		}
		release()
		if err := s.injectFault("pull_after_commit"); err != nil {
			return PullResult{}, err
		}
		if err := s.finishIntent(ctx, it.ID, "COMMITTED"); err != nil {
			return PullResult{}, err
		}
		return PullResult{
			LeaseToken:  leaseToken,
			ExpiresAtMS: expiresAt,
			Replay:      replayActive,
			Events:      delivered,
		}, nil
	})
}

func (s *Service) loadOffset(ctx context.Context, q queryer, topic, group string, partition int) (offsetRow, error) {
	var row offsetRow
	var replayActive int
	err := q.QueryRowContext(ctx, `
		SELECT acked_offset, replay_until, replay_active, version, lease_token, lease_owner,
			lease_expires_at_ms, updated_at_ms
		FROM consumer_offsets WHERE topic = ? AND group_name = ? AND partition = ?`,
		topic, group, partition).
		Scan(&row.AckedOffset, &row.ReplayUntil, &replayActive, &row.Version, &row.LeaseToken,
			&row.LeaseOwner, &row.LeaseExpiresAtMS, &row.UpdatedAtMS)
	if err == sql.ErrNoRows {
		return offsetRow{}, nil
	}
	if err != nil {
		return offsetRow{}, WrapError(CodeInternal, "read consumer offset", err)
	}
	row.ReplayActive = replayActive != 0
	return row, nil
}

func (s *Service) loadEventState(ctx context.Context, q queryer, topic, group string, partition int, seq int64) (EventState, int64, error) {
	var state EventState
	var deliveryCount int64
	err := q.QueryRowContext(ctx, `
		SELECT state, delivery_count FROM event_states
		WHERE topic = ? AND group_name = ? AND partition = ? AND seq = ?`,
		topic, group, partition, seq).Scan(&state, &deliveryCount)
	if err == sql.ErrNoRows {
		return "", 0, nil
	}
	if err != nil {
		return "", 0, WrapError(CodeInternal, "read event state", err)
	}
	return state, deliveryCount, nil
}

func (s *Service) loadEventRange(ctx context.Context, q queryer, topic string, partition int, start, end int64) ([]storedEvent, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT event_id, topic, partition, seq, partition_key, event_time_ms, body_digest,
			batch_id, aggregate_key, metric, content_hash, created_at_ms
		FROM events WHERE topic = ? AND partition = ? AND seq BETWEEN ? AND ? ORDER BY seq`,
		topic, partition, start, end)
	if err != nil {
		return nil, WrapError(CodeInternal, "read event range", err)
	}
	defer rows.Close()
	var events []storedEvent
	for rows.Next() {
		var event storedEvent
		if err := rows.Scan(&event.EventID, &event.Topic, &event.Partition, &event.Seq, &event.PartitionKey,
			&event.EventTimeMS, &event.BodyDigest, &event.BatchID, &event.AggregateKey, &event.Metric,
			&event.ContentHash, &event.CreatedAtMS); err != nil {
			return nil, WrapError(CodeInternal, "scan event", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, WrapError(CodeInternal, "iterate events", err)
	}
	return events, nil
}

func (s *Service) GetOffset(ctx context.Context, topic, group string, partition int) (OffsetState, error) {
	if err := s.validateTopicGroup(ctx, s.db, topic, group); err != nil {
		return OffsetState{}, err
	}
	if err := s.validateTopicPartition(ctx, s.db, topic, partition); err != nil {
		return OffsetState{}, err
	}
	offset, err := s.loadOffset(ctx, s.db, topic, group, partition)
	if err != nil {
		return OffsetState{}, err
	}
	now := s.nowMS()
	return OffsetState{
		Topic:            topic,
		Group:            group,
		Partition:        partition,
		AckedOffset:      offset.AckedOffset,
		ReplayUntil:      offset.ReplayUntil,
		ReplayActive:     offset.ReplayActive,
		Version:          offset.Version,
		LeaseHeld:        offset.LeaseToken != "" && offset.LeaseExpiresAtMS > now,
		LeaseOwner:       offset.LeaseOwner,
		LeaseExpiresAtMS: offset.LeaseExpiresAtMS,
		UpdatedAtMS:      offset.UpdatedAtMS,
	}, nil
}

func (s *Service) GetEventState(ctx context.Context, topic, group string, partition int, seq int64) (EventStateView, error) {
	if err := s.validateTopicGroup(ctx, s.db, topic, group); err != nil {
		return EventStateView{}, err
	}
	var view EventStateView
	var state string
	err := s.db.QueryRowContext(ctx, `
		SELECT event_id, state, delivery_count, updated_at_ms FROM event_states
		WHERE topic = ? AND group_name = ? AND partition = ? AND seq = ?`,
		topic, group, partition, seq).Scan(&view.EventID, &state, &view.DeliveryCount, &view.UpdatedAtMS)
	if err == sql.ErrNoRows {
		var eventID string
		if err := s.db.QueryRowContext(ctx, `SELECT event_id FROM events WHERE topic = ? AND partition = ? AND seq = ?`,
			topic, partition, seq).Scan(&eventID); err == sql.ErrNoRows {
			return EventStateView{}, NewError(CodeNotFound, "event sequence does not exist")
		} else if err != nil {
			return EventStateView{}, WrapError(CodeInternal, "read event", err)
		}
		return EventStateView{
			Topic: topic, Group: group, Partition: partition, Seq: seq, EventID: eventID, State: StateAccepted,
		}, nil
	}
	if err != nil {
		return EventStateView{}, WrapError(CodeInternal, "read event state", err)
	}
	view.Topic = topic
	view.Group = group
	view.Partition = partition
	view.Seq = seq
	view.State = EventState(state)
	return view, nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

type aggregateBatchPayload struct {
	EventBatchIDs []string
}

func eventBatchIDs(events []storedEvent) []string {
	seen := make(map[string]struct{})
	result := make([]string, 0)
	for _, event := range events {
		if _, ok := seen[event.BatchID]; ok {
			continue
		}
		seen[event.BatchID] = struct{}{}
		result = append(result, event.BatchID)
	}
	return result
}

func marshalStringSlice(values []string) string {
	data, _ := json.Marshal(values)
	return string(data)
}
