package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

func (s *Service) Ack(ctx context.Context, request AckRequest) (AckResult, error) {
	request.Topic = strings.TrimSpace(request.Topic)
	request.Group = strings.TrimSpace(request.Group)
	request.ConsumerID = strings.TrimSpace(request.ConsumerID)
	request.LeaseToken = strings.TrimSpace(request.LeaseToken)
	if request.Topic == "" || request.Group == "" || request.ConsumerID == "" || request.LeaseToken == "" {
		return AckResult{}, invalidArgument("topic, group, consumer_id and lease_token are required")
	}
	if len(request.Seqs) == 0 {
		return AckResult{}, invalidArgument("at least one sequence is required")
	}
	if !strictlyIncreasing(request.Seqs) {
		return AckResult{}, NewError(CodeOutOfOrder, "seqs must be unique and strictly increasing")
	}
	hash := contentHash("ack", request)
	return idempotentJSON(s, ctx, "ack", request.RequestID, hash, func() (AckResult, error) {
		lock := s.partitionLock(request.Topic, request.Partition, request.Group)
		lock.Lock()
		defer lock.Unlock()

		if err := s.validateTopicGroup(ctx, s.db, request.Topic, request.Group); err != nil {
			return AckResult{}, err
		}
		if err := s.validateTopicPartition(ctx, s.db, request.Topic, request.Partition); err != nil {
			return AckResult{}, err
		}
		algorithmName, err := s.groupAlgorithm(ctx, s.db, request.Topic, request.Group)
		if err != nil {
			return AckResult{}, err
		}
		algorithm, err := algorithmFromString(algorithmName)
		if err != nil {
			return AckResult{}, err
		}
		offset, err := s.loadOffset(ctx, s.db, request.Topic, request.Group, request.Partition)
		if err != nil {
			return AckResult{}, err
		}
		now := s.nowMS()
		if offset.LeaseToken == "" || offset.LeaseToken != request.LeaseToken || offset.LeaseExpiresAtMS <= now {
			return AckResult{}, NewError(CodeLeaseExpired, "delivery lease is not held or has expired")
		}
		expectedStart := offset.AckedOffset + 1
		if request.Seqs[0] != expectedStart {
			return AckResult{}, NewError(CodeOutOfOrder, fmt.Sprintf("ack must start at sequence %d", expectedStart))
		}
		for i := 1; i < len(request.Seqs); i++ {
			if request.Seqs[i] != request.Seqs[i-1]+1 {
				return AckResult{}, NewError(CodeOutOfOrder, "ack sequence range must be contiguous")
			}
		}
		if offset.ReplayActive && request.Seqs[len(request.Seqs)-1] > offset.ReplayUntil {
			return AckResult{}, NewError(CodeOutOfOrder, "replay acknowledgement exceeds replay boundary")
		}
		events, err := s.loadEventRange(ctx, s.db, request.Topic, request.Partition, request.Seqs[0], request.Seqs[len(request.Seqs)-1])
		if err != nil {
			return AckResult{}, err
		}
		if len(events) != len(request.Seqs) {
			return AckResult{}, NewError(CodeNotFound, "one or more ack sequences do not exist")
		}
		for i, event := range events {
			if event.Seq != request.Seqs[i] {
				return AckResult{}, NewError(CodeOutOfOrder, "ack sequence range is not gapless")
			}
			state, _, err := s.loadEventState(ctx, s.db, request.Topic, request.Group, request.Partition, event.Seq)
			if err != nil {
				return AckResult{}, err
			}
			if state != StateDispatched || !AllowedTransition(state, StateAcked) {
				return AckResult{}, NewError(CodeInvalidTransition, fmt.Sprintf("cannot ACK sequence %d from state %s", event.Seq, state))
			}
			if !s.eventLeaseMatches(ctx, request.Topic, request.Group, request.Partition, event.Seq, request.LeaseToken) {
				return AckResult{}, NewError(CodeLeaseExpired, fmt.Sprintf("sequence %d is not held by this lease", event.Seq))
			}
		}

		prior := map[string]any{
			"acked_offset": offset.AckedOffset,
			"lease_token":  offset.LeaseToken,
			"seqs":         request.Seqs,
		}
		it, err := s.beginIntent(ctx, "ACK", request.RequestID, hash, request.Topic, request.Group, request.Partition, prior, request)
		if err != nil {
			return AckResult{}, err
		}
		if err := s.injectFault("ack_after_intent"); err != nil {
			return AckResult{}, err
		}
		tx, release, err := s.beginWriteTx(ctx)
		if err != nil {
			_ = s.finishIntent(context.WithoutCancel(ctx), it.ID, "ROLLED_BACK")
			return AckResult{}, WrapError(CodeInternal, "begin ack transaction", err)
		}
		defer release()
		defer tx.Rollback()

		offset, err = s.loadOffset(ctx, tx, request.Topic, request.Group, request.Partition)
		if err != nil {
			return AckResult{}, err
		}
		if offset.LeaseToken != request.LeaseToken || offset.LeaseExpiresAtMS <= now {
			return AckResult{}, NewError(CodeLeaseExpired, "delivery lease changed before acknowledgement")
		}
		if offset.AckedOffset+1 != request.Seqs[0] {
			return AckResult{}, NewError(CodeOutOfOrder, "consumer offset changed before acknowledgement")
		}

		now = s.nowMS()
		for _, event := range events {
			state, _, err := s.loadEventState(ctx, tx, request.Topic, request.Group, request.Partition, event.Seq)
			if err != nil {
				return AckResult{}, err
			}
			if state != StateDispatched || !AllowedTransition(state, StateAcked) {
				return AckResult{}, NewError(CodeInvalidTransition, fmt.Sprintf("cannot ACK sequence %d from state %s", event.Seq, state))
			}
			if _, err := tx.ExecContext(ctx, `
				UPDATE event_states SET state = 'ACKED', lease_token = '', updated_at_ms = ?,
					transition_reason = 'ACK', last_intent_id = ?
				WHERE topic = ? AND group_name = ? AND partition = ? AND seq = ?`,
				now, it.ID, request.Topic, request.Group, request.Partition, event.Seq); err != nil {
				return AckResult{}, WrapError(CodeInternal, "mark event acked", err)
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO acks(topic, group_name, partition, seq, event_id, active, intent_id, request_id, acked_at_ms)
				VALUES(?, ?, ?, ?, ?, 1, ?, ?, ?)`,
				request.Topic, request.Group, request.Partition, event.Seq, event.EventID, it.ID, request.RequestID, now); err != nil {
				return AckResult{}, WrapError(CodeInternal, "record acknowledgement", err)
			}
		}

		groupState, err := s.loadAggregateGroup(ctx, tx, request.Topic, request.Group, request.Partition, algorithm)
		if err != nil {
			return AckResult{}, err
		}
		for _, event := range events {
			groupState.Add(event.AggregateKey, event.Metric)
		}
		if err := validateAggregateGroup(&groupState); err != nil {
			return AckResult{}, err
		}
		groupState.Finalize()
		if err := s.persistAggregateGroup(ctx, tx, request.Topic, request.Group, request.Partition, &groupState); err != nil {
			return AckResult{}, err
		}
		partials, batchSumAbs, batchBound, err := batchPartials(algorithm, events)
		if err != nil {
			return AckResult{}, err
		}
		var batchSum float64
		for _, key := range sortedKeys(partials) {
			batchSum += partials[key].Sum
		}
		newOffset := request.Seqs[len(request.Seqs)-1]
		version := offset.Version + 1
		replayComplete := false
		newReplayActive := offset.ReplayActive
		if newReplayActive && newOffset >= offset.ReplayUntil {
			newReplayActive = false
			replayComplete = true
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE consumer_offsets SET acked_offset = ?, replay_active = ?, version = ?, lease_token = '',
				lease_owner = '', lease_expires_at_ms = 0, updated_at_ms = ?, last_intent_id = ?
			WHERE topic = ? AND group_name = ? AND partition = ?`,
			newOffset, boolInt(newReplayActive), version, now, it.ID,
			request.Topic, request.Group, request.Partition); err != nil {
			return AckResult{}, WrapError(CodeInternal, "advance consumer offset", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO offset_history(topic, group_name, partition, old_offset, new_offset, reason, intent_id, created_at_ms)
			VALUES(?, ?, ?, ?, ?, 'ACK', ?, ?)`,
			request.Topic, request.Group, request.Partition, offset.AckedOffset, newOffset, it.ID, now); err != nil {
			return AckResult{}, WrapError(CodeInternal, "record offset advance", err)
		}

		ackBatchID := request.AckBatchID
		if ackBatchID == "" {
			ackBatchID = randomID("batch_")
		}
		partialsJSON, _ := json.Marshal(partials)
		eventBatches := eventBatchIDs(events)
		batchRelative := relativeBound(batchBound, batchSum)
		digest := groupState.Digest()
		batchResult, err := tx.ExecContext(ctx, `
			INSERT INTO agg_batches(topic, group_name, partition, ack_batch_id, event_batch_ids_json,
				start_seq, end_seq, algorithm, event_count, partials_json, sum_abs, abs_error_bound,
				relative_error_bound, state_digest, previous_offset, new_offset, version, active,
				intent_id, created_at_ms)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)`,
			request.Topic, request.Group, request.Partition, ackBatchID, marshalStringSlice(eventBatches),
			request.Seqs[0], newOffset, algorithm, int64(len(events)), string(partialsJSON), batchSumAbs,
			batchBound, batchRelative, digest, offset.AckedOffset, newOffset, version, it.ID, now)
		if err != nil {
			return AckResult{}, WrapError(CodeInternal, "record aggregate batch", err)
		}
		batchID, _ := batchResult.LastInsertId()
		if _, err := s.saveAggregateSnapshot(ctx, tx, request.Topic, request.Group, request.Partition,
			newOffset, version, &groupState, batchID); err != nil {
			return AckResult{}, err
		}
		if err := s.injectFault("ack_before_commit"); err != nil {
			return AckResult{}, err
		}
		if err := tx.Commit(); err != nil {
			_ = s.finishIntent(context.WithoutCancel(ctx), it.ID, "ROLLED_BACK")
			return AckResult{}, WrapError(CodeInternal, "commit ack", err)
		}
		release()
		if err := s.injectFault("ack_after_commit"); err != nil {
			return AckResult{}, err
		}
		if err := s.finishIntent(ctx, it.ID, "COMMITTED"); err != nil {
			return AckResult{}, err
		}
		resultBatch := AggregateBatch{
			ID:                 batchID,
			AckBatchID:         ackBatchID,
			EventBatchIDs:      eventBatches,
			StartSeq:           request.Seqs[0],
			EndSeq:             newOffset,
			Algorithm:          algorithm,
			EventCount:         int64(len(events)),
			Partials:           partials,
			SumAbs:             batchSumAbs,
			AbsErrorBound:      batchBound,
			RelativeErrorBound: batchRelative,
			StateDigest:        digest,
			PreviousOffset:     offset.AckedOffset,
			NewOffset:          newOffset,
			Version:            version,
			Active:             true,
			CreatedAtMS:        now,
		}
		return AckResult{
			Topic:          request.Topic,
			Group:          request.Group,
			Partition:      request.Partition,
			Offset:         newOffset,
			ReplayActive:   newReplayActive,
			ReplayComplete: replayComplete,
			AckedSeqs:      append([]int64(nil), request.Seqs...),
			Batch:          resultBatch,
			StateDigest:    digest,
		}, nil
	})
}

func strictlyIncreasing(values []int64) bool {
	if len(values) == 0 {
		return false
	}
	for i := 1; i < len(values); i++ {
		if values[i] <= values[i-1] {
			return false
		}
	}
	return true
}

func (s *Service) eventLeaseMatches(ctx context.Context, topic, group string, partition int, seq int64, token string) bool {
	var count int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM event_states
		WHERE topic = ? AND group_name = ? AND partition = ? AND seq = ? AND lease_token = ? AND state = 'DISPATCHED'`,
		topic, group, partition, seq, token).Scan(&count)
	return err == nil && count == 1
}

func sortedKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
