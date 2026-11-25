package bus

import (
	"context"
	"fmt"
	"strings"
)

func (s *Service) DeadLetter(ctx context.Context, request DeadLetterRequest) (DeadLetterResult, error) {
	request.Topic = strings.TrimSpace(request.Topic)
	request.Group = strings.TrimSpace(request.Group)
	request.ConsumerID = strings.TrimSpace(request.ConsumerID)
	request.LeaseToken = strings.TrimSpace(request.LeaseToken)
	request.Reason = strings.TrimSpace(request.Reason)
	if request.Topic == "" || request.Group == "" || request.ConsumerID == "" || request.LeaseToken == "" || request.Seq <= 0 {
		return DeadLetterResult{}, invalidArgument("topic, group, consumer_id, lease_token and positive seq are required")
	}
	hash := contentHash("dead_letter", request)
	return idempotentJSON(s, ctx, "dead_letter", request.RequestID, hash, func() (DeadLetterResult, error) {
		lock := s.partitionLock(request.Topic, request.Partition, request.Group)
		lock.Lock()
		defer lock.Unlock()

		if err := s.validateTopicGroup(ctx, s.db, request.Topic, request.Group); err != nil {
			return DeadLetterResult{}, err
		}
		if err := s.validateTopicPartition(ctx, s.db, request.Topic, request.Partition); err != nil {
			return DeadLetterResult{}, err
		}
		offset, err := s.loadOffset(ctx, s.db, request.Topic, request.Group, request.Partition)
		if err != nil {
			return DeadLetterResult{}, err
		}
		now := s.nowMS()
		if offset.LeaseToken == "" || offset.LeaseToken != request.LeaseToken || offset.LeaseExpiresAtMS <= now {
			return DeadLetterResult{}, NewError(CodeLeaseExpired, "delivery lease is not held or has expired")
		}
		if request.Seq != offset.AckedOffset+1 {
			return DeadLetterResult{}, NewError(CodeOutOfOrder, fmt.Sprintf("dead letter must target sequence %d", offset.AckedOffset+1))
		}
		state, _, err := s.loadEventState(ctx, s.db, request.Topic, request.Group, request.Partition, request.Seq)
		if err != nil {
			return DeadLetterResult{}, err
		}
		if state != StateDispatched || !AllowedTransition(state, StateDeadLettered) {
			return DeadLetterResult{}, NewError(CodeInvalidTransition, fmt.Sprintf("cannot DEAD_LETTER sequence %d from state %s", request.Seq, state))
		}
		if !s.eventLeaseMatches(ctx, request.Topic, request.Group, request.Partition, request.Seq, request.LeaseToken) {
			return DeadLetterResult{}, NewError(CodeLeaseExpired, "sequence is not held by this lease")
		}

		it, err := s.beginIntent(ctx, "DEAD_LETTER", request.RequestID, hash,
			request.Topic, request.Group, request.Partition,
			map[string]any{"acked_offset": offset.AckedOffset, "lease_token": offset.LeaseToken}, request)
		if err != nil {
			return DeadLetterResult{}, err
		}
		if err := s.injectFault("dead_letter_after_intent"); err != nil {
			return DeadLetterResult{}, err
		}
		tx, release, err := s.beginWriteTx(ctx)
		if err != nil {
			_ = s.finishIntent(context.WithoutCancel(ctx), it.ID, "ROLLED_BACK")
			return DeadLetterResult{}, WrapError(CodeInternal, "begin dead-letter transaction", err)
		}
		defer release()
		defer tx.Rollback()

		offset, err = s.loadOffset(ctx, tx, request.Topic, request.Group, request.Partition)
		if err != nil {
			return DeadLetterResult{}, err
		}
		if offset.LeaseToken != request.LeaseToken || offset.LeaseExpiresAtMS <= now || offset.AckedOffset+1 != request.Seq {
			return DeadLetterResult{}, NewError(CodeLeaseExpired, "lease or offset changed before dead letter")
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE event_states SET state = 'DEAD_LETTERED', lease_token = '', updated_at_ms = ?,
				transition_reason = ?, last_intent_id = ?
			WHERE topic = ? AND group_name = ? AND partition = ? AND seq = ?`,
			now, firstNonEmpty(request.Reason, "DEAD_LETTER"), it.ID,
			request.Topic, request.Group, request.Partition, request.Seq); err != nil {
			return DeadLetterResult{}, WrapError(CodeInternal, "mark dead letter", err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE event_states SET state = 'REDELIVERED', lease_token = '', updated_at_ms = ?,
				transition_reason = 'DEAD_LETTER_RELEASED_LEASE', last_intent_id = ?
			WHERE topic = ? AND group_name = ? AND partition = ? AND lease_token = ?
				AND state = 'DISPATCHED' AND seq > ?`,
			now, it.ID, request.Topic, request.Group, request.Partition, request.LeaseToken, request.Seq); err != nil {
			return DeadLetterResult{}, WrapError(CodeInternal, "release remaining dead-letter lease", err)
		}
		newOffset := request.Seq
		newReplayActive := offset.ReplayActive
		replayComplete := false
		if newReplayActive && newOffset >= offset.ReplayUntil {
			newReplayActive = false
			replayComplete = true
		}
		version := offset.Version + 1
		if _, err := tx.ExecContext(ctx, `
			UPDATE consumer_offsets SET acked_offset = ?, replay_active = ?, version = ?,
				lease_token = '', lease_owner = '', lease_expires_at_ms = 0,
				updated_at_ms = ?, last_intent_id = ?
			WHERE topic = ? AND group_name = ? AND partition = ?`,
			newOffset, boolInt(newReplayActive), version, now, it.ID,
			request.Topic, request.Group, request.Partition); err != nil {
			return DeadLetterResult{}, WrapError(CodeInternal, "advance dead-letter offset", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO offset_history(topic, group_name, partition, old_offset, new_offset, reason, intent_id, created_at_ms)
			VALUES(?, ?, ?, ?, ?, 'DEAD_LETTER', ?, ?)`,
			request.Topic, request.Group, request.Partition, offset.AckedOffset, newOffset, it.ID, now); err != nil {
			return DeadLetterResult{}, WrapError(CodeInternal, "record dead-letter offset", err)
		}
		if err := tx.Commit(); err != nil {
			_ = s.finishIntent(context.WithoutCancel(ctx), it.ID, "ROLLED_BACK")
			return DeadLetterResult{}, WrapError(CodeInternal, "commit dead letter", err)
		}
		release()
		if err := s.finishIntent(ctx, it.ID, "COMMITTED"); err != nil {
			return DeadLetterResult{}, err
		}
		return DeadLetterResult{
			Topic: request.Topic, Group: request.Group, Partition: request.Partition,
			Seq: request.Seq, State: StateDeadLettered, Offset: newOffset,
			ReplayActive: newReplayActive, ReplayComplete: replayComplete,
		}, nil
	})
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
