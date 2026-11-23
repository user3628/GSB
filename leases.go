package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	InjectionLeaseTimeoutPreempt = "timeout_preempt"
	InjectionReplayNewMessage    = "new_message_on_replay"
)

func validateLeaseInjection(mode string, allowReplayArrival bool) error {
	if mode == "" {
		return nil
	}
	if mode == InjectionLeaseTimeoutPreempt {
		return nil
	}
	if allowReplayArrival && mode == InjectionReplayNewMessage {
		return nil
	}
	return newErrDetails(CodeInvalidArgument, "unsupported lease injection mode",
		map[string]any{"mode": mode})
}

func (s *Service) leaseInjection(mode, topic string, partition int) *InjectionRequest {
	if mode == "" {
		return nil
	}
	active := false
	return &InjectionRequest{
		Kind:   "lease",
		Target: fmt.Sprintf("%s/%d", topic, partition),
		Mode:   mode,
		Active: &active,
	}
}

func (s *Service) resolveStored(decision opDecision) (opDecision, error) {
	if decision.InProgress {
		return s.awaitOperation(context.Background(), decision.RequestID)
	}
	return decision, nil
}

func (s *Service) Pull(ctx context.Context, topic string, partition int, req PullRequest) (PullResult, error) {
	var zero PullResult
	req.Owner = strings.TrimSpace(req.Owner)
	if req.Owner == "" {
		return zero, newErr(CodeInvalidArgument, "owner is required")
	}
	if req.Limit <= 0 {
		req.Limit = 1
	}
	if req.Limit > 100 {
		req.Limit = 100
	}
	if err := validateLeaseInjection(req.LeaseInjection, false); err != nil {
		return zero, err
	}
	fingerprintPayload := req
	fingerprintPayload.RequestID = ""
	decision, err := s.prepareOperation(ctx, req.RequestID, "pull",
		fmt.Sprintf("%s/%d", topic, partition), fingerprintPayload,
		s.leaseInjection(req.LeaseInjection, topic, partition))
	if err != nil {
		return zero, err
	}
	if decision.InProgress {
		decision, err = s.awaitOperation(ctx, decision.RequestID)
		if err != nil {
			return zero, err
		}
	}
	if decision.StoredErr != nil {
		return zero, decision.StoredErr
	}
	if decision.Response != nil {
		var out PullResult
		if err := json.Unmarshal(decision.Response, &out); err != nil {
			return zero, err
		}
		return out, nil
	}

	raw, err := s.commitOperation(ctx, decision.Op, func(tx *sql.Tx) (any, error) {
		ps, err := s.loadPartitionTx(ctx, tx, topic, partition)
		if err != nil {
			return nil, err
		}
		if _, err := s.expireLeaseIfNeededTx(ctx, tx, ps); err != nil {
			return nil, err
		}

		var observation *Observation
		if req.LeaseInjection == InjectionLeaseTimeoutPreempt {
			if ps.LeaseState == LeaseLeased && ps.LeaseOwner != req.Owner {
				oldOwner := ps.LeaseOwner
				if err := s.transitionLeaseTx(ctx, tx, ps, LeaseExpired,
					"injected_lease_timeout_preempt"); err != nil {
					return nil, err
				}
				observation, err = s.recordObservationLocked(ctx, tx, decision.Op.InjectionID,
					"lease", req.LeaseInjection, fmt.Sprintf("%s/%d", topic, partition),
					"pull", "", "preempted_owner="+oldOwner, decision.RequestID)
				if err != nil {
					return nil, err
				}
			} else {
				observation, err = s.recordObservationLocked(ctx, tx, decision.Op.InjectionID,
					"lease", req.LeaseInjection, fmt.Sprintf("%s/%d", topic, partition),
					"pull", CodeInjectionNotApplied, "no_active_other_owner_lease", decision.RequestID)
				if err != nil {
					return nil, err
				}
			}
		}

		if req.LeaseVersion != nil && *req.LeaseVersion != ps.LeaseVersion {
			return nil, newErrDetails(CodeLeaseStale,
				"lease version is stale",
				map[string]any{"expected": *req.LeaseVersion, "current": ps.LeaseVersion})
		}
		switch ps.LeaseState {
		case LeaseLeased:
			if ps.LeaseOwner != req.Owner {
				return nil, newErrDetails(CodeLeaseHeld,
					"partition lease is held by another consumer",
					map[string]any{"owner": ps.LeaseOwner,
						"lease_expires_at_ms": ps.LeaseExpiresAtMS})
			}
			if req.LeaseVersion == nil {
				return nil, newErrDetails(CodeLeaseStale,
					"lease_version is required while the lease is held",
					map[string]any{"current": ps.LeaseVersion})
			}
			ps.LeaseExpiresAtMS = nowMS(s.now()) + s.leaseTTL.Milliseconds()
		case LeaseFree, LeaseExpired:
			if err := s.transitionLeaseTx(ctx, tx, ps, LeaseLeased, "consumer_acquire"); err != nil {
				return nil, err
			}
			ps.LeaseOwner = req.Owner
			ps.LeaseExpiresAtMS = nowMS(s.now()) + s.leaseTTL.Milliseconds()
			ps.LeaseCursor = ps.ConsumeCursor
			ps.LeaseLastDeliveredSeq = nil
		default:
			return nil, newErr(CodeInvalidTransition, "unknown lease state "+ps.LeaseState)
		}
		ps.LeaseOwner = req.Owner

		limit := ps.NextSeq
		if ps.ReplayActive && ps.ReplayLimit != nil && *ps.ReplayLimit < limit {
			limit = *ps.ReplayLimit
		}
		position := ps.LeaseCursor
		if position < ps.EarliestRetainedSeq {
			position = ps.EarliestRetainedSeq
			ps.LeaseCursor = position
		}
		messages := make([]Message, 0, req.Limit)
		for position < limit && len(messages) < req.Limit {
			var m Message
			var tr int
			err := tx.QueryRowContext(ctx, `SELECT message_id, topic, partition_no, partition_key,
				seq, status, transactional, COALESCE(txn_id,''), body_digest, body,
				produced_at_ms, updated_at_ms FROM messages
				WHERE topic=? AND partition_no=? AND seq=?`, topic, partition, position).Scan(
				&m.MessageID, &m.Topic, &m.Partition, &m.PartitionKey, &m.Seq, &m.Status,
				&tr, &m.TxnID, &m.BodyDigest, &m.Body, &m.ProducedAtMS, &m.UpdatedAtMS)
			if errors.Is(err, sql.ErrNoRows) {
				break
			}
			if err != nil {
				return nil, err
			}
			m.Transactional = tr != 0
			switch m.Status {
			case MsgVisible:
				messages = append(messages, m)
				position++
			case MsgRolledBack, MsgDeadLettered, MsgConsumed:
				position++
				if len(messages) == 0 {
					ps.LeaseCursor = position
				}
			case MsgPendingInvisible:
				// Strict order: an unresolved transaction blocks every later sequence.
				position = limit
			default:
				return nil, newErr(CodeInvalidTransition, "unknown message state "+m.Status)
			}
		}
		if len(messages) > 0 {
			last := messages[len(messages)-1].Seq
			ps.LeaseLastDeliveredSeq = &last
		}
		if err := s.savePartitionTx(ctx, tx, ps); err != nil {
			return nil, err
		}
		view, err := s.partitionViewTx(ctx, tx, ps)
		if err != nil {
			return nil, err
		}
		return PullResult{Messages: messages, Lease: view, Injection: observation,
			RequestID: decision.RequestID}, nil
	})
	if err != nil {
		return zero, err
	}
	var out PullResult
	if err := json.Unmarshal(raw, &out); err != nil {
		return zero, err
	}
	return out, nil
}

func (s *Service) Commit(ctx context.Context, topic string, partition int, req CommitRequest) (CommitResult, error) {
	var zero CommitResult
	if strings.TrimSpace(req.Owner) == "" {
		return zero, newErr(CodeInvalidArgument, "owner is required")
	}
	fingerprintPayload := req
	fingerprintPayload.RequestID = ""
	decision, err := s.prepareOperation(ctx, req.RequestID, "commit",
		fmt.Sprintf("%s/%d", topic, partition), fingerprintPayload, nil)
	if err != nil {
		return zero, err
	}
	if decision.InProgress {
		decision, err = s.awaitOperation(ctx, decision.RequestID)
		if err != nil {
			return zero, err
		}
	}
	if decision.StoredErr != nil {
		return zero, decision.StoredErr
	}
	if decision.Response != nil {
		var out CommitResult
		if err := json.Unmarshal(decision.Response, &out); err != nil {
			return zero, err
		}
		return out, nil
	}

	raw, err := s.commitOperation(ctx, decision.Op, func(tx *sql.Tx) (any, error) {
		ps, err := s.loadPartitionTx(ctx, tx, topic, partition)
		if err != nil {
			return nil, err
		}
		if _, err := s.expireLeaseIfNeededTx(ctx, tx, ps); err != nil {
			return nil, err
		}
		if ps.LeaseState != LeaseLeased || ps.LeaseOwner != req.Owner || ps.LeaseVersion != req.LeaseVersion {
			return nil, newErrDetails(CodeLeaseStale,
				"lease version or owner no longer owns the partition",
				map[string]any{"request_owner": req.Owner, "request_version": req.LeaseVersion,
					"current_owner": ps.LeaseOwner, "current_version": ps.LeaseVersion,
					"lease_state": ps.LeaseState})
		}
		if ps.LeaseLastDeliveredSeq == nil || req.ThroughSeq < ps.LeaseCursor ||
			req.ThroughSeq > *ps.LeaseLastDeliveredSeq {
			return nil, newErrDetails(CodeInvalidTransition,
				"through_seq must be within the delivered range held by this lease",
				map[string]any{"lease_cursor": ps.LeaseCursor,
					"last_delivered": ps.LeaseLastDeliveredSeq, "through_seq": req.ThroughSeq})
		}
		consumedFrom := ps.ConsumeCursor
		statuses := make([]string, 0, req.ThroughSeq-ps.LeaseCursor+1)
		messageIDs := make([]string, 0, cap(statuses))
		for seq := ps.LeaseCursor; seq <= req.ThroughSeq; seq++ {
			var status, messageID string
			err := tx.QueryRowContext(ctx, `SELECT message_id, status FROM messages
				WHERE topic=? AND partition_no=? AND seq=?`, topic, partition, seq).Scan(&messageID, &status)
			if err != nil {
				return nil, err
			}
			switch status {
			case MsgVisible, MsgRolledBack, MsgDeadLettered, MsgConsumed:
				statuses = append(statuses, status)
				messageIDs = append(messageIDs, messageID)
			case MsgPendingInvisible:
				return nil, newErrDetails(CodeInvalidTransition,
					"cannot commit across an invisible transactional message",
					map[string]any{"seq": seq, "status": status})
			default:
				return nil, newErr(CodeInvalidTransition, "unknown message state "+status)
			}
		}
		for i, status := range statuses {
			if status == MsgVisible {
				if err := s.transitionMessageTx(ctx, tx, messageIDs[i], MsgVisible, MsgConsumed, "consumer_commit"); err != nil {
					return nil, err
				}
			}
		}
		ps.ConsumeCursor = req.ThroughSeq + 1
		for {
			var status string
			err := tx.QueryRowContext(ctx, `SELECT status FROM messages
				WHERE topic=? AND partition_no=? AND seq=?`, topic, partition, ps.ConsumeCursor).Scan(&status)
			if errors.Is(err, sql.ErrNoRows) {
				break
			}
			if err != nil {
				return nil, err
			}
			if status == MsgRolledBack || status == MsgDeadLettered || status == MsgConsumed {
				ps.ConsumeCursor++
				continue
			}
			break
		}
		if ps.ReplayActive && ps.ReplayLimit != nil && ps.ConsumeCursor >= *ps.ReplayLimit {
			ps.ReplayActive = false
			ps.ReplayFrom = nil
			ps.ReplayLimit = nil
			if err := s.advanceVisibilityFrontierTx(ctx, tx, ps, true); err != nil {
				return nil, err
			}
		}
		if err := s.transitionLeaseTx(ctx, tx, ps, LeaseFree, "consumer_commit"); err != nil {
			return nil, err
		}
		if err := s.savePartitionTx(ctx, tx, ps); err != nil {
			return nil, err
		}
		view, err := s.partitionViewTx(ctx, tx, ps)
		if err != nil {
			return nil, err
		}
		return CommitResult{Partition: view, ConsumedFrom: consumedFrom,
			ConsumedTo: req.ThroughSeq, RequestID: decision.RequestID}, nil
	})
	if err != nil {
		return zero, err
	}
	var out CommitResult
	if err := json.Unmarshal(raw, &out); err != nil {
		return zero, err
	}
	return out, nil
}

func (s *Service) Replay(ctx context.Context, topic string, partition int, req ReplayRequest) (ReplayResult, error) {
	var zero ReplayResult
	if strings.TrimSpace(req.Owner) == "" {
		return zero, newErr(CodeInvalidArgument, "owner is required")
	}
	if err := validateLeaseInjection(req.LeaseInjection, true); err != nil {
		return zero, err
	}
	fingerprintPayload := req
	fingerprintPayload.RequestID = ""
	decision, err := s.prepareOperation(ctx, req.RequestID, "replay",
		fmt.Sprintf("%s/%d", topic, partition), fingerprintPayload,
		s.leaseInjection(req.LeaseInjection, topic, partition))
	if err != nil {
		return zero, err
	}
	if decision.InProgress {
		decision, err = s.awaitOperation(ctx, decision.RequestID)
		if err != nil {
			return zero, err
		}
	}
	if decision.StoredErr != nil {
		return zero, decision.StoredErr
	}
	if decision.Response != nil {
		var out ReplayResult
		if err := json.Unmarshal(decision.Response, &out); err != nil {
			return zero, err
		}
		return out, nil
	}
	raw, err := s.commitOperation(ctx, decision.Op, func(tx *sql.Tx) (any, error) {
		ps, err := s.loadPartitionTx(ctx, tx, topic, partition)
		if err != nil {
			return nil, err
		}
		if _, err := s.expireLeaseIfNeededTx(ctx, tx, ps); err != nil {
			return nil, err
		}
		if ps.LeaseState != LeaseLeased || ps.LeaseOwner != req.Owner || ps.LeaseVersion != req.LeaseVersion {
			return nil, newErrDetails(CodeLeaseStale, "replay requires the current lease",
				map[string]any{"request_owner": req.Owner, "request_version": req.LeaseVersion,
					"current_owner": ps.LeaseOwner, "current_version": ps.LeaseVersion})
		}
		if ps.ReplayActive {
			return nil, newErrDetails(CodeInvalidTransition,
				"partition already has an active replay", map[string]any{"replay_from": ps.ReplayFrom,
					"replay_limit": ps.ReplayLimit})
		}
		if req.FromSeq < ps.EarliestRetainedSeq {
			return nil, newErrDetails(CodeReplayExpired,
				"replay target is older than the earliest retained sequence",
				map[string]any{"from_seq": req.FromSeq, "earliest_retained_seq": ps.EarliestRetainedSeq})
		}
		if req.FromSeq >= ps.VisibleFrontier || req.FromSeq >= ps.ConsumeCursor {
			return nil, newErrDetails(CodeReplayInvalid,
				"replay target has not been consumed or is not yet visible",
				map[string]any{"from_seq": req.FromSeq, "visible_frontier": ps.VisibleFrontier,
					"consume_cursor": ps.ConsumeCursor})
		}
		var targetStatus, targetID string
		if err := tx.QueryRowContext(ctx, `SELECT message_id, status FROM messages
			WHERE topic=? AND partition_no=? AND seq=?`, topic, partition, req.FromSeq).Scan(&targetID, &targetStatus); err != nil {
			return nil, err
		}
		if targetStatus != MsgConsumed && targetStatus != MsgVisible {
			return nil, newErrDetails(CodeReplayInvalid,
				"replay target is not a visible or consumed message",
				map[string]any{"from_seq": req.FromSeq, "status": targetStatus})
		}
		oldCursor := ps.ConsumeCursor
		type replayMessage struct {
			id     string
			status string
		}
		replayMessages := make([]replayMessage, 0, oldCursor-req.FromSeq)
		for seq := req.FromSeq; seq < oldCursor; seq++ {
			var messageID, status string
			if err := tx.QueryRowContext(ctx, `SELECT message_id, status FROM messages
				WHERE topic=? AND partition_no=? AND seq=?`, topic, partition, seq).Scan(&messageID, &status); err != nil {
				return nil, err
			}
			if status == MsgPendingInvisible {
				return nil, newErrDetails(CodeReplayInvalid,
					"replay range contains an invisible message", map[string]any{"seq": seq})
			}
			replayMessages = append(replayMessages, replayMessage{id: messageID, status: status})
		}
		for _, rm := range replayMessages {
			if rm.status == MsgConsumed {
				if err := s.transitionMessageTx(ctx, tx, rm.id, MsgConsumed, MsgVisible, "replay_prepare"); err != nil {
					return nil, err
				}
			}
		}
		ps.ReplayActive = true
		ps.ReplayFrom = int64Ptr(req.FromSeq)
		ps.ReplayLimit = int64Ptr(oldCursor)
		ps.ConsumeCursor = req.FromSeq
		ps.LeaseCursor = req.FromSeq
		ps.LeaseLastDeliveredSeq = nil

		var observation *Observation
		if req.LeaseInjection == InjectionReplayNewMessage {
			newSeq := ps.NextSeq
			now := nowMS(s.now())
			messageID := newID("replay-arrival")
			body := fmt.Sprintf("arrived during replay from %d", req.FromSeq)
			if _, err := tx.ExecContext(ctx, `INSERT INTO messages
				(message_id, topic, partition_no, partition_key, seq, status, transactional, txn_id,
				 body_digest, body, produced_at_ms, updated_at_ms)
				VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
				messageID, topic, partition, "injected-during-replay", newSeq, MsgVisible, 0, nil,
				digestString(body), body, now, now); err != nil {
				return nil, err
			}
			ps.NextSeq++
			observation, err = s.recordObservationLocked(ctx, tx, decision.Op.InjectionID,
				"lease", req.LeaseInjection, fmt.Sprintf("%s/%d", topic, partition),
				"replay", "", fmt.Sprintf("message_id=%s seq=%d excluded_until_replay_limit=%d",
					messageID, newSeq, oldCursor), decision.RequestID)
			if err != nil {
				return nil, err
			}
		}
		if err := s.savePartitionTx(ctx, tx, ps); err != nil {
			return nil, err
		}
		view, err := s.partitionViewTx(ctx, tx, ps)
		if err != nil {
			return nil, err
		}
		return ReplayResult{Partition: view, ReplayFrom: req.FromSeq, ReplayLimit: oldCursor,
			Injection: observation, RequestID: decision.RequestID}, nil
	})
	if err != nil {
		return zero, err
	}
	var out ReplayResult
	if err := json.Unmarshal(raw, &out); err != nil {
		return zero, err
	}
	return out, nil
}

func (s *Service) ExpireLease(ctx context.Context, topic string, partition int) (bool, PartitionView, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, PartitionView{}, err
	}
	defer tx.Rollback()
	ps, err := s.loadPartitionTx(ctx, tx, topic, partition)
	if err != nil {
		return false, PartitionView{}, err
	}
	expired, err := s.expireLeaseIfNeededTx(ctx, tx, ps)
	if err != nil {
		return false, PartitionView{}, err
	}
	if err := tx.Commit(); err != nil {
		return false, PartitionView{}, err
	}
	view, err := s.GetPartition(ctx, topic, partition)
	return expired, view, err
}

func (s *Service) ForceExpireLease(ctx context.Context, topic string, partition int, reason string) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	ps, err := s.loadPartitionTx(ctx, tx, topic, partition)
	if err != nil {
		return false, err
	}
	if ps.LeaseState != LeaseLeased {
		return false, tx.Commit()
	}
	if err := s.transitionLeaseTx(ctx, tx, ps, LeaseExpired, reason); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

func (s *Service) DeadLetter(ctx context.Context, messageID, requestID string) (Message, error) {
	decision, err := s.prepareOperation(ctx, requestID, "dead_letter", messageID,
		map[string]string{"message_id": messageID}, nil)
	if err != nil {
		return Message{}, err
	}
	if decision.InProgress {
		decision, err = s.awaitOperation(ctx, decision.RequestID)
		if err != nil {
			return Message{}, err
		}
	}
	if decision.StoredErr != nil {
		return Message{}, decision.StoredErr
	}
	if decision.Response != nil {
		var out Message
		if err := json.Unmarshal(decision.Response, &out); err != nil {
			return Message{}, err
		}
		return out, nil
	}
	raw, err := s.commitOperation(ctx, decision.Op, func(tx *sql.Tx) (any, error) {
		m, err := s.loadMessageTx(ctx, tx, messageID)
		if err != nil {
			return nil, err
		}
		if err := s.transitionMessageTx(ctx, tx, messageID, m.Status, MsgDeadLettered, "dead_letter"); err != nil {
			return nil, err
		}
		ps, err := s.loadPartitionTx(ctx, tx, m.Topic, m.Partition)
		if err != nil {
			return nil, err
		}
		ps.LeaseLastDeliveredSeq = nil
		if err := s.savePartitionTx(ctx, tx, ps); err != nil {
			return nil, err
		}
		return s.loadMessageTx(ctx, tx, messageID)
	})
	if err != nil {
		return Message{}, err
	}
	var out Message
	if err := json.Unmarshal(raw, &out); err != nil {
		return Message{}, err
	}
	return out, nil
}
