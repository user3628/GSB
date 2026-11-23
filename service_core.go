package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

func validateTopic(t Topic) error {
	t.Name = strings.TrimSpace(t.Name)
	if t.Name == "" {
		return newErr(CodeInvalidArgument, "topic name is required")
	}
	if t.Partitions <= 0 || t.Partitions > 4096 {
		return newErr(CodeInvalidArgument, "topic partitions must be between 1 and 4096")
	}
	if t.RoutingRule != "hash" && t.RoutingRule != "explicit" {
		return newErr(CodeRouteInvalid, "routing_rule must be hash or explicit")
	}
	if len(t.PartitionPriority) > 0 && len(t.PartitionPriority) != t.Partitions {
		return newErr(CodeInvalidArgument, "partition_priorities length must equal partitions")
	}
	return nil
}

func (s *Service) CreateTopic(ctx context.Context, input Topic) (Topic, error) {
	input.Name = strings.TrimSpace(input.Name)
	if err := validateTopic(input); err != nil {
		return Topic{}, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Topic{}, err
	}
	defer tx.Rollback()

	existing, err := s.getTopicTx(ctx, tx, input.Name)
	if err == nil {
		if existing.Partitions != input.Partitions || existing.RoutingRule != input.RoutingRule {
			return Topic{}, newErr(CodeConflictingRequest,
				"topic already exists with a different partition count or routing rule")
		}
		return existing, nil
	}
	var we *WorkbenchError
	if !errors.As(err, &we) || we.Code != CodeNotFound {
		return Topic{}, err
	}

	now := nowMS(s.now())
	if _, err := tx.ExecContext(ctx, `INSERT INTO topics
		(name, partitions, routing_rule, created_at_ms) VALUES (?,?,?,?)`,
		input.Name, input.Partitions, input.RoutingRule, now); err != nil {
		return Topic{}, err
	}
	priorities := make([]int, input.Partitions)
	for i := range priorities {
		priorities[i] = 100
	}
	if len(input.PartitionPriority) == input.Partitions {
		copy(priorities, input.PartitionPriority)
	}
	for p := 0; p < input.Partitions; p++ {
		if priorities[p] <= 0 {
			return Topic{}, newErr(CodeInvalidArgument, "partition priorities must be positive")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO partitions
			(topic, partition_no, priority, updated_at_ms) VALUES (?,?,?,?)`,
			input.Name, p, priorities[p], now); err != nil {
			return Topic{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO consumer_offsets
			(topic, partition_no, updated_at_ms) VALUES (?,?,?)`, input.Name, p, now); err != nil {
			return Topic{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return Topic{}, err
	}
	return Topic{Name: input.Name, Partitions: input.Partitions, RoutingRule: input.RoutingRule,
		CreatedAtMS: now, PartitionPriority: priorities}, nil
}

func (s *Service) ListTopics(ctx context.Context) ([]Topic, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT name, partitions, routing_rule, created_at_ms
		FROM topics ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Topic, 0)
	for rows.Next() {
		var t Topic
		if err := rows.Scan(&t.Name, &t.Partitions, &t.RoutingRule, &t.CreatedAtMS); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func publishPayloadFingerprint(req PublishRequest) PublishRequest {
	req.RequestID = ""
	return req
}

func (s *Service) Publish(ctx context.Context, req PublishRequest) (PublishResult, error) {
	var zero PublishResult
	if strings.TrimSpace(req.MessageID) == "" {
		return zero, newErr(CodeInvalidArgument, "message_id is required")
	}
	if strings.TrimSpace(req.Topic) == "" {
		return zero, newErr(CodeInvalidArgument, "topic is required")
	}
	if req.Transactional && strings.TrimSpace(req.TxnID) == "" {
		return zero, newErr(CodeInvalidArgument, "transactional message requires txn_id")
	}
	if !req.Transactional && req.TxnID != "" {
		return zero, newErr(CodeInvalidArgument, "txn_id is only valid for transactional messages")
	}

	decision, err := s.prepareOperation(ctx, req.RequestID, "publish",
		req.Topic+"/"+req.MessageID, publishPayloadFingerprint(req), nil)
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
		var out PublishResult
		if err := decodeStoredResponse(string(decision.Response), &out); err != nil {
			return zero, err
		}
		return out, nil
	}

	raw, err := s.commitOperation(ctx, decision.Op, func(tx *sql.Tx) (any, error) {
		var existing Message
		var existingTrans int
		var existingTxn sql.NullString
		var existingDigest string
		err := tx.QueryRowContext(ctx, `SELECT message_id, topic, partition_no, partition_key,
			seq, status, transactional, COALESCE(txn_id,''), body_digest
			FROM messages WHERE message_id=?`, req.MessageID).Scan(
			&existing.MessageID, &existing.Topic, &existing.Partition, &existing.PartitionKey,
			&existing.Seq, &existing.Status, &existingTrans, &existingTxn, &existingDigest)
		bodyDigest := digestString(req.Body)
		if err == nil {
			if existing.Topic != req.Topic || existingDigest != bodyDigest ||
				existingTrans != boolInt(req.Transactional) || existingTxn.String != req.TxnID ||
				(existing.PartitionKey != req.PartitionKey &&
					(req.Partition == nil || existing.Partition != *req.Partition)) {
				return nil, newErr(CodeConflictingMessage,
					"message_id already exists with different content")
			}
			m, err := s.loadMessageTx(ctx, tx, req.MessageID)
			if err != nil {
				return nil, err
			}
			return PublishResult{Message: m, Duplicate: true, RequestID: decision.RequestID}, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}

		topic, err := s.getTopicTx(ctx, tx, req.Topic)
		if err != nil {
			return nil, err
		}
		partition, err := routePartition(topic, req)
		if err != nil {
			return nil, err
		}
		ps, err := s.loadPartitionTx(ctx, tx, req.Topic, partition)
		if err != nil {
			return nil, err
		}
		seq := ps.NextSeq
		status := MsgVisible
		transactional := boolInt(req.Transactional)
		if req.Transactional {
			status = MsgPendingInvisible
		}
		now := nowMS(s.now())
		if _, err := tx.ExecContext(ctx, `INSERT INTO messages
			(message_id, topic, partition_no, partition_key, seq, status, transactional, txn_id,
			 body_digest, body, produced_at_ms, updated_at_ms)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
			req.MessageID, req.Topic, partition, req.PartitionKey, seq, status,
			transactional, stringPtr(req.TxnID), bodyDigest, req.Body, now, now); err != nil {
			return nil, err
		}
		if req.Transactional {
			if _, err := tx.ExecContext(ctx, `INSERT INTO txn_visibility
				(txn_id, message_id, topic, partition_no, seq, status) VALUES (?,?,?,?,?,?)`,
				req.TxnID, req.MessageID, req.Topic, partition, seq, TxnPending); err != nil {
				return nil, err
			}
		}
		ps.NextSeq++
		if err := s.advanceVisibilityFrontierTx(ctx, tx, ps, false); err != nil {
			return nil, err
		}
		if err := s.savePartitionTx(ctx, tx, ps); err != nil {
			return nil, err
		}
		m, err := s.loadMessageTx(ctx, tx, req.MessageID)
		if err != nil {
			return nil, err
		}
		return PublishResult{Message: m, RequestID: decision.RequestID}, nil
	})
	if err != nil {
		return zero, err
	}
	var out PublishResult
	if err := json.Unmarshal(raw, &out); err != nil {
		return zero, err
	}
	return out, nil
}

func (s *Service) GetPartition(ctx context.Context, topic string, partition int) (PartitionView, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return PartitionView{}, err
	}
	defer tx.Rollback()
	ps, err := s.loadPartitionTx(ctx, tx, topic, partition)
	if err != nil {
		return PartitionView{}, err
	}
	return s.partitionViewTx(ctx, tx, ps)
}

func (s *Service) SetPartitionPriority(ctx context.Context, topic string, partition, priority int) (PartitionView, error) {
	if priority <= 0 {
		return PartitionView{}, newErr(CodeInvalidArgument, "priority must be positive")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PartitionView{}, err
	}
	defer tx.Rollback()
	ps, err := s.loadPartitionTx(ctx, tx, topic, partition)
	if err != nil {
		return PartitionView{}, err
	}
	ps.Priority = priority
	if err := s.savePartitionTx(ctx, tx, ps); err != nil {
		return PartitionView{}, err
	}
	view, err := s.partitionViewTx(ctx, tx, ps)
	if err != nil {
		return PartitionView{}, err
	}
	if err := tx.Commit(); err != nil {
		return PartitionView{}, err
	}
	return view, nil
}

func (s *Service) ListPartitions(ctx context.Context) ([]PartitionView, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT topic, partition_no FROM partitions
		ORDER BY topic, partition_no`)
	if err != nil {
		return nil, err
	}
	type key struct {
		topic string
		p     int
	}
	keys := make([]key, 0)
	for rows.Next() {
		var k key
		if err := rows.Scan(&k.topic, &k.p); err != nil {
			rows.Close()
			return nil, err
		}
		keys = append(keys, k)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	out := make([]PartitionView, 0, len(keys))
	for _, k := range keys {
		ps, err := s.loadPartitionTx(ctx, tx, k.topic, k.p)
		if err != nil {
			return nil, err
		}
		v, err := s.partitionViewTx(ctx, tx, ps)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func (s *Service) ListMessages(ctx context.Context, topic string, partition int, limit int) ([]Message, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := `SELECT message_id, topic, partition_no, partition_key, seq, status, transactional,
		COALESCE(txn_id,''), body_digest, body, produced_at_ms, updated_at_ms FROM messages`
	args := []any{}
	if topic != "" {
		query += ` WHERE topic=?`
		args = append(args, topic)
		if partition >= 0 {
			query += ` AND partition_no=?`
			args = append(args, partition)
		}
	}
	query += ` ORDER BY produced_at_ms DESC, topic, partition_no, seq LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Message{}
	for rows.Next() {
		var m Message
		var transactional int
		if err := rows.Scan(&m.MessageID, &m.Topic, &m.Partition, &m.PartitionKey, &m.Seq,
			&m.Status, &transactional, &m.TxnID, &m.BodyDigest, &m.Body,
			&m.ProducedAtMS, &m.UpdatedAtMS); err != nil {
			return nil, err
		}
		m.Transactional = transactional != 0
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Service) ListTransactions(ctx context.Context, pendingOnly bool) ([]Transaction, error) {
	where := ""
	if pendingOnly {
		where = " WHERE status='PENDING'"
	}
	rows, err := s.db.QueryContext(ctx, `SELECT txn_id, message_id, topic, partition_no, seq,
		status, COALESCE(decision,''), query_count, COALESCE(coordinator_result,''),
		COALESCE(queried_at_ms,0), COALESCE(decided_at_ms,0)
		FROM txn_visibility`+where+` ORDER BY topic, partition_no, seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Transaction{}
	for rows.Next() {
		var t Transaction
		if err := rows.Scan(&t.TxnID, &t.MessageID, &t.Topic, &t.Partition, &t.Seq,
			&t.Status, &t.Decision, &t.QueryCount, &t.CoordinatorResult,
			&t.QueriedAtMS, &t.DecidedAtMS); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Service) RetainFrom(ctx context.Context, topic string, partition int, req RetainRequest) (PartitionView, error) {
	if req.FromSeq < 0 {
		return PartitionView{}, newErr(CodeInvalidArgument, "from_seq must be non-negative")
	}
	decision, err := s.prepareOperation(ctx, req.RequestID, "retain",
		fmt.Sprintf("%s/%d", topic, partition), req, nil)
	if err != nil {
		return PartitionView{}, err
	}
	if decision.InProgress {
		decision, err = s.awaitOperation(ctx, decision.RequestID)
		if err != nil {
			return PartitionView{}, err
		}
	}
	if decision.StoredErr != nil {
		return PartitionView{}, decision.StoredErr
	}
	if decision.Response != nil {
		var out PartitionView
		if err := decodeStoredResponse(string(decision.Response), &out); err != nil {
			return PartitionView{}, err
		}
		return out, nil
	}
	raw, err := s.commitOperation(ctx, decision.Op, func(tx *sql.Tx) (any, error) {
		ps, err := s.loadPartitionTx(ctx, tx, topic, partition)
		if err != nil {
			return nil, err
		}
		if req.FromSeq > ps.ConsumeCursor {
			return nil, newErr(CodeInvalidArgument, "retention cannot pass the consumer cursor")
		}
		if req.FromSeq > ps.NextSeq {
			return nil, newErr(CodeInvalidArgument, "retention cannot pass the produced sequence")
		}
		if req.FromSeq < ps.EarliestRetainedSeq {
			return nil, newErr(CodeInvalidArgument, "retention cannot move backwards")
		}
		ps.EarliestRetainedSeq = req.FromSeq
		if err := s.savePartitionTx(ctx, tx, ps); err != nil {
			return nil, err
		}
		return s.partitionViewTx(ctx, tx, ps)
	})
	if err != nil {
		return PartitionView{}, err
	}
	var out PartitionView
	if err := json.Unmarshal(raw, &out); err != nil {
		return PartitionView{}, err
	}
	return out, nil
}

func (s *Service) ListStateTransitions(ctx context.Context, limit int) ([]map[string]any, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, entity_kind, entity_id, from_state,
		to_state, reason, at_ms FROM state_transitions ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, at int64
		var kind, entity, from, to, reason string
		if err := rows.Scan(&id, &kind, &entity, &from, &to, &reason, &at); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"id": id, "entity_kind": kind, "entity_id": entity,
			"from": from, "to": to, "reason": reason, "at_ms": at})
	}
	return out, rows.Err()
}

func (s *Service) listRecentMessagesTx(ctx context.Context, tx *sql.Tx, limit int) ([]Message, error) {
	rows, err := tx.QueryContext(ctx, `SELECT message_id, topic, partition_no, partition_key,
		seq, status, transactional, COALESCE(txn_id,''), body_digest, body,
		produced_at_ms, updated_at_ms FROM messages
		ORDER BY produced_at_ms DESC, topic, partition_no, seq DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Message{}
	for rows.Next() {
		var m Message
		var tr int
		if err := rows.Scan(&m.MessageID, &m.Topic, &m.Partition, &m.PartitionKey, &m.Seq,
			&m.Status, &tr, &m.TxnID, &m.BodyDigest, &m.Body, &m.ProducedAtMS, &m.UpdatedAtMS); err != nil {
			return nil, err
		}
		m.Transactional = tr != 0
		out = append(out, m)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].ProducedAtMS == out[j].ProducedAtMS {
			return out[i].MessageID > out[j].MessageID
		}
		return out[i].ProducedAtMS > out[j].ProducedAtMS
	})
	return out, rows.Err()
}
