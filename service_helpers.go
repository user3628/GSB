package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Service struct {
	db             *sql.DB
	writeMu        sync.Mutex
	leaseTTL       time.Duration
	coordinatorURL string
	httpClient     *http.Client
	now            func() time.Time
	txnLocks       sync.Map
	idemWait       time.Duration
}

func NewService(db *sql.DB, coordinatorURL string, leaseTTL time.Duration) *Service {
	return &Service{
		db:             db,
		leaseTTL:       leaseTTL,
		coordinatorURL: strings.TrimRight(coordinatorURL, "/"),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				DisableKeepAlives: true,
			},
		},
		now:      time.Now,
		idemWait: 6 * time.Second,
	}
}

func (s *Service) Close() error { return s.db.Close() }

type partitionState struct {
	Topic                 string
	Partition             int
	NextSeq               int64
	VisibleFrontier       int64
	ConsumeCursor         int64
	EarliestRetainedSeq   int64
	Priority              int
	LeaseState            string
	LeaseOwner            string
	LeaseVersion          int64
	LeaseExpiresAtMS      int64
	LeaseCursor           int64
	LeaseLastDeliveredSeq *int64
	ReplayActive          bool
	ReplayFrom            *int64
	ReplayLimit           *int64
	UpdatedAtMS           int64
}

func nullableInt64(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func scanNullableInt64(v sql.NullInt64) int64 {
	if v.Valid {
		return v.Int64
	}
	return 0
}

func (s *Service) loadPartitionTx(ctx context.Context, tx *sql.Tx, topic string, p int) (*partitionState, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT topic, partition_no, next_seq, visible_frontier, consume_cursor,
		       earliest_retained_seq, priority, lease_state, COALESCE(lease_owner,''),
		       lease_version, COALESCE(lease_expires_at_ms,0), lease_cursor,
		       lease_last_delivered_seq, replay_active, replay_from, replay_limit, updated_at_ms
		FROM partitions WHERE topic = ? AND partition_no = ?`, topic, p)
	var ps partitionState
	var last, replayFrom, replayLimit sql.NullInt64
	var replayActive int
	if err := row.Scan(&ps.Topic, &ps.Partition, &ps.NextSeq, &ps.VisibleFrontier,
		&ps.ConsumeCursor, &ps.EarliestRetainedSeq, &ps.Priority, &ps.LeaseState,
		&ps.LeaseOwner, &ps.LeaseVersion, &ps.LeaseExpiresAtMS, &ps.LeaseCursor,
		&last, &replayActive, &replayFrom, &replayLimit, &ps.UpdatedAtMS); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, newErr(CodeNotFound, fmt.Sprintf("partition %s/%d not found", topic, p))
		}
		return nil, err
	}
	if last.Valid {
		v := last.Int64
		ps.LeaseLastDeliveredSeq = &v
	}
	ps.ReplayActive = replayActive != 0
	if replayFrom.Valid {
		v := replayFrom.Int64
		ps.ReplayFrom = &v
	}
	if replayLimit.Valid {
		v := replayLimit.Int64
		ps.ReplayLimit = &v
	}
	return &ps, nil
}

func (s *Service) savePartitionTx(ctx context.Context, tx *sql.Tx, ps *partitionState) error {
	ps.UpdatedAtMS = nowMS(s.now())
	_, err := tx.ExecContext(ctx, `
		UPDATE partitions SET next_seq=?, visible_frontier=?, consume_cursor=?,
		       earliest_retained_seq=?, priority=?, lease_state=?, lease_owner=?,
		       lease_version=?, lease_expires_at_ms=?, lease_cursor=?,
		       lease_last_delivered_seq=?, replay_active=?, replay_from=?, replay_limit=?,
		       updated_at_ms=?
		WHERE topic=? AND partition_no=?`,
		ps.NextSeq, ps.VisibleFrontier, ps.ConsumeCursor, ps.EarliestRetainedSeq,
		ps.Priority, ps.LeaseState, stringPtr(ps.LeaseOwner), ps.LeaseVersion,
		nullableInt64(ps.LeaseExpiresAtMS), ps.LeaseCursor, ps.LeaseLastDeliveredSeq,
		boolInt(ps.ReplayActive), ps.ReplayFrom, ps.ReplayLimit, ps.UpdatedAtMS,
		ps.Topic, ps.Partition)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO consumer_offsets
		  (topic, partition_no, committed_seq, lease_state, lease_owner, lease_version,
		   lease_expires_at_ms, lease_cursor, replay_active, replay_from, replay_limit, updated_at_ms)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(topic, partition_no) DO UPDATE SET
		  committed_seq=excluded.committed_seq, lease_state=excluded.lease_state,
		  lease_owner=excluded.lease_owner, lease_version=excluded.lease_version,
		  lease_expires_at_ms=excluded.lease_expires_at_ms, lease_cursor=excluded.lease_cursor,
		  replay_active=excluded.replay_active, replay_from=excluded.replay_from,
		  replay_limit=excluded.replay_limit, updated_at_ms=excluded.updated_at_ms`,
		ps.Topic, ps.Partition, ps.ConsumeCursor, ps.LeaseState, stringPtr(ps.LeaseOwner),
		ps.LeaseVersion, nullableInt64(ps.LeaseExpiresAtMS), ps.LeaseCursor,
		boolInt(ps.ReplayActive), ps.ReplayFrom, ps.ReplayLimit, ps.UpdatedAtMS)
	return err
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func (s *Service) transitionMessageTx(ctx context.Context, tx *sql.Tx, messageID, from, to, reason string) error {
	if !canTransitionMessage(from, to) {
		return newErrDetails(CodeInvalidTransition,
			fmt.Sprintf("message transition %s -> %s is not allowed", from, to),
			map[string]any{"entity": "message", "id": messageID, "from": from, "to": to})
	}
	res, err := tx.ExecContext(ctx, `UPDATE messages SET status=?, updated_at_ms=?
		WHERE message_id=? AND status=?`, to, nowMS(s.now()), messageID, from)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return newErrDetails(CodeInvalidTransition,
			fmt.Sprintf("message %s is not in state %s", messageID, from),
			map[string]any{"entity": "message", "id": messageID, "from": from, "to": to})
	}
	return s.recordTransitionTx(ctx, tx, "message", messageID, from, to, reason)
}

func (s *Service) transitionLeaseTx(ctx context.Context, tx *sql.Tx, ps *partitionState, to, reason string) error {
	if ps.LeaseState == to {
		return nil
	}
	if !canTransitionLease(ps.LeaseState, to) {
		return newErrDetails(CodeInvalidTransition,
			fmt.Sprintf("lease transition %s -> %s is not allowed", ps.LeaseState, to),
			map[string]any{"entity": "lease", "topic": ps.Topic, "partition": ps.Partition,
				"from": ps.LeaseState, "to": to})
	}
	from := ps.LeaseState
	ps.LeaseState = to
	ps.LeaseVersion++
	if to == LeaseExpired || to == LeaseFree {
		ps.LeaseOwner = ""
		ps.LeaseExpiresAtMS = 0
		ps.LeaseLastDeliveredSeq = nil
		ps.LeaseCursor = ps.ConsumeCursor
	}
	if err := s.savePartitionTx(ctx, tx, ps); err != nil {
		return err
	}
	return s.recordTransitionTx(ctx, tx, "lease",
		fmt.Sprintf("%s/%d", ps.Topic, ps.Partition), from, to, reason)
}

func (s *Service) recordTransitionTx(ctx context.Context, tx *sql.Tx, kind, id, from, to, reason string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO state_transitions
		(entity_kind, entity_id, from_state, to_state, reason, at_ms) VALUES (?,?,?,?,?,?)`,
		kind, id, from, to, reason, nowMS(s.now()))
	return err
}

func (s *Service) expireLeaseIfNeededTx(ctx context.Context, tx *sql.Tx, ps *partitionState) (bool, error) {
	if ps.LeaseState != LeaseLeased || ps.LeaseExpiresAtMS == 0 {
		return false, nil
	}
	if nowMS(s.now()) < ps.LeaseExpiresAtMS {
		return false, nil
	}
	if err := s.transitionLeaseTx(ctx, tx, ps, LeaseExpired, "lease_idle_timeout"); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Service) advanceVisibilityFrontierTx(ctx context.Context, tx *sql.Tx, ps *partitionState, allowDuringReplay bool) error {
	if ps.ReplayActive && !allowDuringReplay {
		return nil
	}
	limit := ps.NextSeq
	if ps.ReplayActive && ps.ReplayLimit != nil && *ps.ReplayLimit < limit {
		limit = *ps.ReplayLimit
	}
	for pos := ps.VisibleFrontier; pos < limit; pos++ {
		var status string
		err := tx.QueryRowContext(ctx, `SELECT status FROM messages
			WHERE topic=? AND partition_no=? AND seq=?`, ps.Topic, ps.Partition, pos).Scan(&status)
		if errors.Is(err, sql.ErrNoRows) {
			break
		}
		if err != nil {
			return err
		}
		if status == MsgPendingInvisible {
			break
		}
		ps.VisibleFrontier = pos + 1
	}
	return s.savePartitionTx(ctx, tx, ps)
}

func (s *Service) partitionViewTx(ctx context.Context, tx *sql.Tx, ps *partitionState) (PartitionView, error) {
	effective := ps.VisibleFrontier
	if ps.ReplayActive && ps.ReplayLimit != nil && *ps.ReplayLimit < effective {
		effective = *ps.ReplayLimit
	}
	var pending int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM txn_visibility
		WHERE topic=? AND partition_no=? AND status='PENDING'`, ps.Topic, ps.Partition).Scan(&pending); err != nil {
		return PartitionView{}, err
	}
	var backlog int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages
		WHERE topic=? AND partition_no=? AND seq>=? AND status IN ('VISIBLE','PENDING_INVISIBLE')`,
		ps.Topic, ps.Partition, ps.ConsumeCursor).Scan(&backlog); err != nil {
		return PartitionView{}, err
	}
	return PartitionView{
		Topic:                 ps.Topic,
		Partition:             ps.Partition,
		Priority:              ps.Priority,
		NextSeq:               ps.NextSeq,
		VisibleFrontier:       ps.VisibleFrontier,
		EffectiveFrontier:     effective,
		ConsumeCursor:         ps.ConsumeCursor,
		EarliestRetainedSeq:   ps.EarliestRetainedSeq,
		LeaseState:            ps.LeaseState,
		LeaseOwner:            ps.LeaseOwner,
		LeaseVersion:          ps.LeaseVersion,
		LeaseExpiresAtMS:      ps.LeaseExpiresAtMS,
		LeaseCursor:           ps.LeaseCursor,
		LeaseLastDeliveredSeq: ps.LeaseLastDeliveredSeq,
		ReplayActive:          ps.ReplayActive,
		ReplayFrom:            ps.ReplayFrom,
		ReplayLimit:           ps.ReplayLimit,
		PendingTxnCount:       pending,
		Backlog:               backlog,
		UpdatedAtMS:           ps.UpdatedAtMS,
	}, nil
}

func (s *Service) loadMessageTx(ctx context.Context, tx *sql.Tx, messageID string) (Message, error) {
	var m Message
	var txnID sql.NullString
	var transactional int
	err := tx.QueryRowContext(ctx, `SELECT message_id, topic, partition_no, partition_key, seq, status,
		transactional, COALESCE(txn_id,''), body_digest, body, produced_at_ms, updated_at_ms
		FROM messages WHERE message_id=?`, messageID).Scan(
		&m.MessageID, &m.Topic, &m.Partition, &m.PartitionKey, &m.Seq, &m.Status, &transactional,
		&txnID, &m.BodyDigest, &m.Body, &m.ProducedAtMS, &m.UpdatedAtMS)
	if errors.Is(err, sql.ErrNoRows) {
		return m, newErr(CodeNotFound, "message not found: "+messageID)
	}
	if err != nil {
		return m, err
	}
	m.Transactional = transactional != 0
	m.TxnID = txnID.String
	return m, nil
}

func (s *Service) loadTxnTx(ctx context.Context, tx *sql.Tx, txnID string) (Transaction, error) {
	var txn Transaction
	var decision, coord sql.NullString
	var queried, decided sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT txn_id, message_id, topic, partition_no, seq,
		status, decision, query_count, coordinator_result, queried_at_ms, decided_at_ms
		FROM txn_visibility WHERE txn_id=?`, txnID).Scan(
		&txn.TxnID, &txn.MessageID, &txn.Topic, &txn.Partition, &txn.Seq, &txn.Status,
		&decision, &txn.QueryCount, &coord, &queried, &decided)
	if errors.Is(err, sql.ErrNoRows) {
		return txn, newErr(CodeNotFound, "transaction not found: "+txnID)
	}
	if err != nil {
		return txn, err
	}
	txn.Decision = decision.String
	txn.CoordinatorResult = coord.String
	txn.QueriedAtMS = scanNullableInt64(queried)
	txn.DecidedAtMS = scanNullableInt64(decided)
	return txn, nil
}

func routePartition(topic Topic, req PublishRequest) (int, error) {
	if topic.RoutingRule == "explicit" {
		if req.Partition == nil {
			return 0, newErr(CodeRouteInvalid, "explicit routing requires partition")
		}
		if *req.Partition < 0 || *req.Partition >= topic.Partitions {
			return 0, newErr(CodeRouteInvalid, "partition number out of range")
		}
		return *req.Partition, nil
	}
	if topic.RoutingRule == "hash" {
		if strings.TrimSpace(req.PartitionKey) == "" {
			return 0, newErr(CodeRouteInvalid, "hash routing requires a non-empty partition_key")
		}
		h := fnv.New32a()
		_, _ = h.Write([]byte(req.PartitionKey))
		return int(h.Sum32() % uint32(topic.Partitions)), nil
	}
	return 0, newErr(CodeRouteInvalid, "unsupported routing rule: "+topic.RoutingRule)
}

func (s *Service) getTopicTx(ctx context.Context, tx *sql.Tx, name string) (Topic, error) {
	var t Topic
	err := tx.QueryRowContext(ctx, `SELECT name, partitions, routing_rule, created_at_ms
		FROM topics WHERE name=?`, name).Scan(&t.Name, &t.Partitions, &t.RoutingRule, &t.CreatedAtMS)
	if errors.Is(err, sql.ErrNoRows) {
		return t, newErr(CodeNotFound, "topic not found: "+name)
	}
	return t, err
}

func (s *Service) txnLock(txnID string) *sync.Mutex {
	v, _ := s.txnLocks.LoadOrStore(txnID, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func (s *Service) checkSQLiteBusy(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(strings.ToLower(err.Error()), "database is locked") {
		return newErr(CodeDatabase, "sqlite database is busy")
	}
	return err
}

func decodeStoredResponse(raw string, target any) error {
	if raw == "" {
		return nil
	}
	return json.Unmarshal([]byte(raw), target)
}
