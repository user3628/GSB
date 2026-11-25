package bus

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"strings"
)

type eventContent struct {
	EventID      string  `json:"event_id"`
	Topic        string  `json:"topic"`
	PartitionKey string  `json:"partition_key"`
	EventTimeMS  int64   `json:"event_time_ms"`
	BodyDigest   string  `json:"body_digest"`
	BatchID      string  `json:"batch_id"`
	AggregateKey string  `json:"aggregate_key"`
	Metric       float64 `json:"metric"`
}

func (s *Service) Publish(ctx context.Context, request PublishRequest) (PublishResult, error) {
	request.EventID = strings.TrimSpace(request.EventID)
	request.Topic = strings.TrimSpace(request.Topic)
	request.PartitionKey = strings.TrimSpace(request.PartitionKey)
	request.BatchID = strings.TrimSpace(request.BatchID)
	request.AggregateKey = strings.TrimSpace(request.AggregateKey)
	if request.EventID == "" || request.Topic == "" || request.PartitionKey == "" || request.BatchID == "" || request.AggregateKey == "" {
		return PublishResult{}, invalidArgument("event_id, topic, partition_key, batch_id and aggregate_key are required")
	}
	if math.IsNaN(request.Metric) || math.IsInf(request.Metric, 0) {
		return PublishResult{}, invalidArgument("metric must be finite")
	}
	if request.EventTimeMS < 0 {
		return PublishResult{}, invalidArgument("event_time_ms must be non-negative")
	}
	digest, err := eventBodyDigest(request)
	if err != nil {
		return PublishResult{}, err
	}
	request.BodyDigest = digest
	content := eventContent{
		EventID:      request.EventID,
		Topic:        request.Topic,
		PartitionKey: request.PartitionKey,
		EventTimeMS:  request.EventTimeMS,
		BodyDigest:   request.BodyDigest,
		BatchID:      request.BatchID,
		AggregateKey: request.AggregateKey,
		Metric:       request.Metric,
	}
	hash := contentHash("publish", content)
	return idempotentJSON(s, ctx, "publish", request.RequestID, hash, func() (PublishResult, error) {
		s.publishMu.Lock()
		defer s.publishMu.Unlock()

		existing, err := s.getEventByID(ctx, request.EventID)
		if err == nil {
			if existing.ContentHash != contentHash("event", content) {
				return PublishResult{}, NewError(CodeConflictingEvent, "event id already exists with different content")
			}
			return PublishResult{
				EventID:     existing.EventID,
				Topic:       existing.Topic,
				Partition:   existing.Partition,
				Seq:         existing.Seq,
				BodyDigest:  existing.BodyDigest,
				State:       string(StateAccepted),
				Idempotent:  true,
				CreatedAtMS: existing.CreatedAtMS,
			}, nil
		}
		if ErrorCode(err) != CodeNotFound {
			return PublishResult{}, err
		}
		if err := s.validateTopicPartition(ctx, s.db, request.Topic, 0); err != nil {
			return PublishResult{}, err
		}
		var partitions int
		if err := s.db.QueryRowContext(ctx, `SELECT partitions FROM topics WHERE topic = ?`, request.Topic).Scan(&partitions); err != nil {
			return PublishResult{}, WrapError(CodeInternal, "read partition count", err)
		}
		partition := routePartition(request.PartitionKey, partitions)
		eventHash := contentHash("event", content)
		it, err := s.beginIntent(ctx, "PUBLISH", request.RequestID, hash, request.Topic, "", partition, nil, content)
		if err != nil {
			return PublishResult{}, err
		}
		if err := s.injectFault("publish_after_intent"); err != nil {
			return PublishResult{}, err
		}
		now := s.nowMS()
		tx, release, err := s.beginWriteTx(ctx)
		if err != nil {
			_ = s.finishIntent(context.WithoutCancel(ctx), it.ID, "ROLLED_BACK")
			return PublishResult{}, WrapError(CodeInternal, "begin publish transaction", err)
		}
		defer release()
		defer tx.Rollback()
		var seq int64
		if err := tx.QueryRowContext(ctx, `
			UPDATE partition_counters SET last_seq = last_seq + 1, last_intent_id = ?
			WHERE topic = ? AND partition = ?
			RETURNING last_seq`, it.ID, request.Topic, partition).Scan(&seq); err != nil {
			_ = s.finishIntent(context.WithoutCancel(ctx), it.ID, "ROLLED_BACK")
			return PublishResult{}, WrapError(CodeInternal, "allocate production sequence", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO events(event_id, topic, partition, seq, partition_key, event_time_ms, body_digest,
				batch_id, aggregate_key, metric, content_hash, created_at_ms, intent_id)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			request.EventID, request.Topic, partition, seq, request.PartitionKey, request.EventTimeMS,
			request.BodyDigest, request.BatchID, request.AggregateKey, request.Metric, eventHash, now, it.ID); err != nil {
			_ = s.finishIntent(context.WithoutCancel(ctx), it.ID, "ROLLED_BACK")
			return PublishResult{}, WrapError(CodeInternal, "insert event", err)
		}
		if err := tx.Commit(); err != nil {
			_ = s.finishIntent(context.WithoutCancel(ctx), it.ID, "ROLLED_BACK")
			return PublishResult{}, WrapError(CodeInternal, "commit publish", err)
		}
		release()
		if err := s.injectFault("publish_after_commit"); err != nil {
			return PublishResult{}, err
		}
		if err := s.finishIntent(ctx, it.ID, "COMMITTED"); err != nil {
			return PublishResult{}, err
		}
		return PublishResult{
			EventID:     request.EventID,
			Topic:       request.Topic,
			Partition:   partition,
			Seq:         seq,
			BodyDigest:  request.BodyDigest,
			State:       string(StateAccepted),
			Idempotent:  false,
			CreatedAtMS: now,
		}, nil
	})
}

type storedEvent struct {
	EventID      string
	Topic        string
	Partition    int
	Seq          int64
	PartitionKey string
	EventTimeMS  int64
	BodyDigest   string
	BatchID      string
	AggregateKey string
	Metric       float64
	ContentHash  string
	CreatedAtMS  int64
}

func (s *Service) getEventByID(ctx context.Context, eventID string) (storedEvent, error) {
	var event storedEvent
	err := s.db.QueryRowContext(ctx, `
		SELECT event_id, topic, partition, seq, partition_key, event_time_ms, body_digest,
			batch_id, aggregate_key, metric, content_hash, created_at_ms
		FROM events WHERE event_id = ?`, eventID).
		Scan(&event.EventID, &event.Topic, &event.Partition, &event.Seq, &event.PartitionKey,
			&event.EventTimeMS, &event.BodyDigest, &event.BatchID, &event.AggregateKey,
			&event.Metric, &event.ContentHash, &event.CreatedAtMS)
	if err == sql.ErrNoRows {
		return storedEvent{}, NewError(CodeNotFound, "event does not exist: "+eventID)
	}
	if err != nil {
		return storedEvent{}, WrapError(CodeInternal, "read event", err)
	}
	return event, nil
}

func eventBodyDigest(request PublishRequest) (string, error) {
	if len(request.Body) == 0 {
		if request.BodyDigest == "" {
			return "", invalidArgument("body or body_digest is required")
		}
		return request.BodyDigest, nil
	}
	var value any
	if err := json.Unmarshal(request.Body, &value); err != nil {
		return "", invalidArgument("body must be valid JSON: %v", err)
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", invalidArgument("body cannot be canonicalized: %v", err)
	}
	sum := sha256.Sum256(canonical)
	digest := hex.EncodeToString(sum[:])
	if request.BodyDigest != "" && request.BodyDigest != digest {
		return "", invalidArgument("body_digest does not match body")
	}
	return digest, nil
}

func routePartition(key string, partitions int) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum64() % uint64(partitions))
}

func (e storedEvent) delivered() DeliveredEvent {
	return DeliveredEvent{
		EventID:      e.EventID,
		Topic:        e.Topic,
		Partition:    e.Partition,
		Seq:          e.Seq,
		PartitionKey: e.PartitionKey,
		EventTimeMS:  e.EventTimeMS,
		BatchID:      e.BatchID,
		AggregateKey: e.AggregateKey,
		Metric:       e.Metric,
		BodyDigest:   e.BodyDigest,
		State:        StateAccepted,
	}
}

func describeEvent(e storedEvent) string {
	return fmt.Sprintf("%s/%d/%d", e.Topic, e.Partition, e.Seq)
}
