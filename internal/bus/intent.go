package bus

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

type intent struct {
	ID        string
	Operation string
}

func randomID(prefix string) string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic("crypto/rand unavailable: " + err.Error())
	}
	return prefix + hex.EncodeToString(raw[:])
}

func (s *Service) beginIntent(
	ctx context.Context,
	operation, requestID, requestHash, topic, group string,
	partition int,
	prior any,
	payload any,
) (*intent, error) {
	id := randomID("int_")
	priorJSON, err := json.Marshal(prior)
	if err != nil {
		return nil, WrapError(CodeInternal, "encode intent prior state", err)
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return nil, WrapError(CodeInternal, "encode intent payload", err)
	}
	now := s.nowMS()
	_, err = s.execWrite(ctx, `
		INSERT INTO intents(id, operation, request_id, request_hash, topic, group_name, partition,
			status, prior_json, payload_json, created_at_ms)
		VALUES(?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?)`,
		id, operation, requestID, requestHash, topic, group, partition, string(priorJSON), string(payloadJSON), now)
	if err != nil {
		return nil, WrapError(CodeInternal, "write intent", err)
	}
	return &intent{ID: id, Operation: operation}, nil
}

func (s *Service) finishIntent(ctx context.Context, id, status string) error {
	result, err := s.execWrite(ctx,
		`UPDATE intents SET status = ?, finished_at_ms = ? WHERE id = ?`,
		status, s.nowMS(), id)
	if err != nil {
		return WrapError(CodeInternal, "finish intent", err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return NewError(CodeInternal, "intent disappeared: "+id)
	}
	return nil
}

func intentEvidence(ctx context.Context, tx *sql.Tx, id string) (string, error) {
	checks := []struct {
		label string
		query string
	}{
		{"ack", `SELECT COUNT(*) FROM acks WHERE intent_id = ?`},
		{"offset", `SELECT COUNT(*) FROM offset_history WHERE intent_id = ?`},
		{"aggregate_batch", `SELECT COUNT(*) FROM agg_batches WHERE intent_id = ?`},
		{"event", `SELECT COUNT(*) FROM events WHERE intent_id = ?`},
		{"topic", `SELECT COUNT(*) FROM topics WHERE intent_id = ?`},
		{"topic_group", `SELECT COUNT(*) FROM topic_groups WHERE intent_id = ?`},
		{"delivery", `SELECT COUNT(*) FROM event_states WHERE last_intent_id = ?`},
		{"lease", `SELECT COUNT(*) FROM consumer_offsets WHERE last_intent_id = ?`},
		{"partition_counter", `SELECT COUNT(*) FROM partition_counters WHERE last_intent_id = ?`},
	}
	for _, check := range checks {
		var count int64
		if err := tx.QueryRowContext(ctx, check.query, id).Scan(&count); err != nil {
			return "", err
		}
		if count > 0 {
			return fmt.Sprintf("%s:%d", check.label, count), nil
		}
	}
	return "", nil
}
