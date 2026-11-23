package cache

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
)

const (
	intentWrite      = "WRITE"
	intentTransition = "TRANSITION"
	intentEvict      = "EVICT"
	intentSnapshot   = "SNAPSHOT"

	intentRunning = "RUNNING"
	intentDone    = "DONE"
	intentAborted = "ABORTED"
)

type intentRecord struct {
	IntentID    string
	Operation   string
	Target      string
	Status      string
	Payload     []byte
	CreatedAtNS int64
}

func randomID(prefix string) (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", wrapError(CodeInternal, "cannot generate identifier", nil, err)
	}
	return prefix + hex.EncodeToString(buf), nil
}

// insertIntent commits RUNNING before the corresponding mutation transaction.
// That ordering is what makes recovery possible after a process kill.
func (s *Store) insertIntent(ctx context.Context, operation, target string, payload any) (string, error) {
	id, err := randomID("int_")
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", wrapError(CodeInternal, "cannot encode intent payload", map[string]any{"operation": operation}, err)
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO intents(intent_id, operation, target, status, payload, created_at_ns)
VALUES (?, ?, ?, ?, ?, ?)`, id, operation, target, intentRunning, string(data), ns(s.Now()))
	if err != nil {
		return "", wrapError(CodeIO, "cannot persist intent", map[string]any{"operation": operation, "target": target}, err)
	}
	return id, nil
}

func markIntentDoneTx(ctx context.Context, tx *sql.Tx, id string, at int64) error {
	result, err := tx.ExecContext(ctx, `UPDATE intents SET status=?, completed_at_ns=? WHERE intent_id=? AND status='RUNNING'`, intentDone, at, id)
	if err != nil {
		return wrapError(CodeIO, "cannot complete intent", map[string]any{"intent_id": id}, err)
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return newError(CodeInternal, "intent was not RUNNING", map[string]any{"intent_id": id})
	}
	return nil
}

func (s *Store) markIntentAborted(ctx context.Context, id string) {
	if id == "" {
		return
	}
	_, _ = s.db.ExecContext(ctx, `UPDATE intents SET status=?, completed_at_ns=? WHERE intent_id=? AND status='RUNNING'`, intentAborted, ns(s.Now()), id)
}

func (s *Store) abortRunningIntentsForTarget(ctx context.Context, target string) {
	_, _ = s.db.ExecContext(ctx, `UPDATE intents SET status=?, completed_at_ns=? WHERE target=? AND status='RUNNING'`, intentAborted, ns(s.Now()), target)
}

func loadRunningIntents(ctx context.Context, db *sql.DB) ([]intentRecord, error) {
	rows, err := db.QueryContext(ctx, `
SELECT intent_id, operation, target, status, payload, created_at_ns
FROM intents WHERE status='RUNNING'
ORDER BY created_at_ns ASC, intent_id ASC`)
	if err != nil {
		return nil, dbError("cannot scan unfinished intents", err)
	}
	defer rows.Close()
	var intents []intentRecord
	for rows.Next() {
		var record intentRecord
		var payload string
		if err := rows.Scan(&record.IntentID, &record.Operation, &record.Target, &record.Status, &payload, &record.CreatedAtNS); err != nil {
			return nil, dbError("cannot decode unfinished intent", err)
		}
		record.Payload = []byte(payload)
		intents = append(intents, record)
	}
	if err := rows.Err(); err != nil {
		return nil, dbError("cannot iterate unfinished intents", err)
	}
	return intents, nil
}
