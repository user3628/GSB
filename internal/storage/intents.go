package storage

import (
	"database/sql"
	"encoding/json"
)

// Intent kinds.
const (
	IntentIngestObservations = "INGEST_OBSERVATIONS"
	IntentReplaceSnapshot    = "REPLACE_SNAPSHOT"
	IntentRebuildDerived     = "REBUILD_DERIVED"
	IntentTransitionAlert    = "TRANSITION_ALERT"
)

// Intent statuses.
const (
	IntentStarted   = "STARTED"
	IntentCommitted = "COMMITTED"
)

// Intent is a durable write-ahead record. Every state-changing engine
// operation inserts a STARTED intent (and flushes it) before touching domain
// tables, then marks it COMMITTED inside the same transaction.
type Intent struct {
	ID        string `json:"intent_id"`
	Kind      string `json:"kind"`
	Status    string `json:"status"`
	Version   int64  `json:"version"`
	Payload   string `json:"payload_json"`
	CreatedAt int64  `json:"created_at_ms"`
}

// InsertIntent writes a STARTED intent inside tx and returns its ID.
func InsertIntent(tx *sql.Tx, id, kind string, version int64, payload any, nowMS int64) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		`INSERT INTO intents(intent_id,kind,status,version,payload_json,created_at_ms)
		 VALUES(?,?,?,?,?,?)`,
		id, kind, IntentStarted, version, string(raw), nowMS)
	return err
}

// CompleteIntent marks an intent COMMITTED inside tx.
func CompleteIntent(tx *sql.Tx, id string) error {
	_, err := tx.Exec(`UPDATE intents SET status=? WHERE intent_id=?`, IntentCommitted, id)
	return err
}

// ListUnfinishedIntents returns all STARTED intents in creation order. These
// are the only facts the crash recovery logic is allowed to rely on.
func ListUnfinishedIntents(q Querier) ([]Intent, error) {
	rows, err := q.Query(
		`SELECT intent_id,kind,status,version,payload_json,created_at_ms
		 FROM intents WHERE status=? ORDER BY created_at_ms, intent_id`, IntentStarted)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Intent
	for rows.Next() {
		var it Intent
		if err := rows.Scan(&it.ID, &it.Kind, &it.Status, &it.Version, &it.Payload, &it.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// MarkIntentCommitted is used by recovery to close a dangling intent after its
// rollback effects have been applied.
func MarkIntentCommitted(tx *sql.Tx, id string) error {
	return CompleteIntent(tx, id)
}
