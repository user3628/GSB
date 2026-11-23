package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

type EntryUndo struct {
	Exists bool  `json:"exists"`
	Entry  Entry `json:"entry"`
}

type IntentPayload struct {
	Undo    []EntryUndo    `json:"undo,omitempty"`
	Details map[string]any `json:"details,omitempty"`
}

type Intent struct {
	ID              int64
	Kind            string
	RequestID       string
	SnapshotVersion int64
	Payload         IntentPayload
	Status          string
	BusinessCode    string
	CreatedAtMs     int64
	CompletedAtMs   int64
}

func CreateIntentTx(ctx context.Context, tx *sql.Tx, kind, requestID string, snapshotVersion int64, payload IntentPayload, nowMs int64) (int64, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO intents(kind,request_id,snapshot_version,payload_json,status,created_at_ms) VALUES(?,?,?,?,?,?)`,
		kind, requestID, snapshotVersion, string(raw), "pending", nowMs)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func MarkIntentCommittedTx(ctx context.Context, tx *sql.Tx, id int64, nowMs int64) error {
	res, err := tx.ExecContext(ctx, `UPDATE intents SET status='committed',completed_at_ms=? WHERE id=? AND status='pending'`, nowMs, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return errors.New("intent is not pending")
	}
	return nil
}

func AbortIntentTx(ctx context.Context, tx *sql.Tx, id int64, code string, nowMs int64) error {
	_, err := tx.ExecContext(ctx, `UPDATE intents SET status='aborted',business_code=?,completed_at_ms=? WHERE id=? AND status='pending'`, code, nowMs, id)
	return err
}

func ListPendingIntentsTx(ctx context.Context, tx *sql.Tx) ([]Intent, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id,kind,request_id,snapshot_version,payload_json,status,business_code,created_at_ms,completed_at_ms FROM intents WHERE status='pending' ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Intent
	for rows.Next() {
		var in Intent
		var raw string
		if err := rows.Scan(&in.ID, &in.Kind, &in.RequestID, &in.SnapshotVersion, &raw, &in.Status, &in.BusinessCode, &in.CreatedAtMs, &in.CompletedAtMs); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(raw), &in.Payload)
		out = append(out, in)
	}
	return out, rows.Err()
}

func ApplyUndoTx(ctx context.Context, tx *sql.Tx, undo []EntryUndo, nowMs int64) (int, error) {
	changed := 0
	for _, u := range undo {
		if !u.Exists {
			res, err := tx.ExecContext(ctx, `DELETE FROM cache_entries WHERE node_id=? AND cache_key=?`, u.Entry.NodeID, u.Entry.CacheKey)
			if err != nil {
				return changed, err
			}
			n, _ := res.RowsAffected()
			changed += int(n)
			continue
		}
		e := u.Entry
		if e.UpdatedAtMs == 0 {
			e.UpdatedAtMs = nowMs
		}
		id, err := UpsertEntryTx(ctx, tx, e)
		if err != nil {
			return changed, err
		}
		e.ID = id
		if err := ReplaceIndexesTx(ctx, tx, e, e.State != "PURGED"); err != nil {
			return changed, err
		}
		changed++
	}
	return changed, nil
}

func TimeMs(t time.Time) int64 { return t.UnixMilli() }
