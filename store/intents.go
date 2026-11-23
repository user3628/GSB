package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"releasegate/domain"
)

type Intent struct {
	IntentID            string
	ReleaseID           string
	Kind                string
	Status              string
	PayloadJSON         string
	FromState           string
	FromStage           int
	FromTrafficPerMille int
	FromVersion         int64
	CreatedAtMS         int64
	CompletedAtMS       int64
	RecoveryDetail      string
}

func (s *Store) BeginIntent(ctx context.Context, intent Intent) error {
	if intent.CreatedAtMS == 0 {
		intent.CreatedAtMS = time.Now().UnixMilli()
	}
	if intent.PayloadJSON == "" {
		intent.PayloadJSON = "{}"
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO intents
		(intent_id,release_id,kind,status,payload_json,from_state,from_stage,from_traffic_per_mille,from_version,created_at_ms)
		VALUES (?,?,?,?,?,?,?,?,?,?)`,
		intent.IntentID, intent.ReleaseID, intent.Kind, IntentPending, intent.PayloadJSON,
		intent.FromState, intent.FromStage, intent.FromTrafficPerMille, intent.FromVersion, intent.CreatedAtMS)
	return translateWriteErr(err)
}

func (s *Store) CompleteIntent(ctx context.Context, tx *sql.Tx, intentID, detail string) error {
	res, err := tx.ExecContext(ctx, `UPDATE intents SET status=?,completed_at_ms=?,recovery_detail=?
		WHERE intent_id=? AND status=?`, IntentCompleted, time.Now().UnixMilli(), detail, intentID, IntentPending)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return errors.New("intent was not pending at completion")
	}
	return nil
}

func (s *Store) FailIntent(ctx context.Context, intentID, detail string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE intents SET status=?,completed_at_ms=?,recovery_detail=?
		WHERE intent_id=? AND status=?`, IntentFailed, time.Now().UnixMilli(), detail, intentID, IntentPending)
	return err
}

func (s *Store) PendingIntents(ctx context.Context, q DBTX) ([]Intent, error) {
	rows, err := q.QueryContext(ctx, `SELECT intent_id,release_id,kind,status,payload_json,from_state,from_stage,
		from_traffic_per_mille,from_version,created_at_ms,COALESCE(completed_at_ms,0),recovery_detail
		FROM intents WHERE status=? ORDER BY created_at_ms,intent_id`, IntentPending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Intent
	for rows.Next() {
		var in Intent
		if err := rows.Scan(&in.IntentID, &in.ReleaseID, &in.Kind, &in.Status, &in.PayloadJSON, &in.FromState, &in.FromStage,
			&in.FromTrafficPerMille, &in.FromVersion, &in.CreatedAtMS, &in.CompletedAtMS, &in.RecoveryDetail); err != nil {
			return nil, err
		}
		out = append(out, in)
	}
	return out, rows.Err()
}

func (s *Store) Intent(ctx context.Context, q DBTX, intentID string) (Intent, error) {
	var in Intent
	err := q.QueryRowContext(ctx, `SELECT intent_id,release_id,kind,status,payload_json,from_state,from_stage,
		from_traffic_per_mille,from_version,created_at_ms,COALESCE(completed_at_ms,0),recovery_detail
		FROM intents WHERE intent_id=?`, intentID).Scan(
		&in.IntentID, &in.ReleaseID, &in.Kind, &in.Status, &in.PayloadJSON, &in.FromState, &in.FromStage,
		&in.FromTrafficPerMille, &in.FromVersion, &in.CreatedAtMS, &in.CompletedAtMS, &in.RecoveryDetail)
	if errors.Is(err, sql.ErrNoRows) {
		return Intent{}, domain.WithContext(domain.NewError(domain.CodeNotFound, "intent not found"), "intent_id", intentID)
	}
	return in, err
}

func MarshalIntentPayload(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}
