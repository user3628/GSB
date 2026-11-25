package engine

import (
	"context"
	"database/sql"
	"encoding/json"

	"nonceguard/internal/model"
	"nonceguard/internal/store"
)

type OccupancyView struct {
	ClientID       string `json:"client_id"`
	Nonce          string `json:"nonce"`
	RequestID      string `json:"request_id"`
	BatchID        string `json:"batch_id"`
	IntentID       string `json:"intent_id"`
	ConfigVersion  int64  `json:"config_version"`
	FirstSeenMs    int64  `json:"first_seen_ms"`
	WindowMs       int64  `json:"window_ms"`
	ExpiresAtMs    int64  `json:"expires_at_ms"`
	RecycledFromMs int64  `json:"recycled_from_ms"`
	CreatedAtMs    int64  `json:"created_at_ms"`
}

type BatchItemView struct {
	Index           int             `json:"index"`
	RequestID       string          `json:"request_id"`
	Code            string          `json:"code"`
	IdempotentReuse bool            `json:"idempotent_reuse"`
	Result          json.RawMessage `json:"result"`
}

type BatchView struct {
	BatchID       string          `json:"batch_id"`
	ConfigVersion int64           `json:"config_version"`
	ReferenceMs   int64           `json:"reference_ms"`
	Mode          string          `json:"mode"`
	Status        string          `json:"status"`
	Total         int             `json:"total"`
	Processed     int             `json:"processed"`
	CreatedAtMs   int64           `json:"created_at_ms"`
	UpdatedAtMs   int64           `json:"updated_at_ms"`
	Items         []BatchItemView `json:"items"`
}

func (e *Engine) ListDecisions(ctx context.Context, configVersion int64, batchID, requestID string, limit int) ([]ItemResult, error) {
	where := []string{}
	args := []any{}
	if configVersion > 0 {
		where = append(where, "config_version=?")
		args = append(args, configVersion)
	}
	if batchID != "" {
		where = append(where, "batch_id=?")
		args = append(args, batchID)
	}
	if requestID != "" {
		where = append(where, "request_id=?")
		args = append(args, requestID)
	}
	query := "SELECT " + decisionColumns + " FROM decisions"
	if len(where) > 0 {
		query += " WHERE " + joinAnd(where)
	}
	query += " ORDER BY config_version DESC, batch_index ASC"
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	query += " LIMIT ?"
	args = append(args, limit)

	rows, err := e.Store.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ItemResult{}
	for rows.Next() {
		var d ItemResult
		var mode string
		var accepted, reuse, recycled int
		var offset, boundary, firstSeen, remaining, expires sql.NullInt64
		var detailJSON string
		var decidedAt int64
		if err := rows.Scan(
			&d.RequestID, &d.IntentID, &d.BatchID, &d.Index, &d.ConfigVersion, &d.ReferenceMs, &mode,
			&d.Code, &d.Status, &accepted, &offset, &boundary, &firstSeen, &remaining, &expires,
			&reuse, &recycled, &d.ContentHash, &detailJSON, &decidedAt); err != nil {
			return nil, err
		}
		d.Accepted = accepted == 1
		d.IdempotentReuse = reuse == 1
		d.Recycled = recycled == 1
		assignNullInt(&d.OffsetMs, offset)
		assignNullInt(&d.BoundaryMs, boundary)
		assignNullInt(&d.FirstSeenMs, firstSeen)
		assignNullInt(&d.RemainingWindowMs, remaining)
		assignNullInt(&d.ExpiresAtMs, expires)
		if detailJSON != "" && detailJSON != "{}" {
			var m map[string]any
			if json.Unmarshal([]byte(detailJSON), &m) == nil {
				d.Context = m
			}
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (e *Engine) ListOccupancy(ctx context.Context, configVersion int64, clientID string, limit int) ([]OccupancyView, error) {
	query := `SELECT client_id,nonce,request_id,batch_id,intent_id,config_version,first_seen_ms,window_ms,expires_at_ms,recycled_from_ms,created_at_ms FROM nonce_occupancy`
	where := []string{}
	args := []any{}
	if configVersion > 0 {
		where = append(where, "config_version=?")
		args = append(args, configVersion)
	}
	if clientID != "" {
		where = append(where, "client_id=?")
		args = append(args, clientID)
	}
	if len(where) > 0 {
		query += " WHERE " + joinAnd(where)
	}
	query += " ORDER BY first_seen_ms DESC, client_id, nonce"
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	query += " LIMIT ?"
	args = append(args, limit)
	rows, err := e.Store.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []OccupancyView{}
	for rows.Next() {
		var o OccupancyView
		if err := rows.Scan(&o.ClientID, &o.Nonce, &o.RequestID, &o.BatchID, &o.IntentID, &o.ConfigVersion, &o.FirstSeenMs, &o.WindowMs, &o.ExpiresAtMs, &o.RecycledFromMs, &o.CreatedAtMs); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func (e *Engine) GetBatch(ctx context.Context, batchID string) (*BatchView, error) {
	var b BatchView
	err := e.Store.DB.QueryRowContext(ctx, `SELECT batch_id,config_version,reference_ms,mode,status,total,processed,created_at_ms,updated_at_ms FROM batch_jobs WHERE batch_id=?`, batchID).
		Scan(&b.BatchID, &b.ConfigVersion, &b.ReferenceMs, &b.Mode, &b.Status, &b.Total, &b.Processed, &b.CreatedAtMs, &b.UpdatedAtMs)
	if err != nil {
		return nil, err
	}
	rows, err := e.Store.DB.QueryContext(ctx, `SELECT batch_index,request_id,code,idempotent_reuse,result_json FROM batch_items WHERE batch_id=? ORDER BY batch_index`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var item BatchItemView
		var reuse int
		var raw string
		if err := rows.Scan(&item.Index, &item.RequestID, &item.Code, &reuse, &raw); err != nil {
			return nil, err
		}
		item.IdempotentReuse = reuse == 1
		item.Result = json.RawMessage(raw)
		b.Items = append(b.Items, item)
	}
	return &b, rows.Err()
}

func (e *Engine) RunRecovery(ctx context.Context) (*store.RecoveryReport, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.Store.Recover(ctx, e.Clock())
}

type ReclaimReport struct {
	ConfigVersion int64 `json:"config_version"`
	AsOfMs        int64 `json:"as_of_ms"`
	WindowMs      int64 `json:"window_ms"`
	Reclaimed     int64 `json:"reclaimed"`
}

func (e *Engine) ReclaimExpiredNonces(ctx context.Context, configVersion, asOfMs int64) (*ReclaimReport, *Failure) {
	e.mu.Lock()
	defer e.mu.Unlock()
	cfg, ferr := e.loadConfigForWrite(ctx, configVersion)
	if ferr != nil {
		return nil, ferr
	}
	ref, err := e.resolveReference(ctx, asOfMs, false)
	if err != nil {
		return nil, NewFailure(model.CodeInternal, "cannot resolve reclaim as-of time", map[string]any{"error": err.Error()})
	}
	res, err := e.Store.DB.ExecContext(ctx,
		`DELETE FROM nonce_occupancy WHERE ? >= first_seen_ms + ?`,
		ref.ReferenceMs, cfg.NonceWindowMs)
	if err != nil {
		return nil, NewFailure(model.CodeInternal, "reclaim transaction failed", map[string]any{"error": err.Error()})
	}
	count, _ := res.RowsAffected()
	return &ReclaimReport{ConfigVersion: cfg.Version, AsOfMs: ref.ReferenceMs, WindowMs: cfg.NonceWindowMs, Reclaimed: count}, nil
}

func joinAnd(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += " AND "
		}
		out += p
	}
	return out
}

var _ = model.CodeOK
