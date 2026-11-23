package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"cdn-edge-consistency/internal/cache"
	"cdn-edge-consistency/internal/model"
)

type PurgeDelivery struct {
	PurgeID       string `json:"purge_id"`
	NodeID        string `json:"node_id"`
	Status        string `json:"status"`
	EntryCount    int64  `json:"entry_count"`
	ErrorCode     string `json:"error_code,omitempty"`
	AttemptedAtMs int64  `json:"attempted_at_ms"`
	CompletedAtMs int64  `json:"completed_at_ms"`
}

type PurgeRecord struct {
	PurgeID         string          `json:"purge_id"`
	Kind            string          `json:"kind"`
	Target          string          `json:"target"`
	RequestID       string          `json:"request_id"`
	SnapshotVersion int64           `json:"snapshot_version"`
	Status          string          `json:"status"`
	DeadlineMs      int64           `json:"deadline_ms"`
	PurgeWindowMs   int64           `json:"purge_window_ms"`
	IncompleteNodes []string        `json:"incomplete_nodes"`
	ErrorCode       string          `json:"error_code,omitempty"`
	AffectedEntries int64           `json:"affected_entries"`
	Details         map[string]any  `json:"details,omitempty"`
	CreatedAtMs     int64           `json:"created_at_ms"`
	CompletedAtMs   int64           `json:"completed_at_ms"`
	Deliveries      []PurgeDelivery `json:"deliveries,omitempty"`
}

func CreatePurgeSessionTx(ctx context.Context, tx *sql.Tx, p PurgeRecord, nodes []string, nowMs int64) error {
	incomplete, _ := json.Marshal([]string{})
	details, _ := json.Marshal(p.Details)
	_, err := tx.ExecContext(ctx, `INSERT INTO purge_records(purge_id,kind,target,request_id,snapshot_version,status,deadline_ms,purge_window_ms,incomplete_nodes_json,error_code,affected_entries,details_json,created_at_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		p.PurgeID, p.Kind, p.Target, p.RequestID, p.SnapshotVersion, "preparing", p.DeadlineMs, p.PurgeWindowMs, string(incomplete), "", 0, string(details), nowMs)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if _, err := tx.ExecContext(ctx, `INSERT INTO purge_deliveries(purge_id,node_id,status,attempted_at_ms) VALUES(?,?,?,?)`, p.PurgeID, node, "pending", nowMs); err != nil {
			return err
		}
	}
	return nil
}

func SetDeliveryStatusTx(ctx context.Context, tx *sql.Tx, purgeID, node, status, code string, entryCount, nowMs int64) error {
	_, err := tx.ExecContext(ctx, `UPDATE purge_deliveries SET status=?,error_code=?,entry_count=?,completed_at_ms=? WHERE purge_id=? AND node_id=?`,
		status, code, entryCount, nowMs, purgeID, node)
	return err
}

func AbortPurgeTx(ctx context.Context, tx *sql.Tx, purgeID, code string, incomplete []string, nowMs int64) error {
	raw, _ := json.Marshal(incomplete)
	_, err := tx.ExecContext(ctx, `UPDATE purge_records SET status='aborted',error_code=?,incomplete_nodes_json=?,completed_at_ms=? WHERE purge_id=? AND status='preparing'`,
		code, string(raw), nowMs, purgeID)
	return err
}

func PurgeEntriesTx(ctx context.Context, tx *sql.Tx, purgeID, kind, target string, nodes []string, requestID string, nowMs int64) (int64, error) {
	if len(nodes) == 0 {
		return 0, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(nodes)), ",")
	args := make([]any, 0, len(nodes)+2)
	for _, n := range nodes {
		args = append(args, n)
	}
	var query string
	switch kind {
	case "path":
		query = `SELECT ` + entryColumns + ` FROM cache_entries WHERE node_id IN (` + placeholders + `) AND norm_path=?`
		args = append(args, target)
	case "tag":
		query = `SELECT ` + entryColumns + ` FROM cache_entries WHERE node_id IN (` + placeholders + `) AND id IN (SELECT entry_id FROM entry_tags WHERE tag=? AND active=1)`
		args = append(args, target)
	default:
		return 0, model.NewBiz(model.CodeBadRequest, "purge kind must be path or tag")
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	entries := make([]Entry, 0)
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			rows.Close()
			return 0, err
		}
		entries = append(entries, e)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	var affected int64
	keys := map[string]bool{}
	for _, e := range entries {
		keys[e.CacheKey] = true
		if e.State == cache.StatePurged {
			continue
		}
		if err := cache.ValidateTransition(e.State, cache.StatePurged); err != nil {
			return 0, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE cache_entries SET state='PURGED',updated_at_ms=?,last_request_id=? WHERE id=?`, nowMs, requestID, e.ID); err != nil {
			return 0, err
		}
		if err := InsertTransitionTx(ctx, tx, e, e.State, cache.StatePurged, requestID, nowMs); err != nil {
			return 0, err
		}
		if err := InvalidateIndexesTx(ctx, tx, e.ID, purgeID); err != nil {
			return 0, err
		}
		affected++
	}
	for key := range keys {
		if _, err := tx.ExecContext(ctx, `INSERT INTO key_epochs(cache_key,epoch) VALUES(?,1)
			ON CONFLICT(cache_key) DO UPDATE SET epoch=epoch+1`, key); err != nil {
			return 0, err
		}
	}
	return affected, nil
}

func KeyEpochTx(ctx context.Context, tx *sql.Tx, key string) (int64, error) {
	var epoch int64
	err := tx.QueryRowContext(ctx, `SELECT epoch FROM key_epochs WHERE cache_key=?`, key).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return epoch, err
}

func KeyEpoch(ctx context.Context, db *sql.DB, key string) (int64, error) {
	var epoch int64
	err := db.QueryRowContext(ctx, `SELECT epoch FROM key_epochs WHERE cache_key=?`, key).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return epoch, err
}

func CompletePurgeTx(ctx context.Context, tx *sql.Tx, purgeID string, affected int64, nowMs int64) error {
	res, err := tx.ExecContext(ctx, `UPDATE purge_records SET status='committed',affected_entries=?,completed_at_ms=? WHERE purge_id=? AND status='preparing'`, affected, nowMs, purgeID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("purge %s is not preparing", purgeID)
	}
	_, err = tx.ExecContext(ctx, `UPDATE purge_deliveries SET status='complete',completed_at_ms=? WHERE purge_id=? AND status IN ('pending','inflight')`, nowMs, purgeID)
	return err
}

func scanPurge(scan interface{ Scan(dest ...any) error }) (PurgeRecord, error) {
	var p PurgeRecord
	var incompleteJSON, detailsJSON string
	err := scan.Scan(&p.PurgeID, &p.Kind, &p.Target, &p.RequestID, &p.SnapshotVersion, &p.Status, &p.DeadlineMs, &p.PurgeWindowMs, &incompleteJSON, &p.ErrorCode, &p.AffectedEntries, &detailsJSON, &p.CreatedAtMs, &p.CompletedAtMs)
	if err != nil {
		return PurgeRecord{}, err
	}
	_ = json.Unmarshal([]byte(incompleteJSON), &p.IncompleteNodes)
	_ = json.Unmarshal([]byte(detailsJSON), &p.Details)
	return p, nil
}

const purgeColumns = `purge_id,kind,target,request_id,snapshot_version,status,deadline_ms,purge_window_ms,incomplete_nodes_json,error_code,affected_entries,details_json,created_at_ms,completed_at_ms`

func (s *Store) GetPurge(ctx context.Context, id string) (*PurgeRecord, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+purgeColumns+` FROM purge_records WHERE purge_id=?`, id)
	p, err := scanPurge(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	drows, err := s.db.QueryContext(ctx, `SELECT purge_id,node_id,status,entry_count,error_code,attempted_at_ms,completed_at_ms FROM purge_deliveries WHERE purge_id=? ORDER BY node_id`, id)
	if err != nil {
		return nil, err
	}
	defer drows.Close()
	for drows.Next() {
		var d PurgeDelivery
		if err := drows.Scan(&d.PurgeID, &d.NodeID, &d.Status, &d.EntryCount, &d.ErrorCode, &d.AttemptedAtMs, &d.CompletedAtMs); err != nil {
			return nil, err
		}
		p.Deliveries = append(p.Deliveries, d)
	}
	return &p, drows.Err()
}

func (s *Store) ListPurges(ctx context.Context, limit int) ([]PurgeRecord, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+purgeColumns+` FROM purge_records ORDER BY created_at_ms DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PurgeRecord
	for rows.Next() {
		p, err := scanPurge(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
