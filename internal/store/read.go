package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"meshmigrate/internal/model"
)

var ErrNotFound = errors.New("not found")

func (s *Store) GetRequest(ctx context.Context, requestID string) (*RequestRecord, error) {
	row := s.db.QueryRowContext(ctx, `SELECT request_id, request_hash, status, http_status, response_json, report_id, created_at_ms, updated_at_ms
		FROM request_registry WHERE request_id = ?`, requestID)
	var r RequestRecord
	if err := row.Scan(&r.RequestID, &r.RequestHash, &r.Status, &r.HTTPStatus, &r.ResponseJSON, &r.ReportID, &r.CreatedAtMs, &r.UpdatedAtMs); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &r, nil
}

func (s *Store) ActiveVersion(ctx context.Context) (uint64, error) {
	var v sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT snapshot_version FROM active_snapshot WHERE id = 1`).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) || !v.Valid || v.Int64 == 0 {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return uint64(v.Int64), nil
}

func ActiveVersionTx(ctx context.Context, tx *sql.Tx) (uint64, error) {
	var v sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT snapshot_version FROM active_snapshot WHERE id = 1`).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) || !v.Valid || v.Int64 == 0 {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return uint64(v.Int64), nil
}

func (s *Store) GetSnapshot(ctx context.Context, version uint64) (*SnapshotData, error) {
	tx, err := s.BeginRead(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return LoadSnapshotTx(ctx, tx, version)
}

func (s *Store) GetActiveSnapshot(ctx context.Context) (*SnapshotData, uint64, error) {
	tx, err := s.BeginRead(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback()
	version, err := ActiveVersionTx(ctx, tx)
	if err != nil || version == 0 {
		return nil, version, err
	}
	data, err := LoadSnapshotTx(ctx, tx, version)
	if err != nil {
		return nil, version, err
	}
	return data, version, nil
}

func LoadSnapshotTx(ctx context.Context, tx *sql.Tx, version uint64) (*SnapshotData, error) {
	row := tx.QueryRowContext(ctx, `SELECT snapshot_version, snapshot_id, generated_at_ms, source_format, format_version,
		source_digest, content_digest, policy_count, cumulative_rounding_error_us, defaults_json, imported_at_ms
		FROM snapshots WHERE snapshot_version = ?`, int64(version))
	data := &SnapshotData{}
	var defaultsJSON string
	var v int64
	if err := row.Scan(&v, &data.Meta.SnapshotID, &data.Meta.GeneratedAtMs, &data.Meta.SourceFormat, &data.Meta.FormatVersion,
		&data.Meta.SourceDigest, &data.Meta.ContentDigest, &data.Meta.PolicyCount, &data.Meta.CumulativeRoundingError,
		&defaultsJSON, &data.Meta.ImportedAtMs); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	data.Meta.SnapshotVersion = uint64(v)
	_ = json.Unmarshal([]byte(defaultsJSON), &data.Meta.DefaultDetails)
	rows, err := tx.QueryContext(ctx, `SELECT policy_json FROM policies WHERE snapshot_version = ? ORDER BY policy_id`, int64(version))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var p model.Policy
		if err := json.Unmarshal([]byte(raw), &p); err != nil {
			return nil, fmt.Errorf("decode stored policy: %w", err)
		}
		data.Policies = append(data.Policies, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return data, nil
}

func (s *Store) GetDerived(ctx context.Context, version uint64) (*DerivedData, error) {
	tx, err := s.BeginRead(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if version == 0 {
		version, err = ActiveVersionTx(ctx, tx)
		if err != nil {
			return nil, err
		}
	}
	if version == 0 {
		return &DerivedData{}, nil
	}
	// Confirm the snapshot and derivation marker are in this read transaction.
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshots WHERE snapshot_version = ?`, int64(version)).Scan(&count); err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, ErrNotFound
	}
	d := &DerivedData{SnapshotVersion: version}
	var affectedJSON string
	if err := tx.QueryRowContext(ctx, `SELECT affected_services_json, rebuilt_at_ms FROM snapshot_derivations WHERE snapshot_version = ?`, int64(version)).Scan(&affectedJSON, &d.RebuiltAtMs); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	_ = json.Unmarshal([]byte(affectedJSON), &d.Affected)
	rows, err := tx.QueryContext(ctx, `SELECT virtual_service_id, route_key, policy_id, method, path_prefix, headers_json FROM route_index WHERE snapshot_version = ? ORDER BY virtual_service_id, route_key, policy_id`, int64(version))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var r RouteRow
		var headers string
		if err := rows.Scan(&r.VirtualServiceID, &r.RouteKey, &r.PolicyID, &r.Method, &r.PathPrefix, &headers); err != nil {
			rows.Close()
			return nil, err
		}
		_ = json.Unmarshal([]byte(headers), &r.Headers)
		d.Routes = append(d.Routes, r)
	}
	rows.Close()
	rows, err = tx.QueryContext(ctx, `SELECT virtual_service_id, window_start_ms, window_end_ms, window_ms, limit_bps, policy_count, policy_ids_json FROM rate_window_stats WHERE snapshot_version = ? ORDER BY virtual_service_id, window_start_ms, window_ms`, int64(version))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var r RateWindowRow
		var limit string
		var ids string
		if err := rows.Scan(&r.VirtualServiceID, &r.WindowStartMs, &r.WindowEndMs, &r.WindowMs, &limit, &r.PolicyCount, &ids); err != nil {
			rows.Close()
			return nil, err
		}
		r.LimitBps, _ = strconv.ParseUint(limit, 10, 64)
		_ = json.Unmarshal([]byte(ids), &r.PolicyIDs)
		d.RateWindows = append(d.RateWindows, r)
	}
	rows.Close()
	rows, err = tx.QueryContext(ctx, `SELECT virtual_service_id, state, window_ms, policy_ids_json FROM circuit_state WHERE snapshot_version = ? ORDER BY virtual_service_id`, int64(version))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var r CircuitRow
		var ids string
		if err := rows.Scan(&r.VirtualServiceID, &r.State, &r.WindowMs, &ids); err != nil {
			rows.Close()
			return nil, err
		}
		_ = json.Unmarshal([]byte(ids), &r.PolicyIDs)
		d.Circuits = append(d.Circuits, r)
	}
	rows.Close()
	rows, err = tx.QueryContext(ctx, `SELECT virtual_service_id, policy_count, route_count, rate_window_count, total_rate_bps, circuit_state, policy_ids_json FROM service_summaries WHERE snapshot_version = ? ORDER BY virtual_service_id`, int64(version))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var r SummaryRow
		var total string
		var ids string
		if err := rows.Scan(&r.VirtualServiceID, &r.PolicyCount, &r.RouteCount, &r.RateWindowCount, &total, &r.CircuitState, &ids); err != nil {
			rows.Close()
			return nil, err
		}
		r.TotalRateBps, _ = strconv.ParseUint(total, 10, 64)
		_ = json.Unmarshal([]byte(ids), &r.PolicyIDs)
		d.Summaries = append(d.Summaries, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	return d, nil
}

func (s *Store) ListVersions(ctx context.Context) ([]SnapshotMeta, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT snapshot_version, snapshot_id, generated_at_ms, source_format, format_version,
		source_digest, content_digest, policy_count, cumulative_rounding_error_us, defaults_json, imported_at_ms
		FROM snapshots ORDER BY snapshot_version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]SnapshotMeta, 0)
	for rows.Next() {
		var m SnapshotMeta
		var v int64
		var defaults string
		if err := rows.Scan(&v, &m.SnapshotID, &m.GeneratedAtMs, &m.SourceFormat, &m.FormatVersion, &m.SourceDigest,
			&m.ContentDigest, &m.PolicyCount, &m.CumulativeRoundingError, &defaults, &m.ImportedAtMs); err != nil {
			return nil, err
		}
		m.SnapshotVersion = uint64(v)
		_ = json.Unmarshal([]byte(defaults), &m.DefaultDetails)
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) GetImportReport(ctx context.Context, reportID string) ([]byte, []ReportItem, error) {
	var body string
	err := s.db.QueryRowContext(ctx, `SELECT report_json FROM import_reports WHERE report_id = ?`, reportID).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, ErrNotFound
	}
	if err != nil {
		return nil, nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT policy_id, outcome, basis_json, detail_json FROM import_report_items WHERE report_id = ? ORDER BY seq`, reportID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var items []ReportItem
	for rows.Next() {
		var it ReportItem
		var basis, detail string
		if err := rows.Scan(&it.PolicyID, &it.Outcome, &basis, &detail); err != nil {
			return nil, nil, err
		}
		_ = json.Unmarshal([]byte(basis), &it.Basis)
		_ = json.Unmarshal([]byte(detail), &it.Detail)
		items = append(items, it)
	}
	return []byte(body), items, rows.Err()
}

func (s *Store) LatestRecoveryReport(ctx context.Context) (map[string]any, error) {
	var body string
	err := s.db.QueryRowContext(ctx, `SELECT report_json FROM recovery_reports ORDER BY created_at_ms DESC, report_id DESC LIMIT 1`).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) ListPendingIntents(ctx context.Context) ([]IntentRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT intent_id, request_id, kind, snapshot_version, status, created_at_ms, completed_at_ms, facts_json FROM intents WHERE status = 'pending' ORDER BY created_at_ms, intent_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []IntentRecord
	for rows.Next() {
		var r IntentRecord
		var version int64
		var completed sql.NullInt64
		var facts string
		if err := rows.Scan(&r.IntentID, &r.RequestID, &r.Kind, &version, &r.Status, &r.CreatedAtMs, &completed, &facts); err != nil {
			return nil, err
		}
		r.SnapshotVersion = uint64(version)
		if completed.Valid {
			v := completed.Int64
			r.CompletedAtMs = &v
		}
		_ = json.Unmarshal([]byte(facts), &r.Facts)
		out = append(out, r)
	}
	return out, rows.Err()
}
