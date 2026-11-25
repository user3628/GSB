package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"local/windowengine/internal/domain"
)

func (s *Store) LoadSnapshot(ctx context.Context, snapshot *domain.Snapshot, windows []domain.Window) (*LoadResult, error) {
	result := &LoadResult{
		Snapshot: SnapshotRecord{
			SnapshotID: snapshot.SnapshotID, ContentHash: snapshot.ContentHash, LoadedAtMS: nowMS(),
		},
		WindowCount: len(windows),
		EventCount:  len(snapshot.Events),
	}
	err := s.withWriteTx(ctx, func(tx *sql.Tx) error {
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshots WHERE snapshot_id = ?`, snapshot.SnapshotID).Scan(&exists); err != nil {
			return err
		}
		if exists > 0 {
			return domain.NewError(domain.CodeInvalidTransition, "snapshot_id already exists", map[string]any{
				"snapshot_id": snapshot.SnapshotID,
			})
		}
		var maxVersion sql.NullInt64
		if err := tx.QueryRowContext(ctx, `SELECT MAX(load_version) FROM snapshots`).Scan(&maxVersion); err != nil {
			return err
		}
		result.Snapshot.LoadVersion = maxVersion.Int64 + 1
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO snapshots(snapshot_id, load_version, content_hash, raw_json, loaded_at_ms)
			VALUES(?, ?, ?, ?, ?)`,
			snapshot.SnapshotID, result.Snapshot.LoadVersion, snapshot.ContentHash, string(snapshot.RawJSON), result.Snapshot.LoadedAtMS,
		); err != nil {
			return err
		}
		for _, tenant := range snapshot.Tenants {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO tenants(snapshot_id, tenant_id, tenant_name, priority) VALUES(?, ?, ?, ?)`,
				snapshot.SnapshotID, tenant.ID, tenant.Name, tenant.Priority,
			); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO tenant_intermediate(
					snapshot_id, tenant_id, version, total_count, total_sum_scaled, min_scaled, max_scaled,
					committed_window_count, group_count, group_index_json, updated_at_ms
				) VALUES(?, ?, 0, 0, 0, NULL, NULL, 0, 0, '[]', ?)`,
				snapshot.SnapshotID, tenant.ID, result.Snapshot.LoadedAtMS,
			); err != nil {
				return err
			}
		}
		for _, spec := range snapshot.WindowSpecs {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO window_specs(
					snapshot_id, spec_id, window_type, width_ms, slide_ms, session_gap_ms, boundary
				) VALUES(?, ?, ?, ?, ?, ?, ?)`,
				snapshot.SnapshotID, spec.ID, spec.Type, spec.WidthMS, spec.SlideMS, spec.SessionGapMS, spec.Boundary,
			); err != nil {
				return err
			}
		}
		for _, event := range snapshot.Events {
			var measure any
			if event.Measure.Present {
				measure = event.Measure.Scaled
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO events(
					snapshot_id, event_id, tenant_id, group_key, timestamp_ms, seq, measure_present, measure_scaled
				) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
				snapshot.SnapshotID, event.EventID, event.TenantID, event.GroupKey,
				event.TimestampMS, event.Seq, boolInt(event.Measure.Present), measure,
			); err != nil {
				return err
			}
		}
		for _, window := range windows {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO windows(
					snapshot_id, window_id, tenant_id, spec_id, group_key, start_ms, end_ms,
					boundary, status, version, event_count, created_at_ms, updated_at_ms
				) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, ?, ?, ?)`,
				snapshot.SnapshotID, window.WindowID, window.TenantID, window.SpecID, window.GroupKey,
				window.StartMS, window.EndMS, window.Boundary, window.EventCount,
				result.Snapshot.LoadedAtMS, result.Snapshot.LoadedAtMS,
			); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		if _, ok := domain.BusinessError(err); ok {
			return nil, err
		}
		return nil, fmt.Errorf("load snapshot transaction: %w", err)
	}
	return result, nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (s *Store) LatestSnapshotID(ctx context.Context) (string, *domain.Error) {
	var snapshotID string
	err := s.db.QueryRowContext(ctx, `SELECT snapshot_id FROM snapshots ORDER BY load_version DESC LIMIT 1`).Scan(&snapshotID)
	if err == sql.ErrNoRows {
		return "", domain.NewError(domain.CodeNotFound, "no snapshot has been loaded", nil)
	}
	if err != nil {
		return "", domain.NewError(domain.CodeInternal, "query latest snapshot failed", map[string]any{"reason": err.Error()})
	}
	return snapshotID, nil
}

func (s *Store) SnapshotRecord(ctx context.Context, snapshotID string) (SnapshotRecord, *domain.Error) {
	var record SnapshotRecord
	err := s.db.QueryRowContext(ctx, `
		SELECT snapshot_id, load_version, content_hash, loaded_at_ms
		FROM snapshots WHERE snapshot_id = ?`, snapshotID,
	).Scan(&record.SnapshotID, &record.LoadVersion, &record.ContentHash, &record.LoadedAtMS)
	if err == sql.ErrNoRows {
		return record, domain.NewError(domain.CodeNotFound, "snapshot not found", map[string]any{"snapshot_id": snapshotID})
	}
	if err != nil {
		return record, domain.NewError(domain.CodeInternal, "query snapshot failed", map[string]any{"reason": err.Error()})
	}
	return record, nil
}

func (s *Store) LoadSnapshotData(ctx context.Context, snapshotID string) (*domain.Snapshot, *domain.Error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT raw_json FROM snapshots WHERE snapshot_id = ?`, snapshotID).Scan(&raw)
	if err == sql.ErrNoRows {
		return nil, domain.NewError(domain.CodeNotFound, "snapshot not found", map[string]any{"snapshot_id": snapshotID})
	}
	if err != nil {
		return nil, domain.NewError(domain.CodeInternal, "read snapshot JSON failed", map[string]any{"reason": err.Error()})
	}
	snapshot, parseErr := domain.ParseSnapshot([]byte(raw))
	if parseErr != nil {
		return nil, parseErr
	}
	return snapshot, nil
}

func (s *Store) ListSnapshots(ctx context.Context) ([]SnapshotRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT snapshot_id, load_version, content_hash, loaded_at_ms
		FROM snapshots ORDER BY load_version DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := make([]SnapshotRecord, 0)
	for rows.Next() {
		var record SnapshotRecord
		if err := rows.Scan(&record.SnapshotID, &record.LoadVersion, &record.ContentHash, &record.LoadedAtMS); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (s *Store) ListIntents(ctx context.Context, status string) ([]Intent, error) {
	query := `SELECT id, intent_id, intent_type, status, round_id, task_id, tenant_id, window_id,
		payload_json, recovery_note, created_at_ms, updated_at_ms FROM intents`
	args := []any{}
	if status != "" {
		query += ` WHERE status = ?`
		args = append(args, status)
	}
	query += ` ORDER BY id`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	intents := make([]Intent, 0)
	for rows.Next() {
		var intent Intent
		if err := rows.Scan(
			&intent.ID, &intent.IntentID, &intent.IntentType, &intent.Status, &intent.RoundID,
			&intent.TaskID, &intent.TenantID, &intent.WindowID, &intent.PayloadJSON,
			&intent.RecoveryNote, &intent.CreatedAtMS, &intent.UpdatedAtMS,
		); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(intent.PayloadJSON), &intent.Payload)
		intents = append(intents, intent)
	}
	return intents, rows.Err()
}
