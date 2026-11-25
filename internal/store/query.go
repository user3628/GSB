package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"local/windowengine/internal/domain"
)

type TenantSummary struct {
	TenantID    string `json:"tenant_id"`
	Name        string `json:"name"`
	Priority    int    `json:"priority"`
	Version     int64  `json:"version"`
	WindowCount int64  `json:"committed_window_count"`
}

func (s *Store) QueryAggregates(
	ctx context.Context, snapshotID, tenantID, windowID, specID, groupKey string, expectedVersion *int64,
) (AggregateQueryResult, *domain.Error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return AggregateQueryResult{}, domain.NewError(domain.CodeInternal, "begin read transaction failed", map[string]any{"reason": err.Error()})
	}
	defer rollback(tx)

	state, err := tenantStateTx(ctx, tx, snapshotID, tenantID)
	if err != nil {
		if business, ok := domain.BusinessError(err); ok {
			return AggregateQueryResult{}, business
		}
		return AggregateQueryResult{}, domain.NewError(domain.CodeInternal, "read tenant state failed", map[string]any{"reason": err.Error()})
	}
	if expectedVersion != nil && *expectedVersion != state.Version {
		return AggregateQueryResult{}, domain.NewError(domain.CodeStaleVersion, "requested tenant version is stale", map[string]any{
			"snapshot_id": snapshotID, "tenant_id": tenantID,
			"expected_version": *expectedVersion, "current_version": state.Version,
		})
	}

	query := `
		SELECT w.window_id, w.tenant_id, w.spec_id, w.group_key, w.start_ms, w.end_ms,
			w.boundary, s.window_type, w.status, w.version, w.event_count,
			a.count_value, a.sum_scaled, a.min_scaled, a.max_scaled, a.sample_size,
			a.rounding_mode, a.version
		FROM windows w
		JOIN window_specs s ON s.snapshot_id = w.snapshot_id AND s.spec_id = w.spec_id
		LEFT JOIN window_aggregates a
			ON a.snapshot_id = w.snapshot_id AND a.window_id = w.window_id
		WHERE w.snapshot_id = ? AND w.tenant_id = ?`
	args := []any{snapshotID, tenantID}
	if windowID != "" {
		query += ` AND w.window_id = ?`
		args = append(args, windowID)
	}
	if specID != "" {
		query += ` AND w.spec_id = ?`
		args = append(args, specID)
	}
	if groupKey != "" {
		query += ` AND w.group_key = ?`
		args = append(args, groupKey)
	}
	query += ` ORDER BY w.start_ms, w.spec_id, w.group_key, w.window_id`
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return AggregateQueryResult{}, domain.NewError(domain.CodeInternal, "query window aggregates failed", map[string]any{"reason": err.Error()})
	}
	windows := make([]WindowView, 0)
	for rows.Next() {
		var view WindowView
		var status, specIDValue string
		var windowVersion, eventCount int64
		var countValue, sampleSize, aggregateVersion sql.NullInt64
		var sumValue, minValue, maxValue sql.NullInt64
		var rounding sql.NullString
		if err := rows.Scan(
			&view.WindowID, &view.TenantID, &view.SpecID, &view.GroupKey, &view.StartMS,
			&view.EndMS, &view.Boundary, &specIDValue, &status, &windowVersion, &eventCount,
			&countValue, &sumValue, &minValue, &maxValue, &sampleSize, &rounding,
			&aggregateVersion,
		); err != nil {
			rows.Close()
			return AggregateQueryResult{}, domain.NewError(domain.CodeInternal, "scan window aggregate failed", map[string]any{"reason": err.Error()})
		}
		view.SnapshotID = snapshotID
		view.WindowType = domain.WindowType(specIDValue)
		view.Status = status
		view.Version = windowVersion
		view.EventCount = int(eventCount)
		view.ZeroLength = view.StartMS >= view.EndMS
		if countValue.Valid {
			aggregate := domain.Aggregate{
				SnapshotID: snapshotID, WindowID: view.WindowID, TenantID: tenantID,
				SpecID: view.SpecID, GroupKey: view.GroupKey, WindowType: view.WindowType,
				StartMS: view.StartMS, EndMS: view.EndMS, Boundary: view.Boundary,
				Count: countValue.Int64, SumScaled: sumValue.Int64,
				MinScaled: nullInt64Pointer(minValue), MaxScaled: nullInt64Pointer(maxValue),
				SampleSize: sampleSize.Int64, Rounding: rounding.String, Version: aggregateVersion.Int64,
			}
			aggregateView := aggregate.View(status)
			view.Aggregate = &aggregateView
		}
		windows = append(windows, view)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return AggregateQueryResult{}, domain.NewError(domain.CodeInternal, "iterate window aggregates failed", map[string]any{"reason": err.Error()})
	}
	rows.Close()
	if windowID != "" && len(windows) == 0 {
		return AggregateQueryResult{}, domain.NewError(domain.CodeNotFound, "window not found for tenant", map[string]any{
			"snapshot_id": snapshotID, "tenant_id": tenantID, "window_id": windowID,
		})
	}
	if err := tx.Commit(); err != nil {
		return AggregateQueryResult{}, domain.NewError(domain.CodeInternal, "commit read transaction failed", map[string]any{"reason": err.Error()})
	}
	return AggregateQueryResult{
		SnapshotID: snapshotID, TenantID: tenantID, Version: state.Version,
		Windows: windows, State: state,
	}, nil
}

func (s *Store) TenantStateVersioned(ctx context.Context, snapshotID, tenantID string, expectedVersion *int64) (TenantState, *domain.Error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return TenantState{}, domain.NewError(domain.CodeInternal, "begin read transaction failed", map[string]any{"reason": err.Error()})
	}
	defer rollback(tx)
	state, err := tenantStateTx(ctx, tx, snapshotID, tenantID)
	if err != nil {
		if business, ok := domain.BusinessError(err); ok {
			return TenantState{}, business
		}
		return TenantState{}, domain.NewError(domain.CodeInternal, "read tenant state failed", map[string]any{"reason": err.Error()})
	}
	if expectedVersion != nil && *expectedVersion != state.Version {
		return TenantState{}, domain.NewError(domain.CodeStaleVersion, "requested tenant version is stale", map[string]any{
			"snapshot_id": snapshotID, "tenant_id": tenantID,
			"expected_version": *expectedVersion, "current_version": state.Version,
		})
	}
	if err := tx.Commit(); err != nil {
		return TenantState{}, domain.NewError(domain.CodeInternal, "commit read transaction failed", map[string]any{"reason": err.Error()})
	}
	return state, nil
}

func (s *Store) Tenants(ctx context.Context, snapshotID string) ([]TenantSummary, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT t.tenant_id, t.tenant_name, t.priority, COALESCE(i.version, 0),
			COALESCE(i.committed_window_count, 0)
		FROM tenants t
		LEFT JOIN tenant_intermediate i
			ON i.snapshot_id = t.snapshot_id AND i.tenant_id = t.tenant_id
		WHERE t.snapshot_id = ?
		ORDER BY t.priority, t.tenant_id`, snapshotID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tenants := make([]TenantSummary, 0)
	for rows.Next() {
		var tenant TenantSummary
		if err := rows.Scan(
			&tenant.TenantID, &tenant.Name, &tenant.Priority, &tenant.Version, &tenant.WindowCount,
		); err != nil {
			return nil, err
		}
		tenants = append(tenants, tenant)
	}
	return tenants, rows.Err()
}

func (s *Store) IntentCounts(ctx context.Context) (map[string]int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT status, COUNT(*) FROM intents GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := make(map[string]int)
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		counts[status] = count
	}
	return counts, rows.Err()
}

func parseVersion(raw string) (*int64, *domain.Error) {
	if raw == "" {
		return nil, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return nil, domain.NewError(domain.CodeBadRequest, "version must be a non-negative int64", map[string]any{"version": raw})
	}
	return &value, nil
}

func ParseExpectedVersion(raw string) (*int64, *domain.Error) {
	return parseVersion(raw)
}

func (s *Store) SnapshotExists(ctx context.Context, snapshotID string) (bool, error) {
	var exists int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshots WHERE snapshot_id = ?`, snapshotID).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists > 0, nil
}

func (s *Store) CountWindows(ctx context.Context, snapshotID string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM windows WHERE snapshot_id = ?`, snapshotID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count windows: %w", err)
	}
	return count, nil
}

func (s *Store) WindowsAndSpecs(ctx context.Context, snapshotID string) ([]domain.Window, map[string]domain.WindowSpec, error) {
	snapshot, parseErr := s.LoadSnapshotData(ctx, snapshotID)
	if parseErr != nil {
		return nil, nil, parseErr
	}
	specs := make(map[string]domain.WindowSpec)
	for _, spec := range snapshot.WindowSpecs {
		specs[spec.ID] = spec
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT window_id, tenant_id, spec_id, group_key, start_ms, end_ms, boundary,
			event_count, status
		FROM windows WHERE snapshot_id = ? ORDER BY tenant_id, spec_id, group_key, start_ms`, snapshotID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	windows := make([]domain.Window, 0)
	for rows.Next() {
		var window domain.Window
		var status string
		if err := rows.Scan(
			&window.WindowID, &window.TenantID, &window.SpecID, &window.GroupKey,
			&window.StartMS, &window.EndMS, &window.Boundary, &window.EventCount, &status,
		); err != nil {
			return nil, nil, err
		}
		window.SnapshotID = snapshotID
		window.WindowType = specs[window.SpecID].Type
		window.ZeroLength = window.StartMS >= window.EndMS
		windows = append(windows, window)
	}
	return windows, specs, rows.Err()
}

func (s *Store) WindowStatuses(ctx context.Context, snapshotID string) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT window_id, status FROM windows WHERE snapshot_id = ?`, snapshotID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	statuses := make(map[string]string)
	for rows.Next() {
		var id, status string
		if err := rows.Scan(&id, &status); err != nil {
			return nil, err
		}
		statuses[id] = status
	}
	return statuses, rows.Err()
}

func IsNoRows(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}
