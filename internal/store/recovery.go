package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"example.com/gsb-router/internal/model"
)

func (s *Store) recover(ctx context.Context) error {
	report := RecoveryReport{
		GeneratedAt:   nowString(s.now()),
		Actions:       []RecoveryAction{},
		CacheAction:   "none",
		FactsOnly:     true,
		FactsOnlyNote: "report contains only state read from committed SQLite rows and intent outcomes written during this recovery scan",
	}

	rows, err := s.db.QueryContext(ctx, `SELECT id, commit_id, kind, base_version, target_version, status FROM intents WHERE status='pending' ORDER BY created_at, id`)
	if err != nil {
		return fmt.Errorf("scan pending intents: %w", err)
	}
	type pendingIntent struct {
		id, commitID, kind, status string
		base, target               int64
	}
	pending := []pendingIntent{}
	for rows.Next() {
		var item pendingIntent
		if err := rows.Scan(&item.id, &item.commitID, &item.kind, &item.base, &item.target, &item.status); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	report.PendingFound = len(pending)
	for _, item := range pending {
		var exists int
		err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshots WHERE version=?`, item.target).Scan(&exists)
		if err != nil {
			return fmt.Errorf("check target snapshot %d: %w", item.target, err)
		}
		decision := "aborted_incomplete"
		status, phase, reason := "aborted", "recovery_aborted", "target version is not present in committed snapshots"
		if exists > 0 {
			decision = "reconciled_committed"
			status, phase, reason = "committed", "recovery_reconciled", "target version is present in committed snapshots"
		}
		if _, err := s.db.ExecContext(ctx, `UPDATE intents SET status=?, phase=?, finished_at=? WHERE id=? AND status='pending'`, status, phase, nowString(s.now()), item.id); err != nil {
			return fmt.Errorf("finish pending intent %s: %w", item.id, err)
		}
		report.Actions = append(report.Actions, RecoveryAction{
			IntentID:      item.id,
			CommitID:      item.commitID,
			Kind:          item.kind,
			BaseVersion:   item.base,
			TargetVersion: item.target,
			Decision:      decision,
			Reason:        reason,
		})
	}

	var currentVersion int64
	var raw string
	err = s.db.QueryRowContext(ctx, `SELECT version, snapshot_json FROM snapshots ORDER BY version DESC LIMIT 1`).Scan(&currentVersion, &raw)
	if err == sql.ErrNoRows {
		report.CurrentVersion = 0
		if report.PendingFound > 0 {
			if err := s.saveRecoveryReport(ctx, report); err != nil {
				return err
			}
		}
		return s.loadLatestRecovery(ctx)
	}
	if err != nil {
		return fmt.Errorf("read recovery snapshot: %w", err)
	}
	report.CurrentVersion = currentVersion
	var snapshot model.Snapshot
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		return fmt.Errorf("decode recovery snapshot: %w", err)
	}
	if _, err := model.ValidateSnapshot(snapshot, model.ValidationOptions{}); err != nil {
		return fmt.Errorf("recovery snapshot failed integrity validation: %w", err)
	}
	caches, err := loadRouteCaches(ctx, s.db)
	if err != nil {
		return err
	}
	aggregates, err := loadAggregateCaches(ctx, s.db)
	if err != nil {
		return err
	}
	consistent, err := cachesConsistent(snapshot, caches, aggregates)
	if err != nil {
		return err
	}
	if !consistent {
		builtAt := nowString(s.now())
		recoveryIntentID, err := s.persistIntent(ctx, fmt.Sprintf("recovery-cache-v%d", currentVersion), "cache_recovery", currentVersion, currentVersion, map[string]any{
			"reason":           "cache drift detected during startup recovery",
			"snapshot_version": currentVersion,
		})
		if err != nil {
			return fmt.Errorf("persist cache recovery intent: %w", err)
		}
		rebuildRoute := make(map[string]RouteCache, len(snapshot.LogicalTables))
		for _, table := range snapshot.LogicalTables {
			cache, err := buildRouteCache(table, currentVersion, currentVersion, builtAt)
			if err != nil {
				return err
			}
			rebuildRoute[table.ID] = cache
			report.CacheTables = append(report.CacheTables, table.ID)
		}
		rebuildAggregates := make(map[string]AggregateCache, len(snapshot.Aggregates))
		for _, view := range snapshot.Aggregates {
			cache, err := buildAggregateCache(view, snapshot, rebuildRoute, currentVersion, currentVersion, builtAt)
			if err != nil {
				return err
			}
			rebuildAggregates[view.ID] = cache
			report.CacheViews = append(report.CacheViews, view.ID)
		}
		sort.Strings(report.CacheTables)
		sort.Strings(report.CacheViews)
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin cache recovery: %w", err)
		}
		if err := replaceAllCachesTx(ctx, tx, rebuildRoute, rebuildAggregates); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("rebuild recovered caches: %w", err)
		}
		if err := markIntentCommittedTx(ctx, tx, recoveryIntentID, builtAt); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("finalize cache recovery intent: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit recovered caches: %w", err)
		}
		report.CacheAction = "rebuilt_current_snapshot"
	}

	if report.PendingFound > 0 || report.CacheAction != "none" {
		if err := s.saveRecoveryReport(ctx, report); err != nil {
			return err
		}
	}
	return s.loadLatestRecovery(ctx)
}

func cachesConsistent(snapshot model.Snapshot, caches map[string]RouteCache, aggregates map[string]AggregateCache) (bool, error) {
	tableSet := make(map[string]struct{}, len(snapshot.LogicalTables))
	for _, table := range snapshot.LogicalTables {
		tableSet[table.ID] = struct{}{}
		cache, ok := caches[table.ID]
		if !ok {
			return false, nil
		}
		if cache.CacheVersion < 1 || cache.CacheVersion > snapshot.Version || cache.ExpectedVersion > snapshot.Version || cache.ExpectedVersion > cache.CacheVersion {
			return false, nil
		}
		expected, err := buildRouteCache(table, cache.CacheVersion, cache.ExpectedVersion, cache.BuiltAt)
		if err != nil {
			return false, err
		}
		if expected.Checksum != cache.Checksum ||
			!reflect.DeepEqual(expected.PhysicalTables, cache.PhysicalTables) ||
			!reflect.DeepEqual(expected.BoundaryIndex, cache.BoundaryIndex) ||
			!reflect.DeepEqual(expected.Refs, cache.Refs) {
			return false, nil
		}
	}
	if len(caches) != len(tableSet) {
		return false, nil
	}
	viewSet := make(map[string]struct{}, len(snapshot.Aggregates))
	for _, view := range snapshot.Aggregates {
		viewSet[view.ID] = struct{}{}
		cache, ok := aggregates[view.ID]
		if !ok {
			return false, nil
		}
		if cache.CacheVersion < 1 || cache.CacheVersion > snapshot.Version || cache.ExpectedVersion > snapshot.Version || cache.ExpectedVersion > cache.CacheVersion {
			return false, nil
		}
		expected, err := buildAggregateCache(view, snapshot, caches, cache.CacheVersion, cache.ExpectedVersion, cache.BuiltAt)
		if err != nil {
			return false, err
		}
		expectedPayload, err := json.Marshal(expected.Payload)
		if err != nil {
			return false, err
		}
		actualPayload, err := json.Marshal(cache.Payload)
		if err != nil {
			return false, err
		}
		if string(expectedPayload) != string(actualPayload) ||
			!reflect.DeepEqual(expected.PhysicalTables, cache.PhysicalTables) ||
			!reflect.DeepEqual(expected.DependsOn, cache.DependsOn) ||
			!reflect.DeepEqual(expected.Refs, cache.Refs) {
			return false, nil
		}
	}
	if len(aggregates) != len(viewSet) {
		return false, nil
	}
	return true, nil
}

func (s *Store) saveRecoveryReport(ctx context.Context, report RecoveryReport) error {
	raw, err := json.Marshal(report)
	if err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO recovery_reports(created_at, report_json) VALUES(?,?)`, report.GeneratedAt, string(raw)); err != nil {
		return fmt.Errorf("persist recovery report: %w", err)
	}
	return nil
}

func (s *Store) loadLatestRecovery(ctx context.Context) error {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT report_json FROM recovery_reports ORDER BY id DESC LIMIT 1`).Scan(&raw)
	if err == sql.ErrNoRows {
		s.mu.Lock()
		s.recovery = nil
		s.mu.Unlock()
		return nil
	}
	if err != nil {
		return fmt.Errorf("load latest recovery report: %w", err)
	}
	var report RecoveryReport
	if err := json.Unmarshal([]byte(raw), &report); err != nil {
		return fmt.Errorf("decode latest recovery report: %w", err)
	}
	s.mu.Lock()
	s.recovery = &report
	s.mu.Unlock()
	return nil
}

func (s *Store) RecoveryReport() *RecoveryReport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.recovery == nil {
		return nil
	}
	copyReport := *s.recovery
	copyReport.Actions = append([]RecoveryAction(nil), s.recovery.Actions...)
	copyReport.CacheTables = append([]string(nil), s.recovery.CacheTables...)
	copyReport.CacheViews = append([]string(nil), s.recovery.CacheViews...)
	return &copyReport
}
