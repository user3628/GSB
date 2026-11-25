package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"

	"example.com/gsb-router/internal/errs"
	"example.com/gsb-router/internal/model"
)

type SnapshotMeta struct {
	Version     int64  `json:"version"`
	GeneratedAt string `json:"generated_at"`
	Checksum    string `json:"checksum"`
	CreatedAt   string `json:"created_at"`
}

func (s *Store) CurrentVersion() int64 {
	return s.currentVersion()
}

func (s *Store) CurrentSnapshot() (model.Snapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.state.version == 0 {
		return model.Snapshot{}, false
	}
	return s.state.snapshot.Clone(), true
}

func (s *Store) Snapshots(ctx context.Context) ([]SnapshotMeta, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT version, generated_at, checksum, created_at FROM snapshots ORDER BY version DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []SnapshotMeta{}
	for rows.Next() {
		var item SnapshotMeta
		if err := rows.Scan(&item.Version, &item.GeneratedAt, &item.Checksum, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) Snapshot(ctx context.Context, version int64) (model.Snapshot, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT snapshot_json FROM snapshots WHERE version=?`, version).Scan(&raw)
	if err == sql.ErrNoRows {
		return model.Snapshot{}, errs.New(errs.CodeNotFound, "snapshot version not found", map[string]any{"version": version})
	}
	if err != nil {
		return model.Snapshot{}, err
	}
	var snapshot model.Snapshot
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		return model.Snapshot{}, fmt.Errorf("decode snapshot: %w", err)
	}
	return snapshot, nil
}

func (s *Store) RouteCaches() []RouteCache {
	s.mu.RLock()
	defer s.mu.RUnlock()
	caches := make([]RouteCache, 0, len(s.state.caches))
	for _, cache := range s.state.caches {
		caches = append(caches, cache)
	}
	sort.Slice(caches, func(i, j int) bool { return caches[i].TableID < caches[j].TableID })
	return caches
}

func (s *Store) AggregateCaches() []AggregateCache {
	s.mu.RLock()
	defer s.mu.RUnlock()
	caches := make([]AggregateCache, 0, len(s.state.aggregates))
	for _, cache := range s.state.aggregates {
		caches = append(caches, cache)
	}
	sort.Slice(caches, func(i, j int) bool { return caches[i].ViewID < caches[j].ViewID })
	return caches
}

func (s *Store) Intents(ctx context.Context, limit int) ([]IntentRecord, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, commit_id, kind, base_version, target_version, phase, status, created_at, finished_at FROM intents ORDER BY created_at DESC, id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []IntentRecord{}
	for rows.Next() {
		var item IntentRecord
		if err := rows.Scan(&item.ID, &item.CommitID, &item.Kind, &item.BaseVersion, &item.TargetVersion, &item.Phase, &item.Status, &item.CreatedAt, &item.FinishedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) Commits(ctx context.Context, limit int) ([]CommitRecord, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT commit_id, kind, status, version, created_at, content_hash FROM commits ORDER BY created_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []CommitRecord{}
	for rows.Next() {
		var item CommitRecord
		if err := rows.Scan(&item.CommitID, &item.Kind, &item.Status, &item.Version, &item.CreatedAt, &item.ContentHash); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) CommitReceipt(ctx context.Context, commitID string) (*CommitResult, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT response_json FROM commits WHERE commit_id=?`, commitID).Scan(&raw)
	if err == sql.ErrNoRows {
		return nil, errs.New(errs.CodeNotFound, "commit receipt not found", map[string]any{"commit_id": commitID})
	}
	if err != nil {
		return nil, err
	}
	var result CommitResult
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return nil, err
	}
	result.Reused = true
	return &result, nil
}
