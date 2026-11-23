package cache

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
)

type EntryRollback struct {
	IntentID string `json:"intent_id"`
	EntryID  string `json:"entry_id"`
	CacheKey string `json:"cache_key"`
	From     State  `json:"from"`
	To       State  `json:"to"`
}

type EvictionRollback struct {
	IntentID string `json:"intent_id"`
	EntryID  string `json:"entry_id"`
	CacheKey string `json:"cache_key"`
	From     State  `json:"from"`
	To       State  `json:"to"`
}

type RecoveryFacts struct {
	EntryCounts      map[string]int `json:"entry_counts"`
	ArtifactCount    int            `json:"artifact_count"`
	UsedBytes        int64          `json:"used_bytes"`
	SnapshotSequence int64          `json:"snapshot_sequence"`
}

type RecoveryReport struct {
	GeneratedAt         string             `json:"generated_at"`
	IntentsExamined     int                `json:"intents_examined"`
	IntentsAborted      int                `json:"intents_aborted"`
	EntriesRolledBack   []EntryRollback    `json:"entries_rolled_back"`
	EvictionsRolledBack []EvictionRollback `json:"evictions_rolled_back"`
	OrphansRemoved      []string           `json:"orphans_removed"`
	MissingArtifacts    []string           `json:"missing_artifacts"`
	Facts               RecoveryFacts      `json:"facts"`
}

type intentPayload struct {
	EntryID string `json:"entry_id"`
}

func (s *Store) recover(ctx context.Context) (*RecoveryReport, error) {
	now := s.Now()
	intents, err := loadRunningIntents(ctx, s.db)
	if err != nil {
		return nil, err
	}
	report := RecoveryReport{
		GeneratedAt:         formatTime(now),
		IntentsExamined:     len(intents),
		EntriesRolledBack:   make([]EntryRollback, 0),
		EvictionsRolledBack: make([]EvictionRollback, 0),
		OrphansRemoved:      make([]string, 0),
		MissingArtifacts:    make([]string, 0),
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, dbError("cannot start recovery transaction", err)
	}
	for _, intent := range intents {
		var payload intentPayload
		if len(intent.Payload) > 0 {
			if err := json.Unmarshal(intent.Payload, &payload); err != nil {
				_ = tx.Rollback()
				return nil, wrapError(CodeRecoveryIncomplete, "unfinished intent payload is corrupt", map[string]any{
					"intent_id": intent.IntentID,
					"operation": intent.Operation,
				}, err)
			}
		}
		switch intent.Operation {
		case intentWrite:
			if payload.EntryID == "" {
				continue
			}
			var cacheKey, state string
			err := tx.QueryRowContext(ctx, `SELECT cache_key, state FROM entries WHERE entry_id=?`, payload.EntryID).Scan(&cacheKey, &state)
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			if err != nil {
				_ = tx.Rollback()
				return nil, dbError("cannot inspect interrupted write", err)
			}
			if State(state) == StatePending || State(state) == StateWriting {
				_, err = tx.ExecContext(ctx, `UPDATE entries SET state='PENDING', version=version+1 WHERE entry_id=?`, payload.EntryID)
				if err != nil {
					_ = tx.Rollback()
					return nil, dbError("cannot roll interrupted write back to PENDING", err)
				}
				report.EntriesRolledBack = append(report.EntriesRolledBack, EntryRollback{
					IntentID: intent.IntentID,
					EntryID:  payload.EntryID,
					CacheKey: cacheKey,
					From:     State(state),
					To:       StatePending,
				})
			}
		case intentEvict:
			if payload.EntryID == "" {
				continue
			}
			var cacheKey, state string
			err := tx.QueryRowContext(ctx, `SELECT cache_key, state FROM entries WHERE entry_id=?`, payload.EntryID).Scan(&cacheKey, &state)
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			if err != nil {
				_ = tx.Rollback()
				return nil, dbError("cannot inspect interrupted eviction", err)
			}
			if State(state) == StateStale {
				_, err = tx.ExecContext(ctx, `UPDATE entries SET state='AVAILABLE', version=version+1 WHERE entry_id=?`, payload.EntryID)
				if err != nil {
					_ = tx.Rollback()
					return nil, dbError("cannot roll interrupted eviction back to AVAILABLE", err)
				}
				report.EvictionsRolledBack = append(report.EvictionsRolledBack, EvictionRollback{
					IntentID: intent.IntentID,
					EntryID:  payload.EntryID,
					CacheKey: cacheKey,
					From:     State(state),
					To:       StateAvailable,
				})
			}
		case intentTransition, intentSnapshot:
			// The state/snapshot mutation and intent completion share one
			// transaction, so a RUNNING intent means the mutation never committed.
		}
		result, err := tx.ExecContext(ctx, `UPDATE intents SET status='ABORTED', completed_at_ns=? WHERE intent_id=? AND status='RUNNING'`, ns(now), intent.IntentID)
		if err != nil {
			_ = tx.Rollback()
			return nil, dbError("cannot abort unfinished intent", err)
		}
		affected, _ := result.RowsAffected()
		report.IntentsAborted += int(affected)
	}
	if err := tx.Commit(); err != nil {
		return nil, dbError("cannot commit recovery rollback", err)
	}

	orphans, missing, err := s.reconcileArtifactFiles(ctx)
	if err != nil {
		return nil, err
	}
	report.OrphansRemoved = orphans
	report.MissingArtifacts = missing

	factsCatalog, err := s.loadCatalog(ctx)
	if err != nil {
		return nil, err
	}
	report.Facts = RecoveryFacts{
		EntryCounts:      factsCatalog.entryCounts(),
		ArtifactCount:    len(factsCatalog.artifacts),
		UsedBytes:        factsCatalog.usedBytes,
		SnapshotSequence: factsCatalog.sequence,
	}
	reportJSON, err := json.Marshal(report)
	if err != nil {
		return nil, wrapError(CodeInternal, "cannot encode recovery report", nil, err)
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO recovery_reports(generated_at_ns, report_json) VALUES (?, ?)`, ns(now), string(reportJSON))
	if err != nil {
		return nil, dbError("cannot persist recovery report", err)
	}
	return &report, nil
}

// reconcileArtifactFiles only acts on filesystem facts plus artifact rows that
// survived the recovery transaction. It removes unreferenced content and
// reports missing files for referenced digests.
func (s *Store) reconcileArtifactFiles(ctx context.Context) ([]string, []string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT relative_path FROM artifacts`)
	if err != nil {
		return nil, nil, dbError("cannot enumerate persisted artifacts", err)
	}
	referenced := make(map[string]bool)
	for rows.Next() {
		var relative string
		if err := rows.Scan(&relative); err != nil {
			rows.Close()
			return nil, nil, dbError("cannot decode persisted artifact path", err)
		}
		referenced[filepath.Clean(s.artifactPathForRelative(relative))] = true
	}
	if err := rows.Close(); err != nil {
		return nil, nil, dbError("cannot close artifact query", err)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, dbError("cannot iterate persisted artifacts", err)
	}

	removed := make([]string, 0)
	missing := make([]string, 0)
	for path := range referenced {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			missing = append(missing, path)
		} else if err != nil {
			return nil, nil, wrapError(CodeIO, "cannot inspect referenced artifact", map[string]any{"path": path}, err)
		}
	}
	entries, err := os.ReadDir(s.artifactDir)
	if err != nil {
		return nil, nil, wrapError(CodeIO, "cannot list artifact directory", nil, err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(s.artifactDir, entry.Name())
		if referenced[filepath.Clean(path)] {
			continue
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, nil, wrapError(CodeIO, "cannot remove orphan artifact", map[string]any{"path": path}, err)
		}
		removed = append(removed, path)
	}
	staged, err := os.ReadDir(s.stagingDir)
	if err != nil {
		return nil, nil, wrapError(CodeIO, "cannot list staging directory", nil, err)
	}
	for _, entry := range staged {
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(s.stagingDir, entry.Name())
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, nil, wrapError(CodeIO, "cannot remove staged artifact", map[string]any{"path": path}, err)
		}
		removed = append(removed, path)
	}
	sort.Strings(removed)
	sort.Strings(missing)
	return removed, missing, nil
}

func (s *Store) LatestRecoveryReport(ctx context.Context) (RecoveryReport, error) {
	var data string
	err := s.db.QueryRowContext(ctx, `SELECT report_json FROM recovery_reports ORDER BY id DESC LIMIT 1`).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return RecoveryReport{}, newError(CodeCacheMiss, "no recovery report has been persisted", map[string]any{"reason": "REPORT_NOT_FOUND"})
	}
	if err != nil {
		return RecoveryReport{}, dbError("cannot read recovery report", err)
	}
	var report RecoveryReport
	if err := json.Unmarshal([]byte(data), &report); err != nil {
		return RecoveryReport{}, wrapError(CodeIO, "persisted recovery report is corrupt", nil, err)
	}
	return report, nil
}
