package configimpact

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

func recoverDatabase(ctx context.Context, db *sql.DB) (RecoveryReport, error) {
	report := RecoveryReport{ScanPerformed: true}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return report, storageError(err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT id, kind, COALESCE(patch_id,''), base_version, target_version
		FROM intents
		WHERE status IN ('pending','in_progress')
		ORDER BY id`)
	if err != nil {
		return report, storageError(err)
	}
	type pendingIntent struct {
		id, baseVersion, targetVersion int64
		kind, patchID                  string
	}
	pending := make([]pendingIntent, 0)
	for rows.Next() {
		var item pendingIntent
		if err := rows.Scan(&item.id, &item.kind, &item.patchID, &item.baseVersion, &item.targetVersion); err != nil {
			rows.Close()
			return report, storageError(err)
		}
		pending = append(pending, item)
	}
	if err := rows.Close(); err != nil {
		return report, storageError(err)
	}
	if err := rows.Err(); err != nil {
		return report, storageError(err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, item := range pending {
		detail, _ := json.Marshal(map[string]any{
			"recovery":          "process terminated before snapshot commit",
			"aborted_intent_id": item.id,
			"target_version":    item.targetVersion,
		})
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='aborted', finished_at=?, detail_json=? WHERE id=?`, now, string(detail), item.id); err != nil {
			return report, storageError(err)
		}
		if item.patchID != "" {
			if _, err := tx.ExecContext(ctx, `
				UPDATE patch_log SET state='aborted', result_json=NULL, document_version=NULL, updated_at=?
				WHERE patch_id=? AND state <> 'complete'`, now, item.patchID); err != nil {
				return report, storageError(err)
			}
		}
		report.AbortedIntents = append(report.AbortedIntents, RecoveredIntent{
			IntentID:      item.id,
			Kind:          item.kind,
			PatchID:       item.patchID,
			BaseVersion:   item.baseVersion,
			TargetVersion: item.targetVersion,
			Status:        "aborted",
			FinishedAt:    now,
		})
	}
	report.PendingIntentsScanned = len(pending)

	completeVersions, discardedVersions, err := inspectPersistedDocuments(ctx, tx)
	if err != nil {
		return report, storageError(err)
	}
	report.CompleteDocumentVersions = completeVersions
	report.DiscardedIncompleteVersions = discardedVersions
	if len(completeVersions) > 0 {
		report.LastCompleteVersion = completeVersions[len(completeVersions)-1]
	}

	orphanRows, err := countOrphanSnapshotRows(ctx, tx)
	if err != nil {
		return report, storageError(err)
	}
	report.OrphanSnapshotRows = orphanRows
	if orphanRows > 0 {
		for _, table := range []string{"materializations", "impacts", "refs"} {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE snapshot_version NOT IN (SELECT version FROM documents)`, table)); err != nil {
				return report, storageError(err)
			}
		}
	}

	if err := setCurrentVersion(ctx, tx, report.LastCompleteVersion); err != nil {
		return report, storageError(err)
	}
	if err := tx.Commit(); err != nil {
		return report, storageError(err)
	}
	return report, nil
}

// inspectPersistedDocuments treats a version as complete only when its
// document, every materialization and every impact record were all committed.
// Rows from an incomplete version are removed inside the recovery transaction.
func inspectPersistedDocuments(ctx context.Context, tx *sql.Tx) ([]int64, []int64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT version, payload FROM documents ORDER BY version`)
	if err != nil {
		return nil, nil, err
	}
	type persistedDocument struct {
		version int64
		payload []byte
	}
	documents := make([]persistedDocument, 0)
	for rows.Next() {
		var item persistedDocument
		if err := rows.Scan(&item.version, &item.payload); err != nil {
			rows.Close()
			return nil, nil, err
		}
		documents = append(documents, item)
	}
	if err := rows.Close(); err != nil {
		return nil, nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	complete := make([]int64, 0, len(documents))
	discarded := make([]int64, 0)
	for _, item := range documents {
		var doc Document
		valid := json.Unmarshal(item.payload, &doc) == nil && doc.Version == item.version
		if valid {
			valid, err = snapshotComponentSetMatches(ctx, tx, "materializations", item.version, doc.Components)
			if err != nil {
				return nil, nil, err
			}
		}
		if valid {
			valid, err = snapshotComponentSetMatches(ctx, tx, "impacts", item.version, doc.Components)
			if err != nil {
				return nil, nil, err
			}
		}
		if valid {
			complete = append(complete, item.version)
			continue
		}
		discarded = append(discarded, item.version)
		for _, table := range []string{"materializations", "impacts", "refs"} {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE snapshot_version=?`, table), item.version); err != nil {
				return nil, nil, err
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM documents WHERE version=?`, item.version); err != nil {
			return nil, nil, err
		}
	}
	return complete, discarded, nil
}

func snapshotComponentSetMatches(ctx context.Context, tx *sql.Tx, table string, version int64, components []Component) (bool, error) {
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`SELECT component_id FROM %s WHERE snapshot_version=?`, table), version)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	persisted := make(map[string]struct{})
	for rows.Next() {
		var componentID string
		if err := rows.Scan(&componentID); err != nil {
			return false, err
		}
		persisted[componentID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if len(persisted) != len(components) {
		return false, nil
	}
	for _, component := range components {
		if _, exists := persisted[component.ID]; !exists {
			return false, nil
		}
	}
	return true, nil
}

func countOrphanSnapshotRows(ctx context.Context, tx *sql.Tx) (int64, error) {
	var total int64
	for _, table := range []string{"materializations", "impacts", "refs"} {
		var count int64
		query := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE snapshot_version NOT IN (SELECT version FROM documents)`, table)
		if err := tx.QueryRowContext(ctx, query).Scan(&count); err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}
