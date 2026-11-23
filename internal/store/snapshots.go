package store

import "database/sql"

// RegisterSnapshot records an active reader snapshot so recycling knows which
// old commit versions still have readers.
func (s *Store) RegisterSnapshot(snap ActiveSnapshot) error {
	_, err := s.DB.Exec(`INSERT INTO active_snapshots(snapshot_ver, holder, opened_at, orphaned)
		VALUES(?,?,?,0) ON CONFLICT(snapshot_ver, holder) DO UPDATE SET orphaned=0`,
		snap.SnapshotVer, snap.Holder, snap.OpenedAtMs)
	return err
}

// ReleaseSnapshot removes a reader registration.
func (s *Store) ReleaseSnapshot(snapshotVer int64, holder string) error {
	_, err := s.DB.Exec(`DELETE FROM active_snapshots WHERE snapshot_ver=? AND holder=?`,
		snapshotVer, holder)
	return err
}

// MinActiveSnapshot returns the lowest non-orphaned snapshot still open, or
// (0, false) when no reader is registered. Orphaned rows left by a killed
// process are ignored unless no live readers exist, in which case the oldest
// orphan still acts conservatively until explicitly pruned.
func (s *Store) MinActiveSnapshot() (int64, bool, error) {
	var minLive sql.NullInt64
	if err := s.DB.QueryRow(`SELECT MIN(snapshot_ver) FROM active_snapshots WHERE orphaned=0`).
		Scan(&minLive); err != nil {
		return 0, false, err
	}
	if minLive.Valid {
		return minLive.Int64, true, nil
	}
	var minOrphan sql.NullInt64
	if err := s.DB.QueryRow(`SELECT MIN(snapshot_ver) FROM active_snapshots WHERE orphaned=1`).
		Scan(&minOrphan); err != nil {
		return 0, false, err
	}
	if minOrphan.Valid {
		return minOrphan.Int64, true, nil
	}
	return 0, false, nil
}

// MarkSnapshotsOrphaned flags every registration as belonging to a dead
// process during recovery. They continue to protect versions conservatively.
func (s *Store) MarkSnapshotsOrphaned() (int64, error) {
	res, err := s.DB.Exec(`UPDATE active_snapshots SET orphaned=1 WHERE orphaned=0`)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// PruneOrphanedSnapshots removes orphaned registrations (operator action).
func (s *Store) PruneOrphanedSnapshots() (int64, error) {
	res, err := s.DB.Exec(`DELETE FROM active_snapshots WHERE orphaned=1`)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// ListSnapshots returns all registrations for diagnostics and the matrix demo.
func (s *Store) ListSnapshots() ([]ActiveSnapshot, error) {
	rows, err := s.DB.Query(`SELECT snapshot_ver, holder, opened_at, orphaned
		FROM active_snapshots ORDER BY snapshot_ver, holder`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ActiveSnapshot
	for rows.Next() {
		var a ActiveSnapshot
		var orphan int
		if err := rows.Scan(&a.SnapshotVer, &a.Holder, &a.OpenedAtMs, &orphan); err != nil {
			return nil, err
		}
		a.Orphaned = orphan == 1
		out = append(out, a)
	}
	return out, rows.Err()
}

// Execer is satisfied by both *sql.DB and *sql.Tx.
type Execer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// RecordRecoveryEvent appends a fact-only recovery audit event.
func RecordRecoveryEvent(exec Execer, ev RecoveryEvent) error {
	_, err := exec.Exec(`INSERT INTO recovery_events(intent_id, kind, row_id, action, detail, created_at)
		VALUES(?,?,?,?,?,?)`, ev.IntentID, string(ev.Kind), ev.RowID, ev.Action, ev.Detail, ev.CreatedMs)
	return err
}

// RecoveryEvents returns the audit trail for /recovery/report.
func (s *Store) RecoveryEvents() ([]RecoveryEvent, error) {
	rows, err := s.DB.Query(`SELECT intent_id, kind, row_id, action, detail, created_at
		FROM recovery_events ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RecoveryEvent
	for rows.Next() {
		var ev RecoveryEvent
		var kind string
		if err := rows.Scan(&ev.IntentID, &kind, &ev.RowID, &ev.Action, &ev.Detail, &ev.CreatedMs); err != nil {
			return nil, err
		}
		ev.Kind = IntentKind(kind)
		out = append(out, ev)
	}
	return out, rows.Err()
}
