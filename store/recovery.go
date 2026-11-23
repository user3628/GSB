package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"releasegate/domain"
)

const ClaimBoundary = "Only durable facts observed after restart are reported. No report existed at the instant of forced termination; a process cannot emit an exit report after being killed."

type RecoveryFact struct {
	IntentID      string `json:"intent_id,omitempty"`
	ReleaseID     string `json:"release_id,omitempty"`
	Kind          string `json:"kind"`
	Action        string `json:"action"`
	Detail        string `json:"detail"`
	PersistedFact string `json:"persisted_fact"`
}

type RecoveryReport struct {
	ReportID                  int64          `json:"report_id"`
	StartedAtMS               int64          `json:"started_at_ms"`
	FinishedAtMS              int64          `json:"finished_at_ms"`
	ScanCompletedAfterRestart bool           `json:"scan_completed_after_restart"`
	ReleaseIDs                []string       `json:"release_ids,omitempty"`
	PendingIntentsFound       int            `json:"pending_intents_found"`
	IntentsRecovered          int            `json:"intents_recovered"`
	OrphanBatchesAborted      int            `json:"orphan_batches_aborted"`
	VersionsRestored          int            `json:"versions_restored"`
	Facts                     []RecoveryFact `json:"facts"`
	ClaimBoundary             string         `json:"claim_boundary"`
}

// Recover deliberately treats intent records as durable claims, not as evidence
// that the associated mutation committed. It repairs only facts it can verify in
// the database and writes a fresh report after restart.
func (s *Store) Recover(ctx context.Context) (RecoveryReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	started := time.Now().UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RecoveryReport{}, err
	}
	defer tx.Rollback()

	pending, err := s.PendingIntents(ctx, tx)
	if err != nil {
		return RecoveryReport{}, err
	}
	report := RecoveryReport{
		StartedAtMS:               started,
		PendingIntentsFound:       len(pending),
		ScanCompletedAfterRestart: true,
		ClaimBoundary:             ClaimBoundary,
		Facts:                     []RecoveryFact{},
	}
	releaseSet := map[string]struct{}{}

	// Abort every unreachable batch, whether or not its intent survived.
	orphans, err := rowsStringPair(ctx, tx, `SELECT release_id,batch_id FROM observation_batches WHERE status=? ORDER BY received_at_ms,batch_id`, BatchStaging)
	if err != nil {
		return RecoveryReport{}, err
	}
	for _, pair := range orphans {
		res, err := tx.ExecContext(ctx, `UPDATE observation_batches SET status=?,abort_reason=?
			WHERE release_id=? AND batch_id=? AND status=?`, BatchAborted, "recovered after forced termination", pair[0], pair[1], BatchStaging)
		if err != nil {
			return RecoveryReport{}, err
		}
		if n, _ := res.RowsAffected(); n > 0 {
			report.OrphanBatchesAborted++
			releaseSet[pair[0]] = struct{}{}
			report.Facts = append(report.Facts, RecoveryFact{
				ReleaseID: pair[0], Kind: "INGEST_BATCH", Action: "ABORT_ORPHAN_BATCH",
				Detail: "batch never committed a statistics version", PersistedFact: "observation_batches.status=ABORTED",
			})
		}
	}

	for _, in := range pending {
		releaseSet[in.ReleaseID] = struct{}{}
		detail := "transaction did not commit; release header unchanged"
		action := "RESTORE_LAST_COMPLETE_VERSION"
		persisted := "release header unchanged"
		switch in.Kind {
		case "INGEST_BATCH", "RECOMPUTE_STATS":
			restored, err := repairStatsPointer(ctx, tx, in.ReleaseID)
			if err != nil {
				return RecoveryReport{}, err
			}
			if restored > 0 {
				report.VersionsRestored += restored
				detail = fmt.Sprintf("restored current_stats_version to last complete version after %d repair(s)", restored)
				persisted = "releases.current_stats_version points at a COMPLETE stats_versions row"
			}
		case "GATE_DECISION":
			action = "VERIFY_ATOMIC_DECISION"
			detail = "no committed decision/state/traffic tuple found for pending gate intent"
			persisted = "release state, stage and traffic remain on the pre-intent tuple"
		case "TRANSITION", "START_RELEASE":
			action = "VERIFY_ATOMIC_TRANSITION"
			detail = "pending transition intent has no committed matching release header"
			persisted = "release header unchanged"
		case "CREATE_RELEASE":
			action = "VERIFY_CREATE"
			detail = "release row is absent if the create transaction did not commit"
			persisted = "no partially-created release is visible"
		}
		report.Facts = append(report.Facts, RecoveryFact{
			IntentID: in.IntentID, ReleaseID: in.ReleaseID, Kind: in.Kind,
			Action: action, Detail: detail, PersistedFact: persisted,
		})
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET status=?,completed_at_ms=?,recovery_detail=?
			WHERE intent_id=? AND status=?`, IntentRecovered, time.Now().UnixMilli(), detail, in.IntentID, IntentPending); err != nil {
			return RecoveryReport{}, err
		}
		report.IntentsRecovered++
	}

	report.ReleaseIDs = sortedKeys(releaseSet)
	factsJSON, err := json.Marshal(report.Facts)
	if err != nil {
		return RecoveryReport{}, err
	}
	releasesJSON, err := json.Marshal(report.ReleaseIDs)
	if err != nil {
		return RecoveryReport{}, err
	}
	report.FinishedAtMS = time.Now().UnixMilli()
	res, err := tx.ExecContext(ctx, `INSERT INTO recovery_reports
		(started_at_ms,finished_at_ms,release_ids_json,pending_intents_found,intents_recovered,orphan_batches_aborted,versions_restored,facts_json,claim_boundary)
		VALUES (?,?,?,?,?,?,?,?,?)`,
		report.StartedAtMS, report.FinishedAtMS, string(releasesJSON), report.PendingIntentsFound,
		report.IntentsRecovered, report.OrphanBatchesAborted, report.VersionsRestored, string(factsJSON), report.ClaimBoundary)
	if err != nil {
		return RecoveryReport{}, err
	}
	report.ReportID, _ = res.LastInsertId()
	if err := tx.Commit(); err != nil {
		return RecoveryReport{}, err
	}
	return report, nil
}

func repairStatsPointer(ctx context.Context, tx *sql.Tx, releaseID string) (int, error) {
	var current int64
	if err := tx.QueryRowContext(ctx, `SELECT current_stats_version FROM releases WHERE release_id=?`, releaseID).Scan(&current); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	if current == 0 {
		return 0, nil
	}
	var status string
	err := tx.QueryRowContext(ctx, `SELECT status FROM stats_versions WHERE release_id=? AND version=?`, releaseID, current).Scan(&status)
	if err == nil && status == StatsComplete {
		return 0, nil
	}
	var last sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(version) FROM stats_versions WHERE release_id=? AND status=?`, releaseID, StatsComplete).Scan(&last); err != nil {
		return 0, err
	}
	if !last.Valid {
		return 0, errors.New("release references incomplete stats but no complete version exists")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE releases SET current_stats_version=? WHERE release_id=?`, last.Int64, releaseID); err != nil {
		return 0, err
	}
	return 1, nil
}

func (s *Store) LatestRecoveryReport(ctx context.Context, releaseID string) (RecoveryReport, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT report_id,started_at_ms,finished_at_ms,release_ids_json,pending_intents_found,
		intents_recovered,orphan_batches_aborted,versions_restored,facts_json,claim_boundary
		FROM recovery_reports ORDER BY report_id DESC`)
	if err != nil {
		return RecoveryReport{}, err
	}
	defer rows.Close()
	var latest *RecoveryReport
	for rows.Next() {
		var r RecoveryReport
		var releasesJSON, factsJSON string
		if err := rows.Scan(&r.ReportID, &r.StartedAtMS, &r.FinishedAtMS, &releasesJSON,
			&r.PendingIntentsFound, &r.IntentsRecovered, &r.OrphanBatchesAborted, &r.VersionsRestored, &factsJSON, &r.ClaimBoundary); err != nil {
			return RecoveryReport{}, err
		}
		if err := json.Unmarshal([]byte(releasesJSON), &r.ReleaseIDs); err != nil {
			return RecoveryReport{}, err
		}
		if err := json.Unmarshal([]byte(factsJSON), &r.Facts); err != nil {
			return RecoveryReport{}, err
		}
		r.ScanCompletedAfterRestart = true
		if latest == nil {
			copy := r
			latest = &copy
		}
		if releaseID == "" {
			return r, nil
		}
		if containsString(r.ReleaseIDs, releaseID) {
			return filterReportForRelease(r, releaseID), nil
		}
	}
	if err := rows.Err(); err != nil {
		return RecoveryReport{}, err
	}
	if latest == nil {
		return RecoveryReport{}, domain.NewError(domain.CodeNotFound, "no recovery report exists")
	}
	return filterReportForRelease(*latest, releaseID), nil
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func filterReportForRelease(r RecoveryReport, releaseID string) RecoveryReport {
	filtered := make([]RecoveryFact, 0, len(r.Facts))
	for _, fact := range r.Facts {
		if fact.ReleaseID == releaseID {
			filtered = append(filtered, fact)
		}
	}
	r.Facts = filtered
	if !containsString(r.ReleaseIDs, releaseID) {
		r.ReleaseIDs = []string{}
	}
	return r
}

func rowsStringPair(ctx context.Context, q DBTX, query string, args ...any) ([][2]string, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][2]string
	for rows.Next() {
		var a, b string
		if err := rows.Scan(&a, &b); err != nil {
			return nil, err
		}
		out = append(out, [2]string{a, b})
	}
	return out, rows.Err()
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
