package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
)

func (s *Store) CommitImport(ctx context.Context, in ImportCommit) error {
	if in.Snapshot == nil {
		return fmt.Errorf("commit snapshot is nil")
	}
	if in.Snapshot.SnapshotVersion > math.MaxInt64 {
		return fmt.Errorf("snapshot version overflows sqlite integer")
	}
	derived, err := BuildDerived(in.Snapshot, in.AffectedServices, in.NowMs)
	if err != nil {
		return err
	}
	tx, err := s.BeginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	defaultsJSON := marshalAny(in.Snapshot.DefaultDetails)
	_, err = tx.ExecContext(ctx, `INSERT INTO snapshots(snapshot_version, snapshot_id, generated_at_ms, source_format, format_version,
		source_digest, content_digest, policy_count, cumulative_rounding_error_us, defaults_json, imported_at_ms)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		int64(in.Snapshot.SnapshotVersion), in.Snapshot.SnapshotID, in.Snapshot.GeneratedAtMs, in.Snapshot.SourceFormat,
		in.Snapshot.FormatVersion, in.Snapshot.SourceDigest, in.Snapshot.ContentDigest, len(in.Snapshot.Policies),
		in.Snapshot.CumulativeRoundingError, defaultsJSON, in.NowMs)
	if err != nil {
		return fmt.Errorf("insert snapshot: %w", err)
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO policies(snapshot_version, policy_id, virtual_service_id, policy_revision, last_write_ms, policy_json)
		VALUES(?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	for _, p := range in.Snapshot.Policies {
		if p.PolicyRevision > math.MaxInt64 {
			stmt.Close()
			return fmt.Errorf("policy revision overflows sqlite integer: %s", p.PolicyID)
		}
		raw, err := json.Marshal(p)
		if err != nil {
			stmt.Close()
			return err
		}
		if _, err := stmt.ExecContext(ctx, int64(in.Snapshot.SnapshotVersion), p.PolicyID, p.VirtualServiceID, int64(p.PolicyRevision), p.LastWriteMs, string(raw)); err != nil {
			stmt.Close()
			return fmt.Errorf("insert policy %s: %w", p.PolicyID, err)
		}
	}
	stmt.Close()
	if err := insertDerivedRowsTx(ctx, tx, derived); err != nil {
		return err
	}
	if in.MidCommitHook != nil {
		if err := in.MidCommitHook(ctx, tx); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO active_snapshot(id, snapshot_version) VALUES(1, ?)
		ON CONFLICT(id) DO UPDATE SET snapshot_version = excluded.snapshot_version`, int64(in.Snapshot.SnapshotVersion))
	if err != nil {
		return fmt.Errorf("switch active snapshot: %w", err)
	}
	report := ReportRecord{
		ReportID:        in.ReportID,
		RequestID:       in.RequestID,
		SnapshotVersion: in.Snapshot.SnapshotVersion,
		Status:          "committed",
		Decision:        in.Decision,
		SourceFormat:    in.Snapshot.SourceFormat,
		FormatVersion:   in.Snapshot.FormatVersion,
		SourceDigest:    in.Snapshot.SourceDigest,
		ContentDigest:   in.Snapshot.ContentDigest,
		CreatedAtMs:     in.NowMs,
		ReportJSON:      in.ReportJSON,
		Items:           in.ReportItems,
	}
	if err := insertReportTx(ctx, tx, report); err != nil {
		return err
	}
	if err := completeRequestTx(ctx, tx, in.RequestID, in.HTTPStatus, in.ResponseJSON, in.ReportID, in.NowMs); err != nil {
		return err
	}
	facts := marshalAny(map[string]any{
		"committedSnapshotVersion": in.Snapshot.SnapshotVersion,
		"activeSnapshotVersion":    in.Snapshot.SnapshotVersion,
		"reportId":                 in.ReportID,
		"policyCount":              len(in.Snapshot.Policies),
		"affectedServices":         in.AffectedServices,
	})
	res, err := tx.ExecContext(ctx, `UPDATE intents SET status = 'completed', completed_at_ms = ?, facts_json = ? WHERE intent_id = ? AND status = 'pending'`, in.NowMs, facts, in.IntentID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("intent %s is not pending", in.IntentID)
	}
	return tx.Commit()
}

// insertDerivedRowsTx inserts all derived structures for exactly one snapshot.
// The caller's transaction is also the only transaction that switches
// active_snapshot, so readers never observe a mixed snapshot/version pair.
func insertDerivedRowsTx(ctx context.Context, tx *sql.Tx, d *DerivedData) error {
	for _, r := range d.Routes {
		headers, _ := json.Marshal(r.Headers)
		if _, err := tx.ExecContext(ctx, `INSERT INTO route_index(snapshot_version, virtual_service_id, route_key, policy_id, method, path_prefix, headers_json)
			VALUES(?, ?, ?, ?, ?, ?, ?)`, int64(d.SnapshotVersion), r.VirtualServiceID, r.RouteKey, r.PolicyID, r.Method, r.PathPrefix, string(headers)); err != nil {
			return fmt.Errorf("insert route index: %w", err)
		}
	}
	for _, r := range d.RateWindows {
		if _, err := tx.ExecContext(ctx, `INSERT INTO rate_window_stats(snapshot_version, virtual_service_id, window_start_ms, window_end_ms, window_ms, limit_bps, policy_count, policy_ids_json)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?)`, int64(d.SnapshotVersion), r.VirtualServiceID, r.WindowStartMs, r.WindowEndMs, r.WindowMs, strconv.FormatUint(r.LimitBps, 10), r.PolicyCount, marshalStrings(r.PolicyIDs)); err != nil {
			return fmt.Errorf("insert rate window: %w", err)
		}
	}
	for _, r := range d.Circuits {
		if _, err := tx.ExecContext(ctx, `INSERT INTO circuit_state(snapshot_version, virtual_service_id, state, window_ms, policy_ids_json)
			VALUES(?, ?, ?, ?, ?)`, int64(d.SnapshotVersion), r.VirtualServiceID, r.State, r.WindowMs, marshalStrings(r.PolicyIDs)); err != nil {
			return fmt.Errorf("insert circuit state: %w", err)
		}
	}
	for _, r := range d.Summaries {
		if _, err := tx.ExecContext(ctx, `INSERT INTO service_summaries(snapshot_version, virtual_service_id, policy_count, route_count, rate_window_count, total_rate_bps, circuit_state, policy_ids_json)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?)`, int64(d.SnapshotVersion), r.VirtualServiceID, r.PolicyCount, r.RouteCount, r.RateWindowCount,
			strconv.FormatUint(r.TotalRateBps, 10), r.CircuitState, marshalStrings(r.PolicyIDs)); err != nil {
			return fmt.Errorf("insert service summary: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO snapshot_derivations(snapshot_version, affected_services_json, rebuilt_at_ms, route_count, window_count, circuit_count, summary_count)
		VALUES(?, ?, ?, ?, ?, ?, ?)`, int64(d.SnapshotVersion), marshalStrings(d.Affected), d.RebuiltAtMs, len(d.Routes), len(d.RateWindows), len(d.Circuits), len(d.Summaries)); err != nil {
		return fmt.Errorf("insert derivation marker: %w", err)
	}
	return nil
}
