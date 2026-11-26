package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"example.com/gsb/dnscache/internal/dnsmsg"
	"example.com/gsb/dnscache/internal/errs"
)

func (s *Store) SubmitSnapshot(ctx context.Context, snap ZoneSnapshot, batchID string) (ZoneCommitResult, error) {
	snap.Zone = dnsmsg.CanonicalName(snap.Zone)
	snap.RRsets = normalizedRRsets(snap.RRsets, snap.Zone)
	if snap.Zone == "." || snap.Zone == "" {
		return ZoneCommitResult{}, errs.New(errs.EInvalidRequest, "snapshot zone is empty")
	}
	if snap.Version <= 0 {
		return ZoneCommitResult{}, errs.New(errs.EInvalidRequest, "snapshot version must be positive")
	}
	if snap.GeneratedAtMS <= 0 {
		return ZoneCommitResult{}, errs.New(errs.EInvalidRequest, "snapshot generated_at_ms must be positive")
	}
	if snap.Checksum == "" {
		return ZoneCommitResult{}, errs.New(errs.EChecksumRequired, "snapshot checksum is required")
	}
	if !strings.EqualFold(strings.TrimSpace(snap.Checksum), ComputeSnapshotChecksum(snap)) {
		return ZoneCommitResult{}, errs.New(errs.EChecksumMismatch, "snapshot checksum does not match canonical content")
	}
	seen := map[string]bool{}
	for _, rs := range snap.RRsets {
		if err := validateRRset(snap.Zone, rs); err != nil {
			return ZoneCommitResult{}, err
		}
		key := fmt.Sprintf("%s|%d", rs.Owner, rs.Type)
		if seen[key] {
			return ZoneCommitResult{}, errs.New(errs.EInvalidRequest, "duplicate RRset %s/%s", rs.Owner, rs.Type)
		}
		seen[key] = true
	}
	hash := strings.ToLower(snap.Checksum)
	if batchID == "" {
		batchID = s.nextID("snapshot")
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if result, hit, err := s.checkZoneIdempotency(ctx, "zone_snapshot", batchID, hash); err != nil {
		return ZoneCommitResult{}, err
	} else if hit {
		return result, nil
	}
	var currentVersion int64
	err := s.db.QueryRowContext(ctx, `SELECT version FROM zones WHERE zone=?`, snap.Zone).Scan(&currentVersion)
	if err != nil && err != sql.ErrNoRows {
		return ZoneCommitResult{}, err
	}
	if err == nil && snap.Version <= currentVersion {
		e := errs.New(errs.EStaleBase, "snapshot version %d is not greater than committed version %d", snap.Version, currentVersion)
		s.recordIdemFailure(ctx, "zone_snapshot", batchID, hash, e)
		return ZoneCommitResult{}, e
	}
	prev := int64(0)
	if err == nil {
		prev = currentVersion
	}
	now := s.nowMS()
	intentID := s.nextID("intent-zone")
	if err := prepareBatch(ctx, s.db, intentID, batchID, snap.Zone, "snapshot", prev, snap.Version, snap.Checksum, snap, now); err != nil {
		return ZoneCommitResult{}, err
	}
	if s.options.AfterIntent != nil {
		s.options.AfterIntent("snapshot", batchID)
	}

	result, err := s.commitSnapshot(ctx, intentID, batchID, snap, prev, hash, now)
	if err != nil {
		s.abortBatch(ctx, intentID, batchID, err)
		return ZoneCommitResult{}, err
	}
	return result, nil
}

func (s *Store) commitSnapshot(ctx context.Context, intentID, batchID string, snap ZoneSnapshot, prev int64, hash string, now int64) (ZoneCommitResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ZoneCommitResult{}, err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `INSERT INTO zones(zone, version, generated_at_ms, checksum, updated_at_ms)
VALUES(?,?,?,?,?)
ON CONFLICT(zone) DO UPDATE SET version=excluded.version, generated_at_ms=excluded.generated_at_ms,
 checksum=excluded.checksum, updated_at_ms=excluded.updated_at_ms`, snap.Zone, snap.Version, snap.GeneratedAtMS, snap.Checksum, now); err != nil {
		return ZoneCommitResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM rrsets WHERE zone=?`, snap.Zone); err != nil {
		return ZoneCommitResult{}, err
	}
	for _, rs := range snap.RRsets {
		records, err := json.Marshal(rs.Records)
		if err != nil {
			return ZoneCommitResult{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO rrsets(zone, owner, rrtype, ttl, records_json, version) VALUES(?,?,?,?,?,?)`,
			snap.Zone, rs.Owner, rs.Type, rs.TTL, string(records), snap.Version); err != nil {
			return ZoneCommitResult{}, err
		}
	}
	invalidated, err := invalidateZoneCachesTx(ctx, tx, snap.Zone)
	if err != nil {
		return ZoneCommitResult{}, err
	}
	derived, err := rebuildDerivedTx(ctx, tx, snap.Zone, snap.Version)
	if err != nil {
		return ZoneCommitResult{}, err
	}
	if err := bumpZoneEpochTx(ctx, tx); err != nil {
		return ZoneCommitResult{}, err
	}
	result := ZoneCommitResult{
		BatchID: batchID, Zone: snap.Zone, Kind: "snapshot", PreviousVersion: prev,
		Version: snap.Version, ChangedRRSets: len(snap.RRsets),
		InvalidatedCacheEntries: invalidated, DerivedRows: derived,
	}
	resultJSON, _ := json.Marshal(result)
	if _, err := tx.ExecContext(ctx, `UPDATE zone_batches SET status='committed', code='', message='', result_json=?, updated_ms=? WHERE batch_id=?`,
		string(resultJSON), now, batchID); err != nil {
		return ZoneCommitResult{}, err
	}
	if err := markIntentTx(ctx, tx, intentID, "committed", "zone snapshot committed", now); err != nil {
		return ZoneCommitResult{}, err
	}
	if err := saveIdempotency(ctx, tx, "zone_snapshot", batchID, hash, "completed", "", "", result, now); err != nil {
		return ZoneCommitResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return ZoneCommitResult{}, err
	}
	return result, nil
}

func (s *Store) SubmitDelta(ctx context.Context, d ZoneDelta, batchID string) (ZoneCommitResult, error) {
	d.Zone = dnsmsg.CanonicalName(d.Zone)
	d.Changes = normalizedChanges(d.Changes)
	if d.Zone == "." || d.Zone == "" {
		return ZoneCommitResult{}, errs.New(errs.EInvalidRequest, "delta zone is empty")
	}
	if d.BaseVersion <= 0 || d.Version <= d.BaseVersion {
		return ZoneCommitResult{}, errs.New(errs.EInvalidRequest, "delta versions require base_version > 0 and version > base_version")
	}
	if d.GeneratedAtMS <= 0 {
		return ZoneCommitResult{}, errs.New(errs.EInvalidRequest, "delta generated_at_ms must be positive")
	}
	if d.Checksum == "" {
		return ZoneCommitResult{}, errs.New(errs.EChecksumRequired, "delta checksum is required")
	}
	if !strings.EqualFold(strings.TrimSpace(d.Checksum), ComputeDeltaChecksum(d)) {
		return ZoneCommitResult{}, errs.New(errs.EChecksumMismatch, "delta checksum does not match canonical content")
	}
	if len(d.Changes) == 0 {
		return ZoneCommitResult{}, errs.New(errs.EInvalidRequest, "delta contains no RRset changes")
	}
	seen := map[string]bool{}
	for _, ch := range d.Changes {
		key := fmt.Sprintf("%s|%d", ch.Owner, ch.Type)
		if seen[key] {
			return ZoneCommitResult{}, errs.New(errs.EInvalidRequest, "duplicate RRset change %s/%s", ch.Owner, ch.Type)
		}
		seen[key] = true
		if ch.Delete {
			if !dnsmsg.NameWithin(ch.Owner, d.Zone) {
				return ZoneCommitResult{}, errs.New(errs.EInvalidRequest, "owner %s is outside zone %s", ch.Owner, d.Zone)
			}
			continue
		}
		if err := validateRRset(d.Zone, RRset{Owner: ch.Owner, Type: ch.Type, TTL: ch.TTL, Records: ch.Records}); err != nil {
			return ZoneCommitResult{}, err
		}
	}
	if batchID == "" && d.DeltaID != "" {
		batchID = d.DeltaID
	}
	if batchID == "" {
		batchID = s.nextID("delta")
	}
	hash := strings.ToLower(d.Checksum)

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if result, hit, err := s.checkZoneIdempotency(ctx, "zone_delta", batchID, hash); err != nil {
		return ZoneCommitResult{}, err
	} else if hit {
		return result, nil
	}
	var currentVersion int64
	if err := s.db.QueryRowContext(ctx, `SELECT version FROM zones WHERE zone=?`, d.Zone).Scan(&currentVersion); err != nil {
		if err == sql.ErrNoRows {
			e := errs.New(errs.EZoneNotFound, "zone %s has no committed snapshot", d.Zone)
			s.recordIdemFailure(ctx, "zone_delta", batchID, hash, e)
			return ZoneCommitResult{}, e
		}
		return ZoneCommitResult{}, err
	}
	if d.BaseVersion != currentVersion {
		e := errs.New(errs.EStaleBase, "delta base version %d does not match committed version %d", d.BaseVersion, currentVersion)
		s.recordIdemFailure(ctx, "zone_delta", batchID, hash, e)
		return ZoneCommitResult{}, e
	}
	now := s.nowMS()
	intentID := s.nextID("intent-zone")
	if err := prepareBatch(ctx, s.db, intentID, batchID, d.Zone, "delta", d.BaseVersion, d.Version, d.Checksum, d, now); err != nil {
		return ZoneCommitResult{}, err
	}
	if s.options.AfterIntent != nil {
		s.options.AfterIntent("delta", batchID)
	}
	result, err := s.commitDelta(ctx, intentID, batchID, d, hash, now)
	if err != nil {
		s.abortBatch(ctx, intentID, batchID, err)
		return ZoneCommitResult{}, err
	}
	return result, nil
}

func (s *Store) commitDelta(ctx context.Context, intentID, batchID string, d ZoneDelta, hash string, now int64) (ZoneCommitResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ZoneCommitResult{}, err
	}
	defer tx.Rollback()
	for _, ch := range d.Changes {
		if ch.Delete {
			if _, err := tx.ExecContext(ctx, `DELETE FROM rrsets WHERE zone=? AND owner=? AND rrtype=?`, d.Zone, ch.Owner, ch.Type); err != nil {
				return ZoneCommitResult{}, err
			}
			continue
		}
		records, err := json.Marshal(ch.Records)
		if err != nil {
			return ZoneCommitResult{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO rrsets(zone, owner, rrtype, ttl, records_json, version) VALUES(?,?,?,?,?,?)
ON CONFLICT(zone, owner, rrtype) DO UPDATE SET ttl=excluded.ttl, records_json=excluded.records_json, version=excluded.version`,
			d.Zone, ch.Owner, ch.Type, ch.TTL, string(records), d.Version); err != nil {
			return ZoneCommitResult{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE zones SET version=?, generated_at_ms=?, checksum=?, updated_at_ms=? WHERE zone=?`,
		d.Version, d.GeneratedAtMS, d.Checksum, now, d.Zone); err != nil {
		return ZoneCommitResult{}, err
	}
	invalidated, err := invalidateZoneCachesTx(ctx, tx, d.Zone)
	if err != nil {
		return ZoneCommitResult{}, err
	}
	derived, err := rebuildDerivedTx(ctx, tx, d.Zone, d.Version)
	if err != nil {
		return ZoneCommitResult{}, err
	}
	if err := bumpZoneEpochTx(ctx, tx); err != nil {
		return ZoneCommitResult{}, err
	}
	result := ZoneCommitResult{
		BatchID: batchID, Zone: d.Zone, Kind: "delta", PreviousVersion: d.BaseVersion,
		Version: d.Version, ChangedRRSets: len(d.Changes),
		InvalidatedCacheEntries: invalidated, DerivedRows: derived,
	}
	resultJSON, _ := json.Marshal(result)
	if _, err := tx.ExecContext(ctx, `UPDATE zone_batches SET status='committed', code='', message='', result_json=?, updated_ms=? WHERE batch_id=?`,
		string(resultJSON), now, batchID); err != nil {
		return ZoneCommitResult{}, err
	}
	if err := markIntentTx(ctx, tx, intentID, "committed", "zone delta committed", now); err != nil {
		return ZoneCommitResult{}, err
	}
	if err := saveIdempotency(ctx, tx, "zone_delta", batchID, hash, "completed", "", "", result, now); err != nil {
		return ZoneCommitResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return ZoneCommitResult{}, err
	}
	return result, nil
}

func (s *Store) checkZoneIdempotency(ctx context.Context, kind, batchID, hash string) (ZoneCommitResult, bool, error) {
	if batchID == "" {
		return ZoneCommitResult{}, false, nil
	}
	rec, err := loadIdempotency(ctx, s.db, kind, batchID)
	if err != nil {
		return ZoneCommitResult{}, false, err
	}
	if rec == nil {
		return ZoneCommitResult{}, false, nil
	}
	if rec.ContentHash != hash {
		return ZoneCommitResult{}, false, errs.New(errs.EConflictingRequest, "zone batch id %q was already used with different content", batchID)
	}
	if rec.Code != "" {
		return ZoneCommitResult{}, false, idempotentError(rec)
	}
	var result ZoneCommitResult
	if err := json.Unmarshal([]byte(rec.ResultJSON), &result); err != nil {
		return ZoneCommitResult{}, false, fmt.Errorf("decode idempotent zone result: %w", err)
	}
	result.Idempotent = true
	return result, true, nil
}

func (s *Store) recordIdemFailure(ctx context.Context, kind, key, hash string, failure error) {
	if key == "" {
		return
	}
	now := s.nowMS()
	_, _ = s.db.ExecContext(ctx, `INSERT INTO idempotency(kind, request_key, content_hash, status, code, message, result_json, created_ms, updated_ms)
VALUES(?,?,?,'rejected',?,?,'',?,?)
ON CONFLICT(kind, request_key) DO NOTHING`, kind, key, hash, string(errs.CodeOf(failure)), errs.MessageOf(failure), now, now)
}

func (s *Store) abortBatch(ctx context.Context, intentID, batchID string, failure error) {
	now := s.nowMS()
	msg := ""
	if failure != nil {
		msg = failure.Error()
	}
	_, _ = s.db.ExecContext(ctx, `UPDATE zone_batches SET status='aborted', code=?, message=?, updated_ms=? WHERE batch_id=? AND status='pending'`,
		string(errs.CodeOf(failure)), msg, now, batchID)
	_, _ = s.db.ExecContext(ctx, `UPDATE intention_log SET status='aborted', detail=?, updated_ms=? WHERE intent_id=? AND status='pending'`,
		msg, now, intentID)
}

func invalidateZoneCachesTx(ctx context.Context, tx *sql.Tx, zone string) (int64, error) {
	suffix := "%." + zone
	if zone == "." {
		suffix = "%"
	}
	// Entries written before the zone existed have an empty zone column. They
	// are also downstream views and must not survive the zone's first commit.
	where := `zone=? OR (zone='' AND (qname=? OR qname LIKE ?)) OR id IN (SELECT cache_id FROM cache_deps WHERE zone=?)`
	args := []any{zone, zone, suffix, zone}
	var count int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM cache_entries WHERE `+where, args...).Scan(&count); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM cache_entries WHERE `+where, args...); err != nil {
		return 0, err
	}
	return count, nil
}

type derivedRef struct {
	Zone        string      `json:"zone"`
	ZoneVersion int64       `json:"zone_version"`
	SourceOwner string      `json:"source_owner"`
	SourceType  dnsmsg.Type `json:"source_type"`
	TTL         uint32      `json:"ttl"`
	RecordCount int         `json:"record_count"`
}

func rebuildDerivedTx(ctx context.Context, tx *sql.Tx, zone string, version int64) (int, error) {
	if _, err := tx.ExecContext(ctx, `DELETE FROM zone_derived WHERE zone=?`, zone); err != nil {
		return 0, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT owner, rrtype, ttl, records_json FROM rrsets WHERE zone=? ORDER BY owner, rrtype`, zone)
	if err != nil {
		return 0, err
	}
	type rrrow struct {
		owner   string
		typ     dnsmsg.Type
		ttl     uint32
		records string
	}
	var all []rrrow
	for rows.Next() {
		var r rrrow
		if err := rows.Scan(&r.owner, &r.typ, &r.ttl, &r.records); err != nil {
			rows.Close()
			return 0, err
		}
		all = append(all, r)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	count := 0
	for _, r := range all {
		var records []dnsmsg.RData
		if err := json.Unmarshal([]byte(r.records), &records); err != nil {
			return 0, err
		}
		refs, _ := json.Marshal(derivedRef{Zone: zone, ZoneVersion: version, SourceOwner: r.owner, SourceType: r.typ, TTL: r.ttl, RecordCount: len(records)})
		for _, kind := range []string{"signature_domain", "negative_range"} {
			key := fmt.Sprintf("%s|%s|%d", zone, r.owner, r.typ)
			if _, err := tx.ExecContext(ctx, `INSERT INTO zone_derived(zone, kind, key, owner, rrtype, source_owner, source_type, refs_json, version)
VALUES(?,?,?,?,?,?,?,?,?)`, zone, kind, key, r.owner, r.typ, r.owner, r.typ, string(refs), version); err != nil {
				return 0, err
			}
			count++
		}
	}
	return count, nil
}

func (s *Store) ZoneState(ctx context.Context, zone string) (ZoneState, error) {
	zone = dnsmsg.CanonicalName(zone)
	var st ZoneState
	err := s.db.QueryRowContext(ctx, `SELECT zone, version, generated_at_ms, checksum, updated_at_ms FROM zones WHERE zone=?`, zone).
		Scan(&st.Zone, &st.Version, &st.GeneratedAtMS, &st.Checksum, &st.UpdatedAtMS)
	if err == sql.ErrNoRows {
		return ZoneState{}, errs.New(errs.EZoneNotFound, "zone %s is not committed", zone)
	}
	if err != nil {
		return ZoneState{}, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT owner, rrtype, ttl, records_json FROM rrsets WHERE zone=? ORDER BY owner, rrtype`, zone)
	if err != nil {
		return ZoneState{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var rs RRset
		var records string
		if err := rows.Scan(&rs.Owner, &rs.Type, &rs.TTL, &records); err != nil {
			return ZoneState{}, err
		}
		if err := json.Unmarshal([]byte(records), &rs.Records); err != nil {
			return ZoneState{}, err
		}
		st.RRsets = append(st.RRsets, rs)
	}
	return st, rows.Err()
}

func (s *Store) ListZones(ctx context.Context) ([]ZoneState, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT zone, version, generated_at_ms, checksum, updated_at_ms FROM zones ORDER BY zone`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ZoneState
	for rows.Next() {
		var z ZoneState
		if err := rows.Scan(&z.Zone, &z.Version, &z.GeneratedAtMS, &z.Checksum, &z.UpdatedAtMS); err != nil {
			return nil, err
		}
		out = append(out, z)
	}
	return out, rows.Err()
}

func (s *Store) DerivedRows(ctx context.Context, zone string) ([]DerivedRow, error) {
	zone = dnsmsg.CanonicalName(zone)
	rows, err := s.db.QueryContext(ctx, `SELECT kind, key, owner, rrtype, source_owner, source_type, refs_json, version FROM zone_derived WHERE zone=? ORDER BY kind, key`, zone)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DerivedRow
	for rows.Next() {
		var d DerivedRow
		if err := rows.Scan(&d.Kind, &d.Key, &d.Owner, &d.Type, &d.SourceOwner, &d.SourceType, &d.Refs, &d.Version); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
