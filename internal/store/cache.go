package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"example.com/gsb/dnscache/internal/dnsmsg"
	"example.com/gsb/dnscache/internal/errs"
)

const (
	CachePositive = "positive"
	CacheNXDomain = "nxdomain"
	CacheNODATA   = "nodata"
)

func cacheKey(name string, typ dnsmsg.Type) string {
	return fmt.Sprintf("%s|%d", dnsmsg.CanonicalName(name), typ)
}

func normalizePayload(p CachePayload) CachePayload {
	out := p
	out.Answers = dnsmsg.CloneRRs(p.Answers)
	out.Authorities = dnsmsg.CloneRRs(p.Authorities)
	out.Additionals = dnsmsg.CloneRRs(p.Additionals)
	for _, group := range [][]dnsmsg.RR{out.Answers, out.Authorities, out.Additionals} {
		for i := range group {
			group[i].Name = dnsmsg.CanonicalName(group[i].Name)
			if group[i].Class == 0 {
				group[i].Class = 1
			}
			switch group[i].Type {
			case dnsmsg.TypeCNAME, dnsmsg.TypeNS, dnsmsg.TypeMX:
				group[i].Data.Target = dnsmsg.CanonicalName(group[i].Data.Target)
			case dnsmsg.TypeSOA:
				if group[i].Data.SOA != nil {
					group[i].Data.SOA.MName = dnsmsg.CanonicalName(group[i].Data.SOA.MName)
					group[i].Data.SOA.RName = dnsmsg.CanonicalName(group[i].Data.SOA.RName)
				}
			}
		}
	}
	return out
}

func cacheStatus(p CachePayload, qtype dnsmsg.Type) (string, error) {
	switch p.RCode {
	case dnsmsg.RCodeNXDomain:
		return CacheNXDomain, nil
	case dnsmsg.RCodeSuccess:
		for _, rr := range p.Answers {
			if rr.Type == qtype {
				return CachePositive, nil
			}
		}
		// A CNAME chain that ends without the requested RRset is NODATA, even
		// though the answer section contains CNAME records for negative-cache
		// interpretation purposes.
		return CacheNODATA, nil
	default:
		return "", errs.New(errs.EInvalidRequest, "rcode %d is not cacheable", p.RCode)
	}
}

func negativeTTLMillis(p CachePayload) (int64, error) {
	var soa *dnsmsg.RR
	for i := range p.Authorities {
		if p.Authorities[i].Type == dnsmsg.TypeSOA && p.Authorities[i].Data.SOA != nil {
			soa = &p.Authorities[i]
			break
		}
	}
	if soa == nil {
		for i := range p.Answers {
			if p.Answers[i].Type == dnsmsg.TypeSOA && p.Answers[i].Data.SOA != nil {
				soa = &p.Answers[i]
				break
			}
		}
	}
	if soa == nil {
		return 0, errs.New(errs.ENegativeTTLUndefined, "negative response lacks an SOA record; RFC 2308 negative TTL cannot be derived (answer_count=%d authority_count=%d)", len(p.Answers), len(p.Authorities))
	}
	ttl := soa.TTL
	if soa.Data.SOA.Minimum < ttl {
		ttl = soa.Data.SOA.Minimum
	}
	return int64(ttl) * 1000, nil
}

func positiveTTLMillis(p CachePayload) int64 {
	if len(p.Answers) == 0 {
		return 0
	}
	minTTL := p.Answers[0].TTL
	for _, rr := range p.Answers[1:] {
		if rr.TTL < minTTL {
			minTTL = rr.TTL
		}
	}
	return int64(minTTL) * 1000
}

// WriteCache serializes cache replacement behind the single-writer mutex. An
// intention is durably committed before the cache transaction begins, so a
// process crash can never leave a cache row without a discoverable intent.
func (s *Store) WriteCache(ctx context.Context, w CacheWrite) (CacheWriteResult, error) {
	if !dnsmsg.SupportedType(w.QType) {
		return CacheWriteResult{}, errs.New(errs.EUnsupportedType, "query type %s cannot be cached", w.QType)
	}
	w.QName = dnsmsg.CanonicalName(w.QName)
	if w.QName == "." || w.QName == "" {
		return CacheWriteResult{}, errs.New(errs.EInvalidRequest, "cache query name is empty")
	}
	if w.SourceType == 0 {
		w.SourceType = w.QType
	}
	w.Payload = normalizePayload(w.Payload)
	status, err := cacheStatus(w.Payload, w.QType)
	if err != nil {
		return CacheWriteResult{}, err
	}
	ttlMS := w.TTLMillis
	if status != CachePositive {
		ttlMS, err = negativeTTLMillis(w.Payload)
		if err != nil {
			return CacheWriteResult{}, err
		}
		w.SourceType = dnsmsg.TypeSOA
	} else if ttlMS <= 0 {
		ttlMS = positiveTTLMillis(w.Payload)
	}
	if w.Zone != "" {
		w.Zone = dnsmsg.CanonicalName(w.Zone)
	}
	hash := payloadHash(struct {
		QName               string
		QType               dnsmsg.Type
		SourceType          dnsmsg.Type
		Zone                string
		ZoneVersion         int64
		TTLMillis           int64
		ExpectedZone        string
		ExpectedZoneVersion int64
		RequireZoneStable   bool
		Payload             CachePayload
	}{w.QName, w.QType, w.SourceType, w.Zone, w.ZoneVersion, ttlMS, w.ExpectedZone, w.ExpectedZoneVersion, w.RequireZoneStable, w.Payload})

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if w.QueryID != "" {
		rec, err := loadIdempotency(ctx, s.db, "cache_write", w.QueryID)
		if err != nil {
			return CacheWriteResult{}, err
		}
		if rec != nil {
			if rec.ContentHash != hash {
				return CacheWriteResult{}, errs.New(errs.EConflictingRequest, "cache query id %q was already used with different content", w.QueryID)
			}
			if rec.Code != "" {
				return CacheWriteResult{}, idempotentError(rec)
			}
			var result CacheWriteResult
			if err := json.Unmarshal([]byte(rec.ResultJSON), &result); err != nil {
				return CacheWriteResult{}, fmt.Errorf("decode idempotent cache result: %w", err)
			}
			result.Idempotent = true
			return result, nil
		}
	}

	now := s.nowMS()
	intentID := s.nextID("intent-cache")
	if err := s.writeIntentPending(ctx, intentID, "cache_write", cacheKey(w.QName, w.QType), hash); err != nil {
		return CacheWriteResult{}, fmt.Errorf("write cache intent: %w", err)
	}
	if s.options.AfterIntent != nil {
		s.options.AfterIntent("cache_write", cacheKey(w.QName, w.QType))
	}
	intentCommitted := false
	defer func() {
		if !intentCommitted {
			_ = s.markIntent(context.Background(), intentID, "aborted", "cache transaction did not commit")
		}
	}()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		_ = s.markIntent(ctx, intentID, "aborted", err.Error())
		return CacheWriteResult{}, err
	}
	defer tx.Rollback()

	result := CacheWriteResult{CacheKey: cacheKey(w.QName, w.QType), Status: status, TTLMillis: ttlMS, WrittenMS: now}
	if err := s.resolveWriteZone(ctx, tx, &w); err != nil {
		_ = s.markIntent(ctx, intentID, "aborted", err.Error())
		return CacheWriteResult{}, err
	}
	payloadJSON, err := json.Marshal(w.Payload)
	if err != nil {
		return CacheWriteResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO cache_entries(cache_key, qname, qtype, status, source_type, zone, zone_version, written_ms, ttl_ms, payload_json, intent_id, created_ms)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(cache_key) DO UPDATE SET qname=excluded.qname, qtype=excluded.qtype, status=excluded.status,
 source_type=excluded.source_type, zone=excluded.zone, zone_version=excluded.zone_version,
 written_ms=excluded.written_ms, ttl_ms=excluded.ttl_ms, payload_json=excluded.payload_json,
 intent_id=excluded.intent_id, created_ms=excluded.created_ms`,
		result.CacheKey, w.QName, w.QType, status, w.SourceType, w.Zone, w.ZoneVersion, now, ttlMS, string(payloadJSON), intentID, now); err != nil {
		return CacheWriteResult{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT id FROM cache_entries WHERE cache_key=?`, result.CacheKey).Scan(&result.CacheID); err != nil {
		return CacheWriteResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM cache_deps WHERE cache_id=?`, result.CacheID); err != nil {
		return CacheWriteResult{}, err
	}
	deps, err := s.cacheDependencies(ctx, tx, w)
	if err != nil {
		return CacheWriteResult{}, err
	}
	for _, dep := range deps {
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO cache_deps(cache_id, zone, zone_version, owner, rrtype) VALUES(?,?,?,?,?)`,
			result.CacheID, dep.Zone, dep.Version, dep.Owner, dep.Type); err != nil {
			return CacheWriteResult{}, err
		}
	}
	if err := markIntentTx(ctx, tx, intentID, "committed", "cache row committed", now); err != nil {
		return CacheWriteResult{}, err
	}
	if err := saveIdempotency(ctx, tx, "cache_write", w.QueryID, hash, "completed", "", "", result, now); err != nil {
		return CacheWriteResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return CacheWriteResult{}, err
	}
	intentCommitted = true
	return result, nil
}

type cacheDep struct {
	Zone    string
	Version int64
	Owner   string
	Type    dnsmsg.Type
}

func (s *Store) resolveWriteZone(ctx context.Context, tx *sql.Tx, w *CacheWrite) error {
	if w.RequireEpochStable {
		var epoch int64
		if err := tx.QueryRowContext(ctx, `SELECT value FROM state WHERE key='zone_epoch'`).Scan(&epoch); err != nil {
			return err
		}
		if epoch != w.ExpectedZoneEpoch {
			return errs.New(errs.EStaleBase, "a zone commit occurred while upstream query was in flight (expected epoch %d, now %d)", w.ExpectedZoneEpoch, epoch)
		}
	}
	zone, version := w.Zone, w.ZoneVersion
	if w.RequireZoneStable {
		z, v, err := findZone(ctx, tx, w.QName)
		if err != nil {
			return err
		}
		if z != w.ExpectedZone || v != w.ExpectedZoneVersion {
			return errs.New(errs.EStaleBase, "zone changed while upstream query was in flight (expected %s@%d, now %s@%d)", w.ExpectedZone, w.ExpectedZoneVersion, z, v)
		}
		zone, version = z, v
	} else if zone == "" {
		z, v, err := findZone(ctx, tx, w.QName)
		if err != nil {
			return err
		}
		zone, version = z, v
	} else {
		err := tx.QueryRowContext(ctx, `SELECT version FROM zones WHERE zone=?`, zone).Scan(&version)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		if err == sql.ErrNoRows {
			version = 0
		}
	}
	w.Zone, w.ZoneVersion = zone, version
	return nil
}

func findZone(ctx context.Context, q queryer, name string) (string, int64, error) {
	rows, err := q.QueryContext(ctx, `SELECT zone, version FROM zones`)
	if err != nil {
		return "", 0, err
	}
	defer rows.Close()
	var bestZone string
	var bestVersion int64
	for rows.Next() {
		var zone string
		var version int64
		if err := rows.Scan(&zone, &version); err != nil {
			return "", 0, err
		}
		if dnsmsg.NameWithin(name, zone) && len(zone) > len(bestZone) {
			bestZone, bestVersion = zone, version
		}
	}
	return bestZone, bestVersion, rows.Err()
}

func (s *Store) cacheDependencies(ctx context.Context, tx *sql.Tx, w CacheWrite) ([]cacheDep, error) {
	names := map[string]dnsmsg.Type{}
	if w.Zone != "" {
		names[w.QName] = w.QType
	}
	for _, group := range [][]dnsmsg.RR{w.Payload.Answers, w.Payload.Authorities, w.Payload.Additionals} {
		for _, rr := range group {
			names[rr.Name] = rr.Type
			switch rr.Type {
			case dnsmsg.TypeCNAME, dnsmsg.TypeNS, dnsmsg.TypeMX:
				if rr.Data.Target != "" {
					names[rr.Data.Target] = rr.Type
				}
			}
		}
	}
	seen := map[string]bool{}
	var deps []cacheDep
	for owner, typ := range names {
		zone, version, err := findZone(ctx, tx, owner)
		if err != nil {
			return nil, err
		}
		if zone == "" {
			continue
		}
		key := zone + "|" + owner + "|" + fmt.Sprint(typ)
		if seen[key] {
			continue
		}
		seen[key] = true
		deps = append(deps, cacheDep{Zone: zone, Version: version, Owner: owner, Type: typ})
	}
	return deps, nil
}

// Lookup reads cache metadata and every referenced zone version in a single
// read transaction. A reader therefore sees either a complete old commit or a
// complete new commit, never a mixed zone/cache state.
func (s *Store) Lookup(ctx context.Context, name string, typ dnsmsg.Type) (CacheLookup, error) {
	name = dnsmsg.CanonicalName(name)
	now := s.nowMS()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return CacheLookup{}, err
	}
	defer tx.Rollback()
	var entry CacheEntry
	var payloadJSON string
	err = tx.QueryRowContext(ctx, `SELECT id, qname, qtype, status, source_type, zone, zone_version, written_ms, ttl_ms, payload_json
FROM cache_entries WHERE cache_key=?`, cacheKey(name, typ)).
		Scan(&entry.CacheID, &entry.QName, &entry.QType, &entry.Status, &entry.SourceType, &entry.Zone, &entry.ZoneVersion, &entry.WrittenMS, &entry.TTLMillis, &payloadJSON)
	if err == sql.ErrNoRows {
		return CacheLookup{Hit: false, Reason: "not_found"}, nil
	}
	if err != nil {
		return CacheLookup{}, err
	}
	remaining := entry.WrittenMS + entry.TTLMillis - now
	if remaining <= 0 {
		_ = tx.Commit()
		s.deleteExpiredCacheID(context.Background(), entry.CacheID, now)
		return CacheLookup{Hit: false, Reason: "expired"}, nil
	}
	if entry.Zone != "" {
		var current int64
		err := tx.QueryRowContext(ctx, `SELECT version FROM zones WHERE zone=?`, entry.Zone).Scan(&current)
		if err != nil || current != entry.ZoneVersion {
			return CacheLookup{Hit: false, Reason: "zone_version_changed"}, nil
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT d.zone, d.zone_version, z.version FROM cache_deps d JOIN zones z ON z.zone=d.zone WHERE d.cache_id=?`, entry.CacheID)
	if err != nil {
		return CacheLookup{}, err
	}
	for rows.Next() {
		var zone string
		var depVersion, current int64
		if err := rows.Scan(&zone, &depVersion, &current); err != nil {
			rows.Close()
			return CacheLookup{}, err
		}
		if depVersion != current {
			rows.Close()
			return CacheLookup{Hit: false, Reason: "dependency_zone_version_changed"}, nil
		}
	}
	if err := rows.Close(); err != nil {
		return CacheLookup{}, err
	}
	if err := json.Unmarshal([]byte(payloadJSON), &entry.Payload); err != nil {
		return CacheLookup{}, fmt.Errorf("decode cache payload: %w", err)
	}
	elapsed := now - entry.WrittenMS
	entry.RemainingMS = remaining
	entry.Payload.Answers = dnsmsg.DecrementRRs(entry.Payload.Answers, elapsed)
	entry.Payload.Authorities = dnsmsg.DecrementRRs(entry.Payload.Authorities, elapsed)
	entry.Payload.Additionals = dnsmsg.DecrementRRs(entry.Payload.Additionals, elapsed)
	if err := tx.Commit(); err != nil {
		return CacheLookup{}, err
	}
	return CacheLookup{Hit: true, Entry: &entry}, nil
}

func (s *Store) deleteExpiredCacheID(ctx context.Context, id, now int64) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, _ = s.db.ExecContext(ctx, `DELETE FROM cache_entries WHERE id=? AND written_ms+ttl_ms<=?`, id, now)
}

func (s *Store) CleanupExpired(ctx context.Context) (int64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now := s.nowMS()
	res, err := s.db.ExecContext(ctx, `DELETE FROM cache_entries WHERE written_ms+ttl_ms<=?`, now)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (s *Store) ListCache(ctx context.Context, includeExpired bool) ([]CacheEntry, error) {
	query := `SELECT id, qname, qtype, status, source_type, zone, zone_version, written_ms, ttl_ms, payload_json FROM cache_entries`
	args := []any{}
	if !includeExpired {
		query += ` WHERE written_ms+ttl_ms>?`
		args = append(args, s.nowMS())
	}
	query += ` ORDER BY id`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	now := s.nowMS()
	var out []CacheEntry
	for rows.Next() {
		var e CacheEntry
		var payload string
		if err := rows.Scan(&e.CacheID, &e.QName, &e.QType, &e.Status, &e.SourceType, &e.Zone, &e.ZoneVersion, &e.WrittenMS, &e.TTLMillis, &payload); err != nil {
			return nil, err
		}
		e.RemainingMS = e.WrittenMS + e.TTLMillis - now
		if err := json.Unmarshal([]byte(payload), &e.Payload); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) DeleteCache(ctx context.Context, name string, typ dnsmsg.Type) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.ExecContext(ctx, `DELETE FROM cache_entries WHERE cache_key=?`, cacheKey(name, typ))
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) CacheDependencies(ctx context.Context, cacheID int64) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT zone || '|' || owner || '|' || rrtype || '|' || zone_version FROM cache_deps WHERE cache_id=? ORDER BY zone, owner, rrtype`, cacheID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var dep string
		if err := rows.Scan(&dep); err != nil {
			return nil, err
		}
		out = append(out, dep)
	}
	return out, rows.Err()
}

// ZoneForName returns the longest committed enclosing zone and its version.
// An empty zone means no authoritative metadata has been committed yet.
func (s *Store) ZoneForName(ctx context.Context, name string) (string, int64, error) {
	return findZone(ctx, s.db, dnsmsg.CanonicalName(name))
}

// ZoneEpoch is a monotonically increasing SQLite fact bumped in every zone
// commit transaction. Resolver cache writes compare it before insertion.
func (s *Store) ZoneEpoch(ctx context.Context) (int64, error) {
	var epoch int64
	err := s.db.QueryRowContext(ctx, `SELECT value FROM state WHERE key='zone_epoch'`).Scan(&epoch)
	return epoch, err
}

func bumpZoneEpochTx(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO state(key, value) VALUES('zone_epoch', 1)
ON CONFLICT(key) DO UPDATE SET value=value+1`)
	return err
}
