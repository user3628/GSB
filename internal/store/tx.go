package store

import (
	"database/sql"
	"fmt"
)

// snapshotResult reports the version produced by a state transition.
type snapshotResult struct {
	Version int64
	Digest  string
	Changed bool
}

// begin starts an immediate write transaction so writers are serialized at the
// database level and the per-ticket mutex only reduces contention.
func (s *Store) begin() (*sql.Tx, error) {
	return s.db.Begin()
}

// appendSnapshotTx inserts the next version for a tenant inside tx. It reads
// the tenant's whole state through the same transaction so the digest always
// reflects the uncommitted transition atomically.
func (s *Store) appendSnapshotTx(tx *sql.Tx, tenantID, reason string, atNS int64) (snapshotResult, error) {
	tickets, err := s.ticketsTx(tx, tenantID)
	if err != nil {
		return snapshotResult{}, err
	}
	redemptions, err := s.redemptionsTx(tx, tenantID)
	if err != nil {
		return snapshotResult{}, err
	}
	digest := canonicalDigest(tickets, redemptions)

	var prev sql.NullString
	if err := tx.QueryRow(
		`SELECT digest FROM snapshots WHERE tenant_id=? ORDER BY version DESC LIMIT 1`,
		tenantID).Scan(&prev); err != nil && err != sql.ErrNoRows {
		return snapshotResult{}, err
	}
	changed := !prev.Valid || prev.String != digest

	var version int64
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(version),0)+1 FROM snapshots WHERE tenant_id=?`,
		tenantID).Scan(&version); err != nil {
		return snapshotResult{}, err
	}
	changedInt := 0
	if changed {
		changedInt = 1
	}
	if _, err := tx.Exec(
		`INSERT INTO snapshots(tenant_id, version, digest, changed, reason, at_ns)
		 VALUES(?,?,?,?,?,?)`,
		tenantID, version, digest, changedInt, reason, atNS); err != nil {
		return snapshotResult{}, err
	}
	return snapshotResult{Version: version, Digest: digest, Changed: changed}, nil
}

func (s *Store) ticketsTx(tx *sql.Tx, tenantID string) ([]Ticket, error) {
	rows, err := tx.Query(
		`SELECT ticket_id, tenant_id, sp_id, user_id, raw_ticket, iat, exp, status
		 FROM tickets WHERE tenant_id=? ORDER BY ticket_id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Ticket
	for rows.Next() {
		var t Ticket
		if err := rows.Scan(&t.ID, &t.TenantID, &t.SPID, &t.UserID, &t.Raw,
			&t.IssuedAt, &t.ExpiresAt, &t.Status); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) redemptionsTx(tx *sql.Tx, tenantID string) ([]Redemption, error) {
	rows, err := tx.Query(
		`SELECT ticket_id, request_seq, sp_id, user_id, at_ns, snapshot_version
		 FROM redemptions WHERE tenant_id=? ORDER BY ticket_id, request_seq`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Redemption
	for rows.Next() {
		var r Redemption
		if err := rows.Scan(&r.TicketID, &r.Seq, &r.SPID, &r.UserID, &r.AtNS, &r.Version); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Issue inserts a new ticket and its first (or next) snapshot in one transaction.
func (s *Store) Issue(t Ticket, atNS int64) (snapshotResult, error) {
	tx, err := s.begin()
	if err != nil {
		return snapshotResult{}, err
	}
	defer rollback(tx)
	if err := s.insertTicketTx(tx, t); err != nil {
		return snapshotResult{}, err
	}
	res, err := s.appendSnapshotTx(tx, t.TenantID, "ISSUE", atNS)
	if err != nil {
		return snapshotResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return snapshotResult{}, fmt.Errorf("commit issue: %w", err)
	}
	return res, nil
}

// Redeem performs the one-time consumption of a ticket. The status transition,
// redemption row and the next snapshot are all written in one transaction.
// Returning sql.ErrNoRows semantics: the caller first reads the row.
type RedeemOutcome struct {
	Version int64
	Digest  string
	Changed bool
}

func (s *Store) Redeem(t *Ticket, seq int64, atNS int64) (RedeemOutcome, error) {
	tx, err := s.begin()
	if err != nil {
		return RedeemOutcome{}, err
	}
	defer rollback(tx)

	var status string
	if err := tx.QueryRow(
		`SELECT status FROM tickets WHERE ticket_id=?`, t.ID).Scan(&status); err != nil {
		return RedeemOutcome{}, err
	}
	if status != StatusValid {
		// Lost the race: do not mutate anything.
		return RedeemOutcome{Version: -1}, nil
	}
	if _, err := tx.Exec(
		`UPDATE tickets SET status=? WHERE ticket_id=? AND status=?`,
		StatusRedeemed, t.ID, StatusValid); err != nil {
		return RedeemOutcome{}, err
	}
	res, err := s.appendSnapshotTx(tx, t.TenantID, "REDEEM", atNS)
	if err != nil {
		return RedeemOutcome{}, err
	}
	if _, err := tx.Exec(
		`INSERT INTO redemptions(ticket_id, tenant_id, request_seq, sp_id, user_id, at_ns, snapshot_version)
		 VALUES(?,?,?,?,?,?,?)`,
		t.ID, t.TenantID, seq, t.SPID, t.UserID, atNS, res.Version); err != nil {
		return RedeemOutcome{}, err
	}
	if err := tx.Commit(); err != nil {
		return RedeemOutcome{}, fmt.Errorf("commit redeem: %w", err)
	}
	return RedeemOutcome{Version: res.Version, Digest: res.Digest, Changed: res.Changed}, nil
}

// Expire marks a ticket expired and records the snapshot in one transaction.
func (s *Store) Expire(t *Ticket, atNS int64) (snapshotResult, error) {
	tx, err := s.begin()
	if err != nil {
		return snapshotResult{}, err
	}
	defer rollback(tx)
	if _, err := tx.Exec(
		`UPDATE tickets SET status=? WHERE ticket_id=? AND status=?`,
		StatusExpired, t.ID, StatusValid); err != nil {
		return snapshotResult{}, err
	}
	res, err := s.appendSnapshotTx(tx, t.TenantID, "EXPIRE", atNS)
	if err != nil {
		return snapshotResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return snapshotResult{}, fmt.Errorf("commit expire: %w", err)
	}
	return res, nil
}

// InsertConflict records a losing or failed validation attempt.
func (s *Store) InsertConflict(c Conflict) error {
	_, err := s.db.Exec(
		`INSERT INTO conflicts(tenant_id, ticket_id, request_seq, code, at_ns)
		 VALUES(?,?,?,?,?)`, c.TenantID, c.TicketID, c.Seq, c.Code, c.AtNS)
	return err
}

// InjectGap deletes the highest snapshot of a tenant to create a version gap.
// It is driven purely through the fault-injection parameters, never by normal
// traffic, and simulates a corrupted / partially lost snapshot chain.
func (s *Store) InjectGap(tenantID string) (int64, error) {
	var version int64
	if err := s.db.QueryRow(
		`SELECT COALESCE(MAX(version),0) FROM snapshots WHERE tenant_id=?`,
		tenantID).Scan(&version); err != nil {
		return 0, err
	}
	if version == 0 {
		return 0, fmt.Errorf("tenant %q has no snapshots to gap", tenantID)
	}
	if _, err := s.db.Exec(
		`DELETE FROM snapshots WHERE tenant_id=? AND version=?`, tenantID, version); err != nil {
		return 0, err
	}
	return version, nil
}

// ChainStatus describes the verified integrity of a tenant's snapshot chain.
type ChainStatus struct {
	TenantID    string
	Contiguous  bool
	HeadVersion int64
	LastContig  int64 // highest version reachable from v1 without a gap
	GapAt       int64 // first missing version, 0 when contiguous
	HeadMatches bool  // head snapshot digest equals live state digest
	Versions    []int64
}

// VerifyChain checks version contiguity (v1..vN with no gaps) and that the
// head digest matches the current live state, detecting whole-content
// replacements even when version numbers keep increasing.
func (s *Store) VerifyChain(tenantID string) (ChainStatus, error) {
	snaps, err := s.ListSnapshots(tenantID)
	if err != nil {
		return ChainStatus{}, err
	}
	st := ChainStatus{TenantID: tenantID, Contiguous: true, HeadMatches: true}
	if len(snaps) == 0 {
		return st, nil
	}
	for _, sp := range snaps {
		st.Versions = append(st.Versions, sp.Version)
	}
	st.HeadVersion = snaps[len(snaps)-1].Version
	// Contiguity: expected sequence 1,2,3,...
	for i, sp := range snaps {
		want := int64(i + 1)
		if sp.Version != want {
			st.Contiguous = false
			st.GapAt = want
			break
		}
		st.LastContig = sp.Version
	}
	// Content check on the head snapshot vs live durable state.
	live, err := s.LiveDigest(tenantID)
	if err != nil {
		return st, err
	}
	st.HeadMatches = snaps[len(snaps)-1].Digest == live
	return st, nil
}

// Rollback truncates a tenant's snapshot chain to its last contiguous version
// and then appends a fresh snapshot built from the live state, restoring a
// contiguous chain while preserving every ticket and redemption row.
func (s *Store) Rollback(tenantID string, lastContig int64, atNS int64) (snapshotResult, error) {
	tx, err := s.begin()
	if err != nil {
		return snapshotResult{}, err
	}
	defer rollback(tx)
	if _, err := tx.Exec(
		`DELETE FROM snapshots WHERE tenant_id=? AND version>?`,
		tenantID, lastContig); err != nil {
		return snapshotResult{}, err
	}
	res, err := s.appendSnapshotTx(tx, tenantID, "ROLLBACK", atNS)
	if err != nil {
		return snapshotResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return snapshotResult{}, fmt.Errorf("commit rollback: %w", err)
	}
	return res, nil
}

// rollback is a no-op after a successful Commit.
func rollback(tx *sql.Tx) { _ = tx.Rollback() }
