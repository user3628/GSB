// Package store provides the durable SQLite-backed state of the engine.
//
// A single pooled connection is used: writes are serialized by SQLite itself,
// while the engine layers per-ticket mutexes and a weighted semaphore on top.
// All state transitions (issue / redeem / expire) are committed together with
// their snapshot row in one transaction, so a forced process kill can never
// leave a ticket state without its matching snapshot version.
package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	_ "modernc.org/sqlite"
)

// Ticket statuses.
const (
	StatusValid    = "VALID"
	StatusRedeemed = "REDEEMED"
	StatusExpired  = "EXPIRED"
)

// Ticket is the persisted service ticket record.
type Ticket struct {
	ID        string
	TenantID  string
	SPID      string
	UserID    string
	Raw       string
	IssuedAt  int64
	ExpiresAt int64
	Status    string
}

// Redemption is the one-time consumption record of a ticket.
type Redemption struct {
	TicketID string `json:"ticketId"`
	Seq      int64  `json:"requestSeq"`
	SPID     string `json:"spId"`
	UserID   string `json:"userId"`
	AtNS     int64  `json:"atNs"`
	Version  int64  `json:"snapshotVersion"`
}

// Snapshot is a versioned, content-addressed tenant state snapshot.
type Snapshot struct {
	TenantID string `json:"tenantId"`
	Version  int64  `json:"version"`
	Digest   string `json:"contentDigest"`
	Changed  bool   `json:"changed"`
	Reason   string `json:"reason"`
	AtNS     int64  `json:"atNs"`
}

// Conflict records every losing (or otherwise notable) validation attempt.
type Conflict struct {
	TenantID string `json:"tenantId"`
	TicketID string `json:"ticketId"`
	Seq      int64  `json:"requestSeq"`
	Code     string `json:"code"`
	AtNS     int64  `json:"atNs"`
}

// Store wraps the SQLite database handle.
type Store struct {
	db *sql.DB
}

// Open opens (or creates) the database and applies the schema idempotently.
func Open(path string) (*Store, error) {
	dsn := "file:" + path +
		"?_pragma=journal_mode(WAL)" +
		"&_pragma=synchronous(FULL)" +
		"&_pragma=busy_timeout(5000)" +
		"&_pragma=foreign_keys(ON)" +
		"&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One connection removes SQLITE_BUSY races and makes transactions strict.
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS tenants (
			tenant_id TEXT PRIMARY KEY,
			name      TEXT NOT NULL,
			secret    TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS tickets (
			ticket_id  TEXT PRIMARY KEY,
			tenant_id  TEXT NOT NULL,
			sp_id      TEXT NOT NULL,
			user_id    TEXT NOT NULL,
			raw_ticket TEXT NOT NULL,
			iat        INTEGER NOT NULL,
			exp        INTEGER NOT NULL,
			status     TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS redemptions (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			ticket_id  TEXT NOT NULL,
			tenant_id  TEXT NOT NULL,
			request_seq INTEGER NOT NULL,
			sp_id      TEXT NOT NULL,
			user_id    TEXT NOT NULL,
			at_ns      INTEGER NOT NULL,
			snapshot_version INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS snapshots (
			tenant_id TEXT NOT NULL,
			version   INTEGER NOT NULL,
			digest    TEXT NOT NULL,
			changed   INTEGER NOT NULL,
			reason    TEXT NOT NULL,
			at_ns     INTEGER NOT NULL,
			PRIMARY KEY (tenant_id, version)
		)`,
		`CREATE TABLE IF NOT EXISTS conflicts (
			id        INTEGER PRIMARY KEY AUTOINCREMENT,
			tenant_id TEXT NOT NULL,
			ticket_id TEXT NOT NULL,
			request_seq INTEGER NOT NULL,
			code      TEXT NOT NULL,
			at_ns     INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS meta (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
	}
	for _, ddl := range stmts {
		if _, err := s.db.Exec(ddl); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

// UpsertTenant registers a tenant and its HMAC secret.
func (s *Store) UpsertTenant(id, name, secret string) error {
	_, err := s.db.Exec(
		`INSERT INTO tenants(tenant_id, name, secret) VALUES(?,?,?)
		 ON CONFLICT(tenant_id) DO UPDATE SET name=excluded.name, secret=excluded.secret`,
		id, name, secret)
	return err
}

// TenantSecret returns the HMAC secret of a tenant.
func (s *Store) TenantSecret(id string) (string, error) {
	var secret string
	err := s.db.QueryRow(`SELECT secret FROM tenants WHERE tenant_id=?`, id).Scan(&secret)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("unknown tenant %q", id)
	}
	return secret, err
}

// InsertTicket persists a newly issued ticket inside an existing transaction.
func (s *Store) insertTicketTx(tx *sql.Tx, t Ticket) error {
	_, err := tx.Exec(
		`INSERT INTO tickets(ticket_id, tenant_id, sp_id, user_id, raw_ticket, iat, exp, status)
		 VALUES(?,?,?,?,?,?,?,?)`,
		t.ID, t.TenantID, t.SPID, t.UserID, t.Raw, t.IssuedAt, t.ExpiresAt, t.Status)
	return err
}

// GetTicket loads a ticket by id.
func (s *Store) GetTicket(id string) (*Ticket, error) {
	row := s.db.QueryRow(
		`SELECT ticket_id, tenant_id, sp_id, user_id, raw_ticket, iat, exp, status
		 FROM tickets WHERE ticket_id=?`, id)
	var t Ticket
	if err := row.Scan(&t.ID, &t.TenantID, &t.SPID, &t.UserID, &t.Raw,
		&t.IssuedAt, &t.ExpiresAt, &t.Status); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &t, nil
}

// ListTickets returns all tickets of a tenant ordered by id for stable output.
func (s *Store) ListTickets(tenantID string) ([]Ticket, error) {
	rows, err := s.db.Query(
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

// ListAllTickets returns every ticket across tenants in stable id order.
func (s *Store) ListAllTickets() ([]Ticket, error) {
	rows, err := s.db.Query(
		`SELECT ticket_id, tenant_id, sp_id, user_id, raw_ticket, iat, exp, status
		 FROM tickets ORDER BY ticket_id`)
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

// ListRedemptions returns a tenant's redemption records in stable order.
func (s *Store) ListRedemptions(tenantID string) ([]Redemption, error) {
	rows, err := s.db.Query(
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

// ListConflicts returns conflict records in stable order.
func (s *Store) ListConflicts() ([]Conflict, error) {
	rows, err := s.db.Query(
		`SELECT tenant_id, ticket_id, request_seq, code, at_ns
		 FROM conflicts ORDER BY tenant_id, ticket_id, request_seq, code`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Conflict
	for rows.Next() {
		var c Conflict
		if err := rows.Scan(&c.TenantID, &c.TicketID, &c.Seq, &c.Code, &c.AtNS); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ListSnapshots returns a tenant's snapshot chain in version order.
func (s *Store) ListSnapshots(tenantID string) ([]Snapshot, error) {
	rows, err := s.db.Query(
		`SELECT tenant_id, version, digest, changed, reason, at_ns
		 FROM snapshots WHERE tenant_id=? ORDER BY version`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Snapshot
	for rows.Next() {
		var sp Snapshot
		var changed int
		if err := rows.Scan(&sp.TenantID, &sp.Version, &sp.Digest, &changed, &sp.Reason, &sp.AtNS); err != nil {
			return nil, err
		}
		sp.Changed = changed == 1
		out = append(out, sp)
	}
	return out, rows.Err()
}

// LatestVersion returns the highest snapshot version of a tenant (0 if none).
func (s *Store) LatestVersion(tenantID string) (int64, error) {
	var v sql.NullInt64
	if err := s.db.QueryRow(
		`SELECT MAX(version) FROM snapshots WHERE tenant_id=?`, tenantID).Scan(&v); err != nil {
		return 0, err
	}
	if !v.Valid {
		return 0, nil
	}
	return v.Int64, nil
}

// SetMeta / GetMeta store small engine-level key/value pairs.
func (s *Store) SetMeta(key, value string) error {
	_, err := s.db.Exec(
		`INSERT INTO meta(key, value) VALUES(?,?)
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	return err
}

func (s *Store) GetMeta(key string) (string, error) {
	var v string
	err := s.db.QueryRow(`SELECT value FROM meta WHERE key=?`, key).Scan(&v)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return v, err
}

// canonicalDigest builds a content-addressed SHA-256 digest over a tenant's
// ticket and redemption set. Ordering is fully deterministic so identical
// logical states always hash identically.
func canonicalDigest(tickets []Ticket, redemptions []Redemption) string {
	sort.Slice(tickets, func(i, j int) bool { return tickets[i].ID < tickets[j].ID })
	sort.Slice(redemptions, func(i, j int) bool {
		if redemptions[i].TicketID != redemptions[j].TicketID {
			return redemptions[i].TicketID < redemptions[j].TicketID
		}
		return redemptions[i].Seq < redemptions[j].Seq
	})
	type tView struct {
		ID     string `json:"id"`
		SP     string `json:"sp"`
		User   string `json:"user"`
		Iat    int64  `json:"iat"`
		Exp    int64  `json:"exp"`
		Status string `json:"status"`
	}
	type rView struct {
		Ticket string `json:"ticket"`
		Seq    int64  `json:"seq"`
		SP     string `json:"sp"`
		User   string `json:"user"`
	}
	view := struct {
		Tickets     []tView `json:"tickets"`
		Redemptions []rView `json:"redemptions"`
	}{}
	for _, t := range tickets {
		view.Tickets = append(view.Tickets, tView{t.ID, t.SPID, t.UserID, t.IssuedAt, t.ExpiresAt, t.Status})
	}
	for _, r := range redemptions {
		view.Redemptions = append(view.Redemptions, rView{r.TicketID, r.Seq, r.SPID, r.UserID})
	}
	raw, _ := json.Marshal(view)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// LiveDigest recomputes the digest of a tenant's current durable state.
func (s *Store) LiveDigest(tenantID string) (string, error) {
	ts, err := s.ListTickets(tenantID)
	if err != nil {
		return "", err
	}
	rs, err := s.ListRedemptions(tenantID)
	if err != nil {
		return "", err
	}
	return canonicalDigest(ts, rs), nil
}
