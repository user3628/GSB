package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"configaudit/internal/fixedpoint"
	"configaudit/internal/report"
)

// Store is the SQLite persistence layer.
type Store struct {
	db *sql.DB
}

// Snapshot is a versioned, checksummed full audit snapshot.
type Snapshot struct {
	Version   int64          `json:"version"`
	CreatedAt string         `json:"created_at"`
	Checksum  string         `json:"checksum"`
	Report    *report.Report `json:"report"`
}

// Intent kinds.
const (
	IntentAudit       = "audit"
	IntentSnapshot    = "snapshot_replace"
	IntentIncremental = "incremental"
	StatusPending     = "pending"
	StatusCommitted   = "committed"
	StatusAborted     = "aborted"
	StatusRunning     = "running"
	StatusComplete    = "complete"
)

// RecoveryReport contains only facts recovered from disk after a crash.
type RecoveryReport struct {
	RecoveredVersion int64   `json:"recovered_version"`
	AbortedAudits    []int64 `json:"aborted_audits"`
	AbortedIntents   []int64 `json:"aborted_intents"`
	PendingIntents   []int64 `json:"pending_intents"`
}

// Open opens (and initializes) the database file.
func Open(ctx context.Context, path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_txlock=immediate")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

// DB exposes the handle for lower-level operations in tests.
func (s *Store) DB() *sql.DB { return s.db }

// ChecksumPayload returns the sha256 over canonical JSON of payload.
func ChecksumPayload(v any) (string, []byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", nil, err
	}
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:]), b, nil
}

// BeginIntent writes a pending intent row before the mutating transaction.
func (s *Store) BeginIntent(ctx context.Context, kind, requestID string, snapVersion, baseVersion sql.NullInt64, detail any) (int64, error) {
	var detailBlob []byte
	if detail != nil {
		b, err := json.Marshal(detail)
		if err != nil {
			return 0, err
		}
		detailBlob = b
	}
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO intents(intent_type, request_id, snapshot_version, base_version, status, detail, created_at)
		 VALUES(?,?,?,?,?,?,?)`,
		kind, nullString(requestID), snapVersion, baseVersion, StatusPending, detailBlob, nowText())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// ResolveIntent marks an intent committed or aborted.
func (s *Store) ResolveIntent(ctx context.Context, id int64, status string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE intents SET status=?, resolved_at=? WHERE id=?`, status, nowText(), id)
	return err
}

// LatestSnapshotVersion returns the highest complete version, or 0 if none.
func (s *Store) LatestSnapshotVersion(ctx context.Context) (int64, error) {
	var v int64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version),0) FROM snapshots WHERE status='complete'`).Scan(&v)
	return v, err
}

// SaveSnapshot validates monotonic version + checksum and atomically replaces
// the active snapshot set. A pending snapshot row is inserted first via intent.
func (s *Store) SaveSnapshot(ctx context.Context, snap *Snapshot) error {
	current, err := s.LatestSnapshotVersion(ctx)
	if err != nil {
		return err
	}
	if snap.Version <= current {
		return fixedpoint.NewError(fixedpoint.ESnapshotInvalid,
			"snapshot version %d is not greater than current %d", snap.Version, current)
	}
	checksum, payload, err := ChecksumPayload(snap.Report)
	if err != nil {
		return fixedpoint.NewError(fixedpoint.ESnapshotInvalid, "checksum failed: %v", err)
	}
	if snap.Checksum != "" && snap.Checksum != checksum {
		return fixedpoint.NewError(fixedpoint.ESnapshotInvalid,
			"checksum mismatch: declared %s computed %s", snap.Checksum, checksum)
	}
	snap.Checksum = checksum
	intentID, err := s.BeginIntent(ctx, IntentSnapshot, "",
		sql.NullInt64{Int64: snap.Version, Valid: true}, sql.NullInt64{}, snap)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO snapshots(version, created_at, checksum, status, payload)
		 VALUES(?,?,?,?,?)`, snap.Version, snap.CreatedAt, checksum, "complete", payload); err != nil {
		return err
	}
	if err := s.replaceErrorRecordsTx(ctx, tx, snap.Version, snap.Report); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return s.ResolveIntent(ctx, intentID, StatusCommitted)
}

func (s *Store) replaceErrorRecordsTx(ctx context.Context, tx *sql.Tx, version int64, r *report.Report) error {
	if r == nil {
		return nil
	}
	for _, kr := range r.Keys {
		for _, c := range kr.Combinations {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO error_records(snapshot_version,item_id,node_id,key_path,channel,abs_deviation,rel_deviation,error_bound,within_bound)
				 VALUES(?,?,?,?,?,?,?,?,?)`,
				version, c.ItemID, c.NodeID, c.KeyPath, c.Channel,
				c.AbsDeviation, c.RelDeviation, c.ErrorBound, boolInt(c.WithinBound)); err != nil {
				return err
			}
		}
	}
	return nil
}

// LoadSnapshot returns a complete snapshot by version.
func (s *Store) LoadSnapshot(ctx context.Context, version int64) (*Snapshot, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT version, created_at, checksum, payload FROM snapshots WHERE version=? AND status='complete'`, version)
	var (
		createdAt, checksum string
		payload             []byte
	)
	if err := row.Scan(&version, &createdAt, &checksum, &payload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fixedpoint.NewError(fixedpoint.ENotFound, "snapshot version %d not found", version)
		}
		return nil, err
	}
	var r report.Report
	if err := json.Unmarshal(payload, &r); err != nil {
		return nil, err
	}
	got, _, err := ChecksumPayload(&r)
	if err != nil {
		return nil, err
	}
	if got != checksum {
		return nil, fixedpoint.NewError(fixedpoint.ESnapshotInvalid,
			"stored checksum mismatch for version %d", version)
	}
	return &Snapshot{Version: version, CreatedAt: createdAt, Checksum: checksum, Report: &r}, nil
}

// CreateAuditIntent registers a running audit request and its intent.
func (s *Store) CreateAuditIntent(ctx context.Context, requestID, requestHash, kind string, base, version int64) error {
	intentID, err := s.BeginIntent(ctx, IntentAudit, requestID,
		sql.NullInt64{Int64: version, Valid: true},
		sql.NullInt64{Int64: base, Valid: true}, nil)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO audits(request_id,base_version,snapshot_version,status,kind,request_hash,created_at,updated_at)
		 VALUES(?,?,?,?,?,?,?,?)`,
		requestID, base, version, StatusRunning, kind, requestHash, nowText(), nowText())
	if err != nil {
		_ = s.ResolveIntent(ctx, intentID, StatusAborted)
		return err
	}
	return nil
}

// CompleteAudit stores the result and resolves the intent.
func (s *Store) CompleteAudit(ctx context.Context, requestID string, r *report.Report, version int64) error {
	payload, err := json.Marshal(r)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`UPDATE audits SET status=?, result=?, updated_at=?, snapshot_version=? WHERE request_id=?`,
		StatusComplete, payload, nowText(), version, requestID)
	if err != nil {
		return err
	}
	var intentID int64
	err = s.db.QueryRowContext(ctx,
		`SELECT id FROM intents WHERE request_id=? AND status=? ORDER BY id DESC LIMIT 1`,
		requestID, StatusPending).Scan(&intentID)
	if err == nil {
		return s.ResolveIntent(ctx, intentID, StatusCommitted)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	return err
}

// FailAudit marks an audit aborted and resolves its pending intent.
func (s *Store) FailAudit(ctx context.Context, requestID, code string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE audits SET status=?, error_code=?, updated_at=? WHERE request_id=?`,
		StatusAborted, code, nowText(), requestID)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`UPDATE intents SET status=?, resolved_at=? WHERE request_id=? AND status=?`,
		StatusAborted, nowText(), requestID, StatusPending)
	return err
}

// AuditRow is the persisted audit request state.
type AuditRow struct {
	RequestID   string
	Status      string
	RequestHash string
	Result      []byte
	ErrorCode   sql.NullString
	Version     int64
	Base        int64
}

// GetAudit returns the persisted audit row for a request id.
func (s *Store) GetAudit(ctx context.Context, requestID string) (*AuditRow, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT request_id,status,request_hash,COALESCE(result,x''),COALESCE(error_code,''),snapshot_version,base_version
		 FROM audits WHERE request_id=?`, requestID)
	var a AuditRow
	if err := row.Scan(&a.RequestID, &a.Status, &a.RequestHash, &a.Result, &a.ErrorCode, &a.Version, &a.Base); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &a, nil
}

// Recover scans unfinished intents after a forced restart. Running audits are
// aborted; pending snapshot intents without a committed snapshot are aborted.
// Only on-disk facts are reported; the latest complete snapshot stays usable.
func (s *Store) Recover(ctx context.Context) (*RecoveryReport, error) {
	rep := &RecoveryReport{}
	v, err := s.LatestSnapshotVersion(ctx)
	if err != nil {
		return nil, err
	}
	rep.RecoveredVersion = v

	rows, err := s.db.QueryContext(ctx,
		`SELECT id, intent_type, request_id, snapshot_version FROM intents WHERE status=?`, StatusPending)
	if err != nil {
		return nil, err
	}
	type pending struct {
		id      int64
		kind    string
		req     sql.NullString
		version sql.NullInt64
	}
	var pendings []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.id, &p.kind, &p.req, &p.version); err != nil {
			rows.Close()
			return nil, err
		}
		pendings = append(pendings, p)
	}
	rows.Close()
	seenAudit := map[int64]bool{}
	for _, p := range pendings {
		rep.PendingIntents = append(rep.PendingIntents, p.id)
		if p.kind == IntentAudit || p.kind == IntentIncremental {
			if p.req.Valid {
				if _, err := s.db.ExecContext(ctx,
					`UPDATE audits SET status=?, updated_at=? WHERE request_id=? AND status=?`,
					StatusAborted, nowText(), p.req.String, StatusRunning); err != nil {
					return nil, err
				}
				var aid int64
				if err := s.db.QueryRowContext(ctx,
					`SELECT id FROM audits WHERE request_id=?`, p.req.String).Scan(&aid); err == nil {
					if !seenAudit[aid] {
						seenAudit[aid] = true
						rep.AbortedAudits = append(rep.AbortedAudits, aid)
					}
				}
			}
		}
		if p.kind == IntentSnapshot && p.version.Valid {
			var cnt int
			if err := s.db.QueryRowContext(ctx,
				`SELECT COUNT(1) FROM snapshots WHERE version=? AND status='complete'`, p.version.Int64).Scan(&cnt); err != nil {
				return nil, err
			}
			if cnt == 0 {
				rep.AbortedIntents = append(rep.AbortedIntents, p.id)
			} else {
				// Snapshot committed but intent resolution was lost; mark it now.
				if err := s.ResolveIntent(ctx, p.id, StatusCommitted); err != nil {
					return nil, err
				}
				continue
			}
		}
		if err := s.ResolveIntent(ctx, p.id, StatusAborted); err != nil {
			return nil, err
		}
	}
	return rep, nil
}

// SaveIncremental validates the base version and writes an incremental audit.
func (s *Store) SaveIncremental(ctx context.Context, requestID, requestHash string, baseVersion int64, fragment *report.Report) (int64, error) {
	current, err := s.LatestSnapshotVersion(ctx)
	if err != nil {
		return 0, err
	}
	if baseVersion != current {
		return 0, fixedpoint.NewError(fixedpoint.EStaleBase,
			"incremental base %d != current version %d", baseVersion, current)
	}
	newVersion := current + 1
	if err := s.CreateAuditIntent(ctx, requestID, requestHash, "incremental", baseVersion, newVersion); err != nil {
		return 0, err
	}
	// Persist the fragment as a new complete snapshot version so readers always
	// observe a fully committed, checksummed version (never a partial state).
	snap := &Snapshot{Version: newVersion, CreatedAt: nowText(), Report: fragment}
	if err := s.SaveSnapshot(ctx, snap); err != nil {
		_ = s.FailAudit(ctx, requestID, string(fixedpoint.CodeOf(err)))
		return 0, err
	}
	if err := s.CompleteAudit(ctx, requestID, fragment, newVersion); err != nil {
		_ = s.FailAudit(ctx, requestID, string(fixedpoint.CodeOf(err)))
		return 0, err
	}
	return newVersion, nil
}

func nowText() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func nullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// String renders a snapshot for logging.
func (s *Snapshot) String() string {
	return fmt.Sprintf("Snapshot(v=%d checksum=%s)", s.Version, s.Checksum)
}
