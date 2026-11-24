package walgroup

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

const schemaVersion = "1"

// Engine coordinates one writer, many readers, WAL segment files and SQLite
// metadata. All writes are serialized by mu; reads use SQLite read snapshots.
type Engine struct {
	dir  string
	opts Options
	db   *sql.DB

	mu            sync.Mutex
	closed        atomic.Bool
	needsRecovery atomic.Bool

	report RecoveryReport
}

type dbTX interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// Open opens or creates an engine directory. It performs intent recovery and
// strict validation of every surviving segment before returning.
func Open(dir string, options Options) (*Engine, error) {
	opts := defaultOptions(options)
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, wrapError(CodeIO, err, "resolve data directory")
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, wrapError(CodeIO, err, "create data directory")
	}
	dbPath := filepath.Join(abs, "meta.sqlite")
	uriPath := filepath.ToSlash(dbPath)
	if !strings.HasPrefix(uriPath, "/") {
		uriPath = "/" + uriPath
	}
	u := &url.URL{Scheme: "file", Path: uriPath}
	q := u.Query()
	q.Set("_pragma", "busy_timeout(10000)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(FULL)")
	q.Add("_pragma", "foreign_keys(ON)")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, wrapError(CodeIO, err, "open SQLite metadata")
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(8)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, wrapError(CodeIO, err, "connect SQLite metadata")
	}
	e := &Engine{dir: abs, opts: opts, db: db}
	if err := e.initSchema(ctx); err != nil {
		db.Close()
		return nil, err
	}
	report, err := e.recover(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}
	e.report = report
	if err := e.writeRecoveryReport(report); err != nil {
		db.Close()
		return nil, err
	}
	return e, nil
}

func (e *Engine) initSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS segments (
			segment_no INTEGER PRIMARY KEY,
			file_name TEXT NOT NULL,
			start_lsn INTEGER NOT NULL,
			last_lsn INTEGER NOT NULL,
			prev_digest TEXT NOT NULL,
			record_count INTEGER NOT NULL,
			txn_count INTEGER NOT NULL,
			byte_size INTEGER NOT NULL,
			state TEXT NOT NULL,
			digest TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS transactions (
			id TEXT PRIMARY KEY,
			fingerprint TEXT NOT NULL,
			state TEXT NOT NULL,
			segment_no INTEGER NOT NULL DEFAULT 0,
			start_lsn INTEGER NOT NULL DEFAULT 0,
			end_lsn INTEGER NOT NULL DEFAULT 0,
			start_offset INTEGER NOT NULL DEFAULT 0,
			end_offset INTEGER NOT NULL DEFAULT 0,
			record_count INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS txn_staging (
			txn_id TEXT PRIMARY KEY REFERENCES transactions(id) ON DELETE CASCADE,
			payload BLOB NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS txn_records (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			txn_id TEXT NOT NULL REFERENCES transactions(id),
			ordinal INTEGER NOT NULL,
			lsn INTEGER NOT NULL UNIQUE,
			segment_no INTEGER NOT NULL,
			offset INTEGER NOT NULL,
			length INTEGER NOT NULL,
			page_no INTEGER NOT NULL,
			version INTEGER NOT NULL,
			digest TEXT NOT NULL,
			data BLOB NOT NULL,
			UNIQUE(txn_id, ordinal),
			UNIQUE(page_no, version)
		)`,
		`CREATE INDEX IF NOT EXISTS txn_records_lsn ON txn_records(lsn)`,
		`CREATE INDEX IF NOT EXISTS txn_records_page ON txn_records(page_no, version)`,
		`CREATE TABLE IF NOT EXISTS page_versions (
			page_no INTEGER NOT NULL,
			version INTEGER NOT NULL,
			lsn INTEGER NOT NULL,
			txn_id TEXT NOT NULL,
			digest TEXT NOT NULL,
			data BLOB NOT NULL,
			PRIMARY KEY(page_no, version)
		)`,
		`CREATE INDEX IF NOT EXISTS page_versions_lsn ON page_versions(lsn)`,
		`CREATE TABLE IF NOT EXISTS checkpoints (
			sequence INTEGER PRIMARY KEY,
			replay_start_lsn INTEGER NOT NULL,
			pages_digest TEXT NOT NULL,
			digest TEXT NOT NULL,
			active INTEGER NOT NULL,
			created_at INTEGER NOT NULL
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS one_active_checkpoint ON checkpoints(active) WHERE active=1`,
		`CREATE TABLE IF NOT EXISTS checkpoint_pages (
			checkpoint_seq INTEGER NOT NULL REFERENCES checkpoints(sequence),
			page_no INTEGER NOT NULL,
			version INTEGER NOT NULL,
			lsn INTEGER NOT NULL,
			digest TEXT NOT NULL,
			data BLOB NOT NULL,
			PRIMARY KEY(checkpoint_seq, page_no)
		)`,
		`CREATE TABLE IF NOT EXISTS intents (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			kind TEXT NOT NULL,
			status TEXT NOT NULL,
			payload TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := e.db.ExecContext(ctx, stmt); err != nil {
			return wrapError(CodeIO, err, "initialize SQLite schema")
		}
	}
	defaults := map[string]string{
		"schema_version":     schemaVersion,
		"next_lsn":           "1",
		"next_segment_no":    "1",
		"current_segment_no": "0",
	}
	for k, v := range defaults {
		if _, err := e.db.ExecContext(ctx, `INSERT OR IGNORE INTO meta(key,value) VALUES(?,?)`, k, v); err != nil {
			return wrapError(CodeIO, err, "initialize meta %s", k)
		}
	}
	return nil
}

func metaString(ctx context.Context, q dbTX, key string) (string, error) {
	var v string
	err := q.QueryRowContext(ctx, `SELECT value FROM meta WHERE key=?`, key).Scan(&v)
	if err != nil {
		return "", err
	}
	return v, nil
}

func metaUint(ctx context.Context, q dbTX, key string) (uint64, error) {
	v, err := metaString(ctx, q, key)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, wrapError(CodeIO, err, "parse meta %s", key)
	}
	return n, nil
}

func setMeta(ctx context.Context, q dbTX, key, value string) error {
	_, err := q.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES(?,?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	return err
}

func setMetaUint(ctx context.Context, q dbTX, key string, value uint64) error {
	return setMeta(ctx, q, key, strconv.FormatUint(value, 10))
}

func beginWrite(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	return db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
}

func scanSegment(row interface{ Scan(...any) error }) (SegmentInfo, error) {
	var s SegmentInfo
	var state string
	err := row.Scan(&s.SegmentNo, &s.FileName, &s.StartLSN, &s.LastLSN, &s.PrevDigest,
		&s.RecordCount, &s.TxnCount, &s.ByteSize, &state, &s.Digest)
	if err != nil {
		return SegmentInfo{}, err
	}
	s.State = SegmentState(state)
	return s, nil
}

const segmentColumns = `segment_no,file_name,start_lsn,last_lsn,prev_digest,record_count,txn_count,byte_size,state,digest`

func getSegment(ctx context.Context, q dbTX, no uint64) (SegmentInfo, error) {
	row := q.QueryRowContext(ctx, `SELECT `+segmentColumns+` FROM segments WHERE segment_no=?`, no)
	s, err := scanSegment(row)
	if errors.Is(err, sql.ErrNoRows) {
		return SegmentInfo{}, newError(CodeNotFound, "segment %d not found", no)
	}
	if err != nil {
		return SegmentInfo{}, wrapError(CodeIO, err, "read segment %d", no)
	}
	return s, nil
}

func listSegments(ctx context.Context, q dbTX, includeReclaimed bool) ([]SegmentInfo, error) {
	query := `SELECT ` + segmentColumns + ` FROM segments`
	if !includeReclaimed {
		query += ` WHERE state <> 'RECLAIMED'`
	}
	query += ` ORDER BY segment_no`
	rows, err := q.QueryContext(ctx, query)
	if err != nil {
		return nil, wrapError(CodeIO, err, "list segments")
	}
	defer rows.Close()
	var out []SegmentInfo
	for rows.Next() {
		s, err := scanSegment(rows)
		if err != nil {
			return nil, wrapError(CodeIO, err, "scan segment")
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func scanTxn(row interface{ Scan(...any) error }) (Transaction, error) {
	var t Transaction
	var state string
	err := row.Scan(&t.ID, &t.Fingerprint, &state, &t.SegmentNo, &t.StartLSN, &t.EndLSN,
		&t.StartOffset, &t.EndOffset, &t.RecordCount)
	if err != nil {
		return Transaction{}, err
	}
	t.State = TransactionState(state)
	return t, nil
}

const txnColumns = `id,fingerprint,state,segment_no,start_lsn,end_lsn,start_offset,end_offset,record_count`

func getTxn(ctx context.Context, q dbTX, id string) (Transaction, error) {
	row := q.QueryRowContext(ctx, `SELECT `+txnColumns+` FROM transactions WHERE id=?`, id)
	t, err := scanTxn(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Transaction{}, newError(CodeNotFound, "transaction %q not found", id)
	}
	if err != nil {
		return Transaction{}, wrapError(CodeIO, err, "read transaction %q", id)
	}
	return t, nil
}

// Transaction returns the persisted state of one transaction.
func (e *Engine) Transaction(ctx context.Context, id string) (Transaction, error) {
	if e.closed.Load() {
		return Transaction{}, newError(CodeClosed, "engine is closed")
	}
	return getTxn(ctx, e.db, id)
}

// Segments returns the persisted segment catalog in segment-number order.
func (e *Engine) Segments(ctx context.Context) ([]SegmentInfo, error) {
	if e.closed.Load() {
		return nil, newError(CodeClosed, "engine is closed")
	}
	return listSegments(ctx, e.db, false)
}

// RecoveryReport returns the report produced at the most recent Open.
func (e *Engine) RecoveryReport() RecoveryReport { return e.report }

// Dir returns the absolute data directory.
func (e *Engine) Dir() string { return e.dir }

// Close releases SQLite handles. It does not seal the current segment: an OPEN
// segment remains appendable after a clean restart.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed.Swap(true) {
		return nil
	}
	return e.db.Close()
}

func (e *Engine) fault(point string, segNo uint64, txnIDs []string) error {
	if e.opts.Fault == nil {
		return nil
	}
	return e.opts.Fault(FaultPoint{Point: point, SegmentNo: segNo, TxnIDs: append([]string(nil), txnIDs...)})
}

func (e *Engine) crash(point string, details map[string]any) {
	if e.opts.CrashPoint != nil {
		e.opts.CrashPoint(point, details)
	}
}

func syncCounter(e *Engine, segNo uint64) {
	if e != nil && e.opts.TestSyncCounter != nil {
		e.opts.TestSyncCounter(segNo)
	}
}

func (e *Engine) writeRecoveryReport(r RecoveryReport) error {
	path := filepath.Join(e.dir, "recovery-report.json")
	b, err := jsonMarshalIndent(r)
	if err != nil {
		return wrapError(CodeIO, err, "encode recovery report")
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return wrapError(CodeIO, err, "write recovery report")
	}
	if err := os.Rename(tmp, path); err != nil {
		return wrapError(CodeIO, err, "replace recovery report")
	}
	return nil
}

func checkClosed(e *Engine) error {
	if e.closed.Load() {
		return newError(CodeClosed, "engine is closed")
	}
	if e.needsRecovery.Load() {
		return newError(CodeRecoveryRequired, "engine requires restart recovery before further operations")
	}
	return nil
}
