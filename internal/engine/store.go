package engine

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"mvccengine/pkg/mvcc"
)

const driverName = "sqlite"

type Options struct {
	Now                    func() int64
	CommitIntentHook       func(intentID string, requestID string)
	SwitchIntentHook       func(intentID string, snapshot mvcc.Snapshot)
	ReclaimIntentHook      func(intentID string)
}

type Engine struct {
	db  *sql.DB
	opts Options
	now  func() int64

	mu sync.RWMutex

	nextTxnSeq     int64
	nextVersionSeq int64
	current        mvcc.Snapshot
	txns           map[int64]mvcc.Txn
	versions       map[int64]mvcc.RowVersion
	heads          map[string]int64
	snapshots      map[int64]mvcc.Snapshot
	recovery       *RecoveryReport

	callsMu sync.Mutex
	calls   map[string]*inflightCall
}

type inflightCall struct {
	done     chan struct{}
	scope    string
	hash     string
	response any
	raw      json.RawMessage
	err      error
}

func Open(dbPath string) (*Engine, error) {
	return OpenWithOptions(dbPath, Options{})
}

func OpenWithOptions(dbPath string, opts Options) (*Engine, error) {
	if opts.Now == nil {
		opts.Now = func() int64 { return time.Now().UnixMilli() }
	}
	if dbPath != ":memory:" && dbPath != "" {
		if dir := filepath.Dir(dbPath); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, err
			}
		}
	}

	db, err := sql.Open(driverName, dbPath)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if _, err := db.Exec(`PRAGMA busy_timeout=5000;`); err != nil {
		_ = db.Close()
		return nil, err
	}
	if dbPath != ":memory:" {
		if _, err := db.Exec(`PRAGMA journal_mode=WAL;`); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	if _, err := db.Exec(`PRAGMA foreign_keys=ON;`); err != nil {
		_ = db.Close()
		return nil, err
	}

	e := &Engine{
		db:        db,
		opts:      opts,
		now:       opts.Now,
		txns:      make(map[int64]mvcc.Txn),
		versions:  make(map[int64]mvcc.RowVersion),
		heads:     make(map[string]int64),
		snapshots: make(map[int64]mvcc.Snapshot),
		calls:     make(map[string]*inflightCall),
	}
	if err := e.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := e.initializeMeta(); err != nil {
		_ = db.Close()
		return nil, err
	}
	e.mu.Lock()
	err = e.loadStateLocked()
	e.mu.Unlock()
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return e, nil
}

func (e *Engine) Close() error {
	if e.db == nil {
		return nil
	}
	return e.db.Close()
}

func (e *Engine) DB() *sql.DB { return e.db }

func (e *Engine) migrate() error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS txns (
			seq INTEGER PRIMARY KEY,
			status TEXT NOT NULL,
			started_at_ms INTEGER NOT NULL,
			snapshot_seq INTEGER NOT NULL,
			upper_bound INTEGER NOT NULL,
			request_id TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS versions (
			version_seq INTEGER PRIMARY KEY,
			row_key TEXT NOT NULL,
			create_txn_seq INTEGER NOT NULL,
			commit_state TEXT NOT NULL,
			digest TEXT NOT NULL,
			content TEXT NOT NULL DEFAULT '',
			prev_version_seq INTEGER,
			is_delete INTEGER NOT NULL DEFAULT 0,
			superseding_commit_seq INTEGER NOT NULL DEFAULT 0,
			reclaimed INTEGER NOT NULL DEFAULT 0,
			detached INTEGER NOT NULL DEFAULT 0,
			created_at_ms INTEGER NOT NULL,
			updated_at_ms INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_versions_row ON versions(row_key, version_seq DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_versions_txn ON versions(create_txn_seq)`,
		`CREATE TABLE IF NOT EXISTS row_heads (
			row_key TEXT PRIMARY KEY,
			version_seq INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS intents (
			intent_id TEXT PRIMARY KEY,
			kind TEXT NOT NULL,
			request_id TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL,
			status TEXT NOT NULL,
			result_json TEXT,
			error_json TEXT,
			created_at_ms INTEGER NOT NULL,
			updated_at_ms INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status, created_at_ms)`,
		`CREATE TABLE IF NOT EXISTS idempotent_requests (
			request_id TEXT PRIMARY KEY,
			scope TEXT NOT NULL,
			request_hash TEXT NOT NULL,
			status TEXT NOT NULL,
			response_json TEXT,
			error_json TEXT,
			intent_id TEXT NOT NULL DEFAULT '',
			created_at_ms INTEGER NOT NULL,
			updated_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS snapshots (
			snapshot_seq INTEGER PRIMARY KEY,
			generated_at_ms INTEGER NOT NULL,
			txn_upper_bound INTEGER NOT NULL,
			checksum TEXT NOT NULL,
			is_current INTEGER NOT NULL DEFAULT 0,
			source_path TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS reclaimed_versions (
			version_seq INTEGER PRIMARY KEY,
			row_key TEXT NOT NULL,
			superseding_commit_seq INTEGER NOT NULL,
			intent_id TEXT NOT NULL,
			reclaimed_at_ms INTEGER NOT NULL
		)`,
	}
	for _, stmt := range statements {
		if _, err := e.db.Exec(stmt); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

func (e *Engine) initializeMeta() error {
	defaults := map[string]string{
		"next_txn_seq":     "0",
		"next_version_seq": "0",
	}
	tx, err := e.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for key, value := range defaults {
		if _, err := tx.Exec(`INSERT OR IGNORE INTO meta(key, value) VALUES(?, ?)`, key, value); err != nil {
			return err
		}
	}
	var count int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM snapshots`).Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		snap := mvcc.Snapshot{SnapshotSeq: 1, GeneratedAtMS: e.now(), TxnUpperBound: 0}.WithChecksum()
		if _, err := tx.Exec(`INSERT INTO snapshots(snapshot_seq, generated_at_ms, txn_upper_bound, checksum, is_current, source_path)
			VALUES(?, ?, ?, ?, 1, '')`, snap.SnapshotSeq, snap.GeneratedAtMS, snap.TxnUpperBound, snap.Checksum); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT OR REPLACE INTO meta(key,value) VALUES('current_snapshot_seq','1')`); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (e *Engine) loadState() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.loadStateLocked()
}

func (e *Engine) loadStateLocked() error {
	if err := e.db.QueryRow(`SELECT CAST(value AS INTEGER) FROM meta WHERE key='next_txn_seq'`).Scan(&e.nextTxnSeq); err != nil {
		return err
	}
	if err := e.db.QueryRow(`SELECT CAST(value AS INTEGER) FROM meta WHERE key='next_version_seq'`).Scan(&e.nextVersionSeq); err != nil {
		return err
	}

	rows, err := e.db.Query(`SELECT seq,status,started_at_ms,snapshot_seq,upper_bound,request_id FROM txns ORDER BY seq`)
	if err != nil {
		return err
	}
	defer rows.Close()
	e.txns = make(map[int64]mvcc.Txn)
	for rows.Next() {
		var txn mvcc.Txn
		var status string
		if err := rows.Scan(&txn.Seq, &status, &txn.StartedAtMS, &txn.SnapshotSeq, &txn.UpperBound, &txn.RequestID); err != nil {
			return err
		}
		txn.Status = mvcc.TxnStatus(status)
		e.txns[txn.Seq] = txn
	}
	if err := rows.Err(); err != nil {
		return err
	}

	vrows, err := e.db.Query(`SELECT row_key,version_seq,create_txn_seq,commit_state,digest,content,prev_version_seq,is_delete,superseding_commit_seq,reclaimed,detached FROM versions ORDER BY version_seq`)
	if err != nil {
		return err
	}
	defer vrows.Close()
	e.versions = make(map[int64]mvcc.RowVersion)
	for vrows.Next() {
		var v mvcc.RowVersion
		var state string
		var prev sql.NullInt64
		var isDelete, reclaimed, detached int
		if err := vrows.Scan(&v.RowKey, &v.VersionSeq, &v.CreateTxnSeq, &state, &v.Digest, &v.Content, &prev, &isDelete, &v.SupersedingCommitSeq, &reclaimed, &detached); err != nil {
			return err
		}
		v.CommitState = mvcc.CommitState(state)
		if prev.Valid {
			p := prev.Int64
			v.PrevVersionSeq = &p
		}
		v.IsDelete = isDelete == 1
		v.Reclaimed = reclaimed == 1
		v.Detached = detached == 1
		e.versions[v.VersionSeq] = v
	}
	if err := vrows.Err(); err != nil {
		return err
	}

	hrows, err := e.db.Query(`SELECT row_key, version_seq FROM row_heads`)
	if err != nil {
		return err
	}
	defer hrows.Close()
	e.heads = make(map[string]int64)
	for hrows.Next() {
		var key string
		var seq int64
		if err := hrows.Scan(&key, &seq); err != nil {
			return err
		}
		e.heads[key] = seq
	}
	if err := hrows.Err(); err != nil {
		return err
	}

	srows, err := e.db.Query(`SELECT snapshot_seq,generated_at_ms,txn_upper_bound,checksum FROM snapshots ORDER BY snapshot_seq`)
	if err != nil {
		return err
	}
	defer srows.Close()
	e.snapshots = make(map[int64]mvcc.Snapshot)
	for srows.Next() {
		var s mvcc.Snapshot
		if err := srows.Scan(&s.SnapshotSeq, &s.GeneratedAtMS, &s.TxnUpperBound, &s.Checksum); err != nil {
			return err
		}
		e.snapshots[s.SnapshotSeq] = s
	}
	if err := srows.Err(); err != nil {
		return err
	}

	var currentSeq int64
	if err := e.db.QueryRow(`SELECT CAST(value AS INTEGER) FROM meta WHERE key='current_snapshot_seq'`).Scan(&currentSeq); err != nil {
		return err
	}
	e.current = e.snapshots[currentSeq]
	return nil
}

func (e *Engine) persistMetaTx(tx *sql.Tx) error {
	if _, err := tx.Exec(`INSERT OR REPLACE INTO meta(key,value) VALUES('next_txn_seq',?)`, fmt.Sprint(e.nextTxnSeq)); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT OR REPLACE INTO meta(key,value) VALUES('next_version_seq',?)`, fmt.Sprint(e.nextVersionSeq)); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT OR REPLACE INTO meta(key,value) VALUES('current_snapshot_seq',?)`, fmt.Sprint(e.current.SnapshotSeq)); err != nil {
		return err
	}
	return nil
}

func cloneViewLocked(e *Engine) mvcc.View {
	view := mvcc.NewView()
	for seq, txn := range e.txns {
		view.Txns[seq] = txn
	}
	for seq, version := range e.versions {
		view.Versions[seq] = version
	}
	for key, seq := range e.heads {
		view.Heads[key] = seq
	}
	return view
}

func jsonMarshal(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func newIntentID(kind string) string {
	return fmt.Sprintf("intent-%s-%s", kind, randomSuffix())
}
