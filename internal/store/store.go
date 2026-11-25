// Package store persists verification state in SQLite with write-ahead
// intent records: before page digests, intermediate nodes, consensus
// conclusions or checkpoint advances are committed, an intent row is
// written. After a hard kill, recovery scans unfinished intents and marks
// them aborted; committed checkpoints stay intact because each intent is
// applied in a single transaction.
package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"blockinspect/internal/manifest"
)

// Intent statuses.
const (
	IntentPending = "pending"
	IntentDone    = "done"
	IntentAborted = "aborted"
)

// Store wraps the SQLite database.
type Store struct {
	db *sql.DB
}

// Open opens (and migrates) the database at path.
// Use ":memory:" for tests.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	stmts := []string{
		`PRAGMA journal_mode=WAL`,
		`CREATE TABLE IF NOT EXISTS intents(
			id TEXT PRIMARY KEY,
			block_id TEXT NOT NULL,
			kind TEXT NOT NULL,
			payload TEXT NOT NULL,
			status TEXT NOT NULL,
			created_at TEXT NOT NULL,
			finished_at TEXT)`,
		`CREATE TABLE IF NOT EXISTS checkpoints(
			block_id TEXT NOT NULL,
			checkpoint_id INTEGER NOT NULL,
			root_digest BLOB NOT NULL,
			created_at TEXT NOT NULL,
			PRIMARY KEY(block_id, checkpoint_id))`,
		`CREATE TABLE IF NOT EXISTS leaves(
			block_id TEXT NOT NULL,
			checkpoint_id INTEGER NOT NULL,
			page_no INTEGER NOT NULL,
			digest BLOB NOT NULL,
			PRIMARY KEY(block_id, checkpoint_id, page_no))`,
		`CREATE TABLE IF NOT EXISTS nodes(
			block_id TEXT NOT NULL,
			checkpoint_id INTEGER NOT NULL,
			level INTEGER NOT NULL,
			idx INTEGER NOT NULL,
			digest BLOB NOT NULL,
			PRIMARY KEY(block_id, checkpoint_id, level, idx))`,
		`CREATE TABLE IF NOT EXISTS consensus(
			block_id TEXT NOT NULL,
			checkpoint_id INTEGER NOT NULL,
			request_id TEXT NOT NULL,
			root_digest BLOB NOT NULL,
			result TEXT NOT NULL,
			PRIMARY KEY(block_id, checkpoint_id))`,
		`CREATE TABLE IF NOT EXISTS requests(
			request_id TEXT PRIMARY KEY,
			block_id TEXT NOT NULL,
			payload_hash TEXT NOT NULL,
			status TEXT NOT NULL,
			result TEXT)`,
	}
	for _, q := range stmts {
		if _, err := s.db.Exec(q); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// WriteIntent records a pending intent and returns its ID. The intent is
// committed on its own before any state mutation begins.
func (s *Store) WriteIntent(id, blockID, kind string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO intents(id, block_id, kind, payload, status, created_at)
		 VALUES(?,?,?,?,?,?)`, id, blockID, kind, string(b), IntentPending, now())
	return err
}

// Intent describes one persisted intent row.
type Intent struct {
	ID         string `json:"id"`
	BlockID    string `json:"block_id"`
	Kind       string `json:"kind"`
	Payload    string `json:"payload"`
	Status     string `json:"status"`
	CreatedAt  string `json:"created_at"`
	FinishedAt string `json:"finished_at,omitempty"`
}

// PendingIntents lists all intents still marked pending.
func (s *Store) PendingIntents() ([]Intent, error) {
	rows, err := s.db.Query(
		`SELECT id, block_id, kind, payload, status, created_at FROM intents WHERE status=?`,
		IntentPending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Intent
	for rows.Next() {
		var it Intent
		if err := rows.Scan(&it.ID, &it.BlockID, &it.Kind, &it.Payload, &it.Status, &it.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// AbortIntent marks an intent aborted (used by recovery).
func (s *Store) AbortIntent(id string) error {
	_, err := s.db.Exec(`UPDATE intents SET status=?, finished_at=? WHERE id=?`,
		IntentAborted, now(), id)
	return err
}

// CommitCheckpoint atomically persists one verification outcome: tree
// leaves, intermediate nodes, the consensus conclusion and the checkpoint
// advance, and marks the intent done — all in a single transaction.
func (s *Store) CommitCheckpoint(intentID, blockID string, checkpointID int64,
	tree *manifest.Tree, consensusResult []byte, requestID string, requestResult []byte) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	root := tree.Root()
	if _, err := tx.Exec(
		`INSERT INTO checkpoints(block_id, checkpoint_id, root_digest, created_at) VALUES(?,?,?,?)`,
		blockID, checkpointID, root[:], now()); err != nil {
		return err
	}
	leafStmt, err := tx.Prepare(
		`INSERT INTO leaves(block_id, checkpoint_id, page_no, digest) VALUES(?,?,?,?)`)
	if err != nil {
		return err
	}
	defer leafStmt.Close()
	for p, d := range tree.Levels[0] {
		if _, err := leafStmt.Exec(blockID, checkpointID, p, d[:]); err != nil {
			return err
		}
	}
	nodeStmt, err := tx.Prepare(
		`INSERT INTO nodes(block_id, checkpoint_id, level, idx, digest) VALUES(?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer nodeStmt.Close()
	for level := 1; level < len(tree.Levels); level++ {
		for idx, d := range tree.Levels[level] {
			if _, err := nodeStmt.Exec(blockID, checkpointID, level, idx, d[:]); err != nil {
				return err
			}
		}
	}
	if consensusResult != nil {
		if _, err := tx.Exec(
			`INSERT INTO consensus(block_id, checkpoint_id, request_id, root_digest, result) VALUES(?,?,?,?,?)`,
			blockID, checkpointID, requestID, root[:], string(consensusResult)); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(
		`UPDATE requests SET status=?, result=? WHERE request_id=?`,
		"done", string(requestResult), requestID); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE intents SET status=?, finished_at=? WHERE id=?`,
		IntentDone, now(), intentID); err != nil {
		return err
	}
	return tx.Commit()
}

// RegisterRequest records an incoming idempotent request. It returns
// false when the request ID already exists.
func (s *Store) RegisterRequest(requestID, blockID, payloadHash string) (bool, error) {
	res, err := s.db.Exec(
		`INSERT INTO requests(request_id, block_id, payload_hash, status) VALUES(?,?,?,?)`,
		requestID, blockID, payloadHash, "running")
	if err != nil {
		return false, nil // primary-key conflict: already registered
	}
	n, _ := res.RowsAffected()
	return n == 1, err
}

// Request is a persisted idempotency record.
type Request struct {
	RequestID   string `json:"request_id"`
	BlockID     string `json:"block_id"`
	PayloadHash string `json:"payload_hash"`
	Status      string `json:"status"`
	Result      string `json:"result,omitempty"`
}

// GetRequest loads a request record by ID.
func (s *Store) GetRequest(requestID string) (*Request, error) {
	row := s.db.QueryRow(
		`SELECT request_id, block_id, payload_hash, status, COALESCE(result,'') FROM requests WHERE request_id=?`,
		requestID)
	var r Request
	if err := row.Scan(&r.RequestID, &r.BlockID, &r.PayloadHash, &r.Status, &r.Result); err != nil {
		return nil, err
	}
	return &r, nil
}

// FailRequest marks a running request as failed with its error payload.
func (s *Store) FailRequest(requestID string, result []byte) error {
	_, err := s.db.Exec(`UPDATE requests SET status=?, result=? WHERE request_id=?`,
		"failed", string(result), requestID)
	return err
}

// Checkpoint holds a persisted checkpoint header.
type Checkpoint struct {
	BlockID      string `json:"block_id"`
	CheckpointID int64  `json:"checkpoint_id"`
	RootDigest   string `json:"root_digest"`
	CreatedAt    string `json:"created_at"`
}

// LatestCheckpoint returns the newest committed checkpoint for a block,
// or sql.ErrNoRows.
func (s *Store) LatestCheckpoint(blockID string) (*Checkpoint, error) {
	row := s.db.QueryRow(
		`SELECT block_id, checkpoint_id, root_digest, created_at FROM checkpoints
		 WHERE block_id=? ORDER BY checkpoint_id DESC LIMIT 1`, blockID)
	var c Checkpoint
	var root []byte
	if err := row.Scan(&c.BlockID, &c.CheckpointID, &root, &c.CreatedAt); err != nil {
		return nil, err
	}
	var d manifest.Digest
	copy(d[:], root)
	c.RootDigest = d.Hex()
	return &c, nil
}

// LoadTree reconstructs the digest tree registered at a checkpoint.
func (s *Store) LoadTree(blockID string, checkpointID int64) (*manifest.Tree, error) {
	rows, err := s.db.Query(
		`SELECT page_no, digest FROM leaves WHERE block_id=? AND checkpoint_id=? ORDER BY page_no`,
		blockID, checkpointID)
	if err != nil {
		return nil, err
	}
	var leaves []manifest.Digest
	for rows.Next() {
		var p int
		var b []byte
		if err := rows.Scan(&p, &b); err != nil {
			rows.Close()
			return nil, err
		}
		var d manifest.Digest
		copy(d[:], b)
		leaves = append(leaves, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(leaves) == 0 {
		return nil, sql.ErrNoRows
	}
	t := &manifest.Tree{Levels: [][]manifest.Digest{leaves}}
	width := len(leaves)
	level := 1
	for width > 1 {
		width = (width + 1) / 2
		t.Levels = append(t.Levels, make([]manifest.Digest, width))
		level++
	}
	nrows, err := s.db.Query(
		`SELECT level, idx, digest FROM nodes WHERE block_id=? AND checkpoint_id=? ORDER BY level, idx`,
		blockID, checkpointID)
	if err != nil {
		return nil, err
	}
	defer nrows.Close()
	for nrows.Next() {
		var lvl, idx int
		var b []byte
		if err := nrows.Scan(&lvl, &idx, &b); err != nil {
			return nil, err
		}
		if lvl < len(t.Levels) && idx < len(t.Levels[lvl]) {
			copy(t.Levels[lvl][idx][:], b)
		}
	}
	return t, nrows.Err()
}

// RecoveryReport lists what recovery found and did, using only facts that
// were actually persisted.
type RecoveryReport struct {
	RecoveredAt string `json:"recovered_at"`
	// NewlyAborted is how many pending intents this recovery run aborted.
	NewlyAborted int `json:"newly_aborted"`
	// AbortedIntents lists every intent persisted as aborted (facts only).
	AbortedIntents  []Intent     `json:"aborted_intents"`
	LastCheckpoints []Checkpoint `json:"last_checkpoints"`
}

// Recover scans for unfinished intents and marks them aborted. Because
// each intent is applied in one transaction, no partial checkpoint data
// can exist; the digest tree and consensus conclusions simply remain at
// the last fully committed checkpoint.
func (s *Store) Recover() (*RecoveryReport, error) {
	rep := &RecoveryReport{RecoveredAt: now()}
	pending, err := s.PendingIntents()
	if err != nil {
		return nil, err
	}
	for _, it := range pending {
		if err := s.AbortIntent(it.ID); err != nil {
			return nil, err
		}
		rep.NewlyAborted++
	}
	// the report reflects persisted facts: every aborted intent on disk
	rows2, err := s.db.Query(
		`SELECT id, block_id, kind, payload, status, created_at, COALESCE(finished_at,'')
		 FROM intents WHERE status=? ORDER BY created_at`, IntentAborted)
	if err != nil {
		return nil, err
	}
	defer rows2.Close()
	for rows2.Next() {
		var it Intent
		if err := rows2.Scan(&it.ID, &it.BlockID, &it.Kind, &it.Payload, &it.Status,
			&it.CreatedAt, &it.FinishedAt); err != nil {
			return nil, err
		}
		rep.AbortedIntents = append(rep.AbortedIntents, it)
	}
	if err := rows2.Err(); err != nil {
		return nil, err
	}
	rows, err := s.db.Query(
		`SELECT c.block_id, c.checkpoint_id, c.root_digest, c.created_at
		 FROM checkpoints c
		 JOIN (SELECT block_id, MAX(checkpoint_id) mc FROM checkpoints GROUP BY block_id) m
		   ON c.block_id=m.block_id AND c.checkpoint_id=m.mc`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var c Checkpoint
		var root []byte
		if err := rows.Scan(&c.BlockID, &c.CheckpointID, &root, &c.CreatedAt); err != nil {
			return nil, err
		}
		var d manifest.Digest
		copy(d[:], root)
		c.RootDigest = d.Hex()
		rep.LastCheckpoints = append(rep.LastCheckpoints, c)
	}
	return rep, rows.Err()
}

// ConsensusResult loads the persisted consensus conclusion of a block.
func (s *Store) ConsensusResult(blockID string, checkpointID int64) (string, error) {
	var out string
	err := s.db.QueryRow(
		`SELECT result FROM consensus WHERE block_id=? AND checkpoint_id=?`,
		blockID, checkpointID).Scan(&out)
	return out, err
}
