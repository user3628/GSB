package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"example.com/ntfsacl/internal/domain"
)

const (
	CodeSnapshotVersion = "E_SNAPSHOT_VERSION"

	KindAudit           = "audit"
	KindWritebackBefore = "writeback_before"
	KindWritebackAfter  = "writeback_after"

	SessionPending    = "PENDING"
	SessionRunning    = "RUNNING"
	SessionCompleted  = "COMPLETED"
	SessionRolledBack = "ROLLED_BACK"
	SessionRecovered  = "RECOVERED"
	SessionIncomplete = "RECOVERY_INCOMPLETE"

	OpPending    = "PENDING"
	OpCommitted  = "COMMITTED"
	OpRolledBack = "ROLLED_BACK"
)

type Snapshot struct {
	Version         int64
	Kind            string
	ParentVersion   int64
	ParentChainHash string
	Payload         []byte
	PayloadSHA256   string
	ChainHash       string
	ContentHash     string
	ChangeKind      string
}

type SnapshotInfo struct {
	Version         int64  `json:"version"`
	Kind            string `json:"kind"`
	ParentVersion   int64  `json:"parent_version"`
	ParentChainHash string `json:"parent_chain_hash"`
	PayloadSHA256   string `json:"payload_sha256"`
	ChainHash       string `json:"chain_hash"`
	ContentHash     string `json:"content_hash"`
	ChangeKind      string `json:"change_kind"`
}

type SnapshotVersionError struct {
	Code          string `json:"code"`
	Requested     int64  `json:"requested_version"`
	Fallback      int64  `json:"fallback_version"`
	FallbackBytes []byte `json:"-"`
	Detail        string `json:"detail"`
}

func (e *SnapshotVersionError) Error() string {
	return fmt.Sprintf("%s: %s (requested=%d fallback=%d)", e.Code, e.Detail, e.Requested, e.Fallback)
}

type DB struct {
	db   *sql.DB
	path string
}

func Open(path string) (*DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	dsn := sqliteDSN(abs)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	s := &DB{db: db, path: abs}
	if err := s.migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func sqliteDSN(abs string) string {
	slashPath := filepath.ToSlash(abs)
	if filepath.VolumeName(abs) != "" && !strings.HasPrefix(slashPath, "/") {
		slashPath = "/" + slashPath
	}
	u := url.URL{Scheme: "file", Path: slashPath}
	return u.String() + "?_pragma=busy_timeout(10000)&_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)"
}

func (s *DB) Close() error { return s.db.Close() }
func (s *DB) Path() string { return s.path }

func (s *DB) migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);`,
		`CREATE TABLE IF NOT EXISTS snapshots (
			version INTEGER PRIMARY KEY,
			kind TEXT NOT NULL,
			parent_version INTEGER NOT NULL,
			parent_chain_hash TEXT NOT NULL,
			payload BLOB NOT NULL,
			payload_sha256 TEXT NOT NULL,
			chain_hash TEXT NOT NULL,
			content_hash TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS writeback_sessions (
			id TEXT PRIMARY KEY,
			audit_version INTEGER NOT NULL,
			plan_hash TEXT NOT NULL,
			status TEXT NOT NULL,
			code TEXT NOT NULL,
			before_snapshot_version INTEGER NOT NULL DEFAULT 0,
			after_snapshot_version INTEGER NOT NULL DEFAULT 0,
			created_seq INTEGER NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS acl_revisions (
			revision INTEGER PRIMARY KEY AUTOINCREMENT,
			session_id TEXT NOT NULL,
			node_path TEXT NOT NULL,
			phase TEXT NOT NULL,
			payload BLOB NOT NULL,
			payload_sha256 TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS writeback_ops (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			session_id TEXT NOT NULL,
			seq INTEGER NOT NULL,
			node_path TEXT NOT NULL,
			phase TEXT NOT NULL,
			before_revision INTEGER NOT NULL,
			before_payload BLOB NOT NULL,
			before_sha256 TEXT NOT NULL,
			after_revision INTEGER NOT NULL DEFAULT 0,
			after_payload BLOB,
			after_sha256 TEXT NOT NULL DEFAULT '',
			error TEXT NOT NULL DEFAULT '',
			UNIQUE(session_id, seq)
		);`,
		`CREATE TABLE IF NOT EXISTS node_locks (
			path TEXT PRIMARY KEY,
			owner TEXT NOT NULL,
			subtree TEXT NOT NULL,
			acquired_unix_nano INTEGER NOT NULL
		);`,
		`INSERT INTO schema_meta(key,value) VALUES('schema_version','1') ON CONFLICT(key) DO NOTHING;`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func beginImmediate(ctx context.Context, db *sql.DB) (*sql.Conn, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func commitConn(conn *sql.Conn) error {
	if _, err := conn.ExecContext(context.Background(), "COMMIT"); err != nil {
		return err
	}
	return conn.Close()
}

func rollbackConn(conn *sql.Conn) {
	_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
	_ = conn.Close()
}

// CommitSnapshot assigns the next version inside one IMMEDIATE SQLite
// transaction. The builder sees the exact version and parent hash that are
// committed, allowing a report to embed immutable replay metadata.
func (s *DB) CommitSnapshot(ctx context.Context, kind string, build func(version, parentVersion int64, parentChainHash string) ([]byte, error)) (Snapshot, error) {
	conn, err := beginImmediate(ctx, s.db)
	if err != nil {
		return Snapshot{}, err
	}
	committed := false
	defer func() {
		if !committed {
			rollbackConn(conn)
		}
	}()
	var parentVersion int64
	var parentHash string
	err = conn.QueryRowContext(ctx, `SELECT version, chain_hash FROM snapshots ORDER BY version DESC LIMIT 1`).Scan(&parentVersion, &parentHash)
	if errors.Is(err, sql.ErrNoRows) {
		parentVersion, parentHash, err = 0, "", nil
	}
	if err != nil {
		return Snapshot{}, err
	}
	version := parentVersion + 1
	payload, err := build(version, parentVersion, parentHash)
	if err != nil {
		return Snapshot{}, err
	}
	if len(payload) == 0 {
		return Snapshot{}, errors.New("empty snapshot payload")
	}
	payloadHash := domain.HashBytes(payload)
	chainHash := hashChain(version, parentHash, payloadHash)
	contentHash, err := contentFingerprint(payload)
	if err != nil {
		return Snapshot{}, err
	}
	_, err = conn.ExecContext(ctx, `INSERT INTO snapshots
		(version, kind, parent_version, parent_chain_hash, payload, payload_sha256, chain_hash, content_hash)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		version, kind, parentVersion, parentHash, payload, payloadHash, chainHash, contentHash)
	if err != nil {
		return Snapshot{}, err
	}
	if err := commitConn(conn); err != nil {
		return Snapshot{}, err
	}
	committed = true
	return Snapshot{
		Version: version, Kind: kind, ParentVersion: parentVersion,
		ParentChainHash: parentHash, Payload: append([]byte(nil), payload...),
		PayloadSHA256: payloadHash, ChainHash: chainHash, ContentHash: contentHash,
		ChangeKind: "append",
	}, nil
}

func hashChain(version int64, parentHash, payloadHash string) string {
	return domain.HashBytes([]byte(fmt.Sprintf("%d\n%s\n%s", version, parentHash, payloadHash)))
}

type contentInfo struct {
	TreeHash string `json:"tree_hash"`
	Nodes    []struct {
		RelPath string `json:"rel_path"`
		ACLHash string `json:"acl_hash"`
	} `json:"nodes"`
}

func parseContent(payload []byte) (contentInfo, []string, error) {
	var c contentInfo
	if err := json.Unmarshal(payload, &c); err != nil {
		return c, nil, err
	}
	ids := make([]string, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		ids = append(ids, n.RelPath+"|"+n.ACLHash)
	}
	sort.Strings(ids)
	return c, ids, nil
}

func contentFingerprint(payload []byte) (string, error) {
	c, ids, err := parseContent(payload)
	if err != nil {
		return "", err
	}
	if c.TreeHash == "" && len(ids) == 0 {
		return domain.HashBytes(payload), nil
	}
	h := sha256.New()
	h.Write([]byte(c.TreeHash))
	h.Write([]byte{0})
	for _, id := range ids {
		h.Write([]byte(id))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func changeKind(prev, cur []byte) string {
	if len(prev) == 0 {
		return "append"
	}
	c1, ids1, err1 := parseContent(prev)
	c2, ids2, err2 := parseContent(cur)
	if err1 != nil || err2 != nil {
		if domain.HashBytes(prev) == domain.HashBytes(cur) {
			return "unchanged"
		}
		return "whole_replace"
	}
	if c1.TreeHash == c2.TreeHash && strings.Join(ids1, "\n") == strings.Join(ids2, "\n") {
		return "unchanged"
	}
	set := make(map[string]bool, len(ids1))
	for _, id := range ids1 {
		set[id] = true
	}
	for _, id := range ids2 {
		if set[id] {
			return "incremental"
		}
	}
	return "whole_replace"
}

// SnapshotChain validates version numbers, parent hashes and payload hashes.
// It returns the longest healthy prefix and a stable error when damage is found.
func (s *DB) SnapshotChain(ctx context.Context) ([]Snapshot, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT version, kind, parent_version, parent_chain_hash, payload, payload_sha256, chain_hash, content_hash FROM snapshots ORDER BY version ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Snapshot
	var prev *Snapshot
	for rows.Next() {
		var snap Snapshot
		if err := rows.Scan(&snap.Version, &snap.Kind, &snap.ParentVersion, &snap.ParentChainHash, &snap.Payload, &snap.PayloadSHA256, &snap.ChainHash, &snap.ContentHash); err != nil {
			return out, err
		}
		expectedVersion := int64(1)
		expectedParent := int64(0)
		expectedParentHash := ""
		if prev != nil {
			expectedVersion = prev.Version + 1
			expectedParent = prev.Version
			expectedParentHash = prev.ChainHash
		}
		if snap.Version != expectedVersion {
			return out, &SnapshotVersionError{Code: CodeSnapshotVersion, Requested: snap.Version, Fallback: fallbackVersion(prev), Detail: fmt.Sprintf("version chain gap: got %d want %d", snap.Version, expectedVersion)}
		}
		if snap.ParentVersion != expectedParent || snap.ParentChainHash != expectedParentHash {
			return out, &SnapshotVersionError{Code: CodeSnapshotVersion, Requested: snap.Version, Fallback: fallbackVersion(prev), Detail: "parent chain link mismatch"}
		}
		if domain.HashBytes(snap.Payload) != snap.PayloadSHA256 || hashChain(snap.Version, snap.ParentChainHash, snap.PayloadSHA256) != snap.ChainHash {
			return out, &SnapshotVersionError{Code: CodeSnapshotVersion, Requested: snap.Version, Fallback: fallbackVersion(prev), Detail: "snapshot payload or chain hash mismatch"}
		}
		computedContent, err := contentFingerprint(snap.Payload)
		if err != nil || computedContent != snap.ContentHash {
			return out, &SnapshotVersionError{Code: CodeSnapshotVersion, Requested: snap.Version, Fallback: fallbackVersion(prev), Detail: "snapshot content fingerprint mismatch"}
		}
		if prev == nil {
			snap.ChangeKind = "append"
		} else {
			snap.ChangeKind = changeKind(prev.Payload, snap.Payload)
		}
		copySnap := snap
		out = append(out, snap)
		prev = &copySnap
	}
	if err := rows.Err(); err != nil {
		return out, err
	}
	return out, nil
}

func fallbackVersion(prev *Snapshot) int64 {
	if prev == nil {
		return 0
	}
	return prev.Version
}

func (s *DB) ListSnapshotInfo(ctx context.Context) ([]SnapshotInfo, error) {
	chain, err := s.SnapshotChain(ctx)
	infos := make([]SnapshotInfo, 0, len(chain))
	for _, snap := range chain {
		infos = append(infos, SnapshotInfo{
			Version: snap.Version, Kind: snap.Kind, ParentVersion: snap.ParentVersion,
			ParentChainHash: snap.ParentChainHash, PayloadSHA256: snap.PayloadSHA256,
			ChainHash: snap.ChainHash, ContentHash: snap.ContentHash, ChangeKind: snap.ChangeKind,
		})
	}
	if err != nil {
		return infos, err
	}
	return infos, nil
}

// Replay returns the exact stored payload bytes. A requested missing or
// unhealthy version yields E_SNAPSHOT_VERSION plus the nearest healthy payload.
func (s *DB) Replay(ctx context.Context, requested int64) (Snapshot, error) {
	chain, chainErr := s.SnapshotChain(ctx)
	if len(chain) == 0 {
		if chainErr != nil {
			return Snapshot{}, &SnapshotVersionError{Code: CodeSnapshotVersion, Requested: requested, Fallback: 0, Detail: "no healthy snapshot available"}
		}
		return Snapshot{}, &SnapshotVersionError{Code: CodeSnapshotVersion, Requested: requested, Detail: "snapshot history is empty"}
	}
	healthyLast := chain[len(chain)-1]
	if chainErr != nil {
		last := healthyLast
		last.ChangeKind = "append"
		return last, &SnapshotVersionError{Code: CodeSnapshotVersion, Requested: requested, Fallback: healthyLast.Version, FallbackBytes: healthyLast.Payload, Detail: chainErr.Error()}
	}
	if requested <= 0 || requested > healthyLast.Version {
		return healthyLast, &SnapshotVersionError{Code: CodeSnapshotVersion, Requested: requested, Fallback: healthyLast.Version, FallbackBytes: healthyLast.Payload, Detail: "requested snapshot is not available"}
	}
	for _, snap := range chain {
		if snap.Version == requested {
			return snap, nil
		}
	}
	return healthyLast, &SnapshotVersionError{Code: CodeSnapshotVersion, Requested: requested, Fallback: healthyLast.Version, FallbackBytes: healthyLast.Payload, Detail: "requested snapshot is missing from a contiguous chain"}
}

func (s *DB) Latest(ctx context.Context, kind string) (Snapshot, error) {
	chain, err := s.SnapshotChain(ctx)
	if err != nil && len(chain) == 0 {
		return Snapshot{}, err
	}
	for i := len(chain) - 1; i >= 0; i-- {
		if kind == "" || chain[i].Kind == kind {
			return chain[i], nil
		}
	}
	return Snapshot{}, sql.ErrNoRows
}
