package smuggle

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"sync"

	_ "modernc.org/sqlite"
)

// SnapshotMarker 是快照的固定采样标记，不使用墙钟时间。
const SnapshotMarker = "SNAPSHOT-SAMPLE-0001"

// Snapshot 是一次提交的带版本号裁决快照。
type Snapshot struct {
	ConnID  string `json:"conn_id"`
	Version int64  `json:"version"`
	Payload []byte `json:"payload"`
	Hash    string `json:"hash"`
	Marker  string `json:"marker"`
}

// Store 将裁决快照持久化到 SQLite。
type Store struct {
	db *sql.DB
	mu sync.Mutex
}

// OpenStore 打开（必要时创建）快照存储。
func OpenStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS snapshots (
		conn_id TEXT NOT NULL,
		version INTEGER NOT NULL,
		payload BLOB NOT NULL,
		hash TEXT NOT NULL,
		marker TEXT NOT NULL,
		PRIMARY KEY (conn_id, version)
	)`); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close 关闭存储。
func (s *Store) Close() error { return s.db.Close() }

// Commit 以递增版本号提交一个连接的快照。
// 主键约束保证已提交快照不会被部分覆盖；提交在事务内完成。
func (s *Store) Commit(connID string, payload []byte) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return Snapshot{}, err
	}
	var version int64
	if err := tx.QueryRow(`SELECT COALESCE(MAX(version),0) FROM snapshots WHERE conn_id=?`, connID).Scan(&version); err != nil {
		tx.Rollback()
		return Snapshot{}, err
	}
	version++
	sum := sha256.Sum256(payload)
	snap := Snapshot{
		ConnID:  connID,
		Version: version,
		Payload: payload,
		Hash:    hex.EncodeToString(sum[:]),
		Marker:  SnapshotMarker,
	}
	if _, err := tx.Exec(`INSERT INTO snapshots(conn_id,version,payload,hash,marker) VALUES(?,?,?,?,?)`,
		connID, version, payload, snap.Hash, snap.Marker); err != nil {
		tx.Rollback()
		return Snapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return Snapshot{}, err
	}
	return snap, nil
}

// LoadChain 加载一个连接的快照版本链并校验连续性。
// 发现跳号时返回最近连续版本前缀与 E_SNAPSHOT_VERSION。
func (s *Store) LoadChain(connID string) ([]Snapshot, Code) {
	rows, err := s.db.Query(`SELECT conn_id,version,payload,hash,marker FROM snapshots WHERE conn_id=? ORDER BY version`, connID)
	if err != nil {
		return nil, CodeSnapshotVersion
	}
	defer rows.Close()
	var out []Snapshot
	var expect int64 = 1
	for rows.Next() {
		var sn Snapshot
		if err := rows.Scan(&sn.ConnID, &sn.Version, &sn.Payload, &sn.Hash, &sn.Marker); err != nil {
			return out, CodeSnapshotVersion
		}
		if sn.Version != expect {
			// 版本链跳号：回退到最近连续版本。
			return out, CodeSnapshotVersion
		}
		out = append(out, sn)
		expect++
	}
	return out, CodeOK
}

// IsWholesaleReplacement 通过内容哈希识别整体替换式版本变化，
// 而非仅比对版本号。
func IsWholesaleReplacement(a, b Snapshot) bool {
	return a.Hash != b.Hash
}
