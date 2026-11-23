package capver

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"

	_ "modernc.org/sqlite"
)

// Snapshot 是一个带版本号的策略快照（整体替换语义）。
type Snapshot struct {
	Version  int64
	Hash     string
	Policy   Policy
	CommitID int64 // 单调递增提交序号
}

// Store 是基于 SQLite 的策略快照与进程记录存储。
// 所有写操作在单事务内完成；强制终止后已提交事务保留、未提交事务整体丢弃。
type Store struct {
	db *sql.DB
}

const storeSchema = `
CREATE TABLE IF NOT EXISTS snapshots (
    version   INTEGER PRIMARY KEY,
    hash      TEXT NOT NULL,
    commit_id INTEGER NOT NULL,
    payload   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS processes (
    snapshot_version INTEGER NOT NULL,
    proc_id          TEXT NOT NULL,
    declaration      TEXT NOT NULL,
    PRIMARY KEY (snapshot_version, proc_id)
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
`

// OpenStore 打开（必要时创建）快照存储。
func OpenStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(wal)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, newError(CodeStore, "open sqlite", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(storeSchema); err != nil {
		db.Close()
		return nil, newError(CodeStore, "init schema", err)
	}
	return &Store{db: db}, nil
}

// Close 关闭存储。
func (s *Store) Close() error { return s.db.Close() }

// CommitSnapshot 在单个事务内提交新版本快照：版本号 = 当前最大版本 + 1，
// 快照整体写入并保留全部旧快照。
func (s *Store) CommitSnapshot(p *Policy) (int64, error) {
	if err := p.Validate(); err != nil {
		return 0, err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, newError(CodeStore, "begin tx", err)
	}
	defer tx.Rollback()

	var maxVer sql.NullInt64
	if err := tx.QueryRow("SELECT MAX(version) FROM snapshots").Scan(&maxVer); err != nil {
		return 0, newError(CodeStore, "read max version", err)
	}
	var maxCommit sql.NullInt64
	if err := tx.QueryRow("SELECT MAX(commit_id) FROM snapshots").Scan(&maxCommit); err != nil {
		return 0, newError(CodeStore, "read max commit id", err)
	}
	version := maxVer.Int64 + 1
	commitID := maxCommit.Int64 + 1
	payload := string(p.CanonicalJSON())

	if _, err := tx.Exec(
		"INSERT INTO snapshots(version, hash, commit_id, payload) VALUES(?,?,?,?)",
		version, p.Hash(), commitID, payload); err != nil {
		return 0, newError(CodeStore, "insert snapshot", err)
	}
	canon := p.canonical()
	for _, d := range canon.Processes {
		decl, err := json.Marshal(d)
		if err != nil {
			return 0, newError(CodeStore, "marshal declaration", err)
		}
		if _, err := tx.Exec(
			"INSERT INTO processes(snapshot_version, proc_id, declaration) VALUES(?,?,?)",
			version, d.ID, string(decl)); err != nil {
			return 0, newError(CodeStore, "insert process record", err)
		}
	}
	if _, err := tx.Exec(
		"INSERT INTO meta(key, value) VALUES('latest_version', ?) "+
			"ON CONFLICT(key) DO UPDATE SET value=excluded.value",
		fmt.Sprintf("%d", version)); err != nil {
		return 0, newError(CodeStore, "update meta", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, newError(CodeStore, "commit tx", err)
	}
	return version, nil
}

// versions 返回全部已提交版本号（升序）。
func (s *Store) versions() ([]int64, error) {
	rows, err := s.db.Query("SELECT version FROM snapshots ORDER BY version")
	if err != nil {
		return nil, newError(CodeStore, "list versions", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, newError(CodeStore, "scan version", err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// continuousPrefix 计算从 1 开始的连续版本前缀长度。
// 若存在跳号，返回最近连续版本号与是否发现跳号。
func continuousPrefix(versions []int64) (lastContinuous int64, gap bool) {
	expect := int64(1)
	for _, v := range versions {
		if v != expect {
			return expect - 1, true
		}
		expect++
	}
	return expect - 1, false
}

// LoadLatest 加载最近连续版本的快照。
// 发现版本链跳号时返回 E_SNAPSHOT_VERSION，并回退到最近连续版本（通过返回的 Snapshot）。
func (s *Store) LoadLatest() (*Snapshot, error) {
	versions, err := s.versions()
	if err != nil {
		return nil, err
	}
	if len(versions) == 0 {
		return nil, newError(CodeSnapshotVersion, "no committed snapshot", nil)
	}
	last, gap := continuousPrefix(versions)
	snap, err := s.loadVersion(last)
	if err != nil {
		return nil, err
	}
	if gap {
		return snap, newError(CodeSnapshotVersion,
			fmt.Sprintf("version chain gap detected, fallback to continuous version %d", last), nil)
	}
	return snap, nil
}

// Chain 返回版本链信息（供报告与回放）。
func (s *Store) Chain() (versions []int64, lastContinuous int64, gap bool, err error) {
	versions, err = s.versions()
	if err != nil {
		return nil, 0, false, err
	}
	lastContinuous, gap = continuousPrefix(versions)
	return versions, lastContinuous, gap, nil
}

// loadVersion 读取指定版本快照。
func (s *Store) loadVersion(v int64) (*Snapshot, error) {
	var hash, payload string
	var commitID int64
	err := s.db.QueryRow(
		"SELECT hash, commit_id, payload FROM snapshots WHERE version=?", v).
		Scan(&hash, &commitID, &payload)
	if err == sql.ErrNoRows {
		return nil, newError(CodeSnapshotVersion, fmt.Sprintf("snapshot version %d not found", v), nil)
	}
	if err != nil {
		return nil, newError(CodeStore, "read snapshot", err)
	}
	var p Policy
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		return nil, newError(CodeStore, "decode snapshot payload", err)
	}
	snap := &Snapshot{Version: v, Hash: hash, Policy: p, CommitID: commitID}
	// 校验内容哈希与版本号分离：识别整体替换式变化。
	if snap.Policy.Hash() != hash {
		return nil, newError(CodeStore, fmt.Sprintf("snapshot %d hash mismatch", v), nil)
	}
	return snap, nil
}

// LoadVersion 加载指定版本快照（用于回放）。
func (s *Store) LoadVersion(v int64) (*Snapshot, error) {
	return s.loadVersion(v)
}

// ProcessRecords 读取指定快照版本下的进程记录（按进程 ID 排序）。
func (s *Store) ProcessRecords(version int64) ([]ProcessDecl, error) {
	rows, err := s.db.Query(
		"SELECT declaration FROM processes WHERE snapshot_version=? ORDER BY proc_id", version)
	if err != nil {
		return nil, newError(CodeStore, "read process records", err)
	}
	defer rows.Close()
	var out []ProcessDecl
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, newError(CodeStore, "scan process record", err)
		}
		var d ProcessDecl
		if err := json.Unmarshal([]byte(payload), &d); err != nil {
			return nil, newError(CodeStore, "decode process record", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// InjectSnapshotGap 异常注入：在版本链中制造跳号（写入 version = max+skip 的副本）。
// 仅由外部参数触发，用于验证 E_SNAPSHOT_VERSION 与回退逻辑；不修改已提交快照内容。
func (s *Store) InjectSnapshotGap(skip int64) (int64, error) {
	if skip < 2 {
		skip = 2
	}
	versions, err := s.versions()
	if err != nil {
		return 0, err
	}
	if len(versions) == 0 {
		return 0, newError(CodeSnapshotVersion, "no committed snapshot to gap", nil)
	}
	latest := versions[len(versions)-1]
	snap, err := s.loadVersion(latest)
	if err != nil {
		return 0, err
	}
	gapVersion := latest + skip
	payload := string(snap.Policy.CanonicalJSON())
	_, err = s.db.Exec(
		"INSERT INTO snapshots(version, hash, commit_id, payload) VALUES(?,?,?,?)",
		gapVersion, snap.Hash, snap.CommitID+1, payload)
	if err != nil {
		return 0, newError(CodeStore, "inject gap snapshot", err)
	}
	return gapVersion, nil
}

// Recover 重启恢复入口：仅依赖 SQLite 已提交事务。
// 返回最近连续快照与恢复结论；跳号时返回 E_SNAPSHOT_VERSION 并保留回放缓照。
func (s *Store) Recover() (*Snapshot, RecoveryConclusion, error) {
	snap, err := s.LoadLatest()
	conclusion := RecoveryClean
	if CodeOf(err) == CodeSnapshotVersion && snap != nil {
		conclusion = RecoveryFallback
	}
	if err != nil && snap == nil {
		return nil, RecoveryFailed, err
	}
	return snap, conclusion, err
}

// RecoveryConclusion 恢复结论。
type RecoveryConclusion string

const (
	RecoveryClean    RecoveryConclusion = "clean"
	RecoveryFallback RecoveryConclusion = "fallback-to-continuous"
	RecoveryFailed   RecoveryConclusion = "failed"
)

// ensure deterministic ordering helper for chain slices
func sortedVersions(v []int64) []int64 {
	out := append([]int64(nil), v...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
