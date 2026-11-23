package store

import (
	"database/sql"
	"encoding/json"
	"fmt"

	_ "modernc.org/sqlite"

	"mitmpin/internal/policy"
	"mitmpin/internal/sample"
)

// Store 封装管线的 SQLite 持久化。
type Store struct {
	db *sql.DB
}

// Checkpoint 是一条连接按事件提交的检查点。
type Checkpoint struct {
	ConnID        string `json:"conn_id"`
	CheckpointVer int    `json:"checkpoint_version"` // 检查点版本链，随事件 1..N 递增
	EventSeq      int    `json:"event_seq"`
	EventType     string `json:"event_type"`
	PolicyVersion int    `json:"policy_version"`
	PinCode       string `json:"pin_code,omitempty"` // 最近一次固定校验中间结论
	TraceJSON     string `json:"trace_json"`
	Marker        string `json:"marker"`
}

// Open 打开（必要时创建）数据文件并执行迁移。
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // SQLite 单写者，串行化事务，保证热更新原子性
	st := &Store{db: db}
	if err := st.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return st, nil
}

// Close 关闭底层数据库。
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS policy_snapshots (
			version INTEGER PRIMARY KEY,
			marker TEXT NOT NULL,
			content_hash TEXT NOT NULL,
			payload TEXT NOT NULL,
			committed INTEGER NOT NULL DEFAULT 1
		)`,
		`CREATE TABLE IF NOT EXISTS snapshot_chain (
			ord INTEGER PRIMARY KEY AUTOINCREMENT,
			version INTEGER NOT NULL UNIQUE,
			content_hash TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS observations (
			conn_id TEXT PRIMARY KEY,
			payload TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS checkpoints (
			conn_id TEXT NOT NULL,
			checkpoint_version INTEGER NOT NULL,
			event_seq INTEGER NOT NULL,
			event_type TEXT NOT NULL,
			policy_version INTEGER NOT NULL,
			pin_code TEXT NOT NULL DEFAULT '',
			trace_json TEXT NOT NULL,
			marker TEXT NOT NULL,
			PRIMARY KEY (conn_id, checkpoint_version)
		)`,
		`CREATE TABLE IF NOT EXISTS connection_results (
			conn_id TEXT PRIMARY KEY,
			policy_version INTEGER NOT NULL,
			final_code TEXT NOT NULL,
			processed_events INTEGER NOT NULL,
			trace_json TEXT NOT NULL,
			marker TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS run_meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("迁移失败: %w", err)
		}
	}
	return nil
}

// CommitSnapshot 在单个事务内整体提交策略快照并推进快照版本链。
// 版本号必须严格大于当前最新版本；内容与最新快照一致时返回 E_POLICY_UNCHANGED。
func (s *Store) CommitSnapshot(snap sample.Snapshot) error {
	if err := sample.ValidateSnapshot(snap); err != nil {
		return err
	}
	payload, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	contentHash := policy.ContentHash(snap)

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var latestVersion int
	var latestHash sql.NullString
	row := tx.QueryRow(`SELECT version, content_hash FROM policy_snapshots ORDER BY version DESC LIMIT 1`)
	if err := row.Scan(&latestVersion, &latestHash); err != nil && err != sql.ErrNoRows {
		return err
	}
	if latestHash.Valid && latestHash.String == contentHash {
		return sample.NewBizError(sample.E_POLICY_UNCHANGED,
			fmt.Sprintf("快照 v%d 与最新 v%d 策略内容完全一致，拒绝空更新", snap.Version, latestVersion))
	}
	if latestHash.Valid && snap.Version <= latestVersion {
		return sample.NewBizError(sample.E_POLICY_VERSION_CONFLICT,
			fmt.Sprintf("快照版本 %d 未大于当前最新版本 %d", snap.Version, latestVersion))
	}
	if _, err := tx.Exec(
		`INSERT INTO policy_snapshots(version, marker, content_hash, payload) VALUES(?, ?, ?, ?)`,
		snap.Version, snap.Marker, contentHash, string(payload)); err != nil {
		return err
	}
	if _, err := tx.Exec(
		`INSERT INTO snapshot_chain(version, content_hash) VALUES(?, ?)`,
		snap.Version, contentHash); err != nil {
		return err
	}
	return tx.Commit()
}

// Snapshots 返回已提交快照（按版本升序），含内容哈希。
type StoredSnapshot struct {
	Snapshot    sample.Snapshot
	ContentHash string
}

// ListSnapshots 读取全部已提交快照。
func (s *Store) ListSnapshots() ([]StoredSnapshot, error) {
	rows, err := s.db.Query(`SELECT payload, content_hash FROM policy_snapshots ORDER BY version ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StoredSnapshot
	for rows.Next() {
		var payload, hash string
		if err := rows.Scan(&payload, &hash); err != nil {
			return nil, err
		}
		var snap sample.Snapshot
		if err := json.Unmarshal([]byte(payload), &snap); err != nil {
			return nil, err
		}
		out = append(out, StoredSnapshot{Snapshot: snap, ContentHash: hash})
	}
	return out, rows.Err()
}

// SnapshotChain 按提交顺序返回 (版本号, 内容哈希)。
func (s *Store) SnapshotChain() ([][2]string, error) {
	rows, err := s.db.Query(`SELECT version, content_hash FROM snapshot_chain ORDER BY ord ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var chain [][2]string
	for rows.Next() {
		var version int
		var hash string
		if err := rows.Scan(&version, &hash); err != nil {
			return nil, err
		}
		chain = append(chain, [2]string{fmt.Sprintf("%d", version), hash})
	}
	return chain, rows.Err()
}

// SaveObservation 持久化原始观测，使重启回放自包含、不依赖外部文件。
func (s *Store) SaveObservation(obs sample.Observation) error {
	payload, err := json.Marshal(obs)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT OR REPLACE INTO observations(conn_id, payload) VALUES(?, ?)`,
		obs.ConnID, string(payload))
	return err
}

// SaveCheckpoint 提交单事件检查点。
func (s *Store) SaveCheckpoint(cp Checkpoint) error {
	_, err := s.db.Exec(`INSERT INTO checkpoints
		(conn_id, checkpoint_version, event_seq, event_type, policy_version, pin_code, trace_json, marker)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
		cp.ConnID, cp.CheckpointVer, cp.EventSeq, cp.EventType, cp.PolicyVersion, cp.PinCode,
		cp.TraceJSON, cp.Marker)
	return err
}

// ListCheckpoints 返回某连接已提交检查点（按检查点版本升序）。
func (s *Store) ListCheckpoints(connID string) ([]Checkpoint, error) {
	rows, err := s.db.Query(`SELECT conn_id, checkpoint_version, event_seq, event_type,
		policy_version, pin_code, trace_json, marker
		FROM checkpoints WHERE conn_id = ? ORDER BY checkpoint_version ASC`, connID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Checkpoint
	for rows.Next() {
		var cp Checkpoint
		if err := rows.Scan(&cp.ConnID, &cp.CheckpointVer, &cp.EventSeq, &cp.EventType,
			&cp.PolicyVersion, &cp.PinCode, &cp.TraceJSON, &cp.Marker); err != nil {
			return nil, err
		}
		out = append(out, cp)
	}
	return out, rows.Err()
}

// RollbackCheckpointsAfter 删除检查点版本大于 keepVer 的所有检查点（回退到最近连续版本）。
func (s *Store) RollbackCheckpointsAfter(connID string, keepVer int) error {
	_, err := s.db.Exec(`DELETE FROM checkpoints WHERE conn_id = ? AND checkpoint_version > ?`,
		connID, keepVer)
	return err
}

// SaveResult 写入连接结算终态。
func (s *Store) SaveResult(connID string, policyVersion int, finalCode string, processedEvents int, traceJSON, marker string) error {
	_, err := s.db.Exec(`INSERT OR REPLACE INTO connection_results
		(conn_id, policy_version, final_code, processed_events, trace_json, marker)
		VALUES(?, ?, ?, ?, ?, ?)`,
		connID, policyVersion, finalCode, processedEvents, traceJSON, marker)
	return err
}

// SetMeta / GetMeta 记录运行期元数据（如预算裁决），不参与报告确定性字段。
func (s *Store) SetMeta(key, value string) error {
	_, err := s.db.Exec(`INSERT OR REPLACE INTO run_meta(key, value) VALUES(?, ?)`, key, value)
	return err
}

// DB 暴露底层连接，仅供异常注入等受控场景使用。
func (s *Store) DB() *sql.DB { return s.db }
