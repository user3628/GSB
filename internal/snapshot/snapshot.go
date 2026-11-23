package snapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	_ "modernc.org/sqlite"

	"depverify/internal/model"
)

// Record 是带版本号的核验快照。
type Record struct {
	Version     int                 `json:"version"`
	ContentHash string              `json:"content_hash"`
	Roots       []string            `json:"roots"`
	Result      model.ClosureResult `json:"result"`
}

// LoadReport 描述加载快照时的版本链状态。
type LoadReport struct {
	RequestedVersion int    `json:"requested_version"`
	LoadedVersion    int    `json:"loaded_version"`
	FallbackVersion  int    `json:"fallback_version"`
	Continuous       bool   `json:"continuous"`
	Code             string `json:"code,omitempty"`
	Message          string `json:"message,omitempty"`
}

// Store 封装 SQLite 持久化，全部写操作在单事务内提交。
type Store struct {
	db *sql.DB
}

// Open 打开（或创建）快照库并初始化 schema。
func Open(path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) init() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS snapshots (
    version      INTEGER PRIMARY KEY,
    content_hash TEXT NOT NULL,
    roots        TEXT NOT NULL,
    payload      BLOB NOT NULL,
    created_marker TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS task_runs (
    task_id    TEXT PRIMARY KEY,
    state      TEXT NOT NULL,
    updated_marker TEXT NOT NULL,
    detail     TEXT NOT NULL
);`)
	return err
}

// DB 暴露底层连接，供任务进度恢复复用同一事务边界。
func (s *Store) DB() *sql.DB { return s.db }

// Close 关闭连接。
func (s *Store) Close() error { return s.db.Close() }

// Save 在单个事务内提交递增版本号快照。
// 内容哈希覆盖完整结论，因此整体替换式变化不会因版本号相同而漏判。
func (s *Store) Save(ctx context.Context, result model.ClosureResult, roots []string) (Record, error) {
	payload, err := json.Marshal(result)
	if err != nil {
		return Record{}, err
	}
	sum := sha256.Sum256(payload)
	rec := Record{
		ContentHash: hex.EncodeToString(sum[:]),
		Roots:       append([]string(nil), roots...),
		Result:      result,
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Record{}, err
	}
	defer func() { _ = tx.Rollback() }()

	var next int
	row := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) + 1 FROM snapshots`)
	if err := row.Scan(&next); err != nil {
		return Record{}, err
	}
	rec.Version = next
	rec.Result.SampleMarker = model.FixedSampleMarker
	payload, err = json.Marshal(rec.Result)
	if err != nil {
		return Record{}, err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO snapshots(version, content_hash, roots, payload, created_marker) VALUES (?, ?, ?, ?, ?)`,
		next, rec.ContentHash, string(mustJSON(rec.Roots)), payload, model.FixedSampleMarker); err != nil {
		return Record{}, err
	}
	if err := tx.Commit(); err != nil {
		return Record{}, err
	}
	return rec, nil
}

// SaveOrReuse 在单事务内提交快照；若最新快照内容哈希相同则复用该版本，
// 保证相同输入重复运行得到字节级一致的报告，同时仍能识别整体替换式变化。
func (s *Store) SaveOrReuse(ctx context.Context, result model.ClosureResult, roots []string) (Record, error) {
	payload, err := json.Marshal(result)
	if err != nil {
		return Record{}, err
	}
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Record{}, err
	}
	defer func() { _ = tx.Rollback() }()

	var latestVersion int
	var latestHash string
	row := tx.QueryRowContext(ctx, `SELECT version, content_hash FROM snapshots ORDER BY version DESC LIMIT 1`)
	switch err := row.Scan(&latestVersion, &latestHash); {
	case err == nil && latestHash == hash:
		// 内容未变化：复用既有版本，不推进版本号。
		var existing []byte
		if err := tx.QueryRowContext(ctx, `SELECT payload FROM snapshots WHERE version = ?`, latestVersion).Scan(&existing); err != nil {
			return Record{}, err
		}
		rec, derr := decodeRecord(latestVersion, existing)
		if derr != nil {
			return Record{}, derr
		}
		rec.Roots = append([]string(nil), roots...)
		if err := tx.Commit(); err != nil {
			return Record{}, err
		}
		return rec, nil
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		return Record{}, err
	}

	var next int
	row = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) + 1 FROM snapshots`)
	if err := row.Scan(&next); err != nil {
		return Record{}, err
	}
	rec := Record{Version: next, ContentHash: hash, Roots: append([]string(nil), roots...), Result: result}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO snapshots(version, content_hash, roots, payload, created_marker) VALUES (?, ?, ?, ?, ?)`,
		next, hash, string(mustJSON(roots)), payload, model.FixedSampleMarker); err != nil {
		return Record{}, err
	}
	if err := tx.Commit(); err != nil {
		return Record{}, err
	}
	return rec, nil
}

// Latest 返回最近一次提交的快照；库为空时返回 EOF 哨兵错误。
func (s *Store) Latest(ctx context.Context) (Record, error) {
	row := s.db.QueryRowContext(ctx, `SELECT version, payload FROM snapshots ORDER BY version DESC LIMIT 1`)
	var version int
	var payload []byte
	if err := row.Scan(&version, &payload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Record{}, err
		}
		return Record{}, err
	}
	return decodeRecord(version, payload)
}

// Load 校验版本链连续性并加载目标版本快照。
// 发现跳号时返回 E_SNAPSHOT_VERSION，并回退到最近连续版本（从 1 起无空洞的最大版本）。
func (s *Store) Load(ctx context.Context, requested int) (Record, LoadReport, error) {
	report := LoadReport{RequestedVersion: requested}
	rows, err := s.db.QueryContext(ctx, `SELECT version, payload FROM snapshots ORDER BY version ASC`)
	if err != nil {
		return Record{}, report, err
	}
	defer rows.Close()

	records := make(map[int][]byte)
	maxVersion := 0
	for rows.Next() {
		var version int
		var payload []byte
		if err := rows.Scan(&version, &payload); err != nil {
			return Record{}, report, err
		}
		records[version] = payload
		if version > maxVersion {
			maxVersion = version
		}
	}
	if err := rows.Err(); err != nil {
		return Record{}, report, err
	}
	if len(records) == 0 {
		return Record{}, report, errors.New("snapshot store is empty")
	}

	// 版本链：从 1 起必须连续，首个空洞即回退点。
	continuousTop := 0
	for v := 1; ; v++ {
		if _, ok := records[v]; !ok {
			break
		}
		continuousTop = v
	}
	target := requested
	if target <= 0 || target > maxVersion {
		target = maxVersion
	}
	report.LoadedVersion = target
	if target <= continuousTop {
		report.Continuous = true
		rec, err := decodeRecord(target, records[target])
		report.LoadedVersion = target
		return rec, report, err
	}

	report.Continuous = false
	report.Code = model.ESnapshotVersion
	report.FallbackVersion = continuousTop
	report.Message = fmt.Sprintf("version chain gap detected before version %d; falling back to %d",
		target, continuousTop)
	rec, err := decodeRecord(continuousTop, records[continuousTop])
	report.LoadedVersion = continuousTop
	return rec, report, &ChainError{Report: report, Err: err}
}

// ChainError 携带回退报告，调用方可通过 errors.As 提取。
type ChainError struct {
	Report LoadReport
	Err    error
}

func (e *ChainError) Error() string { return e.Report.Message }
func (e *ChainError) Unwrap() error { return e.Err }

// CompareContent 比较两条快照的内容哈希，识别整体替换式版本变化。
func CompareContent(a, b Record) bool {
	return bytes.Equal([]byte(a.ContentHash), []byte(b.ContentHash))
}

// InjectGap 仅供异常注入测试使用：在连续版本链中制造一个跳号空洞。
func (s *Store) InjectGap(ctx context.Context, gapVersion int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `DELETE FROM snapshots WHERE version = ?`, gapVersion); err != nil {
		return err
	}
	return tx.Commit()
}

// AllVersions 返回已存储的全部版本号（升序）。
func (s *Store) AllVersions(ctx context.Context) ([]int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT version FROM snapshots ORDER BY version ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func decodeRecord(version int, payload []byte) (Record, error) {
	var result model.ClosureResult
	if err := json.Unmarshal(payload, &result); err != nil {
		return Record{}, err
	}
	sum := sha256.Sum256(payload)
	return Record{
		Version:     version,
		ContentHash: hex.EncodeToString(sum[:]),
		Result:      result,
		Roots:       result.Roots,
	}, nil
}

func mustJSON(v []string) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
