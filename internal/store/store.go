// Package store 提供基于 SQLite 的路由表持久化、
// 写意图记录与强制终止后的恢复扫描。
package store

import (
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"gsb/internal/format"
)

// Store 封装 SQLite 持久化。
type Store struct {
	db *sql.DB
}

// Open 打开（必要时创建）数据库并初始化表结构。
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS routes(
			model_id TEXT PRIMARY KEY,
			endpoint TEXT NOT NULL,
			vram_bytes INTEGER NOT NULL,
			concurrency INTEGER NOT NULL,
			toolchain TEXT NOT NULL,
			base_time_ms INTEGER NOT NULL,
			retention_ms INTEGER NOT NULL,
			expires_at_ms INTEGER NOT NULL,
			last_write_ms INTEGER NOT NULL,
			version INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS intents(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			kind TEXT NOT NULL,
			detail TEXT NOT NULL,
			status TEXT NOT NULL,
			created_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS hits(
			model_id TEXT PRIMARY KEY,
			hit_count INTEGER NOT NULL,
			last_hit_ms INTEGER NOT NULL
		)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			return nil, err
		}
	}
	return &Store{db: db}, nil
}

// Close 关闭数据库。
func (s *Store) Close() error { return s.db.Close() }

// TableVersion 返回当前已提交的路由表版本。
func (s *Store) TableVersion() int64 {
	var v int64
	_ = s.db.QueryRow(`SELECT COALESCE(MAX(version),0) FROM routes`).Scan(&v)
	return v
}

// MetaInt 读取整型元数据。
func (s *Store) MetaInt(key string) int64 {
	var v int64
	_ = s.db.QueryRow(`SELECT CAST(value AS INTEGER) FROM meta WHERE key=?`, key).Scan(&v)
	return v
}

// SetMeta 在独立事务中写元数据。
func (s *Store) SetMeta(key string, value int64) error {
	_, err := s.db.Exec(`INSERT INTO meta(key,value) VALUES(?,?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, fmt.Sprint(value))
	return err
}

// BeginIntent 先写 SQLite 意图记录（独立事务立即落盘），返回意图 ID。
func (s *Store) BeginIntent(kind, detail string) (int64, error) {
	res, err := s.db.Exec(`INSERT INTO intents(kind,detail,status,created_ms) VALUES(?,?, 'pending', ?)`,
		kind, detail, time.Now().UnixMilli())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// FinishIntent 标记意图完成。
func (s *Store) FinishIntent(id int64) error {
	_, err := s.db.Exec(`UPDATE intents SET status='committed' WHERE id=?`, id)
	return err
}

// PendingIntent 描述一条未完成的意图。
type PendingIntent struct {
	ID        int64  `json:"id"`
	Kind      string `json:"kind"`
	Detail    string `json:"detail"`
	CreatedMs int64  `json:"created_ms"`
}

// PendingIntents 扫描未完成意图。
func (s *Store) PendingIntents() ([]PendingIntent, error) {
	rows, err := s.db.Query(`SELECT id,kind,detail,created_ms FROM intents WHERE status='pending' ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PendingIntent
	for rows.Next() {
		var p PendingIntent
		if err := rows.Scan(&p.ID, &p.Kind, &p.Detail, &p.CreatedMs); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// AbortIntent 把未完成意图标记为已回滚。
func (s *Store) AbortIntent(id int64) error {
	_, err := s.db.Exec(`UPDATE intents SET status='aborted' WHERE id=?`, id)
	return err
}

// LoadRoutes 加载某个完整提交版本的全部路由。
func (s *Store) LoadRoutes() ([]format.RouteRecord, int64, error) {
	version := s.TableVersion()
	rows, err := s.db.Query(`SELECT model_id,endpoint,vram_bytes,concurrency,toolchain,
		base_time_ms,retention_ms,expires_at_ms,last_write_ms FROM routes WHERE version=?`, version)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var recs []format.RouteRecord
	for rows.Next() {
		var r format.RouteRecord
		if err := rows.Scan(&r.ModelID, &r.Endpoint, &r.VRAMBytes, &r.Concurrency,
			&r.Toolchain, &r.BaseTimeMs, &r.RetentionMs, &r.ExpiresAtMs, &r.LastWriteMs); err != nil {
			return nil, 0, err
		}
		recs = append(recs, r)
	}
	return recs, version, rows.Err()
}

// CommitBatch 在单个事务中应用一批路由变更并标记意图完成，
// 同时落盘 committed_version。崩溃时整个事务回滚，
// 路由表保持最后一个完整版本。
func (s *Store) CommitBatch(intentID, version int64, upserts []format.RouteRecord, replaceAll bool) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if replaceAll {
		if _, err := tx.Exec(`DELETE FROM routes`); err != nil {
			return err
		}
	}
	for _, r := range upserts {
		if _, err := tx.Exec(`INSERT INTO routes(model_id,endpoint,vram_bytes,concurrency,toolchain,
			base_time_ms,retention_ms,expires_at_ms,last_write_ms,version)
			VALUES(?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(model_id) DO UPDATE SET endpoint=excluded.endpoint,
			vram_bytes=excluded.vram_bytes, concurrency=excluded.concurrency,
			toolchain=excluded.toolchain, base_time_ms=excluded.base_time_ms,
			retention_ms=excluded.retention_ms, expires_at_ms=excluded.expires_at_ms,
			last_write_ms=excluded.last_write_ms, version=excluded.version`,
			r.ModelID, r.Endpoint, r.VRAMBytes, r.Concurrency, r.Toolchain,
			r.BaseTimeMs, r.RetentionMs, r.ExpiresAtMs, r.LastWriteMs, version); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(`INSERT INTO meta(key,value) VALUES('committed_version',?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, fmt.Sprint(version)); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE intents SET status='committed' WHERE id=?`, intentID); err != nil {
		return err
	}
	return tx.Commit()
}

// RecordHit 累加模型命中统计。
func (s *Store) RecordHit(modelID string, nowMs int64) error {
	_, err := s.db.Exec(`INSERT INTO hits(model_id,hit_count,last_hit_ms) VALUES(?,1,?)
		ON CONFLICT(model_id) DO UPDATE SET hit_count=hit_count+1, last_hit_ms=excluded.last_hit_ms`,
		modelID, nowMs)
	return err
}

// HitStat 是模型命中统计。
type HitStat struct {
	ModelID   string `json:"model_id"`
	HitCount  int64  `json:"hit_count"`
	LastHitMs int64  `json:"last_hit_ms"`
}

// LoadHits 读取全部命中统计。
func (s *Store) LoadHits() ([]HitStat, error) {
	rows, err := s.db.Query(`SELECT model_id,hit_count,last_hit_ms FROM hits ORDER BY model_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []HitStat
	for rows.Next() {
		var h HitStat
		if err := rows.Scan(&h.ModelID, &h.HitCount, &h.LastHitMs); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// ResetHits 清空命中统计（快照切换成功后释放派生统计）。
func (s *Store) ResetHits() error {
	_, err := s.db.Exec(`DELETE FROM hits`)
	return err
}
