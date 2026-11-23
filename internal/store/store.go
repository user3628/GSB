package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Store 封装 SQLite 连接与意图日志/恢复裁决。
type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_txlock=immediate")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // 单写连接，配合事务保证跨模块一致性
	if _, err := db.Exec(schemaSQL); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }
func (s *Store) DB() *sql.DB  { return s.db }

func nowMillis() int64 { return time.Now().UnixMilli() }

// BeginIntent 开启事务并先落一条 OPEN 意图（同一事务内），满足“先落意图再提交”。
func (s *Store) BeginIntent(ctx context.Context, roundID, kind string, payload any) (*sql.Tx, int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, 0, err
	}
	payloadJSON, _ := json.Marshal(payload)
	res, err := tx.ExecContext(ctx,
		`INSERT INTO intents(round_id, kind, payload_json, status, created_at) VALUES(?,?,?,?,?)`,
		roundID, kind, string(payloadJSON), "OPEN", nowMillis())
	if err != nil {
		_ = tx.Rollback()
		return nil, 0, err
	}
	intentID, _ := res.LastInsertId()
	return tx, intentID, nil
}

// CommitIntent 在同一事务内把意图标记为 COMMITTED 并提交，构成完整提交边界。
func (s *Store) CommitIntent(ctx context.Context, tx *sql.Tx, intentID int64) error {
	if _, err := tx.ExecContext(ctx,
		`UPDATE intents SET status='COMMITTED', resolved_at=? WHERE id=?`,
		nowMillis(), intentID); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// AbortIntent 回滚事务（OPEN 意图随之消失），供业务失败路径使用。
func AbortIntent(tx *sql.Tx) { _ = tx.Rollback() }

// CommitOpenIntent 用独立事务把此前已落盘的 OPEN 意图推进为 COMMITTED，
// 构成第二阶段完整提交边界；进程在此前被强杀时，OPEN 意图保留并由恢复流程裁决。
func (s *Store) CommitOpenIntent(ctx context.Context, intentID int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE intents SET status='COMMITTED', resolved_at=? WHERE id=? AND status='OPEN'`,
		nowMillis(), intentID); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// IntentStatus 读取意图当前状态。
func (s *Store) IntentStatus(ctx context.Context, intentID int64) (string, error) {
	var status string
	err := s.db.QueryRowContext(ctx, `SELECT status FROM intents WHERE id=?`, intentID).
		Scan(&status)
	return status, err
}

// RecoveryDetail 是只基于已落盘事实的恢复差异明细。
type RecoveryDetail struct {
	RecoveredAt          int64    `json:"recovered_at"`
	OpenIntents          []Intent `json:"open_intents"`
	AbortedRounds        []string `json:"aborted_rounds"`
	RolledBackRequests   []int64  `json:"rolled_back_requests"`
	RolledBackDispatches []int64  `json:"rolled_back_dispatches"`
	SnapshotKept         int64    `json:"snapshot_kept_version"`
	Notes                []string `json:"notes"`
}

type Intent struct {
	ID          int64  `json:"id"`
	RoundID     string `json:"round_id"`
	Kind        string `json:"kind"`
	PayloadJSON string `json:"payload_json"`
	Status      string `json:"status"`
	CreatedAt   int64  `json:"created_at"`
}

// RecoverOnStartup 扫描未完成意图并裁决：
//  1. 未完成探测/调度轮次标记为 ABORTED；
//  2. 未提交派发回滚为待调度；
//  3. 权重分配保持最后一个 COMMITTED 快照（OPEN 预备事务从未提交，天然回滚）。
func (s *Store) RecoverOnStartup(ctx context.Context) (*RecoveryDetail, error) {
	detail := &RecoveryDetail{RecoveredAt: nowMillis()}

	rows, err := s.db.QueryContext(ctx,
		`SELECT id, round_id, kind, payload_json, status, created_at FROM intents WHERE status='OPEN' ORDER BY id`)
	if err != nil {
		return nil, err
	}
	var open []Intent
	for rows.Next() {
		var it Intent
		if err := rows.Scan(&it.ID, &it.RoundID, &it.Kind, &it.PayloadJSON, &it.Status, &it.CreatedAt); err != nil {
			rows.Close()
			return nil, err
		}
		open = append(open, it)
	}
	rows.Close()
	detail.OpenIntents = open
	if len(open) == 0 {
		detail.Notes = append(detail.Notes, "no open intent found, storage already at last committed boundary")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	for _, it := range open {
		if _, err := tx.ExecContext(ctx,
			`UPDATE intents SET status='ABORTED', resolved_at=? WHERE id=?`, nowMillis(), it.ID); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
		if it.RoundID != "" {
			if res, err := tx.ExecContext(ctx,
				`UPDATE rounds SET status='ABORTED', finished_at=? WHERE id=? AND status IN ('RUNNING','PENDING','PLANNED')`,
				nowMillis(), it.RoundID); err != nil {
				_ = tx.Rollback()
				return nil, err
			} else if n, _ := res.RowsAffected(); n > 0 {
				detail.AbortedRounds = append(detail.AbortedRounds, it.RoundID)
			}
		}

		if it.Kind == "dispatch" && it.RoundID != "" {
			dispRows, err := tx.QueryContext(ctx,
				`SELECT id, request_seq FROM dispatches WHERE round_id=? AND status IN ('PENDING','RUNNING') ORDER BY id`, it.RoundID)
			if err != nil {
				_ = tx.Rollback()
				return nil, err
			}
			for dispRows.Next() {
				var dispatchID int64
				var reqSeq int64
				if err := dispRows.Scan(&dispatchID, &reqSeq); err != nil {
					dispRows.Close()
					_ = tx.Rollback()
					return nil, err
				}
				detail.RolledBackDispatches = append(detail.RolledBackDispatches, dispatchID)
				detail.RolledBackRequests = append(detail.RolledBackRequests, reqSeq)
			}
			dispRows.Close()
			if _, err := tx.ExecContext(ctx,
				`UPDATE dispatches SET status='ROLLED_BACK', finished_at=? WHERE round_id=? AND status IN ('PENDING','RUNNING')`,
				nowMillis(), it.RoundID); err != nil {
				_ = tx.Rollback()
				return nil, err
			}
			if _, err := tx.ExecContext(ctx,
				`UPDATE requests SET status='PENDING', round_id=NULL, decision_json=NULL WHERE round_id=? AND status='DISPATCHED'`,
				it.RoundID); err != nil {
				_ = tx.Rollback()
				return nil, err
			}
		}
	}

	var kept sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(version) FROM snapshots`).Scan(&kept); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if kept.Valid {
		detail.SnapshotKept = kept.Int64
	}

	reportJSON, _ := json.Marshal(detail)
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO recovery_reports(created_at, detail_json) VALUES(?,?)`,
		nowMillis(), string(reportJSON)); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return detail, nil
}

// LatestRecoveryReport 返回最近一次恢复报告。
func (s *Store) LatestRecoveryReport(ctx context.Context) (json.RawMessage, error) {
	var raw string
	err := s.db.QueryRowContext(ctx,
		`SELECT detail_json FROM recovery_reports ORDER BY id DESC LIMIT 1`).Scan(&raw)
	if err == sql.ErrNoRows {
		return json.RawMessage(`{"note":"no recovery has run"}`), nil
	}
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf(`{"marshal_error":%q}`, err.Error())
	}
	return string(b)
}
