package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"elastic-scale/internal/model"
	"elastic-scale/internal/store"
)

type App struct {
	store    *store.Store
	db       *sql.DB
	writeMu  sync.Mutex
	recovery *model.RecoveryReport
}

func New(ctx context.Context, path string) (*App, error) {
	st, err := store.Open(path)
	if err != nil {
		return nil, err
	}
	a := &App{store: st, db: st.DB}
	if err := a.ensureMeta(ctx); err != nil {
		st.Close()
		return nil, err
	}
	report, err := a.recover(ctx)
	if err != nil {
		st.Close()
		return nil, err
	}
	a.recovery = report
	return a, nil
}

func (a *App) Close() error { return a.store.Close() }

func (a *App) DB() *sql.DB { return a.db }

func (a *App) ensureMeta(ctx context.Context) error {
	for _, kv := range [][2]string{{"current_version", "0"}, {"materialized_version", "0"}} {
		if _, err := a.db.ExecContext(ctx, `INSERT OR IGNORE INTO meta(key,value) VALUES(?,?)`, kv[0], kv[1]); err != nil {
			return err
		}
	}
	return nil
}

func getMeta(ctx context.Context, q queryRower, key string) (int64, error) {
	var value string
	err := q.QueryRowContext(ctx, `SELECT value FROM meta WHERE key=?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var n int64
	if _, err := fmt.Sscanf(value, "%d", &n); err != nil {
		return 0, err
	}
	return n, nil
}

func setMeta(ctx context.Context, q executor, key string, value any) error {
	_, err := q.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, fmt.Sprint(value))
	return err
}

type queryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type querier interface {
	queryRower
	executor
}

func (a *App) Versions(ctx context.Context) (current int64, materialized int64, err error) {
	current, err = getMeta(ctx, a.db, "current_version")
	if err != nil {
		return 0, 0, err
	}
	materialized, err = getMeta(ctx, a.db, "materialized_version")
	return current, materialized, err
}

func (a *App) Health(ctx context.Context) model.HealthResponse {
	current, materialized, _ := a.Versions(ctx)
	var one int
	err := a.db.QueryRowContext(ctx, `SELECT 1`).Scan(&one)
	return model.HealthResponse{
		Status: "ok", TimeMS: store.NowMS(), CurrentVersion: current,
		MaterializedVersion: materialized, DBWritable: err == nil && one == 1,
	}
}

func canonicalHash(kind string, payload any) string {
	b, err := json.Marshal(struct {
		Kind    string `json:"kind"`
		Payload any    `json:"payload"`
	}{Kind: kind, Payload: payload})
	if err != nil {
		return "sha256:marshal-error"
	}
	return hashBytes(b)
}
