package engine

import (
	"context"
	"database/sql"
	"net/http"
	"sync"
	"time"

	"local/rollout-guard/internal/store"
)

type Options struct {
	RollbackURL string
	HTTPClient  *http.Client
	CrashPoint  string
	CrashBatch  string
	CrashHook   func(point string, intentID int64, batchID string)
	Now         func() int64
}

type Engine struct {
	store       *store.Store
	rollbackURL string
	client      *http.Client
	crashPoint  string
	crashBatch  string
	crashHook   func(string, int64, string)
	now         func() int64
	recoveryMu  sync.Mutex
}

func New(st *store.Store, options Options) *Engine {
	client := options.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}
	now := options.Now
	if now == nil {
		now = store.NowMS
	}
	return &Engine{
		store: st, rollbackURL: options.RollbackURL, client: client, crashPoint: options.CrashPoint,
		crashBatch: options.CrashBatch, crashHook: options.CrashHook, now: now,
	}
}

func (e *Engine) Store() *store.Store { return e.store }

func (e *Engine) maybeCrash(point string, intentID int64, batchID string) {
	if e.crashHook == nil || e.crashPoint != point {
		return
	}
	if e.crashBatch != "" && e.crashBatch != batchID {
		return
	}
	e.crashHook(point, intentID, batchID)
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func failIntentTx(ctx context.Context, tx *sql.Tx, intentID int64, note string) error {
	if err := store.SetIntentTx(ctx, tx, intentID, "FAILED", note, nil); err != nil {
		return err
	}
	return tx.Commit()
}
