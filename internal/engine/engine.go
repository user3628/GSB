package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"configtree/internal/spec"
)

// CrashHook is invoked at named pipeline phases. Returning true forces an
// immediate process exit (os.Exit) to simulate a hard kill mid-recompute.
type CrashHook func(phase string) bool

// Engine is the single-writer / multi-reader pipeline facade.
type Engine struct {
	db         *sql.DB
	writeMu    sync.Mutex
	mu         sync.Mutex
	inflight   map[string]*inflightOp
	hook       CrashHook
	startTime  time.Time
	lastReport *RecoveryReport
	// testSkipExit turns the hard-kill hook into a returnable signal so unit
	// tests can exercise crash recovery without killing the test binary.
	testSkipExit bool
}

type inflightOp struct {
	done          chan struct{}
	result        commitResult
	resultPayload json.RawMessage
	err           error
}

type commitResult struct {
	Version  int64        `json:"version"`
	CommitID string       `json:"commit_id"`
	Reused   bool         `json:"reused"`
	Affected *AffectedSet `json:"affected,omitempty"`
}

// Open opens (or creates) the SQLite-backed pipeline and runs crash recovery.
func Open(path string) (*Engine, error) {
	db, err := sql.Open("sqlite", path+"?_txlock=immediate&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	// SQLite allows one writer; a modest pool keeps multi-reader concurrency
	// while serializing writers via the database itself plus writeMu.
	db.SetMaxOpenConns(8)
	if _, err := db.Exec(schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	e := &Engine{
		db:        db,
		inflight:  map[string]*inflightOp{},
		startTime: time.Now().UTC(),
	}
	if err := e.recoverPendingIntents(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("crash recovery: %w", err)
	}
	return e, nil
}

func (e *Engine) SetCrashHook(h CrashHook) { e.hook = h }

func (e *Engine) Close() error { return e.db.Close() }

func (e *Engine) now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// currentVersion returns the latest committed version (0 when empty).
func (e *Engine) currentVersion(ctx context.Context) (int64, error) {
	var v int64
	err := e.db.QueryRowContext(ctx,
		`SELECT COALESCE((SELECT version FROM commits WHERE status='committed'
		   ORDER BY version DESC LIMIT 1), 0)`).Scan(&v)
	return v, err
}

// loadMaterialized reads the committed state used for derivation.
func (e *Engine) loadMaterialized(ctx context.Context) (*materializedState, error) {
	ver, err := e.currentVersion(ctx)
	if err != nil {
		return nil, err
	}
	st := &materializedState{
		version:    ver,
		items:      map[string]spec.ConfigItem{},
		components: map[string]spec.Component{},
	}
	rows, err := e.db.QueryContext(ctx,
		`SELECT key_path, type, value, ord FROM config_items`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var it spec.ConfigItem
		var raw string
		if err := rows.Scan(&it.KeyPath, &it.Type, &raw, &it.Order); err != nil {
			rows.Close()
			return nil, err
		}
		it.Value = json.RawMessage(raw)
		st.items[it.KeyPath] = it
	}
	rows.Close()

	rows, err = e.db.QueryContext(ctx, `SELECT spec_json FROM components`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			rows.Close()
			return nil, err
		}
		var c spec.Component
		if err := json.Unmarshal([]byte(raw), &c); err != nil {
			rows.Close()
			return nil, err
		}
		st.components[c.ID] = c
	}
	rows.Close()
	return st, nil
}
