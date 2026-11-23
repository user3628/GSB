package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

var ErrBatchNotFound = errors.New("batch not found")

type Options struct {
	Path               string
	ConfigVersion      string
	ConfigSnapshotJSON string
	SigningKey         []byte
	SigningKeyID       string
	FastPrecheckLimit  int
}

type Store struct {
	db         *sql.DB
	opt        Options
	writeMu    sync.Mutex
	recoverMu  sync.Mutex
	recovered  bool
	crashPoint string
}

func Open(ctx context.Context, opt Options) (*Store, error) {
	if opt.Path == "" {
		return nil, fmt.Errorf("database path is required")
	}
	if len(opt.SigningKey) == 0 {
		return nil, fmt.Errorf("signing key is required")
	}
	if opt.FastPrecheckLimit <= 0 {
		return nil, fmt.Errorf("fast precheck limit must be positive")
	}
	if opt.ConfigVersion == "" || opt.ConfigSnapshotJSON == "" {
		return nil, fmt.Errorf("config version and snapshot are required")
	}
	if err := os.MkdirAll(filepath.Dir(opt.Path), 0o755); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(10000)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(FULL)")
	q.Add("_pragma", "foreign_keys(1)")
	dbPath := filepath.ToSlash(opt.Path)
	if !strings.HasPrefix(dbPath, "/") {
		// Windows drive paths must be represented as /C:/... so net/url emits
		// file:///C:/... rather than treating C: as a URL host.
		dbPath = "/" + dbPath
	}
	dsnURL := &url.URL{Scheme: "file", Path: dbPath, RawQuery: q.Encode()}
	dsn := dsnURL.String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate schema: %w", err)
	}
	now := time.Now().UnixMilli()
	if _, err := db.ExecContext(ctx,
		`INSERT OR IGNORE INTO config_versions(version, snapshot_json, created_at_ms) VALUES(?,?,?)`,
		opt.ConfigVersion, opt.ConfigSnapshotJSON, now); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("record config version: %w", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT OR IGNORE INTO server_state(id,last_committed_version,last_reference_time_ms,last_config_version,last_committed_at_ms)
		 VALUES(1,0,0,?,0)`, opt.ConfigVersion); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize server state: %w", err)
	}
	return &Store{db: db, opt: opt, crashPoint: os.Getenv("WORKBENCH_CRASH_POINT")}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) ConfigVersion() string  { return s.opt.ConfigVersion }
func (s *Store) SigningKeyID() string   { return s.opt.SigningKeyID }
func (s *Store) FastPrecheckLimit() int { return s.opt.FastPrecheckLimit }

func (s *Store) IsRecovered() bool {
	s.recoverMu.Lock()
	defer s.recoverMu.Unlock()
	return s.recovered
}

func crashNow(point string) {
	// These hooks are enabled only by the explicitly documented crash-recovery
	// demo/test environment variable. They intentionally bypass defers so the
	// process dies with an open transaction, just like TerminateProcess.
	if point == "after_intent" {
		os.Exit(70)
	}
	if point == "after_writes" {
		os.Exit(71)
	}
}
