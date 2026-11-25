package storage

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// Store wraps a SQLite database file. A single *sql.DB is used; WAL mode lets
// multiple readers run concurrently while one writer transaction is active.
type Store struct {
	db       *sql.DB
	path     string
	readOnly *sql.DB
}

// Open opens (or creates) the database at path and applies the schema.
func Open(path string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create db dir: %w", err)
		}
	}
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // writes serialized through one connection
	if _, err := db.Exec(schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	ro, err := sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=query_only(ON)")
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	ro.SetMaxOpenConns(8)
	return &Store{db: db, path: path, readOnly: ro}, nil
}

// DB exposes the writer connection (engine keeps serialization itself).
func (s *Store) DB() *sql.DB { return s.db }

// ReadDB exposes the read-only connection pool for concurrent queries.
func (s *Store) ReadDB() *sql.DB { return s.readOnly }

// Path returns the database file path.
func (s *Store) Path() string { return s.path }

// Close releases both connection pools.
func (s *Store) Close() error {
	err1 := s.db.Close()
	err2 := s.readOnly.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

// BeginWrite starts a serialized write transaction.
func (s *Store) BeginWrite() (*sql.Tx, error) {
	return s.db.Begin()
}
