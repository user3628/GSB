package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	_ "modernc.org/sqlite"

	"local/objectstore/internal/model"
)

type Store struct {
	db      *sql.DB
	writeMu sync.Mutex
}

func Open(ctx context.Context, path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("database path is required")
	}
	if path != ":memory:" {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("resolve database path: %w", err)
		}
		if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
			return nil, fmt.Errorf("create database directory: %w", err)
		}
		path = absolute
	}

	dsnPath := path
	if path == ":memory:" {
		dsnPath = "file:objectstore-memory?mode=memory&cache=shared"
	} else {
		slashPath := filepath.ToSlash(path)
		if !strings.HasPrefix(slashPath, "/") {
			slashPath = "/" + slashPath
		}
		dsnPath = (&url.URL{Scheme: "file", Path: slashPath}).String()
	}
	dsn := dsnPath + "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(ON)"

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(0)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate sqlite: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) withWriteTx(ctx context.Context, fn func(*sql.Conn) error) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()
	if err := fn(conn); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return err
	}
	committed = true
	return nil
}

func (s *Store) withReadTx(ctx context.Context, fn func(*sql.Conn) error) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return err
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), "ROLLBACK") }()
	if err := fn(conn); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, "COMMIT")
	return err
}

func (s *Store) AppendIntent(ctx context.Context, intent model.Intent, nowMS int64) (model.Intent, error) {
	intent.Status = model.IntentPending
	intent.CreatedAtMS = nowMS
	err := s.withWriteTx(ctx, func(conn *sql.Conn) error {
		result, err := conn.ExecContext(ctx, `
INSERT INTO intents(request_id, action, session_id, object_key, part_number, sha256, size, payload_json, status, created_at_ms)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			intent.RequestID, intent.Action, intent.SessionID, intent.ObjectKey, nullableInt64(intent.PartNumber),
			intent.SHA256, nullableInt64(intent.Size), intent.PayloadJSON, model.IntentPending, nowMS)
		if err != nil {
			return err
		}
		intent.Seq, err = result.LastInsertId()
		return err
	})
	if err != nil {
		return model.Intent{}, fmt.Errorf("append intent: %w", err)
	}
	return intent, nil
}

func (s *Store) RejectIntent(ctx context.Context, seq int64, nowMS int64) error {
	return s.finalizeIntent(ctx, seq, model.IntentRejected, nowMS)
}

func (s *Store) finalizeIntent(ctx context.Context, seq int64, status string, nowMS int64) error {
	return s.withWriteTx(ctx, func(conn *sql.Conn) error {
		result, err := conn.ExecContext(ctx, `
UPDATE intents SET status = ?, committed_at_ms = ? WHERE seq = ? AND status = 'pending'`,
			status, nowMS, seq)
		if err != nil {
			return err
		}
		rows, _ := result.RowsAffected()
		if rows != 1 {
			return fmt.Errorf("intent %d is not pending", seq)
		}
		return nil
	})
}

func markIntentCommitted(ctx context.Context, conn *sql.Conn, seq int64, nowMS int64) error {
	result, err := conn.ExecContext(ctx, `
UPDATE intents SET status = 'committed', committed_at_ms = ? WHERE seq = ? AND status = 'pending'`, nowMS, seq)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("intent %d is not pending", seq)
	}
	return nil
}

func nullableInt64(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func fromNullInt64(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	v := value.Int64
	return &v
}

func marshalPayload(value any) string {
	if value == nil {
		return ""
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(encoded)
}

func isConstraintError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "constraint failed") || strings.Contains(message, "unique constraint")
}
