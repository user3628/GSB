package dbconn

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"local/sqlitepool/internal/domain"

	_ "modernc.org/sqlite"
)

var (
	ErrConnClosed   = errors.New("underlying connection is closed")
	ErrInjectedDial = errors.New("injected dial failure")
)

// Conn is the narrow database surface required by the pool. A connection is
// owned by one caller at a time; all methods are safe once that lease exists.
type Conn interface {
	Ping(context.Context) error
	Query(context.Context, string, ...any) (domain.QueryResult, error)
	Close() error
}

type Opener interface {
	Open(context.Context) (Conn, error)
}

type SQLiteOpener struct {
	Path string
}

func (o SQLiteOpener) Open(ctx context.Context) (Conn, error) {
	if strings.TrimSpace(o.Path) == "" {
		return nil, fmt.Errorf("sqlite path is empty")
	}
	if dir := filepath.Dir(o.Path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	// Journal mode is a database-wide setting. Setting WAL on every concurrent
	// connection would race during first-use initialization and surface
	// SQLITE_BUSY as a dial failure, so each connection only sets connection-local
	// pragmas here. The metadata ledger separately initializes its own WAL file.
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(10000)&_pragma=synchronous(FULL)", o.Path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One sql.DB handle backs one pool connection. Closing the *sql.Conn is a
	// real close of the native SQLite connection, not a logical flag.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	c := &sqliteConn{db: db, conn: conn}
	if _, err := conn.ExecContext(ctx, "PRAGMA busy_timeout = 5000"); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

type sqliteConn struct {
	db     *sql.DB
	conn   *sql.Conn
	once   sync.Once
	closed bool
	mu     sync.Mutex
}

func (c *sqliteConn) Ping(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrConnClosed
	}
	if err := c.conn.PingContext(ctx); err != nil {
		if isClosedError(err) {
			return ErrConnClosed
		}
		return err
	}
	return nil
}

func (c *sqliteConn) Query(ctx context.Context, query string, args ...any) (domain.QueryResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return domain.QueryResult{}, ErrConnClosed
	}
	if strings.TrimSpace(query) == "" {
		return domain.QueryResult{}, fmt.Errorf("query is empty")
	}
	if isReadStatement(query) {
		rows, err := c.conn.QueryContext(ctx, query, args...)
		if err != nil {
			return domain.QueryResult{}, normalizeError(err)
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			return domain.QueryResult{}, normalizeError(err)
		}
		out := domain.QueryResult{Columns: cols}
		for rows.Next() {
			values := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return domain.QueryResult{}, normalizeError(err)
			}
			for i, v := range values {
				switch x := v.(type) {
				case []byte:
					values[i] = string(x)
				case time.Time:
					values[i] = x.Format(time.RFC3339Nano)
				default:
					values[i] = x
				}
			}
			out.Rows = append(out.Rows, values)
		}
		if err := rows.Err(); err != nil {
			return domain.QueryResult{}, normalizeError(err)
		}
		return out, nil
	}
	res, err := c.conn.ExecContext(ctx, query, args...)
	if err != nil {
		return domain.QueryResult{}, normalizeError(err)
	}
	n, _ := res.RowsAffected()
	return domain.QueryResult{RowsAffected: n}, nil
}

func (c *sqliteConn) Close() error {
	var err error
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		if c.conn != nil {
			err = c.conn.Close()
		}
		if c.db != nil {
			if closeErr := c.db.Close(); err == nil {
				err = closeErr
			}
		}
	})
	return err
}

func normalizeError(err error) error {
	if isClosedError(err) {
		return ErrConnClosed
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return err
	}
	return err
}

func isClosedError(err error) bool {
	if errors.Is(err, sql.ErrConnDone) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "connection is already closed") ||
		strings.Contains(message, "closed connection") ||
		strings.Contains(message, "bad connection") ||
		strings.Contains(message, "connection has been closed")
}

func isReadStatement(query string) bool {
	q := strings.ToUpper(strings.TrimSpace(query))
	return strings.HasPrefix(q, "SELECT ") || strings.HasPrefix(q, "WITH ") ||
		strings.HasPrefix(q, "PRAGMA ") || strings.HasPrefix(q, "EXPLAIN ")
}
