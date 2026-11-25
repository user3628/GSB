// Package localsqlite implements a small database/sql driver for the system
// SQLite library. The project intentionally keeps the driver local so the lab
// runs without downloading a third-party Go SQLite package.
package localsqlite

/*
#cgo LDFLAGS: -lsqlite3
#include <sqlite3.h>
#include <stdlib.h>

static int bind_text_transient(sqlite3_stmt *stmt, int idx, const char *value, int n) {
	return sqlite3_bind_text(stmt, idx, value, n, SQLITE_TRANSIENT);
}
static int bind_blob_transient(sqlite3_stmt *stmt, int idx, const void *value, int n) {
	return sqlite3_bind_blob(stmt, idx, value, n, SQLITE_TRANSIENT);
}
static int exec_sql(sqlite3 *db, const char *sql) {
	char *err = 0;
	int rc = sqlite3_exec(db, sql, 0, 0, &err);
	if (err) sqlite3_free(err);
	return rc;
}
*/
import "C"

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"unsafe"
)

func init() {
	sql.Register("registry-sqlite", &Driver{})
}

// Driver is the database/sql driver entry point.
type Driver struct{}

func (d *Driver) Open(name string) (driver.Conn, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	flags := C.int(C.SQLITE_OPEN_READWRITE | C.SQLITE_OPEN_CREATE | C.SQLITE_OPEN_FULLMUTEX)
	var db *C.sqlite3
	rc := C.sqlite3_open_v2(cname, &db, flags, nil)
	if rc != C.SQLITE_OK {
		msg := "unable to open sqlite database"
		if db != nil {
			msg = C.GoString(C.sqlite3_errmsg(db))
			C.sqlite3_close_v2(db)
		}
		return nil, &Error{Code: int(rc), Message: msg}
	}
	C.sqlite3_extended_result_codes(db, 1)
	C.sqlite3_busy_timeout(db, 5000)
	pragmas := "PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL;"
	cpragmas := C.CString(pragmas)
	rc = C.exec_sql(db, cpragmas)
	C.free(unsafe.Pointer(cpragmas))
	if rc != C.SQLITE_OK {
		msg := C.GoString(C.sqlite3_errmsg(db))
		C.sqlite3_close_v2(db)
		return nil, &Error{Code: int(rc), Message: msg}
	}
	return &conn{db: db}, nil
}

// Error exposes SQLite's native result code and message.
type Error struct {
	Code    int
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("sqlite(%d): %s", e.Code, e.Message) }

type conn struct {
	db *C.sqlite3
}

func (c *conn) Prepare(query string) (driver.Stmt, error) { return c.prepare(query) }
func (c *conn) Close() error {
	if c.db == nil {
		return nil
	}
	rc := C.sqlite3_close_v2(c.db)
	c.db = nil
	if rc != C.SQLITE_OK {
		return &Error{Code: int(rc), Message: "close failed"}
	}
	return nil
}
func (c *conn) Begin() (driver.Tx, error) { return c.begin(false) }
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.begin(opts.ReadOnly)
}

func (c *conn) begin(readOnly bool) (driver.Tx, error) {
	stmt, err := c.prepare("BEGIN")
	if err != nil {
		return nil, err
	}
	// A read-only database/sql transaction can use deferred BEGIN. Writes use
	// IMMEDIATE to serialize lock acquisition and avoid SQLITE_BUSY upgrades.
	if !readOnly {
		stmt.Close()
		stmt, err = c.prepare("BEGIN IMMEDIATE")
		if err != nil {
			return nil, err
		}
	}
	defer stmt.Close()
	if _, err := stmt.step(); err != nil {
		return nil, err
	}
	return &tx{c: c}, nil
}

func (c *conn) prepare(query string) (*stmt, error) {
	if c.db == nil {
		return nil, errors.New("sqlite: connection closed")
	}
	cq := C.CString(query)
	defer C.free(unsafe.Pointer(cq))
	var raw *C.sqlite3_stmt
	rc := C.sqlite3_prepare_v2(c.db, cq, -1, &raw, nil)
	if rc != C.SQLITE_OK {
		return nil, c.err(rc)
	}
	return &stmt{c: c, raw: raw}, nil
}

func (c *conn) err(rc C.int) error {
	return &Error{Code: int(rc), Message: C.GoString(C.sqlite3_errmsg(c.db))}
}

type stmt struct {
	c      *conn
	raw    *C.sqlite3_stmt
	closed bool
}

func (s *stmt) Close() error {
	if s.closed || s.raw == nil {
		return nil
	}
	rc := C.sqlite3_finalize(s.raw)
	s.raw = nil
	s.closed = true
	if rc != C.SQLITE_OK {
		return s.c.err(rc)
	}
	return nil
}

func (s *stmt) NumInput() int { return int(C.sqlite3_bind_parameter_count(s.raw)) }

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	if err := s.bind(args); err != nil {
		return nil, err
	}
	for {
		hasRow, err := s.step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			break
		}
	}
	return result{id: int64(C.sqlite3_last_insert_rowid(s.c.db)), changed: int64(C.sqlite3_changes(s.c.db))}, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	if err := s.bind(args); err != nil {
		return nil, err
	}
	cols := int(C.sqlite3_column_count(s.raw))
	names := make([]string, cols)
	for i := 0; i < cols; i++ {
		names[i] = C.GoString(C.sqlite3_column_name(s.raw, C.int(i)))
	}
	return &rows{s: s, cols: names}, nil
}

func (s *stmt) bind(args []driver.Value) error {
	if len(args) != s.NumInput() {
		return fmt.Errorf("sqlite: expected %d arguments, got %d", s.NumInput(), len(args))
	}
	for i, arg := range args {
		idx := C.int(i + 1)
		var rc C.int
		switch v := arg.(type) {
		case nil:
			rc = C.sqlite3_bind_null(s.raw, idx)
		case int64:
			rc = C.sqlite3_bind_int64(s.raw, idx, C.sqlite3_int64(v))
		case float64:
			rc = C.sqlite3_bind_double(s.raw, idx, C.double(v))
		case bool:
			n := int64(0)
			if v {
				n = 1
			}
			rc = C.sqlite3_bind_int64(s.raw, idx, C.sqlite3_int64(n))
		case string:
			cs := C.CString(v)
			rc = C.bind_text_transient(s.raw, idx, cs, C.int(len(v)))
			C.free(unsafe.Pointer(cs))
		case []byte:
			if len(v) == 0 {
				rc = C.sqlite3_bind_blob(s.raw, idx, nil, 0, nil)
			} else {
				rc = C.bind_blob_transient(s.raw, idx, unsafe.Pointer(&v[0]), C.int(len(v)))
			}
		default:
			return fmt.Errorf("sqlite: unsupported bind type %T", arg)
		}
		if rc != C.SQLITE_OK {
			return s.c.err(rc)
		}
	}
	return nil
}

func (s *stmt) step() (bool, error) {
	rc := C.sqlite3_step(s.raw)
	switch rc {
	case C.SQLITE_ROW:
		return true, nil
	case C.SQLITE_DONE:
		return false, nil
	default:
		return false, s.c.err(rc)
	}
}

type rows struct {
	s    *stmt
	cols []string
	done bool
}

func (r *rows) Columns() []string { return r.cols }
func (r *rows) Close() error      { return r.s.Close() }
func (r *rows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	hasRow, err := r.s.step()
	if err != nil {
		return err
	}
	if !hasRow {
		r.done = true
		return io.EOF
	}
	for i := range dest {
		ct := C.sqlite3_column_type(r.s.raw, C.int(i))
		switch ct {
		case C.SQLITE_NULL:
			dest[i] = nil
		case C.SQLITE_INTEGER:
			dest[i] = int64(C.sqlite3_column_int64(r.s.raw, C.int(i)))
		case C.SQLITE_FLOAT:
			dest[i] = float64(C.sqlite3_column_double(r.s.raw, C.int(i)))
		case C.SQLITE_BLOB:
			n := C.sqlite3_column_bytes(r.s.raw, C.int(i))
			if n == 0 {
				dest[i] = []byte{}
			} else {
				ptr := C.sqlite3_column_blob(r.s.raw, C.int(i))
				dest[i] = C.GoBytes(ptr, n)
			}
		default:
			n := C.sqlite3_column_bytes(r.s.raw, C.int(i))
			ptr := C.sqlite3_column_text(r.s.raw, C.int(i))
			dest[i] = []byte(C.GoStringN((*C.char)(unsafe.Pointer(ptr)), n))
		}
	}
	return nil
}

type result struct {
	id      int64
	changed int64
}

func (r result) LastInsertId() (int64, error) { return r.id, nil }
func (r result) RowsAffected() (int64, error) { return r.changed, nil }

type tx struct{ c *conn }

func (t *tx) Commit() error {
	stmt, err := t.c.prepare("COMMIT")
	if err != nil {
		return err
	}
	defer stmt.Close()
	if _, err := stmt.step(); err != nil {
		return err
	}
	return nil
}
func (t *tx) Rollback() error {
	stmt, err := t.c.prepare("ROLLBACK")
	if err != nil {
		return err
	}
	defer stmt.Close()
	if _, err := stmt.step(); err != nil {
		return err
	}
	return nil
}
