package blockpool

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

const defaultLeaseTTL = 60 * time.Second

type Options struct {
	// DefaultLeaseTTL is used when a request omits lease_ms or supplies zero.
	DefaultLeaseTTL time.Duration
	// BusyTimeout bounds SQLite lock waiting. Zero selects five seconds.
	BusyTimeout time.Duration
	// Now is injectable for deterministic tests. Nil selects time.Now.
	Now func() time.Time
}

type Pool struct {
	db               *sql.DB
	path             string
	defaultLeaseTTL  time.Duration
	now              func() time.Time
	locks            *keyedLocks
	batchGate        *keyedLocks
	instanceID       string
	leaseIssuedEpoch atomic.Uint64
	leaseIssues      sync.Map // node+digest -> in-process issuance epoch
}

var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Open opens or creates a local SQLite-backed block pool and runs crash
// recovery before returning. A pending intent is never treated as committed.
func Open(path string, opts Options) (*Pool, error) {
	if opts.DefaultLeaseTTL <= 0 {
		opts.DefaultLeaseTTL = defaultLeaseTTL
	}
	if opts.BusyTimeout <= 0 {
		opts.BusyTimeout = 5 * time.Second
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	dsn, inMemory, err := sqliteDSN(path, opts.BusyTimeout)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if inMemory {
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
	} else {
		db.SetMaxOpenConns(16)
		db.SetMaxIdleConns(4)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	if err := migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	p := &Pool{
		db:              db,
		path:            path,
		defaultLeaseTTL: opts.DefaultLeaseTTL,
		now:             opts.Now,
		locks:           newKeyedLocks(),
		batchGate:       newKeyedLocks(),
		instanceID:      randomID(12),
	}
	if err := p.recoverOnOpen(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return p, nil
}

func sqliteDSN(path string, busyTimeout time.Duration) (dsn string, inMemory bool, err error) {
	busyMS := busyTimeout.Milliseconds()
	if busyMS < 1 {
		busyMS = 1
	}
	q := url.Values{}
	q.Add("_txlock", "immediate")
	q.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", busyMS))
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "synchronous(FULL)")
	if path == ":memory:" || path == "" {
		inMemory = true
		q.Set("mode", "memory")
		q.Set("cache", "shared")
		name := "blockpool-" + randomID(12)
		return "file:" + name + "?" + q.Encode(), true, nil
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", false, fmt.Errorf("resolve database path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return "", false, fmt.Errorf("create database directory: %w", err)
	}
	q.Add("_pragma", "journal_mode(WAL)")
	slashPath := filepath.ToSlash(abs)
	if runtime.GOOS == "windows" && !strings.HasPrefix(slashPath, "/") {
		slashPath = "/" + slashPath
	}
	u := url.URL{Scheme: "file", Path: slashPath, RawQuery: q.Encode()}
	return u.String(), false, nil
}

func (p *Pool) Close() error {
	if p == nil || p.db == nil {
		return nil
	}
	return p.db.Close()
}

func (p *Pool) Path() string { return p.path }

func (p *Pool) nowMS() int64 { return p.now().UnixMilli() }

func (p *Pool) leaseTTL(ms int64) (int64, *Error) {
	if ms < 0 {
		return 0, Errorf(CodeSpecInvalid, "lease_ms must be a non-negative integer")
	}
	if ms == 0 {
		ms = p.defaultLeaseTTL.Milliseconds()
	}
	if ms <= 0 {
		return 0, Errorf(CodeSpecInvalid, "lease_ms resolves to zero")
	}
	return ms, nil
}

func validateDigest(digest string) *Error {
	if !digestPattern.MatchString(digest) {
		return Errorf(CodeSpecInvalid, "digest must be 64 lowercase hexadecimal SHA-256 characters", "digest", digest)
	}
	return nil
}

func validateNode(namespace, path string) *Error {
	if strings.TrimSpace(namespace) == "" {
		return Errorf(CodeSpecInvalid, "namespace is required")
	}
	if len(namespace) > 255 || strings.ContainsRune(namespace, '\x00') {
		return Errorf(CodeSpecInvalid, "namespace is invalid", "namespace", namespace)
	}
	if path == "" {
		return Errorf(CodeSpecInvalid, "path is required")
	}
	if len(path) > 4096 || strings.ContainsRune(path, '\x00') {
		return Errorf(CodeSpecInvalid, "path is invalid", "path", path)
	}
	return nil
}

func validateNonNegative(name string, value int64) *Error {
	if value < 0 {
		return Errorf(CodeSpecInvalid, name+" must be a non-negative integer", name, value)
	}
	return nil
}

func nodeKey(namespace, path string) string  { return "n:" + namespace + "\x00" + path }
func digestKey(digest string) string         { return "d:" + digest }
func batchKey(namespace, path string) string { return "b:" + namespace + "\x00" + path }

func randomID(bytes int) string {
	buf := make([]byte, bytes)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}

func (p *Pool) beginWrite(ctx context.Context) (*sql.Tx, error) {
	return p.db.BeginTx(ctx, nil)
}

func (p *Pool) beginRead(ctx context.Context) (*sql.Tx, error) {
	return p.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
}

func rollback(tx *sql.Tx) {
	if tx != nil {
		_ = tx.Rollback()
	}
}

func scanBlock(scanner interface{ Scan(...any) error }) (Block, error) {
	var b Block
	err := scanner.Scan(&b.Digest, &b.SizeBytes, &b.StoredAtMS, &b.RetentionMS, &b.State, &b.RefCount)
	return b, err
}

const blockColumns = `digest, size_bytes, stored_at_ms, retention_ms, state, ref_count`

func scanLease(scanner interface{ Scan(...any) error }) (Lease, error) {
	var l Lease
	err := scanner.Scan(&l.Namespace, &l.Path, &l.Digest, &l.Version, &l.MountedAtMS, &l.ExpiresAtMS, &l.State)
	return l, err
}

const leaseColumns = `namespace, path, digest, version, mounted_at_ms, expires_at_ms, state`
