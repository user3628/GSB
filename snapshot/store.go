package snapshot

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"

	"sandboxpolicy/business"
	"sandboxpolicy/policy"
)

// Store is backed by a local SQLite file. It never keeps policy state that is
// authoritative; every apply starts from a committed row.
type Store struct {
	db   *sql.DB
	path string
}

// Open initializes a pure-Go SQLite store. WAL and immediate transactions are
// enabled so concurrent namespaces receive a consistent writer view.
func Open(databasePath string) (*Store, error) {
	if strings.TrimSpace(databasePath) == "" {
		return nil, fmt.Errorf("database path is required")
	}
	if databasePath != ":memory:" {
		dir := filepath.Dir(databasePath)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create database directory: %w", err)
		}
	}
	dsn := sqliteDSN(databasePath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if databasePath == ":memory:" {
		db.SetMaxOpenConns(1)
	} else {
		db.SetMaxOpenConns(8)
	}
	db.SetMaxIdleConns(4)
	store := &Store{db: db, path: databasePath}
	if err := store.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func sqliteDSN(databasePath string) string {
	if databasePath == ":memory:" {
		name := atomic.AddUint64(&memoryDatabaseSequence, 1)
		return fmt.Sprintf("file:sandboxpolicy-memory-%d?mode=memory&cache=shared&_pragma=busy_timeout(100)&_pragma=foreign_keys(ON)&_txlock=immediate", name)
	}
	path := filepath.ToSlash(databasePath)
	if !strings.HasPrefix(path, "/") && !strings.Contains(path, ":") {
		if abs, err := filepath.Abs(databasePath); err == nil {
			path = filepath.ToSlash(abs)
		}
	}
	return "file:" + path + "?_pragma=busy_timeout(100)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)&_txlock=immediate"
}

var memoryDatabaseSequence uint64

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) migrate(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS snapshots (
			namespace TEXT NOT NULL,
			version INTEGER NOT NULL,
			parent_version INTEGER NOT NULL,
			digest TEXT NOT NULL,
			policy_json BLOB NOT NULL,
			diagnostics_json BLOB NOT NULL,
			created_at TEXT NOT NULL,
			PRIMARY KEY (namespace, version)
		)`,
		`CREATE TABLE IF NOT EXISTS change_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			namespace TEXT NOT NULL,
			version INTEGER NOT NULL,
			operation TEXT NOT NULL,
			event_type TEXT NOT NULL,
			code TEXT NOT NULL,
			detail TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_change_events_namespace ON change_events(namespace, id)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate sqlite: %w", err)
		}
	}
	return nil
}

// Initialize commits version 1 from a strict baseline policy.
func (s *Store) Initialize(ctx context.Context, input *policy.Policy) (*Snapshot, error) {
	if input == nil {
		return nil, business.New(business.CodePolicyMalformed, "baseline policy is required")
	}
	normalized := input.Clone()
	if err := policy.NormalizeStrict(normalized); err != nil {
		return nil, err
	}
	tx, err := s.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshots WHERE namespace = ?`, normalized.Namespace).Scan(&count); err != nil {
		return nil, err
	}
	if count != 0 {
		return nil, &business.Error{Code: business.CodeChangeConflict, Entry: "baseline", Message: fmt.Sprintf("namespace %q already has snapshots", normalized.Namespace)}
	}
	closure := emptyClosure(normalized.Namespace, 1)
	snapshot, err := s.insertSnapshotTx(ctx, tx, normalized, closure, 0)
	if err != nil {
		return nil, err
	}
	if err := s.RecordEventTx(ctx, tx, Event{Namespace: normalized.Namespace, Version: 1, Operation: "init", EventType: "applied", Code: "OK", Detail: "baseline snapshot committed"}); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return snapshot, nil
}

func emptyClosure(namespace string, version int) policy.Closure {
	return policy.Closure{
		Namespace:     namespace,
		Version:       version,
		ParentVersion: version - 1,
		Triggers:      []policy.Trigger{},
		Direct:        []policy.AffectedRule{},
		Transitive:    []policy.AffectedRule{},
		BusinessCodes: []business.Code{},
	}
}

// Begin starts an immediate SQLite transaction. The store never advances from
// an in-memory version.
func (s *Store) Begin(ctx context.Context) (*sql.Tx, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	return tx, nil
}

// LoadChain validates the namespace chain and returns the longest contiguous
// prefix. A gap or digest replacement produces a warning rather than exposing
// untrusted later rows.
func (s *Store) LoadChain(ctx context.Context, namespace string) (*Chain, error) {
	return s.loadChain(ctx, s.db, namespace)
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func (s *Store) loadChain(ctx context.Context, q queryer, namespace string) (*Chain, error) {
	if strings.TrimSpace(namespace) == "" {
		return nil, business.New(business.CodePolicyMalformed, "namespace is required")
	}
	rows, err := q.QueryContext(ctx, `SELECT namespace, version, parent_version, digest, policy_json, diagnostics_json, created_at FROM snapshots WHERE namespace = ? ORDER BY version ASC`, namespace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	all := make([]Snapshot, 0)
	for rows.Next() {
		var row Snapshot
		var policyBytes, diagnosticsBytes []byte
		if err := rows.Scan(&row.Namespace, &row.Version, &row.ParentVersion, &row.Digest, &policyBytes, &diagnosticsBytes, &row.CreatedAt); err != nil {
			return nil, err
		}
		row.Policy = nil
		row.Closure = policy.Closure{}
		row.policyJSON = append([]byte(nil), policyBytes...)
		row.diagnosticsJSON = append([]byte(nil), diagnosticsBytes...)
		all = append(all, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	chain := &Chain{Namespace: namespace, Entries: []ChainEntry{}, Snapshots: []Snapshot{}}
	expected := 1
	for _, row := range all {
		if row.Version != expected {
			chain.Warning = snapshotWarning(fmt.Sprintf("expected version %d, found %d", expected, row.Version), expected, row.Version, fallbackVersion(chain))
			break
		}
		if row.ParentVersion != expected-1 {
			chain.Warning = snapshotWarning(fmt.Sprintf("version %d parent is %d, expected %d", row.Version, row.ParentVersion, expected-1), expected, row.Version, fallbackVersion(chain))
			break
		}
		computed := digestSnapshot(row.policyJSON, row.diagnosticsJSON)
		if !strings.EqualFold(computed, row.Digest) {
			chain.Warning = snapshotWarning(fmt.Sprintf("version %d content digest mismatch", row.Version), expected, row.Version, fallbackVersion(chain))
			break
		}
		var decoded policy.Policy
		if err := json.Unmarshal(row.policyJSON, &decoded); err != nil {
			chain.Warning = snapshotWarning(fmt.Sprintf("version %d policy JSON is invalid: %v", row.Version, err), expected, row.Version, fallbackVersion(chain))
			break
		}
		if err := policy.NormalizeSnapshot(&decoded); err != nil {
			chain.Warning = snapshotWarning(fmt.Sprintf("version %d policy is malformed: %v", row.Version, err), expected, row.Version, fallbackVersion(chain))
			break
		}
		var closure policy.Closure
		if err := json.Unmarshal(row.diagnosticsJSON, &closure); err != nil {
			chain.Warning = snapshotWarning(fmt.Sprintf("version %d diagnostics are invalid: %v", row.Version, err), expected, row.Version, fallbackVersion(chain))
			break
		}
		if row.Namespace != namespace || decoded.Namespace != namespace {
			chain.Warning = snapshotWarning(fmt.Sprintf("version %d namespace mismatch", row.Version), expected, row.Version, fallbackVersion(chain))
			break
		}
		if closure.Namespace != namespace || closure.Version != row.Version || closure.ParentVersion != row.ParentVersion {
			chain.Warning = snapshotWarning(fmt.Sprintf("version %d diagnostics are not bound to the snapshot version", row.Version), expected, row.Version, fallbackVersion(chain))
			break
		}
		row.Policy = &decoded
		row.Closure = closure
		row.policyJSON = nil
		row.diagnosticsJSON = nil
		chain.Snapshots = append(chain.Snapshots, row)
		chain.Entries = append(chain.Entries, ChainEntry{Version: row.Version, ParentVersion: row.ParentVersion, Digest: row.Digest})
		expected++
	}
	if len(chain.Snapshots) == 0 && chain.Warning == nil && len(all) == 0 {
		return chain, nil
	}
	return chain, nil
}

func fallbackVersion(chain *Chain) int {
	if chain == nil || len(chain.Entries) == 0 {
		return 0
	}
	return chain.Entries[len(chain.Entries)-1].Version
}

func snapshotWarning(message string, expected, invalid, fallback int) *ChainWarning {
	return &ChainWarning{
		Code:            business.CodeSnapshotVersion,
		Message:         message,
		ExpectedVersion: expected,
		InvalidVersion:  invalid,
		FallbackVersion: fallback,
	}
}

// Current returns the latest contiguous snapshot and its chain/warning.
func (s *Store) Current(ctx context.Context, namespace string) (*Snapshot, *Chain, error) {
	chain, err := s.LoadChain(ctx, namespace)
	if err != nil {
		return nil, nil, err
	}
	current, ok := chain.CurrentSnapshot()
	if !ok {
		if chain.Warning != nil {
			return nil, chain, &business.Error{Code: business.CodeSnapshotVersion, Message: "no contiguous snapshot is available"}
		}
		return nil, chain, business.New(business.CodeBaselineMissing, fmt.Sprintf("namespace %q has no snapshots", namespace))
	}
	return current, chain, nil
}

// SnapshotAt loads a specific version. Versions beyond the contiguous prefix
// are rejected with E_SNAPSHOT_VERSION while still returning the fallback chain.
func (s *Store) SnapshotAt(ctx context.Context, namespace string, version int) (*Snapshot, *Chain, error) {
	chain, err := s.LoadChain(ctx, namespace)
	if err != nil {
		return nil, nil, err
	}
	for i := range chain.Snapshots {
		if chain.Snapshots[i].Version == version {
			return &chain.Snapshots[i], chain, nil
		}
	}
	fallback := fallbackVersion(chain)
	return nil, chain, &business.Error{Code: business.CodeSnapshotVersion, Version: version, Message: fmt.Sprintf("version %d is not in the contiguous chain; fallback version is %d", version, fallback)}
}

// LatestForUpdate loads and validates the current chain inside an apply
// transaction. Any integrity warning aborts the apply.
func (s *Store) LatestForUpdate(ctx context.Context, tx *sql.Tx, namespace string) (*Snapshot, *Chain, error) {
	chain, err := s.loadChain(ctx, tx, namespace)
	if err != nil {
		return nil, nil, err
	}
	current, ok := chain.CurrentSnapshot()
	if !ok {
		return nil, chain, nil
	}
	return current, chain, nil
}

// InsertSnapshotTx computes the content digest and commits a new snapshot row
// inside the caller's transaction.
func (s *Store) InsertSnapshotTx(ctx context.Context, tx *sql.Tx, p *policy.Policy, closure policy.Closure, parentVersion int) (*Snapshot, error) {
	cloned := p.Clone()
	if err := policy.NormalizeSnapshot(cloned); err != nil {
		return nil, err
	}
	if closure.Version != parentVersion+1 {
		return nil, business.New(business.CodeInternal, fmt.Sprintf("closure version %d does not match new version %d", closure.Version, parentVersion+1))
	}
	if closure.Namespace != cloned.Namespace {
		return nil, &business.Error{Code: business.CodeNamespaceScope, Message: "closure namespace does not match policy namespace"}
	}
	return s.insertSnapshotTx(ctx, tx, cloned, closure, parentVersion)
}

func (s *Store) insertSnapshotTx(ctx context.Context, tx *sql.Tx, p *policy.Policy, closure policy.Closure, parentVersion int) (*Snapshot, error) {
	policyBytes, err := policy.CanonicalJSON(p)
	if err != nil {
		return nil, err
	}
	closureBytes, err := json.Marshal(closure)
	if err != nil {
		return nil, err
	}
	version := parentVersion + 1
	digest := digestSnapshot(policyBytes, closureBytes)
	created := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `INSERT INTO snapshots(namespace, version, parent_version, digest, policy_json, diagnostics_json, created_at) VALUES(?,?,?,?,?,?,?)`, p.Namespace, version, parentVersion, digest, policyBytes, closureBytes, created); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") || strings.Contains(strings.ToLower(err.Error()), "constraint") {
			return nil, &business.Error{Code: business.CodeChangeConflict, Version: version, Message: "snapshot version already exists"}
		}
		return nil, err
	}
	return &Snapshot{Namespace: p.Namespace, Version: version, ParentVersion: parentVersion, Digest: digest, Policy: p.Clone(), Closure: closure, CreatedAt: created}, nil
}

func digestSnapshot(policyBytes, diagnosticsBytes []byte) string {
	h := sha256.New()
	h.Write([]byte("sandboxpolicy-snapshot-v1\x00"))
	h.Write(policyBytes)
	h.Write([]byte{0})
	h.Write(diagnosticsBytes)
	return hex.EncodeToString(h.Sum(nil))
}

// Namespaces lists known namespaces in stable order.
func (s *Store) Namespaces(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT namespace FROM snapshots ORDER BY namespace ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var namespace string
		if err := rows.Scan(&namespace); err != nil {
			return nil, err
		}
		out = append(out, namespace)
	}
	return out, rows.Err()
}

// ResolveNamespace uses an explicit value, or infers a sole local namespace.
func (s *Store) ResolveNamespace(ctx context.Context, explicit string) (string, error) {
	if strings.TrimSpace(explicit) != "" {
		return strings.TrimSpace(explicit), nil
	}
	namespaces, err := s.Namespaces(ctx)
	if err != nil {
		return "", err
	}
	if len(namespaces) == 1 {
		return namespaces[0], nil
	}
	if len(namespaces) == 0 {
		return "", business.New(business.CodeBaselineMissing, "no namespace exists; pass --namespace or initialize a baseline")
	}
	return "", business.New(business.CodeNamespaceScope, "multiple namespaces exist; pass --namespace explicitly")
}

// RecordEvent appends queue/conflict/application activity.
func (s *Store) RecordEvent(ctx context.Context, event Event) error {
	_, err := s.db.ExecContext(ctx, insertEventSQL, eventValues(event)...)
	if err != nil {
		return fmt.Errorf("record %s event: %w", event.EventType, err)
	}
	return nil
}

// RecordEventTx records an event atomically with a snapshot change.
func (s *Store) RecordEventTx(ctx context.Context, tx *sql.Tx, event Event) error {
	_, err := tx.ExecContext(ctx, insertEventSQL, eventValues(event)...)
	if err != nil {
		return fmt.Errorf("record %s event in transaction: %w", event.EventType, err)
	}
	return nil
}

const insertEventSQL = `INSERT INTO change_events(namespace, version, operation, event_type, code, detail, created_at) VALUES(?,?,?,?,?,?,?)`

func eventValues(event Event) []any {
	if event.Operation == "" {
		event.Operation = "apply"
	}
	if event.CreatedAt == "" {
		event.CreatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	return []any{event.Namespace, event.Version, event.Operation, event.EventType, event.Code, event.Detail, event.CreatedAt}
}

// Events returns recent events in insertion order.
func (s *Store) Events(ctx context.Context, namespace string, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, namespace, version, operation, event_type, code, detail, created_at FROM change_events WHERE namespace = ? ORDER BY id DESC LIMIT ?`, namespace, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Event, 0)
	for rows.Next() {
		var event Event
		if err := rows.Scan(&event.ID, &event.Namespace, &event.Version, &event.Operation, &event.EventType, &event.Code, &event.Detail, &event.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
