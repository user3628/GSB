package truststore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	defaultMaxInflight = 4
	defaultWaitTimeout = 250 * time.Millisecond
	defaultBusyTimeout = 5 * time.Second
)

// StoreOptions configures the SQLite state repository and concurrency guard.
type StoreOptions struct {
	DBPath            string
	MaxInflight       int
	WaitTimeout       time.Duration
	BusyTimeout       time.Duration
	ContentionLogPath string
}

// Store owns a SQLite trust-state database. One Store may serve multiple
// namespaces; namespace gates are independent while SQLite serializes actual
// write transactions.
type Store struct {
	db   *sql.DB
	opts StoreOptions
	sem  chan struct{}

	gatesMu sync.Mutex
	gates   map[string]chan struct{}

	eventMu sync.Mutex
	events  []ContentionRecord
}

// ContentionRecord is persisted outside the SQLite state database so a rejected
// request can be audited without modifying trust versions.
type ContentionRecord struct {
	Namespace  string `json:"namespace"`
	RequestID  string `json:"request_id"`
	WaitMillis int64  `json:"wait_millis"`
	Outcome    string `json:"outcome"`
	Code       Code   `json:"code"`
}

// Open creates or opens a state database and applies the schema. It never
// installs or consults operating-system root certificates.
func Open(ctx context.Context, options StoreOptions) (*Store, error) {
	if strings.TrimSpace(options.DBPath) == "" {
		return nil, NewError(CodeInputInvalid, "open store", "DBPath is required", nil)
	}
	if options.MaxInflight <= 0 {
		options.MaxInflight = defaultMaxInflight
	}
	if options.WaitTimeout < 0 {
		return nil, NewError(CodeInputInvalid, "open store", "WaitTimeout must not be negative", nil)
	}
	if options.WaitTimeout == 0 {
		options.WaitTimeout = defaultWaitTimeout
	}
	if options.BusyTimeout <= 0 {
		options.BusyTimeout = defaultBusyTimeout
	}
	if options.ContentionLogPath == "" && options.DBPath != ":memory:" && !strings.Contains(options.DBPath, "mode=memory") {
		options.ContentionLogPath = options.DBPath + ".contention.jsonl"
	}

	// SQLite's busy wait is the cross-process equivalent of the namespace gate.
	// Cap it by the contention threshold so a writer never waits indefinitely or
	// beyond the configured service deadline before returning E_TRUST_CONTENTION.
	sqliteBusy := options.BusyTimeout
	if sqliteBusy > options.WaitTimeout {
		sqliteBusy = options.WaitTimeout
	}
	dsn, memory := sqliteDSN(options.DBPath, sqliteBusy)
	if !memory {
		if dir := filepath.Dir(options.DBPath); dir != "." && dir != "" {
			if err := os.MkdirAll(dir, 0o700); err != nil {
				return nil, NewError(CodeStoreFailure, "open store", "create database directory", err)
			}
		}
	}
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, NewError(CodeStoreFailure, "open store", "open SQLite database", err)
	}
	maxConns := options.MaxInflight + 2
	if maxConns < 2 {
		maxConns = 2
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxLifetime(0)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, NewError(CodeStoreFailure, "open store", "ping SQLite database", err)
	}
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		db.Close()
		return nil, NewError(CodeStoreFailure, "open store", "apply schema", err)
	}
	sem := make(chan struct{}, options.MaxInflight)
	for i := 0; i < options.MaxInflight; i++ {
		sem <- struct{}{}
	}
	return &Store{
		db:    db,
		opts:  options,
		sem:   sem,
		gates: make(map[string]chan struct{}),
	}, nil
}

func sqliteDSN(path string, busyTimeout time.Duration) (string, bool) {
	if path == ":memory:" {
		name := fmt.Sprintf("pkitrust-memory-%d", time.Now().UnixNano())
		return fmt.Sprintf("file:%s?mode=memory&cache=shared&_foreign_keys=on&_txlock=immediate&_busy_timeout=%d", name, busyTimeout.Milliseconds()), true
	}
	memory := strings.Contains(path, "mode=memory")
	separator := "?"
	if strings.Contains(path, "?") {
		separator = "&"
	}
	dsn := path + separator + fmt.Sprintf("_foreign_keys=on&_txlock=immediate&_busy_timeout=%d", busyTimeout.Milliseconds())
	if !memory {
		dsn += "&_journal_mode=WAL&_synchronous=FULL"
	}
	return dsn, memory
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS namespaces (
    namespace       TEXT PRIMARY KEY,
    current_version INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshots (
    namespace       TEXT NOT NULL,
    version         INTEGER NOT NULL,
    parent_version  INTEGER NOT NULL,
    content_hash    TEXT NOT NULL,
    trust_hash      TEXT NOT NULL,
    replacement     INTEGER NOT NULL,
    content_json    BLOB NOT NULL,
    audit_json      BLOB NOT NULL,
    PRIMARY KEY (namespace, version),
    FOREIGN KEY (namespace) REFERENCES namespaces(namespace) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS snapshots_namespace_version
    ON snapshots(namespace, version);
`

// Close releases SQLite resources.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) acquire(ctx context.Context, namespace, requestID string, timeout time.Duration) (time.Duration, error) {
	if timeout <= 0 {
		timeout = s.opts.WaitTimeout
	}
	start := time.Now()
	deadline := start.Add(timeout)
	gate := s.gateFor(namespace)
	if err := waitChannel(ctx, gate, deadline); err != nil {
		wait := time.Since(start)
		rec := ContentionRecord{Namespace: namespace, RequestID: requestID, WaitMillis: wait.Milliseconds(), Outcome: "rejected", Code: CodeTrustContention}
		s.recordContention(rec)
		return wait, NewError(CodeTrustContention, "acquire namespace", fmt.Sprintf("namespace %q exceeded wait threshold %s", namespace, timeout), err)
	}
	if err := waitChannel(ctx, s.sem, deadline); err != nil {
		gate <- struct{}{}
		wait := time.Since(start)
		rec := ContentionRecord{Namespace: namespace, RequestID: requestID, WaitMillis: wait.Milliseconds(), Outcome: "rejected", Code: CodeTrustContention}
		s.recordContention(rec)
		return wait, NewError(CodeTrustContention, "acquire transaction slot", fmt.Sprintf("in-flight limit %d exceeded wait threshold %s", s.opts.MaxInflight, timeout), err)
	}
	wait := time.Since(start)
	if wait.Milliseconds() > 0 {
		s.recordContention(ContentionRecord{Namespace: namespace, RequestID: requestID, WaitMillis: wait.Milliseconds(), Outcome: "acquired", Code: CodeOK})
	}
	return wait, nil
}

func (s *Store) release(namespace string) {
	s.sem <- struct{}{}
	gate := s.gateFor(namespace)
	gate <- struct{}{}
}

func (s *Store) gateFor(namespace string) chan struct{} {
	s.gatesMu.Lock()
	defer s.gatesMu.Unlock()
	gate, ok := s.gates[namespace]
	if !ok {
		gate = make(chan struct{}, 1)
		gate <- struct{}{}
		s.gates[namespace] = gate
	}
	return gate
}

func waitChannel(ctx context.Context, channel chan struct{}, deadline time.Time) error {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-channel:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return context.DeadlineExceeded
	}
}

func (s *Store) recordContention(record ContentionRecord) {
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	s.events = append(s.events, record)
	if s.opts.ContentionLogPath == "" {
		return
	}
	data, err := json.Marshal(record)
	if err != nil {
		return
	}
	file, err := os.OpenFile(s.opts.ContentionLogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer file.Close()
	_, _ = file.Write(append(data, '\n'))
}

// Bootstrap atomically creates version 1 for a namespace.
func (s *Store) Bootstrap(ctx context.Context, namespace string, content SnapshotContent) (*Snapshot, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return nil, NewError(CodeInputInvalid, "bootstrap", "namespace is required", nil)
	}
	normalized, err := content.Normalize()
	if err != nil {
		return nil, err
	}
	contentHash, err := normalized.ContentHash()
	if err != nil {
		return nil, err
	}
	trustHash, err := normalized.TrustHash()
	if err != nil {
		return nil, err
	}
	if _, err := s.acquire(ctx, namespace, "bootstrap", s.opts.WaitTimeout); err != nil {
		return nil, err
	}
	defer s.release(namespace)

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, NewError(CodeStoreFailure, "bootstrap", "begin transaction", err)
	}
	defer tx.Rollback()
	var existing int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshots WHERE namespace = ?`, namespace).Scan(&existing); err != nil {
		return nil, NewError(CodeStoreFailure, "bootstrap", "inspect namespace", err)
	}
	if existing != 0 {
		return nil, invalid("bootstrap", "namespace %q already exists", namespace)
	}
	snapshot := Snapshot{
		Namespace:     namespace,
		Version:       1,
		ParentVersion: 0,
		ContentHash:   contentHash,
		TrustHash:     trustHash,
		Content:       normalized,
	}
	initialChains := VerifyAll(normalized, VerificationOptions{})
	audit := ApplyAudit{
		Namespace:      namespace,
		RequestID:      "bootstrap",
		Code:           CodeOK,
		SnapshotBefore: SnapshotRef{},
		SnapshotAfter:  snapshot.Ref(),
		Chains:         chainDiffs(initialChains, initialChains),
		Codes:          collectCodes(initialChains, initialChains, nil, CodeOK),
	}
	auditJSON, err := json.Marshal(audit)
	if err != nil {
		return nil, NewError(CodeStoreFailure, "bootstrap", "encode audit", err)
	}
	contentJSON, err := json.Marshal(normalized)
	if err != nil {
		return nil, NewError(CodeStoreFailure, "bootstrap", "encode snapshot", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO namespaces(namespace, current_version) VALUES (?, 0)`, namespace); err != nil {
		return nil, NewError(CodeStoreFailure, "bootstrap", "create namespace", err)
	}
	if err := insertSnapshot(ctx, tx, snapshot, contentJSON, auditJSON); err != nil {
		return nil, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE namespaces SET current_version = 1 WHERE namespace = ? AND current_version = 0`, namespace)
	if err != nil {
		return nil, NewError(CodeStoreFailure, "bootstrap", "advance namespace", err)
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return nil, NewError(CodeTrustContention, "bootstrap", "namespace changed concurrently", nil)
	}
	if err := tx.Commit(); err != nil {
		return nil, NewError(CodeStoreFailure, "bootstrap", "commit", err)
	}
	return &snapshot, nil
}

func insertSnapshot(ctx context.Context, tx *sql.Tx, snapshot Snapshot, contentJSON, auditJSON []byte) error {
	replacement := 0
	if snapshot.Replacement {
		replacement = 1
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO snapshots(namespace, version, parent_version, content_hash, trust_hash, replacement, content_json, audit_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		snapshot.Namespace, snapshot.Version, snapshot.ParentVersion, snapshot.ContentHash,
		snapshot.TrustHash, replacement, contentJSON, auditJSON)
	if err != nil {
		return NewError(CodeStoreFailure, "insert snapshot", "write snapshot", err)
	}
	return nil
}

type storedSnapshot struct {
	Snapshot Snapshot
	Audit    ApplyAudit
	RowRef   SnapshotRef
}

type loadResult struct {
	Namespace         string
	StoredCurrent     int64
	ContinuousVersion int64
	Snapshots         []storedSnapshot
	Continuous        []storedSnapshot
	Latest            *Snapshot
	LatestAudit       ApplyAudit
	Health            *Error
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) loadNamespace(ctx context.Context, q queryer, namespace string) (*loadResult, error) {
	var storedCurrent int64
	if err := q.QueryRowContext(ctx, `SELECT current_version FROM namespaces WHERE namespace = ?`, namespace).Scan(&storedCurrent); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, NewError(CodeNotFound, "load namespace", fmt.Sprintf("namespace %q does not exist", namespace), nil)
		}
		return nil, NewError(CodeStoreFailure, "load namespace", "read namespace", err)
	}
	rows, err := q.QueryContext(ctx, `
		SELECT version, parent_version, content_hash, trust_hash, replacement, content_json, audit_json
		FROM snapshots WHERE namespace = ? ORDER BY version`, namespace)
	if err != nil {
		return nil, NewError(CodeStoreFailure, "load namespace", "query snapshots", err)
	}
	defer rows.Close()
	result := &loadResult{Namespace: namespace, StoredCurrent: storedCurrent}
	expectedVersion := int64(1)
	expectedParent := int64(0)
	for rows.Next() {
		var snapshot Snapshot
		var replacement int
		var contentJSON, auditJSON []byte
		if err := rows.Scan(&snapshot.Version, &snapshot.ParentVersion, &snapshot.ContentHash, &snapshot.TrustHash, &replacement, &contentJSON, &auditJSON); err != nil {
			return nil, NewError(CodeStoreFailure, "load namespace", "scan snapshot", err)
		}
		snapshot.Namespace = namespace
		snapshot.Replacement = replacement != 0
		rowRef := snapshot.Ref()
		continuous := result.Health == nil && snapshot.Version == expectedVersion && snapshot.ParentVersion == expectedParent
		if continuous {
			if err := json.Unmarshal(contentJSON, &snapshot.Content); err != nil {
				result.Health = NewError(CodeSnapshotVersion, "load namespace", fmt.Sprintf("version %d has invalid content JSON", snapshot.Version), err)
				continuous = false
			} else if err := snapshot.Validate(); err != nil {
				result.Health = NewError(CodeSnapshotVersion, "load namespace", fmt.Sprintf("version %d failed validation", snapshot.Version), err)
				continuous = false
			}
		}
		stored := storedSnapshot{Snapshot: snapshot, RowRef: rowRef}
		if continuous {
			if len(auditJSON) > 0 {
				if err := json.Unmarshal(auditJSON, &stored.Audit); err != nil {
					result.Health = NewError(CodeSnapshotVersion, "load namespace", fmt.Sprintf("version %d has invalid audit JSON", snapshot.Version), err)
					continuous = false
				}
			}
		}
		result.Snapshots = append(result.Snapshots, stored)
		if continuous {
			result.Continuous = append(result.Continuous, stored)
			result.ContinuousVersion = snapshot.Version
			copySnapshot := snapshot
			result.Latest = &copySnapshot
			result.LatestAudit = stored.Audit
			expectedVersion++
			expectedParent = snapshot.Version
		} else if result.Health == nil {
			result.Health = NewError(CodeSnapshotVersion, "load namespace", fmt.Sprintf("expected version %d with parent %d, found version %d with parent %d", expectedVersion, expectedParent, snapshot.Version, snapshot.ParentVersion), nil)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, NewError(CodeStoreFailure, "load namespace", "iterate snapshots", err)
	}
	if len(result.Snapshots) == 0 {
		return nil, NewError(CodeSnapshotVersion, "load namespace", "namespace has no snapshots", nil)
	}
	if result.Latest == nil {
		return nil, NewError(CodeSnapshotVersion, "load namespace", "no continuous snapshot is available", nil)
	}
	if result.StoredCurrent != result.ContinuousVersion && result.Health == nil {
		result.Health = NewError(CodeSnapshotVersion, "load namespace", fmt.Sprintf("namespace points to version %d but continuous version is %d", result.StoredCurrent, result.ContinuousVersion), nil)
	}
	return result, nil
}

// Load returns the latest continuous snapshot. If a version gap or hash failure
// is discovered, it returns the newest continuous snapshot together with an
// E_SNAPSHOT_VERSION error so callers can explicitly serve in degraded mode or
// reject service. It does not mutate the database.
func (s *Store) Load(ctx context.Context, namespace string) (*Snapshot, error) {
	result, err := s.loadNamespace(ctx, s.db, namespace)
	if err != nil {
		return nil, err
	}
	if result.Health != nil {
		return result.Latest, result.Health
	}
	return result.Latest, nil
}

// LoadSnapshotVersion replays an exact retained snapshot version.
func (s *Store) LoadSnapshotVersion(ctx context.Context, namespace string, version int64) (*Snapshot, error) {
	result, err := s.loadNamespace(ctx, s.db, namespace)
	if err != nil {
		return nil, err
	}
	for _, stored := range result.Continuous {
		if stored.Snapshot.Version == version {
			snapshot := stored.Snapshot
			return &snapshot, nil
		}
	}
	return nil, NewError(CodeNotFound, "load snapshot version", fmt.Sprintf("continuous snapshot version %d does not exist", version), nil)
}

// ListSnapshotVersions returns retained rows in ascending version order. Gaps,
// if present, are represented by the missing version numbers.
func (s *Store) ListSnapshotVersions(ctx context.Context, namespace string) ([]SnapshotRef, error) {
	result, err := s.loadNamespace(ctx, s.db, namespace)
	if err != nil {
		return nil, err
	}
	refs := make([]SnapshotRef, 0, len(result.Snapshots))
	for _, stored := range result.Snapshots {
		refs = append(refs, stored.RowRef)
	}
	return refs, nil
}

// InjectVersionGap is a diagnostic-only operation used by the example CLI to
// validate E_SNAPSHOT_VERSION handling. It clones the current snapshot into a
// version beyond a hole and advances the namespace pointer. Production code
// must not call it. The next Load/Apply/Report rejects service.
func (s *Store) InjectVersionGap(ctx context.Context, namespace string, missingVersion int64) error {
	if missingVersion < 2 {
		return NewError(CodeInputInvalid, "inject version gap", "missingVersion must be at least 2", nil)
	}
	if _, err := s.acquire(ctx, namespace, "diagnostic-version-gap", s.opts.WaitTimeout); err != nil {
		return err
	}
	defer s.release(namespace)
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return NewError(CodeStoreFailure, "inject version gap", "begin transaction", err)
	}
	defer tx.Rollback()
	result, err := s.loadNamespace(ctx, tx, namespace)
	if err != nil {
		return err
	}
	if result.Health != nil {
		return result.Health
	}
	gapVersion := missingVersion + 1
	if gapVersion <= result.ContinuousVersion {
		return NewError(CodeInputInvalid, "inject version gap", "gap must follow the current version", nil)
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshots WHERE namespace = ? AND version = ?`, namespace, gapVersion).Scan(&count); err != nil {
		return NewError(CodeStoreFailure, "inject version gap", "inspect target", err)
	}
	if count != 0 {
		return NewError(CodeInputInvalid, "inject version gap", "target version already exists", nil)
	}
	clone := *result.Latest
	clone.Version = gapVersion
	clone.ParentVersion = result.ContinuousVersion
	contentJSON, err := json.Marshal(clone.Content)
	if err != nil {
		return NewError(CodeStoreFailure, "inject version gap", "encode clone", err)
	}
	auditJSON, _ := json.Marshal(ApplyAudit{Namespace: namespace, RequestID: "diagnostic-version-gap", Code: CodeSnapshotVersion, SnapshotBefore: result.Latest.Ref(), SnapshotAfter: clone.Ref()})
	if err := insertSnapshot(ctx, tx, clone, contentJSON, auditJSON); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE namespaces SET current_version = ? WHERE namespace = ?`, gapVersion, namespace); err != nil {
		return NewError(CodeStoreFailure, "inject version gap", "advance namespace", err)
	}
	if err := tx.Commit(); err != nil {
		return NewError(CodeStoreFailure, "inject version gap", "commit", err)
	}
	return nil
}

func sortedContention(records []ContentionRecord) []ContentionRecord {
	out := make([]ContentionRecord, len(records))
	copy(out, records)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		if out[i].RequestID != out[j].RequestID {
			return out[i].RequestID < out[j].RequestID
		}
		if out[i].WaitMillis != out[j].WaitMillis {
			return out[i].WaitMillis < out[j].WaitMillis
		}
		if out[i].Outcome != out[j].Outcome {
			return out[i].Outcome < out[j].Outcome
		}
		return out[i].Code < out[j].Code
	})
	return out
}

func (s *Store) contentionRecords() []ContentionRecord {
	s.eventMu.Lock()
	inMemory := append([]ContentionRecord(nil), s.events...)
	path := s.opts.ContentionLogPath
	s.eventMu.Unlock()
	if path == "" {
		return sortedContention(inMemory)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return sortedContention(inMemory)
	}
	records := make([]ContentionRecord, 0)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var record ContentionRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue
		}
		records = append(records, record)
	}
	return sortedContention(records)
}
