package cache

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type Config struct {
	Dir           string
	CapacityBytes int64
	Clock         Clock
	CrashPoint    string
	CrashHook     CrashHook
}

type Store struct {
	dir           string
	dbPath        string
	artifactDir   string
	stagingDir    string
	db            *sql.DB
	capacityBytes int64
	clock         Clock
	crashPoint    string
	crashHook     CrashHook

	writeMu    sync.Mutex
	visibility sync.RWMutex
	catalog    *catalog
	generation uint64
}

func Open(ctx context.Context, config Config) (*Store, error) {
	if config.Dir == "" {
		return nil, newError(CodeEntryInvalid, "cache directory is required", nil)
	}
	if config.CapacityBytes < 0 {
		return nil, newError(CodeEntryInvalid, "cache capacity must be non-negative", map[string]any{
			"capacity_bytes": config.CapacityBytes,
		})
	}
	clock := config.Clock
	if clock == nil {
		clock = systemClock{}
	}
	dir, err := filepath.Abs(config.Dir)
	if err != nil {
		return nil, wrapError(CodeIO, "cannot resolve cache directory", map[string]any{"dir": config.Dir}, err)
	}
	artifactDir := filepath.Join(dir, "artifacts")
	stagingDir := filepath.Join(dir, "staging")
	for _, path := range []string{dir, artifactDir, stagingDir} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			return nil, wrapError(CodeIO, "cannot create cache directory", map[string]any{"path": path}, err)
		}
	}

	dbPath := filepath.Join(dir, "cache.sqlite3")
	dsnURL := &url.URL{Scheme: "file", Path: filepath.ToSlash(dbPath)}
	query := dsnURL.Query()
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "journal_mode(WAL)")
	query.Add("_pragma", "foreign_keys(ON)")
	query.Add("_pragma", "synchronous(FULL)")
	dsnURL.RawQuery = query.Encode()
	dsn := dsnURL.String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, wrapError(CodeIO, "cannot open SQLite database", map[string]any{"path": dbPath}, err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, wrapError(CodeIO, "cannot connect to SQLite database", map[string]any{"path": dbPath}, err)
	}
	store := &Store{
		dir:           dir,
		dbPath:        dbPath,
		artifactDir:   artifactDir,
		stagingDir:    stagingDir,
		db:            db,
		capacityBytes: config.CapacityBytes,
		clock:         clock,
		crashPoint:    config.CrashPoint,
		crashHook:     config.CrashHook,
		catalog:       emptyCatalog(),
	}
	if err := store.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := store.recover(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	c, err := store.loadCatalog(ctx)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	store.publish(c)
	return store, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) Dir() string          { return s.dir }
func (s *Store) ArtifactDir() string  { return s.artifactDir }
func (s *Store) CapacityBytes() int64 { return s.capacityBytes }
func (s *Store) DBPath() string       { return s.dbPath }
func (s *Store) Now() time.Time       { return s.clock.Now().UTC() }
func (s *Store) artifactPath(digest string) (string, error) {
	name, err := DigestPath(digest)
	if err != nil {
		return "", err
	}
	return filepath.Join(s.artifactDir, name), nil
}

func (s *Store) relativeArtifactPath(digest string) (string, error) {
	name, err := DigestPath(digest)
	if err != nil {
		return "", err
	}
	return filepath.ToSlash(filepath.Join("artifacts", name)), nil
}

func (s *Store) readSequence(ctx context.Context) (int64, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='snapshot_sequence'`).Scan(&value)
	if err != nil {
		return 0, wrapError(CodeIO, "cannot read snapshot sequence", nil, err)
	}
	sequence, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, wrapError(CodeIO, "snapshot sequence is corrupt", map[string]any{"value": value}, err)
	}
	return sequence, nil
}

func (s *Store) setSequenceTx(ctx context.Context, tx *sql.Tx, sequence int64) error {
	_, err := tx.ExecContext(ctx, `UPDATE metadata SET value=? WHERE key='snapshot_sequence'`, strconv.FormatInt(sequence, 10))
	if err != nil {
		return wrapError(CodeIO, "cannot persist snapshot sequence", nil, err)
	}
	return nil
}

func ns(t time.Time) int64 { return t.UTC().UnixNano() }
func fromNS(value int64) time.Time {
	return time.Unix(0, value).UTC()
}
func formatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
func parseTime(value string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, value)
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanEntry(row rowScanner) (Entry, error) {
	var entry Entry
	var retainUntilNS, createdAtNS, lastHitNS int64
	var state string
	err := row.Scan(
		&entry.EntryID, &entry.CacheKey, &entry.StageID, &entry.InputFingerprint,
		&entry.ToolchainVersion, &entry.ArtifactDigest, &entry.ArtifactSize,
		&entry.Hits, &retainUntilNS, &createdAtNS, &lastHitNS, &state, &entry.Version,
	)
	if err != nil {
		return Entry{}, err
	}
	entry.State = State(state)
	entry.RetainUntil = fromNS(retainUntilNS)
	entry.CreatedAt = fromNS(createdAtNS)
	entry.LastHitAt = fromNS(lastHitNS)
	return entry, nil
}

func dbError(message string, err error) error {
	return wrapError(CodeIO, message, nil, err)
}

func (s *Store) String() string {
	return fmt.Sprintf("Store(%s, capacity=%d)", s.dbPath, s.capacityBytes)
}
