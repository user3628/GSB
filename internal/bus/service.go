package bus

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const defaultLeaseDuration = 5 * time.Second

type Service struct {
	db            *sql.DB
	dbPath        string
	leaseDuration time.Duration
	now           func() time.Time

	locksMu sync.Mutex
	locks   map[string]*sync.Mutex

	writeMu sync.Mutex

	publishMu sync.Mutex

	requestsMu sync.Mutex
	requests   map[string]*requestCall

	recoveryMu sync.Mutex

	faultMu sync.Mutex
	fault   func(stage string) error
}

func Open(ctx context.Context, options ServiceOptions) (*Service, error) {
	if strings.TrimSpace(options.DBPath) == "" {
		return nil, invalidArgument("db path is required")
	}
	if options.LeaseDuration <= 0 {
		options.LeaseDuration = defaultLeaseDuration
	}
	if options.Now == nil {
		options.Now = func() time.Time { return time.Now().UTC() }
	}

	path := options.DBPath
	if path != ":memory:" && !strings.HasPrefix(path, "file:") {
		parent := filepath.Dir(path)
		if parent != "." {
			if err := os.MkdirAll(parent, 0o755); err != nil {
				return nil, fmt.Errorf("create database directory: %w", err)
			}
		}
		path = filepath.ToSlash(path)
	}
	separator := "?"
	if strings.Contains(path, "?") {
		separator = "&"
	}
	dsn := path + separator + "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)"
	if options.DBPath == ":memory:" {
		dsn = "file:eventbus-memory?mode=memory&cache=shared&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)
	if options.DBPath == ":memory:" {
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize schema: %w", err)
	}

	service := &Service{
		db:            db,
		dbPath:        options.DBPath,
		leaseDuration: options.LeaseDuration,
		now:           options.Now,
		locks:         make(map[string]*sync.Mutex),
		requests:      make(map[string]*requestCall),
	}
	if !options.SkipRecovery {
		if _, err := service.Recover(ctx); err != nil {
			db.Close()
			return nil, err
		}
	}
	return service, nil
}

func (s *Service) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Service) DB() *sql.DB { return s.db }

func (s *Service) nowMS() int64 { return s.now().UTC().UnixMilli() }

func (s *Service) partitionLock(topic string, partition int, group string) *sync.Mutex {
	// All consumer groups on one physical partition share the writer lock. The
	// group argument is retained in the signature for call-site clarity.
	key := fmt.Sprintf("%s\x00%d", topic, partition)
	s.locksMu.Lock()
	defer s.locksMu.Unlock()
	lock := s.locks[key]
	if lock == nil {
		lock = &sync.Mutex{}
		s.locks[key] = lock
	}
	return lock
}

// SetFaultInjector is used by tests to terminate an operation at a durable
// boundary. It is intentionally small and does not expose production faults.
func (s *Service) SetFaultInjector(fn func(stage string) error) {
	s.faultMu.Lock()
	defer s.faultMu.Unlock()
	s.fault = fn
}

func (s *Service) injectFault(stage string) error {
	s.faultMu.Lock()
	fn := s.fault
	s.faultMu.Unlock()
	if fn == nil {
		return nil
	}
	return fn(stage)
}

// beginWriteTx serializes SQLite write transactions inside this process. The
// returned release function is idempotent and must be called before any
// follow-up write that uses execWrite (for example, marking the intent done).
func (s *Service) beginWriteTx(ctx context.Context) (*sql.Tx, func(), error) {
	s.writeMu.Lock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.writeMu.Unlock()
		return nil, nil, err
	}
	var once sync.Once
	release := func() { once.Do(s.writeMu.Unlock) }
	return tx, release, nil
}

func (s *Service) execWrite(ctx context.Context, query string, args ...any) (sql.Result, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.db.ExecContext(ctx, query, args...)
}

func (s *Service) validateTopicGroup(ctx context.Context, q queryer, topic, group string) error {
	var algorithm string
	err := q.QueryRowContext(ctx, `SELECT algorithm FROM topic_groups WHERE topic = ? AND group_name = ?`, topic, group).Scan(&algorithm)
	if err == sql.ErrNoRows {
		var exists int
		if e := q.QueryRowContext(ctx, `SELECT 1 FROM topics WHERE topic = ?`, topic).Scan(&exists); e == sql.ErrNoRows {
			return NewError(CodeUnknownTopic, "topic does not exist: "+topic)
		} else if e != nil {
			return WrapError(CodeInternal, "read topic", e)
		}
		return NewError(CodeUnknownGroup, "consumer group does not exist: "+group)
	}
	if err != nil {
		return WrapError(CodeInternal, "read topic group", err)
	}
	return nil
}

func (s *Service) validateTopicPartition(ctx context.Context, q queryer, topic string, partition int) error {
	var partitions int
	err := q.QueryRowContext(ctx, `SELECT partitions FROM topics WHERE topic = ?`, topic).Scan(&partitions)
	if err == sql.ErrNoRows {
		return NewError(CodeUnknownTopic, "topic does not exist: "+topic)
	}
	if err != nil {
		return WrapError(CodeInternal, "read topic", err)
	}
	if partition < 0 || partition >= partitions {
		return NewError(CodePartitionIDsInvalid, fmt.Sprintf("partition %d is outside [0,%d)", partition, partitions))
	}
	return nil
}

func (s *Service) groupAlgorithm(ctx context.Context, q queryer, topic, group string) (string, error) {
	var algorithm string
	if err := q.QueryRowContext(ctx, `SELECT algorithm FROM topic_groups WHERE topic = ? AND group_name = ?`, topic, group).Scan(&algorithm); err != nil {
		if err == sql.ErrNoRows {
			return "", NewError(CodeUnknownGroup, "consumer group does not exist: "+group)
		}
		return "", WrapError(CodeInternal, "read group algorithm", err)
	}
	return algorithm, nil
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}
