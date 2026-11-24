package tokenforge

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

// Service is the host-facing token refresh, revocation, quota and snapshot
// component. It is safe for concurrent use by multiple goroutines.
type Service struct {
	db *sql.DB

	cfg        Config
	signingKey []byte

	admission  *admissionLimiter
	tenantGate *keyedGate
	tokenGate  *keyedGate
	sqliteGate *keyedGate
	attempts   *attemptTracker
	metrics    *metricsCollector

	skipSnapshotOnce atomic.Bool
}

// NewService opens (or creates) the SQLite database and initializes its schema.
func NewService(ctx context.Context, cfg Config) (*Service, error) {
	cfg = cfg.normalized()
	if strings.TrimSpace(cfg.DatabasePath) == "" {
		return nil, codeError(CodeInvalidArgument, "database path is required")
	}
	if len(cfg.SigningKey) < 16 {
		return nil, codeError(CodeInvalidArgument, "signing key must be at least 16 bytes")
	}
	if strings.TrimSpace(cfg.DatabasePath) != ":memory:" && !strings.HasPrefix(cfg.DatabasePath, "file:") {
		parent := filepath.Dir(cfg.DatabasePath)
		if parent != "." && parent != "" {
			if err := os.MkdirAll(parent, 0o755); err != nil {
				return nil, fmt.Errorf("create database directory: %w", err)
			}
		}
	}
	db, err := sql.Open("sqlite", sqliteDSN(cfg.DatabasePath, cfg.SQLiteBusyTimeout))
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(cfg.MaxInFlight + 2)
	db.SetMaxIdleConns(cfg.MaxInFlight + 2)
	db.SetConnMaxLifetime(0)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	metrics := newMetrics(cfg.MaxInFlight)
	s := &Service{
		db:         db,
		cfg:        cfg,
		signingKey: append([]byte(nil), cfg.SigningKey...),
		admission:  newAdmissionLimiter(cfg.MaxInFlight, cfg.AdmissionWait, metrics),
		tenantGate: newKeyedGate(),
		tokenGate:  newKeyedGate(),
		sqliteGate: newKeyedGate(),
		attempts:   newAttemptTracker(),
		metrics:    metrics,
	}
	if err := s.initSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func sqliteDSN(path string, busy time.Duration) string {
	timeoutMS := busy.Milliseconds()
	if timeoutMS <= 0 {
		timeoutMS = 1
	}
	base := path
	if !strings.HasPrefix(base, "file:") && base != ":memory:" {
		base = "file:" + filepath.ToSlash(base)
	}
	pragma := fmt.Sprintf("?_pragma=busy_timeout(%d)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)&_pragma=synchronous(FULL)", timeoutMS)
	if strings.Contains(base, "?") {
		return base + "&" + strings.TrimPrefix(pragma, "?")
	}
	return base + pragma
}

func (s *Service) Close() error { return s.db.Close() }

// Metrics returns an instantaneous, concurrency-safe admission snapshot.
func (s *Service) Metrics() Metrics { return s.metrics.snapshot() }

// InjectNextSnapshotGap intentionally makes the next successful mutating
// transaction skip exactly one snapshot version. It exists for external fault
// injection/acceptance samples; normal hosts never call it. The gap is detected
// by ReplaySnapshots and returns E_SNAPSHOT_VERSION with the contiguous prefix.
func (s *Service) InjectNextSnapshotGap() { s.skipSnapshotOnce.Store(true) }

func (s *Service) initSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS tenants (
			tenant_id TEXT PRIMARY KEY,
			user_id TEXT NOT NULL,
			unit_cost_fixed INTEGER NOT NULL CHECK (unit_cost_fixed >= 0),
			usage_fixed INTEGER NOT NULL CHECK (usage_fixed >= 0),
			balance_fixed INTEGER NOT NULL CHECK (balance_fixed >= 0),
			spent_fixed INTEGER NOT NULL DEFAULT 0 CHECK (spent_fixed >= 0),
			error_entry_count INTEGER NOT NULL DEFAULT 0 CHECK (error_entry_count >= 0),
			truncating_count INTEGER NOT NULL DEFAULT 0 CHECK (truncating_count >= 0),
			remainder_units_total INTEGER NOT NULL DEFAULT 0 CHECK (remainder_units_total >= 0)
		)`,
		`CREATE TABLE IF NOT EXISTS families (
			tenant_id TEXT NOT NULL,
			family_id TEXT NOT NULL,
			user_id TEXT NOT NULL,
			current_sequence INTEGER NOT NULL CHECK (current_sequence >= 1),
			status TEXT NOT NULL,
			revoke_reason TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			PRIMARY KEY (tenant_id, family_id),
			FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
		)`,
		`CREATE TABLE IF NOT EXISTS tokens (
			token_id TEXT PRIMARY KEY,
			token_hash TEXT NOT NULL UNIQUE,
			tenant_id TEXT NOT NULL,
			user_id TEXT NOT NULL,
			family_id TEXT NOT NULL,
			sequence INTEGER NOT NULL CHECK (sequence >= 1),
			kind TEXT NOT NULL,
			issued_at INTEGER NOT NULL,
			expires_at INTEGER NOT NULL,
			status TEXT NOT NULL,
			consumed_at INTEGER NOT NULL DEFAULT 0,
			revoked_at INTEGER NOT NULL DEFAULT 0,
			FOREIGN KEY (tenant_id, family_id) REFERENCES families(tenant_id, family_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_tokens_tenant_family
			ON tokens(tenant_id, family_id, sequence, kind, token_id)`,
		`CREATE INDEX IF NOT EXISTS idx_families_tenant_order
			ON families(tenant_id, family_id)`,
		`CREATE TABLE IF NOT EXISTS charge_ledger (
			entry_id TEXT PRIMARY KEY,
			tenant_id TEXT NOT NULL,
			family_id TEXT NOT NULL,
			refresh_token_id TEXT NOT NULL,
			sequence INTEGER NOT NULL,
			unit_cost_fixed INTEGER NOT NULL,
			usage_fixed INTEGER NOT NULL,
			product_fixed INTEGER NOT NULL,
			charged_fixed INTEGER NOT NULL,
			remainder_units INTEGER NOT NULL,
			snapshot_version INTEGER NOT NULL,
			created_at INTEGER NOT NULL,
			FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_charge_tenant_order
			ON charge_ledger(tenant_id, entry_id)`,
		`CREATE TABLE IF NOT EXISTS tenant_state (
			tenant_id TEXT PRIMARY KEY,
			version INTEGER NOT NULL CHECK (version >= 0),
			content_hash TEXT NOT NULL,
			FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
		)`,
		`CREATE TABLE IF NOT EXISTS snapshots (
			tenant_id TEXT NOT NULL,
			version INTEGER NOT NULL CHECK (version >= 1),
			prev_hash TEXT NOT NULL,
			content_hash TEXT NOT NULL,
			content TEXT NOT NULL,
			operation TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			PRIMARY KEY (tenant_id, version),
			FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_snapshots_tenant_version
			ON snapshots(tenant_id, version)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("initialize sqlite schema: %w", err)
		}
	}
	return nil
}

// VerifySnapshotChain validates all snapshots for a tenant without mutating the
// database. The caller receives the latest contiguous state even when a gap or
// integrity error is present.
func (s *Service) VerifySnapshotChain(ctx context.Context, tenantID string) error {
	_, err := s.ReplaySnapshots(ctx, tenantID)
	return err
}
