// Package sqlitepool is the embeddable public facade for the local SQLite
// connection pool. It owns the durable metadata store and exposes only stable
// domain types and pool operations to upper-layer services.
package sqlitepool

import (
	"context"
	"fmt"
	"sync"

	"local/sqlitepool/internal/dbconn"
	"local/sqlitepool/internal/domain"
	corepool "local/sqlitepool/internal/pool"
	"local/sqlitepool/internal/store"
)

type Config = domain.Config
type Lease = domain.Lease
type ReleaseResult = domain.ReleaseResult
type QueryResult = domain.QueryResult
type ConnectionRecord = domain.ConnectionRecord
type PoolStats = domain.PoolStats
type PoolSnapshot = domain.PoolSnapshot
type Event = domain.Event
type RecoveryReport = domain.RecoveryReport
type AcquireOptions = corepool.AcquireOptions
type QueryOptions = corepool.QueryOptions
type ForceCloseResult = corepool.ForceCloseResult
type BusinessError = domain.BusinessError

const (
	CodeAcquireTimeout     = domain.CodeAcquireTimeout
	CodePoolExhausted      = domain.CodePoolExhausted
	CodeDialFailed         = domain.CodeDialFailed
	CodeQueryTimeout       = domain.CodeQueryTimeout
	CodeConnReset          = domain.CodeConnReset
	CodeConflictingRequest = domain.CodeConflictingRequest
	CodeStaleConfig        = domain.CodeStaleConfig
)

type Options struct {
	DatabasePath string
	MetadataPath string
	Config       Config
}

type Pool struct {
	inner        *corepool.Pool
	store        *store.Store
	shutdownOnce sync.Once
	shutdownErr  error
}

// Open initializes or recovers the metadata ledger, scans unfinished intents,
// then starts the single-writer pool. If metadata already has a config version,
// it wins over Options.Config; otherwise Options.Config is seeded.
func Open(ctx context.Context, opts Options) (*Pool, error) {
	if opts.DatabasePath == "" || opts.MetadataPath == "" {
		return nil, domain.NewError(domain.CodeConfigInvalid, "database and metadata paths are required", map[string]any{
			"database_path": opts.DatabasePath,
			"metadata_path": opts.MetadataPath,
		})
	}
	if err := opts.Config.Validate(); err != nil {
		return nil, domain.WrapError(domain.CodeConfigInvalid, "invalid pool configuration", map[string]any{
			"config": opts.Config,
		}, err)
	}
	st, err := store.Open(ctx, opts.MetadataPath)
	if err != nil {
		return nil, fmt.Errorf("open metadata store: %w", err)
	}
	if _, err := st.SeedConfig(ctx, opts.Config); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("seed config: %w", err)
	}
	cfg, err := st.LoadConfig(ctx)
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("load config: %w", err)
	}
	if _, err := st.Recover(ctx); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("recover intents: %w", err)
	}
	inner, err := corepool.New(ctx, cfg, st, dbconn.SQLiteOpener{Path: opts.DatabasePath})
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	return &Pool{inner: inner, store: st}, nil
}

func (p *Pool) Acquire(ctx context.Context, opts AcquireOptions) (Lease, error) {
	return p.inner.Acquire(ctx, opts)
}

func (p *Pool) Release(ctx context.Context, connectionID, requestID string) (ReleaseResult, error) {
	return p.inner.Release(ctx, connectionID, requestID)
}

func (p *Pool) Query(ctx context.Context, connectionID, query string, opts QueryOptions) (QueryResult, error) {
	return p.inner.Query(ctx, connectionID, query, opts)
}

func (p *Pool) ForceClose(ctx context.Context, connectionID, reason string) (ForceCloseResult, error) {
	return p.inner.ForceClose(ctx, connectionID, reason)
}

func (p *Pool) Reconfigure(ctx context.Context, cfg Config) (Config, error) {
	return p.inner.Reconfigure(ctx, cfg)
}

func (p *Pool) Config() Config { return p.inner.Config() }

func (p *Pool) Snapshot() PoolSnapshot { return p.inner.Snapshot() }

func (p *Pool) LatestRecoveryReport(ctx context.Context) (RecoveryReport, error) {
	return p.store.LatestRecoveryReport(ctx)
}

func (p *Pool) Shutdown(ctx context.Context) error {
	p.shutdownOnce.Do(func() {
		err := p.inner.Shutdown(ctx)
		closeErr := p.store.Close()
		if err != nil {
			p.shutdownErr = err
		} else {
			p.shutdownErr = closeErr
		}
	})
	return p.shutdownErr
}
