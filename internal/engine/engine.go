package engine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"nonceguard/internal/canonical"
	"nonceguard/internal/model"
	"nonceguard/internal/store"
)

type Failure struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
}

func (f *Failure) Error() string { return f.Code + ": " + f.Message }

func NewFailure(code, message string, context map[string]any) *Failure {
	if context == nil {
		context = map[string]any{}
	}
	return &Failure{Code: code, Message: message, Context: context}
}

type Clock func() int64

type Engine struct {
	Store              *store.Store
	mu                 sync.Mutex
	Clock              Clock
	AllowRefOverride   bool
	DefaultRecordDelay time.Duration
	attemptSeq         uint64
	attemptPrefix      string
}

func New(st *store.Store) *Engine {
	prefixBytes := make([]byte, 6)
	if _, err := rand.Read(prefixBytes); err != nil {
		prefixBytes = []byte(time.Now().Format("150405.000000"))
	}
	return &Engine{
		Store: st, Clock: func() int64 { return time.Now().UnixMilli() },
		AllowRefOverride: true, attemptPrefix: hex.EncodeToString(prefixBytes),
	}
}

func (e *Engine) EnsureDefaultConfig(ctx context.Context, nowMs int64) (model.Snapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if nowMs <= 0 {
		nowMs = e.Clock()
	}
	var version int64
	err := e.Store.DB.QueryRowContext(ctx, `SELECT version FROM active_config WHERE id=1`).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		cfg := model.DefaultSnapshot(1, nowMs)
		if err := e.saveConfigTx(ctx, nil, cfg, true); err != nil {
			return model.Snapshot{}, err
		}
		return cfg, nil
	}
	if err != nil {
		return model.Snapshot{}, err
	}
	return e.GetConfig(ctx, version)
}

func (e *Engine) saveConfigTx(ctx context.Context, tx *sql.Tx, cfg model.Snapshot, setActive bool) error {
	fieldsJSON := model.MustJSON(cfg.SignatureFields)
	exec := func(query string, args ...any) (sql.Result, error) {
		if tx != nil {
			return tx.ExecContext(ctx, query, args...)
		}
		return e.Store.DB.ExecContext(ctx, query, args...)
	}
	if _, err := exec(`INSERT INTO config_snapshots(version,generated_at_ms,min_offset_ms,max_offset_ms,nonce_window_ms,fields_json,digest,created_at_ms)
		VALUES(?,?,?,?,?,?,?,?)`,
		cfg.Version, cfg.GeneratedAtMs, cfg.MinOffsetMs, cfg.MaxOffsetMs, cfg.NonceWindowMs, string(fieldsJSON), cfg.Digest, time.Now().UnixMilli()); err != nil {
		return err
	}
	if setActive {
		if _, err := exec(`INSERT INTO active_config(id,version) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET version=excluded.version`, cfg.Version); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) GetConfig(ctx context.Context, version int64) (model.Snapshot, error) {
	var cfg model.Snapshot
	var fieldsJSON string
	err := e.Store.DB.QueryRowContext(ctx, `SELECT version,generated_at_ms,min_offset_ms,max_offset_ms,nonce_window_ms,fields_json,digest FROM config_snapshots WHERE version=?`, version).
		Scan(&cfg.Version, &cfg.GeneratedAtMs, &cfg.MinOffsetMs, &cfg.MaxOffsetMs, &cfg.NonceWindowMs, &fieldsJSON, &cfg.Digest)
	if err != nil {
		return model.Snapshot{}, err
	}
	if err := unmarshalFields(fieldsJSON, &cfg.SignatureFields); err != nil {
		return model.Snapshot{}, err
	}
	return cfg, nil
}

func (e *Engine) ActiveConfig(ctx context.Context) (model.Snapshot, error) {
	var version int64
	if err := e.Store.DB.QueryRowContext(ctx, `SELECT version FROM active_config WHERE id=1`).Scan(&version); err != nil {
		return model.Snapshot{}, err
	}
	return e.GetConfig(ctx, version)
}

func (e *Engine) ListConfigs(ctx context.Context) ([]model.Snapshot, error) {
	rows, err := e.Store.DB.QueryContext(ctx, `SELECT version,generated_at_ms,min_offset_ms,max_offset_ms,nonce_window_ms,fields_json,digest FROM config_snapshots ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Snapshot{}
	for rows.Next() {
		var cfg model.Snapshot
		var fieldsJSON string
		if err := rows.Scan(&cfg.Version, &cfg.GeneratedAtMs, &cfg.MinOffsetMs, &cfg.MaxOffsetMs, &cfg.NonceWindowMs, &fieldsJSON, &cfg.Digest); err != nil {
			return nil, err
		}
		if err := unmarshalFields(fieldsJSON, &cfg.SignatureFields); err != nil {
			return nil, err
		}
		out = append(out, cfg)
	}
	return out, rows.Err()
}

func unmarshalFields(raw string, target *[]string) error {
	return json.Unmarshal([]byte(raw), target)
}

func (e *Engine) SubmitConfig(ctx context.Context, incoming model.Snapshot) (model.Snapshot, *Failure) {
	e.mu.Lock()
	defer e.mu.Unlock()
	current, err := e.ActiveConfig(ctx)
	if err != nil {
		return model.Snapshot{}, NewFailure(model.CodeInternal, "active configuration is unavailable", map[string]any{"error": err.Error()})
	}
	if incoming.Version <= current.Version {
		return model.Snapshot{}, NewFailure(model.CodeStaleSnapshot, "snapshot version is not greater than active version", map[string]any{
			"submitted_version": incoming.Version,
			"active_version":    current.Version,
		})
	}
	if incoming.Version != current.Version+1 {
		return model.Snapshot{}, NewFailure(model.CodeConfigVersionGap, "snapshot version skips a required contiguous version", map[string]any{
			"submitted_version": incoming.Version,
			"active_version":    current.Version,
			"expected_version":  current.Version + 1,
		})
	}
	if !incoming.VerifyDigest() {
		return model.Snapshot{}, NewFailure(model.CodeSnapshotDigest, "snapshot overall digest verification failed; active configuration remains available", map[string]any{
			"submitted_version": incoming.Version,
			"expected_digest":   configDigestOrEmpty(incoming),
			"provided_digest":   incoming.Digest,
			"active_version":    current.Version,
		})
	}
	if err := incoming.Validate(); err != nil {
		return model.Snapshot{}, NewFailure(model.CodeSnapshotInvalid, "snapshot is invalid; active configuration remains available", map[string]any{
			"submitted_version": incoming.Version,
			"reason":            err.Error(),
			"active_version":    current.Version,
		})
	}
	tx, err := e.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return model.Snapshot{}, NewFailure(model.CodeInternal, "cannot begin config transaction", map[string]any{"error": err.Error()})
	}
	defer tx.Rollback()
	if err := e.saveConfigTx(ctx, tx, incoming, true); err != nil {
		return model.Snapshot{}, NewFailure(model.CodeInternal, "cannot atomically replace configuration", map[string]any{"error": err.Error()})
	}
	if err := tx.Commit(); err != nil {
		return model.Snapshot{}, NewFailure(model.CodeInternal, "cannot commit configuration replacement", map[string]any{"error": err.Error()})
	}
	return incoming, nil
}

type referenceResolution struct {
	RequestedMs       int64 `json:"requested_ms,omitempty"`
	HighWaterMs       int64 `json:"high_water_ms"`
	ReferenceMs       int64 `json:"reference_ms"`
	RollbackProtected bool  `json:"clock_rollback_protected"`
}

func (e *Engine) resolveReference(ctx context.Context, requested int64, update bool) (referenceResolution, error) {
	high, err := e.Store.HighWaterMs(ctx)
	if err != nil {
		return referenceResolution{}, err
	}
	base := e.Clock()
	if requested > 0 && e.AllowRefOverride {
		base = requested
	}
	ref := base
	protected := false
	if high > ref {
		ref = high
		protected = true
	}
	if update && ref > high {
		if _, err := e.Store.DB.ExecContext(ctx, `UPDATE clock_state SET high_water_ms=?, updated_at_ms=? WHERE id=1 AND high_water_ms<?`, ref, e.Clock(), ref); err != nil {
			return referenceResolution{}, err
		}
	}
	return referenceResolution{RequestedMs: requested, HighWaterMs: high, ReferenceMs: ref, RollbackProtected: protected}, nil
}

func configDigestOrEmpty(cfg model.Snapshot) string {
	return canonical.ConfigDigest(cfg.DigestInput())
}
