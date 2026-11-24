package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"checkpointengine/internal/model"
	"checkpointengine/internal/store"
)

type Options struct {
	DataDir   string
	Failpoint func(point string) error
	Now       func() time.Time
}

type Engine struct {
	store     *store.Store
	dataDir   string
	blobDir   string
	writeMu   sync.Mutex
	failpoint func(point string) error
	now       func() time.Time
}

func New(st *store.Store, opts Options) (*Engine, error) {
	dataDir := strings.TrimSpace(opts.DataDir)
	if dataDir == "" {
		dataDir = filepath.Join(filepath.Dir(st.Path), "data")
	}
	abs, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, err
	}
	blobDir := filepath.Join(abs, "blobs")
	if err := os.MkdirAll(blobDir, 0o755); err != nil {
		return nil, err
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Engine{
		store:     st,
		dataDir:   abs,
		blobDir:   blobDir,
		failpoint: opts.Failpoint,
		now:       now,
	}, nil
}

func (e *Engine) Store() *store.Store { return e.store }

func (e *Engine) nowMS() int64 { return e.now().UTC().UnixMilli() }

func (e *Engine) nowText() string { return e.now().UTC().Format(time.RFC3339Nano) }

func (e *Engine) hitFailpoint(point string) error {
	if e.failpoint == nil {
		return nil
	}
	return e.failpoint(point)
}

type dbState struct {
	Revision             int64
	CurrentPolicyVersion int64
	LossScale            string
	LossScaleGrowth      string
}

type policyRecord struct {
	model.Snapshot
	CanonicalJSON string
}

func loadStateTx(ctx context.Context, tx *sql.Tx) (dbState, error) {
	var s dbState
	err := tx.QueryRowContext(ctx, `SELECT revision, current_policy_version, loss_scale, loss_scale_growth FROM engine_state WHERE id=1`).
		Scan(&s.Revision, &s.CurrentPolicyVersion, &s.LossScale, &s.LossScaleGrowth)
	return s, err
}

func loadStateDB(ctx context.Context, db *sql.DB) (dbState, error) {
	var s dbState
	err := db.QueryRowContext(ctx, `SELECT revision, current_policy_version, loss_scale, loss_scale_growth FROM engine_state WHERE id=1`).
		Scan(&s.Revision, &s.CurrentPolicyVersion, &s.LossScale, &s.LossScaleGrowth)
	return s, err
}

func loadPolicyTx(ctx context.Context, tx *sql.Tx, version int64) (*policyRecord, error) {
	var p policyRecord
	var vram, persisted []string
	err := tx.QueryRowContext(ctx, `SELECT version, generated_at, vram_limit_bytes,
		write_budget_bytes_per_round, time_budget_ms_per_round, write_queue_capacity,
		loss_scale_initial, loss_scale_growth, precision_abs_limit_nanos,
		precision_rel_limit_ppm, write_preference, checksum, canonical_json
		FROM strategy_snapshots WHERE version=?`, version).Scan(
		&p.Version, &p.GeneratedAt, &p.VRAMLimitBytes, &p.WriteBudgetBytesPerRound,
		&p.TimeBudgetMS, &p.WriteQueueCapacity, &p.LossScaleInitial, &p.LossScaleGrowth,
		&p.PrecisionAbsLimitNanos, &p.PrecisionRelLimitPPM, &p.WritePreference,
		&p.Checksum, &p.CanonicalJSON,
	)
	if err != nil {
		return nil, err
	}
	vram, err = queryStringList(ctx, tx, `SELECT shard_id FROM snapshot_residency WHERE policy_version=? AND target_pool='VRAM' ORDER BY shard_id`, version)
	if err != nil {
		return nil, err
	}
	persisted, err = queryStringList(ctx, tx, `SELECT shard_id FROM snapshot_residency WHERE policy_version=? AND target_pool='PERSISTED' ORDER BY shard_id`, version)
	if err != nil {
		return nil, err
	}
	p.VRAMResidency = vram
	p.PersistedResidency = persisted
	return &p, nil
}

func queryStringList(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]string, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func loadAllShards(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) ([]model.Shard, error) {
	rows, err := q.QueryContext(ctx, `SELECT round, shard_id, parameter_id, precision, bytes,
		digest, momentum_digest, restored_from_digest, location, desired_pool,
		write_state, last_access_ms, priority, write_ms, pinned, quantization,
		fp16_bits_json, fp32_nanos_json, payload_hex, revision, updated_at
		FROM shards ORDER BY round, shard_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanShards(rows)
}

func loadShardsTx(ctx context.Context, tx *sql.Tx, round int, pool string) ([]model.Shard, error) {
	query := `SELECT round, shard_id, parameter_id, precision, bytes,
		digest, momentum_digest, restored_from_digest, location, desired_pool,
		write_state, last_access_ms, priority, write_ms, pinned, quantization,
		fp16_bits_json, fp32_nanos_json, payload_hex, revision, updated_at
		FROM shards WHERE 1=1`
	var args []any
	if round > 0 {
		query += ` AND round=?`
		args = append(args, round)
	}
	if pool != "" {
		query += ` AND location=?`
		args = append(args, pool)
	}
	query += ` ORDER BY round, shard_id`
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanShards(rows)
}

func scanShards(rows *sql.Rows) ([]model.Shard, error) {
	var out []model.Shard
	for rows.Next() {
		var s model.Shard
		var bitsJSON, nanosJSON string
		var pinned int
		if err := rows.Scan(&s.Round, &s.ShardID, &s.ParameterID, &s.Precision, &s.Bytes,
			&s.Digest, &s.MomentumDigest, &s.RestoredFromDigest, &s.Location,
			&s.DesiredPool, &s.WriteState, &s.LastAccessMS, &s.Priority, &s.WriteMS,
			&pinned, &s.Quantization, &bitsJSON, &nanosJSON, &s.PayloadHex,
			&s.Revision, &s.UpdatedAt); err != nil {
			return nil, err
		}
		s.Pinned = pinned != 0
		if bitsJSON != "" {
			if err := json.Unmarshal([]byte(bitsJSON), &s.FP16Bits); err != nil {
				return nil, fmt.Errorf("decode fp16 bits for %d/%s: %w", s.Round, s.ShardID, err)
			}
		}
		if nanosJSON != "" {
			if err := json.Unmarshal([]byte(nanosJSON), &s.FP32Nanos); err != nil {
				return nil, fmt.Errorf("decode fp32 nanos for %d/%s: %w", s.Round, s.ShardID, err)
			}
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func shardKey(round int, shardID string) string {
	return strconv.Itoa(round) + "\x00" + shardID
}

func shardMap(shards []model.Shard) map[string]model.Shard {
	m := make(map[string]model.Shard, len(shards))
	for _, s := range shards {
		m[shardKey(s.Round, s.ShardID)] = s
	}
	return m
}

func sortedShards(shards []model.Shard) []model.Shard {
	out := append([]model.Shard(nil), shards...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Round != out[j].Round {
			return out[i].Round < out[j].Round
		}
		return out[i].ShardID < out[j].ShardID
	})
	return out
}

func isNoRows(err error) bool { return errors.Is(err, sql.ErrNoRows) }
