package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"checkpointengine/internal/model"
)

type durableFact struct {
	Round    int
	ShardID  string
	Digest   string
	BlobPath string
}

func loadDurableFactsTx(ctx context.Context, tx *sql.Tx) (map[string]durableFact, error) {
	rows, err := tx.QueryContext(ctx, `SELECT round, shard_id, digest, blob_path FROM write_records`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]durableFact)
	for rows.Next() {
		var f durableFact
		if err := rows.Scan(&f.Round, &f.ShardID, &f.Digest, &f.BlobPath); err != nil {
			return nil, err
		}
		out[shardKey(f.Round, f.ShardID)] = f
	}
	return out, rows.Err()
}

func rebuildAllDerivedTx(ctx context.Context, tx *sql.Tx, policy model.Snapshot, revision int64, now string) ([]model.RoundSummary, error) {
	shards, err := loadAllShards(ctx, tx)
	if err != nil {
		return nil, err
	}
	results, err := loadPrecisionResultsTx(ctx, tx, 0)
	if err != nil {
		return nil, err
	}
	facts, err := loadDurableFactsTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM shard_derivatives`); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM round_consistency`); err != nil {
		return nil, err
	}

	resultByShard := make(map[string]model.PrecisionResult)
	failedRounds := make(map[int]int)
	unverifiedRounds := make(map[int]int)
	for _, r := range results {
		resultByShard[shardKey(r.Round, r.FP16ShardID)] = r
		resultByShard[shardKey(r.Round, r.FP32ShardID)] = r
		if !r.Passed {
			failedRounds[r.Round]++
		}
		if r.CheckMode != "NUMERIC_RNE_FIXED_NANOS" {
			unverifiedRounds[r.Round]++
		}
	}
	pairs, pairErr := buildPrecisionPairs(shards, 0)
	if pairErr != nil {
		return nil, pairErr
	}
	pairedFP16Master := make(map[string]string)
	for _, p := range pairs {
		pairedFP16Master[shardKey(p.FP16.Round, p.FP16.ShardID)] = p.FP32.Digest
		if _, ok := resultByShard[shardKey(p.FP32.Round, p.FP32.ShardID)]; !ok {
			unverifiedRounds[p.FP32.Round]++
		}
	}

	rounds := make(map[int][]model.Shard)
	for _, s := range shards {
		rounds[s.Round] = append(rounds[s.Round], s)
		key := shardKey(s.Round, s.ShardID)
		fact, durable := facts[key]
		if durable && fact.Digest != s.Digest {
			durable = false
		}
		masterDigest := s.Digest
		if s.Precision == model.PrecisionFP16 {
			if md := pairedFP16Master[key]; md != "" {
				masterDigest = md
			} else if s.RestoredFromDigest != "" {
				masterDigest = s.RestoredFromDigest
			}
		}
		consistencyStatus := "UNVERIFIED"
		consistencyDigest := ""
		if r, ok := resultByShard[key]; ok {
			consistencyDigest = r.ResultDigest
			if !r.Passed {
				consistencyStatus = "FAIL"
			} else if r.CheckMode == "NUMERIC_RNE_FIXED_NANOS" {
				consistencyStatus = "PASS"
			} else {
				consistencyStatus = "DIGEST_ONLY"
			}
		}
		baseline := ""
		if durable {
			baseline = s.Digest
		}
		vramEstimate := int64(0)
		if s.Location == model.PoolVRAM {
			vramEstimate = s.Bytes
		}
		fingerprint := shardFingerprint(policy, s, durable)
		if _, err := tx.ExecContext(ctx, `INSERT INTO shard_derivatives(
			round, shard_id, master_digest, consistency_status, consistency_digest,
			vram_estimate_bytes, recovery_baseline, source_fingerprint, rebuilt_at)
			VALUES(?,?,?,?,?,?,?,?,?)`, s.Round, s.ShardID, masterDigest, consistencyStatus,
			consistencyDigest, vramEstimate, baseline, fingerprint, now); err != nil {
			return nil, err
		}
	}

	var summaries []model.RoundSummary
	var roundNumbers []int
	for round := range rounds {
		roundNumbers = append(roundNumbers, round)
	}
	sort.Ints(roundNumbers)
	for _, round := range roundNumbers {
		items := rounds[round]
		summary := model.RoundSummary{
			Round: round, ShardCount: len(items), FailedPrecision: failedRounds[round],
			UnverifiedPairs: unverifiedRounds[round], DecidedRevision: revision, RebuiltAt: now,
		}
		var fingerprints []string
		for _, s := range items {
			key := shardKey(s.Round, s.ShardID)
			fact, durable := facts[key]
			durable = durable && fact.Digest == s.Digest && s.WriteState == model.WriteWritten
			if durable {
				summary.WrittenShards++
			} else {
				summary.PendingShards++
			}
			fingerprints = append(fingerprints, shardFingerprint(policy, s, durable))
		}
		sort.Strings(fingerprints)
		summary.SourceFingerprint = model.NewDigest(append([]string{"ROUND_CONSISTENCY_V1", strconv.Itoa(round)}, fingerprints...)...)
		switch {
		case summary.FailedPrecision > 0:
			summary.Status = "FAIL"
		case summary.PendingShards > 0:
			summary.Status = "INCOMPLETE"
		case summary.UnverifiedPairs > 0:
			summary.Status = "UNVERIFIED"
		default:
			summary.Status = "PASS"
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO round_consistency(
			round, status, shard_count, written_shards, pending_shards, failed_precision,
			unverified_pairs, source_fingerprint, decided_revision, rebuilt_at)
			VALUES(?,?,?,?,?,?,?,?,?,?)`, summary.Round, summary.Status, summary.ShardCount,
			summary.WrittenShards, summary.PendingShards, summary.FailedPrecision,
			summary.UnverifiedPairs, summary.SourceFingerprint, summary.DecidedRevision, summary.RebuiltAt); err != nil {
			return nil, err
		}
		summaries = append(summaries, summary)
	}
	return summaries, nil
}

func shardFingerprint(policy model.Snapshot, s model.Shard, durable bool) string {
	d := "0"
	if durable {
		d = "1"
	}
	return model.NewDigest("SHARD_DERIVATION_V1", strconv.Itoa(s.Round), s.ShardID, s.ParameterID,
		s.Precision, strconv.FormatInt(s.Bytes, 10), s.Digest, s.MomentumDigest,
		s.RestoredFromDigest, s.Location, s.DesiredPool, s.WriteState, s.Quantization,
		policy.LossScaleInitial, policy.LossScaleGrowth, d)
}

func invalidateDerivedForShardsTx(ctx context.Context, tx *sql.Tx, keys []string) error {
	for _, key := range keys {
		parts := strings.SplitN(key, "\x00", 2)
		if len(parts) != 2 {
			continue
		}
		round, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM shard_derivatives WHERE round=? AND shard_id=?`, round, parts[1]); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM precision_results WHERE round=? AND (shard_id=? OR fp16_shard_id=? OR fp32_shard_id=?)`,
			round, parts[1], parts[1], parts[1]); err != nil {
			return err
		}
	}
	return nil
}

func loadRoundSummariesTx(ctx context.Context, tx *sql.Tx) ([]model.RoundSummary, error) {
	rows, err := tx.QueryContext(ctx, `SELECT round, status, shard_count, written_shards,
		pending_shards, failed_precision, unverified_pairs, source_fingerprint,
		decided_revision, rebuilt_at FROM round_consistency ORDER BY round`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.RoundSummary
	for rows.Next() {
		var s model.RoundSummary
		if err := rows.Scan(&s.Round, &s.Status, &s.ShardCount, &s.WrittenShards,
			&s.PendingShards, &s.FailedPrecision, &s.UnverifiedPairs,
			&s.SourceFingerprint, &s.DecidedRevision, &s.RebuiltAt); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func loadDerivativesTx(ctx context.Context, tx *sql.Tx, round int) ([]model.Derivative, error) {
	query := `SELECT round, shard_id, master_digest, consistency_status, consistency_digest,
		vram_estimate_bytes, recovery_baseline, source_fingerprint, rebuilt_at FROM shard_derivatives`
	var args []any
	if round > 0 {
		query += ` WHERE round=?`
		args = append(args, round)
	}
	query += ` ORDER BY round, shard_id`
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Derivative
	for rows.Next() {
		var d model.Derivative
		if err := rows.Scan(&d.Round, &d.ShardID, &d.MasterDigest, &d.ConsistencyStatus,
			&d.ConsistencyDigest, &d.VRAMEstimateBytes, &d.RecoveryBaseline,
			&d.SourceFingerprint, &d.RebuiltAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func encodeDetails(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf(`{"marshal_error":%q}`, err.Error())
	}
	return string(b)
}
