package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"

	"checkpointengine/internal/model"
)

type precisionPair struct {
	FP16 model.Shard
	FP32 model.Shard
}

func buildPrecisionPairs(shards []model.Shard, round int) ([]precisionPair, error) {
	fp16ByParamDigest := make(map[string][]model.Shard)
	fp16ByParam := make(map[string][]model.Shard)
	var fp32s []model.Shard
	for _, s := range shards {
		if round > 0 && s.Round != round {
			continue
		}
		key := fmt.Sprintf("%d\x00%s", s.Round, s.ParameterID)
		if s.Precision == model.PrecisionFP16 {
			fp16ByParam[key] = append(fp16ByParam[key], s)
			fp16ByParamDigest[key+"\x00"+s.Digest] = append(fp16ByParamDigest[key+"\x00"+s.Digest], s)
		} else if s.Precision == model.PrecisionFP32 {
			fp32s = append(fp32s, s)
		}
	}
	sort.Slice(fp32s, func(i, j int) bool {
		if fp32s[i].Round != fp32s[j].Round {
			return fp32s[i].Round < fp32s[j].Round
		}
		return fp32s[i].ShardID < fp32s[j].ShardID
	})
	var pairs []precisionPair
	for _, master := range fp32s {
		key := fmt.Sprintf("%d\x00%s", master.Round, master.ParameterID)
		ref := master.RestoredFromDigest
		var candidates []model.Shard
		if ref != "" {
			candidates = fp16ByParamDigest[key+"\x00"+ref]
			if len(candidates) == 0 {
				return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "FP32 master references a missing FP16 digest"),
					map[string]any{"round": master.Round, "shard_id": master.ShardID, "restored_from_digest": ref})
			}
		} else {
			candidates = fp16ByParam[key]
			if len(candidates) > 1 {
				return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "ambiguous FP16 pair"),
					map[string]any{"round": master.Round, "shard_id": master.ShardID})
			}
		}
		if len(candidates) == 1 {
			pairs = append(pairs, precisionPair{FP16: candidates[0], FP32: master})
		}
	}
	return pairs, nil
}

func computePrecisionResult(pair precisionPair, lossScale string, policy model.Snapshot, now string) (model.PrecisionResult, error) {
	ls, err := model.ParsePositiveRat(lossScale)
	if err != nil {
		return model.PrecisionResult{}, model.WrapAPIError(model.CodePrecisionDrift, "invalid loss scale", err)
	}
	result := model.PrecisionResult{
		Round:         pair.FP32.Round,
		ShardID:       pair.FP32.ShardID,
		ParameterID:   pair.FP32.ParameterID,
		FP16ShardID:   pair.FP16.ShardID,
		FP32ShardID:   pair.FP32.ShardID,
		FP16Digest:    pair.FP16.Digest,
		FP32Digest:    pair.FP32.Digest,
		LossScale:     lossScale,
		RoundingMode:  model.RoundingRNE,
		AbsLimitNanos: policy.PrecisionAbsLimitNanos,
		RelLimitPPM:   policy.PrecisionRelLimitPPM,
		CheckedAt:     now,
	}
	if pair.FP32.RestoredFromDigest != pair.FP16.Digest {
		return result, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "FP32 restoration relation references a different FP16 digest"),
			map[string]any{"round": pair.FP32.Round, "shard_id": pair.FP32.ShardID,
				"restored_from_digest": pair.FP32.RestoredFromDigest, "fp16_digest": pair.FP16.Digest})
	}
	if len(pair.FP16.FP16Bits) == 0 || len(pair.FP32.FP32Nanos) == 0 {
		result.CheckMode = "DIGEST_ONLY"
		result.Passed = true
		result.DriftReason = "numeric payload not supplied; digest restoration relation checked only"
		result.ResultDigest = precisionResultDigest(result)
		return result, nil
	}
	if len(pair.FP16.FP16Bits) != len(pair.FP32.FP32Nanos) {
		return result, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "FP16 and FP32 element counts differ"),
			map[string]any{"round": pair.FP32.Round, "shard_id": pair.FP32.ShardID,
				"fp16_elements": len(pair.FP16.FP16Bits), "fp32_elements": len(pair.FP32.FP32Nanos)})
	}
	result.CheckMode = "NUMERIC_RNE_FIXED_NANOS"
	result.Elements = len(pair.FP16.FP16Bits)
	result.Details = make([]model.PrecisionElement, 0, result.Elements)
	exactSum := new(big.Rat)
	var restoredSum int64
	for i, bits := range pair.FP16.FP16Bits {
		exactHalf, err := halfBitsToRat(bits)
		if err != nil {
			return result, model.WithDetails(model.WrapAPIError(model.CodeShardInvalid, "invalid FP16 bit pattern", err),
				map[string]any{"round": pair.FP32.Round, "shard_id": pair.FP16.ShardID, "index": i, "fp16_bits": bits})
		}
		exactRestored := new(big.Rat).Quo(exactHalf, ls)
		restored := model.RoundRatToNanos(exactRestored)
		master := pair.FP32.FP32Nanos[i]
		signed := saturatingSub(restored, master)
		absErr := model.AbsInt64(signed)
		rel := relativePPM(absErr, model.AbsInt64(master))
		if rel > result.MaxRelativePPM {
			result.MaxRelativePPM = rel
		}
		if absErr > result.MaxAbsDeviationNanos {
			result.MaxAbsDeviationNanos = absErr
		}
		result.SumAbsDeviationNanos = saturatingAdd(result.SumAbsDeviationNanos, absErr)
		exactSum.Add(exactSum, exactRestored)
		restoredSum = saturatingAdd(restoredSum, restored)
		result.Details = append(result.Details, model.PrecisionElement{
			Index: i, FP16Bits: bits, FP32Nanos: master, RestoredNanos: restored,
			SignedErrorNanos: signed, AbsErrorNanos: absErr, RelativePPM: rel,
		})
	}
	roundedExactSum := model.RoundRatToNanos(exactSum)
	result.CumulativeRoundingNanos = saturatingSub(roundedExactSum, restoredSum)
	result.Passed = true
	var reasons []string
	if result.MaxAbsDeviationNanos > policy.PrecisionAbsLimitNanos {
		result.Passed = false
		reasons = append(reasons, fmt.Sprintf("max_abs_deviation_nanos=%d exceeds abs_limit_nanos=%d", result.MaxAbsDeviationNanos, policy.PrecisionAbsLimitNanos))
	}
	if result.MaxRelativePPM > policy.PrecisionRelLimitPPM {
		result.Passed = false
		reasons = append(reasons, fmt.Sprintf("max_relative_ppm=%d exceeds rel_limit_ppm=%d", result.MaxRelativePPM, policy.PrecisionRelLimitPPM))
	}
	result.DriftReason = strings.Join(reasons, "; ")
	result.ResultDigest = precisionResultDigest(result)
	return result, nil
}

func precisionResultDigest(result model.PrecisionResult) string {
	copyResult := result
	copyResult.CheckedAt = ""
	copyResult.ResultDigest = ""
	b, err := json.Marshal(copyResult)
	if err != nil {
		return ""
	}
	return model.NewDigest("PRECISION_RESULT_V1", string(b))
}

func halfBitsToRat(bits uint16) (*big.Rat, error) {
	sign := bits >> 15
	exp := int((bits >> 10) & 0x1f)
	frac := int64(bits & 0x03ff)
	if exp == 0x1f {
		return nil, fmt.Errorf("NaN and infinity are not valid checkpoint values")
	}
	var r *big.Rat
	if exp == 0 {
		if frac == 0 {
			return new(big.Rat), nil
		}
		r = new(big.Rat).SetFrac(big.NewInt(frac), new(big.Int).Lsh(big.NewInt(1), 24))
	} else {
		mantissa := int64(1024) + frac
		shift := exp - 25
		r = new(big.Rat).SetInt64(mantissa)
		if shift >= 0 {
			r.Mul(r, new(big.Rat).SetInt(new(big.Int).Lsh(big.NewInt(1), uint(shift))))
		} else {
			r.Quo(r, new(big.Rat).SetInt(new(big.Int).Lsh(big.NewInt(1), uint(-shift))))
		}
	}
	if sign == 1 {
		r.Neg(r)
	}
	return r, nil
}

func relativePPM(absErr, absMaster int64) int64 {
	den := absMaster
	if den < 1 {
		den = 1
	}
	r := new(big.Rat).SetFrac(
		new(big.Int).Mul(big.NewInt(absErr), big.NewInt(1_000_000)),
		big.NewInt(den),
	)
	return roundRatToInt(r)
}

func roundRatToInt(r *big.Rat) int64 {
	if r == nil || r.Sign() == 0 {
		return 0
	}
	q, rem := new(big.Int), new(big.Int)
	q.QuoRem(r.Num(), r.Denom(), rem)
	if rem.Sign() == 0 {
		if q.IsInt64() {
			return q.Int64()
		}
		return 1<<63 - 1
	}
	absRem := new(big.Int).Abs(rem)
	twice := new(big.Int).Lsh(absRem, 1)
	cmp := twice.Cmp(new(big.Int).Abs(r.Denom()))
	if cmp > 0 || (cmp == 0 && q.Bit(0) == 1) {
		if r.Sign() < 0 {
			q.Sub(q, big.NewInt(1))
		} else {
			q.Add(q, big.NewInt(1))
		}
	}
	if q.IsInt64() {
		return q.Int64()
	}
	if q.Sign() < 0 {
		return -1 << 63
	}
	return 1<<63 - 1
}

func saturatingAdd(a, b int64) int64 {
	c := a + b
	if b > 0 && c < a {
		return 1<<63 - 1
	}
	if b < 0 && c > a {
		return -1 << 63
	}
	return c
}

func saturatingSub(a, b int64) int64 {
	if b == -1<<63 {
		return saturatingAdd(a, 1<<63-1)
	}
	return saturatingAdd(a, -b)
}

func makePrecisionReport(round int, lossScale string, results []model.PrecisionResult) model.PrecisionReport {
	report := model.PrecisionReport{Round: round, LossScale: lossScale, RoundingMode: model.RoundingRNE, Results: results, Passed: true}
	var summaryParts []string
	for _, r := range results {
		if r.CheckMode == "NUMERIC_RNE_FIXED_NANOS" {
			report.VerifiedPairs++
		} else {
			report.DigestOnlyPairs++
		}
		if !r.Passed {
			report.FailedPairs++
			report.Passed = false
		}
		if r.MaxAbsDeviationNanos > report.MaxAbsDeviationNanos {
			report.MaxAbsDeviationNanos = r.MaxAbsDeviationNanos
		}
		if r.MaxRelativePPM > report.MaxRelativePPM {
			report.MaxRelativePPM = r.MaxRelativePPM
		}
		report.CumulativeAbsDeviation = saturatingAdd(report.CumulativeAbsDeviation, r.SumAbsDeviationNanos)
		report.CumulativeRoundingNanos = saturatingAdd(report.CumulativeRoundingNanos, r.CumulativeRoundingNanos)
		summaryParts = append(summaryParts, strconv.Itoa(r.Round), r.ShardID, r.FP16Digest, r.FP32Digest, r.LossScale, r.ResultDigest)
	}
	report.ResultDigest = model.NewDigest(append([]string{"PRECISION_REPORT_V1"}, summaryParts...)...)
	return report
}

func savePrecisionResultsTx(ctx context.Context, tx *sql.Tx, results []model.PrecisionResult) ([]model.PrecisionResult, error) {
	out := make([]model.PrecisionResult, 0, len(results))
	for _, result := range results {
		var previousDigest, previousChecked string
		err := tx.QueryRowContext(ctx, `SELECT result_digest, checked_at FROM precision_results WHERE round=? AND shard_id=?`,
			result.Round, result.ShardID).Scan(&previousDigest, &previousChecked)
		if err != nil && !isNoRows(err) {
			return nil, err
		}
		if err == nil && previousDigest == result.ResultDigest {
			result.CheckedAt = previousChecked
		}
		details, err := json.Marshal(result.Details)
		if err != nil {
			return nil, err
		}
		passed := 0
		if result.Passed {
			passed = 1
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO precision_results(
			round, shard_id, parameter_id, fp16_shard_id, fp32_shard_id,
			fp16_digest, fp32_digest, loss_scale, rounding_mode, elements,
			max_abs_nanos, max_rel_ppm, sum_abs_nanos, cumulative_rounding_nanos,
			abs_limit_nanos, rel_limit_ppm, check_mode, passed, drift_reason,
			checked_at, result_digest, details_json)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(round, shard_id) DO UPDATE SET
			parameter_id=excluded.parameter_id, fp16_shard_id=excluded.fp16_shard_id,
			fp32_shard_id=excluded.fp32_shard_id, fp16_digest=excluded.fp16_digest,
			fp32_digest=excluded.fp32_digest, loss_scale=excluded.loss_scale,
			rounding_mode=excluded.rounding_mode, elements=excluded.elements,
			max_abs_nanos=excluded.max_abs_nanos, max_rel_ppm=excluded.max_rel_ppm,
			sum_abs_nanos=excluded.sum_abs_nanos, cumulative_rounding_nanos=excluded.cumulative_rounding_nanos,
			abs_limit_nanos=excluded.abs_limit_nanos, rel_limit_ppm=excluded.rel_limit_ppm,
			check_mode=excluded.check_mode, passed=excluded.passed, drift_reason=excluded.drift_reason,
			checked_at=excluded.checked_at, result_digest=excluded.result_digest, details_json=excluded.details_json`,
			result.Round, result.ShardID, result.ParameterID, result.FP16ShardID, result.FP32ShardID,
			result.FP16Digest, result.FP32Digest, result.LossScale, result.RoundingMode, result.Elements,
			result.MaxAbsDeviationNanos, result.MaxRelativePPM, result.SumAbsDeviationNanos,
			result.CumulativeRoundingNanos, result.AbsLimitNanos, result.RelLimitPPM,
			result.CheckMode, passed, result.DriftReason, result.CheckedAt, result.ResultDigest, string(details)); err != nil {
			return nil, err
		}
		out = append(out, result)
	}
	return out, nil
}

func loadPrecisionResultsTx(ctx context.Context, tx *sql.Tx, round int) ([]model.PrecisionResult, error) {
	query := `SELECT round, shard_id, parameter_id, fp16_shard_id, fp32_shard_id,
		fp16_digest, fp32_digest, loss_scale, rounding_mode, elements,
		max_abs_nanos, max_rel_ppm, sum_abs_nanos, cumulative_rounding_nanos,
		abs_limit_nanos, rel_limit_ppm, check_mode, passed, drift_reason,
		checked_at, result_digest, details_json FROM precision_results`
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
	var out []model.PrecisionResult
	for rows.Next() {
		var r model.PrecisionResult
		var passed int
		var details string
		if err := rows.Scan(&r.Round, &r.ShardID, &r.ParameterID, &r.FP16ShardID, &r.FP32ShardID,
			&r.FP16Digest, &r.FP32Digest, &r.LossScale, &r.RoundingMode, &r.Elements,
			&r.MaxAbsDeviationNanos, &r.MaxRelativePPM, &r.SumAbsDeviationNanos,
			&r.CumulativeRoundingNanos, &r.AbsLimitNanos, &r.RelLimitPPM, &r.CheckMode,
			&passed, &r.DriftReason, &r.CheckedAt, &r.ResultDigest, &details); err != nil {
			return nil, err
		}
		r.Passed = passed != 0
		if details != "" && details != "null" {
			if err := json.Unmarshal([]byte(details), &r.Details); err != nil {
				return nil, err
			}
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
