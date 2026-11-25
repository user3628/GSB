package bus

import (
	"context"
	"database/sql"
	"encoding/json"
	"math"

	"local/eventbus/internal/agg"
)

func (s *Service) loadAggregateGroup(
	ctx context.Context,
	q queryer,
	topic, group string,
	partition int,
	algorithm agg.Algorithm,
) (agg.Group, error) {
	result := agg.NewGroup(algorithm)
	rows, err := q.QueryContext(ctx, `
		SELECT event_key, event_count, sum_value, compensation, sum_abs, buckets_json
		FROM agg_states WHERE topic = ? AND group_name = ? AND partition = ?`,
		topic, group, partition)
	if err != nil {
		return result, WrapError(CodeInternal, "read aggregate state", err)
	}
	defer rows.Close()
	for rows.Next() {
		var key, buckets string
		var state agg.State
		if err := rows.Scan(&key, &state.Count, &state.Sum, &state.Compensation, &state.SumAbs, &buckets); err != nil {
			return result, WrapError(CodeInternal, "scan aggregate state", err)
		}
		if buckets != "" && buckets != "null" {
			if err := json.Unmarshal([]byte(buckets), &state.Buckets); err != nil {
				return result, WrapError(CodeInternal, "decode aggregate buckets", err)
			}
		}
		result.Keys[key] = state
		result.Bounds[key] = state.AbsErrorBound(algorithm)
	}
	if err := rows.Err(); err != nil {
		return result, WrapError(CodeInternal, "iterate aggregate state", err)
	}
	return result, nil
}

func (s *Service) persistAggregateGroup(
	ctx context.Context,
	tx *sql.Tx,
	topic, group string,
	partition int,
	groupState *agg.Group,
) error {
	groupState.Finalize()
	for key, state := range groupState.Keys {
		buckets, err := json.Marshal(state.Buckets)
		if err != nil {
			return WrapError(CodeInternal, "encode magnitude buckets", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO agg_states(topic, group_name, partition, event_key, event_count, sum_value,
				compensation, sum_abs, buckets_json, abs_error_bound)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(topic, group_name, partition, event_key) DO UPDATE SET
				event_count = excluded.event_count,
				sum_value = excluded.sum_value,
				compensation = excluded.compensation,
				sum_abs = excluded.sum_abs,
				buckets_json = excluded.buckets_json,
				abs_error_bound = excluded.abs_error_bound`,
			topic, group, partition, key, state.Count, state.Sum, state.Compensation,
			state.SumAbs, string(buckets), groupState.Bounds[key]); err != nil {
			return WrapError(CodeInternal, "persist aggregate state", err)
		}
	}
	return nil
}

func (s *Service) saveAggregateSnapshot(
	ctx context.Context,
	tx *sql.Tx,
	topic, group string,
	partition int,
	offset, version int64,
	groupState *agg.Group,
	batchID int64,
) (string, error) {
	groupState.Finalize()
	digest := groupState.Digest()
	stateJSON, err := json.Marshal(groupState)
	if err != nil {
		return "", WrapError(CodeInternal, "encode aggregate snapshot", err)
	}
	errorsJSON, err := json.Marshal(groupState.Bounds)
	if err != nil {
		return "", WrapError(CodeInternal, "encode aggregate error snapshot", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agg_snapshots(topic, group_name, partition, offset_value, version, algorithm,
			state_json, errors_json, state_digest, batch_id, active, created_at_ms)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)`,
		topic, group, partition, offset, version, groupState.Algorithm,
		string(stateJSON), string(errorsJSON), digest, batchID, s.nowMS()); err != nil {
		return "", WrapError(CodeInternal, "persist aggregate snapshot", err)
	}
	return digest, nil
}

func batchPartials(algorithm agg.Algorithm, events []storedEvent) (map[string]KeyPartial, float64, float64, error) {
	group := agg.NewGroup(algorithm)
	for _, event := range events {
		group.Add(event.AggregateKey, event.Metric)
	}
	group.Finalize()
	partials := make(map[string]KeyPartial, len(group.Keys))
	var sumAbs, totalBound float64
	for _, key := range sortedKeys(group.Keys) {
		state := group.Keys[key]
		partial := KeyPartial{
			Count:              state.Count,
			Sum:                state.Value(algorithm),
			SumAbs:             state.SumAbs,
			AbsErrorBound:      state.AbsErrorBound(algorithm),
			RelativeErrorBound: state.RelativeErrorBound(algorithm),
		}
		partials[key] = partial
		if err := checkFinite("batch partial sum "+key, partial.Sum); err != nil {
			return nil, 0, 0, err
		}
		if err := checkFinite("batch sum_abs "+key, partial.SumAbs); err != nil {
			return nil, 0, 0, err
		}
		if err := checkFinite("batch error bound "+key, partial.AbsErrorBound); err != nil {
			return nil, 0, 0, err
		}
		sumAbs += partial.SumAbs
		totalBound += partial.AbsErrorBound
	}
	return partials, sumAbs, totalBound, nil
}

func relativeBound(absBound float64, values ...float64) float64 {
	if absBound == 0 {
		return 0
	}
	var magnitude float64
	for _, value := range values {
		magnitude += value
	}
	if magnitude == 0 {
		return math.MaxFloat64
	}
	result := absBound / math.Abs(magnitude)
	if math.IsInf(result, 0) || math.IsNaN(result) {
		return math.MaxFloat64
	}
	return result
}

func aggregateResultFromGroup(
	topic, group string,
	partition int,
	offset, version int64,
	replayActive bool,
	groupState *agg.Group,
) AggregateResult {
	groupState.Finalize()
	result := AggregateResult{
		Topic:        topic,
		Group:        group,
		Partition:    partition,
		Algorithm:    groupState.Algorithm,
		Offset:       offset,
		Version:      version,
		ReplayActive: replayActive,
		Keys:         make(map[string]AggregateKeyResult, len(groupState.Keys)),
	}
	var totalSum float64
	for _, key := range sortedKeys(groupState.Keys) {
		state := groupState.Keys[key]
		value := state.Value(groupState.Algorithm)
		bound := state.AbsErrorBound(groupState.Algorithm)
		result.Keys[key] = AggregateKeyResult{
			Count:              state.Count,
			Sum:                value,
			SumAbs:             state.SumAbs,
			AbsErrorBound:      bound,
			RelativeErrorBound: state.RelativeErrorBound(groupState.Algorithm),
		}
		result.EventCount += state.Count
		result.SumAbs += state.SumAbs
		result.AbsErrorBound += bound
		totalSum += value
	}
	result.RelativeBound = relativeBound(result.AbsErrorBound, totalSum)
	result.RelativeTolerance = result.RelativeBound
	result.StateDigest = groupState.Digest()
	return result
}

func algorithmFromString(value string) (agg.Algorithm, error) {
	algorithm, err := agg.ParseAlgorithm(value)
	if err != nil {
		return "", invalidArgument("%s", err.Error())
	}
	return algorithm, nil
}

func checkFinite(label string, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return invalidArgument("non-finite aggregate value for %s: %v", label, value)
	}
	return nil
}

func validateAggregateGroup(groupState *agg.Group) error {
	for _, key := range sortedKeys(groupState.Keys) {
		state := groupState.Keys[key]
		values := []float64{state.Sum, state.SumAbs, state.Compensation, state.Value(groupState.Algorithm)}
		for _, bucket := range state.Buckets {
			values = append(values, bucket.Sum, bucket.SumAbs, bucket.Compensation)
		}
		for _, value := range values {
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return invalidArgument("non-finite aggregate value for key %s", key)
			}
		}
	}
	return nil
}
