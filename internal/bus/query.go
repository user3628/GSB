package bus

import (
	"context"
	"database/sql"
	"encoding/json"
	"math"
	"strings"

	"local/eventbus/internal/agg"
)

func (s *Service) GetAggregate(ctx context.Context, topic, group string, partition int) (AggregateResult, error) {
	topic = strings.TrimSpace(topic)
	group = strings.TrimSpace(group)
	if err := s.validateTopicGroup(ctx, s.db, topic, group); err != nil {
		return AggregateResult{}, err
	}
	if err := s.validateTopicPartition(ctx, s.db, topic, partition); err != nil {
		return AggregateResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return AggregateResult{}, WrapError(CodeInternal, "begin aggregate read snapshot", err)
	}
	defer tx.Rollback()
	return s.aggregateInTx(ctx, tx, topic, group, partition)
}

func (s *Service) aggregateInTx(ctx context.Context, tx *sql.Tx, topic, group string, partition int) (AggregateResult, error) {
	algorithmName, err := s.groupAlgorithm(ctx, tx, topic, group)
	if err != nil {
		return AggregateResult{}, err
	}
	algorithm, err := algorithmFromString(algorithmName)
	if err != nil {
		return AggregateResult{}, err
	}
	offset, err := s.loadOffset(ctx, tx, topic, group, partition)
	if err != nil {
		return AggregateResult{}, err
	}
	groupState, err := s.loadAggregateGroup(ctx, tx, topic, group, partition, algorithm)
	if err != nil {
		return AggregateResult{}, err
	}
	return aggregateResultFromGroup(topic, group, partition, offset.AckedOffset, offset.Version, offset.ReplayActive, &groupState), nil
}

func (s *Service) GetErrorLedger(ctx context.Context, topic, group string, partition int) (ErrorLedger, error) {
	topic = strings.TrimSpace(topic)
	group = strings.TrimSpace(group)
	if err := s.validateTopicGroup(ctx, s.db, topic, group); err != nil {
		return ErrorLedger{}, err
	}
	if err := s.validateTopicPartition(ctx, s.db, topic, partition); err != nil {
		return ErrorLedger{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ErrorLedger{}, WrapError(CodeInternal, "begin error-ledger read snapshot", err)
	}
	defer tx.Rollback()
	cumulative, err := s.aggregateInTx(ctx, tx, topic, group, partition)
	if err != nil {
		return ErrorLedger{}, err
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT id, ack_batch_id, event_batch_ids_json, start_seq, end_seq, algorithm, event_count,
			partials_json, sum_abs, abs_error_bound, relative_error_bound, state_digest,
			previous_offset, new_offset, version, active, created_at_ms, reverted_at_ms
		FROM agg_batches WHERE topic = ? AND group_name = ? AND partition = ? ORDER BY id`,
		topic, group, partition)
	if err != nil {
		return ErrorLedger{}, WrapError(CodeInternal, "read aggregate batches", err)
	}
	defer rows.Close()
	ledger := ErrorLedger{
		Topic: topic, Group: group, Partition: partition,
		Algorithm: cumulative.Algorithm, Offset: cumulative.Offset,
		Cumulative: cumulative,
	}
	var activeBound, activeSumAbs float64
	var activeSum float64
	for rows.Next() {
		var batch AggregateBatch
		var eventBatchIDsJSON, partialsJSON, algorithmName string
		var active int
		var revertedAt sql.NullInt64
		if err := rows.Scan(&batch.ID, &batch.AckBatchID, &eventBatchIDsJSON, &batch.StartSeq,
			&batch.EndSeq, &algorithmName, &batch.EventCount, &partialsJSON, &batch.SumAbs,
			&batch.AbsErrorBound, &batch.RelativeErrorBound, &batch.StateDigest,
			&batch.PreviousOffset, &batch.NewOffset, &batch.Version, &active,
			&batch.CreatedAtMS, &revertedAt); err != nil {
			return ErrorLedger{}, WrapError(CodeInternal, "scan aggregate batch", err)
		}
		batch.Algorithm = agg.Algorithm(algorithmName)
		if err := json.Unmarshal([]byte(eventBatchIDsJSON), &batch.EventBatchIDs); err != nil {
			return ErrorLedger{}, WrapError(CodeInternal, "decode event batch ids", err)
		}
		if err := json.Unmarshal([]byte(partialsJSON), &batch.Partials); err != nil {
			return ErrorLedger{}, WrapError(CodeInternal, "decode batch partials", err)
		}
		batch.Active = active != 0
		if revertedAt.Valid {
			value := revertedAt.Int64
			batch.RevertedAtMS = &value
		}
		if batch.Active {
			activeBound += batch.AbsErrorBound
			activeSumAbs += batch.SumAbs
			for _, key := range sortedKeys(batch.Partials) {
				activeSum += batch.Partials[key].Sum
			}
			ledger.BatchCount++
		} else {
			ledger.RevertedBatchCount++
		}
		ledger.Batches = append(ledger.Batches, batch)
	}
	if err := rows.Err(); err != nil {
		return ErrorLedger{}, WrapError(CodeInternal, "iterate aggregate batches", err)
	}
	// The per-batch ledger is conservatively composable: errors add, and a final
	// merge of B batch partials can add at most gamma(B) to the L1 magnitude.
	batchCount := float64(ledger.BatchCount)
	if batchCount > 1 {
		nu := batchCount * agg.UnitRoundoff
		if nu < 1 {
			activeBound += (nu / (1 - nu)) * activeSumAbs
		} else {
			activeBound = math.MaxFloat64
		}
	}
	ledger.LedgerAbsBound = activeBound
	if activeSum != 0 {
		ledger.LedgerRelBound = activeBound / math.Abs(activeSum)
	} else if activeBound != 0 {
		ledger.LedgerRelBound = math.MaxFloat64
	}
	if math.IsInf(ledger.LedgerRelBound, 0) || math.IsNaN(ledger.LedgerRelBound) {
		ledger.LedgerRelBound = math.MaxFloat64
	}
	return ledger, nil
}
