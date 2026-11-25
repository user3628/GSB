package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"

	"spmmopt/spmm"
)

type BlockExpectation struct {
	PlanKey string
	Version int64
	Digest  string
}

func (s *Store) PublishBlock(ctx context.Context, result *spmm.BlockResult, expected BlockExpectation) (*BlockRecord, error) {
	if result == nil {
		return nil, spmm.NewError(spmm.CodeInput, "block result is required")
	}
	ref := PlanRef{PlanKey: expected.PlanKey, Version: expected.Version, Digest: expected.Digest}
	lock, err := s.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer lock.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	plan, err := s.ensurePlanTx(ctx, tx, ref)
	if err != nil {
		return nil, err
	}
	history, err := loadHistoryTx(ctx, tx, expected.PlanKey)
	if err != nil {
		return nil, err
	}
	if err := validateHistory(expected.PlanKey, history); err != nil {
		_ = invalidateTx(ctx, tx, expected.PlanKey, IntegrityReplacement, []string{err.Error()})
		_ = tx.Commit()
		return nil, staleError(ref, "validated history chain", err)
	}
	idx := result.Summary.ExecutionRowIndex
	if idx < 0 || idx >= len(plan.Memory.Rows) {
		return nil, staleError(ref, fmt.Sprintf("execution row index %d", idx), fmt.Errorf("index outside plan"))
	}
	wantRow := plan.Memory.Rows[idx]
	if wantRow.RowStart != result.Summary.RowStart || wantRow.RowEnd != result.Summary.RowEnd || wantRow.PlanRowBlock != result.Summary.PlanRowBlockIndex {
		return nil, staleError(ref, fmt.Sprintf("row block coordinate [%d,%d)", result.Summary.RowStart, result.Summary.RowEnd), fmt.Errorf("cross-plan row block mismatch"))
	}
	var lastIdx int
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(execution_row_index), -1) FROM block_results WHERE plan_key=? AND version_seq=? AND plan_digest=?`, ref.PlanKey, ref.Version, ref.Digest).Scan(&lastIdx)
	if err != nil {
		return nil, err
	}
	if idx != lastIdx+1 {
		return nil, staleError(ref, fmt.Sprintf("next execution row index %d", lastIdx+1), fmt.Errorf("got index %d (duplicate, gap, or stale worker)", idx))
	}
	valuesJSON, err := json.Marshal(result.Values)
	if err != nil {
		return nil, err
	}
	traceJSON, err := json.Marshal(result.Trace)
	if err != nil {
		return nil, err
	}
	errorJSON, err := json.Marshal(result.Error)
	if err != nil {
		return nil, err
	}
	errorDigest := spmm.HashJSON(result.Error)
	if errorDigest != result.Summary.ErrorDigest {
		return nil, staleError(ref, result.Summary.ErrorDigest, fmt.Errorf("error report digest mismatch"))
	}
	if result.InputDigest == "" {
		return nil, staleError(ref, result.InputDigest, fmt.Errorf("input fingerprint is missing"))
	}
	if result.Summary.ColumnStart != 0 || result.Summary.ColumnEnd <= 0 || len(result.Values) != (result.Summary.RowEnd-result.Summary.RowStart)*result.Summary.ColumnEnd {
		return nil, staleError(ref, result.Summary.OutputDigest, fmt.Errorf("block output shape is inconsistent"))
	}
	if got := spmm.DigestValues(result.Summary.RowStart, result.Summary.RowEnd, result.Summary.ColumnEnd, result.Values); got != result.Summary.OutputDigest {
		return nil, staleError(ref, result.Summary.OutputDigest, fmt.Errorf("output digest mismatch"))
	}
	if got := spmm.HashJSON(result.Trace); got != result.Summary.TraceDigest {
		return nil, staleError(ref, result.Summary.TraceDigest, fmt.Errorf("trace digest mismatch"))
	}
	created := now()
	_, err = tx.ExecContext(ctx, `INSERT INTO block_results(
		plan_key, version_seq, plan_digest, execution_row_index, plan_row_block_index,
		row_start, row_end, col_start, col_end, input_fingerprint, output_digest,
		trace_digest, error_digest, output_json, trace_json, error_json, created_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		ref.PlanKey, ref.Version, ref.Digest, idx, result.Summary.PlanRowBlockIndex,
		result.Summary.RowStart, result.Summary.RowEnd, result.Summary.ColumnStart, result.Summary.ColumnEnd,
		result.InputDigest, result.Summary.OutputDigest, result.Summary.TraceDigest, errorDigest,
		valuesJSON, traceJSON, errorJSON, created)
	if err != nil {
		return nil, staleError(ref, fmt.Sprintf("execution row index %d", idx), err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO error_reports(plan_key, version_seq, execution_row_index, report_digest, report_json, created_at)
		VALUES (?,?,?,?,?,?)`, ref.PlanKey, ref.Version, idx, errorDigest, errorJSON, created)
	if err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO operator_state(plan_key, version_seq, plan_digest, last_execution_row_index, last_row_start, last_row_end, state, updated_at)
		VALUES (?,?,?,?,?,?,?,?)
		ON CONFLICT(plan_key) DO UPDATE SET version_seq=excluded.version_seq, plan_digest=excluded.plan_digest,
		last_execution_row_index=excluded.last_execution_row_index, last_row_start=excluded.last_row_start,
		last_row_end=excluded.last_row_end, state=excluded.state, updated_at=excluded.updated_at`,
		ref.PlanKey, ref.Version, ref.Digest, idx, result.Summary.RowStart, result.Summary.RowEnd, "running", created)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit row block: %w", err)
	}
	return &BlockRecord{
		PlanKey: ref.PlanKey, Version: ref.Version, PlanDigest: ref.Digest,
		ExecutionRowIndex: idx, PlanRowBlockIndex: result.Summary.PlanRowBlockIndex,
		RowStart: result.Summary.RowStart, RowEnd: result.Summary.RowEnd,
		ColStart: result.Summary.ColumnStart, ColEnd: result.Summary.ColumnEnd,
		InputFingerprint: result.InputDigest, OutputDigest: result.Summary.OutputDigest,
		TraceDigest: result.Summary.TraceDigest, ErrorDigest: errorDigest,
		Values: result.Values, Trace: result.Trace, Error: result.Error, CreatedAt: created,
	}, nil
}

func (s *Store) LoadBlocks(ctx context.Context, ref PlanRef) ([]BlockRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT b.execution_row_index, b.plan_row_block_index, b.row_start, b.row_end,
		b.col_start, b.col_end, b.input_fingerprint, b.output_digest, b.trace_digest, b.error_digest,
		b.output_json, b.trace_json, b.error_json, b.created_at,
		COALESCE(e.report_digest, ''), COALESCE(e.report_json, X'')
		FROM block_results b LEFT JOIN error_reports e
		ON e.plan_key=b.plan_key AND e.version_seq=b.version_seq AND e.execution_row_index=b.execution_row_index
		WHERE b.plan_key=? AND b.version_seq=? AND b.plan_digest=? ORDER BY b.execution_row_index`, ref.PlanKey, ref.Version, ref.Digest)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BlockRecord
	for rows.Next() {
		var rec BlockRecord
		var outJSON, traceJSON, errJSON, errReportJSON []byte
		var errReportDigest string
		if err := rows.Scan(&rec.ExecutionRowIndex, &rec.PlanRowBlockIndex, &rec.RowStart, &rec.RowEnd,
			&rec.ColStart, &rec.ColEnd, &rec.InputFingerprint, &rec.OutputDigest, &rec.TraceDigest, &rec.ErrorDigest,
			&outJSON, &traceJSON, &errJSON, &rec.CreatedAt, &errReportDigest, &errReportJSON); err != nil {
			return nil, err
		}
		rec.PlanKey, rec.Version, rec.PlanDigest = ref.PlanKey, ref.Version, ref.Digest
		if err := json.Unmarshal(outJSON, &rec.Values); err != nil {
			return nil, fmt.Errorf("decode block values: %w", err)
		}
		if err := json.Unmarshal(traceJSON, &rec.Trace); err != nil {
			return nil, fmt.Errorf("decode block trace: %w", err)
		}
		if err := json.Unmarshal(errJSON, &rec.Error); err != nil {
			return nil, fmt.Errorf("decode block error: %w", err)
		}
		if len(errReportJSON) > 0 {
			var stored spmm.ErrorReport
			if err := json.Unmarshal(errReportJSON, &stored); err != nil {
				return nil, fmt.Errorf("decode error report: %w", err)
			}
			if errReportDigest != spmm.HashJSON(stored) {
				return nil, spmm.NewError(spmm.CodeState, "persisted error report digest mismatch at row block %d", rec.ExecutionRowIndex)
			}
			if errReportDigest != rec.ErrorDigest {
				return nil, spmm.NewError(spmm.CodeState, "independent error report and block error digest differ at row block %d", rec.ExecutionRowIndex)
			}
		} else {
			return nil, spmm.NewError(spmm.CodeState, "persisted error report is missing at row block %d", rec.ExecutionRowIndex)
		}
		if got := spmm.HashJSON(rec.Error); got != rec.ErrorDigest {
			return nil, spmm.NewError(spmm.CodeState, "block error digest mismatch at row block %d", rec.ExecutionRowIndex)
		}
		if got := spmm.DigestValues(rec.RowStart, rec.RowEnd, rec.ColEnd, rec.Values); got != rec.OutputDigest {
			return nil, spmm.NewError(spmm.CodeState, "block output digest mismatch at row block %d", rec.ExecutionRowIndex)
		}
		if got := spmm.HashJSON(rec.Trace); got != rec.TraceDigest {
			return nil, spmm.NewError(spmm.CodeState, "block trace digest mismatch at row block %d", rec.ExecutionRowIndex)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		if out[i].ExecutionRowIndex != i {
			return nil, spmm.NewError(spmm.CodeState, "persisted row block sequence has a gap at index %d", i)
		}
	}
	return out, nil
}

func (s *Store) BlockCount(ctx context.Context, ref PlanRef) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM block_results WHERE plan_key=? AND version_seq=? AND plan_digest=?`, ref.PlanKey, ref.Version, ref.Digest).Scan(&n)
	return n, err
}

func (s *Store) LastBlockIndex(ctx context.Context, ref PlanRef) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(execution_row_index), -1) FROM block_results WHERE plan_key=? AND version_seq=? AND plan_digest=?`, ref.PlanKey, ref.Version, ref.Digest).Scan(&n)
	return n, err
}

func (s *Store) LatestBlock(ctx context.Context, ref PlanRef) (*BlockRecord, error) {
	blocks, err := s.LoadBlocks(ctx, ref)
	if err != nil {
		return nil, err
	}
	if len(blocks) == 0 {
		return nil, spmm.NewError(spmm.CodeState, "no committed blocks")
	}
	return &blocks[len(blocks)-1], nil
}

func (s *Store) State(ctx context.Context, key string) (*OperatorState, error) {
	var st OperatorState
	err := s.db.QueryRowContext(ctx, `SELECT plan_key, version_seq, plan_digest, last_execution_row_index, last_row_start, last_row_end, state, updated_at FROM operator_state WHERE plan_key=?`, key).
		Scan(&st.PlanKey, &st.Version, &st.PlanDigest, &st.LastExecutionRow, &st.LastRowStart, &st.LastRowEnd, &st.State, &st.UpdatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, spmm.NewError(spmm.CodeState, "operator state not found")
		}
		return nil, err
	}
	return &st, nil
}

func (s *Store) SetState(ctx context.Context, ref PlanRef, state string) error {
	lock, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer lock.Unlock()
	res, err := s.db.ExecContext(ctx, `UPDATE operator_state SET state=?, updated_at=? WHERE plan_key=? AND version_seq=? AND plan_digest=?`, state, now(), ref.PlanKey, ref.Version, ref.Digest)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return staleError(ref, "state publication", fmt.Errorf("operator state row missing or stale"))
	}
	return nil
}

func (s *Store) ClearBlocks(ctx context.Context, ref PlanRef) error {
	lock, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer lock.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM block_results WHERE plan_key=? AND version_seq=? AND plan_digest=?`, ref.PlanKey, ref.Version, ref.Digest); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM error_reports WHERE plan_key=? AND version_seq=?`, ref.PlanKey, ref.Version); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM operator_state WHERE plan_key=?`, ref.PlanKey); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) SortedKeys(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT plan_key FROM plan_heads`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, rows.Err()
}
