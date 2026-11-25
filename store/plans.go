package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"spmmopt/spmm"
)

func (s *Store) PublishPlan(ctx context.Context, plan *spmm.Plan, expected PlanExpectation) (*PlanRecord, error) {
	if plan == nil {
		return nil, spmm.NewError(spmm.CodeInput, "plan is required")
	}
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	key := spmm.PlanKey(plan)
	payload, err := json.Marshal(plan)
	if err != nil {
		return nil, fmt.Errorf("encode plan: %w", err)
	}
	payloadHash := spmm.HashBytes(payload)
	lock, err := s.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer lock.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin plan transaction: %w", err)
	}
	defer tx.Rollback()

	history, err := loadHistoryTx(ctx, tx, key)
	if err != nil {
		return nil, err
	}
	if len(history) > 0 {
		if err := validateHistory(key, history); err != nil {
			_ = invalidateTx(ctx, tx, key, IntegrityReplacement, []string{err.Error()})
			if commitErr := tx.Commit(); commitErr != nil {
				return nil, fmt.Errorf("commit invalidation: %w", commitErr)
			}
			return nil, staleError(PlanRef{PlanKey: key}, formatExpectation(expected), err)
		}
	}
	head, err := loadHeadTx(ctx, tx, key)
	if err != nil && !isNotFound(err) {
		return nil, err
	}
	hasHead := err == nil
	if len(history) > 0 {
		latest := history[len(history)-1]
		if !hasHead || !headMatchesHistory(head, latest) {
			status := IntegrityReplacement
			reason := "plan head does not match immutable latest history"
			refVersion := latest.Version
			if hasHead && head.Version < latest.Version {
				status = IntegrityRollback
				reason = "head sequence is behind valid history"
			}
			if hasHead {
				refVersion = head.Version
			}
			_ = invalidateTx(ctx, tx, key, status, []string{reason})
			// Restoring the head from the immutable digest chain makes a later
			// retry safe; this already-stale publish attempt is still rejected.
			if _, repairErr := tx.ExecContext(ctx, `INSERT INTO plan_heads(plan_key, version_seq, plan_digest, payload_hash, shape_fingerprint, structure_fingerprint, dense_fingerprint, updated_at)
				VALUES (?,?,?,?,?,?,?,?)
				ON CONFLICT(plan_key) DO UPDATE SET version_seq=excluded.version_seq, plan_digest=excluded.plan_digest,
				payload_hash=excluded.payload_hash, shape_fingerprint=excluded.shape_fingerprint,
				structure_fingerprint=excluded.structure_fingerprint, dense_fingerprint=excluded.dense_fingerprint,
				updated_at=excluded.updated_at`,
				key, latest.Version, latest.Digest, latest.PayloadHash,
				latest.Plan.ShapeFingerprint, latest.Plan.StructureFingerprint, latest.Plan.DenseFingerprint, now()); repairErr != nil {
				return nil, repairErr
			}
			if err := tx.Commit(); err != nil {
				return nil, err
			}
			return nil, staleError(PlanRef{PlanKey: key, Version: refVersion}, formatExpectation(expected), fmt.Errorf("%s", reason))
		}
	} else if hasHead {
		_ = invalidateTx(ctx, tx, key, IntegrityReplacement, []string{"head exists without immutable history"})
		if _, err := tx.ExecContext(ctx, `DELETE FROM plan_heads WHERE plan_key=?`, key); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return nil, staleError(PlanRef{PlanKey: key, Version: head.Version, Digest: head.Digest}, formatExpectation(expected), fmt.Errorf("orphan head detected"))
	}
	var newVersion int64
	prevDigest := ""
	if hasHead {
		if expected.Version != head.Version || expected.Digest == "" || expected.Digest != head.Digest {
			return nil, staleError(PlanRef{PlanKey: key, Version: head.Version, Digest: head.Digest}, formatExpectation(expected), fmt.Errorf("expected head does not match"))
		}
		newVersion = head.Version + 1
		prevDigest = head.Digest
	} else {
		if expected.Version != 0 || expected.Digest != "" {
			return nil, staleError(PlanRef{PlanKey: key}, formatExpectation(expected), fmt.Errorf("no plan head exists"))
		}
		newVersion = 1
	}
	digest := versionDigest(key, newVersion, payloadHash, prevDigest)
	created := now()
	_, err = tx.ExecContext(ctx, `INSERT INTO plan_versions(
		plan_key, version_seq, shape_fingerprint, structure_fingerprint, dense_fingerprint,
		sum_strategy, cache_budget, peak_memory, row_block_size, col_block_size,
		payload_hash, plan_digest, prev_digest, plan_json, created_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		key, newVersion, plan.ShapeFingerprint, plan.StructureFingerprint, plan.DenseFingerprint,
		string(plan.SumStrategy), plan.CacheBudgetBytes, plan.PeakMemoryBytes,
		plan.RowBlockSize, plan.ColBlockSize, payloadHash, digest, prevDigest, payload, created)
	if err != nil {
		return nil, staleError(PlanRef{PlanKey: key, Version: newVersion}, formatExpectation(expected), err)
	}

	if hasHead {
		res, err := tx.ExecContext(ctx, `UPDATE plan_heads SET version_seq=?, plan_digest=?, payload_hash=?, shape_fingerprint=?, structure_fingerprint=?, dense_fingerprint=?, updated_at=? WHERE plan_key=? AND version_seq=? AND plan_digest=?`,
			newVersion, digest, payloadHash, plan.ShapeFingerprint, plan.StructureFingerprint, plan.DenseFingerprint, created,
			key, expected.Version, expected.Digest)
		if err != nil {
			return nil, err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return nil, staleError(PlanRef{PlanKey: key, Version: newVersion}, formatExpectation(expected), fmt.Errorf("conditional head update matched %d rows", n))
		}
	} else {
		res, err := tx.ExecContext(ctx, `INSERT INTO plan_heads(plan_key, version_seq, plan_digest, payload_hash, shape_fingerprint, structure_fingerprint, dense_fingerprint, updated_at)
			VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(plan_key) DO NOTHING`,
			key, newVersion, digest, payloadHash, plan.ShapeFingerprint, plan.StructureFingerprint, plan.DenseFingerprint, created)
		if err != nil {
			return nil, err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return nil, staleError(PlanRef{PlanKey: key, Version: newVersion}, formatExpectation(expected), fmt.Errorf("head insert conflicted"))
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO operator_state(plan_key, version_seq, plan_digest, last_execution_row_index, last_row_start, last_row_end, state, updated_at)
		VALUES (?,?,?,?,?,?,?,?)
		ON CONFLICT(plan_key) DO UPDATE SET version_seq=excluded.version_seq, plan_digest=excluded.plan_digest,
		last_execution_row_index=excluded.last_execution_row_index, last_row_start=excluded.last_row_start,
		last_row_end=excluded.last_row_end, state=excluded.state, updated_at=excluded.updated_at`,
		key, newVersion, digest, -1, 0, 0, "planned", created); err != nil {
		return nil, fmt.Errorf("initialize operator state: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit plan: %w", err)
	}
	return &PlanRecord{PlanKey: key, Version: newVersion, Digest: digest, PreviousDigest: prevDigest, PayloadHash: payloadHash, Plan: *plan, CreatedAt: created}, nil
}

func headMatchesHistory(head headRecord, latest historyRecord) bool {
	return head.Version == latest.Version && head.Digest == latest.Digest &&
		head.PayloadHash == latest.PayloadHash &&
		head.Shape == latest.Plan.ShapeFingerprint &&
		head.Structure == latest.Plan.StructureFingerprint &&
		head.Dense == latest.Plan.DenseFingerprint
}

func formatExpectation(e PlanExpectation) string {
	return fmt.Sprintf("version=%d digest=%s", e.Version, e.Digest)
}

type historyRecord struct {
	Version     int64
	Digest      string
	PrevDigest  string
	PayloadHash string
	Plan        spmm.Plan
	PlanJSON    []byte
}

func loadHistoryTx(ctx context.Context, tx *sql.Tx, key string) ([]historyRecord, error) {
	rows, err := tx.QueryContext(ctx, `SELECT version_seq, plan_digest, prev_digest, payload_hash, plan_json FROM plan_versions WHERE plan_key=? ORDER BY version_seq`, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []historyRecord
	for rows.Next() {
		var h historyRecord
		if err := rows.Scan(&h.Version, &h.Digest, &h.PrevDigest, &h.PayloadHash, &h.PlanJSON); err != nil {
			return nil, err
		}
		p, err := decodePlan(h.PlanJSON)
		if err != nil {
			return nil, fmt.Errorf("decode plan version %d: %w", h.Version, err)
		}
		h.Plan = p
		out = append(out, h)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func validateHistory(key string, history []historyRecord) error {
	var prev string
	for i := range history {
		h := &history[i]
		wantSeq := int64(i + 1)
		if h.Version != wantSeq {
			return fmt.Errorf("version sequence gap: got %d want %d", h.Version, wantSeq)
		}
		if h.PrevDigest != prev {
			return fmt.Errorf("previous digest mismatch at version %d", h.Version)
		}
		if spmm.PlanKey(&h.Plan) != key {
			return fmt.Errorf("plan key mismatch at version %d", h.Version)
		}
		payloadHash := spmm.HashBytes(h.PlanJSON)
		if payloadHash != h.PayloadHash {
			return fmt.Errorf("payload hash mismatch at version %d", h.Version)
		}
		wantDigest := versionDigest(key, h.Version, payloadHash, prev)
		if wantDigest != h.Digest {
			return fmt.Errorf("plan digest mismatch at version %d", h.Version)
		}
		if h.Plan.ShapeFingerprint == "" || h.Plan.StructureFingerprint == "" || h.Plan.DenseFingerprint == "" {
			return fmt.Errorf("plan fingerprint missing at version %d", h.Version)
		}
		prev = h.Digest
	}
	return nil
}

type headRecord struct {
	Version     int64
	Digest      string
	PayloadHash string
	Shape       string
	Structure   string
	Dense       string
}

func loadHeadTx(ctx context.Context, tx *sql.Tx, key string) (headRecord, error) {
	var h headRecord
	err := tx.QueryRowContext(ctx, `SELECT version_seq, plan_digest, payload_hash, shape_fingerprint, structure_fingerprint, dense_fingerprint FROM plan_heads WHERE plan_key=?`, key).
		Scan(&h.Version, &h.Digest, &h.PayloadHash, &h.Shape, &h.Structure, &h.Dense)
	return h, err
}

func invalidateTx(ctx context.Context, tx *sql.Tx, key string, status IntegrityStatus, reasons []string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM block_results WHERE plan_key=?`, key); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM error_reports WHERE plan_key=?`, key); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM operator_state WHERE plan_key=?`, key); err != nil {
		return err
	}
	b, _ := json.Marshal(reasons)
	_, err := tx.ExecContext(ctx, `INSERT INTO plan_invalidations(plan_key, status, reasons, invalidated_at) VALUES (?,?,?,?)`, key, string(status), string(b), now())
	return err
}

func (s *Store) Head(ctx context.Context, key string) (*PlanRecord, error) {
	row := s.db.QueryRowContext(ctx, `SELECT version_seq, plan_digest, prev_digest, payload_hash, plan_json, created_at FROM plan_versions WHERE plan_key=? ORDER BY version_seq DESC LIMIT 1`, key)
	var rec PlanRecord
	var planJSON []byte
	if err := row.Scan(&rec.Version, &rec.Digest, &rec.PreviousDigest, &rec.PayloadHash, &planJSON, &rec.CreatedAt); err != nil {
		if isNotFound(err) {
			return nil, spmm.NewError(spmm.CodeState, "plan key %s not found", key)
		}
		return nil, err
	}
	p, err := decodePlan(planJSON)
	if err != nil {
		return nil, err
	}
	rec.PlanKey, rec.Plan = key, p
	var headVersion int64
	var headDigest string
	if err := s.db.QueryRowContext(ctx, `SELECT version_seq, plan_digest FROM plan_heads WHERE plan_key=?`, key).Scan(&headVersion, &headDigest); err != nil {
		if isNotFound(err) {
			return nil, spmm.NewError(spmm.CodeState, "plan head %s is missing", key)
		}
		return nil, err
	}
	if headVersion != rec.Version || headDigest != rec.Digest {
		return nil, spmm.NewError(spmm.CodeState, "plan head does not match latest history")
	}
	return &rec, nil
}

func (s *Store) GetPlan(ctx context.Context, key string, version int64) (*PlanRecord, error) {
	row := s.db.QueryRowContext(ctx, `SELECT version_seq, plan_digest, prev_digest, payload_hash, plan_json, created_at FROM plan_versions WHERE plan_key=? AND version_seq=?`, key, version)
	var rec PlanRecord
	var planJSON []byte
	if err := row.Scan(&rec.Version, &rec.Digest, &rec.PreviousDigest, &rec.PayloadHash, &planJSON, &rec.CreatedAt); err != nil {
		if isNotFound(err) {
			return nil, spmm.NewError(spmm.CodeState, "plan version %s/%d not found", key, version)
		}
		return nil, err
	}
	p, err := decodePlan(planJSON)
	if err != nil {
		return nil, err
	}
	rec.PlanKey, rec.Plan = key, p
	return &rec, nil
}

func (s *Store) History(ctx context.Context, key string) ([]PlanRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT version_seq, plan_digest, prev_digest, payload_hash, plan_json, created_at FROM plan_versions WHERE plan_key=? ORDER BY version_seq`, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PlanRecord
	for rows.Next() {
		var rec PlanRecord
		var planJSON []byte
		if err := rows.Scan(&rec.Version, &rec.Digest, &rec.PreviousDigest, &rec.PayloadHash, &planJSON, &rec.CreatedAt); err != nil {
			return nil, err
		}
		p, err := decodePlan(planJSON)
		if err != nil {
			return nil, err
		}
		rec.PlanKey, rec.Plan = key, p
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (s *Store) ReconcilePlan(ctx context.Context, key string, expected *PlanRef) (*IntegrityReport, error) {
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
	report := &IntegrityReport{PlanKey: key, Status: IntegrityOK}
	if expected != nil {
		report.ExpectedVersion, report.ExpectedDigest = expected.Version, expected.Digest
	}
	history, err := loadHistoryTx(ctx, tx, key)
	if err != nil {
		return nil, err
	}
	if len(history) == 0 {
		report.Status = IntegrityMissing
		report.Reason = "no plan history"
		return report, nil
	}
	report.HistoryMaxVersion = history[len(history)-1].Version
	historyValid := true
	if err := validateHistory(key, history); err != nil {
		historyValid = false
		report.Status = IntegrityReplacement
		report.Reason = "immutable history is inconsistent"
		report.Reasons = append(report.Reasons, err.Error())
	}
	head, headErr := loadHeadTx(ctx, tx, key)
	if headErr != nil && !isNotFound(headErr) {
		return nil, headErr
	}
	if headErr == nil {
		report.HeadVersion, report.HeadDigest = head.Version, head.Digest
		latest := history[len(history)-1]
		if head.Version < report.HistoryMaxVersion {
			report.Status = IntegrityRollback
			report.Reason = "head sequence rolled back behind immutable history"
			report.Reasons = append(report.Reasons, fmt.Sprintf("head=%d history_max=%d", head.Version, report.HistoryMaxVersion))
		} else if head.Version > report.HistoryMaxVersion {
			report.Status = IntegrityReplacement
			report.Reason = "head sequence is not present in history"
		} else if head.Digest != latest.Digest || head.PayloadHash != latest.PayloadHash {
			report.Status = IntegrityReplacement
			report.Reason = "head digest no longer matches immutable history"
		} else if head.Shape != latest.Plan.ShapeFingerprint || head.Structure != latest.Plan.StructureFingerprint || head.Dense != latest.Plan.DenseFingerprint {
			report.Status = IntegrityReplacement
			report.Reason = "head fingerprints were changed"
		}
	} else {
		report.Status = IntegrityReplacement
		report.Reason = "plan head is missing"
	}
	if expected != nil && report.Status == IntegrityOK {
		switch {
		case expected.Version > report.HistoryMaxVersion:
			report.Status = IntegrityRollback
			report.Reason = "external observation is ahead of persisted history"
			report.Reasons = append(report.Reasons, fmt.Sprintf("expected=%d history_max=%d", expected.Version, report.HistoryMaxVersion))
		case expected.Version == head.Version && expected.Digest != "" && expected.Digest != head.Digest:
			report.Status = IntegrityReplacement
			report.Reason = "same version has a different digest (overall replacement)"
		case expected.Version < head.Version:
			known := false
			for _, h := range history {
				if h.Version == expected.Version && (expected.Digest == "" || h.Digest == expected.Digest) {
					known = true
					break
				}
			}
			if known {
				report.Status = IntegrityAdvance
				report.Reason = "plan advanced normally"
			} else {
				report.Status = IntegrityReplacement
				report.Reason = "expected version is absent from immutable history"
			}
		}
		if expected.ShapeFingerprint != "" && head.Shape != "" && expected.ShapeFingerprint != head.Shape {
			report.Status = IntegrityReplacement
			report.Reason = "shape fingerprint changed under the same plan key"
		}
		if expected.StructureFingerprint != "" && head.Structure != "" && expected.StructureFingerprint != head.Structure {
			report.Status = IntegrityReplacement
			report.Reason = "sparse structure fingerprint changed under the same plan key"
		}
	}
	if report.Status == IntegrityRollback || report.Status == IntegrityReplacement || report.Status == IntegrityDigestMismatch {
		if err := invalidateTx(ctx, tx, key, report.Status, report.Reasons); err != nil {
			return nil, err
		}
		report.DownstreamInvalidated = true
		if historyValid {
			latest := history[len(history)-1]
			if _, err := tx.ExecContext(ctx, `INSERT INTO plan_heads(plan_key, version_seq, plan_digest, payload_hash, shape_fingerprint, structure_fingerprint, dense_fingerprint, updated_at)
				VALUES (?,?,?,?,?,?,?,?)
				ON CONFLICT(plan_key) DO UPDATE SET version_seq=excluded.version_seq, plan_digest=excluded.plan_digest,
				payload_hash=excluded.payload_hash, shape_fingerprint=excluded.shape_fingerprint,
				structure_fingerprint=excluded.structure_fingerprint, dense_fingerprint=excluded.dense_fingerprint,
				updated_at=excluded.updated_at`,
				key, latest.Version, latest.Digest, latest.PayloadHash,
				latest.Plan.ShapeFingerprint, latest.Plan.StructureFingerprint, latest.Plan.DenseFingerprint, now()); err != nil {
				return nil, err
			}
			report.HeadRestored = true
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return report, nil
}

func (s *Store) Invalidations(ctx context.Context, key string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT status || ' ' || reasons FROM plan_invalidations WHERE plan_key=? ORDER BY id`, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) ensurePlanTx(ctx context.Context, tx *sql.Tx, ref PlanRef) (spmm.Plan, error) {
	head, err := loadHeadTx(ctx, tx, ref.PlanKey)
	if err != nil {
		if isNotFound(err) {
			return spmm.Plan{}, staleError(ref, fmt.Sprintf("version=%d digest=%s", ref.Version, ref.Digest), fmt.Errorf("head missing"))
		}
		return spmm.Plan{}, err
	}
	if head.Version != ref.Version || head.Digest != ref.Digest {
		return spmm.Plan{}, staleError(ref, fmt.Sprintf("version=%d digest=%s", ref.Version, ref.Digest), fmt.Errorf("head advanced"))
	}
	var planJSON []byte
	err = tx.QueryRowContext(ctx, `SELECT plan_json FROM plan_versions WHERE plan_key=? AND version_seq=? AND plan_digest=?`, ref.PlanKey, ref.Version, ref.Digest).Scan(&planJSON)
	if err != nil {
		if isNotFound(err) {
			return spmm.Plan{}, staleError(ref, "cross-plan publication", err)
		}
		return spmm.Plan{}, err
	}
	p, err := decodePlan(planJSON)
	return p, err
}
