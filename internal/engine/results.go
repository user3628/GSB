package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"

	"local/configmiddleware/internal/core"
	"local/configmiddleware/internal/pipeline"
)

func (e *Engine) SubmitResult(ctx context.Context, report core.ResultReport) (core.Result, error) {
	if report.ClientID == "" || report.PushID == "" || report.PickedVersion <= 0 {
		return core.Result{}, core.NewBizError("E_BAD_REQUEST", "client_id, push_id and picked_version are required", nil)
	}
	rid := resultID(report)
	var stored core.Result
	var batchID string
	_, err := e.store.IntentWrite(ctx, "RESULT_APPLY", rid, report, func(tx *sql.Tx) (bool, error) {
		existing, err := getResultTx(ctx, tx, rid)
		if err == nil {
			stored = existing
			push, getErr := getPushTx(ctx, tx, existing.PushID)
			if getErr == nil {
				batchID = push.BatchID
			}
			return true, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		push, err := getPushTx(ctx, tx, report.PushID)
		if errors.Is(err, sql.ErrNoRows) {
			return false, core.NewBizError("E_PUSH_NOT_FOUND", "push not found", map[string]any{"push_id": report.PushID})
		}
		if err != nil {
			return false, err
		}
		if push.ClientID != report.ClientID {
			return false, core.NewBizError("E_RESULT_VERSION_MISMATCH", "result client does not own push", map[string]any{
				"push_client": push.ClientID, "result_client": report.ClientID,
			})
		}
		ack, err := getAckByPushTx(ctx, tx, push.PushID)
		if errors.Is(err, sql.ErrNoRows) {
			return false, core.NewBizError("E_ACK_REQUIRED", "result requires a persisted confirmation", map[string]any{"push_id": push.PushID})
		}
		if err != nil {
			return false, err
		}
		if push.Status != core.StatusAcked && push.Status != core.StatusApplied {
			return false, core.NewBizError("E_INVALID_TRANSITION", "result requires ACKED or APPLIED push", map[string]any{
				"push_id": push.PushID, "status": push.Status,
			})
		}
		var configRaw string
		var config map[string]float64
		err = tx.QueryRowContext(ctx, `SELECT config_json FROM snapshots WHERE version=?`, report.PickedVersion).Scan(&configRaw)
		if errors.Is(err, sql.ErrNoRows) {
			return false, core.NewBizError("E_SNAPSHOT_NOT_FOUND", "picked snapshot does not exist", map[string]any{"picked_version": report.PickedVersion})
		}
		if err != nil {
			return false, err
		}
		if err := json.Unmarshal([]byte(configRaw), &config); err != nil {
			return false, err
		}
		expected, computeErr := pipeline.Compute(report.ClientID, report.PickedVersion, config, e.samples)
		mismatches := make([]core.FieldDiff, 0)
		if computeErr != nil {
			mismatches = append(mismatches, core.FieldDiff{Field: "config", Expected: "valid pipeline config", Actual: computeErr.Error()})
		} else {
			if report.Seed != expected.Seed {
				mismatches = append(mismatches, core.FieldDiff{Field: "seed", Expected: expected.Seed, Actual: report.Seed})
			}
			if report.SampleID != expected.SampleID {
				mismatches = append(mismatches, core.FieldDiff{Field: "sample_id", Expected: expected.SampleID, Actual: report.SampleID})
			}
			if report.SampleDigest != expected.SampleDigest {
				mismatches = append(mismatches, core.FieldDiff{Field: "sample_digest", Expected: expected.SampleDigest, Actual: report.SampleDigest})
			}
			if !nearlyEqual(report.WeightedSum, expected.WeightedSum) {
				mismatches = append(mismatches, core.FieldDiff{Field: "weighted_sum", Expected: expected.WeightedSum, Actual: report.WeightedSum})
			}
			if !nearlyEqual(report.Normalized, expected.Normalized) {
				mismatches = append(mismatches, core.FieldDiff{Field: "normalized", Expected: expected.Normalized, Actual: report.Normalized})
			}
			if report.Decision != expected.Decision {
				mismatches = append(mismatches, core.FieldDiff{Field: "decision", Expected: expected.Decision, Actual: report.Decision})
			}
			canonicalDigest := core.ResultDigest(core.Result{
				PickedVersion: report.PickedVersion, Seed: report.Seed, SampleDigest: report.SampleDigest,
				WeightedSum: report.WeightedSum, Normalized: report.Normalized, Decision: report.Decision,
			})
			if report.ResultDigest != canonicalDigest {
				mismatches = append(mismatches, core.FieldDiff{Field: "result_digest", Expected: canonicalDigest, Actual: report.ResultDigest})
			}
		}
		if ack.Version != report.PickedVersion {
			mismatches = append(mismatches, core.FieldDiff{Field: "ack_version", Expected: report.PickedVersion, Actual: ack.Version})
		}
		if push.Version != report.PickedVersion {
			mismatches = append(mismatches, core.FieldDiff{Field: "push_version", Expected: push.Version, Actual: report.PickedVersion})
		}
		var latest int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM snapshots`).Scan(&latest); err != nil {
			return false, err
		}
		var newer int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM pushes WHERE client_id=? AND version>? AND status IN ('PENDING','PUSHED','ACKED','APPLIED')`,
			push.ClientID, report.PickedVersion).Scan(&newer); err != nil {
			return false, err
		}

		status := ""
		reason := ""
		switch {
		case ack.Version != report.PickedVersion:
			status, reason = "RECOMPUTE_REQUIRED", "ACK_VERSION_MISMATCH"
		case push.Version != report.PickedVersion:
			status, reason = "RECOMPUTE_REQUIRED", "PUSH_VERSION_MISMATCH"
		case newer > report.PickedVersion:
			status, reason = "RECOMPUTE_REQUIRED", "NEWER_VERSION_ACTIVE"
		case report.PickedVersion < latest:
			status, reason = "RECOMPUTE_REQUIRED", "PICKED_VERSION_BEHIND_LATEST"
		case len(mismatches) > 0:
			status, reason = "MISMATCH", "DETERMINISTIC_RESULT_MISMATCH"
		case push.Status == core.StatusApplied:
			status, reason = "APPLIED", "ALREADY_APPLIED"
		case push.Status == core.StatusAcked:
			status, reason = "APPLIED", "LATEST_VERSION_ACK_MATCH"
		default:
			status, reason = "RECOMPUTE_REQUIRED", "PUSH_NOT_CURRENT"
		}

		if status == "APPLIED" && push.Status == core.StatusAcked {
			if err := e.transitionTx(ctx, tx, push, core.StatusApplied, "validated result applied"); err != nil {
				return false, err
			}
		}
		now := core.Now()
		stored = core.Result{
			ResultID: rid, ClientID: report.ClientID, PushID: report.PushID, PickedVersion: report.PickedVersion,
			AckVersion: ack.Version, ResultDigest: report.ResultDigest, WeightedSum: report.WeightedSum,
			Normalized: report.Normalized, Decision: report.Decision, Seed: report.Seed, SampleID: report.SampleID,
			SampleDigest: report.SampleDigest, Status: status, Reason: reason, Mismatches: mismatches, CreatedAt: now,
		}
		decisionInt := 0
		if report.Decision {
			decisionInt = 1
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO results(result_id,client_id,push_id,picked_version,ack_version,result_digest,weighted_sum,normalized,decision,seed,sample_id,sample_digest,status,reason,mismatches_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			stored.ResultID, stored.ClientID, stored.PushID, stored.PickedVersion, stored.AckVersion, stored.ResultDigest,
			stored.WeightedSum, stored.Normalized, decisionInt, stored.Seed, stored.SampleID, stored.SampleDigest,
			stored.Status, stored.Reason, marshal(mismatches), stored.CreatedAt); err != nil {
			return false, err
		}
		batchID = push.BatchID
		return true, nil
	})
	if err != nil {
		return core.Result{}, err
	}
	if stored.Status == "APPLIED" && batchID != "" {
		_ = e.finishBatch(ctx, batchID)
	}
	return stored, nil
}

func getResultTx(ctx context.Context, tx *sql.Tx, resultID string) (core.Result, error) {
	var r core.Result
	var decision int
	var mismatchJSON string
	err := tx.QueryRowContext(ctx, `SELECT result_id,client_id,push_id,picked_version,ack_version,result_digest,weighted_sum,normalized,decision,seed,sample_id,sample_digest,status,reason,mismatches_json,created_at FROM results WHERE result_id=?`, resultID).
		Scan(&r.ResultID, &r.ClientID, &r.PushID, &r.PickedVersion, &r.AckVersion, &r.ResultDigest, &r.WeightedSum, &r.Normalized, &decision,
			&r.Seed, &r.SampleID, &r.SampleDigest, &r.Status, &r.Reason, &mismatchJSON, &r.CreatedAt)
	if err != nil {
		return r, err
	}
	r.Decision = decision != 0
	_ = json.Unmarshal([]byte(mismatchJSON), &r.Mismatches)
	return r, nil
}

func nearlyEqual(a, b float64) bool {
	scale := math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
	return math.Abs(a-b) <= 1e-9*scale
}
