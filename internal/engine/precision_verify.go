package engine

import (
	"context"
	"fmt"
	"strings"

	"checkpointengine/internal/model"
)

func (e *Engine) computeAndCheckPrecision(shards []model.Shard, policy model.Snapshot, lossScale, now string) ([]model.PrecisionResult, model.PrecisionReport, error) {
	pairs, err := buildPrecisionPairs(shards, 0)
	if err != nil {
		return nil, model.PrecisionReport{}, err
	}
	results := make([]model.PrecisionResult, 0, len(pairs))
	for _, pair := range pairs {
		result, err := computePrecisionResult(pair, lossScale, policy, now)
		if err != nil {
			return nil, model.PrecisionReport{}, err
		}
		results = append(results, result)
	}
	report := makePrecisionReport(0, lossScale, results)
	return results, report, nil
}

func (e *Engine) VerifyPrecision(ctx context.Context, req model.VerifyPrecisionRequest) (model.PrecisionReport, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	state, err := loadStateDB(ctx, e.store.DB)
	if err != nil {
		return model.PrecisionReport{}, err
	}
	policyRec, err := e.loadPolicyDB(ctx, state.CurrentPolicyVersion)
	if err != nil {
		return model.PrecisionReport{}, err
	}
	if policyRec == nil {
		return model.PrecisionReport{}, model.NewAPIError(model.CodeNoPolicy, "no precision policy has been committed")
	}
	lossScale := state.LossScale
	if len(strings.TrimSpace(string(req.LossScale))) > 0 && string(req.LossScale) != "null" {
		lossScale, err = model.DecimalValue(req.LossScale)
		if err != nil {
			return model.PrecisionReport{}, model.WrapAPIError(model.CodeBadRequest, "invalid loss scale", err)
		}
	}
	shards, err := loadAllShards(ctx, e.store.DB)
	if err != nil {
		return model.PrecisionReport{}, err
	}
	if req.Round > 0 {
		filtered := shards[:0]
		for _, s := range shards {
			if s.Round == req.Round {
				filtered = append(filtered, s)
			}
		}
		shards = filtered
	}
	shards, err = resolveRelationships(shards)
	if err != nil {
		return model.PrecisionReport{}, err
	}
	now := e.nowText()
	results, report, err := e.computeAndCheckPrecision(shards, policyRec.Snapshot, lossScale, now)
	if err != nil {
		return model.PrecisionReport{}, err
	}
	submission := strings.TrimSpace(req.Submission)
	if submission == "" {
		submission = fmt.Sprintf("precision:%d", e.nowMS())
	}
	intentIDs, err := e.insertIntents(ctx, []intentSpec{{Kind: "PRECISION_VERIFY", SubmissionID: submission,
		After: map[string]any{"round": req.Round, "loss_scale": lossScale, "results": len(results)}}})
	if err != nil {
		return model.PrecisionReport{}, err
	}
	tx, err := e.store.DB.BeginTx(ctx, nil)
	if err != nil {
		e.abortIntents(ctx, intentIDs)
		return model.PrecisionReport{}, err
	}
	defer tx.Rollback()
	saved, err := savePrecisionResultsTx(ctx, tx, results)
	if err != nil {
		e.abortIntents(ctx, intentIDs)
		return model.PrecisionReport{}, err
	}
	report = makePrecisionReport(req.Round, lossScale, saved)
	if _, err := rebuildAllDerivedTx(ctx, tx, policyRec.Snapshot, state.Revision, now); err != nil {
		e.abortIntents(ctx, intentIDs)
		return model.PrecisionReport{}, err
	}
	if err := markIntentsApplied(ctx, tx, intentIDs, now); err != nil {
		e.abortIntents(ctx, intentIDs)
		return model.PrecisionReport{}, err
	}
	if err := tx.Commit(); err != nil {
		e.abortIntents(ctx, intentIDs)
		return model.PrecisionReport{}, err
	}
	if !report.Passed {
		apiErr := model.NewAPIError(model.CodePrecisionDrift, "precision drift exceeds the declared policy bounds")
		apiErr.Details = map[string]any{"precision": report}
		return report, apiErr
	}
	return report, nil
}
