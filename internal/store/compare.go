package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"gatewayfreshness.local/workbench/internal/freshness"
)

func (s *Store) ModeDifferences(ctx context.Context, batchID string, budget time.Duration) (*freshness.ModeDifferenceReport, error) {
	start := time.Now()
	decision, err := s.GetDecision(ctx, batchID)
	if err != nil {
		return nil, err
	}
	report := &freshness.ModeDifferenceReport{
		BatchID: decision.BatchID, WindowVersion: decision.WindowVersion,
		ToleranceConfigVersion: decision.ToleranceConfigVersion, ActualMode: decision.Mode,
		PrecheckLimit: decision.PrecheckLimit, Items: []freshness.ModeDifferenceItem{},
	}
	if budget <= 0 {
		report.Partial = true
		report.Reason = "budget_exhausted_before_mode_projection"
		report.Note = "batch metadata is labelled with one committed version, but no projection was produced within the declared budget"
		return report, nil
	}
	limit := decision.PrecheckLimit
	if limit > len(decision.Items) {
		limit = len(decision.Items)
	}
	if limit > 0 {
		report.FastSampleRate = float64(limit) / float64(len(decision.Items))
	}
	if decision.Mode == freshness.ModeFast {
		report.FullResultAvailable = false
		report.Note = "this batch was adjudicated in fast mode; skipped requests were never fully evaluated and are not presented as a full result"
		for _, item := range decision.Items {
			ex := "actual fast result is shown; a full result was not computed"
			if !item.Prechecked {
				ex = "outside the deterministic precheck prefix; explicitly not a pass and no full result exists"
			}
			report.Items = append(report.Items, freshness.ModeDifferenceItem{
				Ordinal: item.Ordinal, RequestID: item.RequestID,
				FullResult: "not_available", FullCode: "NOT_PRE_EVALUATED",
				FastResult: item.Result, FastCode: item.Code,
				Differs: false, Explanation: ex,
			})
		}
		report.PredictedFastOutcome = decision.Outcome
		report.Complete = true
		return report, nil
	}

	report.FullResultAvailable = true
	report.Note = "full decision is immutable; fast projection uses the same window version and stored precheck limit. Every skipped suffix item is reported as not_prechecked."
	prefixFailure := ""
	for _, item := range decision.Items {
		fastResult, fastCode := item.Result, item.Code
		explanation := "inside deterministic precheck prefix; fast and full conclusions use the same committed fields"
		if item.Ordinal >= limit {
			fastResult, fastCode = freshness.ResultNotPrechecked, freshness.CodePrecheckIncomplete
			explanation = "outside deterministic precheck prefix; fast mode would not check this request and therefore cannot count it as a pass"
		} else if fastCode != freshness.CodeOK && prefixFailure == "" {
			prefixFailure = fastCode
		}
		differs := item.Result != fastResult || item.Code != fastCode
		if differs {
			report.Differences++
		}
		report.Items = append(report.Items, freshness.ModeDifferenceItem{
			Ordinal: item.Ordinal, RequestID: item.RequestID,
			FullResult: item.Result, FullCode: item.Code,
			FastResult: fastResult, FastCode: fastCode,
			Differs: differs, Explanation: explanation,
		})
	}
	switch {
	case prefixFailure != "":
		report.PredictedFastOutcome = freshness.OutcomeRejected
	case limit < len(decision.Items):
		report.PredictedFastOutcome = freshness.OutcomePartial
	default:
		report.PredictedFastOutcome = decision.Outcome
	}
	report.Complete = true
	if time.Since(start) > budget {
		report.Complete = false
		report.Partial = true
		report.Reason = "budget_exhausted_after_first_consistent_projection"
	}
	return report, nil
}

func (s *Store) Inconsistencies(ctx context.Context, batchID string, views []freshness.ClientView, budget time.Duration) (*freshness.InconsistencyReport, error) {
	start := time.Now()
	if len(views) == 0 {
		return nil, freshness.NewAppError(freshness.CodeRequestInvalid, "at least one client view is required", nil)
	}
	latest, _, _, err := s.latestState(ctx)
	if err != nil {
		return nil, err
	}
	for i, v := range views {
		if v.ClientID == "" {
			return nil, freshness.NewAppError(freshness.CodeRequestInvalid, fmt.Sprintf("views[%d].client_id is required", i), nil)
		}
		if v.PickedWindowVersion <= 0 || v.PickedWindowVersion > latest {
			return nil, freshness.NewAppError(freshness.CodeVersionInvalid, "picked_window_version is not a committed version", map[string]any{
				"index": i, "picked_window_version": v.PickedWindowVersion, "latest_committed": latest,
			})
		}
		committed, err := s.committedVersionExists(ctx, v.PickedWindowVersion)
		if err != nil {
			return nil, err
		}
		if !committed {
			return nil, freshness.NewAppError(freshness.CodeVersionInvalid, "picked_window_version is a version gap, not a committed version", map[string]any{
				"index": i, "picked_window_version": v.PickedWindowVersion, "latest_committed": latest,
			})
		}
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	decision, err := s.getDecisionTx(ctx, tx, batchID)
	if err != nil {
		return nil, err
	}
	if decision == nil {
		return nil, ErrBatchNotFound
	}
	report := &freshness.InconsistencyReport{
		BatchID: decision.BatchID, WindowVersion: decision.WindowVersion,
		ToleranceConfigVersion: decision.ToleranceConfigVersion,
		Views:                  []freshness.ClientInconsistency{},
		Note:                   "client views re-evaluate nonce visibility as of picked_window_version while signature and timestamp fields remain immutable. The batch's own nonce occupancy is excluded so a visible accepted decision is not mistaken for a self-replay.",
	}
	budgetCtx, cancel := context.WithDeadline(ctx, start.Add(budget))
	defer cancel()
	if budget <= 0 {
		report.Partial = true
		report.Reason = "budget_exhausted_before_view_evaluation"
		return report, nil
	}
	for _, view := range views {
		if budgetCtx.Err() != nil {
			report.Partial = true
			report.Reason = "budget_exhausted_between_client_views"
			break
		}
		cfgVersion := view.PickedConfigVersion
		if cfgVersion == "" {
			_ = tx.QueryRowContext(budgetCtx, `SELECT config_version FROM batches WHERE window_version<=? ORDER BY window_version DESC LIMIT 1`, view.PickedWindowVersion).Scan(&cfgVersion)
			if cfgVersion == "" {
				cfgVersion = decision.ToleranceConfigVersion
			}
		}
		inv := freshness.ClientInconsistency{
			ClientID: view.ClientID, PickedWindowVersion: view.PickedWindowVersion,
			PickedConfigVersion: cfgVersion, BatchWindowVersion: decision.WindowVersion,
			DecisionConfigVersion: decision.ToleranceConfigVersion,
			BatchVisibleAtVersion: view.PickedWindowVersion >= decision.WindowVersion,
			DecisionBatchCode:     decision.Code, Differences: []freshness.FieldDifference{},
		}
		if !inv.BatchVisibleAtVersion {
			inv.ViewedBatchCode = freshness.CodeVersionNotVisible
			inv.Differs = true
			inv.Explanation = "the picked window version predates the batch commit; the batch was not visible to that client"
			inv.Differences = append(inv.Differences, freshness.FieldDifference{
				Field: "batch_visibility", DecisionValue: true, ClientViewedValue: false,
				Explanation: fmt.Sprintf("batch committed at version %d, client picked version %d", decision.WindowVersion, view.PickedWindowVersion),
			})
			report.Views = append(report.Views, inv)
			report.Differs = true
			continue
		}
		states, stateErr := s.loadNonceStates(budgetCtx, tx, view.PickedWindowVersion, decision.BatchID)
		if stateErr != nil {
			if budgetCtx.Err() != nil || errors.Is(stateErr, context.DeadlineExceeded) {
				report.Partial = true
				report.Reason = "budget_exhausted_during_view_evaluation"
				break
			}
			return nil, stateErr
		}
		viewedFirstFailure := ""
		for _, item := range decision.Items {
			if !item.Prechecked {
				continue
			}
			viewedCode := item.Code
			viewedResult := item.Result
			if item.Code != freshness.CodeSignatureMismatch && item.Code != freshness.CodeTimestampTooOld && item.Code != freshness.CodeTimestampAhead && item.Code != freshness.CodeNonceReplayed {
				// Accepted at decision time: check whether the selected version has a
				// different nonce visibility for this client.
				key := item.ClientID + "\x00" + item.Nonce
				if st, ok := states[key]; ok && st.State == "active" && st.ExpiresAtMS > decision.ReferenceTimeMS {
					viewedCode = freshness.CodeNonceReplayed
					viewedResult = freshness.ResultRejected
				}
			} else if item.Code == freshness.CodeNonceReplayed {
				key := item.ClientID + "\x00" + item.Nonce
				st, ok := states[key]
				if !ok || st.State != "active" || st.ExpiresAtMS <= decision.ReferenceTimeMS {
					viewedCode = freshness.CodeOK
					viewedResult = freshness.ResultAccepted
				}
			}
			if viewedCode != freshness.CodeOK && viewedFirstFailure == "" {
				viewedFirstFailure = viewedCode
			}
			if viewedCode != item.Code || viewedResult != item.Result {
				inv.Differs = true
				inv.Differences = append(inv.Differences, freshness.FieldDifference{
					Field: fmt.Sprintf("items[%d].code", item.Ordinal), DecisionValue: item.Code, ClientViewedValue: viewedCode,
					Explanation: fmt.Sprintf("client %s picked window version %d; request %s is evaluated against nonce visibility at that version", view.ClientID, view.PickedWindowVersion, item.RequestID),
				})
			}
		}
		if viewedFirstFailure != "" {
			inv.ViewedBatchCode = viewedFirstFailure
		} else if decision.Code == freshness.CodePrecheckIncomplete {
			inv.ViewedBatchCode = freshness.CodePrecheckIncomplete
		} else {
			inv.ViewedBatchCode = freshness.CodeOK
		}
		if inv.Differs {
			inv.Explanation = "the selected nonce/window version yields different per-request conclusions from the committed decision"
			report.Differs = true
		} else {
			inv.Explanation = "the selected version yields the same visible conclusion for this batch"
		}
		report.Views = append(report.Views, inv)
	}
	report.Complete = !report.Partial
	return report, nil
}
