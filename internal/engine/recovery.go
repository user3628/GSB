package engine

import (
	"context"
	"encoding/json"
	"time"

	"driftengine/internal/drift"
	"driftengine/internal/errs"
	"driftengine/internal/spec"
	"driftengine/internal/store"
)

func Recover(ctx context.Context, st *store.Store) (*RecoveryReport, error) {
	open, err := st.ListIntentsByStatus(ctx, "prepared", "running", "pending_write")
	if err != nil {
		return nil, errs.New(errs.Internal, "scan intents failed", err.Error())
	}
	report := &RecoveryReport{
		RecoveredAt:             time.Now().UTC(),
		OpenIntentCount:         len(open),
		RecoveredIntents:        []RecoveredIntent{},
		Objects:                 []RecoveryObject{},
		ExitReportClaimed:       false,
		ExternalStateReconciled: false,
		Statement:               recoveryStatement,
	}

	currentVersion, err := st.CurrentVersion(ctx)
	if err != nil {
		return nil, err
	}
	report.CurrentCommittedVersion = currentVersion

	var committed *store.CommittedVersion
	var snapshot *spec.Snapshot
	if currentVersion > 0 {
		row, err := st.LoadCommittedVersion(ctx, currentVersion)
		if err == nil {
			committed = &row
			var snap spec.Snapshot
			if err := json.Unmarshal([]byte(row.SnapshotJSON), &snap); err == nil {
				snapshot = &snap
			}
		}
	}

	for _, intent := range open {
		recovered := RecoveredIntent{
			ID:        intent.ID,
			Type:      intent.Type,
			Version:   intent.Version,
			BatchID:   intent.BatchID,
			ObjectID:  intent.ObjectID,
			OldStatus: intent.Status,
		}
		switch intent.Type {
		case "write_object":
			recovered.NewStatus = "pending_write"
			recovered.Action = "uncommitted_local_write_reset_to_pending_write"
			if intent.Status != "pending_write" {
				if err := st.SetIntentStatus(ctx, intent.ID, "pending_write", "process terminated; local intent reset to pending_write"); err != nil {
					return nil, err
				}
			}
			if snapshot != nil && intent.Version == snapshot.Version && intent.ObjectID != "" {
				if object, ok := spec.ObjectByID(snapshot.Objects)[intent.ObjectID]; ok {
					driftReport := drift.Evaluate(object, snapshot.Policy)
					recoveryObject := RecoveryObject{
						Version:                        intent.Version,
						ObjectID:                       intent.ObjectID,
						FinalStatusBasedOnDurableFacts: "pending_write",
						Attempts:                       0,
						ErrorCode:                      errs.Interrupted,
						Differences:                    driftReport.Differences,
					}
					if recoveryObject.Differences == nil {
						recoveryObject.Differences = []drift.Difference{}
					}
					report.Objects = append(report.Objects, recoveryObject)
					state := ObjectState{
						ObjectID:        intent.ObjectID,
						Phase:           PhasePendingWrite,
						PrimaryCategory: driftReport.PrimaryCategory,
						HitCategories:   driftReport.Categories,
						Attempts:        0,
						MaxRetries:      snapshot.Policy.MaxRetries,
						LastErrorCode:   errs.Interrupted,
						LastMessage:     "process terminated; local intent reset to pending_write",
						Differences:     driftReport.Differences,
						FieldDigest:     digestFields(object.Desired),
						IntentID:        intent.ID,
						UpdatedAt:       report.RecoveredAt,
					}
					raw, _ := json.Marshal(state)
					if err := st.PutObjectState(ctx, intent.Version, intent.ObjectID, string(raw)); err != nil {
						return nil, err
					}
				}
			}
		case "snapshot_switch":
			recovered.NewStatus = "aborted"
			recovered.Action = "snapshot_switch_marked_aborted_old_committed_snapshot_remains_current"
			if err := st.SetIntentStatus(ctx, intent.ID, "aborted", "process terminated before atomic version commit"); err != nil {
				return nil, err
			}
		default:
			recovered.NewStatus = "aborted"
			recovered.Action = "interrupted_batch_organization_marked_aborted"
			if err := st.SetIntentStatus(ctx, intent.ID, "aborted", "process terminated during batch organization or execution"); err != nil {
				return nil, err
			}
		}
		report.RecoveredIntents = append(report.RecoveredIntents, recovered)
	}

	raw, err := json.Marshal(report)
	if err != nil {
		return nil, err
	}
	if err := st.SaveRecoveryReport(ctx, string(raw)); err != nil {
		return nil, err
	}
	_ = committed
	return report, nil
}
