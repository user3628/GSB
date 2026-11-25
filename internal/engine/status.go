package engine

import (
	"context"
	"strconv"

	"featureplatform/internal/configx"
)

type Status struct {
	CurrentVersion           int64                  `json:"current_version"`
	MinimumAcceptableVersion int64                  `json:"minimum_acceptable_version"`
	ConfigFingerprints       configx.FingerprintSet `json:"config_fingerprints"`
	CheckpointCounts         map[string]int64       `json:"checkpoint_counts"`
	TotalCheckpoints         int64                  `json:"total_checkpoints"`
	SampleCount              int                    `json:"sample_count"`
	ValidRecordCount         int                    `json:"valid_record_count"`
	PendingFullRebuild       bool                   `json:"pending_full_rebuild"`
	AffectedCount            int                    `json:"affected_count"`
	ReusedCount              int                    `json:"reused_count"`
	AffectedUnits            []AffectedUnit         `json:"affected_units"`
}

func (e *Engine) Status(ctx context.Context, includeUnits bool) (*Status, error) {
	current, minAccepted, err := e.Store.CurrentVersion(ctx)
	if err != nil {
		return nil, err
	}
	counts, totalCP, err := e.Store.CheckpointCountsByStage(ctx)
	if err != nil {
		return nil, err
	}
	samples, err := e.Store.ListSamples(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := e.Store.ListValidRecordsSorted(ctx)
	if err != nil {
		return nil, err
	}
	status := &Status{
		CurrentVersion: current, MinimumAcceptableVersion: minAccepted,
		CheckpointCounts: counts, TotalCheckpoints: totalCP,
		SampleCount: len(samples), ValidRecordCount: len(rows),
		AffectedUnits: []AffectedUnit{},
	}
	if current > 0 {
		cfg, err := e.Store.LoadConfig(ctx)
		if err != nil {
			return nil, err
		}
		status.ConfigFingerprints = cfg.Fingerprints()
		pending, err := e.Store.GetMeta(ctx, "pending_full_rebuild_version")
		if err != nil {
			return nil, err
		}
		status.PendingFullRebuild = pending == strconv.FormatInt(current, 10)
		plan, err := e.PlanMigration(ctx)
		if err != nil {
			return nil, err
		}
		status.AffectedCount = len(plan.Units)
		status.ReusedCount = plan.ReusedCount
		status.PendingFullRebuild = plan.ForceFullRebuild
		if includeUnits {
			status.AffectedUnits = plan.Units
		}
	}
	return status, nil
}
