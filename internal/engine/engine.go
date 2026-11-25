package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"

	"featureplatform/internal/configx"
	"featureplatform/internal/featurex"
	"featureplatform/internal/model"
	"featureplatform/internal/store"
)

type Engine struct {
	Store *store.Store
}

func Open(dbPath string) (*Engine, error) {
	st, err := store.Open(dbPath)
	if err != nil {
		return nil, err
	}
	return &Engine{Store: st}, nil
}

func (e *Engine) Close() error { return e.Store.Close() }

func (e *Engine) CommitInitialConfig(ctx context.Context, cfg configx.Config) (string, error) {
	current, _, err := e.Store.CurrentVersion(ctx)
	if err != nil {
		return "", err
	}
	if current != 0 {
		return "", &model.CodedError{Code: model.CodeStaleVersion, Message: "initial config can only be committed to an empty version history", CurrentVersion: current}
	}
	cfg.ConfigVersion = 1
	return e.Store.CommitConfig(ctx, store.ConfigCommit{Config: cfg})
}

func (e *Engine) SubmitFragment(ctx context.Context, fragment configx.Fragment) (configx.Config, configx.InvalidationPlan, error) {
	current, err := e.Store.LoadConfig(ctx)
	if err != nil {
		return configx.Config{}, configx.InvalidationPlan{}, err
	}
	next, err := configx.ApplyFragment(current, fragment)
	if err != nil {
		return configx.Config{}, configx.InvalidationPlan{}, err
	}
	plan := configx.PlanFragment(current, next, fragment)
	_, err = e.Store.CommitConfig(ctx, store.ConfigCommit{Config: next, Fragment: &fragment, Plan: &plan})
	if err != nil {
		return configx.Config{}, configx.InvalidationPlan{}, err
	}
	return next, plan, nil
}

type AffectedUnit struct {
	Stored         store.StoredSample
	Reason         string
	EarliestStage  string
	CurrentValidID int64
}

type MigrationPlan struct {
	Config             configx.Config
	Units              []AffectedUnit
	ReusedCount        int
	ForceFullRebuild   bool
	PendingFullVersion int64
}

func (e *Engine) PlanMigration(ctx context.Context) (*MigrationPlan, error) {
	cfg, err := e.Store.LoadConfig(ctx)
	if err != nil {
		return nil, err
	}
	samples, err := e.Store.ListSamples(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(samples, func(i, j int) bool {
		if samples[i].Sample.Task != samples[j].Sample.Task {
			return samples[i].Sample.Task < samples[j].Sample.Task
		}
		return samples[i].TaskSequence < samples[j].TaskSequence
	})
	current, _, err := e.Store.CurrentVersion(ctx)
	if err != nil {
		return nil, err
	}
	pendingRaw, err := e.Store.GetMeta(ctx, "pending_full_rebuild_version")
	if err != nil {
		return nil, err
	}
	pendingVersion, _ := strconv.ParseInt(pendingRaw, 10, 64)
	forceFull := pendingVersion == current && pendingVersion > 0

	plan := &MigrationPlan{Config: cfg, ForceFullRebuild: forceFull, PendingFullVersion: pendingVersion}
	for _, stored := range samples {
		unit := AffectedUnit{Stored: stored}
		if forceFull {
			unit.Reason = "pending_full_rebuild"
			unit.EarliestStage = featurex.StageNormalize
			plan.Units = append(plan.Units, unit)
			continue
		}

		row, err := e.Store.GetValidRecord(ctx, stored.Sample.SampleID)
		if err != nil {
			return nil, err
		}
		if row == nil {
			unit.Reason = "missing_record"
			unit.EarliestStage = featurex.StageNormalize
			plan.Units = append(plan.Units, unit)
			continue
		}
		unit.CurrentValidID = row.ValidID
		d, digestErr := row.Record.Digest()
		if digestErr != nil || d != row.Record.RecordDigest {
			unit.Reason = "record_digest_mismatch"
			unit.EarliestStage = featurex.StageRecord
			plan.Units = append(plan.Units, unit)
			continue
		}

		affected := false
		for _, stage := range featurex.RequiredStages() {
			cp, err := e.Store.GetCheckpoint(ctx, stage, stored.Sample.SampleID)
			if err != nil {
				return nil, err
			}
			if cp == nil {
				unit.Reason = "missing_checkpoint:" + stage
				unit.EarliestStage = stage
				affected = true
				break
			}
			expected, err := featurex.StageDigest(row.Record, stage)
			if err != nil || cp.ArtifactDigest != expected {
				unit.Reason = "checkpoint_digest_mismatch:" + stage
				unit.EarliestStage = stage
				affected = true
				break
			}
			if stage == featurex.StageNormalize && cp.ContentFingerprint != stored.ContentFingerprint {
				unit.Reason = "sample_content_changed"
				unit.EarliestStage = featurex.StageNormalize
				affected = true
				break
			}
			if stage == featurex.StageNormalize || stage == featurex.StageLabel || stage == featurex.StageFeatureMap || stage == featurex.StageCoverage {
				expectedFP := cfg.ScopedStageFingerprint(stored.Sample.Task, stage)
				if cp.StageFingerprint != expectedFP {
					unit.Reason = "config_dependency_changed:" + stage
					unit.EarliestStage = stage
					affected = true
					break
				}
			}
		}
		if affected {
			plan.Units = append(plan.Units, unit)
		} else {
			plan.ReusedCount++
		}
	}
	return plan, nil
}

type RunOptions struct {
	Budget                    int    `json:"budget,omitempty"`
	TargetStage               string `json:"target_stage,omitempty"`
	HangAfterCheckpointWrites int    `json:"hang_after_checkpoint_writes,omitempty"`
	ReadyFile                 string `json:"ready_file,omitempty"`
}

type RunReport struct {
	CurrentVersion     int64    `json:"current_version"`
	Budget             int      `json:"budget"`
	TargetStage        string   `json:"target_stage"`
	TotalSamples       int      `json:"total_samples"`
	AffectedCount      int      `json:"affected_count"`
	SelectedCount      int      `json:"selected_count"`
	RebuiltCount       int      `json:"rebuilt_count"`
	ReusedCount        int      `json:"reused_count"`
	DeferredCount      int      `json:"deferred_count"`
	HasMore            bool     `json:"has_more"`
	CheckpointWrites   int      `json:"checkpoint_writes"`
	ProcessedSampleIDs []string `json:"processed_sample_ids"`
	ProcessedTasks     []string `json:"processed_tasks"`
	MigratableCount    int      `json:"migratable_count"`
	RejectedCount      int      `json:"rejected_count"`
	ForcedFullRebuild  bool     `json:"forced_full_rebuild"`
	Stopped            bool     `json:"stopped,omitempty"`
}

var ErrGracefulStop = errors.New("graceful stop requested")

func (e *Engine) RunMigration(ctx context.Context, opts RunOptions) (*RunReport, error) {
	plan, err := e.PlanMigration(ctx)
	if err != nil {
		return nil, err
	}
	cfg := plan.Config
	budget := opts.Budget
	if budget <= 0 {
		budget = cfg.BatchBudget
	}
	if budget <= 0 {
		budget = len(plan.Units)
	}
	targetStage := opts.TargetStage
	if targetStage == "" {
		targetStage = featurex.StageRecord
	}
	validStage := false
	for _, stage := range featurex.RequiredStages() {
		if stage == targetStage {
			validStage = true
		}
	}
	if !validStage {
		return nil, &model.CodedError{Code: "E_INVALID_STAGE", Message: "unknown target stage: " + targetStage}
	}
	selected := plan.Units
	if len(selected) > budget {
		selected = selected[:budget]
	}
	report := &RunReport{
		CurrentVersion:     cfg.ConfigVersion,
		Budget:             budget,
		TargetStage:        targetStage,
		TotalSamples:       plan.ReusedCount + len(plan.Units),
		AffectedCount:      len(plan.Units),
		SelectedCount:      len(selected),
		ReusedCount:        plan.ReusedCount,
		DeferredCount:      len(plan.Units) - len(selected),
		ForcedFullRebuild:  plan.ForceFullRebuild,
		ProcessedSampleIDs: []string{},
		ProcessedTasks:     []string{},
	}

	readyWritten := false
	hangIfRequested := func() error {
		if opts.HangAfterCheckpointWrites <= 0 || report.CheckpointWrites < opts.HangAfterCheckpointWrites {
			return nil
		}
		if !readyWritten && opts.ReadyFile != "" {
			if err := os.WriteFile(opts.ReadyFile, []byte(fmt.Sprintf(`{"checkpoint_writes":%d}`+"\n", report.CheckpointWrites)), 0o644); err != nil {
				return err
			}
			readyWritten = true
		}
		<-ctx.Done()
		report.Stopped = true
		return ctx.Err()
	}

	taskSeen := map[string]struct{}{}
	for _, unit := range selected {
		if err := ctx.Err(); err != nil {
			report.Stopped = true
			return report, ErrGracefulStop
		}
		stored := unit.Stored
		rec, _ := featurex.BuildRecord(stored.Sample, cfg, stored.TaskSequence)
		reachedRecord := false
		for _, stage := range featurex.RequiredStages() {
			if stage == featurex.StageRecord {
				commitResult, err := e.Store.CommitRecord(ctx, store.RecordCommit{
					Record:             rec,
					Task:               stored.Sample.Task,
					TaskSequence:       stored.TaskSequence,
					ExpectedValidID:    unit.CurrentValidID,
					ContentFingerprint: stored.ContentFingerprint,
				})
				if err != nil {
					return report, err
				}
				_ = commitResult
				report.CheckpointWrites++
				report.RebuiltCount++
				reachedRecord = true
			} else {
				digest, err := featurex.StageDigest(rec, stage)
				if err != nil {
					return report, err
				}
				_, err = e.Store.SaveCheckpoint(ctx, store.Checkpoint{
					Stage:              stage,
					UnitID:             stored.Sample.SampleID,
					SampleID:           stored.Sample.SampleID,
					ContentFingerprint: stored.ContentFingerprint,
					ConfigVersion:      cfg.ConfigVersion,
					ArtifactDigest:     digest,
					StageFingerprint:   cfg.ScopedStageFingerprint(stored.Sample.Task, stage),
				})
				if err != nil {
					return report, err
				}
				report.CheckpointWrites++
			}
			if err := hangIfRequested(); err != nil {
				return report, nil
			}
			if stage == targetStage {
				break
			}
		}
		report.ProcessedSampleIDs = append(report.ProcessedSampleIDs, stored.Sample.SampleID)
		if _, ok := taskSeen[stored.Sample.Task]; !ok {
			taskSeen[stored.Sample.Task] = struct{}{}
			report.ProcessedTasks = append(report.ProcessedTasks, stored.Sample.Task)
		}
		if reachedRecord {
			if rec.Status == "rejected" {
				report.RejectedCount++
			} else {
				report.MigratableCount++
			}
		}
	}

	report.HasMore = report.DeferredCount > 0 || (targetStage != featurex.StageRecord && report.SelectedCount > 0)
	if report.DeferredCount == 0 && targetStage == featurex.StageRecord && plan.ForceFullRebuild {
		if err := e.Store.SetMeta(ctx, "pending_full_rebuild_version", ""); err != nil {
			return report, err
		}
	}
	sort.Strings(report.ProcessedTasks)
	return report, nil
}

type CanonicalExport struct {
	SchemaVersion string         `json:"schema_version"`
	Records       []model.Record `json:"records"`
}

func (e *Engine) ExportCanonical(ctx context.Context) ([]byte, string, int, error) {
	rows, err := e.Store.ListValidRecordsSorted(ctx)
	if err != nil {
		return nil, "", 0, err
	}
	records := make([]model.Record, 0, len(rows))
	for _, row := range rows {
		records = append(records, row.Record)
	}
	export := CanonicalExport{SchemaVersion: "canonical-export/v1", Records: records}
	b, err := model.CanonicalJSON(export)
	if err != nil {
		return nil, "", 0, err
	}
	return b, model.DigestBytes("canonical-export/v1:", b), len(records), nil
}
