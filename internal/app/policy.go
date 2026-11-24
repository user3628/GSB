package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"

	"preprocesspipeline/internal/domain"
	"preprocesspipeline/internal/store"
)

type policyRebuild struct {
	batch  store.BatchRecord
	result *domain.PipelineResult
	shards []store.ShardRecord
}

func (s *Service) EnsureDefaultPolicy(ctx context.Context) (*domain.Policy, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	version, err := s.store.CurrentPolicyVersion(ctx)
	if err != nil {
		return nil, err
	}
	if version > 0 {
		return s.store.CurrentPolicy(ctx)
	}
	policy := domain.DefaultPolicy()
	return s.commitPolicyLocked(ctx, policy, "")
}

func (s *Service) SubmitFullPolicy(ctx context.Context, policy domain.Policy, inject string) (*domain.Policy, error) {
	inject = normalizeInject(inject)
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	current, err := s.store.CurrentPolicyVersion(ctx)
	if err != nil {
		return nil, err
	}
	if policy.Version <= current {
		return nil, domain.NewError(domain.CodeStaleSnapshot,
			"submitted policy version is not greater than current version",
			map[string]any{"submitted_version": policy.Version, "current_version": current})
	}
	return s.commitPolicyLocked(ctx, policy, inject)
}

func (s *Service) SubmitPolicyPatch(ctx context.Context, patch domain.PolicyPatch, inject ...string) (*domain.Policy, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	current, err := s.store.CurrentPolicy(ctx)
	if err != nil {
		return nil, err
	}
	if patch.BaseVersion == nil || *patch.BaseVersion != current.Version {
		base := int64(-1)
		if patch.BaseVersion != nil {
			base = *patch.BaseVersion
		}
		return nil, domain.NewError(domain.CodeStaleBase,
			"patch base_version does not match current policy version",
			map[string]any{"base_version": base, "current_version": current.Version})
	}
	if patch.Version > 0 && patch.Version <= current.Version {
		return nil, domain.NewError(domain.CodeStaleSnapshot,
			"patched policy version is not greater than current version",
			map[string]any{"submitted_version": patch.Version, "current_version": current.Version})
	}
	next := patch.Apply(*current)
	if err := next.Validate(); err != nil {
		return nil, err
	}
	fault := ""
	if len(inject) > 0 {
		fault = normalizeInject(inject[0])
	}
	return s.commitPolicyLocked(ctx, next, fault)
}

func (s *Service) commitPolicyLocked(ctx context.Context, policy domain.Policy, inject string) (*domain.Policy, error) {
	intentID := newID("policy-intent")
	tempRel := filepath.ToSlash(filepath.Join("tmp", intentID))
	tempAbs := filepath.Join(s.dataDir, filepath.FromSlash(tempRel))
	if err := os.MkdirAll(tempAbs, 0o755); err != nil {
		return nil, domain.NewError(domain.CodeInternal, "create policy temp directory: "+err.Error())
	}
	plan := IntentPlan{TempDir: tempRel, ShardCount: policy.Sharding.Count, Fault: inject}
	planJSON, _ := json.Marshal(plan)
	if err := s.store.CreateIntent(ctx, store.Intent{
		IntentID:      intentID,
		BatchID:       "__policy__",
		PolicyVersion: policy.Version,
		Kind:          "policy_commit",
		Status:        "running",
		PlanJSON:      string(planJSON),
	}); err != nil {
		return nil, err
	}

	batches, err := s.store.ListCommittedBatches(ctx)
	if err != nil {
		return nil, s.failExecution(ctx, intentID, "__policy__", tempAbs, err)
	}
	rebuilds := make([]policyRebuild, 0, len(batches))
	allPaths := make([]string, 0)
	affected := make([]string, 0)
	for _, batch := range batches {
		paths := plannedBatchArtifactPaths(intentID, batch.BatchID, policy.Version, policy.Sharding.Count)
		allPaths = append(allPaths, paths...)
		for shardIndex := 0; shardIndex < policy.Sharding.Count; shardIndex++ {
			affected = append(affected, batch.BatchID+":p"+itoa(policy.Version)+":shard-"+itoa(int64(shardIndex)))
		}
	}
	plan.ArtifactPaths = allPaths
	plan.AffectedShards = affected
	planJSON, _ = json.Marshal(plan)
	if err := s.store.UpdateIntentPlan(ctx, intentID, string(planJSON)); err != nil {
		return nil, err
	}
	for _, batch := range batches {
		corruptShard := -1
		if inject == "digest_mismatch" {
			corruptShard = 0
		}
		result, err := domain.RunAllStages(batch.RawInput, policy, corruptShard)
		if err != nil {
			_ = s.finishRejected(ctx, intentID, err)
			_ = os.RemoveAll(tempAbs)
			return nil, err
		}
		batchTemp := filepath.Join(tempAbs, batch.BatchID)
		if err := os.MkdirAll(batchTemp, 0o755); err != nil {
			return nil, s.failExecution(ctx, intentID, "__policy__", tempAbs, err)
		}
		tempPaths, err := writeArtifactsToTemp(batchTemp, result.Artifacts)
		if err != nil {
			return nil, s.failExecution(ctx, intentID, "__policy__", tempAbs, err)
		}
		tensorOut := &domain.TensorOutput{Artifacts: result.Artifacts, ShardStats: result.ShardStats}
		_, shardRecords, err := s.publishArtifacts(intentID, batch.BatchID, policy.Version, tensorOut, tempPaths)
		if err != nil {
			return nil, s.failExecution(ctx, intentID, "__policy__", tempAbs, err)
		}
		for _, rec := range shardRecords {
			affected = append(affected, rec.BatchID+":p"+itoa(rec.PolicyVersion)+":shard-"+itoa(int64(rec.ShardIndex)))
		}
		rebuilds = append(rebuilds, policyRebuild{batch: batch, result: result, shards: shardRecords})
	}

	err = s.store.WithTx(ctx, func(tx *sql.Tx) error {
		if err := store.InsertPolicyTx(ctx, tx, policy); err != nil {
			return err
		}
		for _, rebuild := range rebuilds {
			for seq, stage := range rebuild.result.Stages {
				if err := store.InsertStageSummaryTx(ctx, tx, rebuild.batch.BatchID, policy.Version, seq+1, stage); err != nil {
					return err
				}
			}
			if _, err := store.InvalidateOlderShardsTx(ctx, tx, rebuild.batch.BatchID, policy.Version); err != nil {
				return err
			}
			for _, shard := range rebuild.shards {
				if err := store.UpsertShardTx(ctx, tx, shard); err != nil {
					return err
				}
			}
			response := BatchResponse{
				BatchID:       rebuild.batch.BatchID,
				Status:        "committed",
				PolicyVersion: policy.Version,
				RawDigest:     rebuild.result.RawDigest,
				Stages:        rebuild.result.Stages,
				Shards:        rebuild.shards,
				Warnings:      rebuild.result.Warnings,
			}
			resultJSON, _ := json.Marshal(response)
			if _, err := tx.ExecContext(ctx, `
UPDATE batches SET policy_version=?, status='committed', result_json=?, error_code='', completed_at=?
WHERE batch_id=?`, policy.Version, string(resultJSON), nowISO(), rebuild.batch.BatchID); err != nil {
				return err
			}
		}
		_, err := tx.ExecContext(ctx,
			`UPDATE intents SET status='committed', boundary_stage=?, error_code='', report_json='', updated_at=? WHERE intent_id=?`,
			domain.StageTensorize, nowISO(), intentID)
		return err
	})
	if err != nil {
		return nil, s.failExecution(ctx, intentID, "__policy__", tempAbs,
			domain.NewError(domain.CodeInternal, "commit policy rebuild transaction failed: "+err.Error()))
	}
	_ = os.RemoveAll(tempAbs)
	return &policy, nil
}

func itoa(v int64) string {
	return strconv.FormatInt(v, 10)
}
