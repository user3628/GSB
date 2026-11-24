package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"

	"preprocesspipeline/internal/domain"
)

type RecoveryReport struct {
	RecoveryID       int64             `json:"recovery_id,omitempty"`
	Trigger          string            `json:"trigger"`
	StartedAt        string            `json:"started_at"`
	FinishedAt       string            `json:"finished_at"`
	DataDir          string            `json:"data_dir"`
	UnfinishedCount  int               `json:"unfinished_count"`
	RecoveredIntents []RecoveredIntent `json:"recovered_intents"`
}

type RecoveredIntent struct {
	IntentID          string      `json:"intent_id"`
	BatchID           string      `json:"batch_id"`
	Kind              string      `json:"kind"`
	PolicyVersion     int64       `json:"policy_version"`
	LastCompleteStage string      `json:"last_complete_stage"`
	Action            string      `json:"action"`
	ErrorCode         string      `json:"error_code,omitempty"`
	TempDir           *PathFact   `json:"temp_dir,omitempty"`
	RemovedArtifacts  []*PathFact `json:"removed_artifacts,omitempty"`
	ReadyShards       []int       `json:"ready_shards"`
	ShardsToRebuild   []int       `json:"shards_to_rebuild"`
}

type PathFact struct {
	Path    string `json:"path"`
	Existed bool   `json:"existed"`
	Removed bool   `json:"removed"`
	Error   string `json:"error,omitempty"`
}

func (s *Service) Recover(ctx context.Context, trigger string) (*RecoveryReport, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	started := nowISO()
	intents, err := s.store.ListRunningIntents(ctx)
	if err != nil {
		return nil, err
	}
	report := &RecoveryReport{
		Trigger:          trigger,
		StartedAt:        started,
		FinishedAt:       nowISO(),
		DataDir:          s.dataDir,
		UnfinishedCount:  len(intents),
		RecoveredIntents: make([]RecoveredIntent, 0, len(intents)),
	}

	for _, intent := range intents {
		var plan IntentPlan
		_ = json.Unmarshal([]byte(intent.PlanJSON), &plan)
		if plan.TempDir == "" {
			plan.TempDir = filepath.ToSlash(filepath.Join("tmp", intent.IntentID))
		}
		recovered := RecoveredIntent{
			IntentID:          intent.IntentID,
			BatchID:           intent.BatchID,
			Kind:              intent.Kind,
			PolicyVersion:     intent.PolicyVersion,
			LastCompleteStage: intent.BoundaryStage,
			Action:            "aborted_at_last_complete_boundary",
			ReadyShards:       []int{},
			ShardsToRebuild:   []int{},
		}

		tempAbs := filepath.Join(s.dataDir, filepath.FromSlash(plan.TempDir))
		recovered.TempDir = removePathFact(plan.TempDir, tempAbs, true)
		for _, relPath := range plan.ArtifactPaths {
			absPath := filepath.Join(s.dataDir, filepath.FromSlash(relPath))
			fact := removePathFact(relPath, absPath, false)
			if fact.Existed || fact.Error != "" {
				recovered.RemovedArtifacts = append(recovered.RemovedArtifacts, fact)
			}
		}

		readySet := make(map[int]struct{})
		if intent.BatchID != "__policy__" && intent.PolicyVersion > 0 {
			shards, err := s.store.ListShards(ctx, intent.BatchID, intent.PolicyVersion, false)
			if err != nil {
				return nil, err
			}
			for _, shard := range shards {
				readySet[shard.ShardIndex] = struct{}{}
				recovered.ReadyShards = append(recovered.ReadyShards, shard.ShardIndex)
			}
		}
		for index := 0; index < plan.ShardCount; index++ {
			if _, ready := readySet[index]; !ready {
				recovered.ShardsToRebuild = append(recovered.ShardsToRebuild, index)
			}
		}

		itemJSON, _ := json.Marshal(recovered)
		err = s.store.WithTx(ctx, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx,
				`UPDATE intents SET status='aborted', error_code=?, report_json=?, updated_at=? WHERE intent_id=?`,
				domain.CodeStageInterrupted, string(itemJSON), nowISO(), intent.IntentID); err != nil {
				return err
			}
			if intent.BatchID == "__policy__" {
				return nil
			}
			_, err := tx.ExecContext(ctx,
				`UPDATE batches SET status='aborted', error_code=?, completed_at=? WHERE batch_id=? AND status='running'`,
				domain.CodeStageInterrupted, nowISO(), intent.BatchID)
			return err
		})
		if err != nil {
			return nil, err
		}
		report.RecoveredIntents = append(report.RecoveredIntents, recovered)
	}

	report.FinishedAt = nowISO()
	reportJSON, _ := json.Marshal(report)
	recoveryID, err := s.store.SaveRecoveryRun(ctx, trigger, string(reportJSON))
	if err != nil {
		return nil, err
	}
	report.RecoveryID = recoveryID
	return report, nil
}

func removePathFact(relPath, absPath string, recursive bool) *PathFact {
	_, statErr := os.Stat(absPath)
	fact := &PathFact{Path: filepath.ToSlash(relPath), Existed: statErr == nil}
	if statErr != nil {
		if !os.IsNotExist(statErr) {
			fact.Error = statErr.Error()
		}
		return fact
	}
	var err error
	if recursive {
		err = os.RemoveAll(absPath)
	} else {
		err = os.Remove(absPath)
	}
	if err != nil {
		fact.Error = err.Error()
	} else {
		fact.Removed = true
	}
	return fact
}

func (s *Service) LatestRecoveryReport(ctx context.Context) (*RecoveryReport, error) {
	id, payload, err := s.store.LatestRecoveryReport(ctx)
	if err != nil {
		return nil, err
	}
	var report RecoveryReport
	if err := json.Unmarshal([]byte(payload), &report); err != nil {
		return nil, err
	}
	report.RecoveryID = id
	return &report, nil
}
