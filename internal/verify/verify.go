package verify

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"preprocesspipeline/internal/domain"
	"preprocesspipeline/internal/store"
)

type Report struct {
	BatchID        string          `json:"batch_id"`
	PolicyVersion  int64           `json:"policy_version"`
	OK             bool            `json:"ok"`
	CheckedStages  int             `json:"checked_stages"`
	CheckedShards  int             `json:"checked_shards"`
	Mismatches     []Mismatch      `json:"mismatches"`
	ArtifactChecks []ArtifactCheck `json:"artifact_checks,omitempty"`
}

type Mismatch struct {
	Stage                string `json:"stage"`
	Reason               string `json:"reason"`
	ExpectedOutputDigest string `json:"expected_output_digest,omitempty"`
	ActualOutputDigest   string `json:"actual_output_digest,omitempty"`
	ExpectedInputDigest  string `json:"expected_input_digest,omitempty"`
	ActualInputDigest    string `json:"actual_input_digest,omitempty"`
	Detail               any    `json:"detail,omitempty"`
}

type ArtifactCheck struct {
	ShardIndex     int    `json:"shard_index"`
	Path           string `json:"path"`
	ExpectedDigest string `json:"expected_digest"`
	ActualDigest   string `json:"actual_digest"`
	OK             bool   `json:"ok"`
	Error          string `json:"error,omitempty"`
}

func VerifyBatch(ctx context.Context, st *store.Store, batchID string, policyVersion int64) (*Report, error) {
	batch, err := st.GetBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	if policyVersion == 0 {
		policyVersion = batch.PolicyVersion
	}
	policy, err := st.GetPolicy(ctx, policyVersion)
	if err != nil {
		return nil, err
	}
	persisted, err := st.ListStageSummaries(ctx, batchID, policyVersion)
	if err != nil {
		return nil, err
	}
	persistedByStage := make(map[string]store.StageSummaryRecord, len(persisted))
	for _, stage := range persisted {
		persistedByStage[stage.Stage] = stage
	}

	recomputed, err := domain.RunAllStages(batch.RawInput, *policy, -1)
	if err != nil {
		return nil, err
	}
	report := &Report{
		BatchID:       batchID,
		PolicyVersion: policyVersion,
		OK:            true,
		Mismatches:    []Mismatch{},
	}

	for _, actualStage := range recomputed.Stages {
		expected, exists := persistedByStage[actualStage.Stage]
		if !exists {
			report.OK = false
			report.Mismatches = append(report.Mismatches, Mismatch{
				Stage: actualStage.Stage, Reason: "persisted stage summary missing",
				ActualOutputDigest: actualStage.OutputDigest,
			})
			continue
		}
		report.CheckedStages++
		mismatch := Mismatch{Stage: actualStage.Stage}
		failed := false
		if expected.OutputDigest != actualStage.OutputDigest {
			failed = true
			mismatch.Reason = "output digest mismatch"
			mismatch.ExpectedOutputDigest = expected.OutputDigest
			mismatch.ActualOutputDigest = actualStage.OutputDigest
		}
		if expected.InputDigest != actualStage.InputDigest {
			failed = true
			mismatch.Reason = joinReason(mismatch.Reason, "input digest mismatch")
			mismatch.ExpectedInputDigest = expected.InputDigest
			mismatch.ActualInputDigest = actualStage.InputDigest
		}
		var expectedStats map[string]any
		roundTripStats(actualStage.Stats, &expectedStats)
		if !reflect.DeepEqual(expected.Stats, expectedStats) {
			failed = true
			mismatch.Reason = joinReason(mismatch.Reason, "stats mismatch")
			mismatch.Detail = map[string]any{"expected_stats": expected.Stats, "actual_stats": expectedStats}
		}
		if failed {
			report.OK = false
			if mismatch.ExpectedOutputDigest == "" {
				mismatch.ExpectedOutputDigest = expected.OutputDigest
				mismatch.ActualOutputDigest = actualStage.OutputDigest
			}
			report.Mismatches = append(report.Mismatches, mismatch)
		}
	}

	shards, err := st.ListShards(ctx, batchID, policyVersion, false)
	if err != nil {
		return nil, err
	}
	actualByShard := make(map[int]domain.TensorArtifact, len(recomputed.Artifacts))
	for _, artifact := range recomputed.Artifacts {
		actualByShard[artifact.ShardIndex] = artifact
	}
	for _, shard := range shards {
		report.CheckedShards++
		check := ArtifactCheck{ShardIndex: shard.ShardIndex, Path: shard.ArtifactPath, ExpectedDigest: shard.ArtifactDigest}
		absPath := filepath.Join(st.DataDir, filepath.FromSlash(shard.ArtifactPath))
		data, readErr := os.ReadFile(absPath)
		if readErr != nil {
			check.Error = readErr.Error()
		} else {
			check.ActualDigest = domain.DigestBytes(data)
			if check.ActualDigest != shard.ArtifactDigest {
				check.Error = "file digest differs from SQLite shard index"
			}
			header, _, parseErr := domain.ParseTensorArtifact(data)
			if parseErr != nil {
				check.Error = joinReason(check.Error, parseErr.Error())
			} else if header.SampleCount != shard.SampleCount || header.NullCount != shard.NullCount {
				check.Error = joinReason(check.Error, fmt.Sprintf("header counts mismatch: db=(%d,%d) file=(%d,%d)",
					shard.SampleCount, shard.NullCount, header.SampleCount, header.NullCount))
			}
			if recomputedArtifact, ok := actualByShard[shard.ShardIndex]; ok {
				if recomputedArtifact.Digest != shard.ArtifactDigest {
					check.Error = joinReason(check.Error, "recomputed tensor digest differs")
					if check.ActualDigest == "" {
						check.ActualDigest = recomputedArtifact.Digest
					}
				}
			}
		}
		check.OK = check.Error == ""
		if !check.OK {
			report.OK = false
			report.Mismatches = append(report.Mismatches, Mismatch{
				Stage: domain.StageTensorize, Reason: "artifact verification failed: " + check.Error,
				ExpectedOutputDigest: shard.ArtifactDigest, ActualOutputDigest: check.ActualDigest,
				Detail: check,
			})
		}
		report.ArtifactChecks = append(report.ArtifactChecks, check)
	}
	return report, nil
}

func roundTripStats(in map[string]any, out *map[string]any) {
	data, _ := json.Marshal(in)
	_ = json.Unmarshal(data, out)
}

func joinReason(a, b string) string {
	if a == "" {
		return b
	}
	return a + "; " + b
}
