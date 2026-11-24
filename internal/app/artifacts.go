package app

import (
	"fmt"
	"os"
	"path/filepath"

	"preprocesspipeline/internal/domain"
	"preprocesspipeline/internal/store"
)

func artifactRelPath(intentID, batchID string, policyVersion int64, shardIndex int) string {
	return filepath.ToSlash(filepath.Join(
		"objects", "batches", batchID,
		fmt.Sprintf("p%d", policyVersion), intentID,
		fmt.Sprintf("shard-%03d.tensor", shardIndex),
	))
}

func plannedBatchArtifactPaths(intentID, batchID string, policyVersion int64, shardCount int) []string {
	paths := make([]string, 0, shardCount)
	for shardIndex := 0; shardIndex < shardCount; shardIndex++ {
		paths = append(paths, artifactRelPath(intentID, batchID, policyVersion, shardIndex))
	}
	return paths
}

func writeArtifactsToTemp(tempAbs string, artifacts []domain.TensorArtifact) (map[int]string, error) {
	paths := make(map[int]string, len(artifacts))
	for _, artifact := range artifacts {
		if err := artifact.Verify(); err != nil {
			return nil, err
		}
		tempPath := filepath.Join(tempAbs, fmt.Sprintf("shard-%03d.tensor", artifact.ShardIndex))
		file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return nil, domain.NewError(domain.CodeInternal, "create temp shard: "+err.Error())
		}
		if _, err := file.Write(artifact.Bytes); err != nil {
			_ = file.Close()
			return nil, domain.NewError(domain.CodeInternal, "write temp shard: "+err.Error())
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return nil, domain.NewError(domain.CodeInternal, "sync temp shard: "+err.Error())
		}
		if err := file.Close(); err != nil {
			return nil, domain.NewError(domain.CodeInternal, "close temp shard: "+err.Error())
		}
		paths[artifact.ShardIndex] = tempPath
	}
	return paths, nil
}

func (s *Service) publishArtifacts(
	intentID string,
	batchID string,
	policyVersion int64,
	tensorOut *domain.TensorOutput,
	tempPaths map[int]string,
) ([]string, []store.ShardRecord, error) {
	relPaths := make([]string, 0, len(tensorOut.Artifacts))
	records := make([]store.ShardRecord, 0, len(tensorOut.Artifacts))
	for _, artifact := range tensorOut.Artifacts {
		relPath := artifactRelPath(intentID, batchID, policyVersion, artifact.ShardIndex)
		absPath := filepath.Join(s.dataDir, filepath.FromSlash(relPath))
		absDir := filepath.Dir(absPath)
		if err := os.MkdirAll(absDir, 0o755); err != nil {
			return nil, nil, domain.NewError(domain.CodeInternal, "create shard directory: "+err.Error())
		}
		tempPath := tempPaths[artifact.ShardIndex]
		if err := os.Rename(tempPath, absPath); err != nil {
			return nil, nil, domain.NewError(domain.CodeInternal, "publish shard atomically: "+err.Error())
		}
		relPaths = append(relPaths, relPath)

		stat := tensorOut.ShardStats[artifact.ShardIndex]
		records = append(records, store.ShardRecord{
			BatchID:        batchID,
			PolicyVersion:  policyVersion,
			ShardIndex:     artifact.ShardIndex,
			State:          "ready",
			RecordCount:    stat.RecordCount,
			SampledCount:   stat.SampledCount,
			ByteCount:      stat.ByteCount,
			SampledBytes:   stat.SampledBytes,
			ArtifactPath:   relPath,
			DType:          artifact.Header.DType,
			SampleCount:    artifact.Header.SampleCount,
			FeatureDim:     artifact.Header.FeatureDim,
			NullCount:      artifact.Header.NullCount,
			ArtifactDigest: artifact.Digest,
			BodyDigest:     artifact.Header.BodyDigest,
			Warnings:       stat.Warnings,
		})
	}
	return relPaths, records, nil
}
