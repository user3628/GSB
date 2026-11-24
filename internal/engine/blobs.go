package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"checkpointengine/internal/model"
)

type blobManifest struct {
	Version    string   `json:"version"`
	Round      int      `json:"round"`
	ShardID    string   `json:"shard_id"`
	Precision  string   `json:"precision"`
	Bytes      int64    `json:"bytes"`
	Digest     string   `json:"digest"`
	Submission string   `json:"submission_id"`
	FP16Bits   []uint16 `json:"fp16_bits,omitempty"`
	FP32Nanos  []int64  `json:"fp32_nanos,omitempty"`
	PayloadHex string   `json:"payload_hex,omitempty"`
}

func (e *Engine) blobPathFor(round int, shardID, digest string) string {
	name := model.NewDigest("BLOB_PATH_V1", strconv.Itoa(round), shardID, digest) + ".json"
	return filepath.Join(e.blobDir, name)
}

func (e *Engine) writeShardBlob(shard model.Shard, submissionID string) (string, error) {
	path := e.blobPathFor(shard.Round, shard.ShardID, shard.Digest)
	if _, err := os.Stat(path); err == nil {
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return "", readErr
		}
		var existing blobManifest
		if unmarshalErr := json.Unmarshal(data, &existing); unmarshalErr != nil || existing.Digest != shard.Digest ||
			existing.ShardID != shard.ShardID || existing.Round != shard.Round {
			return "", fmt.Errorf("existing blob manifest does not match shard %d/%s", shard.Round, shard.ShardID)
		}
		return path, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}
	manifest := blobManifest{
		Version: "CHECKPOINT_BLOB_V1", Round: shard.Round, ShardID: shard.ShardID,
		Precision: shard.Precision, Bytes: shard.Bytes, Digest: shard.Digest,
		Submission: submissionID, FP16Bits: shard.FP16Bits, FP32Nanos: shard.FP32Nanos,
		PayloadHex: shard.PayloadHex,
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(e.blobDir, ".write-*")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}
	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return "", err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return "", err
	}
	if err := os.Rename(tmpName, path); err != nil {
		if _, statErr := os.Stat(path); statErr == nil {
			_ = os.Remove(tmpName)
			return path, nil
		}
		_ = os.Remove(tmpName)
		return "", err
	}
	if dir, err := os.Open(e.blobDir); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	if _, err := os.Stat(path); err != nil {
		return "", fmt.Errorf("blob missing after durable rename: %w", err)
	}
	return path, nil
}
