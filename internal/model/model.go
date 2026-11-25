package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type Precision string

const (
	PrecisionFP16 Precision = "FP16"
	PrecisionFP32 Precision = "FP32"
)

type Pool string

const (
	PoolVRAM       Pool = "VRAM"
	PoolPersistent Pool = "PERSISTED"
)

type ShardStatus string

const (
	StatusPending   ShardStatus = "PENDING"
	StatusResident  ShardStatus = "RESIDENT"
	StatusPersisted ShardStatus = "PERSISTED"
)

// Shard is both the wire and persisted description of one parameter shard.
// Priority is optional on the interchange formats; a missing value means zero.
type Shard struct {
	ID             string      `json:"id"`
	Round          int64       `json:"round"`
	Precision      Precision   `json:"precision"`
	SizeBytes      int64       `json:"size_bytes"`
	Digest         string      `json:"digest"`
	Pool           Pool        `json:"pool,omitempty"`
	LastAccess     int64       `json:"last_access,omitempty"`
	SourceDigest   string      `json:"source_digest,omitempty"`
	MomentumDigest string      `json:"momentum_digest,omitempty"`
	Priority       int64       `json:"priority,omitempty"`
	Status         ShardStatus `json:"status,omitempty"`
}

func (s Shard) DesiredStatus() ShardStatus {
	if s.Pool == PoolVRAM {
		return StatusResident
	}
	return StatusPersisted
}

func IsHexDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func ValidateDigest(value, field, shardID string) *BizError {
	if !IsHexDigest(value) {
		return NewError(CodeSpecInvalid, "digest must be 64 hexadecimal characters", map[string]any{
			"shard_id": shardID,
			"field":    field,
			"value":    value,
		})
	}
	return nil
}

func ValidateShard(s Shard) *BizError {
	if strings.TrimSpace(s.ID) == "" {
		return NewError(CodeSpecInvalid, "shard id must not be empty", map[string]any{"field": "id"})
	}
	if s.Round < 1 {
		return NewError(CodeSpecInvalid, "round must be a positive integer", map[string]any{
			"shard_id": s.ID,
			"field":    "round",
			"value":    s.Round,
		})
	}
	if s.Precision != PrecisionFP16 && s.Precision != PrecisionFP32 {
		return NewError(CodeSpecInvalid, "precision is not in the allowed set", map[string]any{
			"shard_id": s.ID,
			"field":    "precision",
			"value":    s.Precision,
			"allowed":  []Precision{PrecisionFP16, PrecisionFP32},
		})
	}
	if s.SizeBytes <= 0 {
		return NewError(CodeSpecInvalid, "size_bytes must be a positive integer", map[string]any{
			"shard_id": s.ID,
			"field":    "size_bytes",
			"value":    s.SizeBytes,
		})
	}
	if err := ValidateDigest(s.Digest, "digest", s.ID); err != nil {
		return err
	}
	if s.Pool != PoolVRAM && s.Pool != PoolPersistent {
		return NewError(CodeSpecInvalid, "pool is not in the allowed set", map[string]any{
			"shard_id": s.ID,
			"field":    "pool",
			"value":    s.Pool,
			"allowed":  []Pool{PoolVRAM, PoolPersistent},
		})
	}
	if s.Precision == PrecisionFP16 {
		if s.SourceDigest == "" {
			return NewError(CodeSpecInvalid, "FP16 shard must reference its FP32 source digest", map[string]any{
				"shard_id": s.ID,
				"field":    "source_digest",
			})
		}
		if err := ValidateDigest(s.SourceDigest, "source_digest", s.ID); err != nil {
			return err
		}
	} else if s.SourceDigest != "" {
		return NewError(CodeSpecInvalid, "FP32 shard must not carry a source digest", map[string]any{
			"shard_id": s.ID,
			"field":    "source_digest",
		})
	}
	if s.Precision == PrecisionFP32 && s.Pool == PoolVRAM {
		return NewError(CodeSpecInvalid, "FP32 master shard must reside in the persistent pool", map[string]any{
			"shard_id": s.ID,
			"field":    "pool",
		})
	}
	if s.MomentumDigest != "" {
		if err := ValidateDigest(s.MomentumDigest, "momentum_digest", s.ID); err != nil {
			return err
		}
	}
	if s.LastAccess < 0 {
		return NewError(CodeSpecInvalid, "last_access must not be negative", map[string]any{
			"shard_id": s.ID,
			"field":    "last_access",
		})
	}
	return nil
}

func ValidateShards(shards []Shard) *BizError {
	seen := make(map[string]struct{}, len(shards))
	for i, shard := range shards {
		if err := ValidateShard(shard); err != nil {
			err.Context["record_index"] = i + 1
			return err
		}
		if _, exists := seen[shard.ID]; exists {
			return NewError(CodeSpecInvalid, "duplicate shard id", map[string]any{
				"shard_id":     shard.ID,
				"record_index": i + 1,
			})
		}
		seen[shard.ID] = struct{}{}
	}
	return nil
}

func CopyShards(shards []Shard) []Shard {
	out := make([]Shard, len(shards))
	copy(out, shards)
	return out
}

func SortShards(shards []Shard) {
	sort.Slice(shards, func(i, j int) bool { return shards[i].ID < shards[j].ID })
}

func CloneJSON[T any](value T) (T, error) {
	var out T
	raw, err := json.Marshal(value)
	if err != nil {
		return out, err
	}
	err = json.Unmarshal(raw, &out)
	return out, err
}

func CanonicalJSON(value any) ([]byte, error) {
	return json.Marshal(value)
}

func SHA256Hex(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func StableListDigest(values []string) string {
	cp := append([]string(nil), values...)
	sort.Strings(cp)
	h := sha256.New()
	for _, value := range cp {
		fmt.Fprintf(h, "%d:%s\n", len(value), value)
	}
	return hex.EncodeToString(h.Sum(nil))
}
