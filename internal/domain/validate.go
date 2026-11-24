package domain

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

func ValidatePartitionBatch(batch PartitionBatch) (PartitionValidation, error) {
	result := PartitionValidation{PartitionCount: len(batch.Partitions)}
	if len(batch.Partitions) == 0 {
		return result, BizError(ErrPartitionInvalid, "partition batch is empty", map[string]any{"field": "partitions"})
	}
	seen := make(map[string]struct{}, len(batch.Partitions))
	masters := make(map[string]PartitionInput, len(batch.Partitions))
	for i, p := range batch.Partitions {
		if strings.TrimSpace(p.CopyID) == "" {
			return result, BizError(ErrPartitionInvalid, "copy_id is required", map[string]any{"index": i, "field": "copy_id"})
		}
		if _, ok := seen[p.CopyID]; ok {
			return result, BizError(ErrPartitionInvalid, "duplicate copy_id", map[string]any{"index": i, "copy_id": p.CopyID})
		}
		seen[p.CopyID] = struct{}{}
		if strings.TrimSpace(p.ShardID) == "" {
			return result, BizError(ErrPartitionInvalid, "shard_id is required", map[string]any{"index": i, "copy_id": p.CopyID, "field": "shard_id"})
		}
		if !p.Precision.Valid() {
			return result, BizError(ErrPartitionInvalid, "precision must be FP16 or FP32", map[string]any{"index": i, "copy_id": p.CopyID, "precision": p.Precision, "allowed": []string{"FP16", "FP32"}})
		}
		if p.SizeBytes <= 0 {
			return result, BizError(ErrPartitionInvalid, "size_bytes must be a positive integer", map[string]any{"index": i, "copy_id": p.CopyID, "size_bytes": p.SizeBytes})
		}
		if p.Round <= 0 {
			return result, BizError(ErrPartitionInvalid, "round must be positive", map[string]any{"index": i, "copy_id": p.CopyID, "round": p.Round})
		}
		if p.SizeBytes > math.MaxInt64-result.TotalBytes {
			return result, BizError(ErrPartitionInvalid, "aggregate byte size overflows int64", map[string]any{"index": i, "copy_id": p.CopyID, "current_total": result.TotalBytes, "size_bytes": p.SizeBytes})
		}
		result.TotalBytes += p.SizeBytes
		if p.Precision == PrecisionFP32 {
			result.FP32Count++
			if p.MasterCopyID != "" {
				return result, BizError(ErrPartitionInvalid, "FP32 master must not reference another master", map[string]any{"index": i, "copy_id": p.CopyID, "master_copy_id": p.MasterCopyID})
			}
			masters[p.CopyID] = p
			continue
		}
		result.FP16Count++
		if p.MasterCopyID == "" || p.MasterDigest == "" {
			return result, BizError(ErrPartitionInvalid, "FP16 copy requires master_copy_id and master_digest", map[string]any{"index": i, "copy_id": p.CopyID})
		}
		if p.Restoration == "" || p.RestoredDigest == "" {
			return result, BizError(ErrPartitionInvalid, "FP16 copy must declare restoration and restored_digest", map[string]any{"index": i, "copy_id": p.CopyID})
		}
	}
	for i, p := range batch.Partitions {
		if p.Precision != PrecisionFP16 {
			continue
		}
		master, ok := masters[p.MasterCopyID]
		if !ok {
			return result, BizError(ErrPartitionInvalid, "FP16 copy references a master absent from the same atomic batch", map[string]any{"index": i, "copy_id": p.CopyID, "master_copy_id": p.MasterCopyID})
		}
		if master.ShardID != p.ShardID || master.Round != p.Round {
			return result, BizError(ErrPartitionInvalid, "FP16 copy and master shard/round mismatch", map[string]any{"index": i, "copy_id": p.CopyID, "master_copy_id": p.MasterCopyID})
		}
		if master.Digest != p.MasterDigest {
			return result, BizError(ErrPartitionInvalid, "FP16 master_digest does not match the batch master", map[string]any{"index": i, "copy_id": p.CopyID, "master_copy_id": p.MasterCopyID})
		}
	}
	if len(masters) == 0 {
		return result, BizError(ErrPartitionInvalid, "batch has no FP32 master partition", map[string]any{"field": "partitions"})
	}
	result.Valid = true
	return result, nil
}

func SortedKnownShardIDs(ids map[string]struct{}) []string {
	out := make([]string, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func ValidatePrecisionPool(p Precision, pool Pool) error {
	if !p.Valid() {
		return BizError(ErrPartitionInvalid, "invalid precision marker", map[string]any{"precision": p, "allowed": []string{"FP16", "FP32"}})
	}
	if p == PrecisionFP32 && pool != PoolPersistent {
		return BizError(ErrPartitionInvalid, "FP32 master parameters must reside in PERSISTENT pool", map[string]any{"precision": p, "pool": pool})
	}
	if p == PrecisionFP16 && pool != PoolVRAM && pool != PoolPersistent {
		return BizError(ErrPartitionInvalid, "FP16 copy must target VRAM or PERSISTENT", map[string]any{"precision": p, "pool": pool})
	}
	return nil
}

func OverflowAdd(a, b int64) error {
	if b > 0 && a > math.MaxInt64-b {
		return fmt.Errorf("int64 overflow")
	}
	if b < 0 && a < math.MinInt64-b {
		return fmt.Errorf("int64 underflow")
	}
	return nil
}
