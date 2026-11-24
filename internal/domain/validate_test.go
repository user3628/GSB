package domain_test

import (
	"math"
	"strings"
	"testing"

	"ampcheckpoint/internal/domain"
)

func validBatch() domain.PartitionBatch {
	return domain.PartitionBatch{Partitions: []domain.PartitionInput{
		{CopyID: "master:s00", ShardID: "s00", Round: 1, Precision: domain.PrecisionFP32, SizeBytes: 4096, Digest: strings.Repeat("a", 64), MomentumDigest: strings.Repeat("b", 64)},
		{CopyID: "fp16:s00", ShardID: "s00", Round: 1, Precision: domain.PrecisionFP16, SizeBytes: 2048, Digest: strings.Repeat("c", 64), MasterCopyID: "master:s00", MasterDigest: strings.Repeat("a", 64), RestoredDigest: strings.Repeat("d", 64), Restoration: domain.RestorationContract},
	}}
}

func TestValidatePartitionBatchRejectsInvalidPrecision(t *testing.T) {
	batch := validBatch()
	batch.Partitions[0].Precision = "BF16"
	_, err := domain.ValidatePartitionBatch(batch)
	if code := domain.ErrorCode(err); code != domain.ErrPartitionInvalid {
		t.Fatalf("code=%s err=%v", code, err)
	}
}

func TestValidatePartitionBatchRejectsNonPositiveSize(t *testing.T) {
	batch := validBatch()
	batch.Partitions[0].SizeBytes = 0
	_, err := domain.ValidatePartitionBatch(batch)
	if code := domain.ErrorCode(err); code != domain.ErrPartitionInvalid {
		t.Fatalf("code=%s err=%v", code, err)
	}
}

func TestValidatePartitionBatchRejectsByteOverflow(t *testing.T) {
	batch := validBatch()
	batch.Partitions[0].SizeBytes = math.MaxInt64
	batch.Partitions[1].SizeBytes = 1
	_, err := domain.ValidatePartitionBatch(batch)
	if code := domain.ErrorCode(err); code != domain.ErrPartitionInvalid {
		t.Fatalf("code=%s err=%v", code, err)
	}
	be, ok := err.(*domain.BusinessError)
	if !ok || be.Context["current_total"] != int64(math.MaxInt64) {
		t.Fatalf("missing overflow context: %#v", err)
	}
}

func TestValidatePartitionBatchRequiresMasterInSameBatch(t *testing.T) {
	batch := validBatch()
	batch.Partitions = batch.Partitions[1:]
	_, err := domain.ValidatePartitionBatch(batch)
	if code := domain.ErrorCode(err); code != domain.ErrPartitionInvalid {
		t.Fatalf("code=%s err=%v", code, err)
	}
}

func TestPolicyChecksumIsDeterministic(t *testing.T) {
	policy := domain.PolicySnapshot{
		Version:                7,
		VRAMPoolBytes:          1024,
		WriteBudgetBytes:       512,
		RoundTimeBudgetMS:      10,
		WriteQueueCapacity:     3,
		TaskTimeMS:             1,
		LossScaleFixed:         128 * domain.FixedScale,
		GrowthFactorFixed:      2 * domain.FixedScale,
		OverflowThresholdFixed: 4096 * domain.FixedScale,
		Placements: []domain.Placement{
			{ShardID: "b", Precision: domain.PrecisionFP16, Pool: domain.PoolVRAM, LastAccessNS: 2},
			{ShardID: "a", Precision: domain.PrecisionFP32, Pool: domain.PoolPersistent, LastAccessNS: 1},
		},
	}
	first := domain.PolicyChecksum(policy)
	policy.Placements[0], policy.Placements[1] = policy.Placements[1], policy.Placements[0]
	second := domain.PolicyChecksum(policy)
	if first != second {
		t.Fatalf("checksum changed with placement order: %s != %s", first, second)
	}
	policy.Version++
	if domain.PolicyChecksum(policy) == first {
		t.Fatal("checksum did not change when a fact changed")
	}
}
