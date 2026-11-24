package sample

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"time"

	"ampcheckpoint/internal/domain"
	"ampcheckpoint/internal/mixed"
)

const (
	DefaultSeed          int64 = 20261125
	ShardCount                 = 10
	InitialResidentCount       = 8
	ValuesPerShard             = 16
)

type ShardSpec struct {
	ShardID       string `json:"shard_id"`
	Priority      int64  `json:"priority"`
	FP32SizeBytes int64  `json:"fp32_size_bytes"`
	FP16SizeBytes int64  `json:"fp16_size_bytes"`
}

type Manifest struct {
	Seed                 int64                 `json:"seed"`
	SampleDir            string                `json:"sample_dir"`
	GeneratedAt          time.Time             `json:"generated_at"`
	Shards               []ShardSpec           `json:"shards"`
	Policy               domain.PolicySnapshot `json:"policy_snapshot"`
	Delta                domain.DeltaFragment  `json:"delta_fragment"`
	TightPolicy          domain.PolicySnapshot `json:"write_budget_policy"`
	QueueFullPolicy      domain.PolicySnapshot `json:"write_queue_full_policy"`
	VRAMFullPolicy       domain.PolicySnapshot `json:"vram_full_policy"`
	PartitionBatchFile   string                `json:"partition_batch_file"`
	Files                []string              `json:"files"`
	FileSHA256           map[string]string     `json:"file_sha256"`
	PartitionBatchSHA256 string                `json:"partition_batch_sha256"`
	RestorationContract  string                `json:"restoration_contract"`
}

type Generator struct {
	seed       int64
	dir        string
	manifest   Manifest
	shardIndex map[string]ShardSpec
}

func New(seed int64, dir string) (*Generator, error) {
	if seed == 0 {
		seed = DefaultSeed
	}
	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "amp-checkpoint-sample-")
		if err != nil {
			return nil, err
		}
	}
	return &Generator{seed: seed, dir: dir, shardIndex: map[string]ShardSpec{}}, nil
}

func (g *Generator) Dir() string { return g.dir }
func (g *Generator) Seed() int64 { return g.seed }

func (g *Generator) Ensure() (Manifest, error) {
	if err := os.MkdirAll(filepath.Join(g.dir, "partitions"), 0o755); err != nil {
		return Manifest{}, err
	}
	manifestPath := filepath.Join(g.dir, "manifest.json")
	if raw, err := os.ReadFile(manifestPath); err == nil {
		if err := json.Unmarshal(raw, &g.manifest); err != nil {
			return Manifest{}, domain.BizError(domain.ErrPartitionInvalid, "cannot parse sample manifest", map[string]any{"path": manifestPath, "reason": err.Error()})
		}
		if g.manifest.Seed != g.seed {
			return Manifest{}, domain.BizError(domain.ErrPartitionInvalid, "sample seed does not match generator seed", map[string]any{"path": manifestPath, "seed": g.manifest.Seed, "requested_seed": g.seed})
		}
		if err := g.validateLoadedManifest(); err != nil {
			return Manifest{}, err
		}
		return g.manifest, nil
	} else if !os.IsNotExist(err) {
		return Manifest{}, err
	}
	if err := g.generate(); err != nil {
		return Manifest{}, err
	}
	return g.manifest, nil
}

func (g *Generator) Manifest() Manifest { return g.manifest }

func (g *Generator) KnownShardIDs() map[string]struct{} {
	out := make(map[string]struct{}, len(g.manifest.Shards))
	for _, spec := range g.manifest.Shards {
		out[spec.ShardID] = struct{}{}
	}
	return out
}

func (g *Generator) Specs() []ShardSpec {
	return append([]ShardSpec(nil), g.manifest.Shards...)
}

func (g *Generator) Round(round int64) ([]domain.LogicalShard, error) {
	if round <= 0 {
		return nil, domain.BizError(domain.ErrInvalidRequest, "round must be positive", map[string]any{"round": round})
	}
	out := make([]domain.LogicalShard, 0, len(g.manifest.Shards))
	for _, spec := range g.manifest.Shards {
		values := deterministicValues(g.seed, spec.ShardID, round)
		out = append(out, domain.LogicalShard{
			ShardID:        spec.ShardID,
			Round:          round,
			Priority:       spec.Priority,
			FP32SizeBytes:  spec.FP32SizeBytes,
			FP16SizeBytes:  spec.FP16SizeBytes,
			ValuesFixed:    values,
			MomentumDigest: mixed.MomentumDigest(g.seed, spec.ShardID, round),
		})
	}
	return out, nil
}

func (g *Generator) generate() error {
	specs := make([]ShardSpec, 0, ShardCount)
	for i := 0; i < ShardCount; i++ {
		specs = append(specs, ShardSpec{
			ShardID:       fmt.Sprintf("shard-%02d", i),
			Priority:      100 - int64(i*5),
			FP32SizeBytes: int64(4096 + i*256),
			FP16SizeBytes: int64(2048 + i*128),
		})
	}
	g.manifest = Manifest{
		Seed:                g.seed,
		SampleDir:           g.dir,
		GeneratedAt:         fixedTime(0),
		Shards:              specs,
		RestorationContract: domain.RestorationContract,
		FileSHA256:          map[string]string{},
	}
	for _, spec := range specs {
		g.shardIndex[spec.ShardID] = spec
	}

	baseAccess := int64(1_760_000_000_000_000_000)
	initialPlacements := make([]domain.Placement, 0, ShardCount)
	for i, spec := range specs {
		placement := domain.Placement{
			ShardID:      spec.ShardID,
			Precision:    domain.PrecisionFP16,
			Pool:         domain.PoolVRAM,
			LastAccessNS: baseAccess + int64(i)*1_000_000_000,
		}
		if i >= InitialResidentCount {
			placement.Precision = domain.PrecisionFP32
			placement.Pool = domain.PoolPersistent
		}
		initialPlacements = append(initialPlacements, placement)
	}
	g.manifest.Policy = g.makePolicy(1, fixedTime(1), 20_480, 80_000, 500, 32, 2, 128, initialPlacements)

	deltaPlacements := make([]domain.Placement, 0, ShardCount-InitialResidentCount)
	for i := InitialResidentCount; i < ShardCount; i++ {
		deltaPlacements = append(deltaPlacements, domain.Placement{
			ShardID:      specs[i].ShardID,
			Precision:    domain.PrecisionFP16,
			Pool:         domain.PoolVRAM,
			LastAccessNS: baseAccess + int64(100+i)*1_000_000_000,
		})
	}
	g.manifest.Delta = domain.DeltaFragment{
		BaseVersion: 1,
		GeneratedAt: fixedTime(2),
		Placements:  deltaPlacements,
		LossScale: &domain.LossScalePatch{
			Action: "grow",
		},
	}
	g.manifest.Delta.Checksum = domain.DeltaChecksum(g.manifest.Delta)

	allFP16 := make([]domain.Placement, 0, ShardCount)
	for i, spec := range specs {
		allFP16 = append(allFP16, domain.Placement{
			ShardID:      spec.ShardID,
			Precision:    domain.PrecisionFP16,
			Pool:         domain.PoolVRAM,
			LastAccessNS: baseAccess + int64(200+i)*1_000_000_000,
		})
	}
	g.manifest.TightPolicy = g.makePolicy(20, fixedTime(20), 20_480, 9_000, 500, 32, 2, 128, allFP16)
	g.manifest.QueueFullPolicy = g.makePolicy(21, fixedTime(21), 20_480, 0, 500, 2, 2, 128, allFP16)
	g.manifest.VRAMFullPolicy = g.makePolicy(22, fixedTime(22), 1_024, 80_000, 500, 32, 2, 128, allFP16)

	batch, err := g.initialPartitionBatch(1, g.manifest.Policy.LossScaleFixed, specs, initialPlacements)
	if err != nil {
		return err
	}
	if _, err := domain.ValidatePartitionBatch(batch); err != nil {
		return err
	}
	batchPath := filepath.Join(g.dir, "partitions.json")
	if err := writeJSON(batchPath, batch); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(g.dir, "policy_snapshot.json"), g.manifest.Policy); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(g.dir, "delta_fragment.json"), g.manifest.Delta); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(g.dir, "policy_write_budget.json"), g.manifest.TightPolicy); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(g.dir, "policy_queue_full.json"), g.manifest.QueueFullPolicy); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(g.dir, "policy_vram_full.json"), g.manifest.VRAMFullPolicy); err != nil {
		return err
	}
	for i, spec := range specs {
		one := domain.PartitionBatch{Partitions: []domain.PartitionInput{batch.Partitions[i]}}
		if err := writeJSON(filepath.Join(g.dir, "partitions", spec.ShardID+".json"), one); err != nil {
			return err
		}
	}
	g.manifest.PartitionBatchFile = batchPath
	batchRaw, err := os.ReadFile(batchPath)
	if err != nil {
		return err
	}
	g.manifest.PartitionBatchSHA256 = sha256Hex(batchRaw)
	g.manifest.Files = []string{
		"policy_snapshot.json", "delta_fragment.json", "policy_write_budget.json",
		"policy_queue_full.json", "policy_vram_full.json", "partitions.json",
	}
	if err := g.populateFileHashes(); err != nil {
		return err
	}
	return writeJSON(filepath.Join(g.dir, "manifest.json"), g.manifest)
}

func (g *Generator) makePolicy(version int64, generatedAt time.Time, vram, writeBudget, roundTimeMS int64, queueCap int, taskTimeMS, lossScale int64, placements []domain.Placement) domain.PolicySnapshot {
	p := domain.PolicySnapshot{
		Version:                version,
		GeneratedAt:            generatedAt,
		VRAMPoolBytes:          vram,
		WriteBudgetBytes:       writeBudget,
		RoundTimeBudgetMS:      roundTimeMS,
		WriteQueueCapacity:     queueCap,
		TaskTimeMS:             taskTimeMS,
		LossScaleFixed:         lossScale * domain.FixedScale,
		GrowthFactorFixed:      2 * domain.FixedScale,
		OverflowThresholdFixed: 65_536 * domain.FixedScale,
		Placements:             append([]domain.Placement(nil), placements...),
	}
	p.Checksum = domain.PolicyChecksum(p)
	return p
}

func (g *Generator) initialPartitionBatch(round, lossScale int64, specs []ShardSpec, placements []domain.Placement) (domain.PartitionBatch, error) {
	masterByShard := make(map[string]domain.PartitionInput, len(specs))
	batch := domain.PartitionBatch{Partitions: make([]domain.PartitionInput, 0, len(specs)+InitialResidentCount)}
	for _, spec := range specs {
		values := deterministicValues(g.seed, spec.ShardID, round)
		digest := mixed.MasterDigest(spec.ShardID, round, values)
		momentum := mixed.MomentumDigest(g.seed, spec.ShardID, round)
		input := domain.PartitionInput{
			CopyID:         "master:" + spec.ShardID,
			ShardID:        spec.ShardID,
			Round:          round,
			Precision:      domain.PrecisionFP32,
			SizeBytes:      spec.FP32SizeBytes,
			Digest:         digest,
			MomentumDigest: momentum,
		}
		masterByShard[spec.ShardID] = input
		batch.Partitions = append(batch.Partitions, input)
	}
	for _, placement := range placements {
		if placement.Precision != domain.PrecisionFP16 {
			continue
		}
		master := masterByShard[placement.ShardID]
		values := deterministicValues(g.seed, placement.ShardID, round)
		q, err := mixed.Quantize(placement.ShardID, round, lossScale, values)
		if err != nil {
			return domain.PartitionBatch{}, err
		}
		spec := g.shardIndex[placement.ShardID]
		batch.Partitions = append(batch.Partitions, domain.PartitionInput{
			CopyID:         "fp16:" + placement.ShardID,
			ShardID:        placement.ShardID,
			Round:          round,
			Precision:      domain.PrecisionFP16,
			SizeBytes:      spec.FP16SizeBytes,
			Digest:         q.Digest,
			MasterCopyID:   master.CopyID,
			MasterDigest:   master.Digest,
			RestoredDigest: q.RestoredDigest,
			Restoration:    domain.RestorationContract,
			MomentumDigest: master.MomentumDigest,
		})
	}
	return batch, nil
}

func (g *Generator) validateLoadedManifest() error {
	if len(g.manifest.Shards) == 0 {
		return domain.BizError(domain.ErrPartitionInvalid, "sample manifest has no shards", map[string]any{"path": g.dir})
	}
	g.shardIndex = make(map[string]ShardSpec, len(g.manifest.Shards))
	for _, spec := range g.manifest.Shards {
		if spec.ShardID == "" || spec.FP32SizeBytes <= 0 || spec.FP16SizeBytes <= 0 {
			return domain.BizError(domain.ErrPartitionInvalid, "invalid shard spec", map[string]any{"shard_id": spec.ShardID, "fp32_size_bytes": spec.FP32SizeBytes, "fp16_size_bytes": spec.FP16SizeBytes})
		}
		if _, exists := g.shardIndex[spec.ShardID]; exists {
			return domain.BizError(domain.ErrPartitionInvalid, "duplicate shard in sample manifest", map[string]any{"shard_id": spec.ShardID})
		}
		g.shardIndex[spec.ShardID] = spec
	}
	if got, want := domain.PolicyChecksum(g.manifest.Policy), g.manifest.Policy.Checksum; got != want {
		return domain.BizError(domain.ErrChecksumMismatch, "sample policy checksum mismatch", map[string]any{"got": got, "want": want})
	}
	if got, want := domain.DeltaChecksum(g.manifest.Delta), g.manifest.Delta.Checksum; got != want {
		return domain.BizError(domain.ErrChecksumMismatch, "sample delta checksum mismatch", map[string]any{"got": got, "want": want})
	}
	batchPath := filepath.Join(g.dir, "partitions.json")
	raw, err := os.ReadFile(batchPath)
	if err != nil {
		return domain.BizError(domain.ErrPartitionInvalid, "cannot read sample partition batch", map[string]any{"path": batchPath, "reason": err.Error()})
	}
	if g.manifest.PartitionBatchSHA256 != "" && sha256Hex(raw) != g.manifest.PartitionBatchSHA256 {
		return domain.BizError(domain.ErrPartitionInvalid, "sample partition batch digest mismatch", map[string]any{"path": batchPath})
	}
	var batch domain.PartitionBatch
	if err := json.Unmarshal(raw, &batch); err != nil {
		return domain.BizError(domain.ErrPartitionInvalid, "cannot parse sample partition batch", map[string]any{"path": batchPath, "reason": err.Error()})
	}
	if _, err := domain.ValidatePartitionBatch(batch); err != nil {
		return err
	}
	for _, policy := range []domain.PolicySnapshot{g.manifest.Policy, g.manifest.TightPolicy, g.manifest.QueueFullPolicy, g.manifest.VRAMFullPolicy} {
		if got, want := domain.PolicyChecksum(policy), policy.Checksum; got != want {
			return domain.BizError(domain.ErrChecksumMismatch, "sample policy checksum mismatch", map[string]any{"version": policy.Version, "got": got, "want": want})
		}
	}
	for name, want := range g.manifest.FileSHA256 {
		raw, err := os.ReadFile(filepath.Join(g.dir, name))
		if err != nil {
			return domain.BizError(domain.ErrPartitionInvalid, "cannot read deterministic sample file", map[string]any{"file": name, "reason": err.Error()})
		}
		if got := sha256Hex(raw); got != want {
			return domain.BizError(domain.ErrPartitionInvalid, "deterministic sample file digest mismatch", map[string]any{"file": name, "got": got, "want": want})
		}
	}
	return nil
}

func (g *Generator) populateFileHashes() error {
	for _, name := range g.manifest.Files {
		raw, err := os.ReadFile(filepath.Join(g.dir, name))
		if err != nil {
			return err
		}
		g.manifest.FileSHA256[name] = sha256Hex(raw)
	}
	return nil
}

func deterministicValues(seed int64, shardID string, round int64) []int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(shardID))
	state := uint64(seed) ^ h.Sum64() ^ uint64(round)*0x9e3779b97f4a7c15
	out := make([]int64, ValuesPerShard)
	for i := range out {
		state += 0x9e3779b97f4a7c15
		z := state
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		z ^= z >> 31
		v := int64(z%4_000_001) - 2_000_000
		if v == 0 {
			v = 125_000
		}
		out[i] = v
	}
	return out
}

func fixedTime(offsetSeconds int64) time.Time {
	return time.Date(2026, 1, 15, 0, 0, int(offsetSeconds), 0, time.UTC)
}

func writeJSON(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(path, raw, 0o644)
}

func sha256Hex(raw []byte) string {
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("%x", sum[:])
}

func (m Manifest) SortedFiles() []string {
	out := append([]string(nil), m.Files...)
	sort.Strings(out)
	return out
}
