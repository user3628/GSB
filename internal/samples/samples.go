package samples

import (
	"fmt"
	"hash/fnv"
	"math"

	"ropechunk/internal/model"
	"ropechunk/internal/rope"
)

const DefaultSeed uint64 = 0x9e3779b97f4a7c15

type SampleMetadata struct {
	Seed        string `json:"seed"`
	Category    string `json:"category"`
	HasExtremes bool   `json:"has_extremes"`
	Note        string `json:"note"`
}

type spec struct {
	id          string
	description string
	category    string
	shape       model.Shape
	extreme     bool
}

func DefaultConfig() model.Config {
	return model.Config{
		SnapshotVersion:    1,
		BaseFrequency:      10_000,
		ScalingMode:        model.ScaleNone,
		ScaleFactor:        1,
		AngleBlockSize:     4,
		CausalMask:         true,
		RowBlockSize:       2,
		KeyBlockSize:       4,
		MaxConcurrentBlock: 4,
		WorkerBlockQuota:   8,
		RoundTimeBudgetMS:  20_000,
		PeakMemoryBytes:    128 << 20,
	}
}

func Generate() (model.Bundle, error) {
	return GenerateWithConfig(DefaultConfig())
}

func GenerateWithConfig(cfg model.Config) (model.Bundle, error) {
	if err := cfg.Validate(); err != nil {
		return model.Bundle{}, err
	}
	specs := []spec{
		{id: "s-short", description: "8-token short sequence, causal mask", category: "short", shape: model.Shape{1, 2, 8, 8}},
		{id: "s-long", description: "96-token long sequence, non-causal profile", category: "long", shape: model.Shape{1, 2, 96, 16}},
		{id: "s-odd", description: "odd head dimension 9 with batch 2", category: "odd_head_dim", shape: model.Shape{2, 3, 13, 9}},
		{id: "s-extreme", description: "batch > 1 with fixed-point extreme values", category: "extremes", shape: model.Shape{2, 2, 24, 12}, extreme: true},
	}
	bundle := model.Bundle{FormatVersion: 1, Config: cfg}
	for _, sp := range specs {
		shard, err := makeShard(sp, cfg)
		if err != nil {
			return model.Bundle{}, err
		}
		bundle.Shards = append(bundle.Shards, shard)
		bundle.Declarations = append(bundle.Declarations, model.ShapeDeclaration{
			ShardID:     sp.id,
			Shape:       sp.shape,
			Description: sp.description,
			HasExtremes: sp.extreme,
		})
	}
	if err := bundle.Validate(); err != nil {
		return model.Bundle{}, err
	}
	return bundle, nil
}

func makeShard(sp spec, cfg model.Config) (model.Shard, error) {
	const scale int64 = 1_000_000
	seed := DefaultSeed ^ hashString(sp.id)
	q := makeTensor(sp.shape, scale, seed^0xa0761d6478bd642f, sp.extreme)
	k := makeTensor(sp.shape, scale, seed^0xe7037ed1a0b428db, sp.extreme)
	refQ, err := rope.ReferenceRotate(q, cfg)
	if err != nil {
		return model.Shard{}, err
	}
	refK, err := rope.ReferenceRotate(k, cfg)
	if err != nil {
		return model.Shard{}, err
	}
	shard := model.Shard{
		ID:             sp.id,
		Description:    sp.description,
		Shape:          sp.shape,
		Q:              q,
		K:              k,
		ReferenceQ:     &refQ,
		ReferenceK:     &refK,
		SampleMetadata: SampleMetadata{Seed: fmt.Sprintf("%d", seed), Category: sp.category, HasExtremes: sp.extreme, Note: sp.description},
	}
	shard.InputDigest = model.InputDigest(shard)
	return shard, nil
}

func makeTensor(shape model.Shape, scale int64, seed uint64, extreme bool) model.QuantTensor {
	rng := splitMix64{state: seed}
	data := make([]int64, shape.Len())
	const amplitude int64 = 2_500_000
	for i := range data {
		v := int64(rng.next()%uint64(2*amplitude+1)) - amplitude
		data[i] = v
	}
	if extreme {
		values := []int64{math.MaxInt32, math.MinInt32, 1_500_000_000, -1_500_000_000, 9_000_000_000, -9_000_000_000}
		for i, v := range values {
			if len(data) > 0 {
				data[(i*7919)%len(data)] = v
			}
		}
	}
	return model.QuantTensor{Shape: shape, Scale: scale, Data: data}
}

type splitMix64 struct{ state uint64 }

func (s *splitMix64) next() uint64 {
	s.state += 0x9e3779b97f4a7c15
	z := s.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func hashString(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

func NextVersion(cfg model.Config, version int64, mutate func(*model.Config)) model.Config {
	cfg.SnapshotVersion = version
	if mutate != nil {
		mutate(&cfg)
	}
	return cfg
}
