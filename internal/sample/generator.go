package sample

import (
	"fmt"
	"math/rand"
	"time"

	"logagg/internal/codec"
	"logagg/internal/model"
)

const (
	Seed         int64 = 20261126
	ShardID            = "shard-local-001"
	GroupColumn        = "g"
	MetricColumn       = "m"
	MetricScale        = 4
)

type BlockSpec struct {
	Partition int
	BlockNo   int
	Rows      int
	Encoding  model.Encoding
}

func DefaultSpecs() []BlockSpec {
	return []BlockSpec{
		{Partition: 0, BlockNo: 0, Rows: 240, Encoding: model.EncodingFixed},
		{Partition: 0, BlockNo: 1, Rows: 180, Encoding: model.EncodingRLE},
		{Partition: 1, BlockNo: 2, Rows: 96, Encoding: model.EncodingDictionary},
		{Partition: 1, BlockNo: 3, Rows: 72, Encoding: model.EncodingFixed},
		{Partition: 2, BlockNo: 4, Rows: 48, Encoding: model.EncodingRLE},
		{Partition: 3, BlockNo: 5, Rows: 24, Encoding: model.EncodingDictionary},
	}
}

type Generator struct {
	Seed int64
}

func NewGenerator() *Generator {
	return &Generator{Seed: Seed}
}

func (g *Generator) Shard(version int64) *model.ShardPackage {
	specs := DefaultSpecs()
	manifest := model.Shard{
		ShardID:     ShardID,
		Version:     version,
		GeneratedAt: time.Date(2026, 11, 26, 8, 0, int(version), 0, time.UTC),
		Columns: []model.ColumnDef{
			{Name: GroupColumn, Role: "group_key", Encoding: model.EncodingFixed, Scale: 0, Width: 8},
			{Name: MetricColumn, Role: "metric", Encoding: model.EncodingFixed, Scale: MetricScale, Width: 8},
		},
		Blocks: make([]model.BlockMeta, 0, len(specs)),
	}
	pkg := &model.ShardPackage{Manifest: manifest}
	for _, spec := range specs {
		block := g.Block(spec, version)
		pkg.Blocks = append(pkg.Blocks, block)
		pkg.Manifest.Blocks = append(pkg.Manifest.Blocks, block.Meta)
	}
	return pkg
}

func (g *Generator) Block(spec BlockSpec, version int64) *model.Block {
	rng := rand.New(rand.NewSource(g.Seed + int64(spec.BlockNo)*7919 + version*104729))
	groups, metrics := valuesForSpec(rng, spec)
	var groupPayload, metricPayload []byte
	switch spec.Encoding {
	case model.EncodingFixed:
		groupPayload = codec.EncodeFixed(groups)
		metricPayload = codec.EncodeFixed(metrics)
	case model.EncodingRLE:
		groupPayload = codec.EncodeRLE(groups)
		metricPayload = codec.EncodeRLE(metrics)
	case model.EncodingDictionary:
		groupPayload = codec.EncodeDictionary(groups)
		metricPayload = codec.EncodeDictionary(metrics)
	default:
		groupPayload = codec.EncodeFixed(groups)
		metricPayload = codec.EncodeFixed(metrics)
	}
	meta := model.BlockMeta{
		BlockNo:           spec.BlockNo,
		Partition:         spec.Partition,
		RowCount:          spec.Rows,
		Encoding:          spec.Encoding,
		UncompressedBytes: int64(16 * spec.Rows),
	}
	meta.Digest = model.DigestPayload(meta, groupPayload, metricPayload)
	return &model.Block{Meta: meta, GroupPayload: groupPayload, MetricPayload: metricPayload}
}

func valuesForSpec(rng *rand.Rand, spec BlockSpec) ([]int64, []int64) {
	groups := make([]int64, spec.Rows)
	metrics := make([]int64, spec.Rows)
	switch spec.Encoding {
	case model.EncodingRLE:
		for row := 0; row < spec.Rows; {
			runLength := 3 + rng.Intn(12)
			if row+runLength > spec.Rows {
				runLength = spec.Rows - row
			}
			groupValue := int64(rng.Intn(5))
			metricValue := int64(100 + rng.Intn(240000))
			for i := 0; i < runLength; i++ {
				groups[row+i] = groupValue
				metrics[row+i] = metricValue
			}
			row += runLength
		}
	case model.EncodingDictionary:
		metricDictionary := []int64{0, 1250, 2500, 4999, 10000, 27500, 88888, 199999}
		for row := 0; row < spec.Rows; row++ {
			groups[row] = int64(rng.Intn(5))
			metrics[row] = metricDictionary[rng.Intn(len(metricDictionary))]
		}
	default:
		for row := 0; row < spec.Rows; row++ {
			groups[row] = int64(rng.Intn(7))
			metrics[row] = int64(1 + rng.Intn(250000))
		}
	}
	return groups, metrics
}

func (g *Generator) OfflineEvents(sessionID, batchPrefix string, count int) []model.OfflineEvent {
	rng := rand.New(rand.NewSource(g.Seed + 325))
	specs := DefaultSpecs()
	base := time.Date(2026, 11, 26, 9, 0, 0, 0, time.UTC)
	events := make([]model.OfflineEvent, 0, count)
	for i := 0; i < count; i++ {
		spec := specs[rng.Intn(len(specs))]
		batchIndex := i % 3
		events = append(events, model.OfflineEvent{
			Seq:       int64(i + 1),
			EventID:   fmt.Sprintf("evt-%04d", i+1),
			BatchID:   fmt.Sprintf("%s-batch-%d", batchPrefix, batchIndex+1),
			SessionID: sessionID,
			ShardID:   ShardID,
			Version:   1,
			Partition: spec.Partition,
			BlockNo:   spec.BlockNo,
			Type:      []string{"block_available", "decode_scheduled", "aggregate_committed"}[i%3],
			At:        base.Add(time.Duration(i) * time.Minute),
		})
	}
	return events
}

func AllRows(pkg *model.ShardPackage) (groups, metrics []int64, err error) {
	total := 0
	for _, meta := range pkg.Manifest.Blocks {
		total += meta.RowCount
	}
	groups = make([]int64, 0, total)
	metrics = make([]int64, 0, total)
	for _, meta := range pkg.Manifest.Blocks {
		block := pkg.BlockByNo(meta.BlockNo)
		groupValues, err := codec.DecodeColumn(block, codec.ColumnGroup, 0, 0)
		if err != nil {
			return nil, nil, err
		}
		metricValues, err := codec.DecodeColumn(block, codec.ColumnMetric, 0, 0)
		if err != nil {
			return nil, nil, err
		}
		groups = append(groups, groupValues...)
		metrics = append(metrics, metricValues...)
	}
	return groups, metrics, nil
}
