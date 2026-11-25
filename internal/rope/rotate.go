package rope

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"math"

	"ropechunk/internal/model"
)

type RotateOptions struct {
	OutputScale int64
}

func RotateBlock(src model.QuantTensor, cfg model.Config, batch, head, rowStart, rowEnd int, opts RotateOptions) (model.RotatedBlock, error) {
	if err := src.Validate("source tensor"); err != nil {
		return model.RotatedBlock{}, err
	}
	s := src.Shape
	if batch < 0 || batch >= s.Batch() || head < 0 || head >= s.Heads() {
		return model.RotatedBlock{}, fmt.Errorf("batch/head out of range")
	}
	if rowStart < 0 || rowEnd <= rowStart || rowEnd > s.Seq() {
		return model.RotatedBlock{}, fmt.Errorf("row range [%d,%d) out of bounds", rowStart, rowEnd)
	}
	if err := ValidateFrequencyConfig(cfg, s.HeadDim()); err != nil {
		return model.RotatedBlock{}, err
	}
	outScale := opts.OutputScale
	if outScale == 0 {
		outScale = src.Scale
	}
	if outScale <= 0 {
		return model.RotatedBlock{}, fmt.Errorf("output scale must be positive")
	}

	d := s.HeadDim()
	rows := rowEnd - rowStart
	outData := make([]int64, rows*d)
	for r := 0; r < rows; r++ {
		base := src.Index(batch, head, rowStart+r, 0)
		copy(outData[r*d:(r+1)*d], src.Data[base:base+d])
	}

	blocks := make([]model.AngleSummary, 0, (d+cfg.AngleBlockSize-1)/cfg.AngleBlockSize)
	unrotated := make(map[int]struct{})
	for dimStart := 0; dimStart < d; dimStart += cfg.AngleBlockSize {
		dimEnd := dimStart + cfg.AngleBlockSize
		if dimEnd > d {
			dimEnd = d
		}
		pairs := (dimEnd - dimStart) / 2
		h := sha256.New()
		writeAngleHeader(h, rowStart, rowEnd, dimStart, dimEnd)
		var firstAngle, lastAngle float64
		seen := 0
		for r := 0; r < rows; r++ {
			rowIdx := rowStart + r
			for dim := dimStart; dim+1 < dimEnd; dim += 2 {
				invFreq := InvFrequency(dim/2, d, cfg)
				angle := float64(rowIdx) * invFreq
				cosine := math.Cos(angle)
				sine := math.Sin(angle)
				writeFloat(h, angle)
				writeFloat(h, cosine)
				writeFloat(h, sine)
				if seen == 0 {
					firstAngle = angle
				}
				lastAngle = angle
				seen++

				x0 := src.FloatAt(src.Index(batch, head, rowIdx, dim))
				x1 := src.FloatAt(src.Index(batch, head, rowIdx, dim+1))
				y0 := x0*cosine - x1*sine
				y1 := x0*sine + x1*cosine
				outData[r*d+dim] = Quantize(y0, outScale)
				outData[r*d+dim+1] = Quantize(y1, outScale)
			}
			if (dimEnd-dimStart)%2 != 0 {
				unrotated[dimEnd-1] = struct{}{}
			}
		}
		summary := model.AngleSummary{
			DimStart:    dimStart,
			DimEnd:      dimEnd,
			PairCount:   pairs,
			FirstAngle:  firstAngle,
			LastAngle:   lastAngle,
			AngleDigest: hex.EncodeToString(h.Sum(nil)),
		}
		if summary.PairCount == 0 {
			summary.FirstAngle = 0
			summary.LastAngle = 0
		}
		blocks = append(blocks, summary)
	}

	unrotatedDims := make([]int, 0, len(unrotated))
	for dim := 0; dim < d; dim++ {
		if _, ok := unrotated[dim]; ok {
			unrotatedDims = append(unrotatedDims, dim)
		}
	}
	result := model.RotatedBlock{
		ShardID:      "",
		Batch:        batch,
		Head:         head,
		RowStart:     rowStart,
		RowEnd:       rowEnd,
		Shape:        s,
		Scale:        outScale,
		Data:         outData,
		Angles:       blocks,
		UnrotatedDim: unrotatedDims,
	}
	result.Digest = RotatedBlockDigest(result)
	return result, nil
}

func RotateTensor(src model.QuantTensor, cfg model.Config) (model.QuantTensor, []model.RotatedBlock, error) {
	if err := src.Validate("source tensor"); err != nil {
		return model.QuantTensor{}, nil, err
	}
	out := model.QuantTensor{Shape: src.Shape, Scale: src.Scale, Data: make([]int64, len(src.Data))}
	blocks := make([]model.RotatedBlock, 0)
	s := src.Shape
	for b := 0; b < s.Batch(); b++ {
		for h := 0; h < s.Heads(); h++ {
			for rowStart := 0; rowStart < s.Seq(); rowStart += cfg.RowBlockSize {
				rowEnd := min(rowStart+cfg.RowBlockSize, s.Seq())
				block, err := RotateBlock(src, cfg, b, h, rowStart, rowEnd, RotateOptions{OutputScale: src.Scale})
				if err != nil {
					return model.QuantTensor{}, nil, err
				}
				base := out.Index(b, h, rowStart, 0)
				copy(out.Data[base:base+len(block.Data)], block.Data)
				blocks = append(blocks, block)
			}
		}
	}
	return out, blocks, nil
}

func Quantize(v float64, scale int64) int64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	scaled := math.Round(v * float64(scale))
	if scaled >= math.MaxInt64 {
		return math.MaxInt64
	}
	if scaled <= math.MinInt64 {
		return math.MinInt64
	}
	return int64(scaled)
}

func RotatedBlockDigest(block model.RotatedBlock) string {
	h := sha256.New()
	writeBlockHeader(h, block.Batch, block.Head, block.RowStart, block.RowEnd, block.Shape, block.Scale)
	writeInts(h, block.Data)
	for _, a := range block.Angles {
		writeAngleHeader(h, block.RowStart, block.RowEnd, a.DimStart, a.DimEnd)
		writeString(h, a.AngleDigest)
	}
	for _, dim := range block.UnrotatedDim {
		writeInt64(h, int64(dim))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func writeBlockHeader(h hash.Hash, batch, head, rowStart, rowEnd int, shape model.Shape, scale int64) {
	writeString(h, "rope-rotated-block-v1")
	writeInt64(h, int64(batch))
	writeInt64(h, int64(head))
	writeInt64(h, int64(rowStart))
	writeInt64(h, int64(rowEnd))
	for _, v := range shape {
		writeInt64(h, int64(v))
	}
	writeInt64(h, scale)
}

func writeAngleHeader(h hash.Hash, rowStart, rowEnd, dimStart, dimEnd int) {
	writeString(h, "rope-angle-block-v1")
	writeInt64(h, int64(rowStart))
	writeInt64(h, int64(rowEnd))
	writeInt64(h, int64(dimStart))
	writeInt64(h, int64(dimEnd))
}

func writeFloat(h hash.Hash, v float64) {
	writeInt64(h, int64(math.Float64bits(v)))
}

func writeInt64(h hash.Hash, v int64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(v))
	_, _ = h.Write(buf[:])
}

func writeInts(h hash.Hash, values []int64) {
	var buf [8]byte
	for _, v := range values {
		binary.LittleEndian.PutUint64(buf[:], uint64(v))
		_, _ = h.Write(buf[:])
	}
}

func writeString(h hash.Hash, s string) {
	writeInt64(h, int64(len(s)))
	_, _ = h.Write([]byte(s))
}
