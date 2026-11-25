package attn

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"

	"flashattention/internal/tensor"
)

func HashFloat32(xs []float32) string {
	h := sha256.New()
	var buf [4]byte
	for _, x := range xs {
		binary.LittleEndian.PutUint32(buf[:], math.Float32bits(x))
		h.Write(buf[:])
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}

func HashState(m, sum, lse float64, acc []float64) string {
	h := sha256.New()
	var buf [8]byte
	for _, x := range []float64{m, sum, lse} {
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(x))
		h.Write(buf[:])
	}
	h.Write(tensor.EncodeFloat64s(acc))
	return hex.EncodeToString(h.Sum(nil))[:32]
}

func (c *BlockContribution) ComputeHash() string {
	h := sha256.New()
	var header [8]byte
	putInt := func(x int) {
		binary.LittleEndian.PutUint64(header[:], uint64(x))
		h.Write(header[:])
	}
	for _, x := range []int{c.BH, c.RowBlock, c.ColBlock, c.RowStart, c.RowEnd, c.ColStart, c.ColEnd, c.HeadDim} {
		putInt(x)
	}
	h.Write(tensor.EncodeFloat64s(c.M))
	h.Write(tensor.EncodeFloat64s(c.Sum))
	h.Write(tensor.EncodeFloat64s(c.PV))
	sum := sha256.Sum256(h.Sum(nil))
	_ = sum
	full := h.Sum(nil)
	return hex.EncodeToString(full)[:32]
}
