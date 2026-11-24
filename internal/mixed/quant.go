package mixed

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"

	"ampcheckpoint/internal/domain"
)

func Float32ToHalfBits(f float32) uint16 {
	x := math.Float32bits(f)
	sign := uint16((x >> 16) & 0x8000)
	exp := int((x >> 23) & 0xff)
	mant := x & 0x7fffff
	if exp == 0xff {
		if mant != 0 {
			return sign | 0x7e00
		}
		return sign | 0x7c00
	}
	e := exp - 127 + 15
	if e >= 0x1f {
		return sign | 0x7c00
	}
	if e <= 0 {
		if e < -10 {
			return sign
		}
		mant |= 0x800000
		shift := uint32(14 - e)
		halfMant := mant >> shift
		rem := mant & ((uint32(1) << shift) - 1)
		halfway := uint32(1) << (shift - 1)
		if rem > halfway || (rem == halfway && halfMant&1 == 1) {
			halfMant++
		}
		return sign | uint16(halfMant)
	}
	out := sign | uint16(e<<10) | uint16(mant>>13)
	rem := mant & 0x1fff
	if rem > 0x1000 || (rem == 0x1000 && out&1 == 1) {
		out++
	}
	return out
}

func HalfBitsToFloat32(h uint16) float32 {
	sign := uint32(h&0x8000) << 16
	exp := uint32(h>>10) & 0x1f
	mant := uint32(h & 0x03ff)
	if exp == 0 {
		if mant == 0 {
			return math.Float32frombits(sign)
		}
		e := uint32(127 - 15 + 1)
		for mant&0x400 == 0 {
			mant <<= 1
			e--
		}
		mant &= 0x03ff
		return math.Float32frombits(sign | (e << 23) | (mant << 13))
	}
	if exp == 0x1f {
		if mant == 0 {
			return math.Float32frombits(sign | 0x7f800000)
		}
		return math.Float32frombits(sign | 0x7fc00000)
	}
	return math.Float32frombits(sign | ((exp + 112) << 23) | (mant << 13))
}

func MasterDigest(shardID string, round int64, values []int64) string {
	h := sha256.New()
	fmt.Fprintf(h, "master-fixed/v1|%s|%d|%d|", shardID, round, len(values))
	var buf [8]byte
	for _, v := range values {
		binary.LittleEndian.PutUint64(buf[:], uint64(v))
		h.Write(buf[:])
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func MomentumDigest(seed int64, shardID string, round int64) string {
	h := sha256.New()
	fmt.Fprintf(h, "momentum/v1|%d|%s|%d", seed, shardID, round)
	return fmt.Sprintf("%x", h.Sum(nil))
}

type Quantization struct {
	Bits             []uint16
	RestoredFixed    []int64
	Digest           string
	RestoredDigest   string
	MaxAbsDeviation  int64
	MaxRelativePPM   int64
	CumulativeAbsDev int64
}

func Quantize(shardID string, round, lossScaleFixed int64, master []int64) (Quantization, error) {
	var out Quantization
	if lossScaleFixed <= 0 {
		return out, fmt.Errorf("loss scale must be positive")
	}
	scale := float64(lossScaleFixed) / float64(domain.FixedScale)
	out.Bits = make([]uint16, len(master))
	out.RestoredFixed = make([]int64, len(master))
	for i, v := range master {
		scaled := (float64(v) / float64(domain.FixedScale)) * scale
		half := Float32ToHalfBits(float32(scaled))
		if half&0x7c00 == 0x7c00 {
			return out, fmt.Errorf("FP16 overflow at element %d", i)
		}
		restoredFloat := float64(HalfBitsToFloat32(half)) / scale
		restored := int64(math.Round(restoredFloat * float64(domain.FixedScale)))
		dev := restored - v
		absDev := dev
		if absDev < 0 {
			absDev = -absDev
		}
		if absDev > out.MaxAbsDeviation {
			out.MaxAbsDeviation = absDev
		}
		denom := v
		if denom < 0 {
			denom = -denom
		}
		var rel int64
		if denom == 0 {
			if absDev != 0 {
				rel = 1_000_000_000
			}
		} else {
			rel = absDev * int64(domain.FixedScale) / denom
		}
		if rel > out.MaxRelativePPM {
			out.MaxRelativePPM = rel
		}
		out.CumulativeAbsDev += absDev
		out.Bits[i] = half
		out.RestoredFixed[i] = restored
	}
	out.Digest = fp16Digest(shardID, round, lossScaleFixed, out.Bits)
	out.RestoredDigest = restoredDigest(shardID, round, lossScaleFixed, out.RestoredFixed)
	return out, nil
}

func fp16Digest(shardID string, round, lossScaleFixed int64, bits []uint16) string {
	h := sha256.New()
	fmt.Fprintf(h, "fp16/binary16-rne/v1|%s|%d|%d|", shardID, round, lossScaleFixed)
	var buf [2]byte
	for _, v := range bits {
		binary.LittleEndian.PutUint16(buf[:], v)
		h.Write(buf[:])
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func restoredDigest(shardID string, round, lossScaleFixed int64, values []int64) string {
	h := sha256.New()
	fmt.Fprintf(h, "restored-fixed/v1|%s|%d|%d|", shardID, round, lossScaleFixed)
	var buf [8]byte
	for _, v := range values {
		binary.LittleEndian.PutUint64(buf[:], uint64(v))
		h.Write(buf[:])
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}
