package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"strconv"
)

func DigestBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func DigestString(values ...string) string {
	h := sha256.New()
	for _, value := range values {
		var length [8]byte
		binaryBigEndianPutUint64(length[:], uint64(len(value)))
		_, _ = h.Write(length[:])
		_, _ = h.Write([]byte(value))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func binaryBigEndianPutUint64(b []byte, v uint64) {
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
}

func CanonicalJSON(value any) []byte {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return data
}

func DigestValue(value any) string {
	return DigestBytes(CanonicalJSON(value))
}

type DeterministicRNG struct {
	state uint64
}

func NewStageRNG(policySeed int64, stage string, inputDigest string) *DeterministicRNG {
	seedMaterial := DigestString("preprocess-pipeline-v1", stage, inputDigest, int64String(policySeed))
	raw, _ := hex.DecodeString(seedMaterial[:16])
	seed := uint64(raw[0])<<56 | uint64(raw[1])<<48 | uint64(raw[2])<<40 | uint64(raw[3])<<32 |
		uint64(raw[4])<<24 | uint64(raw[5])<<16 | uint64(raw[6])<<8 | uint64(raw[7])
	return &DeterministicRNG{state: seed}
}

func int64String(v int64) string {
	return strconv.FormatInt(v, 10)
}

func (r *DeterministicRNG) NextUint64() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func (r *DeterministicRNG) Float64() float64 {
	return float64(r.NextUint64()>>11) / float64(uint64(1)<<53)
}

func NormalizeFloat(value float64, mean float64, std float64) float64 {
	if math.Abs(std) < 1e-12 {
		return 0
	}
	return (value - mean) / std
}
