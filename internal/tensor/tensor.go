// Package tensor 定义 [批量, 头数, 序列长度, 头维度] 张量及确定性摘要。
package tensor

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
)

type Shape struct {
	B int `json:"batch"`
	H int `json:"heads"`
	S int `json:"seq_len"`
	D int `json:"head_dim"`
}

func (s Shape) Numel() int { return s.B * s.H * s.S * s.D }

func (s Shape) Validate() error {
	if s.B <= 0 || s.H <= 0 || s.S <= 0 || s.D <= 0 {
		return fmt.Errorf("invalid shape %+v: all dimensions must be positive", s)
	}
	return nil
}

type Tensor struct {
	Shape Shape     `json:"shape"`
	Data  []float32 `json:"data"`
}

func New(sh Shape) *Tensor {
	if err := sh.Validate(); err != nil {
		panic(err)
	}
	return &Tensor{Shape: sh, Data: make([]float32, sh.Numel())}
}

func Deterministic(sh Shape, seed uint64, lo, hi float32) *Tensor {
	t := New(sh)
	rng := rand.New(rand.NewPCG(seed, seed*7919+104729))
	for i := range t.Data {
		t.Data[i] = lo + rng.Float32()*(hi-lo)
	}
	return t
}

func (t *Tensor) Clone() *Tensor {
	out := New(t.Shape)
	copy(out.Data, t.Data)
	return out
}

func (t *Tensor) Offset(b, h, s, d int) int {
	return ((b*t.Shape.H+h)*t.Shape.S+s)*t.Shape.D + d
}

func (t *Tensor) At(b, h, s, d int) float32 {
	return t.Data[t.Offset(b, h, s, d)]
}

func (t *Tensor) Set(b, h, s, d int, v float32) {
	t.Data[t.Offset(b, h, s, d)] = v
}

func (t *Tensor) Row(b, h, s int) []float32 {
	off := t.Offset(b, h, s, 0)
	return t.Data[off : off+t.Shape.D]
}

func (t *Tensor) BHCount() int { return t.Shape.B * t.Shape.H }

func (t *Tensor) ValidateData() error {
	if len(t.Data) != t.Shape.Numel() {
		return fmt.Errorf("data length %d does not match shape numel %d", len(t.Data), t.Shape.Numel())
	}
	for i, v := range t.Data {
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			b, h, s, d := t.Coord(i)
			return fmt.Errorf("non-finite value at b=%d h=%d s=%d d=%d", b, h, s, d)
		}
	}
	return nil
}

func (t *Tensor) Coord(flat int) (b, h, s, d int) {
	d = flat % t.Shape.D
	flat /= t.Shape.D
	s = flat % t.Shape.S
	flat /= t.Shape.S
	h = flat % t.Shape.H
	b = flat / t.Shape.H
	return
}

// Fingerprint 对形状和 float32 原始位做 SHA-256，避免 JSON 浮点格式影响。
func (t *Tensor) Fingerprint() string {
	h := sha256.New()
	var header [16]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(t.Shape.B))
	binary.LittleEndian.PutUint32(header[4:8], uint32(t.Shape.H))
	binary.LittleEndian.PutUint32(header[8:12], uint32(t.Shape.S))
	binary.LittleEndian.PutUint32(header[12:16], uint32(t.Shape.D))
	h.Write(header[:])
	var buf [4]byte
	for _, v := range t.Data {
		binary.LittleEndian.PutUint32(buf[:], math.Float32bits(v))
		h.Write(buf[:])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func HashFloat64s(xs []float64) string {
	h := sha256.New()
	var buf [8]byte
	for _, v := range xs {
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v))
		h.Write(buf[:])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func EncodeFloat64s(xs []float64) []byte {
	buf := make([]byte, 8*len(xs))
	for i, v := range xs {
		binary.LittleEndian.PutUint64(buf[i*8:], math.Float64bits(v))
	}
	return buf
}

func DecodeFloat64s(blob []byte) []float64 {
	if len(blob)%8 != 0 {
		return nil
	}
	out := make([]float64, len(blob)/8)
	for i := range out {
		out[i] = math.Float64frombits(binary.LittleEndian.Uint64(blob[i*8:]))
	}
	return out
}

type Mismatch struct {
	Index int    `json:"index"`
	B     int    `json:"batch"`
	H     int    `json:"head"`
	S     int    `json:"seq"`
	D     int    `json:"dim"`
	A     uint32 `json:"a_bits"`
	BitsB uint32 `json:"b_bits"`
}

// FirstBitMismatch 返回两个张量的首个原始位差异；nil 表示逐字节一致。
func FirstBitMismatch(a, b *Tensor) *Mismatch {
	if a.Shape != b.Shape || len(a.Data) != len(b.Data) {
		return &Mismatch{Index: -1}
	}
	for i := range a.Data {
		ab := math.Float32bits(a.Data[i])
		bb := math.Float32bits(b.Data[i])
		if ab != bb {
			bb2 := bb
			mi := &Mismatch{Index: i, A: ab, BitsB: bb2}
			mi.B, mi.H, mi.S, mi.D = a.Coord(i)
			return mi
		}
	}
	return nil
}

func SaveJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func LoadJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
