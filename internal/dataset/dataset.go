// Package dataset 以内置固定种子确定性生成张量、配置与定点参考真值。
package dataset

import (
	"path/filepath"

	"flashattention/internal/attn"
	"flashattention/internal/reference"
	"flashattention/internal/tensor"
)

type Spec struct {
	ID         string        `json:"id"`
	B          int           `json:"batch"`
	H          int           `json:"heads"`
	S          int           `json:"seq_len"`
	D          int           `json:"head_dim"`
	Causal     bool          `json:"causal"`
	RowBlock   int           `json:"row_block"`
	ColBlock   int           `json:"col_block"`
	Group      int           `json:"group_blocks"`
	Bits       int           `json:"quant_bits"`
	ExpSumBits int           `json:"exp_sum_bits"`
	Strategy   attn.Strategy `json:"strategy"`
	Extreme    bool          `json:"extreme"`
	Seed       uint64        `json:"seed"`
	KeyLen     int           `json:"key_len,omitempty"`
}

func (s Spec) Shape() tensor.Shape { return tensor.Shape{B: s.B, H: s.H, S: s.S, D: s.D} }

func (s Spec) Scale() float64 { return 1 / sqrtFloat(float64(s.D)) }

func (s Spec) Config() attn.Config {
	keyLen := s.KeyLen
	if keyLen <= 0 {
		keyLen = s.S
	}
	expBits := s.ExpSumBits
	if expBits <= 0 {
		expBits = 64
	}
	return attn.Config{
		ShardID: s.ID, RowBlock: s.RowBlock, ColBlock: s.ColBlock, GroupBlocks: s.Group,
		Causal: s.Causal, Scale: s.Scale(), Strategy: s.Strategy, KeyLen: keyLen,
		ExpSumBits: expBits,
	}
}

type Bundle struct {
	SchemaVersion string            `json:"schema_version"`
	Spec          Spec              `json:"spec"`
	Shape         tensor.Shape      `json:"shape"`
	Config        attn.Config       `json:"config"`
	Q             *tensor.Tensor    `json:"q"`
	K             *tensor.Tensor    `json:"k"`
	V             *tensor.Tensor    `json:"v"`
	Reference     *reference.Output `json:"reference"`
	InputFP       string            `json:"input_fp"`
}

func Catalog() []Spec {
	return []Spec{
		{ID: "short", B: 2, H: 2, S: 16, D: 8, RowBlock: 4, ColBlock: 4, Group: 1, Bits: 32, ExpSumBits: 64, Strategy: attn.Scaled, Seed: 1001},
		{ID: "long", B: 1, H: 2, S: 512, D: 32, RowBlock: 32, ColBlock: 32, Group: 2, Bits: 24, ExpSumBits: 64, Strategy: attn.Scaled, Seed: 2002},
		{ID: "odd_dim", B: 1, H: 1, S: 37, D: 13, RowBlock: 8, ColBlock: 7, Group: 1, Bits: 24, ExpSumBits: 64, Strategy: attn.Scaled, Seed: 3003},
		{ID: "causal", B: 2, H: 2, S: 48, D: 24, Causal: true, RowBlock: 12, ColBlock: 12, Group: 1, Bits: 24, ExpSumBits: 64, Strategy: attn.LogSum, Seed: 4004},
		{ID: "extreme", B: 1, H: 1, S: 160, D: 8, RowBlock: 8, ColBlock: 8, Group: 1, Bits: 16, ExpSumBits: 8, Strategy: attn.LogSum, Extreme: true, Seed: 5005},
	}
}

func Generate(spec Spec) *Bundle {
	sh := spec.Shape()
	q := tensor.Deterministic(sh, spec.Seed, -1, 1)
	k := tensor.Deterministic(sh, spec.Seed+17, -1, 1)
	v := tensor.Deterministic(sh, spec.Seed+31, -1, 1)

	if spec.Extreme {
		for d := 0; d < spec.D; d++ {
			q.Set(0, 0, 0, d, 10)
			k.Set(0, 0, 0, d, 10)
			q.Set(0, 0, 1, d, 0)
		}
	}

	cfg := spec.Config()
	ref := reference.Compute(q, k, v, spec.Causal, spec.Scale(), spec.Bits, cfg.KeyLen)
	b := &Bundle{
		SchemaVersion: "flashattn/v1", Spec: spec, Shape: sh, Config: cfg,
		Q: q, K: k, V: v, Reference: ref,
	}
	b.InputFP = b.ComputeInputFP()
	return b
}

func (b *Bundle) ComputeInputFP() string {
	return tensor.HashFloat64s([]float64{
		float64(0), // domain separator not needed; fingerprints are SHA chains below
	}) + ":" + b.Q.Fingerprint() + ":" + b.K.Fingerprint() + ":" + b.V.Fingerprint()
}

func (b *Bundle) Save(dir string) error {
	return tensor.SaveJSON(filepath.Join(dir, "shard.json"), b)
}

func Load(path string) (*Bundle, error) {
	var b Bundle
	if err := tensor.LoadJSON(path, &b); err != nil {
		return nil, err
	}
	if b.Reference != nil {
		b.Reference = reference.FromCodes(b.Shape, b.Reference.Codes, b.Reference.Scheme)
	}
	return &b, nil
}

func Dir(root, id string) string { return filepath.Join(root, id) }

func Path(root, id string) string { return filepath.Join(Dir(root, id), "shard.json") }
