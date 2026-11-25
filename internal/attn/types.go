// Package attn 实现 FlashAttention 风格的行块/列块在线 Softmax 前向内核。
package attn

import (
	"math"

	"flashattention/internal/errs"
	"flashattention/internal/tensor"
)

type Strategy string

const (
	Scaled Strategy = "scaled"
	LogSum Strategy = "logsum"
)

type Config struct {
	ShardID      string   `json:"shard_id"`
	RowBlock     int      `json:"row_block"`
	ColBlock     int      `json:"col_block"`
	GroupBlocks  int      `json:"group_blocks"`
	Causal       bool     `json:"causal"`
	Scale        float64  `json:"scale"`
	Strategy     Strategy `json:"strategy"`
	KeyLen       int      `json:"key_len,omitempty"`
	ExpFloor     float64  `json:"exp_floor,omitempty"`
	ExtremeLogit float64  `json:"extreme_logit,omitempty"`
	ExpSumBits   int      `json:"exp_sum_bits,omitempty"`
	// StrictNumeric 是临时执行/诊断升级策略，不改变张量数学或版本指纹。
	StrictNumeric bool `json:"-"`
}

func (c *Config) Normalize(sh tensor.Shape) {
	if c.RowBlock <= 0 {
		c.RowBlock = 1
	}
	if c.ColBlock <= 0 {
		c.ColBlock = 1
	}
	if c.GroupBlocks <= 0 {
		c.GroupBlocks = 1
	}
	if c.Strategy == "" {
		c.Strategy = Scaled
	}
	if c.Scale == 0 {
		c.Scale = 1 / math.Sqrt(float64(sh.D))
	}
	if c.KeyLen <= 0 || c.KeyLen > sh.S {
		c.KeyLen = sh.S
	}
	if c.ExpFloor == 0 {
		// 约为 float32 正规指数下限；更小值对定点/输出精度无贡献。
		c.ExpFloor = -87.3365
	}
	if c.ExtremeLogit == 0 {
		c.ExtremeLogit = 80
	}
	if c.ExpSumBits == 0 {
		c.ExpSumBits = 64
	}
}

func (c Config) Validate(sh tensor.Shape) error {
	if c.RowBlock <= 0 || c.ColBlock <= 0 || c.GroupBlocks <= 0 {
		return errs.New(errs.EConfig, "block sizes and group blocks must be positive")
	}
	if c.Strategy != Scaled && c.Strategy != LogSum {
		return errs.New(errs.EConfig, "unknown online accumulation strategy %q", c.Strategy)
	}
	if !(c.Scale > 0) || math.IsNaN(c.Scale) || math.IsInf(c.Scale, 0) {
		return errs.New(errs.EConfig, "scale must be finite and positive")
	}
	if c.KeyLen < 0 || c.KeyLen > sh.S {
		return errs.New(errs.EConfig, "key_len %d outside [0,%d]", c.KeyLen, sh.S)
	}
	if c.ExpSumBits < 2 || c.ExpSumBits > 64 {
		return errs.New(errs.EConfig, "exp_sum_bits must be in [2,64]")
	}
	return nil
}

func (c Config) ExpSumLimit() float64 {
	if c.ExpSumBits >= 63 {
		return math.MaxFloat64
	}
	return float64(int64(1)<<(c.ExpSumBits-1) - 1)
}

type NumericEvent struct {
	Code     string  `json:"code"`
	Severity string  `json:"severity"`
	Kind     string  `json:"kind"`
	BH       int     `json:"bh"`
	Batch    int     `json:"batch"`
	Head     int     `json:"head"`
	RowBlock int     `json:"row_block"`
	LocalRow int     `json:"local_row"`
	Row      int     `json:"row"`
	ColBlock int     `json:"col_block,omitempty"`
	ColStart int     `json:"col_start,omitempty"`
	Col      int     `json:"col,omitempty"`
	Count    int     `json:"count"`
	Value    float64 `json:"value,omitempty"`
	Limit    float64 `json:"limit,omitempty"`
	Bits     int     `json:"bits,omitempty"`
	Message  string  `json:"message"`
}

type RowState struct {
	Valid bool      `json:"valid"`
	M     float64   `json:"m"`
	Sum   float64   `json:"sum"`
	LSE   float64   `json:"lse"`
	Acc   []float64 `json:"acc"`
}

func NewRowState(d int) *RowState { return &RowState{Acc: make([]float64, d)} }

func (s *RowState) snapshot(localRow int) Snapshot {
	return s.ExportSnapshot(localRow)
}

func (s *RowState) ExportSnapshot(localRow int) Snapshot {
	acc := make([]float64, len(s.Acc))
	copy(acc, s.Acc)
	return Snapshot{LocalRow: localRow, Valid: s.Valid, M: s.M, Sum: s.Sum, LSE: s.LSE, Acc: acc}
}

func (s *RowState) restore(sn Snapshot) error {
	if len(sn.Acc) != len(s.Acc) {
		return errs.New(errs.EState, "snapshot dim %d does not match state dim %d", len(sn.Acc), len(s.Acc))
	}
	s.Valid, s.M, s.Sum, s.LSE = sn.Valid, sn.M, sn.Sum, sn.LSE
	copy(s.Acc, sn.Acc)
	return nil
}

type Snapshot struct {
	LocalRow int       `json:"local_row"`
	Valid    bool      `json:"valid"`
	M        float64   `json:"m"`
	Sum      float64   `json:"sum"`
	LSE      float64   `json:"lse"`
	Acc      []float64 `json:"acc"`
}

type BlockContribution struct {
	BH       int       `json:"bh"`
	Batch    int       `json:"batch"`
	Head     int       `json:"head"`
	RowBlock int       `json:"row_block"`
	ColBlock int       `json:"col_block"`
	RowStart int       `json:"row_start"`
	RowEnd   int       `json:"row_end"`
	ColStart int       `json:"col_start"`
	ColEnd   int       `json:"col_end"`
	HeadDim  int       `json:"head_dim"`
	M        []float64 `json:"m"`
	Sum      []float64 `json:"sum"`
	PV       []float64 `json:"pv"`
	Hash     string    `json:"hash"`
}

type RowFinal struct {
	BH         int       `json:"bh"`
	Batch      int       `json:"batch"`
	Head       int       `json:"head"`
	Row        int       `json:"row"`
	M          float64   `json:"m"`
	Sum        float64   `json:"sum"`
	LSE        float64   `json:"lse"`
	Normalizer float64   `json:"normalizer"`
	Output     []float32 `json:"output"`
	StateHash  string    `json:"state_hash"`
	OutputHash string    `json:"output_hash"`
}

type RowBlockSummary struct {
	BH            int            `json:"bh"`
	Batch         int            `json:"batch"`
	Head          int            `json:"head"`
	RowBlock      int            `json:"row_block"`
	RowStart      int            `json:"row_start"`
	RowEnd        int            `json:"row_end"`
	ColEnd        int            `json:"col_end"`
	RowMax        []float64      `json:"row_max"`
	RowSum        []float64      `json:"row_sum"`
	RowLSE        []float64      `json:"row_lse"`
	OutputHash    string         `json:"output_hash"`
	StateHash     string         `json:"state_hash"`
	ClampCount    int            `json:"clamp_count"`
	EventCount    int            `json:"event_count"`
	Events        []NumericEvent `json:"events,omitempty"`
	CheckpointSeq int64          `json:"checkpoint_seq,omitempty"`
}

type GroupInfo struct {
	BH            int
	RowBlock      int
	ColStart      int
	ColEnd        int
	Seq           int64
	Snapshots     []Snapshot
	Contributions []BlockContribution
	Events        []NumericEvent
	Summary       *RowBlockSummary
}

type CheckpointCallback func(GroupInfo) error

type Result struct {
	Output    *tensor.Tensor    `json:"-"`
	Finals    []RowFinal        `json:"finals"`
	Summaries []RowBlockSummary `json:"summaries"`
	Events    []NumericEvent    `json:"events"`
}
