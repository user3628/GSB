// Package versioning 合成阶段指纹并推导字段触发的块级失效范围。
package versioning

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"flashattention/internal/attn"
)

const (
	TriggerPrefix    = "prefix_content"
	TriggerQContent  = "query_content"
	TriggerTiling    = "tiling_param"
	TriggerCausal    = "causal_flag"
	TriggerStrategy  = "strategy"
	TriggerQuantBits = "quant_bits"
	TriggerMemory    = "memory_plan"
)

type Stages struct {
	QFP        string      `json:"q_fp"`
	KFP        string      `json:"k_fp"`
	VFP        string      `json:"v_fp"`
	InputFP    string      `json:"input_fp"`
	Config     attn.Config `json:"config"`
	QuantBits  int         `json:"quant_bits"`
	RefFP      string      `json:"reference_fp"`
	MemoryPlan string      `json:"memory_plan_fp,omitempty"`
}

type Fingerprint struct {
	Stages Stages `json:"stages"`
	Hash   string `json:"hash"`
}

func Compute(stages Stages) Fingerprint {
	raw, err := json.Marshal(stages)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(raw)
	return Fingerprint{Stages: stages, Hash: hex.EncodeToString(sum[:])[:32]}
}

type Change struct {
	Trigger  string `json:"trigger,omitempty"`
	KeyStart int    `json:"key_start,omitempty"`
	KeyEnd   int    `json:"key_end,omitempty"`
	RowStart int    `json:"row_start,omitempty"`
	RowEnd   int    `json:"row_end,omitempty"`
}

type Range struct {
	ShardID        string `json:"shard_id"`
	Trigger        string `json:"trigger"`
	OldFingerprint string `json:"old_fingerprint"`
	NewFingerprint string `json:"new_fingerprint"`
	FirstRowBlock  int    `json:"first_row_block"`
	LastRowBlock   int    `json:"last_row_block"`
	RowStart       int    `json:"row_start"`
	RowEnd         int    `json:"row_end"`
	FirstColBlock  int    `json:"first_col_block"`
	LastColBlock   int    `json:"last_col_block"`
	ColStart       int    `json:"col_start"`
	ColEnd         int    `json:"col_end"`
	AllRowBlocks   bool   `json:"all_row_blocks"`
	AllColBlocks   bool   `json:"all_col_blocks"`
	Reason         string `json:"reason"`
}

func Diff(shardID string, oldFP, newFP Fingerprint, seqLen int, change Change) []Range {
	o, n := oldFP.Stages, newFP.Stages
	rb := maxInt(1, n.Config.RowBlock)
	cb := maxInt(1, n.Config.ColBlock)
	lastRB := (seqLen - 1) / rb
	lastCB := (n.Config.KeyLen - 1 + cb) / cb
	if n.Config.KeyLen <= 0 {
		lastCB = (seqLen - 1) / cb
	}
	all := func(trigger, reason string) Range {
		return Range{
			ShardID: shardID, Trigger: trigger, OldFingerprint: oldFP.Hash, NewFingerprint: newFP.Hash,
			FirstRowBlock: 0, LastRowBlock: lastRB, RowStart: 0, RowEnd: seqLen,
			FirstColBlock: 0, LastColBlock: lastCB, ColStart: 0, ColEnd: seqLen,
			AllRowBlocks: true, AllColBlocks: true, Reason: reason,
		}
	}

	var ranges []Range
	if o.KFP != n.KFP || o.VFP != n.VFP || (change.Trigger == TriggerPrefix) {
		start, end := change.KeyStart, change.KeyEnd
		if end <= start {
			start, end = 0, seqLen
		}
		start = clamp(start, 0, seqLen)
		end = clamp(end, start+1, seqLen)
		firstCB := start / cb
		lastCB = (end - 1) / cb
		firstRB := 0
		if n.Config.Causal {
			firstRB = start / rb
		}
		r := Range{
			ShardID: shardID, Trigger: TriggerPrefix, OldFingerprint: oldFP.Hash, NewFingerprint: newFP.Hash,
			FirstRowBlock: firstRB, LastRowBlock: lastRB,
			RowStart: firstRB * rb, RowEnd: seqLen,
			FirstColBlock: firstCB, LastColBlock: lastCB,
			ColStart: firstCB * cb, ColEnd: minInt(seqLen, (lastCB+1)*cb),
			Reason: fmt.Sprintf("K/V changed in key interval [%d,%d); causal first affected query row block=%d", start, end, firstRB),
		}
		ranges = append(ranges, r)
	}
	if o.QFP != n.QFP {
		start, end := change.RowStart, change.RowEnd
		if end <= start {
			start, end = 0, seqLen
		}
		start = clamp(start, 0, seqLen)
		end = clamp(end, start+1, seqLen)
		firstRB := start / rb
		lastQRB := (end - 1) / rb
		r := all(TriggerQContent, fmt.Sprintf("Q changed in row interval [%d,%d); only row blocks [%d,%d] rebuild", start, end, firstRB, lastQRB))
		r.FirstRowBlock, r.LastRowBlock = firstRB, lastQRB
		r.AllColBlocks = true
		ranges = append(ranges, r)
	}
	if o.Config.RowBlock != n.Config.RowBlock || o.Config.ColBlock != n.Config.ColBlock || o.Config.GroupBlocks != n.Config.GroupBlocks {
		ranges = append(ranges, all(TriggerTiling, "row/col block or checkpoint group changed"))
	}
	if o.Config.Causal != n.Config.Causal {
		ranges = append(ranges, all(TriggerCausal, "causal mask switch changed"))
	}
	if o.Config.Strategy != n.Config.Strategy {
		ranges = append(ranges, all(TriggerStrategy, "online accumulation strategy changed"))
	}
	if o.QuantBits != n.QuantBits {
		ranges = append(ranges, all(TriggerQuantBits, "quantization bit width or fixed-point reference changed"))
	}
	if len(ranges) == 0 && oldFP.Hash != newFP.Hash {
		ranges = append(ranges, all("unknown_fingerprint", "fingerprint changed without a recognized stage diff"))
	}
	return ranges
}

func clamp(x, lo, hi int) int {
	if x < lo {
		return lo
	}
	if x > hi {
		return hi
	}
	return x
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
