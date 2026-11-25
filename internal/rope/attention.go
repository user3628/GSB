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

func NewSoftmaxState(headDim int) model.SoftmaxState {
	return model.SoftmaxState{Max: math.Inf(-1), SumExp: 0, Weighted: make([]float64, headDim)}
}

// ConsumeKeyRange performs one online-softmax update over [keyStart,keyEnd).
// The caller can publish the returned state after every column block.
func ConsumeKeyRange(q, k []float64, seqLen, headDim int, queryIndex int, causal bool, keyStart, keyEnd int, state *model.SoftmaxState, rowBlock, colBlock int, shardID string) error {
	if state == nil || len(q) != headDim || len(state.Weighted) != headDim {
		return &model.AppError{Code: model.CodeNumeric, ShardID: shardID, RowBlock: rowBlock, ColBlock: colBlock, Message: "invalid softmax state dimensions"}
	}
	if keyStart < 0 {
		keyStart = 0
	}
	if keyEnd > seqLen {
		keyEnd = seqLen
	}
	if causal && keyEnd > queryIndex+1 {
		keyEnd = queryIndex + 1
	}
	if keyStart >= keyEnd {
		return nil
	}
	invSqrt := 1 / math.Sqrt(float64(headDim))
	for key := keyStart; key < keyEnd; key++ {
		for d := 0; d < headDim; d++ {
			if math.IsNaN(q[d]) || math.IsInf(q[d], 0) || math.IsNaN(k[key*headDim+d]) || math.IsInf(k[key*headDim+d], 0) {
				return numericError(shardID, queryIndex, rowBlock, colBlock, "non-finite q/k element", float64(key))
			}
		}
		dot := 0.0
		for d := 0; d < headDim; d++ {
			dot += q[d] * k[key*headDim+d]
		}
		score := dot * invSqrt
		if math.IsNaN(score) || math.IsInf(score, 0) {
			return numericError(shardID, queryIndex, rowBlock, colBlock, "non-finite attention score", score)
		}
		if math.IsInf(state.Max, -1) {
			state.Max = score
			state.SumExp = 1
			for d := range state.Weighted {
				state.Weighted[d] = k[key*headDim+d]
			}
			continue
		}
		newMax := math.Max(state.Max, score)
		oldScale := math.Exp(state.Max - newMax)
		newScale := math.Exp(score - newMax)
		if math.IsNaN(oldScale) || math.IsNaN(newScale) || math.IsInf(oldScale, 0) || math.IsInf(newScale, 0) {
			return numericError(shardID, queryIndex, rowBlock, colBlock, "non-finite softmax exponential", newScale)
		}
		state.SumExp = state.SumExp*oldScale + newScale
		for d := 0; d < headDim; d++ {
			state.Weighted[d] = state.Weighted[d]*oldScale + newScale*k[key*headDim+d]
		}
		state.Max = newMax
	}
	if math.IsNaN(state.SumExp) || math.IsInf(state.SumExp, 0) || state.SumExp <= 0 {
		return numericError(shardID, queryIndex, rowBlock, colBlock, "normalization factor is zero or non-finite", state.SumExp)
	}
	for _, v := range state.Weighted {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return numericError(shardID, queryIndex, rowBlock, colBlock, "non-finite weighted output", v)
		}
	}
	return nil
}

func FinalizeAttentionRow(q, k []float64, seqLen, headDim, queryIndex int, causal bool, state model.SoftmaxState, shardID string, rowBlock int) (model.AttentionRow, error) {
	keyStart := 0
	keyEnd := seqLen
	if causal {
		keyEnd = queryIndex + 1
	}
	if keyEnd <= 0 {
		return model.AttentionRow{}, &model.AppError{Code: model.CodeNumeric, ShardID: shardID, RowBlock: rowBlock, Message: "row has no keys"}
	}
	if math.IsInf(state.Max, -1) || state.SumExp <= 0 || math.IsNaN(state.SumExp) || math.IsInf(state.SumExp, 0) {
		return model.AttentionRow{}, &model.AppError{Code: model.CodeNumeric, ShardID: shardID, RowBlock: rowBlock, Message: "invalid online softmax state"}
	}
	weights := make([]float64, seqLen)
	var weightSum float64
	for key := keyStart; key < keyEnd; key++ {
		dot := 0.0
		for d := 0; d < headDim; d++ {
			dot += q[d] * k[key*headDim+d]
		}
		score := dot / math.Sqrt(float64(headDim))
		w := math.Exp(score - state.Max)
		if math.IsNaN(w) || math.IsInf(w, 0) {
			return model.AttentionRow{}, numericError(shardID, queryIndex, rowBlock, 0, "non-finite attention weight", w)
		}
		weights[key] = w
		weightSum += w
	}
	if weightSum <= 0 || math.IsNaN(weightSum) || math.IsInf(weightSum, 0) {
		return model.AttentionRow{}, numericError(shardID, queryIndex, rowBlock, 0, "normalization factor is zero or non-finite", weightSum)
	}
	output := make([]float64, headDim)
	if len(state.Weighted) != headDim {
		return model.AttentionRow{}, numericError(shardID, queryIndex, rowBlock, 0, "partial output dimension mismatch", float64(len(state.Weighted)))
	}
	copy(output, state.Weighted)
	for d := range output {
		output[d] /= state.SumExp
		if math.IsNaN(output[d]) || math.IsInf(output[d], 0) {
			return model.AttentionRow{}, numericError(shardID, queryIndex, rowBlock, 0, "non-finite normalized output", output[d])
		}
	}
	h := sha256.New()
	writeAttentionHeader(h, queryIndex, keyStart, keyEnd)
	for key := keyStart; key < keyEnd; key++ {
		writeInt64(h, int64(key))
		writeInt64(h, QuantizeFloat(weights[key]/state.SumExp, 1_000_000_000))
	}
	lse := state.Max + math.Log(state.SumExp)
	return model.AttentionRow{
		QueryIndex:      queryIndex,
		AllowedKeyStart: keyStart,
		AllowedKeyEnd:   keyEnd,
		MaxScore:        state.Max,
		ExpSum:          state.SumExp,
		LogSumExp:       lse,
		Normalization:   state.SumExp,
		WeightDigest:    hex.EncodeToString(h.Sum(nil)),
		OutputDigest:    digestFloats(output),
		Output:          output,
	}, nil
}

func ScoreBlockDigest(rows []model.AttentionRow) string {
	h := sha256.New()
	writeString(h, "rope-score-block-v1")
	for _, row := range rows {
		writeInt64(h, int64(row.QueryIndex))
		writeString(h, row.WeightDigest)
		writeString(h, row.OutputDigest)
		writeInt64(h, int64(math.Float64bits(row.Normalization)))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func QuantizeFloat(v float64, scale int64) int64 {
	return Quantize(v, scale)
}

func numericError(shardID string, queryIndex, rowBlock, colBlock int, message string, value float64) *model.AppError {
	return &model.AppError{
		Code:     model.CodeNumeric,
		ShardID:  shardID,
		Stage:    model.StageScore,
		RowBlock: rowBlock,
		ColBlock: colBlock,
		Message:  message,
		Details:  map[string]any{"value": value, "query_index": queryIndex},
	}
}

func writeAttentionHeader(h hash.Hash, query, keyStart, keyEnd int) {
	writeString(h, "rope-attention-weights-v1")
	writeInt64(h, int64(query))
	writeInt64(h, int64(keyStart))
	writeInt64(h, int64(keyEnd))
}

func digestFloats(values []float64) string {
	h := sha256.New()
	writeString(h, "rope-attention-output-v1")
	for _, v := range values {
		writeInt64(h, int64(math.Float64bits(v)))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func Float32Bytes(v float64) [8]byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], math.Float64bits(v))
	return b
}

func ValidateFinite(values ...float64) error {
	for _, v := range values {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("non-finite value %v", v)
		}
	}
	return nil
}
