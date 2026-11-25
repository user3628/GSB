// Package control implements stage 1: prompt normalization, tokenization and
// deterministic 32-dimensional unit control vector generation.
package control

import (
	"math"

	"matbench/internal/errorsx"
	"matbench/internal/fp"
	"matbench/internal/textnorm"
)

// VectorDim is the fixed control-vector dimensionality.
const VectorDim = 32

// HashSeed is the fixed hash seed mixed into every token hash and recorded in
// the stage diagnostics for reproducibility.
const HashSeed uint64 = 0x4D41544248323032 // "MATBH202"

// TokenWeight records a token and its analytically derived weight.
type TokenWeight struct {
	Token  string  `json:"token"`
	Weight float64 `json:"weight"`
}

// Result is the stage-1 artifact.
type Result struct {
	MaterialID  string             `json:"material_id"`
	Normalized  string             `json:"normalized_prompt"`
	Tokens      []TokenWeight      `json:"tokens"`
	Vector      [VectorDim]float64 `json:"vector"`
	HashSeed    uint64             `json:"hash_seed"`
	Fingerprint string             `json:"fingerprint"`
}

// hash64 is a fixed splitmix64-based string hash seeded by HashSeed.
func hash64(s string) uint64 {
	const prime = 0x100000001B3
	h := HashSeed ^ 0xCBF29CE484222325
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	z := h + 0x9E3779B97F4A7C15
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// tokenWeight assigns deterministic weights from token length and position,
// then normalizes so weights form a probability distribution.
func tokenWeight(token string, index, total int) float64 {
	raw := (1.0 + float64(len([]rune(token)))) * (1.0 + float64(total-index)/float64(1+total))
	return raw
}

// Generate builds the control vector for a material prompt.
func Generate(materialID, prompt string) (*Result, error) {
	tokens := textnorm.Tokenize(prompt)
	if len(tokens) == 0 {
		return nil, errorsx.New(errorsx.EEmptyPrompt, materialID, "stage control: prompt empty after normalization")
	}
	tws := make([]TokenWeight, len(tokens))
	weightSum := 0.0
	for i, t := range tokens {
		w := tokenWeight(t, i, len(tokens))
		tws[i] = TokenWeight{Token: t, Weight: w}
		weightSum += w
	}
	for i := range tws {
		tws[i].Weight /= weightSum
	}

	var vec [VectorDim]float64
	for _, tw := range tws {
		h := hash64(tw.Token)
		for d := 0; d < VectorDim; d++ {
			// Two independent hash draws per component.
			hd := splitmix(h + uint64(d)*0x9E3779B97F4A7C15)
			unit := float64(hd>>11)/float64(uint64(1)<<53)*2 - 1
			vec[d] += tw.Weight * unit
		}
	}

	norm := 0.0
	for _, x := range vec {
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil, errorsx.New(errorsx.EHashDegenerate, materialID, "stage control: non-finite hash component")
		}
		norm += x * x
	}
	if norm == 0 {
		return nil, errorsx.New(errorsx.EHashDegenerate, materialID, "stage control: zero control vector")
	}
	inv := 1 / math.Sqrt(norm)
	for d := range vec {
		vec[d] *= inv
	}

	normalized := textnorm.Normalize(prompt)
	fingerprint, err := fp.Of("stage:control", map[string]any{
		"material_id": materialID,
		"normalized":  normalized,
		"tokens":      tws,
		"hash_seed":   HashSeed,
	})
	if err != nil {
		return nil, err
	}
	return &Result{
		MaterialID:  materialID,
		Normalized:  normalized,
		Tokens:      tws,
		Vector:      vec,
		HashSeed:    HashSeed,
		Fingerprint: fingerprint,
	}, nil
}

func splitmix(z uint64) uint64 {
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}
