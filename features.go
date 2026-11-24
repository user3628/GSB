package rewardpack

import (
	"fmt"
	"math"
	"math/big"
	"sort"
)

const featurePrecision = 256

// BuildFeatures projects signed term features into the configured fixed
// dimension, L2-normalizes the result using 256-bit arithmetic, then quantizes
// with a shared symmetric per-vector scale. Every dimension carries its scale,
// reference value, observed error and formal half-step error bound.
func BuildFeatures(n NormalizedPair, score ScoreResult, cfg Config) (FeatureResult, error) {
	dimension := cfg.Features.Dimension
	projection := cfg.Features.Projection
	if len(projection) == 0 {
		return FeatureResult{}, &BusinessError{Code: CodeDimMismatch, PairID: n.PairID, Row: 1, Column: 1, Context: "projection matrix is empty", Message: "projection matrix is empty"}
	}
	for row, values := range projection {
		if len(values) != dimension {
			return FeatureResult{}, &BusinessError{
				Code: CodeDimMismatch, PairID: n.PairID, Row: row + 1, Column: len(values) + 1,
				Expected: dimension, Actual: len(values), Context: "projection row width does not match features.dimension",
				Message: fmt.Sprintf("projection row %d has %d columns, want %d", row+1, len(values), dimension),
			}
		}
		for column, value := range values {
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return FeatureResult{}, &BusinessError{
					Code: CodeDimMismatch, PairID: n.PairID, Row: row + 1, Column: column + 1,
					Actual: value, Context: "projection matrix contains NaN or Inf",
					Message: fmt.Sprintf("projection[%d][%d] is not finite", row, column),
				}
			}
		}
	}

	base := baseVector(n, score, cfg)
	unitBase := unitVectorFloat(base)
	refs, projectedMax, err := projectHighPrecision(unitBase, projection, n.PairID)
	if err != nil {
		return FeatureResult{}, err
	}
	if projectedMax.Sign() == 0 {
		return FeatureResult{}, &BusinessError{
			Code: CodeDimMismatch, PairID: n.PairID, Row: 1, Column: 1,
			Context: "quantization scale is zero because the projected reference vector is all zero",
			Message: "quantization scale cannot be zero",
		}
	}

	outputDimension := len(projection)
	maxQ := new(big.Int).Lsh(big.NewInt(1), uint(cfg.Features.QuantizationBits-1))
	maxQ.Sub(maxQ, big.NewInt(1))
	scaleFloat := new(big.Float).SetPrec(featurePrecision).SetInt(maxQ)
	scaleFloat.Quo(scaleFloat, projectedMax)
	if scaleFloat.Sign() == 0 {
		return FeatureResult{}, &BusinessError{
			Code: CodeDimMismatch, PairID: n.PairID, Row: 1, Column: 1,
			Context: "quantization scale is zero", Message: "quantization scale cannot be zero",
		}
	}
	scale, _ := scaleFloat.Float64()
	if !finite(scale) || scale == 0 {
		return FeatureResult{}, &BusinessError{
			Code: CodeDimMismatch, PairID: n.PairID, Row: 1, Column: 1,
			Context: "quantization scale is not finite", Message: "quantization scale must be finite and non-zero",
		}
	}
	errorBoundFloat := new(big.Float).SetPrec(featurePrecision).Quo(big.NewFloat(0.5), scaleFloat)
	errorBound, _ := errorBoundFloat.Float64()

	dimensions := make([]QuantizedDimension, outputDimension)
	for i, ref := range refs {
		scaled := new(big.Float).SetPrec(featurePrecision).Mul(ref, scaleFloat)
		// big.Float.Int truncates toward zero. Use +/- 0.5 before truncation to
		// implement deterministic round-half-away-from-zero.
		rounded := roundBigToInt(scaled)
		if rounded.Cmp(maxQ) > 0 {
			rounded.Set(maxQ)
		}
		negativeMax := new(big.Int).Neg(maxQ)
		if rounded.Cmp(negativeMax) < 0 {
			rounded.Set(negativeMax)
		}
		quantized := rounded.Int64()
		dequantized := new(big.Float).SetPrec(featurePrecision).SetInt(rounded)
		dequantized.Quo(dequantized, scaleFloat)
		observed := new(big.Float).SetPrec(featurePrecision).Sub(ref, dequantized)
		observed.Abs(observed)
		observedFloat, _ := observed.Float64()
		dimensions[i] = QuantizedDimension{
			Index:         i,
			Scale:         scale,
			Quantized:     quantized,
			ErrorBound:    errorBound,
			ObservedError: observedFloat,
			Reference:     ref.Text('g', 20),
		}
	}

	result := FeatureResult{
		PairID:           n.PairID,
		Source:           n.Source,
		Lang:             n.Lang,
		InputDimension:   dimension,
		OutputDimension:  outputDimension,
		QuantizationBits: cfg.Features.QuantizationBits,
		Dimensions:       dimensions,
		InputDigest:      hashBytes([]byte("features\x00"), []byte(score.Digest), []byte(cfg.FeaturesDigest())),
	}
	result.Digest = digestJSON(result)
	return result, nil
}

func baseVector(n NormalizedPair, score ScoreResult, cfg Config) []float64 {
	dimension := cfg.Features.Dimension
	vector := make([]float64, dimension)
	chosen := countsByTerm(n.Chosen)
	rejected := countsByTerm(n.Rejected)
	terms := make([]string, 0, len(chosen)+len(rejected))
	seen := map[string]bool{}
	for term := range chosen {
		seen[term] = true
		terms = append(terms, term)
	}
	for term := range rejected {
		if !seen[term] {
			terms = append(terms, term)
		}
	}
	sort.Strings(terms)
	for _, term := range terms {
		difference := float64(chosen[term] - rejected[term])
		if difference == 0 {
			continue
		}
		bucket := int(hashTerm(term) % uint64(dimension))
		vector[bucket] += difference * termWeight(term, cfg)
	}
	lengthScalar := math.Log(float64(1+n.Chosen.TokenCount) / float64(1+n.Rejected.TokenCount))
	lengthBucket := int(hashTerm("\x00length") % uint64(dimension))
	vector[lengthBucket] += lengthScalar
	consistencyBucket := int(hashTerm("\x00consistency") % uint64(dimension))
	vector[consistencyBucket] += score.Strength
	// A unit bias keeps the base vector non-zero even for two identical replies.
	biasBucket := int(hashTerm("\x00unit-bias") % uint64(dimension))
	vector[biasBucket] += 1e-9
	return vector
}

func countsByTerm(features TextFeatures) map[string]int {
	counts := make(map[string]int, len(features.Terms))
	for _, term := range features.Terms {
		counts[term.Term] = term.Count
	}
	return counts
}

func hashTerm(term string) uint64 {
	const offset64 = 14695981039346656037
	const prime64 = 1099511628211
	h := uint64(offset64)
	for i := 0; i < len(term); i++ {
		h ^= uint64(term[i])
		h *= prime64
	}
	return h
}

func unitVectorFloat(vector []float64) []float64 {
	sum := 0.0
	for _, value := range vector {
		sum += value * value
	}
	if sum == 0 || !finite(sum) {
		return vector
	}
	norm := math.Sqrt(sum)
	out := make([]float64, len(vector))
	for i, value := range vector {
		out[i] = value / norm
	}
	return out
}

func projectHighPrecision(base []float64, projection [][]float64, pairID string) ([]*big.Float, *big.Float, error) {
	rows := len(projection)
	raw := make([]*big.Float, rows)
	normSquared := new(big.Float).SetPrec(featurePrecision)
	for row := 0; row < rows; row++ {
		sum := new(big.Float).SetPrec(featurePrecision)
		for column, baseValue := range base {
			matrixValue := new(big.Float).SetPrec(featurePrecision).SetFloat64(projection[row][column])
			value := new(big.Float).SetPrec(featurePrecision).SetFloat64(baseValue)
			value.Mul(value, matrixValue)
			sum.Add(sum, value)
		}
		raw[row] = sum
		square := new(big.Float).SetPrec(featurePrecision).Mul(sum, sum)
		normSquared.Add(normSquared, square)
	}
	normSquaredFloat, _ := normSquared.Float64()
	if !finite(normSquaredFloat) || normSquared.Sign() == 0 {
		return nil, nil, &BusinessError{
			Code: CodeDimMismatch, PairID: pairID, Row: 1, Column: 1,
			Context: "projected reference vector is all zero, so the quantization scale would be zero",
			Message: "quantization scale cannot be zero",
		}
	}
	norm := new(big.Float).SetPrec(featurePrecision).Sqrt(normSquared)
	refs := make([]*big.Float, rows)
	maxAbs := new(big.Float).SetPrec(featurePrecision)
	for i, value := range raw {
		unit := new(big.Float).SetPrec(featurePrecision).Quo(value, norm)
		refs[i] = unit
		abs := new(big.Float).SetPrec(featurePrecision).Abs(unit)
		if abs.Cmp(maxAbs) > 0 {
			maxAbs.Set(abs)
		}
	}
	return refs, maxAbs, nil
}

func roundBigToInt(value *big.Float) *big.Int {
	if value.Sign() >= 0 {
		shifted := new(big.Float).SetPrec(featurePrecision).Add(value, big.NewFloat(0.5))
		result, _ := shifted.Int(nil)
		return result
	}
	shifted := new(big.Float).SetPrec(featurePrecision).Sub(value, big.NewFloat(0.5))
	result, _ := shifted.Int(nil)
	return result
}

// QuantizationHex is a compact stable representation useful for external
// artifact comparison.
func (f FeatureResult) QuantizationHex() string {
	b := make([]byte, len(f.Dimensions)*8)
	for i, dim := range f.Dimensions {
		u := uint64(dim.Quantized)
		for j := 0; j < 8; j++ {
			b[i*8+j] = byte(u >> (8 * j))
		}
	}
	return digestBytes(b)
}
