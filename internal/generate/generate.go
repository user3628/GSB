// Package generate produces fully deterministic local demo inputs.
package generate

import (
	"math"
	"math/rand"

	"headprune/internal/model"
)

const (
	// Seed is the fixed workspace seed used by the built-in generator.
	Seed int64 = 20261125
	// Layers / HeadsPerLayer satisfy the >=3 layers and >=4 heads requirement.
	Layers         = 3
	HeadsPerLayer  = 4
	HeadDim        = 6
	SentenceCount  = 12
	CandidateCount = 3
)

// Inputs are the generated files plus a default policy snapshot.
type Inputs struct {
	Dataset model.Dataset
}

// Build deterministically creates head shards, masked sentence pairs and the
// pruning policy snapshot. Repeated calls return byte-identical content.
func Build() Inputs {
	rng := rand.New(rand.NewSource(Seed))
	heads := make([]model.Head, 0, Layers*HeadsPerLayer)
	for layer := 0; layer < Layers; layer++ {
		for head := 0; head < HeadsPerLayer; head++ {
			weight := make([]float64, HeadDim)
			activations := make([]float64, HeadDim)
			for d := 0; d < HeadDim; d++ {
				weight[d] = round4(rng.NormFloat64() * 0.6)
				// L0H2 is the quantization-significant head: one dominant
				// long-tail activation forces a coarse scale, so the small
				// tail values collapse toward zero under low-bit quantization.
				if layer == 0 && head == 2 {
					if d == HeadDim-1 {
						activations[d] = 12.0
					} else {
						activations[d] = round4(0.01 + float64(d)*0.01)
					}
				} else {
					activations[d] = round4(rng.NormFloat64()*1.2 + 0.1)
				}
			}
			h := model.Head{
				Layer:       layer,
				Head:        head,
				Dim:         HeadDim,
				Weight:      weight,
				Activations: activations,
			}
			// L0 (H0,H1) and L2 (H0,H1) are designed strongly coupled pairs.
			if (layer == 0 || layer == 2) && head%2 == 1 {
				synergy := make([]float64, HeadDim)
				for d := range synergy {
					synergy[d] = round4(0.9 + rng.Float64()*0.4)
				}
				h.SynergyWeight = synergy
			}
			heads = append(heads, h)
		}
	}

	sentences := make([]model.Sentence, 0, SentenceCount)
	for i := 0; i < SentenceCount; i++ {
		cands := make([]model.Candidate, CandidateCount)
		for c := 0; c < CandidateCount; c++ {
			feat := make([]float64, HeadDim)
			for d := range feat {
				feat[d] = round4(rng.NormFloat64() * 0.8)
			}
			cands[c] = model.Candidate{
				Token: tokenName(i, c),
				Feat:  feat,
			}
		}
		trueIdx := rng.Intn(CandidateCount)
		cands[trueIdx].IsTrue = true
		sentences = append(sentences, model.Sentence{
			ID:         probeID(i),
			MaskedTok:  maskName(i),
			Candidates: cands,
		})
	}

	policy := model.Policy{
		Version:             1,
		CandidateRatios:     []float64{0.1, 0.2, 0.3},
		QuantBits:           4,
		Tolerance:           0.35,
		ProbeLimit:          8,
		MinProbes:           4,
		MaxPruneHeads:       4,
		CouplingThreshold:   0.15,
		RollbackGenerations: 5,
		Seed:                Seed,
	}
	return Inputs{Dataset: model.Dataset{Heads: heads, Sentences: sentences, Policy: policy}}
}

func round4(x float64) float64 {
	return math.Round(x*10000) / 10000
}

func probeID(i int) string  { return "probe-" + pad(i) }
func maskName(i int) string { return "mask-" + pad(i) }
func tokenName(i, c int) string {
	return "cand-" + pad(i) + "-" + pad(c)
}

func pad(i int) string {
	if i < 10 {
		return "0" + string(rune('0'+i))
	}
	return string(rune('0'+i/10)) + string(rune('0'+i%10))
}
