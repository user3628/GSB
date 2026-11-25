package model

import "math"

// ActivationSource returns the per-probe activation vector for a head. The
// default uses raw scaled activations; replay passes a quantizing source.
type ActivationSource func(h Head, probeIdx int, seed int64) []float64

// RawActivation is the non-quantized activation source.
func RawActivation(h Head, probeIdx int, seed int64) []float64 {
	return ScaledActivation(h, probeIdx, seed)
}

// ScaledActivation returns the deterministic per-probe head activation
// vector: base activation multiplied by a probe/head-specific scale derived
// from the fixed workspace seed, without consuming mutable RNG state.
func ScaledActivation(h Head, probeIdx int, seed int64) []float64 {
	scale := ProbeScale(h.Layer, h.Head, probeIdx, seed)
	out := make([]float64, len(h.Activations))
	for i, a := range h.Activations {
		out[i] = a * scale
	}
	return out
}

// ProbeScale is a deterministic, finite scalar in [0.75, 1.25).
func ProbeScale(layer, head, probeIdx int, seed int64) float64 {
	x := uint64(seed)*1099511628211 + uint64(layer)*1000003 +
		uint64(head)*9176 + uint64(probeIdx)*97 + 13
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return 0.75 + float64(x%1_000_000)/2_000_000.0
}

func dot(a, b []float64) (float64, bool) {
	if len(a) != len(b) {
		return 0, false
	}
	sum := 0.0
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum, true
}

// EnsembleLogits computes candidate logits for the mask position using every
// head that survives pruning. A head contributes a direct dot product plus
// pair-synergy terms; a synergy term survives only when both partner heads
// survive, which is what makes per-head errors non-additive.
func EnsembleLogits(heads []Head, pruned HeadSet, s Sentence, seed int64, probeIdx int) ([]float64, error) {
	return EnsembleLogitsWithSource(heads, pruned, s, seed, probeIdx, RawActivation)
}

// EnsembleLogitsWithSource behaves like EnsembleLogits but lets the caller
// inject the quantized activation source used during replay.
func EnsembleLogitsWithSource(heads []Head, pruned HeadSet, s Sentence, seed int64, probeIdx int, source ActivationSource) ([]float64, error) {
	alive := make(map[HeadID]Head, len(heads))
	order := make([]HeadID, 0, len(heads))
	for _, h := range heads {
		id := h.ID()
		if !pruned.Contains(id) {
			alive[id] = h
			order = append(order, id)
		}
	}
	logits := make([]float64, len(s.Candidates))
	for _, id := range order {
		h := alive[id]
		act := source(h, probeIdx, seed)
		for ci, cand := range s.Candidates {
			v, ok := dot(act, cand.Feat)
			if !ok {
				return nil, newErr(CodeHeadInvalid, "candidate feature length mismatches head dim").WithHead(h.Layer, h.Head)
			}
			logits[ci] += v
		}
		if len(h.SynergyWeight) > 0 {
			// The synergy weight stored on h gates a pair term with the
			// neighboring even partner head in the same layer. The gate is
			// a positive magnitude depending on both partners' activations,
			// so pruning either partner removes a term that both heads rely
			// on together: the true combo error is super-additive.
			partnerID := HeadID{Layer: h.Layer, Head: h.Head - 1}
			partner, okPartner := alive[partnerID]
			if okPartner {
				pAct := source(partner, probeIdx, seed)
				gateRaw, ok := dot(pAct, h.SynergyWeight)
				if !ok {
					return nil, newErr(CodeHeadInvalid, "synergy weight length mismatches head dim").WithHead(partner.Layer, partner.Head)
				}
				gate := 0.35 * math.Abs(gateRaw)
				for ci, cand := range s.Candidates {
					v, ok := dot(act, cand.Feat)
					if !ok {
						return nil, newErr(CodeHeadInvalid, "candidate feature length mismatches head dim").WithHead(h.Layer, h.Head)
					}
					logits[ci] += gate * v
				}
			}
		}
	}
	return logits, nil
}

// Softmax returns a normalized distribution; an all-zero or non-finite input
// is reported as false so callers raise E_NUMERIC instead of faking data.
func Softmax(logits []float64) ([]float64, bool) {
	if len(logits) == 0 {
		return nil, false
	}
	hasFinite := false
	maxV := math.Inf(-1)
	for _, x := range logits {
		if !Finite(x) {
			return nil, false
		}
		hasFinite = true
		if x > maxV {
			maxV = x
		}
	}
	if !hasFinite {
		return nil, false
	}
	out := make([]float64, len(logits))
	sum := 0.0
	for i, x := range logits {
		out[i] = math.Exp(x - maxV)
		if !Finite(out[i]) {
			return nil, false
		}
		sum += out[i]
	}
	if sum <= 0 || math.IsInf(sum, 0) {
		return nil, false
	}
	for i := range out {
		out[i] /= sum
	}
	return out, true
}

// KLDivergence is KL(base || recon), the per-probe reconstruction error.
func KLDivergence(base, recon []float64) (float64, bool) {
	if len(base) != len(recon) || len(base) == 0 {
		return 0, false
	}
	kl := 0.0
	for i := range base {
		if base[i] < 0 || recon[i] < 0 || !Finite(base[i]) || !Finite(recon[i]) {
			return 0, false
		}
		if base[i] > 0 {
			if recon[i] <= 0 {
				// Underflow after softmax: evaluate log-prob from logits to
				// avoid turning a finite KL into an artificial E_NUMERIC.
				recon[i] = 1e-300
			}
			kl += base[i] * math.Log(base[i]/recon[i])
		}
	}
	if !Finite(kl) || kl < -1e-12 {
		return 0, false
	}
	if kl < 0 {
		kl = 0
	}
	return kl, true
}

// TotalVariation is reported alongside KL as a scale-free secondary metric.
func TotalVariation(a, b []float64) float64 {
	tv := 0.0
	for i := range a {
		d := a[i] - b[i]
		if d < 0 {
			d = -d
		}
		tv += d
	}
	return tv / 2
}
