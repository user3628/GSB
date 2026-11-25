package engine

import (
	"fmt"
	"math"

	"queryrewrite/internal/spec"
)

// Stage3 adjudicates each candidate in the log domain. Every numeric input is
// explicitly validated: zero/negative weights, non-finite log terms or an
// upper bound outside [0,1] produce a structured E_NUMERIC error carrying the
// candidate identifier.
func Stage3(cands []spec.Candidate, hits []spec.HitResult, p spec.Policy, cache *Cache, turn int) []spec.AdjResult {
	byID := map[string]spec.HitResult{}
	for _, h := range hits {
		byID[h.CandidateID] = h
	}
	out := make([]spec.AdjResult, 0, len(cands))
	for _, cand := range cands {
		hr := byID[cand.ID]
		res := spec.AdjResult{
			CandidateID: cand.ID,
			Components:  map[string]float64{},
			Derivation:  map[string]string{},
		}
		if err := validateWeights(p.Weights, cand.ID); err != nil {
			res.NumericError = err
			out = append(out, res)
			continue
		}

		hitFactor := hitFactor(hr.Outcome)
		age, blockCount, slotID := slotContext(hr, cache, turn)

		// Log-domain benefit:
		// log(benefit) = Wo*log(1+overlap) + Wh*log(hitFactor) - Wa*log(1+age)
		overlapTerm := math.Log1p(cand.Overlap)
		hitTerm := math.Log(hitFactor)
		ageTerm := math.Log1p(float64(age))
		for name, v := range map[string]float64{
			"log_overlap_term": overlapTerm, "log_hit_term": hitTerm, "log_age_term": ageTerm,
		} {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				res.NumericError = numericErr(cand.ID, "non-finite log-domain term",
					map[string]string{"term": name, "value": fmt.Sprintf("%g", v)})
				break
			}
		}
		if res.NumericError != nil {
			out = append(out, res)
			continue
		}

		logBenefit := p.Weights.Overlap*overlapTerm + p.Weights.Hit*hitTerm - p.Weights.Age*ageTerm
		if math.IsNaN(logBenefit) || math.IsInf(logBenefit, 0) {
			res.NumericError = numericErr(cand.ID, "non-finite composed benefit",
				map[string]string{"log_benefit": fmt.Sprintf("%g", logBenefit)})
			out = append(out, res)
			continue
		}
		benefit := math.Exp(logBenefit)

		// Failure upper bound, derived explicitly and monotonically from slot
		// age, prefix block count and hit outcome. The reuse factor (1/hitFactor
		// enters as an additive term rather than a divisor so a non-reusable
		// candidate stays inside the legal [0,1] interval without clamping:
		//   agePart      = age / (1 + age)                    in [0,1)
		//   blockPart    = 1 - exp(-blockCount)               in (0,1)
		//   reusePenalty = (1 - hitFactor)                    in [0,0.85]
		//   raw          = agePart*(0.25+0.25*blockPart) + 0.5*reusePenalty
		agePart := float64(age) / (1 + float64(age))
		blockPart := 1 - math.Exp(-float64(blockCount))
		reusePenalty := 1 - hitFactor
		raw := agePart*(0.25+0.25*blockPart) + 0.5*reusePenalty
		upper := clampUnit(raw)

		res.Components["overlap_term"] = overlapTerm
		res.Components["hit_term"] = hitTerm
		res.Components["age_term"] = ageTerm
		res.Components["log_benefit"] = logBenefit
		res.Components["benefit_score"] = benefit
		res.Components["failure_upper"] = upper
		res.Derivation["hit_factor"] = fmt.Sprintf("%g", hitFactor)
		res.Derivation["slot_age_turns"] = fmt.Sprintf("%d", age)
		res.Derivation["matched_slot"] = slotID
		res.Derivation["prefix_block_count"] = fmt.Sprintf("%d", blockCount)
		res.Derivation["age_part"] = fmt.Sprintf("age/(1+age)=%d/(1+%d)=%.6f", age, age, agePart)
		res.Derivation["block_part"] = fmt.Sprintf("1-exp(-blocks)=1-exp(-%d)=%.6f", blockCount, blockPart)
		res.Derivation["reuse_penalty"] = fmt.Sprintf("1-hit_factor=1-%.2f=%.6f", hitFactor, reusePenalty)
		res.Derivation["upper_formula"] = fmt.Sprintf(
			"%.6f*(0.25+0.25*%.6f)+0.5*%.6f=%.6f (clamp[0,1]=%.6f)",
			agePart, blockPart, reusePenalty, raw, upper)

		res.BenefitScore = round6(benefit)
		res.FailureUpper = round6(upper)
		if raw < 0 || raw > 1 || math.IsNaN(raw) || math.IsInf(raw, 0) {
			res.NumericError = numericErr(cand.ID, "failure upper bound outside legal interval",
				map[string]string{
					"raw_upper": fmt.Sprintf("%g", raw),
					"clamped":   fmt.Sprintf("%g", upper),
					"interval":  "[0,1]",
				})
			out = append(out, res)
			continue
		}
		if benefit >= p.HitGainThreshold && upper <= p.FailureUpperLimit {
			res.Decision = "ACCEPT"
		} else {
			res.Decision = "REJECT"
		}
		res.Derivation["benefit_threshold"] = fmt.Sprintf("%g", p.HitGainThreshold)
		res.Derivation["failure_upper_limit"] = fmt.Sprintf("%g", p.FailureUpperLimit)
		res.Derivation["decision_rule"] = fmt.Sprintf(
			"benefit>=threshold && upper<=threshold => %s", res.Decision)
		out = append(out, res)
	}
	return out
}

func validateWeights(w spec.DecisionWeights, cid string) *spec.BizError {
	zw := []string{}
	if w.Overlap <= 0 {
		zw = append(zw, "overlap")
	}
	if w.Hit <= 0 {
		zw = append(zw, "hit")
	}
	if w.Age <= 0 {
		zw = append(zw, "age")
	}
	if w.FailPenalty <= 0 {
		zw = append(zw, "fail_penalty")
	}
	if len(zw) > 0 {
		return numericErr(cid, "zero or negative decision weight",
			map[string]string{"invalid_weights": joinComma(zw)})
	}
	return nil
}

func hitFactor(outcome string) float64 {
	switch outcome {
	case spec.HitExact:
		return 1.0
	case spec.HitPartial:
		return 0.6
	case spec.HitReusable:
		return 0.3
	default: // below threshold / non-cacheable
		return 0.15
	}
}

func slotContext(hr spec.HitResult, cache *Cache, turn int) (age, blockCount int, slotID string) {
	blockCount = len(hr.Blocks)
	slotID = hr.MatchedSlot
	if slotID == "" {
		// No reusable slot: treat as maximum observed age proxy.
		return turn + 1, blockCount, "none"
	}
	for _, s := range cache.Slots() {
		if s.SlotID == slotID {
			return turn - s.LastUsedTurn, blockCount, slotID
		}
	}
	return turn + 1, blockCount, slotID
}

func clampUnit(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}

func numericErr(cid, msg string, details map[string]string) *spec.BizError {
	e := spec.NewBizError(spec.CodeNumeric, msg, "", 0, spec.StageAdjudge).WithCandidate(cid)
	for k, v := range details {
		e.WithDetail(k, v)
	}
	return e
}

func joinComma(xs []string) string {
	out := ""
	for i, x := range xs {
		if i > 0 {
			out += ","
		}
		out += x
	}
	return out
}

func round6(f float64) float64 {
	return float64(int64(f*1e6+0.5)) / 1e6
}
