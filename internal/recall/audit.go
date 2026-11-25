package recall

import (
	"fmt"
	"math"
	"math/big"

	"multirecall/internal/configx"
	"multirecall/internal/model"
)

const samplingDigits = 6

func candidateLookup(routes map[string]RouteResult) map[string]map[string]Candidate {
	out := map[string]map[string]Candidate{}
	for route, rr := range routes {
		out[route] = map[string]Candidate{}
		for _, c := range rr.Candidates {
			out[route][c.ItemID] = c
		}
	}
	return out
}

func effectiveDigits(bound float64) int {
	if bound <= 0 || math.IsInf(bound, -1) {
		return 15
	}
	d := int(math.Floor(-math.Log10(bound)))
	if d < 0 {
		return 0
	}
	if d > 15 {
		return 15
	}
	return d
}

func computeFusionBounds(cfg configx.Snapshot, routes map[string]RouteResult, lookups map[string]map[string]Candidate, hits []Hit) {
	for i := range hits {
		bound := 0.0
		for _, contrib := range hits[i].Contributions {
			cand := lookups[contrib.Route][hits[i].ItemID]
			if cfg.Fusion.Method == "reciprocal_rank" || cfg.Fusion.Method == "rrf" {
				bound += 0.5 / float64(ScaleFusion)
				rr := routes[contrib.Route]
				idx := contrib.SourceRank - 1
				for _, neighborIdx := range []int{idx - 1, idx + 1} {
					if neighborIdx < 0 || neighborIdx >= len(rr.Candidates) {
						continue
					}
					neighbor := rr.Candidates[neighborIdx]
					gap := math.Abs(cand.DomainScore - neighbor.DomainScore)
					errorBoundary := cand.RawErrorBound + neighbor.RawErrorBound + cfg.Fusion.SimilarityTolerance
					if gap <= errorBoundary {
						k := cfg.Fusion.RRFK
						if k <= 0 {
							k = 60
						}
						delta := math.Abs(contrib.Weight * (1.0/float64(k+contrib.SourceRank) - 1.0/float64(k+neighbor.Rank)))
						bound += delta
					}
				}
			} else {
				bound += math.Abs(contrib.Weight)*cand.NormalizedErrorBound +
					0.5/float64(ScaleFusion) +
					0.5/float64(ScaleWeight)
			}
		}
		hits[i].FusionErrorBound = bound + 1e-12
		hits[i].EffectiveDecimalDigits = effectiveDigits(hits[i].FusionErrorBound)
		hits[i].Derivation = append(hits[i].Derivation,
			fmt.Sprintf("propagated fusion error upper bound %.12g; effective decimal digits %d", hits[i].FusionErrorBound, hits[i].EffectiveDecimalDigits),
		)
	}

	for i := 0; i+1 < len(hits); i++ {
		gap := float64(hits[i].FusionScoreFP-hits[i+1].FusionScoreFP) / float64(ScaleFusion)
		boundary := hits[i].FusionErrorBound + hits[i+1].FusionErrorBound + cfg.Fusion.SimilarityTolerance
		unstable := gap <= boundary
		info := &StabilityInfo{
			AdjacentItemID:    hits[i+1].ItemID,
			ScoreGap:          gap,
			ErrorBoundary:     boundary,
			Unstable:          unstable,
			CrossingMagnitude: math.Max(0, boundary-gap),
		}
		if unstable {
			info.Reason = "adjacent fusion gap is no larger than combined propagated error boundary; order is not numerically certified"
		} else {
			info.Reason = "adjacent fusion gap exceeds combined propagated error boundary; order is certified"
		}
		hits[i].Stability = info
	}
}

func routeReferenceScore(e *Engine, q model.Query, route string, cand Candidate) float64 {
	switch route {
	case "lexical":
		if doc, ok := e.lexDocByID[cand.ItemID]; ok {
			return LexicalReferenceScore(e.Lex, q, doc)
		}
	case "category":
		return cand.DomainScore
	case "vector":
		if cand.Vector != nil {
			return cand.Vector.CosineReference
		}
	}
	return cand.DomainScore
}

func auditNumeric(e *Engine, q model.Query, cfg configx.Snapshot, routes map[string]RouteResult, lookups map[string]map[string]Candidate, hits []Hit) (*NumericAudit, *model.APIError) {
	audit := &NumericAudit{
		ReferencePrecisionBits: 256,
		Tolerance:              cfg.Fusion.NumericTolerance,
		Passed:                 true,
		SamplingChecks:         []SamplingCheck{},
	}
	refBigByItem := map[string]*big.Float{}

	for hi := range hits {
		hit := &hits[hi]
		hn := &HitNumeric{
			ItemID: hit.ItemID, FusionImplementation: hit.FusionScore,
			ErrorBound: hit.FusionErrorBound, WithinBound: true,
			RouteDeviations: []RouteDeviation{},
		}
		refSum := new(big.Float).SetPrec(256).SetFloat64(0)
		for _, contrib := range hit.Contributions {
			cand, ok := lookups[contrib.Route][hit.ItemID]
			if !ok {
				continue
			}
			refRoute := routeReferenceScore(e, q, contrib.Route, cand)
			implRoute := cand.DomainScore
			dev := math.Abs(implRoute - refRoute)
			within := dev <= cand.RawErrorBound+1e-10
			if !within {
				audit.Passed = false
			}
			hn.RouteDeviations = append(hn.RouteDeviations, RouteDeviation{
				ItemID: hit.ItemID, Route: contrib.Route, Implementation: implRoute, Reference: refRoute,
				AbsoluteDeviation: dev, ErrorBound: cand.RawErrorBound, WithinBound: within,
			})
			if dev > audit.MaxDeviation {
				audit.MaxDeviation = dev
				audit.MaxDeviationRoute = contrib.Route
				audit.MaxDeviationItemID = hit.ItemID
			}
			if dev > cfg.Fusion.NumericTolerance {
				return nil, &model.APIError{
					Code: model.CodeNumericMismatch, QueryID: q.QueryID, ItemID: hit.ItemID,
					MaxDeviation: dev, SourceRoute: contrib.Route,
					Message: "route numerical deviation exceeds configured tolerance",
				}
			}

			w := new(big.Float).SetPrec(256).SetFloat64(contrib.Weight)
			if cfg.Fusion.Method == "reciprocal_rank" || cfg.Fusion.Method == "rrf" {
				k := cfg.Fusion.RRFK
				if k <= 0 {
					k = 60
				}
				den := new(big.Float).SetPrec(256).SetInt64(int64(k + contrib.SourceRank))
				term := new(big.Float).SetPrec(256).Quo(w, den)
				refSum.Add(refSum, term)
			} else {
				rr := routes[contrib.Route]
				maxDomain := 1.0
				if len(rr.Candidates) > 0 && rr.Candidates[0].DomainScore != 0 {
					maxDomain = rr.Candidates[0].DomainScore
				}
				normRef := new(big.Float).SetPrec(256).Quo(
					new(big.Float).SetPrec(256).SetFloat64(refRoute),
					new(big.Float).SetPrec(256).SetFloat64(maxDomain),
				)
				refSum.Add(refSum, new(big.Float).SetPrec(256).Mul(w, normRef))
			}
		}
		refBigByItem[hit.ItemID] = refSum
		refFloat, _ := refSum.Float64()
		fusionDev := math.Abs(hit.FusionScore - refFloat)
		hn.FusionReference = refFloat
		hn.AbsoluteDeviation = fusionDev
		hn.WithinBound = fusionDev <= hit.FusionErrorBound+1e-9
		if fusionDev > audit.MaxDeviation {
			audit.MaxDeviation = fusionDev
			audit.MaxDeviationRoute = "fusion"
			audit.MaxDeviationItemID = hit.ItemID
		}
		if fusionDev > cfg.Fusion.NumericTolerance {
			return nil, &model.APIError{
				Code: model.CodeNumericMismatch, QueryID: q.QueryID, ItemID: hit.ItemID,
				MaxDeviation: fusionDev, SourceRoute: "fusion",
				Message: "fusion deviation exceeds configured tolerance",
			}
		}
		if !hn.WithinBound {
			audit.Passed = false
		}
		hit.Numeric = hn
	}

	for _, pos := range cfg.Fusion.SamplingPositions {
		if pos <= 0 || pos > len(hits) {
			continue
		}
		hit := hits[pos-1]
		ref := refBigByItem[hit.ItemID]
		implDigits := fmt.Sprintf("%.*f", samplingDigits, hit.FusionScore)
		refDigits := "0.000000000"
		if ref != nil {
			refDigits = ref.Text('f', samplingDigits)
		}
		match := implDigits == refDigits
		if !match {
			audit.Passed = false
		}
		audit.SamplingChecks = append(audit.SamplingChecks, SamplingCheck{
			Position: pos, ItemID: hit.ItemID, Digits: samplingDigits,
			ImplementationDigits: implDigits, ReferenceDigits: refDigits, DigitByDigitMatch: match,
		})
	}
	audit.ReferenceDigest = model.Digest(struct {
		MaxDeviation float64         `json:"max_deviation"`
		Checks       []SamplingCheck `json:"checks"`
		Hits         []HitNumeric    `json:"hits"`
	}{audit.MaxDeviation, audit.SamplingChecks, numericHits(hits)})
	return audit, nil
}

func numericHits(hits []Hit) []HitNumeric {
	out := make([]HitNumeric, 0, len(hits))
	for _, h := range hits {
		if h.Numeric != nil {
			out = append(out, *h.Numeric)
		}
	}
	return out
}
