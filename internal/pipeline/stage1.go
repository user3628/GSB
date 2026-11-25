package pipeline

import (
	"fmt"
	"math/rand"
	"sort"

	"headprune/internal/model"
)

// ProbeSample records one deterministically selected probe sentence.
type ProbeSample struct {
	Index   int    `json:"index"`
	ProbeID string `json:"probe_id"`
}

// HeadErrorRow is one entry of the byte-stable per-head error table.
type HeadErrorRow struct {
	Head          model.HeadID `json:"head"`
	MeanKL        float64      `json:"mean_kl"`
	MaxKL         float64      `json:"max_kl"`
	MeanTV        float64      `json:"mean_tv"`
	ProbeCoverage []string     `json:"probe_coverage"`
	ReusedFromVer int64        `json:"reused_from_version,omitempty"`
}

// ProbeReport is the stage-1 output including coverage and error table.
type ProbeReport struct {
	Seed       int64          `json:"seed"`
	Limit      int            `json:"limit"`
	Probes     []ProbeSample  `json:"probes"`
	HeadErrors []HeadErrorRow `json:"head_errors"`
}

// SampleProbes deterministically picks up to limit probe sentences using the
// fixed policy seed. Selection order is RNG-driven but never depends on map
// iteration, so the same input always yields the same probes.
func SampleProbes(sentences []model.Sentence, policy model.Policy) ([]ProbeSample, error) {
	if policy.ProbeLimit == 0 {
		return nil, &model.BizError{Code: model.CodePolicyInvalid, Message: "probe sample limit is zero"}
	}
	if len(sentences) < policy.MinProbes {
		return nil, &model.BizError{
			Code:    model.CodeInsufficientProbe,
			Message: fmt.Sprintf("probe pool %d below configured minimum %d", len(sentences), policy.MinProbes),
			Context: map[string]string{"pool": fmt.Sprint(len(sentences)), "min": fmt.Sprint(policy.MinProbes)},
		}
	}
	rng := rand.New(rand.NewSource(policy.Seed))
	idx := make([]int, len(sentences))
	for i := range idx {
		idx[i] = i
	}
	rng.Shuffle(len(idx), func(i, j int) { idx[i], idx[j] = idx[j], idx[i] })
	limit := policy.ProbeLimit
	if limit > len(idx) {
		limit = len(idx)
	}
	chosen := idx[:limit]
	sort.Ints(chosen)
	probes := make([]ProbeSample, 0, limit)
	for _, i := range chosen {
		probes = append(probes, ProbeSample{Index: i, ProbeID: sentences[i].ID})
	}
	if len(probes) == 0 || len(probes) < policy.MinProbes {
		return nil, &model.BizError{
			Code:    model.CodeInsufficientProbe,
			Message: fmt.Sprintf("selected probes %d below configured minimum %d", len(probes), policy.MinProbes),
		}
	}
	return probes, nil
}

func quantSource(table QuantTable) model.ActivationSource {
	scales := make(map[model.HeadID]HeadQuant, len(table.Heads))
	for _, q := range table.Heads {
		scales[q.Head] = q
	}
	return func(h model.Head, probeIdx int, seed int64) []float64 {
		return DequantActivation(h, scales[h.ID()], probeIdx, seed)
	}
}

// RunPerHeadReplay prunes each head independently, recomputes the masked
// reconstruction distribution with all other quantized heads, and aggregates
// KL/TV errors over the deterministic probe coverage.
func RunPerHeadReplay(ds model.Dataset, probes []ProbeSample, quant QuantTable) ([]HeadErrorRow, error) {
	source := quantSource(quant)
	baseline := make([][]float64, len(probes))
	for pi, p := range probes {
		logits, err := model.EnsembleLogitsWithSource(ds.Heads, nil, ds.Sentences[p.Index], ds.Policy.Seed, pi, source)
		if err != nil {
			return nil, err
		}
		dist, ok := model.Softmax(logits)
		if !ok {
			return nil, (&model.BizError{Code: model.CodeNumeric,
				Message: "baseline reconstruction distribution is all zero or non-finite"}).WithProbe(p.ProbeID)
		}
		baseline[pi] = dist
	}
	rows := make([]HeadErrorRow, 0, len(ds.Heads))
	for _, h := range ds.Heads {
		row := HeadErrorRow{Head: h.ID(), ProbeCoverage: make([]string, 0, len(probes))}
		var sumKL, maxKL, sumTV float64
		pruned := model.HeadSet{h.ID()}
		for pi, p := range probes {
			logits, err := model.EnsembleLogitsWithSource(ds.Heads, pruned, ds.Sentences[p.Index], ds.Policy.Seed, pi, source)
			if err != nil {
				return nil, err
			}
			dist, ok := model.Softmax(logits)
			if !ok {
				return nil, (&model.BizError{
					Code:    model.CodeNumeric,
					Message: "per-head reconstruction distribution is all zero or non-finite",
				}).WithHead(h.Layer, h.Head).WithProbe(p.ProbeID)
			}
			kl, ok := model.KLDivergence(baseline[pi], dist)
			if !ok {
				return nil, (&model.BizError{Code: model.CodeNumeric, Message: "KL divergence is non-finite"}).
					WithHead(h.Layer, h.Head).WithProbe(p.ProbeID)
			}
			sumKL += kl
			if kl > maxKL {
				maxKL = kl
			}
			sumTV += model.TotalVariation(baseline[pi], dist)
			row.ProbeCoverage = append(row.ProbeCoverage, p.ProbeID)
		}
		n := float64(len(probes))
		row.MeanKL = sumKL / n
		row.MaxKL = maxKL
		row.MeanTV = sumTV / n
		rows = append(rows, row)
	}
	return rows, nil
}

// Stage1 runs sampling and per-head replay and returns the canonical report.
func Stage1(ds model.Dataset, quant QuantTable) (ProbeReport, error) {
	probes, err := SampleProbes(ds.Sentences, ds.Policy)
	if err != nil {
		if be, ok := model.AsBizError(err); ok && be.Code == model.CodeInsufficientProbe && len(ds.Heads) > 0 {
			return ProbeReport{}, be.WithHead(ds.Heads[0].Layer, ds.Heads[0].Head)
		}
		return ProbeReport{}, err
	}
	rows, err := RunPerHeadReplay(ds, probes, quant)
	if err != nil {
		return ProbeReport{}, err
	}
	return ProbeReport{
		Seed:       ds.Policy.Seed,
		Limit:      ds.Policy.ProbeLimit,
		Probes:     probes,
		HeadErrors: rows,
	}, nil
}
