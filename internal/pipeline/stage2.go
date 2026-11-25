package pipeline

import (
	"fmt"
	"sort"

	"headprune/internal/model"
)

// HeadContribution keeps per-head evidence inside a combo replay log.
type HeadContribution struct {
	Head      model.HeadID `json:"head"`
	SoloError float64      `json:"solo_error"`
	Weight    float64      `json:"weight"`
	Share     float64      `json:"share"`
}

// ComboRow is one combination replay with true error, prediction and verdict.
type ComboRow struct {
	Combo             model.HeadSet      `json:"combo"`
	Size              int                `json:"size"`
	TrueMeanKL        float64            `json:"true_mean_kl"`
	TrueMaxKL         float64            `json:"true_max_kl"`
	PredictedSumKL    float64            `json:"predicted_sum_kl"`
	PredictedWeighted float64            `json:"predicted_weighted_kl"`
	Amplification     float64            `json:"amplification"`
	Deviation         float64            `json:"deviation"`
	Coupled           bool               `json:"coupled"`
	PassedReplay      bool               `json:"passed_replay"`
	Contributions     []HeadContribution `json:"contributions"`
}

// ComboReport groups every combination replay and the coupled subset.
type ComboReport struct {
	Coupled []ComboRow `json:"coupled_combos"`
	All     []ComboRow `json:"all_combos"`
}

// selectCombos enumerates candidate pruning sets: for each candidate ratio it
// takes the most sensitive heads, then adds adjacent-head pairs so designed
// strongly coupled pairs are always replayed rather than inferred.
func selectCombos(rows []HeadErrorRow, ds model.Dataset) []model.HeadSet {
	bySensitivity := append([]HeadErrorRow(nil), rows...)
	sort.SliceStable(bySensitivity, func(i, j int) bool {
		if bySensitivity[i].MeanKL != bySensitivity[j].MeanKL {
			return bySensitivity[i].MeanKL < bySensitivity[j].MeanKL
		}
		return bySensitivity[i].Head.Less(bySensitivity[j].Head)
	})
	total := len(ds.Heads)
	seen := map[string]bool{}
	var combos []model.HeadSet
	add := func(set model.HeadSet) {
		cp := set.Sorted()
		fp := cp.Fingerprint()
		if seen[fp] || len(cp) == 0 || len(cp) > ds.Policy.MaxPruneHeads {
			return
		}
		seen[fp] = true
		combos = append(combos, cp)
	}
	for _, ratio := range ds.Policy.CandidateRatios {
		k := int(ratio*float64(total) + 0.5)
		if k < 1 {
			k = 1
		}
		if k > ds.Policy.MaxPruneHeads {
			k = ds.Policy.MaxPruneHeads
		}
		if k > len(bySensitivity) {
			k = len(bySensitivity)
		}
		set := make(model.HeadSet, 0, k)
		for i := 0; i < k; i++ {
			set = append(set, bySensitivity[i].Head)
		}
		add(set)
	}
	// Adjacent pairs within a layer capture the synergy-coupled pairs.
	layers := map[int][]int{}
	var layerOrder []int
	for _, h := range ds.Heads {
		if _, ok := layers[h.Layer]; !ok {
			layerOrder = append(layerOrder, h.Layer)
		}
		layers[h.Layer] = append(layers[h.Layer], h.Head)
	}
	sort.Ints(layerOrder)
	for _, layer := range layerOrder {
		hs := layers[layer]
		sort.Ints(hs)
		for i := 0; i+1 < len(hs); i += 2 {
			add(model.HeadSet{{Layer: layer, Head: hs[i]}, {Layer: layer, Head: hs[i+1]}})
		}
	}
	sort.Slice(combos, func(i, j int) bool {
		if len(combos[i]) != len(combos[j]) {
			return len(combos[i]) < len(combos[j])
		}
		return combos[i].Fingerprint() < combos[j].Fingerprint()
	})
	return combos
}

// ReplayCombo performs a whole-set replay. The true error is always computed
// from the ensemble; per-head errors are never used as a substitute.
func ReplayCombo(ds model.Dataset, probes []ProbeSample, quant QuantTable, rows []HeadErrorRow, combo model.HeadSet) (ComboRow, error) {
	source := quantSource(quant)
	solo := map[model.HeadID]float64{}
	for _, r := range rows {
		solo[r.Head] = r.MeanKL
	}
	out := ComboRow{Combo: combo.Sorted(), Size: len(combo), PassedReplay: true}
	var sumKL, maxKL, predSum, predWeighted float64
	totalSolo := 0.0
	for _, id := range out.Combo {
		predSum += solo[id]
		totalSolo += solo[id]
	}
	for pi, p := range probes {
		logits, err := model.EnsembleLogitsWithSource(ds.Heads, model.HeadSet(out.Combo), ds.Sentences[p.Index], ds.Policy.Seed, pi, source)
		if err != nil {
			return ComboRow{}, err
		}
		baseLogits, err := model.EnsembleLogitsWithSource(ds.Heads, nil, ds.Sentences[p.Index], ds.Policy.Seed, pi, source)
		if err != nil {
			return ComboRow{}, err
		}
		base, ok := model.Softmax(baseLogits)
		if !ok {
			return ComboRow{}, (&model.BizError{Code: model.CodeNumeric,
				Message: "baseline distribution diverged",
				Context: map[string]string{
					"combo":         fmt.Sprint(out.Combo),
					"probe":         p.ProbeID,
					"contributions": contributionsJSON(rows, out.Combo),
				}}).WithProbe(p.ProbeID)
		}
		dist, ok := model.Softmax(logits)
		if !ok {
			be := &model.BizError{Code: model.CodeNumeric,
				Message: "combo replay diverged or reconstruction distribution is all zero",
				Context: map[string]string{"combo": fmt.Sprint(out.Combo), "probe": p.ProbeID}}
			return ComboRow{}, be
		}
		kl, ok := model.KLDivergence(base, dist)
		if !ok {
			return ComboRow{}, &model.BizError{Code: model.CodeNumeric, Message: "combo KL divergence non-finite"}
		}
		sumKL += kl
		if kl > maxKL {
			maxKL = kl
		}
	}
	n := float64(len(probes))
	out.TrueMeanKL = sumKL / n
	out.TrueMaxKL = maxKL
	out.PredictedSumKL = predSum
	for _, id := range out.Combo {
		w := 1.0
		if totalSolo > 0 {
			w = solo[id] / totalSolo
		}
		predWeighted += w * solo[id] * float64(out.Size)
		out.Contributions = append(out.Contributions, HeadContribution{
			Head:      id,
			SoloError: solo[id],
			Weight:    w,
			Share:     solo[id] / predSum,
		})
	}
	out.PredictedWeighted = predWeighted
	if predSum > 0 {
		out.Amplification = out.TrueMeanKL / predSum
		out.Deviation = (out.TrueMeanKL - predSum) / predSum
	} else {
		out.Amplification = 1
		out.Deviation = 0
	}
	out.Coupled = out.Deviation > ds.Policy.CouplingThreshold
	return out, nil
}

// Stage2 enumerates candidate combinations and replays each as a whole set.
func Stage2(ds model.Dataset, probes []ProbeSample, quant QuantTable, rows []HeadErrorRow) (ComboReport, error) {
	combos := selectCombos(rows, ds)
	report := ComboReport{All: make([]ComboRow, 0, len(combos)), Coupled: []ComboRow{}}
	for _, combo := range combos {
		row, err := ReplayCombo(ds, probes, quant, rows, combo)
		if err != nil {
			if be, ok := model.AsBizError(err); ok && be.Code == model.CodeNumeric {
				be.Context = mergeCtx(be.Context, map[string]string{
					"combo":         fmt.Sprint(combo.Sorted()),
					"contributions": contributionsJSON(rows, combo),
				})
				return ComboReport{}, be
			}
			return ComboReport{}, err
		}
		report.All = append(report.All, row)
		if row.Coupled {
			report.Coupled = append(report.Coupled, row)
		}
	}
	return report, nil
}

func mergeCtx(a, b map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func contributionsJSON(rows []HeadErrorRow, combo model.HeadSet) string {
	solo := map[model.HeadID]float64{}
	for _, r := range rows {
		solo[r.Head] = r.MeanKL
	}
	parts := make([]string, 0, len(combo))
	for _, id := range combo.Sorted() {
		parts = append(parts, fmt.Sprintf("%s=%.12g", id, solo[id]))
	}
	return fmt.Sprint(parts)
}
