package experiment

import (
	"fmt"
	"math"

	"regworkbench/internal/canonical"
	"regworkbench/internal/config"
	"regworkbench/internal/model"
	"regworkbench/internal/workerr"
)

func buildValidationSummary(cfg config.Config, folds map[int]*FoldArtifact) (*ValidationSummary, error) {
	summary := &ValidationSummary{Metric: cfg.Metric, KFolds: cfg.KFolds}
	type indexedMean struct {
		cand CandidateSummary
		mean float64
	}
	built := []indexedMean{}
	for _, lambda := range cfg.Lambdas {
		for _, ratio := range cfg.L1Ratios {
			cand := CandidateSummary{Lambda: lambda, L1Ratio: ratio, Unregularized: lambda == 0, FoldValues: make([]float64, 0, cfg.KFolds)}
			for f := 0; f < cfg.KFolds; f++ {
				p := pointInFold(folds[f], lambda, ratio)
				if p == nil {
					return nil, workerr.InvalidFold(f, fmt.Sprintf("missing_point lambda=%v l1_ratio=%v", lambda, ratio))
				}
				if math.IsNaN(p.SelectedMetric) || math.IsInf(p.SelectedMetric, 0) {
					return nil, workerr.InvalidFold(f, "metric_nan_or_infinite:"+cfg.Metric)
				}
				cand.FoldValues = append(cand.FoldValues, p.SelectedMetric)
			}
			cand.MeanMetric, cand.StdMetric = meanStd(cand.FoldValues)
			built = append(built, indexedMean{cand: cand, mean: cand.MeanMetric})
			summary.Candidates = append(summary.Candidates, cand)
		}
	}

	bestIdx := -1
	bestMean := 0.0
	for i, item := range built {
		if bestIdx == -1 || model.Better(item.mean, bestMean, cfg.Metric) {
			bestIdx = i
			bestMean = item.mean
		}
	}
	if bestIdx < 0 {
		return nil, workerr.New(workerr.EInvalidFold, "no finite validation candidates")
	}
	for i := range summary.Candidates {
		if i == bestIdx {
			summary.Candidates[i].Selected = true
			summary.Selected = summary.Candidates[i]
		}
	}
	direction := "minimize"
	if cfg.Metric == "accuracy" || cfg.Metric == "auc" {
		direction = "maximize"
	}
	summary.Basis = SelectionBasis{
		Metric: cfg.Metric, Direction: direction,
		Rule:           "cross_fold_mean_then_deterministic_path_order",
		TieBreak:       "exact_tie_keeps_first_sorted_lambda_then_l1_ratio",
		SelectedLambda: summary.Selected.Lambda, SelectedL1Ratio: summary.Selected.L1Ratio,
		SelectedMean: summary.Selected.MeanMetric, SelectedStd: summary.Selected.StdMetric,
	}
	for i, cand := range summary.Candidates {
		if i == bestIdx {
			continue
		}
		code, detail := rejectionReason(cfg.Metric, cand.MeanMetric, summary.Selected.MeanMetric, false)
		summary.Rejections = append(summary.Rejections, Rejection{
			Lambda: cand.Lambda, L1Ratio: cand.L1Ratio, Code: code, Detail: detail, MeanMetric: cand.MeanMetric,
		})
	}
	return summary, nil
}

func pointInFold(art *FoldArtifact, lambda, ratio float64) *FoldPointArtifact {
	if art == nil {
		return nil
	}
	for i := range art.Points {
		p := &art.Points[i]
		if p.Lambda == lambda && p.L1Ratio == ratio {
			return p
		}
	}
	return nil
}

func meanStd(values []float64) (float64, float64) {
	if len(values) == 0 {
		return math.NaN(), math.NaN()
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(len(values))
	ss := 0.0
	for _, v := range values {
		d := v - mean
		ss += d * d
	}
	return mean, math.Sqrt(ss / float64(len(values)))
}

func (s *Session) recomputeCurrentBestInto(folds map[int]*FoldArtifact) *CoefficientsArtifact {
	type candidate struct {
		lambda float64
		ratio  float64
		values []float64
		coefs  [][]float64
		inters []float64
	}
	var candidates []*candidate
	index := map[pointKey]*candidate{}
	for _, lambda := range s.Cfg.Lambdas {
		for _, ratio := range s.Cfg.L1Ratios {
			c := &candidate{lambda: lambda, ratio: ratio}
			candidates = append(candidates, c)
			index[pointKey{lambda, ratio}] = c
		}
	}
	for f := 0; f < s.Cfg.KFolds; f++ {
		art := folds[f]
		if art == nil {
			continue
		}
		for _, p := range art.Points {
			c := index[pointKey{p.Lambda, p.L1Ratio}]
			if c == nil {
				continue
			}
			c.values = append(c.values, p.SelectedMetric)
			c.coefs = append(c.coefs, p.Coefficients)
			c.inters = append(c.inters, p.Intercept)
		}
	}
	var best *candidate
	var bestMean float64
	for _, c := range candidates {
		if len(c.values) == 0 {
			continue
		}
		mean, _ := meanStd(c.values)
		if math.IsNaN(mean) {
			continue
		}
		if best == nil || model.Better(mean, bestMean, s.Cfg.Metric) {
			best = c
			bestMean = mean
		}
	}
	if best == nil {
		return nil
	}
	width := len(s.Prepared.FeatureNames)
	avg := make([]float64, width)
	var intercept float64
	n := float64(len(best.coefs))
	for _, coefs := range best.coefs {
		for j := 0; j < width && j < len(coefs); j++ {
			avg[j] += coefs[j] / n
		}
	}
	for _, v := range best.inters {
		intercept += v / n
	}
	digest, _, _ := canonical.SHA256JSON(struct {
		Coefficients []float64 `json:"coefficients"`
		Intercept    float64   `json:"intercept"`
	}{avg, intercept})
	fold := -1
	return &CoefficientsArtifact{
		Scope: "current_best", Fold: &fold, Lambda: best.lambda, L1Ratio: best.ratio,
		Unregularized: best.lambda == 0, FeatureNames: append([]string(nil), s.Prepared.FeatureNames...),
		Coefficients: avg, Intercept: intercept, Digest: digest,
		SampleFingerprint: s.DS.RawSHA, ConfigVersion: s.ConfigVer,
	}
}

func rejectionReason(metric string, mean, best float64, selected bool) (string, string) {
	if math.IsNaN(mean) || math.IsInf(mean, 0) {
		return "nan_metric", metric + "_was_nan_or_infinite"
	}
	if selected {
		return "", ""
	}
	direction := "lower_is_better"
	if metric == "accuracy" || metric == "auc" {
		direction = "higher_is_better"
	}
	return "worse_validation_metric", fmt.Sprintf("%s:%.12g not optimal versus %.12g (%s)", metric, mean, best, direction)
}

var _ = workerr.EInvalidFold
