package engine

import (
	"fmt"
	"math"
	"sort"

	"ocrlab/internal/canonical"
	"ocrlab/internal/config"
	"ocrlab/internal/model"
)

type ProductDigests struct {
	Detection     string
	Recognition   string
	Strategy      string
	Input         string
	ConfigVersion string
}

func ImageDiagnostic(cfg config.Config, spec model.ImageSpec, detection model.DetectionProduct, recognition model.RecognitionProduct, strategy model.StrategyProduct, digests ProductDigests) model.ImageDiagnostic {
	matches := MatchBoxes(detection.Candidates, spec.Truth)
	byCandidate := make(map[int]model.RecognitionResult, len(recognition.Results))
	for _, result := range recognition.Results {
		byCandidate[result.CandidateIndex] = result
	}
	matched := 0
	exact := 0
	edits := 0
	characterTotal := 0
	for _, pair := range matches {
		matched++
		truth := spec.Truth[pair.TruthIndex]
		result, exists := byCandidate[pair.CandidateIndex]
		characterTotal += len([]rune(truth.Text))
		if !exists || result.Status != "recognized" {
			edits += len([]rune(truth.Text))
			continue
		}
		if result.Text == truth.Text {
			exact++
		}
		edits += levenshtein([]rune(result.Text), []rune(truth.Text))
	}
	distanceSum := 0.0
	distanceSquaredSum := 0.0
	distanceSamples := 0
	recognized := 0
	for _, result := range recognition.Results {
		if result.Status == "recognized" {
			recognized++
			distanceSum += result.MatchDistance
			distanceSquaredSum += result.MatchDistance * result.MatchDistance
			distanceSamples++
		}
	}
	notApplicable := 0
	detectionRecall := model.MetricValue{}
	recallBand := model.ErrorBand{}
	if len(spec.Truth) > 0 {
		detectionRecall = model.Metric(float64(matched) / float64(len(spec.Truth)))
		recallBand = WilsonBand(matched, len(spec.Truth))
	} else {
		detectionRecall = model.NotApplicable()
		recallBand = NotApplicableBand()
		notApplicable++
	}
	accuracy := model.MetricValue{}
	accuracyBand := model.ErrorBand{}
	if len(detection.Candidates) > 0 {
		accuracy = model.Metric(float64(exact) / float64(len(detection.Candidates)))
		accuracyBand = WilsonBand(exact, len(detection.Candidates))
	} else {
		accuracy = model.NotApplicable()
		accuracyBand = NotApplicableBand()
		notApplicable++
	}
	charAccuracy := model.MetricValue{}
	if characterTotal > 0 {
		charAccuracy = model.Metric(math.Max(0, 1-float64(edits)/float64(characterTotal)))
	} else {
		charAccuracy = model.NotApplicable()
		notApplicable++
	}
	avgDistance := model.MetricValue{}
	distanceBand := model.ErrorBand{}
	if distanceSamples > 0 {
		avgDistance = model.Metric(distanceSum / float64(distanceSamples))
		distanceBand = MeanBand(distanceSum, distanceSquaredSum, distanceSamples)
	} else {
		avgDistance = model.NotApplicable()
		distanceBand = NotApplicableBand()
		notApplicable++
	}
	budgetUsed := selectedCost(strategy.Detection) + selectedCost(strategy.Recognition)
	issues := make([]model.DiagnosticIssue, 0, len(detection.Errors)+len(recognition.Errors))
	for _, issue := range detection.Errors {
		issues = append(issues, model.DiagnosticIssue{
			Code: issue.Code, Stage: model.StageDetect, ImageID: issue.ImageID,
			BoxIndex: issue.BoxIndex, Message: issue.Message,
		})
	}
	for _, issue := range recognition.Errors {
		issues = append(issues, model.DiagnosticIssue{
			Code: issue.Code, Stage: model.StageRecognize, ImageID: issue.ImageID,
			CandidateIndex: issue.CandidateIndex, Message: issue.Message,
		})
	}
	return model.ImageDiagnostic{
		SchemaVersion:     model.SchemaVersion,
		ImageID:           spec.ImageID,
		Group:             spec.Group,
		ConfigVersion:     digests.ConfigVersion,
		InputVersion:      digests.Input,
		DetectionDigest:   digests.Detection,
		RecognitionDigest: digests.Recognition,
		StrategyDigest:    digests.Strategy,
		TruthBoxes:        len(spec.Truth),
		Detection: model.DetectionMetrics{
			TruthBoxes:      len(spec.Truth),
			MatchedBoxes:    matched,
			Recall:          detectionRecall,
			RecallBand:      recallBand,
			InvalidBoxes:    detection.InvalidBoxes,
			DetectionErrors: len(detection.Errors),
		},
		Recognition: model.RecognitionMetrics{
			Candidates:           len(detection.Candidates),
			Recognized:           recognized,
			ExactMatches:         exact,
			Accuracy:             accuracy,
			AccuracyBand:         accuracyBand,
			CharacterEdits:       edits,
			CharacterTotal:       characterTotal,
			CharacterAccuracy:    charAccuracy,
			AverageMatchDistance: avgDistance,
			DistanceSum:          round9(distanceSum),
			DistanceSquaredSum:   round9(distanceSquaredSum),
			DistanceSamples:      distanceSamples,
			DistanceBand:         distanceBand,
			TemplateMisses:       len(recognition.Errors),
			NotApplicableCount:   notApplicable,
		},
		BudgetUnitsUsed:  budgetUsed,
		BudgetUnitsLimit: cfg.ComputeBudgetUnits,
		Strategy: model.StrategyHits{
			Detection:   []model.StrategyHit{{Code: strategy.Detection.Chosen, Count: 1}},
			Recognition: []model.StrategyHit{{Code: strategy.Recognition.Chosen, Count: 1}},
		},
		Issues:             issues,
		NotApplicableCount: notApplicable,
	}
}

type MatchPair struct {
	TruthIndex     int
	CandidateIndex int
	IOU            float64
}

func MatchBoxes(candidates []model.CandidateBox, truth []model.TruthBox) []MatchPair {
	type proposal struct {
		truth int
		cand  int
		iou   float64
	}
	proposals := make([]proposal, 0, len(candidates)*len(truth))
	for ti, truthBox := range truth {
		for ci, candidate := range candidates {
			iou := IOU(truthBox.Box, candidate.Box)
			if iou >= 0.5 {
				proposals = append(proposals, proposal{truth: ti, cand: ci, iou: iou})
			}
		}
	}
	sort.SliceStable(proposals, func(i, j int) bool {
		if math.Abs(proposals[i].iou-proposals[j].iou) > 1e-12 {
			return proposals[i].iou > proposals[j].iou
		}
		if proposals[i].truth != proposals[j].truth {
			return proposals[i].truth < proposals[j].truth
		}
		return proposals[i].cand < proposals[j].cand
	})
	usedTruth := make([]bool, len(truth))
	usedCandidate := make([]bool, len(candidates))
	matches := make([]MatchPair, 0, minInt(len(truth), len(candidates)))
	for _, proposal := range proposals {
		if usedTruth[proposal.truth] || usedCandidate[proposal.cand] {
			continue
		}
		usedTruth[proposal.truth] = true
		usedCandidate[proposal.cand] = true
		matches = append(matches, MatchPair{
			TruthIndex: proposal.truth, CandidateIndex: candidates[proposal.cand].Index, IOU: round9(proposal.iou),
		})
	}
	sort.SliceStable(matches, func(i, j int) bool { return matches[i].TruthIndex < matches[j].TruthIndex })
	return matches
}

func IOU(a, b model.Rect) float64 {
	left := math.Max(a.X, b.X)
	top := math.Max(a.Y, b.Y)
	right := math.Min(a.X+a.W, b.X+b.W)
	bottom := math.Min(a.Y+a.H, b.Y+b.H)
	if right <= left || bottom <= top {
		return 0
	}
	intersection := (right - left) * (bottom - top)
	union := a.W*a.H + b.W*b.H - intersection
	if union <= 0 {
		return 0
	}
	return intersection / union
}

func levenshtein(a, b []rune) int {
	previous := make([]int, len(b)+1)
	current := make([]int, len(b)+1)
	for j := range previous {
		previous[j] = j
	}
	for i := 1; i <= len(a); i++ {
		current[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			current[j] = minInt3(current[j-1]+1, previous[j]+1, previous[j-1]+cost)
		}
		previous, current = current, previous
	}
	return previous[len(b)]
}

func minInt3(a, b, c int) int {
	if a < b {
		if a < c {
			return a
		}
		return c
	}
	if b < c {
		return b
	}
	return c
}

func WilsonBand(successes, total int) model.ErrorBand {
	if total <= 0 {
		return NotApplicableBand()
	}
	p := float64(successes) / float64(total)
	const z = 1.96
	n := float64(total)
	denominator := 1 + z*z/n
	center := (p + z*z/(2*n)) / denominator
	margin := z * math.Sqrt(p*(1-p)/n+z*z/(4*n*n)) / denominator
	low := round9(math.Max(0, center-margin))
	high := round9(math.Min(1, center+margin))
	return model.ErrorBand{Applicable: true, Method: "wilson_95", Low: &low, High: &high, SampleSize: total}
}

func MeanBand(sum, sumSquares float64, sampleSize int) model.ErrorBand {
	if sampleSize <= 0 {
		return NotApplicableBand()
	}
	mean := sum / float64(sampleSize)
	low, high := mean, mean
	if sampleSize > 1 {
		variance := (sumSquares - float64(sampleSize)*mean*mean) / float64(sampleSize-1)
		if variance < 0 {
			variance = 0
		}
		margin := 1.96 * math.Sqrt(variance/float64(sampleSize))
		low = math.Max(0, mean-margin)
		high = math.Min(1, mean+margin)
	}
	low, high = round9(low), round9(high)
	return model.ErrorBand{Applicable: true, Method: "normal_95", Low: &low, High: &high, SampleSize: sampleSize}
}

func NotApplicableBand() model.ErrorBand {
	return model.ErrorBand{Applicable: false, Method: "not_applicable", SampleSize: 0}
}

func selectedCost(decision model.StrategyDecision) int {
	for _, evaluation := range decision.Evaluations {
		if evaluation.Selected {
			return evaluation.EstimatedCost
		}
	}
	return 0
}

func BuildReport(manifest model.Manifest, cfg config.Config, diagnostics []model.ImageDiagnostic) (model.Report, error) {
	byID := make(map[string]model.ImageDiagnostic, len(diagnostics))
	for _, diagnostic := range diagnostics {
		byID[diagnostic.ImageID] = diagnostic
	}
	ordered := make([]model.ImageDiagnostic, 0, len(manifest.Images))
	for _, imageSpec := range manifest.Images {
		diagnostic, exists := byID[imageSpec.ImageID]
		if !exists {
			return model.Report{}, fmt.Errorf("missing diagnostic for image %s", imageSpec.ImageID)
		}
		ordered = append(ordered, diagnostic)
	}
	groupAccumulators := make(map[string]*aggregate, len(manifest.Groups))
	total := &aggregate{}
	for _, group := range manifest.Groups {
		groupAccumulators[group.Name] = &aggregate{
			group: group.Name, shards: group.Observed.ShardCount, characters: group.Observed.TotalCharacters,
			detectionHits: zeroHits(cfg.Strategy.DetectionTiers), recognitionHits: zeroHits(cfg.Strategy.RecognitionTiers),
		}
	}
	for _, diagnostic := range ordered {
		acc := groupAccumulators[diagnostic.Group]
		if acc == nil {
			return model.Report{}, fmt.Errorf("diagnostic references unknown group %s", diagnostic.Group)
		}
		acc.add(diagnostic)
		total.add(diagnostic)
	}
	for _, group := range manifest.Groups {
		total.shards += group.Observed.ShardCount
		total.characters += group.Observed.TotalCharacters
	}
	total.detectionHits = zeroHits(cfg.Strategy.DetectionTiers)
	total.recognitionHits = zeroHits(cfg.Strategy.RecognitionTiers)
	for _, diagnostic := range ordered {
		total.addHits(diagnostic)
	}
	groups := make([]model.GroupMetrics, 0, len(manifest.Groups))
	for _, group := range manifest.Groups {
		acc := groupAccumulators[group.Name]
		groups = append(groups, acc.metrics(cfg.ComputeBudgetUnits))
	}
	return model.Report{
		SchemaVersion:   model.SchemaVersion,
		EngineVersion:   model.EngineVersion,
		ManifestDigest:  manifestDigest(manifest),
		ConfigVersion:   cfg.Version(),
		LatencyBudgetMS: cfg.Query.LatencyBudgetMS,
		ComputeBudget:   cfg.ComputeBudgetUnits,
		Groups:          groups,
		Totals:          total.totals(cfg.ComputeBudgetUnits),
		Images:          ordered,
	}, nil
}

type aggregate struct {
	group           string
	shards          int
	characters      int
	truth           int
	candidates      int
	matched         int
	exact           int
	distanceSum     float64
	distanceSquares float64
	distanceSamples int
	budgetUsed      int
	notApplicable   int
	detectionHits   []model.StrategyHit
	recognitionHits []model.StrategyHit
}

func (a *aggregate) add(diagnostic model.ImageDiagnostic) {
	a.truth += diagnostic.Detection.TruthBoxes
	a.candidates += diagnostic.Recognition.Candidates
	a.matched += diagnostic.Detection.MatchedBoxes
	a.exact += diagnostic.Recognition.ExactMatches
	a.distanceSum += diagnostic.Recognition.DistanceSum
	a.distanceSquares += diagnostic.Recognition.DistanceSquaredSum
	a.distanceSamples += diagnostic.Recognition.DistanceSamples
	a.budgetUsed += diagnostic.BudgetUnitsUsed
	a.notApplicable += diagnostic.NotApplicableCount
	a.addHits(diagnostic)
}

func (a *aggregate) addHits(diagnostic model.ImageDiagnostic) {
	for _, hit := range diagnostic.Strategy.Detection {
		addHit(a.detectionHits, hit)
	}
	for _, hit := range diagnostic.Strategy.Recognition {
		addHit(a.recognitionHits, hit)
	}
}

func (a *aggregate) metrics(budgetLimit int) model.GroupMetrics {
	recall := model.MetricValue{}
	recallBand := model.ErrorBand{}
	if a.truth > 0 {
		recall = model.Metric(float64(a.matched) / float64(a.truth))
		recallBand = WilsonBand(a.matched, a.truth)
	} else {
		recall = model.NotApplicable()
		recallBand = NotApplicableBand()
	}
	accuracy := model.MetricValue{}
	accuracyBand := model.ErrorBand{}
	if a.candidates > 0 {
		accuracy = model.Metric(float64(a.exact) / float64(a.candidates))
		accuracyBand = WilsonBand(a.exact, a.candidates)
	} else {
		accuracy = model.NotApplicable()
		accuracyBand = NotApplicableBand()
	}
	avgDistance := model.MetricValue{}
	distanceBand := NotApplicableBand()
	if a.distanceSamples > 0 {
		avgDistance = model.Metric(a.distanceSum / float64(a.distanceSamples))
		distanceBand = MeanBand(a.distanceSum, a.distanceSquares, a.distanceSamples)
	} else {
		avgDistance = model.NotApplicable()
	}
	utilization := model.MetricValue{}
	if budgetLimit > 0 {
		utilization = model.Metric(float64(a.budgetUsed) / float64(budgetLimit))
	} else {
		utilization = model.NotApplicable()
	}
	return model.GroupMetrics{
		Group: a.group, ShardCount: a.shards, TotalCharacters: a.characters,
		TruthBoxes: a.truth, Candidates: a.candidates, MatchedBoxes: a.matched,
		DetectionRecall: recall, DetectionRecallBand: recallBand,
		ExactMatches: a.exact, RecognitionAccuracy: accuracy, RecognitionBand: accuracyBand,
		AverageMatchDistance: avgDistance, AverageDistanceBand: distanceBand,
		BudgetUnitsUsed: a.budgetUsed, BudgetUnitsLimit: budgetLimit, BudgetUtilization: utilization,
		StrategyHits:       model.StrategyHits{Detection: append([]model.StrategyHit(nil), a.detectionHits...), Recognition: append([]model.StrategyHit(nil), a.recognitionHits...)},
		NotApplicableCount: a.notApplicable,
	}
}

func (a *aggregate) totals(budgetLimit int) model.TotalsMetrics {
	group := a.metrics(budgetLimit)
	return model.TotalsMetrics{
		ShardCount: group.ShardCount, TotalCharacters: group.TotalCharacters,
		TruthBoxes: group.TruthBoxes, Candidates: group.Candidates, MatchedBoxes: group.MatchedBoxes,
		DetectionRecall: group.DetectionRecall, DetectionRecallBand: group.DetectionRecallBand,
		ExactMatches: group.ExactMatches, RecognitionAccuracy: group.RecognitionAccuracy,
		RecognitionBand: group.RecognitionBand, AverageMatchDistance: group.AverageMatchDistance,
		AverageDistanceBand: group.AverageDistanceBand,
		BudgetUnitsUsed:     group.BudgetUnitsUsed, BudgetUnitsLimit: group.BudgetUnitsLimit,
		BudgetUtilization: group.BudgetUtilization, NotApplicableCount: group.NotApplicableCount,
	}
}

func zeroHits(tiers []config.StrategyTier) []model.StrategyHit {
	hits := make([]model.StrategyHit, 0, len(tiers))
	for _, tier := range tiers {
		hits = append(hits, model.StrategyHit{Code: tier.Code, Count: 0})
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].Code < hits[j].Code })
	return hits
}

func addHit(hits []model.StrategyHit, value model.StrategyHit) {
	for i := range hits {
		if hits[i].Code == value.Code {
			hits[i].Count += value.Count
			return
		}
	}
}

func manifestDigest(manifest model.Manifest) string {
	digest, _ := canonical.Digest(manifest)
	return digest
}
