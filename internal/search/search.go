package search

import (
	"math"
	"sort"

	"imretrace/internal/features"
	"imretrace/internal/model"
	"imretrace/internal/quant"
	"imretrace/internal/reference"
)

type Entry struct {
	Item          model.ManifestItem   `json:"item"`
	InputHash     string               `json:"input_hash"`
	Terms         []features.TermCount `json:"terms"`
	VisualRef     []float64            `json:"visual_ref"`
	TextRef       []float64            `json:"text_ref"`
	VisualQuant   *quant.Vector        `json:"visual_quant"`
	TextQuant     *quant.Vector        `json:"text_quant"`
	FeatureDigest string               `json:"feature_digest"`
}

type Index struct {
	Policy       model.Policy  `json:"policy"`
	MatrixDigest string        `json:"matrix_digest"`
	Matrix       *quant.Matrix `json:"-"`
	VisualParams *quant.Params `json:"visual_params"`
	TextParams   *quant.Params `json:"text_params"`
	Entries      []*Entry      `json:"entries"`
	byID         map[string]*Entry
}

func NewIndex(p model.Policy, matrixDigest string, vp, tp *quant.Params, entries []*Entry) *Index {
	idx := &Index{Policy: p, MatrixDigest: matrixDigest, VisualParams: vp, TextParams: tp, Entries: entries, byID: map[string]*Entry{}}
	for _, e := range entries {
		idx.byID[e.Item.ItemID] = e
	}
	return idx
}

func (idx *Index) Get(itemID string) (*Entry, bool) {
	e, ok := idx.byID[itemID]
	return e, ok
}

type TermHit struct {
	Term           string `json:"term"`
	QueryCount     int    `json:"query_count"`
	CandidateCount int    `json:"candidate_count"`
}

type Candidate struct {
	Rank            int              `json:"rank"`
	ItemID          string           `json:"item_id"`
	ShardID         string           `json:"shard_id"`
	ImageFile       string           `json:"image_file"`
	Caption         string           `json:"caption"`
	Lang            string           `json:"lang"`
	Source          string           `json:"source"`
	ExactSource     bool             `json:"exact_source,omitempty"`
	CosineRef       float64          `json:"cosine_ref"`
	CosineQuant     float64          `json:"cosine_quant"`
	TermScore       float64          `json:"term_score"`
	TermHits        []TermHit        `json:"term_hits"`
	FusedScore      float64          `json:"fused_score"`
	Weights         model.Weights    `json:"weights"`
	Routes          RouteBreakdown   `json:"routes"`
	ScoreDiagnostic quant.ScoreBound `json:"score_diagnostic"`
	NumericOK       bool             `json:"numeric_ok"`
}

type RouteBreakdown struct {
	Query     *quant.RouteDiagnostic `json:"query"`
	Candidate *quant.RouteDiagnostic `json:"candidate"`
}

type NumericCheck struct {
	Passed       bool    `json:"passed"`
	WorstItemID  string  `json:"worst_item_id,omitempty"`
	MaxAbsDiff   float64 `json:"max_abs_diff"`
	MaxRelDiff   float64 `json:"max_rel_diff"`
	AbsTolerance float64 `json:"abs_tolerance"`
	RelTolerance float64 `json:"rel_tolerance"`
}

type Result struct {
	OK                  bool                 `json:"ok"`
	QueryType           string               `json:"query_type"`
	QueryText           string               `json:"query_text,omitempty"`
	NormalizedQueryText string               `json:"normalized_query_text,omitempty"`
	QueryItemID         string               `json:"query_item_id,omitempty"`
	TermQuerySource     string               `json:"term_query_source,omitempty"`
	PolicyVersion       int                  `json:"policy_version"`
	RequestedK          int                  `json:"requested_k"`
	IndexSize           int                  `json:"index_size"`
	Weights             model.Weights        `json:"weights"`
	WeightSum           float64              `json:"weight_sum"`
	FusionValid         bool                 `json:"fusion_valid"`
	Candidates          []Candidate          `json:"candidates"`
	NumericCheck        NumericCheck         `json:"numeric_check"`
	CorpusErrorBound    quant.CorpusEstimate `json:"corpus_error_bound"`
	PartialComponents   map[string]any       `json:"partial_components,omitempty"`
}

func termSparseScore(queryTerms []features.TermCount, candidateTerms []features.TermCount) (float64, []TermHit) {
	cm := map[string]int{}
	for _, tc := range candidateTerms {
		cm[tc.Term] = tc.Count
	}
	var dot, qn, cn float64
	hits := []TermHit{}
	for _, qt := range queryTerms {
		qn += float64(qt.Count * qt.Count)
		if cc, ok := cm[qt.Term]; ok {
			dot += float64(qt.Count * cc)
			hits = append(hits, TermHit{Term: qt.Term, QueryCount: qt.Count, CandidateCount: cc})
		}
	}
	for _, cc := range cm {
		cn += float64(cc * cc)
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i].Term < hits[j].Term })
	if qn == 0 || cn == 0 {
		return 0, hits
	}
	return math.Max(0, math.Min(1, dot/(math.Sqrt(qn)*math.Sqrt(cn)))), hits
}

type querySide struct {
	ref    []float64
	qv     *quant.Vector
	params *quant.Params
	terms  []features.TermCount
	text   string
	itemID string
}

func (idx *Index) SearchText(queryText string, queryTerms []features.TermCount, qref []float64, qv *quant.Vector, k int) (*Result, error) {
	normalized := features.NormalizeText(queryText)
	res := idx.baseResult("text_to_image", queryText, normalized, "", "query_text", k)
	if normalized == "" {
		ids := make([]string, 0, len(idx.Entries))
		for _, e := range idx.Entries {
			ids = append(ids, e.Item.ItemID)
		}
		res.PartialComponents = map[string]any{
			"index_size":    len(idx.Entries),
			"candidate_ids": ids,
			"weights":       idx.Policy.FusionWeights,
			"note":          "query was empty after normalization; scoring components were not computed",
		}
		biz := model.NewBiz(model.CodeQueryInvalid, "search", "adhoc-query", "query text is empty after normalization")
		biz.Partial = res
		return res, biz
	}
	qs := querySide{ref: qref, qv: qv, params: idx.TextParams, terms: queryTerms, text: normalized, itemID: "adhoc-query"}
	candidates, vectorBounds, scoreBounds, numCheck, err := idx.scoreAll(qs, "text", "visual", false)
	res.attach(candidates, vectorBounds, scoreBounds, numCheck)
	if err != nil {
		return res, err
	}
	return idx.finalize(res, candidates, k)
}

func (idx *Index) SearchImage(itemID string, k int) (*Result, error) {
	entry, ok := idx.Get(itemID)
	res := idx.baseResult("image_to_text", "", "", itemID, "source_item_caption_terms", k)
	if !ok {
		biz := model.NewBiz(model.CodeQueryInvalid, "search", itemID, "query image item_id not found in active index")
		biz.Partial = res
		return res, biz
	}
	res.QueryText = entry.Item.Caption
	res.NormalizedQueryText = features.NormalizeText(entry.Item.Caption)
	qs := querySide{ref: entry.VisualRef, qv: entry.VisualQuant, params: idx.VisualParams, terms: entry.Terms, text: entry.Item.Caption, itemID: itemID}
	candidates, vectorBounds, scoreBounds, numCheck, err := idx.scoreAll(qs, "visual", "text", true)
	res.attach(candidates, vectorBounds, scoreBounds, numCheck)
	if err != nil {
		return res, err
	}
	return idx.finalize(res, candidates, k)
}

func (idx *Index) baseResult(queryType, queryText, normalized, itemID, termSource string, k int) *Result {
	if k <= 0 {
		k = idx.Policy.CandidateDepth
	}
	return &Result{
		OK: false, QueryType: queryType, QueryText: queryText, NormalizedQueryText: normalized,
		QueryItemID: itemID, TermQuerySource: termSource, PolicyVersion: idx.Policy.Version,
		RequestedK: k, IndexSize: len(idx.Entries), Weights: idx.Policy.FusionWeights,
		WeightSum: idx.Policy.FusionWeights.Sum(), FusionValid: idx.Policy.FusionWeights.Sum() > 0,
		Candidates: []Candidate{},
	}
}

func (res *Result) attach(candidates []Candidate, vectorBounds, scoreBounds []float64, check NumericCheck) {
	res.Candidates = candidates
	res.NumericCheck = check
	res.CorpusErrorBound = quant.BuildCorpusEstimate(len(candidates), vectorBounds, scoreBounds)
}

func (idx *Index) scoreAll(qs querySide, queryModality, candidateModality string, imageQuery bool) ([]Candidate, []float64, []float64, NumericCheck, error) {
	out := make([]Candidate, 0, len(idx.Entries))
	vectorBounds := []float64{}
	scoreBounds := []float64{}
	check := NumericCheck{Passed: true, AbsTolerance: idx.Policy.NumericAbsTolerance, RelTolerance: idx.Policy.NumericRelTolerance}
	var worstAbs, worstRel float64
	worstItem := ""
	var numericErr error

	queryRoute, err := quant.DiagnoseRoute(qs.itemID, "query", queryModality, qs.ref, qs.qv.Dequant, qs.params)
	if err != nil {
		return nil, nil, nil, check, err
	}
	vectorBounds = append(vectorBounds, queryRoute.VectorL2Bound)

	for _, entry := range idx.Entries {
		var candRef []float64
		var candQv *quant.Vector
		var candParams *quant.Params
		if candidateModality == "visual" {
			candRef, candQv, candParams = entry.VisualRef, entry.VisualQuant, idx.VisualParams
		} else {
			candRef, candQv, candParams = entry.TextRef, entry.TextQuant, idx.TextParams
		}
		candRoute, err := quant.DiagnoseRoute(entry.Item.ItemID, "candidate", candidateModality, candRef, candQv.Dequant, candParams)
		if err != nil {
			return nil, nil, nil, check, err
		}
		vectorBounds = append(vectorBounds, candRoute.VectorL2Bound)
		refCos := reference.Cosine(qs.ref, candRef)
		quantCos := quant.Cosine(qs.qv.Dequant, candQv.Dequant)
		termScore, hits := termSparseScore(qs.terms, entry.Terms)
		cosBound := quant.BoundCosine(refCos, queryRoute, candRoute)
		wc, wt := idx.Policy.FusionWeights.Cosine, idx.Policy.FusionWeights.Term
		ws := wc + wt
		refFused, quantFused := 0.0, 0.0
		if ws > 0 {
			refFused = (wc*refCos + wt*termScore) / ws
			quantFused = (wc*quantCos + wt*termScore) / ws
		}
		sd := quant.ScoreBound{
			CosineBound: cosBound, CosineActualAbs: math.Abs(refCos - quantCos),
			FinalScoreBound: 0, FinalActualAbs: math.Abs(refFused - quantFused),
			ReferenceFusedScore: refFused, QuantFusedScore: quantFused,
			RelativeErrorBound: 0, RelativeActual: 0,
			EffectiveBits: 0, EffectiveSignificantDigits: 0, EffectiveAbsDecimalDigits: 0,
			Formula: "reference=" + reference.Implementation + "; B_cos <= (E_query+E_candidate+E_query E_candidate)(1+|cos_ref|)/((1-E_query)(1-E_candidate)); B_final=w_cos/(w_cos+w_term)*B_cos",
		}
		if ws > 0 {
			sd.FinalScoreBound = math.Abs(wc) / ws * cosBound
		}
		denom := math.Max(math.Abs(refFused), idx.Policy.RelativeScoreFloor)
		sd.RelativeErrorBound = sd.FinalScoreBound / denom
		sd.RelativeActual = sd.FinalActualAbs / denom
		sd.EffectiveBits = effectiveBits(sd.RelativeErrorBound)
		sd.EffectiveSignificantDigits = maxDigits(sd.RelativeErrorBound)
		sd.EffectiveAbsDecimalDigits = absDigits(sd.FinalScoreBound)
		sd = quant.WithVerifiedReference(sd, refFused, idx.Policy.RelativeScoreFloor)
		scoreBounds = append(scoreBounds, sd.FinalScoreBound)

		absDiff := sd.FinalActualAbs
		relDiff := sd.RelativeActual
		if absDiff > worstAbs || (absDiff == worstAbs && relDiff > worstRel) {
			worstAbs, worstRel, worstItem = absDiff, relDiff, entry.Item.ItemID
		}
		okNumeric := absDiff <= idx.Policy.NumericAbsTolerance && relDiff <= idx.Policy.NumericRelTolerance &&
			queryRoute.VectorL2Actual <= idx.Policy.NumericAbsTolerance && candRoute.VectorL2Actual <= idx.Policy.NumericAbsTolerance
		if !okNumeric && numericErr == nil {
			fA, fR := worstAbs, worstRel
			biz := model.NewBiz(model.CodeNumericMismatch, model.StageRecallVerify, entry.Item.ItemID,
				"quantized recall deviation exceeds policy tolerance: max_abs=%.6g max_rel=%.6g", absDiff, relDiff)
			biz.MaxAbsDiff = &fA
			biz.MaxRelDiff = &fR
			biz.Details = map[string]any{"candidate_id": entry.Item.ItemID, "abs_tolerance": idx.Policy.NumericAbsTolerance, "rel_tolerance": idx.Policy.NumericRelTolerance}
			numericErr = biz
		}
		out = append(out, Candidate{
			ItemID: entry.Item.ItemID, ShardID: entry.Item.ShardID, ImageFile: entry.Item.ImageFile,
			Caption: entry.Item.Caption, Lang: entry.Item.Lang, Source: entry.Item.Source,
			ExactSource: imageQuery && entry.Item.ItemID == qs.itemID,
			CosineRef:   refCos, CosineQuant: quantCos, TermScore: termScore, TermHits: hits,
			FusedScore: quantFused, Weights: idx.Policy.FusionWeights,
			Routes: RouteBreakdown{Query: queryRoute, Candidate: candRoute}, ScoreDiagnostic: sd, NumericOK: okNumeric,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].FusedScore != out[j].FusedScore {
			return out[i].FusedScore > out[j].FusedScore
		}
		return out[i].ItemID < out[j].ItemID
	})
	for i := range out {
		out[i].Rank = i + 1
	}
	check.WorstItemID, check.MaxAbsDiff, check.MaxRelDiff = worstItem, worstAbs, worstRel
	check.Passed = numericErr == nil
	if numericErr != nil {
		if biz, ok := numericErr.(*model.BizError); ok {
			biz.MaxAbsDiff = &worstAbs
			biz.MaxRelDiff = &worstRel
		}
	}
	return out, vectorBounds, scoreBounds, check, numericErr
}

func (idx *Index) finalize(res *Result, all []Candidate, k int) (*Result, error) {
	if idx.Policy.FusionWeights.Sum() <= 0 {
		res.PartialComponents = map[string]any{
			"cosine_quant": collectCos(all), "term_score": collectTerm(all),
			"candidate_ids": collectIDs(all), "note": "fusion weights are all zero; components were computed but no ranking score is valid",
		}
		biz := model.NewBiz(model.CodeQueryInvalid, "search", "", "fusion weights cosine+term must not both be zero")
		biz.Partial = res
		return res, biz
	}
	if k <= 0 {
		k = idx.Policy.CandidateDepth
		res.RequestedK = k
	}
	if k > len(all) {
		res.Candidates = all
		res.PartialComponents = map[string]any{"computed_candidate_count": len(all), "requested_k": k}
		biz := model.NewBiz(model.CodeQueryInvalid, "search", "", "k=%d exceeds active index size=%d; all computed components are retained in partial result", k, len(all))
		biz.Partial = res
		return res, biz
	}
	res.Candidates = append([]Candidate(nil), all[:k]...)
	res.OK = res.NumericCheck.Passed
	if !res.NumericCheck.Passed {
		fA, fR := res.NumericCheck.MaxAbsDiff, res.NumericCheck.MaxRelDiff
		biz := model.NewBiz(model.CodeNumericMismatch, model.StageRecallVerify, res.NumericCheck.WorstItemID,
			"recall deviation exceeds tolerance; max_abs=%.6g max_rel=%.6g", fA, fR)
		biz.MaxAbsDiff, biz.MaxRelDiff = &fA, &fR
		biz.Partial = res
		return res, biz
	}
	return res, nil
}

func effectiveBits(bound float64) float64 {
	if bound <= 0 {
		return 53
	}
	return math.Round(math.Max(0, -math.Log2(bound))*1000) / 1000
}
func maxDigits(bound float64) float64 {
	if bound <= 0 {
		return 15
	}
	return math.Max(0, math.Floor(-math.Log10(bound)))
}
func absDigits(bound float64) float64 { return maxDigits(bound) }

func collectCos(cs []Candidate) []float64 {
	out := make([]float64, len(cs))
	for i := range cs {
		out[i] = cs[i].CosineQuant
	}
	return out
}
func collectTerm(cs []Candidate) []float64 {
	out := make([]float64, len(cs))
	for i := range cs {
		out[i] = cs[i].TermScore
	}
	return out
}
func collectIDs(cs []Candidate) []string {
	out := make([]string, len(cs))
	for i := range cs {
		out[i] = cs[i].ItemID
	}
	return out
}
