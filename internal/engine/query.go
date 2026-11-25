package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
	"time"

	"searchlab/internal/apperr"
	"searchlab/internal/compress"
	"searchlab/internal/hp"
	"searchlab/internal/index"
	"searchlab/internal/tokenize"
)

const maxEffectiveDigits = 15

type QueryRequest struct {
	QueryID string   `json:"query_id"`
	Terms   []string `json:"terms"`
	Mode    string   `json:"mode"`
}

type ScoreComponent struct {
	TermID                            int32   `json:"term_id"`
	Term                              string  `json:"term"`
	Quantized                         int64   `json:"quantized"`
	Scale                             float64 `json:"scale"`
	QuantStep                         float64 `json:"quant_step"`
	Dequantized                       float64 `json:"dequantized"`
	ReferenceWeight                   string  `json:"reference_weight"`
	ReferenceWeightFloat              float64 `json:"reference_weight_float"`
	QuantizationError                 float64 `json:"quantization_error"`
	QuantizationErrorBound            float64 `json:"quantization_error_bound"`
	ReferenceRepresentationErrorBound float64 `json:"reference_representation_error_bound"`
	CompressionErrorBound             float64 `json:"compression_error_bound"`
	LogContribution                   float64 `json:"log_contribution"`
	LogContributionDefined            bool    `json:"log_contribution_defined"`
	ReferenceLogContribution          float64 `json:"reference_log_contribution"`
	ReferenceLogDefined               bool    `json:"reference_log_contribution_defined"`
}

type Hit struct {
	Rank                       int              `json:"rank"`
	DocID                      int64            `json:"doc_id"`
	Score                      float64          `json:"score"`
	QuantizedScore             float64          `json:"quantized_score"`
	ReferenceScore             string           `json:"reference_score"`
	TheoreticalErrorBound      float64          `json:"theoretical_error_bound"`
	ObservedReferenceDeviation float64          `json:"observed_reference_deviation"`
	ErrorBound                 float64          `json:"error_bound"`
	RelativeErrorBound         float64          `json:"relative_error_bound"`
	EffectiveDigits            int              `json:"effective_digits"`
	Components                 []ScoreComponent `json:"components"`
}

type QueryDiagnostics struct {
	QueryID                  string   `json:"query_id"`
	Terms                    []string `json:"terms"`
	Mode                     string   `json:"mode"`
	MissingTerms             []string `json:"missing_terms"`
	CacheKey                 string   `json:"cache_key"`
	ReferenceImplementation  string   `json:"reference_implementation"`
	ErrorModel               string   `json:"error_model"`
	CorpusMaxQueryErrorBound float64  `json:"corpus_max_query_error_bound"`
	DecodeErrorBound         float64  `json:"decode_error_bound"`
	MaxTheoreticalErrorBound float64  `json:"max_theoretical_error_bound"`
	MaxObservedDeviation     float64  `json:"max_observed_reference_deviation"`
	ErrorBound               float64  `json:"error_bound"`
	MinEffectiveDigits       int      `json:"min_effective_digits"`
	Hits                     int      `json:"hits"`
	BlocksRead               int      `json:"blocks_read"`
	PostingsDecoded          int      `json:"postings_decoded"`
	SkipJumps                int      `json:"skip_jumps"`
	IntersectionSteps        int      `json:"intersection_steps"`
	DiagnosticDigest         string   `json:"diagnostic_digest"`
}

type QueryResult struct {
	QueryID     string           `json:"query_id"`
	Status      string           `json:"status"`
	Code        string           `json:"code,omitempty"`
	Message     string           `json:"message,omitempty"`
	Terms       []string         `json:"terms"`
	Mode        string           `json:"mode"`
	Hits        []Hit            `json:"hits"`
	Diagnostics QueryDiagnostics `json:"diagnostics"`
}

type QueryEngine struct {
	runtime *Runtime
	cache   *queryCache
	now     func() time.Time
}

func NewQueryEngine(runtime *Runtime) *QueryEngine {
	return &QueryEngine{runtime: runtime, cache: newQueryCache(runtime.Config.Query.CacheCapacity), now: time.Now}
}

func (e *QueryEngine) CacheStats() CacheStats { return e.cache.stats() }

func (e *QueryEngine) Execute(ctx context.Context, req QueryRequest) (*QueryResult, []byte, bool, error) {
	started := e.now()
	mode := strings.ToUpper(strings.TrimSpace(req.Mode))
	if mode == "" {
		mode = "OR"
	}
	if mode != "AND" && mode != "OR" {
		return nil, nil, false, &apperr.Error{Code: apperr.CodeInput, Message: "query mode must be AND or OR", QueryID: req.QueryID}
	}
	terms := tokenize.New(e.runtime.Config).QueryTerms(req.Terms)
	queryID := strings.TrimSpace(req.QueryID)
	if queryID == "" {
		parts := append([]string{"query-id/v1", mode}, terms...)
		queryID = "q-" + digestParts(parts[0], parts[1:]...)[0:16]
	}
	key := queryCacheKey(queryID, terms, mode, e.runtime.ConfigVersion, e.runtime.IndexVersion)
	if payload, ok := e.cache.get(key); ok {
		if err := checkLatencyAt(started, e.now(), e.runtime.Config.Query.LatencyBudgetMS, queryID); err != nil {
			return nil, nil, false, err
		}
		var result QueryResult
		if err := json.Unmarshal(payload, &result); err != nil {
			return nil, nil, false, apperr.E(apperr.CodeInput, "cached query payload is invalid", err)
		}
		return &result, payload, true, nil
	}
	if len(terms) == 0 {
		result := diagnosticResult(queryID, terms, mode, apperr.CodeEmptyQuery, "query contains no indexable terms", key)
		payload, err := finalizeResult(result)
		if err != nil {
			return nil, nil, false, err
		}
		e.cache.put(key, payload)
		return result, payload, false, nil
	}
	result, counts, err := e.compute(ctx, queryID, terms, mode, key, started)
	if err != nil {
		return nil, nil, false, withQueryID(err, queryID)
	}
	if err := checkLatencyAt(started, e.now(), e.runtime.Config.Query.LatencyBudgetMS, queryID); err != nil {
		return nil, nil, false, err
	}
	result.Diagnostics.BlocksRead = counts.blocksRead
	result.Diagnostics.PostingsDecoded = counts.postingsDecoded
	result.Diagnostics.SkipJumps = counts.skipJumps
	result.Diagnostics.IntersectionSteps = counts.intersectionSteps
	result.Diagnostics.Hits = len(result.Hits)
	if len(result.Hits) == 0 {
		result.Status = "diagnostic"
		result.Code = apperr.CodeNoMatch
		result.Message = "no document matched the query terms"
		result.Diagnostics.ErrorBound = 0
		result.Diagnostics.MinEffectiveDigits = 0
	} else {
		result.Status = "ok"
		minDigits := maxEffectiveDigits
		maxBound := 0.0
		maxTheoretical := 0.0
		maxDeviation := 0.0
		for _, hit := range result.Hits {
			if hit.EffectiveDigits < minDigits {
				minDigits = hit.EffectiveDigits
			}
			if hit.ErrorBound > maxBound {
				maxBound = hit.ErrorBound
			}
			if hit.TheoreticalErrorBound > maxTheoretical {
				maxTheoretical = hit.TheoreticalErrorBound
			}
			if hit.ObservedReferenceDeviation > maxDeviation {
				maxDeviation = hit.ObservedReferenceDeviation
			}
		}
		result.Diagnostics.MinEffectiveDigits = minDigits
		result.Diagnostics.ErrorBound = maxBound
		result.Diagnostics.MaxTheoreticalErrorBound = maxTheoretical
		result.Diagnostics.MaxObservedDeviation = maxDeviation
		if minDigits == 0 {
			result.Status = "diagnostic"
			result.Code = apperr.CodeZeroWeight
			result.Message = "decoded score components are all zero or carry no meaningful precision"
		}
	}
	payload, err := finalizeResult(result)
	if err != nil {
		return nil, nil, false, withQueryID(err, queryID)
	}
	e.cache.put(key, payload)
	return result, payload, false, nil
}

type queryCounts struct {
	blocksRead        int
	postingsDecoded   int
	skipJumps         int
	intersectionSteps int
}

func (e *QueryEngine) compute(ctx context.Context, queryID string, terms []string, mode, key string, started time.Time) (*QueryResult, queryCounts, error) {
	missing := make([]string, 0)
	cursors := make([]*postingCursor, 0, len(terms))
	for _, term := range terms {
		stat, ok := e.runtime.StatsByTerm[term]
		if !ok {
			missing = append(missing, term)
			continue
		}
		postingTerm, ok := e.runtime.PostingsByTerm[term]
		if !ok {
			return nil, queryCounts{}, &apperr.Error{Code: apperr.CodeDecodeMismatch, Message: "postings term is missing from runtime", Term: term}
		}
		cursors = append(cursors, newPostingCursor(stat, postingTerm))
	}
	counts := queryCounts{}
	if len(cursors) == 0 || (mode == "AND" && len(missing) > 0) {
		result := diagnosticResult(queryID, terms, mode, apperr.CodeNoMatch, "no document matched the query terms", key)
		result.Diagnostics.MissingTerms = missing
		return result, counts, nil
	}
	results := make(map[int64][]termRef)
	if mode == "AND" {
		sort.Slice(cursors, func(i, j int) bool {
			if cursors[i].term.DF != cursors[j].term.DF {
				return cursors[i].term.DF < cursors[j].term.DF
			}
			return cursors[i].term.TermID < cursors[j].term.TermID
		})
		if err := intersect(ctx, cursors, results, &counts, started, e.runtime.Config.Query.LatencyBudgetMS, queryID, e.now); err != nil {
			return nil, counts, err
		}
	} else {
		if err := unionAll(ctx, cursors, results, &counts, started, e.runtime.Config.Query.LatencyBudgetMS, queryID, e.now); err != nil {
			return nil, counts, err
		}
	}
	for _, cursor := range cursors {
		counts.blocksRead += cursor.blocksRead
		counts.postingsDecoded += cursor.postingsDecoded
		counts.skipJumps += cursor.skipJumps
	}
	hits := make([]Hit, 0, len(results))
	for docID, refs := range results {
		hit, err := e.makeHit(docID, refs)
		if err != nil {
			return nil, counts, err
		}
		hits = append(hits, hit)
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		return hits[i].DocID < hits[j].DocID
	})
	for i := range hits {
		hits[i].Rank = i + 1
	}
	result := &QueryResult{
		QueryID: queryID,
		Status:  "ok",
		Terms:   append([]string(nil), terms...),
		Mode:    mode,
		Hits:    hits,
		Diagnostics: QueryDiagnostics{
			QueryID:                  queryID,
			Terms:                    append([]string(nil), terms...),
			Mode:                     mode,
			MissingTerms:             missing,
			CacheKey:                 key,
			ReferenceImplementation:  fmt.Sprintf("math/big.Float precision=%d; natural-log atanh series; BM25 natural-log domain", hp.Precision),
			ErrorModel:               "quantization half-step plus float64 accumulation and reference-representation bounds; lossless block decode contributes exactly zero",
			CorpusMaxQueryErrorBound: e.runtime.Diagnostics.MaxQueryAbsoluteErrorBound,
			DecodeErrorBound:         0,
			MaxTheoreticalErrorBound: 0,
			MaxObservedDeviation:     0,
			ErrorBound:               0,
			MinEffectiveDigits:       0,
			Hits:                     len(hits),
		},
	}
	return result, counts, nil
}

type termRef struct {
	term    *index.TermStat
	posting compress.Posting
	block   int
}

func intersect(ctx context.Context, cursors []*postingCursor, results map[int64][]termRef, counts *queryCounts, started time.Time, budgetMS int, queryID string, now func() time.Time) error {
	if len(cursors) == 0 {
		return nil
	}
	lead := cursors[0]
	ok, err := lead.seek(ctx, math.MinInt64)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := checkLatencyAt(started, now(), budgetMS, queryID); err != nil {
			return err
		}
		counts.intersectionSteps++
		target := lead.current.DocID
		matched := true
		for _, cursor := range cursors[1:] {
			ok, err := cursor.seek(ctx, target)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			if cursor.current.DocID != target {
				matched = false
				target = cursor.current.DocID
				break
			}
		}
		if matched {
			refs := make([]termRef, 0, len(cursors))
			for _, cursor := range cursors {
				refs = append(refs, termRef{term: cursor.stat, posting: cursor.current, block: cursor.block + 1})
			}
			results[target] = refs
			if target == math.MaxInt64 {
				return nil
			}
			ok, err := lead.seek(ctx, target+1)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			continue
		}
		ok, err := lead.seek(ctx, target)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
}

func unionAll(ctx context.Context, cursors []*postingCursor, results map[int64][]termRef, counts *queryCounts, started time.Time, budgetMS int, queryID string, now func() time.Time) error {
	for _, cursor := range cursors {
		ok, err := cursor.seek(ctx, math.MinInt64)
		if err != nil {
			return err
		}
		for ok {
			if err := checkLatencyAt(started, now(), budgetMS, queryID); err != nil {
				return err
			}
			docID := cursor.current.DocID
			results[docID] = append(results[docID], termRef{term: cursor.stat, posting: cursor.current, block: cursor.block + 1})
			if docID == math.MaxInt64 {
				break
			}
			ok, err = cursor.seek(ctx, docID+1)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *QueryEngine) makeHit(docID int64, refs []termRef) (Hit, error) {
	sort.Slice(refs, func(i, j int) bool { return refs[i].term.TermID < refs[j].term.TermID })
	score := 0.0
	sumAbs := 0.0
	quantBound := 0.0
	refRepresentationBound := 0.0
	referenceSum := new(big.Float).SetPrec(hp.Precision)
	components := make([]ScoreComponent, 0, len(refs))
	for _, ref := range refs {
		expected, ok := e.runtime.StatPostingByTermDoc[ref.term.Term][docID]
		if !ok || expected.Quantized != ref.posting.Quant {
			return Hit{}, &apperr.Error{Code: apperr.CodeDecodeMismatch, Message: "decoded posting weight differs from uncompressed stats", Term: ref.term.Term, DocumentID: docID, FirstDiffBlock: ref.block}
		}
		dequantized := float64(ref.posting.Quant) / ref.term.Scale
		parsed, ok := new(big.Float).SetPrec(hp.Precision).SetString(expected.Reference)
		if !ok {
			return Hit{}, &apperr.Error{Code: apperr.CodeNumeric, Message: "stored high-precision reference weight is invalid", Term: ref.term.Term, DocumentID: docID}
		}
		referenceSum.Add(referenceSum, parsed)
		refFloat, err := hp.Float64(parsed)
		if err != nil {
			return Hit{}, &apperr.Error{Code: apperr.CodeNumeric, Message: "stored high-precision reference weight is non-finite", Term: ref.term.Term, DocumentID: docID, Err: err}
		}
		quantErr := math.Abs(dequantized - refFloat)
		bound := ref.term.QuantErrorBound
		if quantErr > bound {
			bound = math.Nextafter(quantErr, math.Inf(1))
		}
		representationBound := math.Nextafter(1e-19*math.Abs(refFloat), math.Inf(1))
		component := ScoreComponent{
			TermID:                            ref.term.TermID,
			Term:                              ref.term.Term,
			Quantized:                         ref.posting.Quant,
			Scale:                             ref.term.Scale,
			QuantStep:                         ref.term.QuantStep,
			Dequantized:                       dequantized,
			ReferenceWeight:                   expected.Reference,
			ReferenceWeightFloat:              refFloat,
			QuantizationError:                 quantErr,
			QuantizationErrorBound:            bound,
			ReferenceRepresentationErrorBound: representationBound,
			CompressionErrorBound:             0,
		}
		if dequantized > 0 {
			component.LogContribution = math.Log(dequantized)
			component.LogContributionDefined = true
		}
		if refFloat > 0 {
			component.ReferenceLogContribution = math.Log(refFloat)
			component.ReferenceLogDefined = true
		}
		components = append(components, component)
		score += dequantized
		sumAbs += math.Abs(dequantized)
		quantBound += bound
		refRepresentationBound += representationBound
	}
	referenceFloat, err := hp.Float64(referenceSum)
	if err != nil {
		return Hit{}, &apperr.Error{Code: apperr.CodeNumeric, Message: "high-precision reference score is non-finite", DocumentID: docID, Err: err}
	}
	eps := math.Ldexp(1, -53)
	n := float64(len(components))
	gamma := n * eps / (1 - n*eps)
	if gamma < 0 || math.IsNaN(gamma) || math.IsInf(gamma, 0) {
		gamma = n * eps
	}
	fpBound := math.Nextafter(gamma*sumAbs, math.Inf(1))
	theoreticalBound := math.Nextafter(quantBound+refRepresentationBound+fpBound, math.Inf(1))
	observedDeviation := math.Abs(score - referenceFloat)
	finalBound := theoreticalBound
	if observedDeviation > finalBound {
		finalBound = observedDeviation
	}
	finalBound = math.Nextafter(finalBound, math.Inf(1))
	digits := 0
	if score != 0 && finalBound > 0 && !math.IsInf(finalBound, 0) {
		ratio := math.Abs(score) / finalBound
		digits = int(math.Floor(math.Log10(ratio)))
		if digits < 0 {
			digits = 0
		}
		if digits > maxEffectiveDigits {
			digits = maxEffectiveDigits
		}
	}
	relativeBound := 0.0
	if score != 0 {
		relativeBound = finalBound / math.Abs(score)
	}
	return Hit{
		DocID:                      docID,
		Score:                      score,
		QuantizedScore:             score,
		ReferenceScore:             hp.Decimal(referenceSum, 20),
		TheoreticalErrorBound:      theoreticalBound,
		ObservedReferenceDeviation: observedDeviation,
		ErrorBound:                 finalBound,
		RelativeErrorBound:         relativeBound,
		EffectiveDigits:            digits,
		Components:                 components,
	}, nil
}

func diagnosticResult(queryID string, terms []string, mode, code, message, key string) *QueryResult {
	return &QueryResult{
		QueryID: queryID,
		Status:  "diagnostic",
		Code:    code,
		Message: message,
		Terms:   append([]string(nil), terms...),
		Mode:    mode,
		Hits:    make([]Hit, 0),
		Diagnostics: QueryDiagnostics{
			QueryID:                 queryID,
			Terms:                   append([]string(nil), terms...),
			Mode:                    mode,
			MissingTerms:            make([]string, 0),
			CacheKey:                key,
			ReferenceImplementation: fmt.Sprintf("math/big.Float precision=%d; natural-log atanh series; BM25 natural-log domain", hp.Precision),
			ErrorModel:              "quantization half-step plus float64 accumulation and reference-representation bounds; lossless block decode contributes exactly zero",
			DecodeErrorBound:        0,
			ErrorBound:              0,
			MinEffectiveDigits:      0,
		},
	}
}

func finalizeResult(result *QueryResult) ([]byte, error) {
	result.Diagnostics.DiagnosticDigest = ""
	payload, err := json.Marshal(result)
	if err != nil {
		return nil, apperr.E(apperr.CodeInput, "marshal query result", err)
	}
	result.Diagnostics.DiagnosticDigest = digest(payload)
	payload, err = json.Marshal(result)
	if err != nil {
		return nil, apperr.E(apperr.CodeInput, "marshal query result digest", err)
	}
	return payload, nil
}

func queryCacheKey(queryID string, terms []string, mode, configVersion, indexVersion string) string {
	payload, _ := json.Marshal(struct {
		QueryID       string   `json:"query_id"`
		Terms         []string `json:"terms"`
		Mode          string   `json:"mode"`
		ConfigVersion string   `json:"config_version"`
		IndexVersion  string   `json:"index_version"`
	}{queryID, terms, mode, configVersion, indexVersion})
	return digest(payload)
}

func checkLatencyAt(started, now time.Time, budgetMS int, queryID string) error {
	elapsed := now.Sub(started).Milliseconds()
	if budgetMS > 0 && elapsed > int64(budgetMS) {
		return &apperr.Error{Code: apperr.CodeLatencyBudget, Message: "query exceeded configured latency budget", QueryID: queryID, ElapsedMS: elapsed}
	}
	return nil
}

func withQueryID(err error, queryID string) error {
	if e, ok := err.(*apperr.Error); ok && e.QueryID == "" {
		e.QueryID = queryID
	}
	return err
}

type postingCursor struct {
	stat            *index.TermStat
	term            *compress.CompressedTerm
	block           int
	postings        []compress.Posting
	index           int
	base            int
	current         compress.Posting
	initialized     bool
	exhausted       bool
	blocksRead      int
	postingsDecoded int
	skipJumps       int
}

func newPostingCursor(stat *index.TermStat, term *compress.CompressedTerm) *postingCursor {
	return &postingCursor{stat: stat, term: term, block: -1}
}

func (c *postingCursor) seek(ctx context.Context, target int64) (bool, error) {
	if c.exhausted {
		return false, nil
	}
	if !c.initialized {
		if err := c.loadBlock(0); err != nil {
			return false, err
		}
		c.initialized = true
	}
	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}
		if c.index >= len(c.postings) {
			if c.block+1 >= len(c.term.Blocks) {
				c.exhausted = true
				return false, nil
			}
			if err := c.loadBlock(c.block + 1); err != nil {
				return false, err
			}
		}
		current := c.postings[c.index]
		if current.DocID >= target {
			c.current = current
			return true, nil
		}
		best := -1
		globalPosition := c.base + c.index
		for i, skip := range c.term.Skips {
			if skip.DocID <= target && skip.Posting > globalPosition {
				if best < 0 || skip.Posting > c.term.Skips[best].Posting {
					best = i
				}
			}
		}
		if best >= 0 {
			skip := c.term.Skips[best]
			c.skipJumps++
			if skip.Block != c.block {
				if err := c.loadBlock(skip.Block); err != nil {
					return false, err
				}
			}
			c.index = skip.Index
			if skip.DocID >= target {
				c.current = c.postings[c.index]
				return true, nil
			}
			continue
		}
		c.index++
	}
}

func (c *postingCursor) loadBlock(block int) error {
	if block < 0 || block >= len(c.term.Blocks) {
		return &apperr.Error{Code: apperr.CodeDecodeMismatch, Message: "skip pointer references a missing block", Term: c.term.Term, FirstDiffBlock: block + 1}
	}
	decoded, err := compress.DecodeBlock(c.term.Blocks[block])
	if err != nil {
		return &apperr.Error{Code: apperr.CodeDecodeMismatch, Message: "query block decompression failed", Term: c.term.Term, FirstDiffBlock: block + 1, Err: err}
	}
	base := 0
	for i := 0; i < block; i++ {
		base += c.term.Blocks[i].Count
	}
	c.block = block
	c.base = base
	c.postings = decoded
	c.index = 0
	c.blocksRead++
	c.postingsDecoded += len(decoded)
	return nil
}
