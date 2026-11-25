package engine

import (
	"encoding/json"
	"fmt"
	"sort"

	"searchlab/internal/apperr"
	"searchlab/internal/compress"
	"searchlab/internal/config"
	"searchlab/internal/hp"
)

type TermDiagnostic struct {
	TermID          int32   `json:"term_id"`
	Term            string  `json:"term"`
	DF              int     `json:"df"`
	Scale           float64 `json:"scale"`
	QuantStep       float64 `json:"quant_step"`
	QuantErrorBound float64 `json:"quant_error_bound"`
	IDF             string  `json:"idf"`
	MaxWeight       string  `json:"max_weight"`
}

type DocCurvePoint struct {
	Documents                      int     `json:"documents"`
	CumulativeOccurrenceErrorBound float64 `json:"cumulative_occurrence_error_bound"`
	Postings                       int     `json:"postings"`
}

type TermCurvePoint struct {
	Terms                      int     `json:"terms"`
	CorpusOccurrenceErrorBound float64 `json:"corpus_occurrence_error_bound"`
	Postings                   int     `json:"postings"`
}

type DiagnosticsArtifact struct {
	ConfigVersion              string           `json:"config_version"`
	DiagnosticsDigest          string           `json:"diagnostics_digest"`
	ReferenceImplementation    string           `json:"reference_implementation"`
	ErrorModel                 string           `json:"error_model"`
	Terms                      []TermDiagnostic `json:"terms"`
	DocCurve                   []DocCurvePoint  `json:"doc_curve"`
	TermCurve                  []TermCurvePoint `json:"term_curve"`
	CorpusOccurrenceErrorBound float64          `json:"corpus_occurrence_error_bound"`
	MaxQueryAbsoluteErrorBound float64          `json:"max_query_absolute_error_bound"`
	UncompressedPostings       int              `json:"uncompressed_postings"`
}

func BuildDiagnostics(cfg config.Config, stats StatsArtifact, postings compress.Artifact) ([]byte, *DiagnosticsArtifact, error) {
	if len(stats.Terms) != len(postings.Terms) {
		return nil, nil, apperr.E(apperr.CodeDecodeMismatch, "stats and postings term count differ", nil)
	}
	terms := make([]TermDiagnostic, 0, len(stats.Terms))
	allPostings := make([]struct {
		docID int64
		bound float64
	}, 0)
	maxQueryBound := 0.0
	for i := range stats.Terms {
		stat := stats.Terms[i]
		postingTerm := postings.Terms[i]
		if stat.TermID != postingTerm.TermID || stat.Term != postingTerm.Term || stat.DF != postingTerm.DF {
			return nil, nil, &apperr.Error{Code: apperr.CodeDecodeMismatch, Message: "stats and postings term identity differ", Term: stat.Term}
		}
		terms = append(terms, TermDiagnostic{
			TermID:          stat.TermID,
			Term:            stat.Term,
			DF:              stat.DF,
			Scale:           stat.Scale,
			QuantStep:       stat.QuantStep,
			QuantErrorBound: stat.QuantErrorBound,
			IDF:             stat.IDF,
			MaxWeight:       stat.MaxWeight,
		})
		maxQueryBound += stat.QuantErrorBound
		for _, posting := range stat.Postings {
			allPostings = append(allPostings, struct {
				docID int64
				bound float64
			}{docID: posting.DocID, bound: stat.QuantErrorBound})
		}
	}
	sort.Slice(allPostings, func(i, j int) bool { return allPostings[i].docID < allPostings[j].docID })
	corpusBound := 0.0
	for _, posting := range allPostings {
		corpusBound += posting.bound
	}
	docCount := len(stats.Docs)
	docPoints := uniqueInts([]int{1, docCount / 4, docCount / 2, docCount})
	docCurve := make([]DocCurvePoint, 0, len(docPoints))
	for _, limit := range docPoints {
		if limit < 1 {
			continue
		}
		if limit > docCount {
			limit = docCount
		}
		cutoff := stats.Docs[limit-1].DocID
		bound := 0.0
		count := 0
		for _, posting := range allPostings {
			if posting.docID <= cutoff {
				bound += posting.bound
				count++
			}
		}
		docCurve = append(docCurve, DocCurvePoint{Documents: limit, CumulativeOccurrenceErrorBound: bound, Postings: count})
	}
	termCount := len(stats.Terms)
	termPoints := uniqueInts([]int{1, termCount / 4, termCount / 2, termCount})
	termCurve := make([]TermCurvePoint, 0, len(termPoints))
	for _, limit := range termPoints {
		if limit < 1 {
			continue
		}
		if limit > termCount {
			limit = termCount
		}
		bound := 0.0
		count := 0
		for i := 0; i < limit; i++ {
			for range stats.Terms[i].Postings {
				bound += stats.Terms[i].QuantErrorBound
				count++
			}
		}
		termCurve = append(termCurve, TermCurvePoint{Terms: limit, CorpusOccurrenceErrorBound: bound, Postings: count})
	}
	artifact := DiagnosticsArtifact{
		ConfigVersion:              cfg.Version,
		DiagnosticsDigest:          cfg.DiagnosticsDigest(),
		ReferenceImplementation:    fmt.Sprintf("math/big.Float precision=%d; natural-log atanh series; BM25 natural-log domain", hp.Precision),
		ErrorModel:                 "half quant step 0.5/scale per quantized posting; block decode is lossless and must be exactly zero; float64 accumulation bound reported by each query",
		Terms:                      terms,
		DocCurve:                   docCurve,
		TermCurve:                  termCurve,
		CorpusOccurrenceErrorBound: corpusBound,
		MaxQueryAbsoluteErrorBound: maxQueryBound,
		UncompressedPostings:       len(allPostings),
	}
	payload, err := json.Marshal(artifact)
	if err != nil {
		return nil, nil, apperr.E(apperr.CodeInput, "marshal diagnostics artifact", err)
	}
	return payload, &artifact, nil
}

func uniqueInts(values []int) []int {
	sort.Ints(values)
	out := make([]int, 0, len(values))
	for _, value := range values {
		if value < 1 {
			continue
		}
		if len(out) == 0 || out[len(out)-1] != value {
			out = append(out, value)
		}
	}
	return out
}
