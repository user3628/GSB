package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"sort"

	"searchlab/internal/apperr"
	"searchlab/internal/config"
	"searchlab/internal/hp"
	"searchlab/internal/tokenize"
)

func BuildStats(cfg config.Config, tokens tokenize.Artifact) ([]byte, *StatsArtifact, error) {
	docCount := len(tokens.Docs)
	if docCount == 0 {
		return nil, nil, apperr.E(apperr.CodeNumeric, "no documents to index", nil)
	}
	docs := make([]DocStat, 0, docCount)
	lengths := make([]int64, docCount)
	total := int64(0)
	tokenCounts := make([][]int64, docCount)
	termFrequencies := make([][]int64, docCount)
	for i, doc := range tokens.Docs {
		titleLen := len(doc.TitleTokens)
		bodyLen := len(doc.BodyTokens)
		length := titleLen + bodyLen
		lengths[i] = int64(length)
		total += int64(length)
		docs = append(docs, DocStat{DocID: doc.DocID, TitleLen: titleLen, BodyLen: bodyLen, Length: length, Tokenized: true})
		termFrequencies[i] = make([]int64, len(tokens.Terms)+1)
		for _, token := range doc.TitleTokens {
			if token.TermID > 0 && int(token.TermID) < len(termFrequencies[i]) {
				termFrequencies[i][token.TermID]++
			}
		}
		for _, token := range doc.BodyTokens {
			if token.TermID > 0 && int(token.TermID) < len(termFrequencies[i]) {
				termFrequencies[i][token.TermID]++
			}
		}
		tokenCounts[i] = termFrequencies[i]
	}
	if total == 0 {
		return nil, nil, apperr.E(apperr.CodeNumeric, "average document length is zero", nil)
	}
	avg := hp.FromRatio(total, int64(docCount))
	if avg.Sign() <= 0 {
		return nil, nil, apperr.E(apperr.CodeNumeric, "average document length is zero", nil)
	}
	avgFloat, err := hp.Float64(avg)
	if err != nil {
		return nil, nil, apperr.E(apperr.CodeNumeric, "average document length is non-finite", err)
	}
	k1 := hp.FromFloat(cfg.Index.BM25.K1)
	b := hp.FromFloat(cfg.Index.BM25.B)
	if k1 == nil || b == nil {
		error := &apperr.Error{Code: apperr.CodeNumeric, Message: "BM25 parameter is non-finite"}
		if len(tokens.Terms) > 0 {
			error.Term = tokens.Terms[0].Term
		}
		if len(tokens.Docs) > 0 {
			error.DocumentID = tokens.Docs[0].DocID
		}
		return nil, nil, error
	}
	qMax := int64((1 << (cfg.Index.QuantBits - 1)) - 1)
	if qMax <= 0 {
		return nil, nil, apperr.E(apperr.CodeNumeric, "invalid quantization range", nil)
	}
	terms := make([]TermStat, 0, len(tokens.Terms))
	allQuantZero := true
	for _, term := range tokens.Terms {
		df := int64(0)
		for i := range tokenCounts {
			if termFrequencies[i][term.ID] > 0 {
				df++
			}
		}
		if df == 0 {
			return nil, nil, &apperr.Error{Code: apperr.CodeNumeric, Message: "term has zero document frequency", Term: term.Term}
		}
		idf, err := hp.IDF(int64(docCount), df)
		if err != nil {
			return nil, nil, &apperr.Error{Code: apperr.CodeNumeric, Message: "idf is not finite", Term: term.Term, Err: err}
		}
		idfFloat, err := hp.Float64(idf)
		if err != nil {
			return nil, nil, &apperr.Error{Code: apperr.CodeNumeric, Message: "idf is not finite as float64", Term: term.Term, Err: err}
		}
		type hpWeight struct {
			docID int64
			tf    int64
			value *big.Float
		}
		weights := make([]hpWeight, 0)
		for i := range tokenCounts {
			tf := termFrequencies[i][term.ID]
			if tf == 0 {
				continue
			}
			w, err := hp.BM25Weight(idf, tf, lengths[i], avg, k1, b)
			if err != nil {
				return nil, nil, &apperr.Error{Code: apperr.CodeNumeric, Message: "BM25 weight is not finite", Term: term.Term, DocumentID: tokens.Docs[i].DocID, Err: err}
			}
			weights = append(weights, hpWeight{docID: tokens.Docs[i].DocID, tf: tf, value: w})
		}
		maxRef := weights[0].value
		for _, w := range weights[1:] {
			if w.value.Cmp(maxRef) > 0 {
				maxRef = w.value
			}
		}
		maxFloat, err := hp.Float64(maxRef)
		if err != nil {
			return nil, nil, &apperr.Error{Code: apperr.CodeNumeric, Message: "maximum BM25 weight is non-finite", Term: term.Term, DocumentID: weights[0].docID, Err: err}
		}
		if maxFloat <= 0 || math.IsNaN(maxFloat) || math.IsInf(maxFloat, 0) {
			return nil, nil, &apperr.Error{Code: apperr.CodeNumeric, Message: "quantization scale factor is zero or non-finite", Term: term.Term, DocumentID: weights[0].docID}
		}
		scale := math.Nextafter(float64(qMax)/maxFloat, 0)
		if scale <= 0 || math.IsNaN(scale) || math.IsInf(scale, 0) {
			return nil, nil, &apperr.Error{Code: apperr.CodeNumeric, Message: "quantization scale factor is zero or non-finite", Term: term.Term, DocumentID: weights[0].docID}
		}
		step := 1 / scale
		bound := math.Nextafter(0.5*step, math.Inf(1))
		postings := make([]QuantPosting, 0, len(weights))
		termHasNonzero := false
		for _, w := range weights {
			refFloat, err := hp.Float64(w.value)
			if err != nil {
				return nil, nil, &apperr.Error{Code: apperr.CodeNumeric, Message: "BM25 weight is non-finite", Term: term.Term, DocumentID: w.docID, Err: err}
			}
			scaled := refFloat * scale
			if math.IsNaN(scaled) || math.IsInf(scaled, 0) {
				return nil, nil, &apperr.Error{Code: apperr.CodeNumeric, Message: "scaled BM25 weight is non-finite", Term: term.Term, DocumentID: w.docID}
			}
			q := int64(math.Round(scaled))
			if q > qMax || q < -qMax {
				return nil, nil, &apperr.Error{Code: apperr.CodeNumeric, Message: fmt.Sprintf("quantized weight %d exceeds signed %d-bit range", q, cfg.Index.QuantBits), Term: term.Term, DocumentID: w.docID}
			}
			if q != 0 {
				termHasNonzero = true
				allQuantZero = false
			}
			postings = append(postings, QuantPosting{
				DocID:          w.docID,
				Quantized:      q,
				Reference:      hp.Decimal(w.value, 20),
				ReferenceFloat: refFloat,
				Dequantized:    float64(q) / scale,
				ErrorBound:     bound,
			})
		}
		if !termHasNonzero {
			return nil, nil, &apperr.Error{Code: apperr.CodeNumeric, Message: "all quantized weights for term are zero", Term: term.Term, DocumentID: postings[0].DocID}
		}
		sort.Slice(postings, func(i, j int) bool { return postings[i].DocID < postings[j].DocID })
		terms = append(terms, TermStat{
			TermID:          term.ID,
			Term:            term.Term,
			DF:              int(df),
			IDF:             hp.Decimal(idf, 20),
			IDFFloat:        idfFloat,
			MaxWeight:       hp.Decimal(maxRef, 20),
			MaxWeightFloat:  maxFloat,
			Scale:           scale,
			QuantStep:       step,
			QuantErrorBound: bound,
			Postings:        postings,
		})
	}
	if allQuantZero {
		return nil, nil, apperr.E(apperr.CodeNumeric, "all quantized weights are zero", nil)
	}
	artifact := StatsArtifact{
		ConfigVersion: cfg.Version,
		StatsDigest:   cfg.StatsDigest(),
		DocCount:      docCount,
		TotalLength:   int(total),
		AverageLength: hp.Decimal(avg, 20),
		AverageFloat:  avgFloat,
		Docs:          docs,
		Terms:         terms,
	}
	payload, err := json.Marshal(artifact)
	if err != nil {
		return nil, nil, apperr.E(apperr.CodeInput, "marshal stats artifact", err)
	}
	return payload, &artifact, nil
}
