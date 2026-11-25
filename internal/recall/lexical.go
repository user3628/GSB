package recall

import (
	"math"

	"multirecall/internal/model"
	"multirecall/internal/textnorm"
)

type lexicalDoc struct {
	ItemID string
	Result textnorm.Result
	TF     map[string]int
}

type LexicalIndex struct {
	Docs         []lexicalDoc
	DocumentFreq map[string]int
	IndexedTerms int
	IndexedDocs  int
}

func NewLexicalIndex(items []model.Item) *LexicalIndex {
	idx := &LexicalIndex{DocumentFreq: map[string]int{}, IndexedDocs: len(items)}
	for _, item := range items {
		tr := textnorm.Tokenize(item.Title)
		tf := map[string]int{}
		for _, term := range tr.Terms {
			tf[term]++
		}
		for term := range tf {
			idx.DocumentFreq[term]++
		}
		idx.Docs = append(idx.Docs, lexicalDoc{ItemID: item.ItemID, Result: tr, TF: tf})
	}
	idx.IndexedTerms = len(idx.DocumentFreq)
	return idx
}

func lexicalIDF(n, df int) float64 {
	return math.Log(1 + float64(n)/float64(df+1))
}

func lexicalTermErrorBound(idf float64, idfFP int64) float64 {
	quantIDF := float64(idfFP) / float64(ScaleLexical)
	return 0.5/float64(ScaleLexical) +
		quantIDF*(0.5/float64(ScaleLexical)) +
		idf*(0.5/float64(ScaleLexical)) +
		0.5/float64(ScaleLexical)
}

func (idx *LexicalIndex) Search(q model.Query, depth int) (RouteResult, *model.APIError) {
	qr := textnorm.Tokenize(q.RawQuery)
	if qr.Normalized == "" || len(qr.UniqueTerms) == 0 || idx == nil || idx.IndexedTerms == 0 {
		return RouteResult{}, &model.APIError{
			Code: model.CodeEmptyQuery, QueryID: q.QueryID,
			Message: "query normalizes to zero terms or lexical inverted index is empty",
		}
	}

	var cands []Candidate
	for _, doc := range idx.Docs {
		var hits []TermHit
		var scoreFP int64
		var rawBound float64
		for _, term := range qr.UniqueTerms {
			tf, ok := doc.TF[term]
			if !ok {
				continue
			}
			df := idx.DocumentFreq[term]
			idf := lexicalIDF(idx.IndexedDocs, df)
			idfFP := int64(math.Round(idf * float64(ScaleLexical)))
			satFP := int64(math.Round(float64(ScaleLexical) * float64(tf) / float64(tf+1)))
			contributionFP := roundDivNearest(idfFP*satFP, ScaleLexical)
			scoreFP += contributionFP
			termBound := lexicalTermErrorBound(idf, idfFP)
			rawBound += termBound
			hits = append(hits, TermHit{
				Term: term, Kind: termKind(qr, term), DocumentFrequency: df,
				IDF: idf, IDFFP: idfFP, TermFrequency: tf, SaturationFP: satFP,
				ContributionFP: contributionFP, Contribution: float64(contributionFP) / float64(ScaleLexical),
			})
		}
		if scoreFP == 0 {
			continue
		}
		ev := LexicalEvidence{
			IndexedTerms: idx.IndexedTerms, IndexedDocs: idx.IndexedDocs,
			QueryTerms: len(qr.UniqueTerms), MatchedTerms: len(hits), TermHits: hits,
			ScoreFP: scoreFP, Score: float64(scoreFP) / float64(ScaleLexical),
			ErrorBound: rawBound, FixedScale: ScaleLexical,
			Formula: "sum round(round(idf*1e6)*round(tf/(tf+1)*1e6)/1e6); idf=ln(1+N/(df+1))",
		}
		cands = append(cands, Candidate{
			ItemID: doc.ItemID, DomainScore: ev.Score, DomainScoreFP: scoreFP,
			RawErrorBound: rawBound, Lexical: &ev,
		})
	}
	return finalizeRoute("lexical", depth, depth, cands), nil
}

func termKind(qr textnorm.Result, term string) string {
	for _, t := range qr.Tokens {
		if t.Text == term {
			return t.Kind
		}
	}
	return "term"
}

func LexicalReferenceScore(idx *LexicalIndex, q model.Query, doc lexicalDoc) float64 {
	qr := textnorm.Tokenize(q.RawQuery)
	var score float64
	for _, term := range qr.UniqueTerms {
		tf, ok := doc.TF[term]
		if !ok {
			continue
		}
		df := idx.DocumentFreq[term]
		idf := lexicalIDF(idx.IndexedDocs, df)
		sat := float64(tf) / float64(tf+1)
		score += idf * sat
	}
	return score
}
