package engine

import (
	"fmt"
	"sort"
	"strings"

	"queryrewrite/internal/gen"
	"queryrewrite/internal/norm"
	"queryrewrite/internal/spec"
)

// rawCandidate is an unsorted generated rewrite before dedup/ranking.
type rawCandidate struct {
	text     string
	strategy string
	order    int // strategy priority: history=0, synonym=1
	seq      int // stable within-strategy sequence
}

// lexicon is the fixed local Han word set (synonym heads) used by tokenization.
var lexicon = norm.NewLexicon(gen.SynonymKeys())

// Stage1 normalizes the raw query and produces a deterministic, byte-stable
// candidate list. Business failures carry session/turn/stage stable codes.
func Stage1(qt spec.QueryTurn, p spec.Policy) ([]spec.Token, []spec.Candidate, *spec.BizError) {
	stage := spec.StageGenerate
	if strings.TrimSpace(qt.RawQuery) == "" {
		return nil, nil, spec.NewBizError(spec.CodeEmptyQuery,
			"raw query is empty", qt.SessionID, qt.Turn, stage)
	}

	normalized := norm.Normalize(qt.RawQuery)
	if normalized == "" {
		return nil, nil, spec.NewBizError(spec.CodeEmptyQuery,
			"normalized query is empty", qt.SessionID, qt.Turn, stage)
	}
	tokens := norm.Tokenize(normalized, lexicon)
	originTerms := norm.TermSet(tokens)

	raws := make([]rawCandidate, 0)

	// Strategy 1: historical rewrites (ordered as provided, dedup later).
	for seq, h := range qt.History {
		hn := norm.Normalize(h)
		if hn == "" || hn == normalized {
			continue
		}
		raws = append(raws, rawCandidate{text: hn, strategy: "history", order: 0, seq: seq})
	}

	// Strategy 2: local synonym substitution. For every matchable token we emit
	// one variant per (occurrence, synonym); offsets are rune-based so CJK and
	// latin tokens share the same deterministic code path.
	seq := 0
	runeNorm := []rune(normalized)
	occOf := map[string]int{}
	for _, tok := range tokens {
		syns := gen.Synonyms(tok.Text)
		if len(syns) == 0 {
			continue
		}
		for start := 0; start+tok.End-tok.Start <= len(runeNorm); start++ {
			if string(runeNorm[start:start+(tok.End-tok.Start)]) != tok.Text {
				continue
			}
			// Emit each distinct occurrence only once per token position kind.
			key := fmt.Sprintf("%s@%d", tok.Text, start)
			if seen := occOf[key]; seen > 0 {
				continue
			}
			occOf[key]++
			for _, syn := range syns {
				variantRunes := append(append([]rune{}, runeNorm[:start]...),
					append([]rune(syn), runeNorm[start+(tok.End-tok.Start):]...)...)
				variant := norm.Normalize(string(variantRunes))
				if variant == "" || variant == normalized {
					continue
				}
				raws = append(raws, rawCandidate{
					text: variant, strategy: "synonym", order: 1, seq: seq,
				})
				seq++
			}
		}
	}

	// Deterministic dedup: keep the earliest (order, seq) occurrence of each text.
	best := map[string]rawCandidate{}
	for _, rc := range raws {
		if cur, ok := best[rc.text]; !ok || rc.order < cur.order ||
			(rc.order == cur.order && rc.seq < cur.seq) {
			best[rc.text] = rc
		}
	}
	uniq := make([]rawCandidate, 0, len(best))
	for _, rc := range best {
		uniq = append(uniq, rc)
	}
	sort.Slice(uniq, func(i, j int) bool {
		if uniq[i].order != uniq[j].order {
			return uniq[i].order < uniq[j].order
		}
		if uniq[i].seq != uniq[j].seq {
			return uniq[i].seq < uniq[j].seq
		}
		return uniq[i].text < uniq[j].text
	})

	depth := p.CandidateDepth
	if depth <= 0 {
		depth = len(uniq)
	}
	if len(uniq) > depth {
		uniq = uniq[:depth]
	}

	if len(uniq) == 0 {
		return tokens, nil, spec.NewBizError(spec.CodeNoCandidates,
			"no rewrite candidates generated", qt.SessionID, qt.Turn, stage)
	}

	cands := make([]spec.Candidate, 0, len(uniq))
	for i, rc := range uniq {
		candTerms := norm.TermSet(norm.Tokenize(rc.text, lexicon))
		if len(candTerms) == 0 {
			return tokens, nil, spec.NewBizError(spec.CodeEmptyRewrite,
				"rewrite text is empty after normalization", qt.SessionID, qt.Turn, stage).
				WithDetail("candidate_text", rc.text)
		}
		cands = append(cands, spec.Candidate{
			ID:       fmt.Sprintf("c%03d", i+1),
			Text:     rc.text,
			Strategy: rc.strategy,
			Overlap:  round4(norm.Overlap(originTerms, candTerms)),
			Rank:     i + 1,
		})
	}
	return tokens, cands, nil
}

// round4 rounds to 4 decimal places, keeping deterministic JSON output.
func round4(f float64) float64 {
	return float64(int64(f*10000+0.5)) / 10000
}
