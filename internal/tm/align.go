package tm

import (
	"math"
	"sort"
	"strings"
)

type AlignConfig struct {
	NGramSize      int     `json:"ngram_size"`
	NGramWeight    float64 `json:"ngram_weight"`
	LengthWeight   float64 `json:"length_weight"`
	LanguageWeight float64 `json:"language_weight"`
	Threshold      float64 `json:"threshold"`
	Epsilon        float64 `json:"epsilon"`
	CoverageFloor  float64 `json:"coverage_floor"`
}

func DefaultAlignConfig() AlignConfig {
	return AlignConfig{
		NGramSize:      2,
		NGramWeight:    0.45,
		LengthWeight:   0.35,
		LanguageWeight: 0.20,
		Threshold:      0.16,
		Epsilon:        1e-6,
		CoverageFloor:  0.05,
	}
}

func (c AlignConfig) withDefaults() AlignConfig {
	d := DefaultAlignConfig()
	if c.NGramSize <= 0 {
		c.NGramSize = d.NGramSize
	}
	if c.NGramWeight == 0 {
		c.NGramWeight = d.NGramWeight
	}
	if c.LengthWeight == 0 {
		c.LengthWeight = d.LengthWeight
	}
	if c.LanguageWeight == 0 {
		c.LanguageWeight = d.LanguageWeight
	}
	if c.Threshold == 0 {
		c.Threshold = d.Threshold
	}
	if c.Epsilon <= 0 {
		c.Epsilon = d.Epsilon
	}
	if c.CoverageFloor <= 0 {
		c.CoverageFloor = d.CoverageFloor
	}
	return c
}

type Offset [2]int

type AlignmentScore struct {
	NGramCoverage  float64 `json:"ngram_coverage"`
	LengthRatio    float64 `json:"length_ratio"`
	LanguageCompat float64 `json:"language_compat"`
	LogScore       float64 `json:"log_score"`
	Score          float64 `json:"score"`
}

type Pair struct {
	SourceIndex  int            `json:"source_index"`
	TargetIndex  int            `json:"target_index"`
	SourceOffset Offset         `json:"source_offset"`
	TargetOffset Offset         `json:"target_offset"`
	SourceText   string         `json:"source_text"`
	TargetText   string         `json:"target_text"`
	Score        AlignmentScore `json:"score"`
	Arbitration  string         `json:"arbitration"`
	LostTo       *PairRef       `json:"lost_to,omitempty"`
}

type PairRef struct {
	SourceIndex int `json:"source_index"`
	TargetIndex int `json:"target_index"`
}

type Candidate struct {
	Pair
	selected bool
	reason   string
}

type SegmentAlignment struct {
	SegmentID  string      `json:"segment_id"`
	Candidates []Pair      `json:"candidates"`
	Pairs      []Pair      `json:"pairs"`
	Config     AlignConfig `json:"config"`
	Digest     string      `json:"artifact_digest"`
}

func ngrams(s string, n int) map[string]int {
	runes := []rune(CoreText(s))
	out := map[string]int{}
	if len(runes) == 0 {
		return out
	}
	if len(runes) < n {
		out[string(runes)]++
		return out
	}
	for i := 0; i+n <= len(runes); i++ {
		out[string(runes[i:i+n])]++
	}
	return out
}

func NGramCoverage(a, b string, n int) float64 {
	ga := ngrams(a, n)
	gb := ngrams(b, n)
	if len(ga) == 0 || len(gb) == 0 {
		return 0
	}
	intersection := 0
	for g, ca := range ga {
		if cb, ok := gb[g]; ok {
			if ca < cb {
				intersection += ca
			} else {
				intersection += cb
			}
		}
	}
	totalA := 0
	totalB := 0
	for _, c := range ga {
		totalA += c
	}
	for _, c := range gb {
		totalB += c
	}
	precision := float64(intersection) / float64(totalA)
	recall := float64(intersection) / float64(totalB)
	if precision+recall == 0 {
		return 0
	}
	return 2 * precision * recall / (precision + recall)
}

func LengthRatio(a, b string) float64 {
	la := len([]rune(CoreText(a)))
	lb := len([]rune(CoreText(b)))
	if la == 0 || lb == 0 {
		return 0
	}
	if la < lb {
		return float64(la) / float64(lb)
	}
	return float64(lb) / float64(la)
}

func baseLang(tag string) string {
	tag = strings.ToLower(strings.TrimSpace(tag))
	if i := strings.IndexAny(tag, "-_"); i >= 0 {
		tag = tag[:i]
	}
	return tag
}

func LanguageCompatibility(sourceLang, targetLang string) float64 {
	s := baseLang(sourceLang)
	t := baseLang(targetLang)
	if s == "" || t == "" {
		return 0.5
	}
	if s == t {
		return 1
	}
	pair := s + "->" + t
	switch pair {
	case "zh->en", "en->zh", "ja->en", "en->ja", "ko->en", "en->ko":
		return 0.9
	case "zh->ja", "ja->zh", "zh->ko", "ko->zh":
		return 0.75
	case "fr->en", "en->fr", "de->en", "en->de", "es->en", "en->es":
		return 0.85
	default:
		return 0.55
	}
}

func CombineScore(cov, lengthRatio, langCompat float64, cfg AlignConfig) (AlignmentScore, error) {
	cfg = cfg.withDefaults()
	actualCov := cov
	if lengthRatio == 0 {
		return AlignmentScore{NGramCoverage: cov, LengthRatio: lengthRatio, LanguageCompat: langCompat}, errZeroLength
	}
	if !finite(cov) || !finite(lengthRatio) || !finite(langCompat) {
		return AlignmentScore{}, errNonFinite
	}
	if cov < 0 || lengthRatio < 0 || langCompat < 0 {
		return AlignmentScore{}, errNonFinite
	}
	totalWeight := cfg.NGramWeight + cfg.LengthWeight + cfg.LanguageWeight
	logScore := (cfg.NGramWeight*math.Log(math.Max(cov, cfg.CoverageFloor)) +
		cfg.LengthWeight*math.Log(math.Max(lengthRatio, cfg.Epsilon)) +
		cfg.LanguageWeight*math.Log(math.Max(langCompat, cfg.Epsilon))) / totalWeight
	score := math.Exp(logScore)
	if !finite(logScore) || !finite(score) {
		return AlignmentScore{}, errNonFinite
	}
	return AlignmentScore{
		NGramCoverage:  actualCov,
		LengthRatio:    lengthRatio,
		LanguageCompat: langCompat,
		LogScore:       logScore,
		Score:          score,
	}, nil
}

var errZeroLength = &simpleAlignError{"length ratio is zero"}
var errNonFinite = &simpleAlignError{"alignment score is non-finite"}

type simpleAlignError struct{ msg string }

func (e *simpleAlignError) Error() string { return e.msg }

func finite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func AlignSegment(segmentID string, sourceLang, targetLang string, source, target SegmentedText, cfg AlignConfig) (SegmentAlignment, error) {
	cfg = cfg.withDefaults()
	ss := ValidSentences(source.Sentences)
	ts := ValidSentences(target.Sentences)
	result := SegmentAlignment{SegmentID: segmentID, Config: cfg}

	candidates := make([]Candidate, 0, len(ss)*len(ts))
	for _, s := range ss {
		for _, t := range ts {
			cov := NGramCoverage(s.Text, t.Text, cfg.NGramSize)
			lr := LengthRatio(s.Text, t.Text)
			lc := LanguageCompatibility(sourceLang, targetLang)
			score, err := CombineScore(cov, lr, lc, cfg)
			so := Offset{s.Start, s.End}
			to := Offset{t.Start, t.End}
			if err != nil {
				return SegmentAlignment{}, NewAlignInvalid(segmentID, so, to, err.Error())
			}
			candidates = append(candidates, Candidate{
				Pair: Pair{
					SourceIndex:  s.Index,
					TargetIndex:  t.Index,
					SourceOffset: so,
					TargetOffset: to,
					SourceText:   s.Text,
					TargetText:   t.Text,
					Score:        score,
				},
			})
		}
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		a, b := candidates[i].Score.Score, candidates[j].Score.Score
		if a != b {
			return a > b
		}
		if candidates[i].SourceOffset[0] != candidates[j].SourceOffset[0] {
			return candidates[i].SourceOffset[0] < candidates[j].SourceOffset[0]
		}
		return candidates[i].TargetOffset[0] < candidates[j].TargetOffset[0]
	})

	usedSource := map[int]bool{}
	usedTarget := map[int]bool{}
	for i := range candidates {
		c := &candidates[i]
		if c.Score.Score < cfg.Threshold {
			c.reason = "below_threshold"
			continue
		}
		if usedSource[c.SourceIndex] || usedTarget[c.TargetIndex] {
			c.reason = "mutex_lost"
			continue
		}
		c.selected = true
		c.reason = "selected"
		c.Arbitration = "deterministic_greedy:score_desc,source_offset_asc,target_offset_asc"
		usedSource[c.SourceIndex] = true
		usedTarget[c.TargetIndex] = true
	}

	for i := range candidates {
		c := &candidates[i]
		pair := c.Pair
		if !c.selected && c.reason == "mutex_lost" {
			for j := range candidates {
				w := &candidates[j]
				if w.selected && (w.SourceIndex == c.SourceIndex || w.TargetIndex == c.TargetIndex) {
					pair.LostTo = &PairRef{SourceIndex: w.SourceIndex, TargetIndex: w.TargetIndex}
					pair.Arbitration = "rejected_by_mutex:higher_or_tie_deterministic_winner"
					break
				}
			}
		}
		if !c.selected && c.reason == "below_threshold" {
			pair.Arbitration = "rejected:below_configured_threshold"
		}
		result.Candidates = append(result.Candidates, pair)
	}

	pairs := make([]Pair, 0)
	for _, c := range candidates {
		if c.selected {
			pairs = append(pairs, c.Pair)
		}
	}
	sort.SliceStable(pairs, func(i, j int) bool {
		return pairs[i].SourceOffset[0] < pairs[j].SourceOffset[0]
	})
	result.Pairs = pairs
	digest, _, err := DigestCanonical(result)
	if err != nil {
		return SegmentAlignment{}, err
	}
	result.Digest = digest
	return result, nil
}
