package engine

import (
	"hash/fnv"
	"math"
	"sort"

	"csrouter/internal/domain"
)

const UnknownToken = "__UNKNOWN__"

type TrainingExample struct {
	Ticket    domain.Ticket
	Normalize domain.NormalizeArtifact
}

type Model struct {
	Policy           domain.Policy
	Intents          []string
	IntentIndex      map[string]int
	ClassCount       map[string]int
	ClassTermTotal   map[string]int64
	TermClassCount   map[string]map[string]int64
	TermTotal        map[string]int64
	Vocabulary       []string
	VocabularySize   int
	TotalTickets     int
	TotalTerms       int64
	VocabTopSet      map[string]bool
	HashBucketCounts [][]int64
	HashBucketTotals []int64
	ModelFingerprint string
}

func TrainModel(policy domain.Policy, examples []TrainingExample) *Model {
	intentSet := map[string]bool{}
	for _, intent := range policy.IntentNames {
		intentSet[intent] = true
	}
	for _, ex := range examples {
		if ex.Ticket.Intent != "" {
			intentSet[ex.Ticket.Intent] = true
		}
	}
	intents := make([]string, 0, len(intentSet))
	for intent := range intentSet {
		intents = append(intents, intent)
	}
	sort.Strings(intents)

	model := &Model{
		Policy:         policy,
		Intents:        intents,
		IntentIndex:    map[string]int{},
		ClassCount:     map[string]int{},
		ClassTermTotal: map[string]int64{},
		TermClassCount: map[string]map[string]int64{},
		TermTotal:      map[string]int64{},
	}
	for i, intent := range intents {
		model.IntentIndex[intent] = i
		model.ClassCount[intent] = 0
		model.ClassTermTotal[intent] = 0
	}

	for _, ex := range examples {
		model.TotalTickets++
		intent := ex.Ticket.Intent
		if intent == "" {
			intent = intents[0]
		}
		model.ClassCount[intent]++
		for _, token := range ex.Normalize.Tokens {
			term := token.Text
			model.TotalTerms++
			model.ClassTermTotal[intent]++
			model.TermTotal[term]++
			if model.TermClassCount[term] == nil {
				model.TermClassCount[term] = map[string]int64{}
			}
			model.TermClassCount[term][intent]++
		}
	}

	model.Vocabulary = domain.SortedKeys(model.TermTotal)
	model.VocabularySize = len(model.Vocabulary) + 1
	model.buildVocabTopSet()
	model.buildHashBuckets()
	model.ModelFingerprint = domain.FingerprintHex("nb-model/v1",
		policy.EffectiveVersion(), policy.SmoothingAlpha, policy.IntentVocabWeights,
		model.Intents, model.ClassCount, model.ClassTermTotal, model.TermClassCount, model.TermTotal)
	return model
}

func (m *Model) buildVocabTopSet() {
	k := m.Policy.VocabTopK
	if k <= 0 || k >= len(m.Vocabulary) {
		m.VocabTopSet = make(map[string]bool, len(m.Vocabulary))
		for _, term := range m.Vocabulary {
			m.VocabTopSet[term] = true
		}
		return
	}
	type ranked struct {
		term     string
		priority float64
		total    int64
	}
	rows := make([]ranked, 0, len(m.Vocabulary))
	alpha := m.Policy.SmoothingAlpha
	if !domain.FiniteOrNaN(alpha) || alpha < 0 {
		alpha = 0
	}
	vSize := float64(m.VocabularySize)
	totalTerms := float64(m.TotalTerms)
	for _, term := range m.Vocabulary {
		best := 0.0
		bestDiscrimination := -math.MaxFloat64
		for _, intent := range m.Intents {
			count := m.TermClassCount[term][intent]
			denom := float64(m.ClassTermTotal[intent]) + alpha*vSize
			p := (float64(count) + alpha) / denom
			if p > best {
				best = p
			}
			globalP := (float64(m.TermTotal[term]) + alpha) / (totalTerms + alpha*vSize)
			discrimination := math.Log(p / globalP)
			if discrimination > bestDiscrimination {
				bestDiscrimination = discrimination
			}
		}
		weight := maxVocabWeightForTerm(m.Policy, term)
		priority := bestDiscrimination * math.Pow(weight, 4)
		if !domain.FiniteOrNaN(priority) {
			priority = best
		}
		rows = append(rows, ranked{term: term, priority: priority, total: m.TermTotal[term]})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].priority != rows[j].priority {
			return rows[i].priority > rows[j].priority
		}
		if rows[i].total != rows[j].total {
			return rows[i].total > rows[j].total
		}
		return rows[i].term < rows[j].term
	})
	m.VocabTopSet = make(map[string]bool, k)
	for i := 0; i < k && i < len(rows); i++ {
		m.VocabTopSet[rows[i].term] = true
	}
}

func maxVocabWeightForTerm(policy domain.Policy, term string) float64 {
	weight := 1.0
	if group, ok := policy.IntentVocabWeights["*"]; ok {
		if value, ok := group[term]; ok && value > weight {
			weight = value
		}
	}
	for _, group := range policy.IntentVocabWeights {
		if value, ok := group[term]; ok && domain.FiniteOrNaN(value) && value > weight {
			weight = value
		}
	}
	return weight
}

func (m *Model) buildHashBuckets() {
	dim := m.Policy.HashDimensions
	if dim <= 0 {
		dim = 1
	}
	m.HashBucketCounts = make([][]int64, dim)
	for b := range m.HashBucketCounts {
		m.HashBucketCounts[b] = make([]int64, len(m.Intents))
	}
	m.HashBucketTotals = make([]int64, dim)
	for term, byIntent := range m.TermClassCount {
		bucket := HashBucket(term, dim)
		for intent, count := range byIntent {
			idx := m.IntentIndex[intent]
			m.HashBucketCounts[bucket][idx] += count
			m.HashBucketTotals[bucket] += count
		}
	}
}

func HashBucket(term string, dimensions int) int {
	if dimensions <= 0 {
		dimensions = 1
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(term))
	return int(h.Sum32() % uint32(dimensions))
}

func lookupVocabWeight(policy domain.Policy, intent, term string) float64 {
	if group, ok := policy.IntentVocabWeights[intent]; ok {
		if weight, ok := group[term]; ok && domain.FiniteOrNaN(weight) && weight > 0 {
			return weight
		}
	}
	if intent != "*" {
		if group, ok := policy.IntentVocabWeights["*"]; ok {
			if weight, ok := group[term]; ok && domain.FiniteOrNaN(weight) && weight > 0 {
				return weight
			}
		}
	}
	return 1.0
}

type termOccurrence struct {
	term string
	kind string
	n    int
}

func aggregateTerms(normalize domain.NormalizeArtifact) ([]termOccurrence, []string, []string) {
	order := make([]string, 0, len(normalize.Tokens))
	kinds := map[string]string{}
	counts := map[string]int{}
	for _, token := range normalize.Tokens {
		if _, seen := counts[token.Text]; !seen {
			order = append(order, token.Text)
			kinds[token.Text] = token.Kind
		}
		counts[token.Text]++
	}
	occurrences := make([]termOccurrence, 0, len(order))
	for _, term := range order {
		occurrences = append(occurrences, termOccurrence{term: term, kind: kinds[term], n: counts[term]})
	}
	return occurrences, order, nil
}

func (m *Model) ScoreTicket(ticket domain.Ticket, normalize domain.NormalizeArtifact, strategy string) (domain.IntentArtifact, *domain.BizError) {
	occurrences, _, _ := aggregateTerms(normalize)
	result := domain.IntentArtifact{
		TicketID:         ticket.TicketID,
		Strategy:         strategy,
		ModelFingerprint: m.ModelFingerprint,
		VocabFingerprint: m.Policy.SectionFingerprint("vocab_weights"),
		Candidates:       make([]domain.IntentCandidate, 0, len(m.Intents)),
		UnknownTokens:    []string{},
		TruncatedTokens:  []string{},
		HashBucketsUsed:  []int{},
	}

	unknownSet := map[string]bool{}
	truncatedSet := map[string]bool{}
	bucketSet := map[int]bool{}

	if alpha := m.Policy.SmoothingAlpha; !domain.FiniteOrNaN(alpha) {
		bizErr := domain.NewBizError(domain.ErrNonFiniteScore, ticket.TicketID, safeFirstIntent(m.Intents), m.Policy.EffectiveVersion(),
			"smoothing_alpha is NaN or Inf")
		result.ErrorCode = bizErr.Code
		result.Digest = domain.DigestArtifact(domain.StageIntent, result)
		return result, bizErr
	}

	for intentIndex, intent := range m.Intents {
		if m.Policy.SmoothingAlpha == 0 {
			bizErr := domain.NewBizError(domain.ErrInvalidSmoothing, ticket.TicketID, intent, m.Policy.EffectiveVersion(),
				"smoothing_alpha must be non-zero for log-domain scoring")
			result.ErrorCode = bizErr.Code
			result.Digest = domain.DigestArtifact(domain.StageIntent, result)
			return result, bizErr
		}

		candidate := domain.IntentCandidate{Intent: intent, Contributions: []domain.Contribution{}}
		prior := float64(m.ClassCount[intent]) / float64(m.TotalTickets)
		candidate.LogPrior = math.Log(prior)
		bias := 1.0
		if value, ok := m.Policy.IntentBias[intent]; ok && domain.FiniteOrNaN(value) && value > 0 {
			bias = value
		}
		candidate.LogBias = math.Log(bias)
		score := candidate.LogPrior + candidate.LogBias
		candidate.EstimatedOperations = len(occurrences)

		denomExact := float64(m.ClassTermTotal[intent]) + m.Policy.SmoothingAlpha*float64(m.VocabularySize)
		denomHash := float64(m.ClassTermTotal[intent]) + m.Policy.SmoothingAlpha*float64(m.Policy.HashDimensions)

		for _, occ := range occurrences {
			feature := occ.term
			source := "observed"
			bucket := 0
			var conditionalP float64

			switch strategy {
			case StrategyHashBucket:
				if _, known := m.TermTotal[feature]; !known {
					unknownSet[feature] = true
					feature = UnknownToken
					source = "unknown_marker_hashed"
				} else {
					source = "hash_bucket"
				}
				bucket = HashBucket(feature, m.Policy.HashDimensions)
				bucketSet[bucket] = true
				numerator := float64(m.HashBucketCounts[bucket][intentIndex]) + m.Policy.SmoothingAlpha
				conditionalP = numerator / denomHash
			case StrategyVocabPriority:
				if _, known := m.TermTotal[feature]; !known {
					unknownSet[feature] = true
					feature = UnknownToken
					source = "unknown_marker"
					conditionalP = (m.Policy.SmoothingAlpha) / denomExact
				} else if !m.VocabTopSet[occ.term] {
					truncatedSet[occ.term] = true
					continue
				} else {
					count := m.TermClassCount[occ.term][intent]
					conditionalP = (float64(count) + m.Policy.SmoothingAlpha) / denomExact
				}
			default:
				strategy = StrategyExactFull
				if _, known := m.TermTotal[feature]; !known {
					unknownSet[feature] = true
					feature = UnknownToken
					source = "unknown_marker"
					conditionalP = m.Policy.SmoothingAlpha / denomExact
				} else {
					count := m.TermClassCount[occ.term][intent]
					conditionalP = (float64(count) + m.Policy.SmoothingAlpha) / denomExact
				}
			}

			weight := lookupVocabWeight(m.Policy, intent, occ.term)
			logP := math.Log(conditionalP)
			logW := math.Log(weight)
			logContribution := float64(occ.n) * (logP + logW)
			if !domain.FiniteOrNaN(logP) || !domain.FiniteOrNaN(logW) || !domain.FiniteOrNaN(logContribution) {
				bizErr := domain.NewBizError(domain.ErrNonFiniteScore, ticket.TicketID, intent, m.Policy.EffectiveVersion(),
					"non-finite log contribution for term %q", occ.term)
				bizErr.Value = logContribution
				result.ErrorCode = bizErr.Code
				result.Digest = domain.DigestArtifact(domain.StageIntent, result)
				return result, bizErr
			}
			contribution := domain.Contribution{
				Token:           occ.term,
				Kind:            occ.kind,
				Source:          source,
				Bucket:          bucket,
				Occurrences:     occ.n,
				ConditionalP:    conditionalP,
				LogConditional:  logP,
				VocabWeight:     weight,
				LogWeight:       logW,
				LogContribution: logContribution,
			}
			candidate.Contributions = append(candidate.Contributions, contribution)
			score += logContribution
			candidate.LogLikelihood += logContribution
			candidate.EstimatedOperations++
		}

		if !domain.FiniteOrNaN(score) {
			bizErr := domain.NewBizError(domain.ErrNonFiniteScore, ticket.TicketID, intent, m.Policy.EffectiveVersion(),
				"non-finite intent score")
			bizErr.Value = score
			result.ErrorCode = bizErr.Code
			result.Digest = domain.DigestArtifact(domain.StageIntent, result)
			return result, bizErr
		}
		candidate.Score = score
		result.Candidates = append(result.Candidates, candidate)
	}

	sort.Slice(result.Candidates, func(i, j int) bool {
		if result.Candidates[i].Score != result.Candidates[j].Score {
			return result.Candidates[i].Score > result.Candidates[j].Score
		}
		return result.Candidates[i].Intent < result.Candidates[j].Intent
	})

	maxScore := result.Candidates[0].Score
	sumExp := 0.0
	for i := range result.Candidates {
		sumExp += math.Exp(result.Candidates[i].Score - maxScore)
	}
	for i := range result.Candidates {
		c := &result.Candidates[i]
		c.Rank = i + 1
		c.Posterior = math.Exp(c.Score-maxScore) / sumExp
		c.FullContributions = i < 3
		if i >= 3 {
			c.Contributions = nil
		}
	}

	result.TopIntent = result.Candidates[0].Intent
	result.TopScore = result.Candidates[0].Score
	result.TopPosterior = result.Candidates[0].Posterior
	result.UnknownTokens = domain.SortedKeys(unknownSet)
	result.TruncatedTokens = domain.SortedKeys(truncatedSet)
	result.HashBucketsUsed = sortedIntKeys(bucketSet)
	result.Digest = domain.DigestArtifact(domain.StageIntent, result)
	return result, nil
}

func sortedIntKeys(set map[int]bool) []int {
	out := make([]int, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}

func safeFirstIntent(intents []string) string {
	if len(intents) > 0 {
		return intents[0]
	}
	return ""
}
