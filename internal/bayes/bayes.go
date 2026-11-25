// Package bayes implements stage 2: a multinomial naive Bayes intent scorer.
// All arithmetic stays in the log domain; every additive step is guarded so a
// NaN/Inf or a log(0) underflow surfaces as E_NUMERIC with the offending term
// recorded instead of silently corrupting downstream priorities.
package bayes

import (
	"hash/fnv"
	"math"
	"sort"
	"strconv"

	"ticketpipeline/internal/determ"
	"ticketpipeline/internal/model"
	"ticketpipeline/internal/normalize"
)

// UnknownIndex is the explicit unknown marker bucket appended after the
// configurable hash dimension.
const UnknownIndex = -1

// Model is the trained, serializable classifier.
type Model struct {
	Classes         []string             `json:"classes"`
	LogPrior        map[string]float64   `json:"log_prior"`
	LogCond         map[string][]float64 `json:"log_cond"` // per class, len = hashDim+1
	ClassCount      map[string]int       `json:"class_count"`
	TokenTotal      map[string]int       `json:"token_total"`
	ObservedBuckets []int                `json:"observed_buckets"` // global train vocabulary
	Smoothing       float64              `json:"smoothing"`
	HashDim         int                  `json:"hash_dim"`
	TrainingIDs     []string             `json:"training_ids"`
	Digest          string               `json:"digest"`
}

// Result is the persisted per-ticket stage-2 artifact.
type Result struct {
	TicketID    string             `json:"ticket_id"`
	Scores      map[string]float64 `json:"scores"`
	TopIntent   string             `json:"top_intent"`
	SeenTerms   int                `json:"seen_terms"`
	UnknownHits int                `json:"unknown_hits"`
	Digest      string             `json:"digest"`
}

// ValidateConfig enforces the smoothing contract. The zero-smoothing
// injection is rejected here as E_CONFIG; runtime degeneration is still
// guarded separately and reported as E_NUMERIC.
func ValidateConfig(p model.Policy) *model.BizError {
	if p.Smoothing <= 0 {
		return model.NewBizError(model.CodeConfig, model.StageIntent, "",
			"smoothing must be positive", map[string]string{"smoothing": formatFloat(p.Smoothing)})
	}
	if p.HashDim <= 0 {
		return model.NewBizError(model.CodeConfig, model.StageIntent, "",
			"hash dimension must be positive", map[string]string{"hash_dim": itoa(p.HashDim)})
	}
	if len(p.PriorityThresh) != 3 {
		return model.NewBizError(model.CodeConfig, model.StageIntent, "",
			"intent stage requires three priority thresholds", nil)
	}
	return nil
}

// InTrainingSet decides the deterministic train/test partition. The decision
// is a pure function of ticket id and sample ratio, so replaying any version
// reproduces the exact split without storing row numbers that could drift
// under concurrent merge order.
func InTrainingSet(ticketID string, ratio float64) bool {
	h := fnv.New32a()
	_, _ = h.Write([]byte(ticketID))
	v := float64(h.Sum32()%10000) / 10000.0
	return v < ratio
}

// Train builds the model from stage-1 artifacts of labeled tickets. Inputs
// are sorted by ticket id first; all emitted slices/maps are canonical.
func Train(tickets []model.RawTicket, norms map[string]*normalize.Result, policy model.Policy) (*Model, error) {
	if err := ValidateConfig(policy); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(tickets))
	byID := make(map[string]model.RawTicket, len(tickets))
	for _, t := range tickets {
		byID[t.TicketID] = t
		ids = append(ids, t.TicketID)
	}
	sort.Strings(ids)

	classCount := map[string]int{}
	tokenCount := map[string]map[int]int{}
	tokenTotal := map[string]int{}
	trainingIDs := make([]string, 0)
	classes := map[string]struct{}{}
	observed := map[int]struct{}{}
	for _, id := range ids {
		t := byID[id]
		if t.IntentLabel == "" {
			continue
		}
		nr, ok := norms[id]
		if !ok {
			continue // stage-1 failures (e.g. empty text) are not training data
		}
		if !InTrainingSet(id, policy.SampleRatio) {
			continue
		}
		trainingIDs = append(trainingIDs, id)
		classes[t.IntentLabel] = struct{}{}
		classCount[t.IntentLabel]++
		if _, ok := tokenCount[t.IntentLabel]; !ok {
			tokenCount[t.IntentLabel] = map[int]int{}
		}
		for _, tk := range nr.Tokens {
			tokenCount[t.IntentLabel][tk.Index]++
			tokenTotal[t.IntentLabel]++
			observed[tk.Index] = struct{}{}
		}
	}
	classList := make([]string, 0, len(classes))
	for c := range classes {
		classList = append(classList, c)
	}
	sort.Strings(classList)

	totalDocs := len(trainingIDs)
	if totalDocs == 0 {
		return nil, model.NewBizError(model.CodeConfig, model.StageIntent, "",
			"no labeled training tickets after sampling", nil)
	}
	vocab := policy.HashDim + 1 // feature buckets plus explicit unknown marker
	logPrior := map[string]float64{}
	logCond := map[string][]float64{}
	for _, c := range classList {
		prior := float64(classCount[c]) / float64(totalDocs)
		lp, err := safeLog(prior, "class_prior:"+c, "")
		if err != nil {
			return nil, err
		}
		logPrior[c] = lp
		denom := float64(tokenTotal[c]) + policy.Smoothing*float64(vocab)
		row := make([]float64, vocab)
		for idx := 0; idx < vocab; idx++ {
			count := float64(tokenCount[c][idx])
			prob := (count + policy.Smoothing) / denom
			lc, err := safeLog(prob, "cond:"+c, bucketName(idx))
			if err != nil {
				return nil, err
			}
			row[idx] = lc
		}
		logCond[c] = row
	}
	observedList := make([]int, 0, len(observed))
	for idx := range observed {
		observedList = append(observedList, idx)
	}
	sort.Ints(observedList)
	m := &Model{
		Classes: classList, LogPrior: logPrior, LogCond: logCond,
		ClassCount: classCount, TokenTotal: tokenTotal, ObservedBuckets: observedList,
		Smoothing: policy.Smoothing, HashDim: policy.HashDim, TrainingIDs: trainingIDs,
	}
	digest, err := determ.Fingerprint(m)
	if err != nil {
		return nil, err
	}
	m.Digest = digest
	return m, nil
}

// Score computes the log posterior of every class for one stage-1 artifact.
// Unseen feature buckets fall back to the explicit unknown marker index.
func (m *Model) Score(nr *normalize.Result, weights map[string]float64) (*Result, *model.BizError) {
	scores := make(map[string]float64, len(m.Classes))
	unknownHits := 0
	observed := make(map[int]struct{}, len(m.ObservedBuckets))
	for _, idx := range m.ObservedBuckets {
		observed[idx] = struct{}{}
	}
	for _, class := range m.Classes {
		row := m.LogCond[class]
		total := m.LogPrior[class]
		if !isFinite(total) {
			return nil, numericErr(nr.TicketID, "non-finite log prior", class, "")
		}
		for _, tk := range nr.Tokens {
			idx := tk.Index
			_, known := observed[tk.Index]
			if idx < 0 || idx >= m.HashDim || !known {
				idx = m.HashDim // explicit unknown marker slot
				unknownHits++
			}
			step := row[idx]
			if !isFinite(step) || step == 0.0 {
				return nil, numericErr(nr.TicketID, "non-finite or zero log conditional", class, tk.Term)
			}
			total += step
			if !isFinite(total) {
				return nil, numericErr(nr.TicketID, "score became non-finite", class, tk.Term)
			}
		}
		weight := 1.0
		if w, ok := weights[class]; ok {
			weight = w
		}
		if weight <= 0 || !isFinite(weight) {
			return nil, model.NewBizError(model.CodeConfig, model.StageIntent, nr.TicketID,
				"intent weight must be positive and finite",
				map[string]string{"class": class, "weight": formatFloat(weight)})
		}
		total += math.Log(weight)
		if !isFinite(total) {
			return nil, numericErr(nr.TicketID, "weighted score non-finite", class, "")
		}
		scores[class] = total
	}
	top := pickTop(scores, m.Classes)
	res := &Result{
		TicketID: nr.TicketID, Scores: scores, TopIntent: top,
		SeenTerms: len(nr.Tokens), UnknownHits: unknownHits,
	}
	digest, err := determ.Fingerprint(res)
	if err != nil {
		return nil, model.NewBizError(model.CodeNumeric, model.StageIntent, nr.TicketID, err.Error(), nil)
	}
	res.Digest = digest
	return res, nil
}

// pickTop returns the highest-scoring class, breaking ties by class name so
// identical inputs always decode to the same label.
func pickTop(scores map[string]float64, classes []string) string {
	top := classes[0]
	for _, c := range classes[1:] {
		if scores[c] > scores[top] || (scores[c] == scores[top] && c < top) {
			top = c
		}
	}
	return top
}

func safeLog(x float64, ctx, term string) (float64, error) {
	if math.IsNaN(x) || math.IsInf(x, 0) || x <= 0 {
		return 0, numericErr("", "log domain underflow or invalid operand", ctx, term)
	}
	v := math.Log(x)
	if !isFinite(v) {
		return 0, numericErr("", "log produced non-finite value", ctx, term)
	}
	return v, nil
}

func isFinite(f float64) bool { return !math.IsNaN(f) && !math.IsInf(f, 0) }

func numericErr(ticketID, msg, class, term string) *model.BizError {
	ctx := map[string]string{"class": class}
	if term != "" {
		ctx["term"] = term
	}
	return model.NewBizError(model.CodeNumeric, model.StageIntent, ticketID, msg, ctx)
}

func bucketName(idx int) string {
	if idx == UnknownIndex {
		return "unknown"
	}
	return itoa(idx)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}
