// Package priority implements stage 3: deterministic priority decisions and
// the verdict against annotated labels. Out-of-range labels produce
// E_LABEL_RANGE; missing or low agreement triggers an explicit, recorded
// downgrade rather than dropping the ticket.
package priority

import (
	"sort"
	"strconv"

	"ticketpipeline/internal/bayes"
	"ticketpipeline/internal/determ"
	"ticketpipeline/internal/model"
)

// Verdict labels used in the persisted comparison detail.
const (
	VerdictHit      = "hit"      // predicted priority equals the label
	VerdictMissed   = "missed"   // predicted below the labeled priority
	VerdictFalse    = "false"    // predicted above the labeled priority
	VerdictExcluded = "excluded" // label out of range, comparison not possible
)

// Result is the persisted per-ticket stage-3 artifact.
type Result struct {
	TicketID        string   `json:"ticket_id"`
	TopIntent       string   `json:"top_intent"`
	Score           float64  `json:"top_score"`
	Agreement       float64  `json:"agreement"`
	AgreementSeen   bool     `json:"agreement_seen"`
	BasePriority    int      `json:"base_priority"`
	FinalPriority   int      `json:"final_priority"`
	LabeledPriority int      `json:"labeled_priority"`
	Verdict         string   `json:"verdict"`
	DowngradeReason string   `json:"downgrade_reason,omitempty"`
	Notes           []string `json:"notes,omitempty"`
	Digest          string   `json:"digest"`
}

// MinValidPriority / MaxValidPriority define the legal annotation range 0..3.
const (
	MinValidPriority = 0
	MaxValidPriority = 3
)

// ValidateConfig rejects malformed threshold configuration as E_CONFIG.
func ValidateConfig(p model.Policy) *model.BizError {
	if len(p.PriorityThresh) != 3 {
		return model.NewBizError(model.CodeConfig, model.StagePriority, "",
			"exactly three priority thresholds are required", nil)
	}
	for i := 1; i < 3; i++ {
		if p.PriorityThresh[i] <= p.PriorityThresh[i-1] {
			return model.NewBizError(model.CodeConfig, model.StagePriority, "",
				"priority thresholds must be strictly increasing",
				map[string]string{"at": strconv.Itoa(i)})
		}
	}
	if p.Smoothing <= 0 || p.HashDim <= 0 {
		return model.NewBizError(model.CodeConfig, model.StagePriority, "",
			"upstream numeric configuration is invalid", nil)
	}
	return nil
}

// bucketFromThresholds maps a top score to base priority 0..3 using the three
// configured cut points: <t0 -> 0, <t1 -> 1, <t2 -> 2, otherwise 3.
func bucketFromThresholds(score float64, thresholds []float64) int {
	for i, t := range thresholds {
		if score < t {
			return i
		}
	}
	return MaxValidPriority
}

// Decide evaluates one ticket. An out-of-range annotated priority returns
// E_LABEL_RANGE while still returning a diagnostic partial result so the
// caller can persist the fact without losing the ticket.
func Decide(t model.RawTicket, intent *bayes.Result, policy model.Policy) (*Result, *model.BizError) {
	if err := ValidateConfig(policy); err != nil {
		return nil, err
	}
	if t.Priority < MinValidPriority || t.Priority > MaxValidPriority {
		r := &Result{
			TicketID: t.TicketID, TopIntent: intent.TopIntent,
			Score: intent.Scores[intent.TopIntent], LabeledPriority: t.Priority,
			Verdict: VerdictExcluded, DowngradeReason: model.DowngradeLabelRange,
			Notes: []string{"labeled priority outside 0..3; excluded from verdict comparison"},
		}
		r.Agreement, r.AgreementSeen = agreementOf(t)
		digest, _ := determ.Fingerprint(r)
		_ = digest
		r.Digest = digest
		return r, model.NewBizError(model.CodeLabelRange, model.StagePriority, t.TicketID,
			"annotated priority out of range",
			map[string]string{"priority": strconv.Itoa(t.Priority)})
	}

	score := intent.Scores[intent.TopIntent]
	res := &Result{
		TicketID:        t.TicketID,
		TopIntent:       intent.TopIntent,
		Score:           score,
		LabeledPriority: t.Priority,
	}
	agreement, seen := agreementOf(t)
	res.Agreement, res.AgreementSeen = agreement, seen
	res.BasePriority = bucketFromThresholds(score, policy.PriorityThresh)
	res.FinalPriority = res.BasePriority

	notes := []string{}
	if !seen {
		res.DowngradeReason = model.DowngradeAgreementMiss
		notes = append(notes, "agreement missing: treated as 0 and capped at priority 1")
		if res.FinalPriority > 1 {
			res.FinalPriority = 1
		}
	} else if agreement < 50 {
		res.DowngradeReason = model.DowngradeAgreementLow
		notes = append(notes, "agreement below 50: capped at priority 2")
		if res.FinalPriority > 2 {
			res.FinalPriority = 2
		}
	}
	res.Notes = notes
	res.Verdict = compare(res.FinalPriority, t.Priority)
	digest, err := determ.Fingerprint(res)
	if err != nil {
		return nil, model.NewBizError(model.CodeNumeric, model.StagePriority, t.TicketID, err.Error(), nil)
	}
	res.Digest = digest
	return res, nil
}

func compare(predicted, labeled int) string {
	switch {
	case predicted == labeled:
		return VerdictHit
	case predicted < labeled:
		return VerdictMissed
	default:
		return VerdictFalse
	}
}

func agreementOf(t model.RawTicket) (float64, bool) {
	if t.Agreement == nil {
		return 0, false
	}
	return *t.Agreement, true
}

// SortedTicketIDs returns ticket ids in lexicographic order so aggregate
// builders never depend on map iteration order.
func SortedTicketIDs(m map[string]*Result) []string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
