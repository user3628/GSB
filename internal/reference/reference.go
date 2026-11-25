package reference

import (
	"math"

	"csrouter/internal/domain"
	"csrouter/internal/engine"
)

type TicketReference struct {
	TicketID          string             `json:"ticket_id"`
	TopIntent         string             `json:"top_intent"`
	TopScore          float64            `json:"top_score"`
	TopPosterior      float64            `json:"top_posterior"`
	Scores            map[string]float64 `json:"scores"`
	Posteriors        map[string]float64 `json:"posteriors"`
	PredictedPriority *int               `json:"predicted_priority"`
	CombinedScore     float64            `json:"combined_score"`
	Digest            string             `json:"digest"`
}

type Result struct {
	SchemaVersion string            `json:"schema_version"`
	Tier          string            `json:"tier"`
	PolicyVersion int64             `json:"policy_version"`
	Strategy      string            `json:"strategy"`
	Tickets       []TicketReference `json:"tickets"`
	Fingerprint   string            `json:"fingerprint"`
}

func Build(tier string, policy domain.Policy, examples []engine.TrainingExample, model *engine.Model) (*Result, error) {
	result := &Result{
		SchemaVersion: "high-precision-reference/v1",
		Tier:          tier,
		PolicyVersion: policy.EffectiveVersion(),
		Strategy:      engine.StrategyExactFull,
		Tickets:       make([]TicketReference, 0, len(examples)),
	}
	for _, ex := range examples {
		intent, bizErr := model.ScoreTicket(ex.Ticket, ex.Normalize, engine.StrategyExactFull)
		if bizErr != nil {
			return nil, bizErr
		}
		priority := engine.DecidePriority(ex.Ticket, policy, intent)
		scores := map[string]float64{}
		posteriors := map[string]float64{}
		for _, candidate := range intent.Candidates {
			scores[candidate.Intent] = candidate.Score
			posteriors[candidate.Intent] = candidate.Posterior
		}
		row := TicketReference{
			TicketID:          ex.Ticket.TicketID,
			TopIntent:         intent.TopIntent,
			TopScore:          intent.TopScore,
			TopPosterior:      intent.TopPosterior,
			Scores:            scores,
			Posteriors:        posteriors,
			PredictedPriority: priority.PredictedPriority,
			CombinedScore:     priority.CombinedScore,
		}
		row.Digest = domain.DigestArtifact("reference-ticket/v1", row)
		result.Tickets = append(result.Tickets, row)
	}
	result.Fingerprint = domain.FingerprintHex("high-precision-reference/v1", result.Tier, result.PolicyVersion, result.Tickets)
	return result, nil
}

func Compare(policy domain.Policy, engineIntents []domain.IntentArtifact, enginePriorities []domain.PriorityArtifact, ref *Result) domain.ReferenceComparison {
	cmp := domain.ReferenceComparison{
		ReferenceFingerprint: ref.Fingerprint,
		Thresholds: map[string]float64{
			"exact_score_abs_diff":            1e-12,
			"medium_mean_centered_score_diff": 1.25,
			"medium_max_posterior_diff":       0.05,
		},
	}
	refByID := map[string]TicketReference{}
	for _, row := range ref.Tickets {
		refByID[row.TicketID] = row
	}

	var diffSum float64
	var centeredDiffSum float64
	candidateComparisons := 0
	priorityComparable := 0
	for _, intent := range engineIntents {
		refRow := refByID[intent.TicketID]
		cmp.ComparedTickets++
		if intent.TopIntent == refRow.TopIntent {
			cmp.IntentTop1Matches++
		}

		engineMean := 0.0
		refMean := 0.0
		for _, candidate := range intent.Candidates {
			engineMean += candidate.Score
			refMean += refRow.Scores[candidate.Intent]
		}
		if len(intent.Candidates) > 0 {
			engineMean /= float64(len(intent.Candidates))
			refMean /= float64(len(intent.Candidates))
		}

		ticketExact := true
		for _, candidate := range intent.Candidates {
			refScore := refRow.Scores[candidate.Intent]
			diff := abs(candidate.Score - refScore)
			centeredDiff := abs((candidate.Score - engineMean) - (refScore - refMean))
			posteriorDiff := abs(candidate.Posterior - referencePosterior(refRow, candidate.Intent))
			diffSum += diff
			centeredDiffSum += centeredDiff
			candidateComparisons++
			cmp.MaxScoreAbsDiff = math.Max(cmp.MaxScoreAbsDiff, diff)
			cmp.MaxCenteredScoreDiff = math.Max(cmp.MaxCenteredScoreDiff, centeredDiff)
			cmp.MaxPosteriorDiff = math.Max(cmp.MaxPosteriorDiff, posteriorDiff)
			if diff > 1e-12 {
				ticketExact = false
			}
		}
		if ticketExact {
			cmp.ExactScoreTickets++
		}
	}
	if candidateComparisons > 0 {
		cmp.MeanScoreAbsDiff = round12(diffSum / float64(candidateComparisons))
		cmp.MeanCenteredScoreDiff = round12(centeredDiffSum / float64(candidateComparisons))
	}
	cmp.MaxScoreAbsDiff = round12(cmp.MaxScoreAbsDiff)
	cmp.MaxCenteredScoreDiff = round12(cmp.MaxCenteredScoreDiff)
	cmp.MaxPosteriorDiff = round12(cmp.MaxPosteriorDiff)

	for _, priority := range enginePriorities {
		refRow := refByID[priority.TicketID]
		if priority.PredictedPriority == nil || refRow.PredictedPriority == nil {
			continue
		}
		priorityComparable++
		if *priority.PredictedPriority == *refRow.PredictedPriority {
			cmp.PriorityMatches++
		}
	}

	engineFingerprint := domain.FingerprintHex("engine-comparison/v1", engineIntents, enginePriorities)
	cmp.EngineFingerprint = engineFingerprint
	if policy.Tier == "small" {
		cmp.Pass = cmp.IntentTop1Matches == cmp.ComparedTickets &&
			priorityComparable > 0 && cmp.PriorityMatches == priorityComparable &&
			cmp.MaxScoreAbsDiff <= 1e-12
		cmp.Note = "small tier uses exact_full; all candidate scores must match the high-precision reference within 1e-12"
	} else {
		cmp.Pass = cmp.IntentTop1Matches == cmp.ComparedTickets &&
			priorityComparable > 0 && cmp.PriorityMatches == priorityComparable &&
			cmp.MeanCenteredScoreDiff <= 1.25 &&
			cmp.MaxPosteriorDiff <= 0.05
		cmp.Note = "medium uses low-collision hash-bucket scoring; require exact top intent/priority and near-identical posteriors, with bounded centered log-score margin error"
	}
	return cmp
}

func referencePosterior(row TicketReference, intent string) float64 {
	return row.Posteriors[intent]
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

func round12(v float64) float64 {
	return float64(int64(v*1e12+0.5)) / 1e12
}
