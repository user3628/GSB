package engine

import (
	"math"

	"csrouter/internal/domain"
)

func DecidePriority(ticket domain.Ticket, policy domain.Policy, intent domain.IntentArtifact) domain.PriorityArtifact {
	result := domain.PriorityArtifact{
		TicketID:             ticket.TicketID,
		TopIntent:            intent.TopIntent,
		ModelConfidence:      intent.TopPosterior,
		AnnotationConfidence: ticket.ConfidencePct,
		DegradationSources:   []string{},
	}

	severity := 0.35
	if value, ok := policy.IntentSeverity[intent.TopIntent]; ok && domain.FiniteOrNaN(value) {
		severity = value
	}
	result.SeverityScore = severity

	applied := policy.MissingConfidenceFallback
	if ticket.ConfidencePct == nil {
		result.DegradationSources = append(result.DegradationSources, "confidence_missing:fallback_policy_value")
	} else {
		value := *ticket.ConfidencePct
		if math.IsNaN(value) || math.IsInf(value, 0) {
			result.DegradationSources = append(result.DegradationSources, "confidence_non_finite:fallback_policy_value")
		} else if value < 0 || value > 100 {
			applied = math.Max(0, math.Min(100, value))
			result.DegradationSources = append(result.DegradationSources, "confidence_out_of_range:clamped")
		} else {
			applied = value
		}
	}
	result.AppliedConfidencePct = applied

	modelConf := intent.TopPosterior
	if !domain.FiniteOrNaN(modelConf) {
		modelConf = 0
	}
	combined := 0.60*severity + 0.20*modelConf + 0.20*(applied/100.0)
	result.CombinedScore = roundFloat(combined, 12)

	priority := 0
	switch {
	case combined >= policy.PriorityThresholds.P3:
		priority = 3
	case combined >= policy.PriorityThresholds.P2:
		priority = 2
	case combined >= policy.PriorityThresholds.P1:
		priority = 1
	default:
		priority = 0
	}
	result.PredictedPriority = &priority

	if ticket.Priority == nil || *ticket.Priority < 0 || *ticket.Priority > 3 {
		result.DegradationSources = append(result.DegradationSources, "priority_label_out_of_range:comparison_unavailable")
	}
	if (ticket.Priority == nil || *ticket.Priority < 0 || *ticket.Priority > 3) && ticket.ConfidencePct == nil {
		result.ErrorCode = domain.ErrInvalidLabel
		result.PredictedPriority = nil
		result.DegradationSources = append(result.DegradationSources, "all_annotation_basis_unavailable:E_INVALID_LABEL")
	}

	result.Digest = domain.DigestArtifact(domain.StagePriority, result)
	return result
}

func VerifyTicket(ticket domain.Ticket, tier, shard string, intent domain.IntentArtifact, priority domain.PriorityArtifact) domain.VerificationDetail {
	detail := domain.VerificationDetail{
		TicketID:           ticket.TicketID,
		Tier:               tier,
		Shard:              shard,
		Channel:            ticket.Channel,
		Language:           ticket.Language,
		AnnotatedIntent:    ticket.Intent,
		PredictedIntent:    intent.TopIntent,
		IntentHit:          ticket.Intent != "" && ticket.Intent == intent.TopIntent,
		AnnotatedPriority:  ticket.Priority,
		PredictedPriority:  priority.PredictedPriority,
		DegradationSources: append([]string{}, priority.DegradationSources...),
		ErrorCode:          priority.ErrorCode,
	}

	if priority.ErrorCode == domain.ErrInvalidLabel {
		detail.Comparison = "INVALID_LABEL"
		return detail
	}
	if ticket.Priority == nil || *ticket.Priority < 0 || *ticket.Priority > 3 {
		detail.Comparison = "UNCOMPARABLE_PRIORITY"
		if detail.ErrorCode == "" {
			detail.ErrorCode = "DEGRADED_PRIORITY_LABEL"
		}
		return detail
	}
	if priority.PredictedPriority == nil {
		detail.Comparison = "INVALID_LABEL"
		detail.ErrorCode = domain.ErrInvalidLabel
		return detail
	}

	annotated := *ticket.Priority
	predicted := *priority.PredictedPriority
	detail.Delta = annotated - predicted
	switch {
	case annotated == predicted:
		detail.Comparison = "HIT"
	case annotated > predicted:
		detail.Comparison = "MISS"
	default:
		detail.Comparison = "FALSE_POSITIVE"
		detail.Delta = predicted - annotated
	}
	return detail
}

func roundFloat(v float64, digits int) float64 {
	pow := math.Pow10(digits)
	return math.Round(v*pow) / pow
}
