package core

import (
	"fmt"
	"math"
	"strings"
)

// DecideHeal selects an action from the holder's declared strategy. AUTO is a
// deterministic policy, not randomness: severe/expired cases revoke, medium
// cases shorten the lease, and small drift asks for an earlier renewal.
func DecideHeal(holder Holder, result RebuildResult, nowMS int64) *HealDecision {
	if !result.Drift.Drifted {
		return nil
	}
	action := HealAction(holder.HealStrategy)
	if action != HealActionShortenLease && action != HealActionRequireEarlyRenew && action != HealActionRevokeLease {
		action = selectAutoHeal(holder, result)
	}
	trigger := healTrigger(result.Drift)
	reason := "timing drift detected"
	for i := len(result.Drift.Basis) - 1; i >= 0; i-- {
		if result.Drift.Basis[i].Kind == "DECISION" && result.Drift.Basis[i].Message != "" {
			reason = result.Drift.Basis[i].Message
			break
		}
	}
	decision := &HealDecision{
		HolderID:         holder.ID,
		Action:           action,
		Trigger:          trigger,
		Reason:           reason,
		DriftMagnitudeMS: result.Drift.MagnitudeMS,
		PreviousLeaseMS:  holder.LeaseDurationMS,
		DecidedAtMS:      nowMS,
	}
	switch action {
	case HealActionShortenLease:
		newLease := int64(math.Round(float64(holder.LeaseDurationMS) * 0.75))
		if newLease < 1 {
			newLease = 1
		}
		decision.NewLeaseDurationMS = newLease
		decision.ExpectedRecoveryAtMS = nowMS + newLease
	case HealActionRequireEarlyRenew:
		point := int64(math.Round(float64(result.SafeRenewalPointMS) * 0.60))
		if point < 1 {
			point = 1
		}
		decision.EarlyRenewPointMS = point
		decision.ExpectedRecoveryAtMS = nowMS + point
	case HealActionRevokeLease:
		decision.ExpectedRecoveryAtMS = nowMS
	}
	return decision
}

func selectAutoHeal(holder Holder, result RebuildResult) HealAction {
	if result.RenewalsExhausted || !result.LeaseValidAtLast || result.Drift.MagnitudeMS >= holder.LeaseDurationMS {
		return HealActionRevokeLease
	}
	if result.Drift.MagnitudeMS >= maxInt64(200, holder.LeaseDurationMS/5) {
		return HealActionShortenLease
	}
	return HealActionRequireEarlyRenew
}

func healTrigger(drift DriftDecision) string {
	triggers := make([]string, 0, 3)
	for _, basis := range drift.Basis {
		switch basis.Kind {
		case "LATE_RENEWAL":
			if !contains(triggers, "LATE_OUTSIDE_TOLERANCE") {
				triggers = append(triggers, "LATE_OUTSIDE_TOLERANCE")
			}
		case "INTERVAL_THRESHOLD":
			if !contains(triggers, "INTERVAL_OVER_THRESHOLD") {
				triggers = append(triggers, "INTERVAL_OVER_THRESHOLD")
			}
		case "LEASE_EXPIRED":
			if !contains(triggers, "LEASE_EXPIRED") {
				triggers = append(triggers, "LEASE_EXPIRED")
			}
		}
	}
	if len(triggers) == 0 {
		return "DRIFT"
	}
	return strings.Join(triggers, "+")
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

type HealRequest struct {
	RequestID            string     `json:"request_id"`
	HolderID             string     `json:"holder_id"`
	Action               HealAction `json:"action"`
	Trigger              string     `json:"trigger"`
	DriftMagnitudeMS     int64      `json:"drift_magnitude_ms"`
	PreviousLeaseMS      int64      `json:"previous_lease_ms"`
	NewLeaseDurationMS   int64      `json:"new_lease_duration_ms,omitempty"`
	EarlyRenewPointMS    int64      `json:"early_renew_point_ms,omitempty"`
	ExpectedRecoveryAtMS int64      `json:"expected_recovery_at_ms"`
	SentAtMS             int64      `json:"sent_at_ms"`
}

type HealReceipt struct {
	RequestID    string `json:"request_id"`
	HolderID     string `json:"holder_id"`
	Applied      bool   `json:"applied"`
	ServerTimeMS int64  `json:"server_time_ms"`
}

func (d HealDecision) Request(requestID string, sentAtMS int64) HealRequest {
	return HealRequest{
		RequestID:            requestID,
		HolderID:             d.HolderID,
		Action:               d.Action,
		Trigger:              d.Trigger,
		DriftMagnitudeMS:     d.DriftMagnitudeMS,
		PreviousLeaseMS:      d.PreviousLeaseMS,
		NewLeaseDurationMS:   d.NewLeaseDurationMS,
		EarlyRenewPointMS:    d.EarlyRenewPointMS,
		ExpectedRecoveryAtMS: d.ExpectedRecoveryAtMS,
		SentAtMS:             sentAtMS,
	}
}

func (d HealDecision) String() string {
	return fmt.Sprintf("%s -> %s", d.HolderID, d.Action)
}
