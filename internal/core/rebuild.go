package core

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// RebuildAll deliberately returns holders in stable holder-ID order, making
// reports independent of map iteration or task completion order.
func RebuildAll(holders []Holder, stored []StoredObservation, policy Policy) []RebuildResult {
	grouped := make(map[string][]StoredObservation, len(holders))
	for _, record := range stored {
		grouped[record.HolderID] = append(grouped[record.HolderID], record)
	}
	ordered := append([]Holder(nil), holders...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	results := make([]RebuildResult, 0, len(ordered))
	for _, holder := range ordered {
		results = append(results, RebuildHolder(holder, grouped[holder.ID], policy))
	}
	return results
}

// RebuildHolder sorts by client time and breaks equal timestamps with server
// arrival then report sequence. Because every ordering key is carried by the
// record itself, a shuffled delivery batch produces the same rebuilt sequence.
func RebuildHolder(holder Holder, stored []StoredObservation, policy Policy) RebuildResult {
	records := append([]StoredObservation(nil), stored...)
	sort.SliceStable(records, func(i, j int) bool {
		a, b := records[i], records[j]
		if a.ClientTimestampMS != b.ClientTimestampMS {
			return a.ClientTimestampMS < b.ClientTimestampMS
		}
		if a.ServerArrivalMS != b.ServerArrivalMS {
			return a.ServerArrivalMS < b.ServerArrivalMS
		}
		return a.ReportSeq < b.ReportSeq
	})

	safePoint := int64(math.Round(float64(holder.LeaseDurationMS) * policy.SafetyRatio))
	lower := safePoint - policy.LateToleranceMS
	if lower < 0 {
		lower = 0
	}
	upper := safePoint + policy.LateToleranceMS
	result := RebuildResult{
		HolderID:           holder.ID,
		LeaseDurationMS:    holder.LeaseDurationMS,
		SafeRenewalPointMS: safePoint,
		ObservationCount:   len(records),
		Drift: DriftDecision{
			IntervalThresholdMS: policy.DriftIntervalThresholdMS,
			ToleranceMS:         policy.LateToleranceMS,
		},
	}
	if len(records) == 0 {
		result.LeaseValidAtLast = true
		return result
	}

	unique, duplicates := ObservationCount(records)
	result.UniqueReportCount = unique
	result.DuplicateDeliveryCount = duplicates
	anchorClient := records[0].ClientTimestampMS
	var previousClient *int64
	lateRun := 0
	var maxLateRun int
	var maxContribution int64
	var driftIntervalHit bool
	var expiredHit bool

	for i, record := range records {
		rebuilt := RebuiltObservation{
			HolderID:                  record.HolderID,
			ReportSeq:                 record.ReportSeq,
			RebuiltSeq:                i + 1,
			ClientTimestampMS:         record.ClientTimestampMS,
			ServerArrivalMS:           record.ServerArrivalMS,
			Type:                      record.Type,
			Digest:                    record.Digest,
			DuplicateCount:            record.DuplicateCount,
			SafeRenewalPointMS:        safePoint,
			PreviousClientTimestampMS: previousClient,
		}
		if previousClient != nil {
			rebuilt.IntervalMS = record.ClientTimestampMS - *previousClient
		}
		elapsed := record.ClientTimestampMS - anchorClient
		if elapsed < 0 {
			elapsed = 0
		}
		rebuilt.ElapsedSinceRenewalMS = elapsed
		rebuilt.RelativeToSafePointMS = elapsed - safePoint
		switch {
		case elapsed < lower:
			rebuilt.Timing = TimingEarly
		case elapsed > upper:
			rebuilt.Timing = TimingLate
			rebuilt.LateOutsideTolerance = true
		default:
			rebuilt.Timing = TimingOnTime
		}
		rebuilt.WithinLease = elapsed <= holder.LeaseDurationMS
		rebuilt.LeaseRemainingMS = holder.LeaseDurationMS - elapsed
		if rebuilt.LeaseRemainingMS < 0 {
			rebuilt.LeaseRemainingMS = 0
		}

		if record.Type == ObservationRenewal {
			result.RenewalCount++
			rebuilt.RenewalApplied = rebuilt.WithinLease
			if rebuilt.LateOutsideTolerance {
				result.Drift.LateCount++
				lateRun++
				if lateRun > maxLateRun {
					maxLateRun = lateRun
				}
				contribution := elapsed - upper
				if contribution < 0 {
					contribution = 0
				}
				if contribution > maxContribution {
					maxContribution = contribution
				}
				rebuilt.DriftContributionMS = contribution
				result.Drift.Basis = append(result.Drift.Basis, DriftBasis{
					ReportSeq:      record.ReportSeq,
					RebuiltSeq:     rebuilt.RebuiltSeq,
					Kind:           "LATE_RENEWAL",
					Message:        fmt.Sprintf("renewal elapsed %dms is %dms beyond safe point plus tolerance", elapsed, contribution),
					ContributionMS: contribution,
				})
			} else {
				lateRun = 0
			}
			if rebuilt.IntervalMS > policy.DriftIntervalThresholdMS && rebuilt.LateOutsideTolerance {
				driftIntervalHit = true
				rebuilt.DelayOverDriftThresholdMS = rebuilt.IntervalMS - policy.DriftIntervalThresholdMS
				result.Drift.Basis = append(result.Drift.Basis, DriftBasis{
					ReportSeq:      record.ReportSeq,
					RebuiltSeq:     rebuilt.RebuiltSeq,
					Kind:           "INTERVAL_THRESHOLD",
					Message:        fmt.Sprintf("renewal interval %dms exceeds declared drift threshold %dms", rebuilt.IntervalMS, policy.DriftIntervalThresholdMS),
					ContributionMS: rebuilt.DelayOverDriftThresholdMS,
				})
			}
			if !rebuilt.WithinLease {
				expiredHit = true
				expiredContribution := elapsed - holder.LeaseDurationMS
				if expiredContribution > maxContribution {
					maxContribution = expiredContribution
				}
				result.Drift.Basis = append(result.Drift.Basis, DriftBasis{
					ReportSeq:      record.ReportSeq,
					RebuiltSeq:     rebuilt.RebuiltSeq,
					Kind:           "LEASE_EXPIRED",
					Message:        fmt.Sprintf("renewal elapsed %dms exceeds lease duration %dms", elapsed, holder.LeaseDurationMS),
					ContributionMS: expiredContribution,
				})
			}
			if rebuilt.RenewalApplied {
				anchorClient = record.ClientTimestampMS
			}
		} else if rebuilt.IntervalMS > 0 && rebuilt.LateOutsideTolerance {
			// Heartbeats are observations, not renewal anchors. They can show a
			// late alive signal but do not reset the lease anchor or drift run.
			result.Drift.Basis = append(result.Drift.Basis, DriftBasis{
				ReportSeq:  record.ReportSeq,
				RebuiltSeq: rebuilt.RebuiltSeq,
				Kind:       "LATE_HEARTBEAT",
				Message:    fmt.Sprintf("heartbeat arrived %dms after the safe renewal point", elapsed),
			})
		}
		result.LastObservedClientMS = record.ClientTimestampMS
		result.LeaseValidAtLast = rebuilt.WithinLease
		result.Rebuilt = append(result.Rebuilt, rebuilt)
		ts := record.ClientTimestampMS
		previousClient = &ts
	}

	result.Drift.MaxConsecutiveLate = maxLateRun
	result.RenewalsExhausted = result.RenewalCount >= holder.MaxRenewals
	result.Drift.Drifted = maxLateRun >= policy.ConsecutiveLateThreshold || driftIntervalHit || expiredHit
	if result.Drift.Drifted {
		result.Drift.MagnitudeMS = maxContribution
		if result.Drift.MagnitudeMS <= 0 {
			result.Drift.MagnitudeMS = 1
		}
		result.Drift.Basis = append(result.Drift.Basis, DriftBasis{
			Kind:    "DECISION",
			Message: driftDecisionMessage(result.Drift, policy),
		})
	} else {
		result.Drift.MagnitudeMS = 0
	}
	return result
}

func driftDecisionMessage(drift DriftDecision, policy Policy) string {
	parts := make([]string, 0, 2)
	if drift.MaxConsecutiveLate >= policy.ConsecutiveLateThreshold {
		parts = append(parts, fmt.Sprintf("consecutive late renewals %d >= %d", drift.MaxConsecutiveLate, policy.ConsecutiveLateThreshold))
	}
	for _, basis := range drift.Basis {
		if basis.Kind == "INTERVAL_THRESHOLD" {
			parts = append(parts, "renewal interval crossed the declared drift threshold outside tolerance")
			break
		}
	}
	for _, basis := range drift.Basis {
		if basis.Kind == "LEASE_EXPIRED" {
			parts = append(parts, "lease expired before a valid renewal")
			break
		}
	}
	if len(parts) == 0 {
		return "timing drift detected"
	}
	return strings.Join(parts, "; ")
}

func SortDeferred(entries []DeferredEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].DriftMagnitudeMS != entries[j].DriftMagnitudeMS {
			return entries[i].DriftMagnitudeMS > entries[j].DriftMagnitudeMS
		}
		return entries[i].HolderID < entries[j].HolderID
	})
	for i := range entries {
		entries[i].Order = i + 1
	}
}
