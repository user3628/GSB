package sample

import (
	"fmt"
	"math/rand"

	"leasearbiter/internal/core"
)

const DefaultSeed int64 = 20261126

const BaseEpochMS int64 = 1_760_000_000_000

type builder struct {
	rng     *rand.Rand
	policy  core.Policy
	holders []core.Holder
	records []core.ObservationRecord
}

// Generate builds the same names, observations, duplicate deliveries and
// arrival reordering for a given seed. Randomness is used only for small,
// deterministic server-arrival jitter; ordering semantics remain explicit.
func Generate(seed int64) core.Dataset {
	b := &builder{
		rng: rand.New(rand.NewSource(seed)),
		policy: core.Policy{
			SafetyRatio:              0.70,
			LateToleranceMS:          50,
			DriftIntervalThresholdMS: 1200,
			ConsecutiveLateThreshold: 2,
			HealTimeoutMS:            300,
		},
	}
	b.holders = []core.Holder{
		{ID: "holder-alpha", LeaseDurationMS: 1000, MaxRenewals: 8, Status: core.HolderActive, HealStrategy: core.HealAuto},
		{ID: "holder-bravo", LeaseDurationMS: 1000, MaxRenewals: 8, Status: core.HolderActive, HealStrategy: core.HealRequireEarlyRenew},
		{ID: "holder-charlie", LeaseDurationMS: 2000, MaxRenewals: 10, Status: core.HolderActive, HealStrategy: core.HealShortenLease},
		{ID: "holder-delta", LeaseDurationMS: 800, MaxRenewals: 2, Status: core.HolderActive, HealStrategy: core.HealRevokeLease},
	}

	// Healthy cadence with one idempotent duplicate and two arrivals that land
	// out of report order. The duplicate is adjacent in capture order, as the
	// monotonic-sequence intake contract requires.
	b.add("holder-alpha", 1, 0, core.ObservationHeartbeat, 40)
	b.add("holder-alpha", 2, 700, core.ObservationRenewal, 90)
	b.duplicate("holder-alpha", 2, 700, core.ObservationRenewal, 520)
	b.add("holder-alpha", 3, 850, core.ObservationHeartbeat, 800)
	b.add("holder-alpha", 4, 1400, core.ObservationRenewal, 5)
	b.add("holder-alpha", 5, 2100, core.ObservationRenewal, 20)
	b.add("holder-alpha", 6, 2800, core.ObservationRenewal, 10)

	// Two consecutive renewals just outside the safe-point tolerance. The
	// holder declares REQUIRE_EARLY_RENEWAL as its heal action.
	b.add("holder-bravo", 1, 0, core.ObservationHeartbeat, 60)
	b.add("holder-bravo", 2, 700, core.ObservationRenewal, 95)
	b.add("holder-bravo", 3, 1480, core.ObservationRenewal, 840)
	b.add("holder-bravo", 4, 2260, core.ObservationRenewal, 610)
	b.add("holder-bravo", 5, 2320, core.ObservationHeartbeat, 700)

	// Longer lease with increasing late drift. The declared action is lease
	// shortening.
	b.add("holder-charlie", 1, 0, core.ObservationHeartbeat, 55)
	b.add("holder-charlie", 2, 1400, core.ObservationRenewal, 120)
	b.add("holder-charlie", 3, 2950, core.ObservationRenewal, 520)
	b.add("holder-charlie", 4, 4500, core.ObservationRenewal, 260)

	// The second renewal arrives after the lease has expired. This holder has
	// exhausted its renewal budget and declares REVOKE_LEASE.
	b.add("holder-delta", 1, 0, core.ObservationHeartbeat, 75)
	b.add("holder-delta", 2, 560, core.ObservationRenewal, 130)
	b.add("holder-delta", 3, 1400, core.ObservationRenewal, 450)

	return core.Dataset{Policy: b.policy, Holders: b.holders, Observations: b.records}
}

// Snapshot wraps the deterministic seed sample with digest and version fields.
func Snapshot(version int64, seed int64) core.Snapshot {
	dataset := Generate(seed)
	snapshot := core.Snapshot{
		Version:       version,
		GeneratedAtMS: BaseEpochMS + 50_000,
		Dataset:       dataset,
	}
	digest, err := core.ComputeSnapshotDigest(snapshot)
	if err != nil {
		panic(err)
	}
	snapshot.Digest = digest
	return snapshot
}

func (b *builder) add(holderID string, seq int64, offsetMS int64, typ core.ObservationType, arrivalOffsetMS int64) {
	clientTS := BaseEpochMS + offsetMS
	arrivalTS := clientTS + arrivalOffsetMS + int64(b.rng.Intn(17))
	digestInput := fmt.Sprintf("%s|%d|%d|%s", holderID, seq, offsetMS, typ)
	b.records = append(b.records, core.ObservationRecord{
		HolderID: holderID,
		Observation: core.Observation{
			ReportSeq:         seq,
			ClientTimestampMS: clientTS,
			ServerArrivalMS:   arrivalTS,
			Type:              typ,
			Digest:            core.SHA256Digest([]byte(digestInput)),
		},
	})
}

func (b *builder) duplicate(holderID string, seq int64, offsetMS int64, typ core.ObservationType, arrivalOffsetMS int64) {
	clientTS := BaseEpochMS + offsetMS
	arrivalTS := clientTS + arrivalOffsetMS + int64(b.rng.Intn(17))
	digestInput := fmt.Sprintf("%s|%d|%d|%s", holderID, seq, offsetMS, typ)
	b.records = append(b.records, core.ObservationRecord{
		HolderID: holderID,
		Observation: core.Observation{
			ReportSeq:         seq,
			ClientTimestampMS: clientTS,
			ServerArrivalMS:   arrivalTS,
			Type:              typ,
			Digest:            core.SHA256Digest([]byte(digestInput)),
		},
	})
}

// WithConflictingDuplicate returns a dataset whose duplicated report has a
// different digest, for exercising E_REPORT_MISMATCH.
func WithConflictingDuplicate(dataset core.Dataset) core.Dataset {
	copy := cloneDataset(dataset)
	seen := make(map[int64]bool)
	for i, record := range copy.Observations {
		if record.HolderID != "holder-alpha" {
			continue
		}
		if seen[record.ReportSeq] {
			copy.Observations[i].Digest = core.SHA256Digest([]byte("tampered"))
			return copy
		}
		seen[record.ReportSeq] = true
	}
	return copy
}

func cloneDataset(dataset core.Dataset) core.Dataset {
	out := dataset
	out.Holders = append([]core.Holder(nil), dataset.Holders...)
	out.Observations = append([]core.ObservationRecord(nil), dataset.Observations...)
	return out
}
