package core_test

import (
	"encoding/json"
	"math/rand"
	"testing"

	"leasearbiter/internal/core"
	"leasearbiter/internal/sample"
)

func TestValidateDatasetRejectsInvalidRecords(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*core.Dataset)
	}{
		{
			name: "non-positive lease duration",
			mutate: func(dataset *core.Dataset) {
				dataset.Holders[0].LeaseDurationMS = 0
			},
		},
		{
			name: "bad digest",
			mutate: func(dataset *core.Dataset) {
				dataset.Observations[0].Digest = "sha256:NOT_HEX"
			},
		},
		{
			name: "sequence decreases in capture order",
			mutate: func(dataset *core.Dataset) {
				dataset.Observations[2].ReportSeq = 1
			},
		},
		{
			name: "unknown observation type",
			mutate: func(dataset *core.Dataset) {
				dataset.Observations[0].Type = "GRANT"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dataset := sample.Generate(sample.DefaultSeed)
			test.mutate(&dataset)
			err := core.ValidateDataset(dataset)
			if err == nil || err.Code != core.CodeObservationInvalid {
				t.Fatalf("expected %s, got %#v", core.CodeObservationInvalid, err)
			}
		})
	}
	if err := core.ValidateDataset(sample.Generate(sample.DefaultSeed)); err != nil {
		t.Fatalf("generated sample must validate: %v", err)
	}
}

func TestDedupeCountsIdempotentDuplicateAndRetainsFirst(t *testing.T) {
	dataset := sample.Generate(sample.DefaultSeed)
	stored, err := core.DeduplicateDataset(dataset)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, record := range stored {
		if record.HolderID == "holder-alpha" && record.ReportSeq == 2 {
			found = true
			if record.DuplicateCount != 1 {
				t.Fatalf("duplicate count = %d, want 1", record.DuplicateCount)
			}
		}
	}
	if !found {
		t.Fatal("holder-alpha seq 2 was not retained")
	}

	conflicting := sample.WithConflictingDuplicate(dataset)
	firstDigest := ""
	for _, record := range stored {
		if record.HolderID == "holder-alpha" && record.ReportSeq == 2 {
			firstDigest = record.Digest
		}
	}
	merged, mismatch := core.MergeStoredObservations(stored, conflicting.Observations)
	if mismatch == nil || mismatch.Code != core.CodeReportMismatch {
		t.Fatalf("expected %s, got %#v", core.CodeReportMismatch, mismatch)
	}
	for _, record := range merged {
		if record.HolderID == "holder-alpha" && record.ReportSeq == 2 && record.Digest != firstDigest {
			t.Fatal("first digest must be retained on mismatch")
		}
	}
}

func TestRebuildIsIndependentOfDeliveryOrder(t *testing.T) {
	dataset := sample.Generate(sample.DefaultSeed)
	stored, err := core.DeduplicateDataset(dataset)
	if err != nil {
		t.Fatal(err)
	}
	first := core.RebuildAll(dataset.Holders, stored, dataset.Policy)

	shuffled := append([]core.StoredObservation(nil), stored...)
	random := rand.New(rand.NewSource(99))
	random.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	second := core.RebuildAll(dataset.Holders, shuffled, dataset.Policy)
	firstJSON, _ := json.Marshal(first)
	secondJSON, _ := json.Marshal(second)
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("rebuild differs by delivery order\nfirst:  %s\nsecond: %s", firstJSON, secondJSON)
	}
}

func TestDedupedRebuildIsIndependentOfRawBatchOrder(t *testing.T) {
	dataset := sample.Generate(sample.DefaultSeed)
	original, err := core.DeduplicateDataset(dataset)
	if err != nil {
		t.Fatal(err)
	}
	first := core.RebuildAll(dataset.Holders, original, dataset.Policy)
	shuffledDataset := dataset
	shuffledDataset.Observations = append([]core.ObservationRecord(nil), dataset.Observations...)
	random := rand.New(rand.NewSource(123))
	random.Shuffle(len(shuffledDataset.Observations), func(i, j int) {
		shuffledDataset.Observations[i], shuffledDataset.Observations[j] = shuffledDataset.Observations[j], shuffledDataset.Observations[i]
	})
	deduped, mismatch := core.DeduplicateDataset(shuffledDataset)
	if mismatch != nil {
		t.Fatal(mismatch)
	}
	second := core.RebuildAll(shuffledDataset.Holders, deduped, shuffledDataset.Policy)
	firstJSON, _ := json.Marshal(first)
	secondJSON, _ := json.Marshal(second)
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("raw batch order changed rebuild\nfirst:  %s\nsecond: %s", firstJSON, secondJSON)
	}
}

func TestRebuildTieBreakAndSafePointBoundary(t *testing.T) {
	holder := core.Holder{ID: "h", LeaseDurationMS: 1000, MaxRenewals: 8, Status: core.HolderActive, HealStrategy: core.HealAuto}
	policy := core.DefaultPolicy()
	observations := []core.StoredObservation{
		{ObservationRecord: core.ObservationRecord{HolderID: "h", Observation: core.Observation{ReportSeq: 3, ClientTimestampMS: 1700, ServerArrivalMS: 2100, Type: core.ObservationRenewal, Digest: core.SHA256Digest([]byte("3"))}}},
		{ObservationRecord: core.ObservationRecord{HolderID: "h", Observation: core.Observation{ReportSeq: 1, ClientTimestampMS: 1000, ServerArrivalMS: 3000, Type: core.ObservationHeartbeat, Digest: core.SHA256Digest([]byte("1"))}}},
		{ObservationRecord: core.ObservationRecord{HolderID: "h", Observation: core.Observation{ReportSeq: 2, ClientTimestampMS: 1700, ServerArrivalMS: 2000, Type: core.ObservationRenewal, Digest: core.SHA256Digest([]byte("2"))}}},
	}
	result := core.RebuildHolder(holder, observations, policy)
	if got := []int64{result.Rebuilt[0].ReportSeq, result.Rebuilt[1].ReportSeq, result.Rebuilt[2].ReportSeq}; got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("tie-break order = %v, want [1 2 3]", got)
	}
	// Seq 2 elapsed 700ms and sits exactly on the safe point. Seq 3 has the
	// same client timestamp and follows the seq-2 renewal, so its elapsed time
	// from that newly applied anchor is zero (EARLY, not LATE).
	if result.Rebuilt[1].Timing != core.TimingOnTime || result.Rebuilt[2].Timing != core.TimingEarly {
		t.Fatalf("safe-point boundary timings = %s/%s, want ON_TIME/EARLY", result.Rebuilt[1].Timing, result.Rebuilt[2].Timing)
	}
	if result.Drift.Drifted {
		t.Fatalf("within-tolerance records must not drift: %+v", result.Drift)
	}
}

func TestSampleDriftResultsAndHealActions(t *testing.T) {
	dataset := sample.Generate(sample.DefaultSeed)
	stored, err := core.DeduplicateDataset(dataset)
	if err != nil {
		t.Fatal(err)
	}
	results := core.RebuildAll(dataset.Holders, stored, dataset.Policy)
	byID := make(map[string]core.RebuildResult)
	holders := make(map[string]core.Holder)
	for _, holder := range dataset.Holders {
		holders[holder.ID] = holder
	}
	for _, result := range results {
		byID[result.HolderID] = result
	}
	if byID["holder-alpha"].Drift.Drifted {
		t.Fatalf("healthy holder drifted: %+v", byID["holder-alpha"].Drift)
	}
	if byID["holder-alpha"].DuplicateDeliveryCount != 1 {
		t.Fatalf("alpha duplicate count = %d, want 1", byID["holder-alpha"].DuplicateDeliveryCount)
	}
	tests := []struct {
		holderID  string
		magnitude int64
		action    core.HealAction
	}{
		{holderID: "holder-bravo", magnitude: 30, action: core.HealActionRequireEarlyRenew},
		{holderID: "holder-charlie", magnitude: 100, action: core.HealActionShortenLease},
		{holderID: "holder-delta", magnitude: 230, action: core.HealActionRevokeLease},
	}
	for _, test := range tests {
		result := byID[test.holderID]
		if !result.Drift.Drifted {
			t.Errorf("%s did not drift", test.holderID)
			continue
		}
		if result.Drift.MagnitudeMS != test.magnitude {
			t.Errorf("%s magnitude = %d, want %d", test.holderID, result.Drift.MagnitudeMS, test.magnitude)
		}
		decision := core.DecideHeal(holders[test.holderID], result, 1_800_000_000_000)
		if decision == nil || decision.Action != test.action {
			t.Errorf("%s action = %#v, want %s", test.holderID, decision, test.action)
		}
		if decision != nil && decision.ExpectedRecoveryAtMS < decision.DecidedAtMS {
			t.Errorf("%s recovery time precedes decision", test.holderID)
		}
	}
}

func TestAutoHealIsDeterministic(t *testing.T) {
	holder := core.Holder{ID: "auto", LeaseDurationMS: 1000, MaxRenewals: 4, Status: core.HolderActive, HealStrategy: core.HealAuto}
	tests := []struct {
		name   string
		result core.RebuildResult
		want   core.HealAction
	}{
		{
			name:   "small drift asks for early renewal",
			result: core.RebuildResult{Drift: core.DriftDecision{Drifted: true, MagnitudeMS: 30}, LeaseValidAtLast: true},
			want:   core.HealActionRequireEarlyRenew,
		},
		{
			name:   "medium drift shortens lease",
			result: core.RebuildResult{Drift: core.DriftDecision{Drifted: true, MagnitudeMS: 300}, LeaseValidAtLast: true},
			want:   core.HealActionShortenLease,
		},
		{
			name:   "expired lease revokes",
			result: core.RebuildResult{Drift: core.DriftDecision{Drifted: true, MagnitudeMS: 30}, LeaseValidAtLast: false},
			want:   core.HealActionRevokeLease,
		},
		{
			name:   "exhausted renewals revoke",
			result: core.RebuildResult{Drift: core.DriftDecision{Drifted: true, MagnitudeMS: 30}, LeaseValidAtLast: true, RenewalsExhausted: true},
			want:   core.HealActionRevokeLease,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision := core.DecideHeal(holder, test.result, 1000)
			if decision == nil || decision.Action != test.want {
				t.Fatalf("decision = %#v, want %s", decision, test.want)
			}
		})
	}
}
