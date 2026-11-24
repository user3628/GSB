package domain

import (
	"reflect"
	"testing"

	"replicationlag/internal/apperr"
)

const (
	testSourceOne = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	testSourceTwo = "6f9d1a7e-8f5b-4cf4-a4a7-3bdfbc3f7d21"
)

func TestParseGTIDNormalizesAdjacentOverlappingAndMultipleSources(t *testing.T) {
	set, err := ParseGTID(testSourceOne + ":1-3,4-5:" + testSourceTwo + ":10,8-9,11")
	if err == nil {
		t.Fatal("invalid mixed source syntax unexpectedly accepted")
	}
	set, err = ParseGTID(testSourceOne + ":1-3,4-5," + testSourceTwo + ":10,8-9,11")
	if err != nil {
		t.Fatalf("parse GTID: %v", err)
	}
	want := testSourceOne + ":1-5," + testSourceTwo + ":8-11"
	if got := set.String(); got != want {
		t.Fatalf("canonical GTID = %q, want %q", got, want)
	}
	if set.Cardinality() != 9 {
		t.Fatalf("cardinality = %d, want 9", set.Cardinality())
	}
	if !set.IsSuperset(mustGTID(t, testSourceOne+":2-4")) {
		t.Fatal("expected superset relation")
	}
}

func TestParseGTIDRejectsInvalidRangesAndStrictLegacySyntax(t *testing.T) {
	for _, raw := range []string{"", testSourceOne + ":0-1", testSourceOne + ":5-4", "not-a-source:1", testSourceOne + ":1~2"} {
		if _, err := ParseGTID(raw); err == nil {
			t.Fatalf("ParseGTID(%q) unexpectedly succeeded", raw)
		}
	}
	if _, err := ParseGTIDWithOptions(testSourceOne+":1~5", GTIDParseOptions{AllowTildeRanges: true, AllowColonRangeList: true}); err != nil {
		t.Fatalf("explicit compatibility syntax should parse: %v", err)
	}
	legacy, err := ParseGTIDWithOptions("1-5", GTIDParseOptions{AllowIntegerShorthand: true, DefaultSource: testSourceOne})
	if err != nil {
		t.Fatalf("integer shorthand: %v", err)
	}
	explicit := mustGTID(t, testSourceOne+":1-5")
	if !legacy.Equal(explicit) {
		t.Fatalf("shorthand %s != explicit %s", legacy.String(), explicit.String())
	}
}

func TestDifferenceAndIntersection(t *testing.T) {
	left := mustGTID(t, testSourceOne+":1-5,8-10,"+testSourceTwo+":1-3")
	right := mustGTID(t, testSourceOne+":3-8,"+testSourceTwo+":2-4")
	diff := left.Difference(right)
	wantDiff := testSourceOne + ":1-2:9-10," + testSourceTwo + ":1"
	if diff.String() != wantDiff {
		t.Fatalf("difference = %q, want %q", diff.String(), wantDiff)
	}
	intersection := left.Intersect(right)
	wantIntersection := testSourceOne + ":3-5:8," + testSourceTwo + ":2-3"
	if intersection.String() != wantIntersection {
		t.Fatalf("intersection = %q, want %q", intersection.String(), wantIntersection)
	}
}

func TestBuildSequencesSortsOutOfOrderAndDerivesEvolution(t *testing.T) {
	observations := []Observation{
		obs(t, "replica-x", 5000, testSourceOne+":1-9", 20),
		obs(t, "replica-x", 1000, testSourceOne+":1-5", 10),
		obs(t, "replica-x", 3000, testSourceOne+":1-8", 10),
		obs(t, "replica-x", 4000, testSourceOne+":1-6", 10),
		obs(t, "replica-x", 2000, testSourceOne+":1-8", 10),
	}
	report, err := BuildSequences(observations, Policy{LagWarnMS: 100, LagCriticalMS: 500, StallThreshold: 2, RecoveryObservations: 2})
	if err != nil {
		t.Fatalf("build sequences: %v", err)
	}
	if len(report.Sequences) != 1 {
		t.Fatalf("sequences = %d", len(report.Sequences))
	}
	steps := report.Sequences[0].Steps
	gotTimes := []int64{steps[0].Observation.SampledAtMS, steps[1].Observation.SampledAtMS, steps[2].Observation.SampledAtMS, steps[3].Observation.SampledAtMS, steps[4].Observation.SampledAtMS}
	if !reflect.DeepEqual(gotTimes, []int64{1000, 2000, 3000, 4000, 5000}) {
		t.Fatalf("ordered times = %v", gotTimes)
	}
	if steps[1].PositionRelation != PositionAdvance || steps[1].AddedIntervals[0].Intervals[0] != (Interval{Start: 6, End: 8}) {
		t.Fatalf("unexpected advance step: %+v", steps[1])
	}
	if steps[2].PositionRelation != PositionStall || steps[2].StallCount != 1 {
		t.Fatalf("unexpected stall step: %+v", steps[2])
	}
	if steps[3].PositionRelation != PositionRegressed || steps[3].RevokedIntervals[0].Intervals[0] != (Interval{Start: 7, End: 8}) {
		t.Fatalf("unexpected regression/revoked intervals: %+v", steps[3])
	}
	if steps[3].State != StateStalled {
		t.Fatalf("regression state = %s, want STALLED", steps[3].State)
	}
	if steps[4].PositionRelation != PositionAdvance || steps[4].State != StateRecovering {
		t.Fatalf("unexpected post-regression recovery step: %+v", steps[4])
	}
}

func TestSameSampleTimeConflict(t *testing.T) {
	observations := []Observation{
		obs(t, "replica-x", 1000, testSourceOne+":1-5", 10),
		obs(t, "replica-x", 1000, testSourceOne+":1-6", 10),
	}
	_, err := BuildSequences(observations, DefaultPolicy())
	appErr := apperr.As(err)
	if appErr.Code != apperr.CodeObservationConflict {
		t.Fatalf("code = %s, want %s", appErr.Code, apperr.CodeObservationConflict)
	}
	if appErr.Context["replica_id"] != "replica-x" || appErr.Context["sampled_at_ms"] != int64(1000) {
		t.Fatalf("conflict context = %#v", appErr.Context)
	}
}

func TestCrossedGap(t *testing.T) {
	observations := []Observation{
		obs(t, "replica-x", 1000, testSourceOne+":1-5", 10),
		obs(t, "replica-x", 2000, testSourceOne+":1-5:7", 10),
	}
	report, err := BuildSequences(observations, DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if !report.Sequences[0].Steps[1].Crossed {
		t.Fatal("expected advance that skips transaction 6 to be marked crossed")
	}
}

func TestStateMachineStallAndRecovery(t *testing.T) {
	policy := Policy{LagWarnMS: 100, LagCriticalMS: 500, StallThreshold: 2, RecoveryObservations: 2}
	observations := []Observation{
		obs(t, "replica-x", 1000, testSourceOne+":1-5", 10),
		obs(t, "replica-x", 2000, testSourceOne+":1-5", 10),
		obs(t, "replica-x", 3000, testSourceOne+":1-5", 10),
		obs(t, "replica-x", 4000, testSourceOne+":1-5", 10),
		obs(t, "replica-x", 5000, testSourceOne+":1-6", 10),
		obs(t, "replica-x", 6000, testSourceOne+":1-7", 10),
	}
	report, err := BuildSequences(observations, policy)
	if err != nil {
		t.Fatalf("build sequences: %v", err)
	}
	steps := report.Sequences[0].Steps
	wantStates := []ReplicaState{StateHealthy, StateHealthy, StateHealthy, StateStalled, StateRecovering, StateHealthy}
	for i, want := range wantStates {
		if steps[i].State != want {
			t.Fatalf("step %d state = %s, want %s (basis=%v)", i+1, steps[i].State, want, steps[i].TransitionBasis)
		}
	}
	if steps[4].RecoveryStreak != 1 || steps[5].RecoveryStreak != 2 {
		t.Fatalf("recovery streaks = %d,%d", steps[4].RecoveryStreak, steps[5].RecoveryStreak)
	}
	if steps[3].SuggestedAction != "INVESTIGATE_REPLICATION_BREAK_AND_ALERT" {
		t.Fatalf("stalled action = %s", steps[3].SuggestedAction)
	}
}

func TestStalledRemainsWhenLagRises(t *testing.T) {
	policy := Policy{LagWarnMS: 100, LagCriticalMS: 500, StallThreshold: 1, RecoveryObservations: 2}
	observations := []Observation{
		obs(t, "replica-x", 1000, testSourceOne+":1-5", 10),
		obs(t, "replica-x", 2000, testSourceOne+":1-5", 10),
		obs(t, "replica-x", 3000, testSourceOne+":1-5", 900),
	}
	report, err := BuildSequences(observations, policy)
	if err != nil {
		t.Fatalf("stalled lag rise should not cause illegal transition: %v", err)
	}
	last := report.Sequences[0].Steps[2]
	if last.State != StateStalled || last.LagLevel != LagCritical {
		t.Fatalf("last step = state %s lag %s", last.State, last.LagLevel)
	}
}

func TestValidateTransition(t *testing.T) {
	if err := ValidateTransition(StateHealthy, StateLagWarn); err != nil {
		t.Fatal(err)
	}
	err := ValidateTransition(StateHealthy, StateRecovering)
	appErr := apperr.As(err)
	if appErr.Code != apperr.CodeInvalidTransition {
		t.Fatalf("code = %s", appErr.Code)
	}
	if appErr.Context["from"] != StateHealthy || appErr.Context["to"] != StateRecovering {
		t.Fatalf("context = %#v", appErr.Context)
	}
}

func TestObservationValidationContext(t *testing.T) {
	observation := obs(t, "replica-x", 1000, testSourceOne+":1-5", 10)
	observation.LagMS = -1
	appErr := apperr.As(observation.Validate())
	if appErr.Code != apperr.CodeObservationInvalid {
		t.Fatalf("code = %s", appErr.Code)
	}
	for _, key := range []string{"replica_id", "sampled_at_ms", "field"} {
		if _, ok := appErr.Context[key]; !ok {
			t.Fatalf("missing context %s in %#v", key, appErr.Context)
		}
	}
	if appErr.Context["field"] != "lag_ms" {
		t.Fatalf("field = %v", appErr.Context["field"])
	}
}

func obs(t *testing.T, replica string, sampledAt int64, gtid string, lag int64) Observation {
	t.Helper()
	set := mustGTID(t, gtid)
	version, err := ParseVersion("8.0.36")
	if err != nil {
		t.Fatal(err)
	}
	return Observation{
		ReplicaID: replica, SampledAtMS: sampledAt, GTID: set, PositionName: set.String(),
		LagMS: lag, ReportedVersion: version, FormatVersion: 1, LastWriteMS: sampledAt,
	}
}

func mustGTID(t *testing.T, raw string) GTIDSet {
	t.Helper()
	set, err := ParseGTID(raw)
	if err != nil {
		t.Fatalf("parse GTID %q: %v", raw, err)
	}
	return set
}
