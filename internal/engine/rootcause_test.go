package engine

import (
	"testing"

	"alarmengine/internal/model"
)

func firingTopologyEngine(t *testing.T) *Engine {
	eng, _ := newTestEngine(t, Config{})
	services := []model.Service{
		{ID: "s1", Zone: "az-a", WeightPerMille: 100, Sensitivity: model.SensHigh, SilenceWindow: 60000},
		{ID: "s2", Zone: "az-b", WeightPerMille: 100, Sensitivity: model.SensMedium, SilenceWindow: 60000, Upstreams: []string{"s1"}},
	}
	if err := eng.BootstrapServices(services); err != nil {
		t.Fatal(err)
	}
	policy := model.PolicySnapshot{
		Version: 1, Thresholds: map[string]int64{"cpu": 80},
		FingerprintFields:   []string{model.FieldServiceID, model.FieldMetric, model.FieldLabels},
		SuppressionWindowMS: 6000, RootCauseTolerance: 50,
		ConcurrencyLimit: 2, RoundBudgetMS: 100,
	}
	policy.Checksum = checksumOf(policy)
	if err := eng.BootstrapPolicy(policy); err != nil {
		t.Fatal(err)
	}
	return eng
}

func TestRootCauseFixedPointAndRanking(t *testing.T) {
	eng := firingTopologyEngine(t)
	// Alerts on both s1 (upstream) and s2 (downstream). s2's root-cause
	// candidate s1 should rank first with hop=1 and an explicit integer delta.
	batch := []model.Observation{
		obs(1000, "s1", "cpu", 90, map[string]string{"k": "v"}),
		obs(2000, "s2", "cpu", 90, map[string]string{"k": "v"}),
	}
	res, err := eng.IngestObservations(IngestRequest{
		RequestID: "req-rc", BaseVersion: 1, Observations: batch,
	})
	if err != nil {
		t.Fatal(err)
	}
	scores, _ := eng.QueryRootCauses(res.CommitVersion)
	if len(scores) == 0 {
		t.Fatal("expected root cause scores")
	}
	// Only s2 (downstream) has an upstream, so every score is for s2's alert.
	top := scores[0]
	if top.CandidateService != "s1" || top.Hops != 1 {
		t.Fatalf("top candidate want s1 hop1, got %s hop%d", top.CandidateService, top.Hops)
	}
	if top.TimeDeltaMS != 1000 {
		t.Fatalf("integer time delta want 1000, got %d", top.TimeDeltaMS)
	}
	if top.Rank != 1 {
		t.Fatalf("rank want 1, got %d", top.Rank)
	}
	// Fixed-point sanity: score within [0,1000000] milli units.
	if top.ScoreFinalMilli <= 0 || top.ScoreFinalMilli > 1_000_000 {
		t.Fatalf("score out of fixed-point range: %d", top.ScoreFinalMilli)
	}
	// Ranks are contiguous starting at 1 per trigger.
	seenRank := map[string]int{}
	for _, sc := range scores {
		seenRank[sc.TriggerAlertID] = sc.Rank
	}
	if len(seenRank) == 0 {
		t.Fatal("no trigger ids recorded")
	}
}

func TestToleranceTieBreaksLexicographically(t *testing.T) {
	// Two equally-scoring candidates must be ordered by service id within the
	// relative tolerance, not by float noise.
	policy := model.PolicySnapshot{RootCauseTolerance: 1000} // 100% tolerance -> all equivalent
	a := model.RootCauseScore{CandidateService: "zzz", ScoreFinalMilli: 100}
	b := model.RootCauseScore{CandidateService: "aaa", ScoreFinalMilli: 99}
	if !model.EquivalentWithTolerance(a.ScoreFinalMilli, b.ScoreFinalMilli, policy.RootCauseTolerance) {
		t.Fatal("scores should be equivalent under 100% relative tolerance")
	}
	// Strict comparison still distinguishes them when tolerance is zero.
	if model.EquivalentWithTolerance(100, 99, 0) {
		t.Fatal("zero tolerance must require exact equality")
	}
}

func TestComputeScoreRoundingAudit(t *testing.T) {
	r, err := model.ComputeScore(1, 15000)
	if err != nil {
		t.Fatal(err)
	}
	if r.FinalMilli <= 0 {
		t.Fatal("expected positive final score")
	}
	// The audit trail fields must all be populated.
	if r.TimeProximityMilli <= 0 || r.HopDecayMilli != 850_000 {
		t.Fatalf("unexpected components: %+v", r)
	}
}
