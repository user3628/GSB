package engine

import (
	"testing"

	"alarmengine/internal/errcode"
	"alarmengine/internal/model"
)

// buildTopologyEngine creates a small topology s2 -> s1 across two zones so
// suppression rules by service/zone/hop are fully exercised.
func buildTopologyEngine(t *testing.T) (*Engine, model.PolicySnapshot) {
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
		SuppressionRules: []model.SuppressionRule{
			{ID: "r-svc", Service: "s1", Zone: "", Hop: 1},
			{ID: "r-zone", Service: "", Zone: "az-a", Hop: 1},
		},
	}
	policy.Checksum = checksumOf(policy)
	if err := eng.BootstrapPolicy(policy); err != nil {
		t.Fatal(err)
	}
	return eng, policy
}

func TestSuppressionMultiRulePrimaryAndHits(t *testing.T) {
	eng, _ := buildTopologyEngine(t)
	// Alert on s2 (az-b), whose upstream s1 is 1 hop away in az-a. It must hit
	// both rules; declaration order makes r-svc the primary reason.
	batch := []model.Observation{obs(1000, "s2", "cpu", 90, map[string]string{"k": "v"})}
	res, err := eng.IngestObservations(IngestRequest{
		RequestID: "req-sup", BaseVersion: 1, Observations: batch,
	})
	if err != nil {
		t.Fatal(err)
	}
	alerts, _ := eng.QueryAlerts(res.CommitVersion)
	if len(alerts) != 1 || alerts[0].State != model.StateSuppressed {
		t.Fatalf("want one suppressed alert, got %+v", alerts)
	}
	a := alerts[0]
	if a.PrimaryReason != "r-svc" {
		t.Fatalf("primary reason want r-svc (declaration order), got %s", a.PrimaryReason)
	}
	if len(a.SuppressedBy) != 2 {
		t.Fatalf("want all 2 rule hits retained, got %v", a.SuppressedBy)
	}
	if a.ReleaseAtMS != a.WindowEndMS+1 {
		t.Fatalf("release must be window end +1: end=%d release=%d", a.WindowEndMS, a.ReleaseAtMS)
	}
	hits, _ := eng.QuerySuppressionHits(res.CommitVersion)
	if len(hits) != 2 {
		t.Fatalf("want 2 durable suppression hits, got %d", len(hits))
	}
}

func TestStateTransitions(t *testing.T) {
	eng, _ := buildTopologyEngine(t)
	res, err := eng.IngestObservations(IngestRequest{
		RequestID: "req-st", BaseVersion: 1,
		Observations: []model.Observation{obs(1000, "s2", "cpu", 90, map[string]string{"k": "v"})},
	})
	if err != nil {
		t.Fatal(err)
	}
	alerts, _ := eng.QueryAlerts(res.CommitVersion)
	id := alerts[0].ID
	// SUPPRESSED -> FIRING legal.
	if err := eng.TransitionAlert(id, model.StateFiring); err != nil {
		t.Fatalf("legal transition rejected: %v", err)
	}
	// FIRING -> RESOLVED legal.
	if err := eng.TransitionAlert(id, model.StateResolved); err != nil {
		t.Fatalf("legal transition rejected: %v", err)
	}
	// RESOLVED -> FIRING illegal and must not mutate persisted state.
	err = eng.TransitionAlert(id, model.StateFiring)
	if errcode.CodeOf(err) != errcode.EInvalidTransition {
		t.Fatalf("want E_INVALID_TRANSITION, got %v", err)
	}
	after, _ := eng.QueryAlerts(res.CommitVersion)
	if after[0].State != model.StateResolved {
		t.Fatalf("persisted state mutated by illegal transition: %s", after[0].State)
	}
}

func TestTransitionMatrix(t *testing.T) {
	cases := []struct {
		from, to model.AlertState
		legal    bool
	}{
		{model.StateCandidate, model.StateFiring, true},
		{model.StateCandidate, model.StateSuppressed, true},
		{model.StateCandidate, model.StateResolved, true},
		{model.StateFiring, model.StateSuppressed, true},
		{model.StateFiring, model.StateResolved, true},
		{model.StateSuppressed, model.StateFiring, true},
		{model.StateSuppressed, model.StateResolved, true},
		{model.StateResolved, model.StateFiring, false},
		{model.StateFiring, model.StateCandidate, false},
	}
	for _, c := range cases {
		if got := model.CanTransition(c.from, c.to); got != c.legal {
			t.Fatalf("CanTransition(%s,%s)=%v want %v", c.from, c.to, got, c.legal)
		}
	}
}
