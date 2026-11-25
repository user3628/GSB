package engine

import (
	"testing"

	"alarmengine/internal/model"
)

func TestFingerprintDeterministic(t *testing.T) {
	labels := map[string]string{"b": "2", "a": "1"}
	fp1, parts1, err := model.Fingerprint("svc", "cpu", labels,
		[]string{model.FieldServiceID, model.FieldMetric, model.FieldLabels})
	if err != nil {
		t.Fatal(err)
	}
	fp2, _, _ := model.Fingerprint("svc", "cpu", map[string]string{"a": "1", "b": "2"},
		[]string{model.FieldServiceID, model.FieldMetric, model.FieldLabels})
	if fp1 != fp2 {
		t.Fatalf("fingerprint not order-independent: %s vs %s", fp1, fp2)
	}
	if len(parts1) != 3 {
		t.Fatalf("want 3 canonical parts, got %d", len(parts1))
	}
	fp3, _, _ := model.Fingerprint("svc", "cpu", labels, []string{model.FieldServiceID, model.FieldMetric})
	if fp3 == fp1 {
		t.Fatal("fingerprint must change when label field removed")
	}
}

// TestWindowBoundaryInclusive uses a custom service whose silence window is
// larger than the policy suppression window so the effective window is exactly
// the policy value 6000ms. The inclusive rule is [W, W+D]: t==W+D merges,
// t==W+D+1 opens a new entry.
func TestWindowBoundaryInclusive(t *testing.T) {
	eng, _ := newTestEngine(t, Config{})
	services := []model.Service{
		{ID: "s1", Zone: "az-a", WeightPerMille: 100, Sensitivity: model.SensHigh, SilenceWindow: 60000},
	}
	if err := eng.BootstrapServices(services); err != nil {
		t.Fatal(err)
	}
	policy := model.PolicySnapshot{
		Version:             1,
		Thresholds:          map[string]int64{"cpu": 80},
		FingerprintFields:   []string{model.FieldServiceID, model.FieldMetric, model.FieldLabels},
		SuppressionWindowMS: 6000, RootCauseTolerance: 50,
		ConcurrencyLimit: 2, RoundBudgetMS: 100, SuppressionRules: nil,
	}
	policy.Checksum = checksumOf(policy)
	if err := eng.BootstrapPolicy(policy); err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{"k": "v"}
	mk := func(ts int64) model.Observation { return obs(ts, "s1", "cpu", 90, labels) }
	batch := []model.Observation{
		mk(1000),  // group1 W=1000 end=7000
		mk(7000),  // inclusive boundary -> merge
		mk(7001),  // outside -> group2
		mk(13001), // group2 W=7001 end=13001 inclusive -> merge
		mk(13002), // outside -> group3
	}
	res, err := eng.IngestObservations(IngestRequest{
		RequestID: "req-window", BaseVersion: 1, Observations: batch,
	})
	if err != nil {
		t.Fatal(err)
	}
	alerts, _ := eng.QueryAlerts(res.CommitVersion)
	if len(alerts) != 3 {
		t.Fatalf("want 3 window groups, got %d: %+v", len(alerts), alerts)
	}
	if alerts[0].MergedCount != 2 || alerts[0].WindowEndMS != 7000 {
		t.Fatalf("group1 inclusive merge failed: %+v", alerts[0])
	}
	if alerts[1].MergedCount != 2 || alerts[1].WindowStartMS != 7001 {
		t.Fatalf("group2 inclusive merge failed: %+v", alerts[1])
	}
	if alerts[2].MergedCount != 1 {
		t.Fatalf("group3 must be a fresh single alert: %+v", alerts[2])
	}
}

func TestDedupWindowIsMin(t *testing.T) {
	policy := model.PolicySnapshot{SuppressionWindowMS: 6000}
	short := model.Service{SilenceWindow: 4000}
	long := model.Service{SilenceWindow: 9000}
	if got := dedupWindow(policy, short); got != 4000 {
		t.Fatalf("want min window 4000, got %d", got)
	}
	if got := dedupWindow(policy, long); got != 6000 {
		t.Fatalf("want policy window 6000, got %d", got)
	}
}
