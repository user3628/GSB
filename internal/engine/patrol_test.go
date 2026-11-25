package engine

import (
	"context"
	"testing"

	"alarmengine/internal/errcode"
	"alarmengine/internal/model"
)

func TestPatrolBudgetDegradationDeterministic(t *testing.T) {
	eng, _ := newTestEngine(t, Config{})
	services := []model.Service{
		{ID: "low-1", Zone: "az-c", WeightPerMille: 1000, Sensitivity: model.SensLow, SilenceWindow: 1000},
		{ID: "high-1", Zone: "az-b", WeightPerMille: 1000, Sensitivity: model.SensHigh, SilenceWindow: 1000},
		{ID: "high-2", Zone: "az-a", WeightPerMille: 1000, Sensitivity: model.SensHigh, SilenceWindow: 1000},
	}
	if err := eng.BootstrapServices(services); err != nil {
		t.Fatal(err)
	}
	policy := model.PolicySnapshot{
		Version: 1, Thresholds: map[string]int64{"cpu": 80},
		FingerprintFields: []string{model.FieldServiceID}, SuppressionWindowMS: 1000,
		RootCauseTolerance: 50, ConcurrencyLimit: 2, RoundBudgetMS: 21, // each costs 10ms
	}
	policy.Checksum = checksumOf(policy)
	if err := eng.BootstrapPolicy(policy); err != nil {
		t.Fatal(err)
	}
	trace, err := eng.PatrolRound(context.Background(), "r1")
	if err != nil {
		t.Fatal(err)
	}
	// Deterministic order keeps HIGH sensitivity first; the LOW task is degraded.
	if len(trace.Degraded) != 1 || trace.Degraded[0] != "low-1" {
		t.Fatalf("degradation order wrong: %+v", trace.Degraded)
	}
	if trace.RemainingBudget != 1 {
		t.Fatalf("remaining budget want 1, got %d", trace.RemainingBudget)
	}
	if len(trace.Completed) != 2 {
		t.Fatalf("want 2 completed, got %v", trace.Completed)
	}
}

func TestPatrolQueueFull(t *testing.T) {
	eng, _ := newTestEngine(t, Config{PatrolQueueCapacity: 1})
	services := []model.Service{
		{ID: "s1", Zone: "az-a", WeightPerMille: 100, Sensitivity: model.SensHigh, SilenceWindow: 1000},
	}
	if err := eng.BootstrapServices(services); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// A zero-capacity bounded queue must reject admission explicitly rather
	// than silently skipping the task.
	eng2, _ := newTestEngine(t, Config{})
	eng2.cfg.PatrolQueueCapacity = 0 // explicitly saturated/disabled queue
	if err := eng2.BootstrapServices(services); err != nil {
		t.Fatal(err)
	}
	policy := model.PolicySnapshot{
		Version: 1, Thresholds: map[string]int64{"cpu": 80},
		FingerprintFields: []string{model.FieldServiceID}, SuppressionWindowMS: 1000,
		RootCauseTolerance: 50, ConcurrencyLimit: 1, RoundBudgetMS: 1000,
	}
	policy.Checksum = checksumOf(policy)
	if err := eng2.BootstrapPolicy(policy); err != nil {
		t.Fatal(err)
	}
	_, err := eng2.PatrolRound(ctx, "r")
	if errcode.CodeOf(err) != errcode.EPatrolQueueFull {
		t.Fatalf("want E_PATROL_QUEUE_FULL, got %v", err)
	}
}

func TestConcurrentReads(t *testing.T) {
	eng, data := bootstrappedEngine(t)
	res := ingestSeed(t, eng, data)
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			if _, err := eng.QueryAlerts(res.CommitVersion); err != nil {
				t.Errorf("concurrent read failed: %v", err)
			}
			if _, err := eng.QueryRootCauses(res.CommitVersion); err != nil {
				t.Errorf("concurrent read failed: %v", err)
			}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
}
