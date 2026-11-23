package model

import "testing"

func TestStateMachineTransitions(t *testing.T) {
	legal := [][2]Phase{
		{PhaseUnknown, PhaseHealthy}, {PhaseUnknown, PhaseUnhealthy}, {PhaseUnknown, PhaseDraining},
		{PhaseHealthy, PhaseSuspect}, {PhaseHealthy, PhaseDraining},
		{PhaseSuspect, PhaseHealthy}, {PhaseSuspect, PhaseUnhealthy}, {PhaseSuspect, PhaseDraining},
		{PhaseUnhealthy, PhaseHealthy}, {PhaseUnhealthy, PhaseSuspect}, {PhaseUnhealthy, PhaseDraining},
		{PhaseDraining, PhaseUnknown},
	}
	for _, pair := range legal {
		if err := AssertTransition(pair[0], pair[1]); err != nil {
			t.Fatalf("expected legal %s->%s, got %v", pair[0], pair[1], err)
		}
	}
	illegal := [][2]Phase{
		{PhaseUnknown, PhaseSuspect}, {PhaseHealthy, PhaseUnhealthy},
		{PhaseDraining, PhaseHealthy}, {PhaseDraining, PhaseDraining},
		{PhaseHealthy, PhaseHealthy},
	}
	for _, pair := range illegal {
		err := AssertTransition(pair[0], pair[1])
		if err == nil {
			t.Fatalf("expected E_INVALID_TRANSITION for %s->%s", pair[0], pair[1])
		}
		biz, ok := err.(*BizError)
		if !ok || biz.Code != CodeInvalidTransition {
			t.Fatalf("expected business code E_INVALID_TRANSITION, got %v", err)
		}
	}
}

func TestCounterThresholds(t *testing.T) {
	c := &StateCounter{Phase: PhaseHealthy, FailureThreshold: 3, SuccessThreshold: 2}
	for i := 0; i < 2; i++ {
		target, changed := c.ObserveFailure()
		if changed || target != PhaseHealthy {
			t.Fatalf("failure streak %d should stay HEALTHY", i+1)
		}
	}
	target, changed := c.ObserveFailure()
	if !changed || target != PhaseSuspect {
		t.Fatalf("third failure must move HEALTHY->SUSPECT, got %s changed=%v", target, changed)
	}
	c.FailureStreak = 0
	for i := 0; i < c.FailureThreshold-1; i++ {
		if target, changed := c.ObserveFailure(); changed || target != PhaseSuspect {
			t.Fatalf("suspect streak %d should remain SUSPECT", i+1)
		}
	}
	if _, changed := c.ObserveFailure(); !changed || c.Phase != PhaseUnhealthy {
		t.Fatalf("suspect failure reaching threshold must move to UNHEALTHY")
	}
	if _, changed := c.ObserveSuccess(); changed {
		t.Fatalf("first success in UNHEALTHY must not cross threshold yet")
	}
	if _, changed := c.ObserveSuccess(); !changed || c.Phase != PhaseHealthy {
		t.Fatalf("second success must restore HEALTHY")
	}
}

func TestDeterministicHashStable(t *testing.T) {
	first := DeterministicHash("client-alpha", 7)
	second := DeterministicHash("client-alpha", 7)
	if first != second || first == 0 {
		t.Fatalf("hash must be deterministic and non-zero")
	}
	if DeterministicHash("client-alpha", 8) == first {
		t.Fatalf("different request sequence must produce different hash")
	}
}

func TestNormalizeSumsTo1000AndExcludesUnavailable(t *testing.T) {
	instances := []Instance{
		{ID: "a1", Weight: 600, Zone: "z-a"},
		{ID: "a2", Weight: 400, Zone: "z-a"},
		{ID: "b1", Weight: 999, Zone: "z-b"},
		{ID: "b2", Weight: 1, Zone: "z-b"},
	}
	phases := map[string]Phase{
		"a1": PhaseHealthy, "a2": PhaseUnhealthy,
		"b1": PhaseDraining, "b2": PhaseSuspect,
	}
	weights := NormalizeZoneWeights(instances, phases)
	if _, ok := weights["z-a"]["a2"]; ok {
		t.Fatalf("UNHEALTHY instance must be removed from allocation")
	}
	if _, ok := weights["z-b"]["b1"]; ok {
		t.Fatalf("DRAINING instance must be removed from allocation")
	}
	if weights["z-a"]["a1"] != 1000 {
		t.Fatalf("single healthy instance must receive full 1000, got %d", weights["z-a"]["a1"])
	}
	sum := 0
	for _, w := range weights["z-b"] {
		sum += w
	}
	if sum != 1000 || weights["z-b"]["b2"] != 1000 {
		t.Fatalf("zone b normalization mismatch: %+v", weights["z-b"])
	}
}

func TestChooseInstanceDeterministicAndFallback(t *testing.T) {
	// 只在 z-b 提供可调度实例：任何首选 z-a 的请求都必须确定性回退。
	snap := &SnapshotVersion{
		Version:    1,
		ZoneRatios: map[string]int{"z-a": 1000, "z-b": 0},
		ZoneWeights: map[string]map[string]int{
			"z-b": {"b1": 1000},
		},
	}
	d1, err := ChooseInstance(snap, "client-x", 3)
	if err != nil {
		t.Fatal(err)
	}
	d2, err := ChooseInstance(snap, "client-x", 3)
	if err != nil {
		t.Fatal(err)
	}
	if d1.SelectedInstance != "b1" || !d1.Fallback || d1.FallbackReason == "" ||
		d1.PreferredZone != "z-a" || d1.SelectedZone != "z-b" {
		t.Fatalf("expected fallback z-a->z-b to b1, got %+v", d1)
	}
	if d1.HashValue != d2.HashValue || d1.SelectedInstance != d2.SelectedInstance {
		t.Fatalf("same snapshot+request must select the same instance")
	}
}

func TestSampleValidationBusinessCode(t *testing.T) {
	good := &Sample{
		Instances: []Instance{{
			ID: "i1", Port: 8080, Weight: 500, Zone: "z-a",
			Probe: ProbeSpec{Mode: "http", Path: "/h", ExpectedStatus: 200,
				BodyMarker: "OK", TimeoutMillis: 100, SuccessThreshold: 1, FailureThreshold: 1},
		}},
		Trace: []RequestRecord{{
			TimestampMillis: 1, ClientID: "c1", RequestWeight: 1, RequestBytes: 10,
		}},
		Affinity: ZoneAffinity{Ratios: map[string]int{"z-a": 1000}},
	}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}

	dup := *good
	dup.Instances = append(dup.Instances, good.Instances[0])
	err := dup.Validate()
	if biz := mustBiz(t, err); biz.Code != CodeInstanceInvalid {
		t.Fatalf("expected E_INSTANCE_INVALID for duplicate id, got %v", err)
	}

	badPort := *good
	badPort.Instances[0].Port = 70000
	if biz := mustBiz(t, badPort.Validate()); biz.Code != CodeInstanceInvalid {
		t.Fatal("port validation failed")
	}

	badClient := *good
	badClient.Trace = append(badClient.Trace, good.Trace[0])
	if biz := mustBiz(t, badClient.Validate()); biz.Code != CodeInstanceInvalid {
		t.Fatal("duplicate client validation failed")
	}

	badRatio := *good
	badRatio.Affinity = ZoneAffinity{Ratios: map[string]int{"z-a": 999}}
	if biz := mustBiz(t, badRatio.Validate()); biz.Code != CodeInstanceInvalid {
		t.Fatal("affinity ratio validation failed")
	}
}

func mustBiz(t *testing.T, err error) *BizError {
	t.Helper()
	if err == nil {
		t.Fatal("expected validation error")
	}
	biz, ok := err.(*BizError)
	if !ok {
		t.Fatalf("expected BizError, got %T", err)
	}
	return biz
}
