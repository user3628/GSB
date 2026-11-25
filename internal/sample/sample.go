// Package sample produces fully deterministic in-process sample data from a
// fixed seed. No network access or external dataset is involved.
package sample

import (
	"fmt"

	"alarmengine/internal/model"
)

// FixedSeed is the documented seed used by every acceptance run.
const FixedSeed uint64 = 0xA17E_5EED_0001

// Metrics exercised by the sample generator.
var Metrics = []string{"cpu_pct", "mem_pct", "err_rate"}

// lcg is a deterministic linear-congruential generator (Numerical Recipes
// constants). It is intentionally simple and platform independent.
type lcg struct {
	state uint64
}

func newLCG(seed uint64) *lcg { return &lcg{state: seed} }

func (g *lcg) next() uint64 {
	g.state = g.state*6364136223846793005 + 1442695040888963407
	return g.state
}

// intn returns a deterministic value in [0,n).
func (g *lcg) intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(g.next() % uint64(n))
}

// Dataset bundles everything an acceptance run needs.
type Dataset struct {
	Services     []model.Service
	Observations []model.Observation
	Policy       model.PolicySnapshot
}

// Generate builds the fixed-seed dataset.
func Generate() Dataset {
	g := newLCG(FixedSeed)

	zoneCycle := []string{"az-a", "az-b", "az-c"}
	sensCycle := []model.Sensitivity{model.SensHigh, model.SensMedium, model.SensLow}
	services := make([]model.Service, 0, 8)
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("svc-%02d", i+1)
		var ups []string
		if i > 0 {
			// Each service depends on the previous one; a few get a second edge.
			ups = append(ups, fmt.Sprintf("svc-%02d", i))
			if i >= 3 && i%2 == 1 {
				ups = append(ups, fmt.Sprintf("svc-%02d", i-2))
			}
		}
		svc := model.Service{
			ID:             id,
			Zone:           zoneCycle[i%3],
			WeightPerMille: int64(50 + g.intn(200)),
			Upstreams:      ups,
			Sensitivity:    sensCycle[i%3],
			// Deterministic silence window in {1000,1500,2000,...}.
			SilenceWindow: int64(1000 * (1 + g.intn(4))),
		}
		services = append(services, svc)
	}

	// Deterministic observations: 10 per service over a 60s timeline. A
	// fraction of values deliberately exceed thresholds to create candidate
	// alerts, some of which land inside the suppression window to exercise
	// merging.
	observations := make([]model.Observation, 0, len(services)*10)
	baseTS := int64(1_700_000_000_000)
	thresholds := map[string]int64{
		"cpu_pct":  80,
		"mem_pct":  85,
		"err_rate": 50,
	}
	for si, svc := range services {
		for k := 0; k < 10; k++ {
			metric := Metrics[(si+k)%len(Metrics)]
			ts := baseTS + int64(k)*5000 + int64(si)*137
			// Roughly 40% of observations are over threshold, clustered so
			// some fall within the suppression window.
			value := int64(20 + g.intn(40))
			if g.intn(100) < 40 {
				value = thresholds[metric] + int64(1+g.intn(20))
			}
			obs := model.Observation{
				TimestampMS: ts,
				ServiceID:   svc.ID,
				Metric:      metric,
				Value:       value,
				Labels: map[string]string{
					"tier":     []string{"edge", "core", "data"}[g.intn(3)],
					"instance": fmt.Sprintf("inst-%d", g.intn(4)),
				},
			}
			observations = append(observations, obs)
		}
	}

	policy := model.PolicySnapshot{
		Version:             1,
		Thresholds:          thresholds,
		FingerprintFields:   []string{model.FieldServiceID, model.FieldMetric, model.FieldLabels},
		SuppressionWindowMS: 6000,
		RootCauseTolerance:  50,
		ConcurrencyLimit:    3,
		RoundBudgetMS:       25,
		SuppressionRules: []model.SuppressionRule{
			{ID: "rule-zone-hop1", Service: "", Zone: "az-a", Hop: 1},
			{ID: "rule-svc-hop1", Service: "svc-04", Zone: "", Hop: 1},
			{ID: "rule-zone-any", Service: "", Zone: "az-b", Hop: 0},
		},
	}
	policy.Checksum = ComputeChecksum(policy)
	return Dataset{Services: services, Observations: observations, Policy: policy}
}
