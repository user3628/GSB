package seed

import (
	"fmt"
	"math/rand"

	"elastic-scale/internal/model"
)

const FixedSeed int64 = 20261125
const BaseTimeMS int64 = 1764000000000

type Bundle struct {
	Seed             int64                  `json:"seed"`
	Catalog          model.Catalog          `json:"catalog"`
	Snapshot         model.Snapshot         `json:"snapshot"`
	SingleIncrement  model.IncrementRequest `json:"single_increment"`
	GlobalIncrement  model.IncrementRequest `json:"global_increment"`
	CrashIncrement   model.IncrementRequest `json:"crash_increment"`
	SparseSnapshot   model.Snapshot         `json:"sparse_snapshot"`
	StaleReadVersion int64                  `json:"stale_read_version"`
}

type serviceProfile struct {
	id        string
	component string
	group     string
	deps      []string
	target    int64
	budget    int64
	minR      int
	maxR      int
	baseReq   int64
	baseLat   int64
}

func Generate() *Bundle {
	r := rand.New(rand.NewSource(FixedSeed))
	profiles := []serviceProfile{
		{id: "edge-gateway", component: "envoy", group: "sg-edge", deps: []string{"auth-svc", "catalog-svc"}, target: 12, budget: 220, minR: 2, maxR: 10, baseReq: 120, baseLat: 95},
		{id: "auth-svc", component: "auth-api", group: "sg-auth", deps: []string{"session-svc"}, target: 6, budget: 140, minR: 1, maxR: 8, baseReq: 95, baseLat: 70},
		{id: "session-svc", component: "session-store-api", group: "sg-session", target: 4, budget: 90, minR: 1, maxR: 6, baseReq: 70, baseLat: 42},
		{id: "catalog-svc", component: "catalog-api", group: "sg-catalog", deps: []string{"inventory-svc"}, target: 8, budget: 170, minR: 1, maxR: 8, baseReq: 105, baseLat: 82},
		{id: "inventory-svc", component: "inventory-api", group: "sg-inventory", target: 3, budget: 210, minR: 1, maxR: 6, baseReq: 55, baseLat: 120},
	}
	catalog := model.Catalog{}
	samples := []model.Sample{}
	for si, p := range profiles {
		subs := []model.Subscription{
			{Metric: "http.requests", Weight: 3},
			{Metric: "http.latency_ms", Weight: 2},
		}
		catalog.Services = append(catalog.Services, model.Service{
			ID: p.id, Component: p.component, ScalingGroup: p.group, Dependencies: append([]string(nil), p.deps...),
			Subscriptions: subs,
			Policy: model.Policy{
				WindowMS: 60_000, TargetConcurrency: p.target, LatencyBudgetMS: p.budget,
				MinReplicas: p.minR, MaxReplicas: p.maxR, MinLatencySamples: 8, QuantilePercent: 90,
				SaturationWeightMicro: 600_000, LatencyRiskWeightMicro: 400_000,
				IncrementalThresholdMicro: 250_000, PropagationThresholdMicro: 750_000,
			},
		})
		for ii := 0; ii < 2; ii++ {
			instanceID := fmt.Sprintf("%s-%d", p.id, ii)
			for point := 0; point < 6; point++ {
				value := p.baseReq + int64(si*13) + int64(ii*17) + int64(point*4) + int64(r.Intn(45))
				samples = append(samples, model.Sample{
					ServiceID: p.id, InstanceID: instanceID, Metric: "http.requests",
					SampledAtMS: BaseTimeMS + int64(point*10_000), Value: value,
					CollectionWeight: int64(1 + r.Intn(3)),
				})
			}
			for point := 0; point < 12; point++ {
				spike := int64(0)
				if point == 10 || point == 11 {
					spike = int64(60 + r.Intn(90))
				}
				value := p.baseLat + int64(si*9) + int64(ii*11) + int64(r.Intn(45)) + spike
				samples = append(samples, model.Sample{
					ServiceID: p.id, InstanceID: instanceID, Metric: "http.latency_ms",
					SampledAtMS: BaseTimeMS + int64(point*5_000), Value: value,
					CollectionWeight: int64(1 + r.Intn(3)),
				})
			}
		}
	}
	snapshot := model.Snapshot{Version: 1, GeneratedAtMS: BaseTimeMS + 60_000, Samples: samples}
	digest, err := model.DigestSnapshot(snapshot)
	if err != nil {
		panic(err)
	}
	snapshot.Digest = digest

	single := model.IncrementRequest{CommitID: "inc-single-inventory", BaseVersion: 1, Mode: "auto"}
	for i := range samples {
		if samples[i].ServiceID == "inventory-svc" && samples[i].InstanceID == "inventory-svc-0" && samples[i].Metric == "http.requests" && samples[i].SampledAtMS == BaseTimeMS {
			changed := samples[i]
			changed.Value = 424
			changed.CollectionWeight = 3
			single.SampleChanges = []model.SampleChange{{Op: "upsert", Sample: &changed}}
			break
		}
	}

	global := model.IncrementRequest{CommitID: "inc-global-pressure", BaseVersion: 2, Mode: "auto"}
	for _, sample := range samples {
		changed := sample
		if changed.Metric == "http.requests" {
			changed.Value += 35
		} else {
			changed.Value += 25
		}
		global.SampleChanges = append(global.SampleChanges, model.SampleChange{Op: "upsert", Sample: &changed})
	}

	crash := model.IncrementRequest{CommitID: "inc-crash-global", BaseVersion: 3, Mode: "auto", CrashAfterIntent: true}
	for _, sample := range samples {
		changed := sample
		changed.Value += 50
		crash.SampleChanges = append(crash.SampleChanges, model.SampleChange{Op: "upsert", Sample: &changed})
	}

	sparseSamples := []model.Sample{}
	for _, sample := range samples {
		if sample.ServiceID != "auth-svc" {
			continue
		}
		if sample.Metric == "http.latency_ms" && sample.InstanceID == "auth-svc-0" {
			if sample.SampledAtMS > BaseTimeMS+10_000 {
				continue
			}
		}
		sparseSamples = append(sparseSamples, sample)
	}
	sparse := model.Snapshot{Version: 2, GeneratedAtMS: BaseTimeMS + 120_000, Samples: sparseSamples}
	sparseDigest, _ := model.DigestSnapshot(sparse)
	sparse.Digest = sparseDigest

	return &Bundle{
		Seed: FixedSeed, Catalog: catalog, Snapshot: snapshot,
		SingleIncrement: single, GlobalIncrement: global, CrashIncrement: crash,
		SparseSnapshot: sparse, StaleReadVersion: 9_999,
	}
}
