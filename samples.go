package convergence

import "time"

func sampleTime() time.Time { return time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC) }

func D(value string) Decimal {
	decimal, err := ParseDecimal(value)
	if err != nil {
		panic(err)
	}
	return decimal
}

func field(value any, source string) Field { return Field{Value: value, Source: source} }

func SamplePolicy() Policy {
	return Policy{
		PolicyVersion:      1,
		GeneratedAt:        sampleTime(),
		DriftTolerance:     D("0.01"),
		DriftPriority:      append([]string(nil), DefaultDriftPriority...),
		MaxBatchSize:       2,
		MaxBatchUnits:      3,
		RoundBudget:        Duration(30 * time.Second),
		WriteTimeout:       Duration(2 * time.Second),
		RetryLimit:         1,
		MaxParallelBatches: 2,
		WriteQueueSize:     2,
	}
}

func SampleSnapshotV1() *Snapshot {
	policy := SamplePolicy()
	snapshot := &Snapshot{
		SchemaVersion:   "multicluster.convergence/v1",
		SnapshotVersion: 1,
		GeneratedAt:     sampleTime(),
		Policy:          policy,
		Clusters: []Cluster{
			{ID: "cn-east", Endpoint: "http://127.0.0.1:18080", Region: "east"},
			{ID: "cn-north", Endpoint: "http://127.0.0.1:18081", Region: "north"},
		},
		Objects: []Object{
			{
				ClusterID: "cn-east", ID: "alpha-quota", Kind: "ResourceQuota", Namespace: "platform",
				Expected: FieldSet{
					"cpu_limit":    field(4, "git:platform"),
					"memory_limit": field("8Gi", "git:platform"),
					"max_pods":     field(20, "git:platform"),
				},
				Actual: FieldSet{
					"cpu_limit":    field(4, "git:platform"),
					"memory_limit": field("8Gi", "git:platform"),
					"max_pods":     field(20, "git:platform"),
				},
				Budget: ResourceBudget{BatchUnits: 1, CPUMillis: 500, MemoryBytes: 1 << 20},
			},
			{
				ClusterID: "cn-east", ID: "bravo-registry", Kind: "StatefulSet", Namespace: "delivery",
				Expected: FieldSet{
					"image":    field("registry:2.8", "git:delivery"),
					"replicas": field(2, "git:delivery"),
					"storage":  field("100Gi", "git:delivery"),
				},
				Actual: FieldSet{
					"image":   field("registry:2.8", "git:delivery"),
					"storage": field("100Gi", "git:delivery"),
				},
				Budget: ResourceBudget{BatchUnits: 1, CPUMillis: 1000, MemoryBytes: 2 << 30},
			},
			{
				ClusterID: "cn-east", ID: "charlie-network", Kind: "NetworkPolicy", Namespace: "delivery",
				Expected: FieldSet{
					"cidr": field("10.42.0.0/16", "git:network"),
					"mtu":  field(1500, "git:network"),
				},
				Actual: FieldSet{
					"cidr":        field("10.42.0.0/16", "git:network"),
					"mtu":         field(1500, "git:network"),
					"legacy_rule": field(true, "live:controller"),
				},
				Budget: ResourceBudget{BatchUnits: 1},
			},
			{
				ClusterID: "cn-east", ID: "delta-policy", Kind: "ConfigMap", Namespace: "delivery",
				Expected: FieldSet{
					"replicas":  field(3, "git:delivery"),
					"cpu_cores": field(D("2.00"), "git:delivery"),
					"log_level": field("info", "git:delivery"),
				},
				Actual: FieldSet{
					"replicas":  field(5, "git:delivery"),
					"cpu_cores": field(D("2.01"), "git:delivery"),
					"log_level": field("debug", "git:delivery"),
				},
				Budget: ResourceBudget{BatchUnits: 1},
			},
			{
				ClusterID: "cn-east", ID: "echo-dns", Kind: "Deployment", Namespace: "kube-system",
				Expected: FieldSet{
					"replicas":       field(2, "git:dns"),
					"cluster_domain": field("cluster.local", "git:dns"),
				},
				Actual: FieldSet{
					"replicas":       field(2, "live:manual-hotfix"),
					"cluster_domain": field("cluster.local", "git:dns"),
				},
				DependsOn: []string{"alpha-quota"},
				Budget:    ResourceBudget{BatchUnits: 2, CPUMillis: 800},
			},
			{
				ClusterID: "cn-east", ID: "foxtrot-cache", Kind: "StatefulSet", Namespace: "middleware",
				Expected: FieldSet{
					"replicas":           field(3, "git:cache"),
					"max_memory":         field("1Gi", "git:cache"),
					"eviction_policy":    field("allkeys-lru", "git:cache"),
					"tls_enabled":        field(true, "git:cache"),
					"maintenance_window": field("02:00", "git:sre"),
				},
				Actual: FieldSet{
					"replicas":        field(4, "hpa:autoscaler"),
					"max_memory":      field("1Gi", "git:cache"),
					"eviction_policy": field("noeviction", "live:manual"),
					"tls_enabled":     field(true, "git:cache"),
					"legacy_snapshot": field("on", "live:controller"),
				},
				DependsOn: []string{"bravo-registry"},
				Budget:    ResourceBudget{BatchUnits: 2, MemoryBytes: 1 << 30},
			},
			{
				ClusterID: "cn-north", ID: "golf-api", Kind: "Deployment", Namespace: "apps",
				Expected: FieldSet{
					"image":    field("api:v1.4.0", "git:apps"),
					"replicas": field(4, "git:apps"),
					"port":     field(8080, "git:apps"),
				},
				Actual: FieldSet{
					"image":    field("api:v1.3.9", "git:apps"),
					"replicas": field(4, "git:apps"),
					"port":     field(8080, "git:apps"),
				},
				DependsOn: []string{"echo-dns", "foxtrot-cache"},
				Budget:    ResourceBudget{BatchUnits: 2, CPUMillis: 2000},
			},
			{
				ClusterID: "cn-north", ID: "hotel-worker", Kind: "Deployment", Namespace: "apps",
				Expected: FieldSet{
					"replicas": field(2, "git:apps"),
					"queue":    field("high-priority", "git:apps"),
				},
				Actual: FieldSet{
					"replicas": field(2, "git:apps"),
					"queue":    field("high-priority", "git:apps"),
				},
				DependsOn: []string{"foxtrot-cache"},
				Budget:    ResourceBudget{BatchUnits: 1},
			},
			{
				ClusterID: "cn-north", ID: "india-frontend", Kind: "Deployment", Namespace: "web",
				Expected: FieldSet{
					"replicas":    field(3, "git:web"),
					"cdn_enabled": field(true, "git:web"),
				},
				Actual: FieldSet{
					"replicas":    field(2, "git:web"),
					"cdn_enabled": field(true, "git:web"),
				},
				DependsOn: []string{"golf-api"},
				Budget:    ResourceBudget{BatchUnits: 1},
			},
			{
				ClusterID: "cn-north", ID: "juliet-metrics", Kind: "Deployment", Namespace: "observability",
				Expected: FieldSet{
					"scrape_interval": field("30s", "git:observability"),
					"retention_days":  field(14, "git:observability"),
				},
				Actual: FieldSet{
					"scrape_interval": field("30s", "git:observability"),
					"retention_days":  field(14, "git:observability"),
					"debug_pprof":     field(true, "live:sre-debug"),
				},
				DependsOn: []string{"golf-api", "hotel-worker"},
				Budget:    ResourceBudget{BatchUnits: 1},
			},
		},
	}
	_ = PrepareSnapshot(snapshot)
	return snapshot
}

func SampleDeltaV2() *Delta {
	api := Object{
		ClusterID: "cn-north", ID: "golf-api", Kind: "Deployment", Namespace: "apps",
		Expected: FieldSet{
			"image":    field("api:v1.5.0", "git:apps"),
			"replicas": field(5, "git:apps"),
			"port":     field(8080, "git:apps"),
		},
		Actual: FieldSet{
			"image":    field("api:v1.4.0", "git:apps"),
			"replicas": field(4, "git:apps"),
			"port":     field(8080, "git:apps"),
		},
		DependsOn: []string{"echo-dns", "foxtrot-cache", "kilo-sidecar"},
		Budget:    ResourceBudget{BatchUnits: 2, CPUMillis: 2200},
	}
	sidecar := Object{
		ClusterID: "cn-north", ID: "kilo-sidecar", Kind: "Sidecar", Namespace: "apps",
		Expected: FieldSet{
			"image": field("proxy:v2.1", "git:mesh"),
			"mode":  field("strict", "git:mesh"),
		},
		Actual: FieldSet{
			"image": field("proxy:v2.0", "git:mesh"),
			"mode":  field("permissive", "git:mesh"),
		},
		Budget: ResourceBudget{BatchUnits: 1, CPUMillis: 250},
	}
	return &Delta{
		BaseVersion: 1,
		Version:     2,
		GeneratedAt: sampleTime().Add(2 * time.Hour),
		Reason:      "roll API v1.5 and add deterministic sidecar; graph changes force batch rearrangement",
		Upserts:     []Object{api, sidecar},
	}
}

func SampleCycleSnapshot() *Snapshot {
	snapshot := &Snapshot{
		SchemaVersion:   "multicluster.convergence/v1",
		SnapshotVersion: 1,
		GeneratedAt:     sampleTime(),
		Policy:          SamplePolicy(),
		Clusters:        []Cluster{{ID: "cn-cycle", Endpoint: "http://127.0.0.1:18080"}},
		Objects: []Object{
			cycleObject("alpha", []string{"charlie"}),
			cycleObject("bravo", []string{"alpha"}),
			cycleObject("charlie", []string{"bravo"}),
			cycleObject("outside", nil),
			cycleObject("downstream", []string{"alpha"}),
		},
	}
	_ = PrepareSnapshot(snapshot)
	return snapshot
}

func cycleObject(id string, deps []string) Object {
	return Object{
		ClusterID: "cn-cycle", ID: id, Kind: "ConfigMap", Namespace: "cycle",
		Expected:  FieldSet{"value": field("desired", "git")},
		Actual:    FieldSet{"value": field("actual", "live")},
		DependsOn: deps,
		Budget:    ResourceBudget{BatchUnits: 1},
	}
}
