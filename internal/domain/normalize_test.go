package domain

import "testing"

func validSpec() LoadSpec {
	return LoadSpec{
		Groups: []GroupSpec{
			{ID: "g1", MemoryLimit: "40Gi", ComputeLimit: "1000m"},
		},
		Nodes: []NodeSpec{
			{ID: "n1", GroupID: "g1", MemoryTotal: "40Gi", ComputeTotal: "1000m", Health: "HEALTHY"},
		},
		Requests: []RequestSpec{
			{ID: "r1", ModelID: "m1", GroupID: "g1", MemoryNeed: "4Gi", ComputeNeed: "100m",
				SamplesPerBatch: 16, LatencyLimitMs: 100, Priority: 1},
		},
		Snapshots: []PolicySnapshotSpec{
			{Version: 1, Params: PolicyParams{
				GlobalMaxConcurrent: 2, WaitQueueCapacity: 1, RoundBudgetMs: 100,
				LatencyBaseMs: 5, LatencyPerSampleMs: 1, MaxSamplesPerModel: map[string]int64{"m1": 64},
			}},
		},
	}
}

func TestNormalizeOK(t *testing.T) {
	set, err := Normalize(validSpec())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if set.Requests[0].MemoryNeed != 4*1024*1024*1024 {
		t.Errorf("memory normalization wrong: %d", set.Requests[0].MemoryNeed)
	}
	if set.Requests[0].ComputeNeedM != 100 {
		t.Errorf("compute normalization wrong: %d", set.Requests[0].ComputeNeedM)
	}
	if set.Snapshots[0].Digest == "" {
		t.Error("digest must be computed")
	}
}

func TestNormalizeRejectsBadBatches(t *testing.T) {
	cases := map[string]func(LoadSpec) LoadSpec{
		"duplicate request": func(s LoadSpec) LoadSpec {
			s.Requests = append(s.Requests, s.Requests[0])
			return s
		},
		"unknown group": func(s LoadSpec) LoadSpec {
			s.Requests[0].GroupID = "nope"
			return s
		},
		"bad memory unit": func(s LoadSpec) LoadSpec {
			s.Requests[0].MemoryNeed = "40Xi"
			return s
		},
		"negative compute": func(s LoadSpec) LoadSpec {
			s.Requests[0].ComputeNeed = "-5m"
			return s
		},
		"non monotonic version": func(s LoadSpec) LoadSpec {
			s.Snapshots = append(s.Snapshots, s.Snapshots[0])
			return s
		},
		"group exceeds node capacity": func(s LoadSpec) LoadSpec {
			s.Groups[0].MemoryLimit = "100Gi"
			return s
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Normalize(mutate(validSpec()))
			if err == nil {
				t.Fatalf("expected E_SPEC_INVALID for %s", name)
			}
			be, ok := AsBizError(err)
			if !ok || be.Code != ESpecInvalid {
				t.Fatalf("expected E_SPEC_INVALID got %v", err)
			}
		})
	}
}

func TestDigestChaining(t *testing.T) {
	p := PolicyParams{GlobalMaxConcurrent: 1, WaitQueueCapacity: 0, RoundBudgetMs: 10,
		LatencyBaseMs: 1, LatencyPerSampleMs: 1}
	d1, _ := DigestPolicy(1, p, "")
	d2, _ := DigestPolicy(2, p, d1)
	if d1 == d2 || d1 == "" {
		t.Fatal("digests must be distinct and non-empty")
	}
	// Deterministic across calls.
	d1b, _ := DigestPolicy(1, p, "")
	if d1 != d1b {
		t.Error("digest not deterministic")
	}
}
