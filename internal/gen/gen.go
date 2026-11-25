// Package gen produces deterministic sample data from a fixed seed so every
// run on every machine yields byte-identical files.
package gen

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"gpusched/internal/domain"
)

// DefaultSeed is the fixed seed used by both the CLI and the HTTP sample load.
const DefaultSeed int64 = 20261126

// rng is a deterministic xorshift64* generator (no global math/rand state).
type rng struct{ state uint64 }

func newRNG(seed int64) *rng {
	r := &rng{state: uint64(seed)}
	if r.state == 0 {
		r.state = 0x9E3779B97F4A7C15
	}
	// Warm up to decorrelate the first draw from the raw seed.
	for i := 0; i < 16; i++ {
		r.next()
	}
	return r
}

func (r *rng) next() uint64 {
	x := r.state
	x ^= x >> 12
	x ^= x << 25
	x ^= x >> 27
	r.state = x
	return x * 0x2545F4914F6CDD1D
}

// intn returns a deterministic value in [0,n).
func (r *rng) intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(r.next() % uint64(n))
}

func (r *rng) pick(options []string) string {
	return options[r.intn(len(options))]
}

// Generate builds the full deterministic sample set.
func Generate(seed int64) domain.LoadSpec {
	r := newRNG(seed)

	groups := []domain.GroupSpec{
		{ID: "g-ranking", MemoryLimit: "96Gi", ComputeLimit: "1800m"},
		{ID: "g-vision", MemoryLimit: "64Gi", ComputeLimit: "1200m"},
	}

	// Two nodes per group with fixed, reproducible capacities.
	nodes := []domain.NodeSpec{
		{ID: "node-a1", GroupID: "g-ranking", MemoryTotal: "48Gi", ComputeTotal: "900m", Health: "HEALTHY"},
		{ID: "node-a2", GroupID: "g-ranking", MemoryTotal: "48Gi", ComputeTotal: "900m", Health: "HEALTHY"},
		{ID: "node-b1", GroupID: "g-vision", MemoryTotal: "32Gi", ComputeTotal: "600m", Health: "HEALTHY"},
		{ID: "node-b2", GroupID: "g-vision", MemoryTotal: "32Gi", ComputeTotal: "600m", Health: "DEGRADED"},
	}

	models := map[string][]string{
		"g-ranking": {"rank-large", "rank-small"},
		"g-vision":  {"vision-x", "vision-y"},
	}
	memChoices := []string{"4Gi", "6Gi", "8Gi", "10Gi", "12Gi"}
	computeChoices := []string{"120m", "180m", "240m", "300m"}

	var requests []domain.RequestSpec
	seq := 0
	for gi, g := range groups {
		modelSet := models[g.ID]
		// Deterministic request count per group (8 ranking, 7 vision).
		count := 8
		if gi == 1 {
			count = 7
		}
		for k := 0; k < count; k++ {
			seq++
			priority := r.intn(3) // 0 high, 1 normal, 2 low
			req := domain.RequestSpec{
				ID:              fmt.Sprintf("req-%03d", seq),
				ModelID:         r.pick(modelSet),
				GroupID:         g.ID,
				MemoryNeed:      r.pick(memChoices),
				ComputeNeed:     r.pick(computeChoices),
				SamplesPerBatch: int64(8 + r.intn(25)),
				LatencyLimitMs:  int64(40 + r.intn(120)),
				Priority:        priority,
			}
			requests = append(requests, req)
		}
	}

	// Monotonic policy snapshots with chained digests.
	snap1Params := domain.PolicyParams{
		GlobalMaxConcurrent: 3,
		WaitQueueCapacity:   2,
		RoundBudgetMs:       250,
		LatencyBaseMs:       5,
		LatencyPerSampleMs:  1.5,
		MaxSamplesPerModel: map[string]int64{
			"rank-large": 48, "rank-small": 64,
			"vision-x": 32, "vision-y": 40,
		},
	}
	digest1, _ := domain.DigestPolicy(1, snap1Params, "")
	snap1 := domain.PolicySnapshotSpec{Version: 1, Params: snap1Params, PrevDigest: ""}

	snap2Params := snap1Params
	snap2Params.GlobalMaxConcurrent = 4
	snap2Params.RoundBudgetMs = 400
	snap2 := domain.PolicySnapshotSpec{Version: 2, Params: snap2Params, PrevDigest: digest1}

	// Incremental fragment carrying only the changed fields relative to v2.
	fragment := domain.FragmentSpec{
		Version:    2,
		BaseDigest: digest1,
		Fields: map[string]any{
			"global_max_concurrent_batches": 4,
			"round_budget_ms":               400,
		},
	}

	return domain.LoadSpec{
		Groups:    groups,
		Nodes:     nodes,
		Requests:  requests,
		Snapshots: []domain.PolicySnapshotSpec{snap1, snap2},
		Fragments: []domain.FragmentSpec{fragment},
	}
}

// WriteFile atomically writes the generated spec as pretty JSON to dir.
func WriteFile(dir string, seed int64) (string, error) {
	spec := Generate(seed)
	data, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, "sample.json")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", err
	}
	return path, nil
}
