package generator

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"profilehotspot/internal/model"
)

// DemoPolicy is deliberately mixed: checkout crosses its exact limit quickly,
// while search remains exact. That makes path selection visible in one version.
func DemoPolicy(version uint64) model.Policy {
	generatedAt := time.Date(2026, 11, 26, 0, 0, 0, 0, time.UTC).Add(time.Duration(version-1) * time.Minute)
	policy := model.Policy{
		Version:     version,
		GeneratedAt: generatedAt,
		Channels: map[string]model.ChannelPolicy{
			"cpu":  {TargetPermille: 250, BiasCorrection: 4.0},
			"wall": {TargetPermille: 1000, BiasCorrection: 1.0},
		},
		Services: map[string]model.ServicePolicy{
			"checkout": {ReservoirCapacity: 8, ExactLimit: 4, BiasCorrection: 1.1},
			"search":   {ReservoirCapacity: 12, ExactLimit: 20, BiasCorrection: 1.0},
		},
		Stratification:           model.Stratification{DepthPrefixes: []int{1, 3}, Separator: ">"},
		DefaultReservoirCapacity: 12,
		DefaultExactLimit:        16,
		RoundTimeBudgetMS:        200,
		RankingRelativeTolerance: 0.005,
		ReplacementRule:          model.ReplacementRule,
	}
	_ = policy.Normalize()
	return policy
}

type ChunkOptions struct {
	Seed           uint64
	BatchID        string
	PolicyVersion  *uint64
	Count          int
	StartIndex     int
	Services       []string
	DuplicateEvery int
}

type splitMix64 struct{ state uint64 }

func (r *splitMix64) next() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func (r *splitMix64) intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(r.next() % uint64(n))
}

func Chunk(policy model.Policy, options ChunkOptions) model.SampleChunk {
	seed := options.Seed
	if seed == 0 {
		seed = 42
	}
	if options.Count <= 0 {
		options.Count = 32
	}
	rng := &splitMix64{state: seed ^ uint64(options.StartIndex+1)*0xd1b54a32d192ed03}
	channels := make([]string, 0, len(policy.Channels))
	for channel := range policy.Channels {
		channels = append(channels, channel)
	}
	sort.Strings(channels)
	services := append([]string(nil), options.Services...)
	if len(services) == 0 {
		for service := range policy.Services {
			services = append(services, service)
		}
	}
	sort.Strings(services)
	if len(channels) == 0 || len(services) == 0 {
		return model.SampleChunk{BatchID: options.BatchID, PolicyVersion: options.PolicyVersion}
	}

	samples := make([]model.SampleInput, 0, options.Count+options.Count/8)
	for i := 0; i < options.Count; i++ {
		service := services[i%len(services)]
		index := options.StartIndex + i
		channel := channels[index%len(channels)]
		frames := stackFor(service, rng.intn(3)+2)
		sample := model.SampleInput{
			ID:          fmt.Sprintf("s-%016x-%06d", seed, index),
			TimestampMS: 1_700_000_000_000 + int64(index)*7 + int64(rng.intn(4)),
			Service:     service,
			Frames:      frames,
			Weight:      int64(1 + rng.intn(20)),
			Channel:     channel,
		}
		samples = append(samples, sample)
		if options.DuplicateEvery > 0 && (i+1)%options.DuplicateEvery == 0 {
			duplicate := sample
			duplicate.Channel = channels[(index+1)%len(channels)]
			samples = append(samples, duplicate)
		}
	}
	return model.SampleChunk{BatchID: options.BatchID, PolicyVersion: options.PolicyVersion, Samples: samples}
}

func stackFor(service string, depth int) []model.Frame {
	stacks := map[string][]model.Frame{
		"checkout": {
			{ID: "runtime.goexit", Name: "runtime.goexit"},
			{ID: "http.server", Name: "http.server", File: "server.go", Line: 41},
			{ID: "checkout.handler", Name: "checkout.Handler", File: "checkout.go", Line: 77},
			{ID: "pricing.calculate", Name: "pricing.Calculate", File: "pricing.go", Line: 112},
			{ID: "db.query", Name: "db.Query", File: "db.go", Line: 233},
			{ID: "json.encode", Name: "json.Encode", File: "json.go", Line: 18},
		},
		"search": {
			{ID: "runtime.goexit", Name: "runtime.goexit"},
			{ID: "http.server", Name: "http.server", File: "server.go", Line: 41},
			{ID: "search.handler", Name: "search.Handler", File: "search.go", Line: 63},
			{ID: "index.lookup", Name: "index.Lookup", File: "index.go", Line: 154},
			{ID: "cache.get", Name: "cache.Get", File: "cache.go", Line: 29},
			{ID: "sort.sort", Name: "sort.Sort", File: "sort.go", Line: 90},
		},
	}
	base, ok := stacks[service]
	if !ok {
		base = []model.Frame{
			{ID: "runtime.goexit", Name: "runtime.goexit"},
			{ID: service + ".root", Name: service + ".root"},
			{ID: service + ".work", Name: service + ".work"},
			{ID: service + ".finish", Name: service + ".finish"},
		}
	}
	if depth < 1 {
		depth = 1
	}
	if depth > len(base) {
		depth = len(base)
	}
	return append([]model.Frame(nil), base[:depth]...)
}

func MustVersion(value uint64) *uint64 { return &value }

func Slug(value string) string {
	return strings.ReplaceAll(strings.ToLower(value), " ", "-")
}
