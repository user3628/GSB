package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"releasegate/domain"
	"releasegate/service"
)

const baseTimestamp int64 = 1795461800000

type generator struct{ state uint64 }

func (g *generator) next() uint64 {
	g.state += 0x9e3779b97f4a7c15
	z := g.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func (g *generator) jitter(n int64) int64 {
	if n <= 0 {
		return 0
	}
	return int64(g.next() % uint64(n))
}

type stagePlan struct {
	Stage           int
	BatchID         string
	CanarySuccesses int
	StableSuccesses int
	CanaryLatency   int64
	StableLatency   int64
	StartTimestamp  int64
}

func main() {
	outDir := flag.String("out", "testdata", "output directory")
	seed := flag.Int64("seed", 20261124, "fixed deterministic seed")
	releaseID := flag.String("release-id", "demo-progressive-release", "release identifier")
	flag.Parse()

	gate := domain.GateThresholds{
		MaxAbsSuccessDropPPM: 60000, MaxRelativeSuccessDropPPM: 100000,
		MaxLatencyDeltaUS: 2000, MaxCIHalfWidthPPM: 80000, RelativeTolerancePPM: 1000,
	}
	def := domain.ReleaseDefinition{
		ReleaseID: *releaseID, QuantileMethod: "linear", ConfidenceLevelPPM: 9500,
		Stages: []domain.StageDefinition{
			{Number: 1, TargetTrafficPerMille: 100, MinObservationSamples: 100, LatencyQuantilePerMille: 950, Gate: gate},
			{Number: 2, TargetTrafficPerMille: 300, MinObservationSamples: 100, LatencyQuantilePerMille: 950, Gate: gate},
			{Number: 3, TargetTrafficPerMille: 1000, MinObservationSamples: 100, LatencyQuantilePerMille: 950, Gate: gate},
		},
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		panic(err)
	}
	request := service.CreateReleaseRequest{ReleaseID: def.ReleaseID, Definition: def}
	if err := writeJSON(filepath.Join(*outDir, "release.json"), request); err != nil {
		panic(err)
	}

	plans := []stagePlan{
		{Stage: 1, BatchID: "fixture-stage-1", CanarySuccesses: 194, StableSuccesses: 190, CanaryLatency: 1800, StableLatency: 1900, StartTimestamp: baseTimestamp},
		{Stage: 2, BatchID: "fixture-stage-2", CanarySuccesses: 155, StableSuccesses: 190, CanaryLatency: 2300, StableLatency: 1900, StartTimestamp: baseTimestamp + 60000},
		{Stage: 3, BatchID: "fixture-stage-3", CanarySuccesses: 195, StableSuccesses: 192, CanaryLatency: 1750, StableLatency: 1800, StartTimestamp: baseTimestamp + 120000},
	}
	var files []string
	for _, plan := range plans {
		g := &generator{state: uint64(*seed) + uint64(plan.Stage*1009)}
		batch := makeBatch(def.ReleaseID, plan, g)
		name := fmt.Sprintf("observations-stage-%d.json", plan.Stage)
		path := filepath.Join(*outDir, name)
		if err := writeJSON(path, batch); err != nil {
			panic(err)
		}
		files = append(files, path)
	}
	files = append(files, filepath.Join(*outDir, "release.json"))
	manifest := map[string]string{}
	sort.Strings(files)
	for _, p := range files {
		digest, err := fileSHA256(p)
		if err != nil {
			panic(err)
		}
		manifest[filepath.Base(p)] = digest
		fmt.Printf("%s sha256:%s\n", filepath.Base(p), digest)
	}
	if err := writeJSON(filepath.Join(*outDir, "manifest.json"), manifest); err != nil {
		panic(err)
	}
	fmt.Printf("fixtures written to %s with seed=%d\n", *outDir, *seed)
}

func makeBatch(releaseID string, plan stagePlan, g *generator) domain.BatchInput {
	var observations []domain.ObservationInput
	ts := plan.StartTimestamp
	for i := 0; i < 200; i++ {
		for _, group := range []string{domain.GroupCanary, domain.GroupStable} {
			base := plan.CanaryLatency
			successLimit := plan.CanarySuccesses
			if group == domain.GroupStable {
				base = plan.StableLatency
				successLimit = plan.StableSuccesses
			}
			success := successPermutation(i, plan.Stage, group) < successLimit
			latency := base + g.jitter(260) + int64((i*17)%31)
			observations = append(observations, domain.ObservationInput{
				TimestampMS: ts, InstanceID: fmt.Sprintf("%s-i-%03d", group, i), Group: group,
				Success: &success, LatencyUS: &latency, Stage: plan.Stage,
			})
			ts += 3
		}
	}
	return domain.BatchInput{ReleaseID: releaseID, BatchID: plan.BatchID, Stage: plan.Stage, Observations: observations}
}

func successPermutation(i, stage int, group string) int {
	offset := 0
	if group == domain.GroupStable {
		offset = 73
	}
	return (i*97 + stage*31 + offset) % 200
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func fileSHA256(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
