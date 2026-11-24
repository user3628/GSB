package rewardpack

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestCheckpointConditionalPublicationAndStableErrors(t *testing.T) {
	ctx := context.Background()
	store, err := OpenStoreContext(ctx, filepath.Join(t.TempDir(), "reward.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cfg := testConfig(4, 3)
	cfg.Version = 1
	if err := store.ApplyConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	emptyNormalized := []NormalizedPair{}
	payload, _ := json.Marshal(emptyNormalized)
	first, err := store.PublishStage(ctx, StagePublication{
		Stage: StageNormalize, ExpectedSeq: 0, InputFingerprint: "fp-1",
		ConfigVersion: 1, Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Sequence != 1 {
		t.Fatalf("expected first sequence 1, got %d", first.Sequence)
	}
	if _, err := store.PublishStage(ctx, StagePublication{
		Stage: StageNormalize, ExpectedSeq: 1, InputFingerprint: "fp-1",
		ConfigVersion: 1, Payload: payload,
	}); !HasCode(err, CodeStaleCheckpoint) {
		t.Fatalf("duplicate publication should be stale, got %v", err)
	}
	if _, err := store.PublishStage(ctx, StagePublication{
		Stage: StageNormalize, ExpectedSeq: 0, InputFingerprint: "fp-lag",
		ConfigVersion: 1, Payload: payload,
	}); !HasCode(err, CodeStaleCheckpoint) {
		t.Fatalf("lagging publication should be stale, got %v", err)
	}
	if _, err := store.PublishStage(ctx, StagePublication{
		Stage: StageIllegalName, ExpectedSeq: 0, InputFingerprint: "x",
		ConfigVersion: 1, Payload: payload,
	}); !HasCode(err, CodeIllegalStage) {
		t.Fatalf("expected illegal stage, got %v", err)
	}

	scorePayload, _ := json.Marshal([]ScoreResult{})
	if _, err := store.PublishStage(ctx, StagePublication{
		Stage: StageScore, ExpectedSeq: 0, InputFingerprint: "score-fp",
		ConfigVersion: 1, Payload: scorePayload, PreviousArtifactDigest: "wrong",
	}); !HasCode(err, CodeStaleCheckpoint) {
		t.Fatalf("cross-stage mismatch should be stale, got %v", err)
	}
	if _, err := store.PublishStage(ctx, StagePublication{
		Stage: StageFeatures, ExpectedSeq: 0, InputFingerprint: "feature-fp",
		ConfigVersion: 1, Payload: []byte("[]"), PreviousArtifactDigest: "",
	}); !HasCode(err, CodeMissingArtifact) {
		t.Fatalf("missing predecessor should be reported, got %v", err)
	}

	if err := store.ApplyConfig(ctx, Config{}); err == nil {
		t.Fatal("expected invalid empty config to fail")
	}
	cfg2 := cfg
	cfg2.Version = 2
	cfg2.Scoring.TermWeights = map[string]float64{"good": 3}
	if err := store.ApplyConfig(ctx, cfg2); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyConfig(ctx, cfg); !HasCode(err, CodeVersionRollback) {
		t.Fatalf("expected E_VERSION_ROLLBACK, got %v", err)
	}
	cfg2Conflict := cfg2
	cfg2Conflict.Scoring.TermWeights = map[string]float64{"good": 4}
	if err := store.ApplyConfig(ctx, cfg2Conflict); !HasCode(err, CodeVersionConflict) {
		t.Fatalf("expected E_VERSION_CONFLICT, got %v", err)
	}

	checkpoints, err := store.Checkpoints(ctx)
	if err != nil {
		t.Fatal(err)
	}
	valid := 0
	for _, checkpoint := range checkpoints {
		if checkpoint.Valid {
			valid++
		}
	}
	if valid != 1 {
		t.Fatalf("expected exactly one valid checkpoint for normalize/input, got %d", valid)
	}
}

const StageIllegalName = "not-a-stage"

func TestConcurrentBuildsSerializeWithoutStalePublication(t *testing.T) {
	ctx := context.Background()
	store := newSampleStore(t, ctx)
	engine := NewEngine(store)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	results := make(chan *BuildResult, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := engine.Build(ctx, BuildOptions{})
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}()
	}
	wg.Wait()
	close(errs)
	close(results)
	for err := range errs {
		t.Fatalf("concurrent build failed: %v", err)
	}
	var digests []string
	for result := range results {
		digests = append(digests, result.Bundle.Report.Digest)
	}
	if len(digests) != 2 || digests[0] != digests[1] {
		t.Fatalf("concurrent builds produced different reports: %#v", digests)
	}
}

func TestGeneratorIsDeterministicAndCoversStress(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	if err := GenerateSample(dirA, 42); err != nil {
		t.Fatal(err)
	}
	if err := GenerateSample(dirB, 42); err != nil {
		t.Fatal(err)
	}
	files := []string{"config.yaml", "annotations.jsonl", "valid.jsonl", "stress.jsonl", "shards/shard-a.jsonl", "shards/shard-b.jsonl", "shards/shard-c.jsonl", "shards/shard-d.jsonl"}
	for _, name := range files {
		a, err := osReadFile(filepath.Join(dirA, name))
		if err != nil {
			t.Fatal(err)
		}
		b, err := osReadFile(filepath.Join(dirB, name))
		if err != nil {
			t.Fatal(err)
		}
		if string(a) != string(b) {
			t.Fatalf("generated file differs for %s", name)
		}
	}
	stress, err := ReadJSONL(filepath.Join(dirA, "stress.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(stress) < 3 {
		t.Fatalf("stress sample too small: %d", len(stress))
	}
	seenEmpty, seenPunctuation, seenOutOfRange := false, false, false
	for _, annotation := range stress {
		if annotation.Chosen == "" {
			seenEmpty = true
		}
		if annotation.Rejected == "？？？" || annotation.Chosen == "！！！" {
			seenPunctuation = true
		}
		if annotation.Consistency < 0 || annotation.Consistency > 1 {
			seenOutOfRange = true
		}
	}
	if !seenEmpty || !seenPunctuation || !seenOutOfRange {
		t.Fatalf("stress sample misses required cases: empty=%v punctuation=%v range=%v", seenEmpty, seenPunctuation, seenOutOfRange)
	}
}

func osReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func TestCompareArtifactBundlesReportsFirstDifference(t *testing.T) {
	full := ArtifactBundle{Scores: []ScoreResult{{PairID: "p1", Strength: 1}}}
	incremental := ArtifactBundle{Scores: []ScoreResult{{PairID: "p1", Strength: 2}}}
	err := CompareArtifactBundles(incremental, full)
	if !HasCode(err, CodeIncrementalMismatch) {
		t.Fatalf("expected E_INCREMENTAL_MISMATCH, got %v", err)
	}
	be, ok := err.(*BusinessError)
	if !ok || be.PairID != "p1" || be.Actual == "" || be.Expected == "" {
		t.Fatalf("mismatch lacks pair/values: %#v", err)
	}
}
