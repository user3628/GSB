package ttsflow

import (
	"context"
	"errors"
	"testing"
)

const goldenFullManifest = "db9896ddd04940604e4d9d19be86e7e528c7b11179531e64f6ce65645bc62769"

func TestGoldenManifestDigest(t *testing.T) {
	cfg, raw, samples := testDataset(t)
	result := runEngine(t, newTestEngine(t, openTestStore(t, "golden"), cfg, raw, samples), RunOptions{})
	if result.ManifestDigest != goldenFullManifest {
		t.Fatalf("golden manifest = %s, want %s", result.ManifestDigest, goldenFullManifest)
	}
}

func TestFullGracefulStopAndRecoveryMatch(t *testing.T) {
	cfg, raw, samples := testDataset(t)
	fullStore := openTestStore(t, "full")
	full := runEngine(t, newTestEngine(t, fullStore, cfg, raw, samples), RunOptions{})

	stopStore := openTestStore(t, "stop")
	engine := newTestEngine(t, stopStore, cfg, raw, samples)
	stopped, err := engine.Run(context.Background(), RunOptions{MaxSegments: 10})
	if !errors.Is(err, ErrGracefulStop) {
		t.Fatalf("expected graceful stop, got %v", err)
	}
	if !stopped.GracefulStopped {
		t.Fatal("result did not mark graceful stop")
	}
	completed, err := stopStore.CompletedSegments(context.Background(), "utt-001")
	if err != nil {
		t.Fatal(err)
	}
	if completed != 10 {
		t.Fatalf("completed segments = %d, want 10", completed)
	}
	status, ok, err := engine.Status(context.Background(), "utt-001", 0)
	if err != nil || !ok {
		t.Fatalf("status: ok=%v err=%v", ok, err)
	}
	if status.StageMask != completeStageMask {
		t.Fatalf("stopped segment stage mask = %d, want %d", status.StageMask, completeStageMask)
	}
	if _, ok, _ := engine.Status(context.Background(), "utt-001", 10); ok {
		t.Fatal("checkpoint 10 should not exist before the next unit is committed")
	}

	recoveryEngine := newTestEngine(t, stopStore, cfg, raw, samples)
	recovered := runEngine(t, recoveryEngine, RunOptions{ClearStops: true})
	if recovered.ReusedStageCheckpoints < 40 {
		t.Fatalf("recovery reused %d stages, want at least 40", recovered.ReusedStageCheckpoints)
	}
	if err := CompareSynthesisResults(recovered, full); err != nil {
		t.Fatalf("recovery differs from full run: %v", err)
	}
	if recovered.ManifestDigest != full.ManifestDigest {
		t.Fatalf("manifest %s != %s", recovered.ManifestDigest, full.ManifestDigest)
	}
	state, exists, err := stopStore.StreamState(context.Background(), "utt-001")
	if err != nil || !exists || state.Status != "complete" {
		t.Fatalf("final stream state = %#v, exists=%v err=%v", state, exists, err)
	}
}

func TestPersistedGracefulStopRequest(t *testing.T) {
	cfg, raw, samples := testDataset(t)
	store := openTestStore(t, "requested-stop")
	if err := store.RequestStop(context.Background(), "utt-001"); err != nil {
		t.Fatal(err)
	}
	engine := newTestEngine(t, store, cfg, raw, samples)
	result, err := engine.Run(context.Background(), RunOptions{})
	if !errors.Is(err, ErrGracefulStop) {
		t.Fatalf("expected graceful stop, got %v", err)
	}
	if !result.GracefulStopped {
		t.Fatal("result did not mark stop")
	}
	state, exists, err := store.StreamState(context.Background(), "utt-001")
	if err != nil || !exists || state.Status != "stopped" {
		t.Fatalf("stopped stream state = %#v, exists=%v err=%v", state, exists, err)
	}
	completed, err := store.CompletedSegments(context.Background(), "utt-001")
	if err != nil || completed != 1 {
		t.Fatalf("completed after request = %d, err=%v", completed, err)
	}
	recovered := runEngine(t, newTestEngine(t, store, cfg, raw, samples), RunOptions{ClearStops: true})
	if recovered.GracefulStopped {
		t.Fatal("recovery unexpectedly stopped")
	}
}

func TestIncrementalAcousticWeightMatchesFull(t *testing.T) {
	cfg, raw, samples := testDataset(t)
	store := openTestStore(t, "incremental-config")
	_ = runEngine(t, newTestEngine(t, store, cfg, raw, samples), RunOptions{})

	cfg2 := cfg
	cfg2.Revision++
	cfg2.Acoustic.WeightSeed++
	raw2, err := cfg2.EncodeYAML()
	if err != nil {
		t.Fatal(err)
	}
	incremental := runEngine(t, newTestEngine(t, store, cfg2, raw2, samples), RunOptions{})
	if incremental.EarliestInvalidatedStage != StageAcoustic.String() {
		t.Fatalf("earliest stage = %q, want acoustic", incremental.EarliestInvalidatedStage)
	}
	if incremental.EarliestInvalidatedSegment != 0 {
		t.Fatalf("earliest segment = %d, want 0", incremental.EarliestInvalidatedSegment)
	}
	if incremental.ReusedStageCheckpoints == 0 {
		t.Fatal("frontend checkpoints should be reused when only acoustic weights change")
	}

	fullStore := openTestStore(t, "full-v2")
	full := runEngine(t, newTestEngine(t, fullStore, cfg2, raw2, samples), RunOptions{})
	if err := CompareSynthesisResults(incremental, full); err != nil {
		t.Fatalf("incremental differs from full: %v", err)
	}
}

func TestIncrementalTextEditPreservesPrefixAndMatchesFull(t *testing.T) {
	cfg, raw, samples := testDataset(t)
	store := openTestStore(t, "incremental-text")
	_ = runEngine(t, newTestEngine(t, store, cfg, raw, samples), RunOptions{})

	edited := append([]TextSample(nil), samples...)
	for i := range edited {
		if edited[i].UTTID == "utt-004" {
			edited[i].Text += "可复算。"
		}
	}
	incremental := runEngine(t, newTestEngine(t, store, cfg, raw, edited), RunOptions{})
	if incremental.EarliestInvalidatedStage != StageFrontend.String() {
		t.Fatalf("earliest stage = %q, want frontend", incremental.EarliestInvalidatedStage)
	}
	if incremental.EarliestInvalidatedSegment <= 0 {
		t.Fatalf("earliest segment = %d, expected a later segment", incremental.EarliestInvalidatedSegment)
	}
	if incremental.ReusedStageCheckpoints == 0 {
		t.Fatal("expected unaffected prefix checkpoints to be reused")
	}

	fullStore := openTestStore(t, "full-edited")
	full := runEngine(t, newTestEngine(t, fullStore, cfg, raw, edited), RunOptions{})
	if err := CompareSynthesisResults(incremental, full); err != nil {
		t.Fatalf("edited incremental differs from full: %v", err)
	}
}

func TestIncrementalTextShorteningDropsTrailingSegments(t *testing.T) {
	cfg, raw, samples := testDataset(t)
	store := openTestStore(t, "incremental-shorter")
	_ = runEngine(t, newTestEngine(t, store, cfg, raw, samples), RunOptions{})

	edited := append([]TextSample(nil), samples...)
	for i := range edited {
		if edited[i].UTTID == "utt-004" {
			edited[i].Text = "每一帧。"
		}
	}
	incremental := runEngine(t, newTestEngine(t, store, cfg, raw, edited), RunOptions{})
	if incremental.EarliestInvalidatedStage != StageFrontend.String() {
		t.Fatalf("earliest stage = %q, want frontend", incremental.EarliestInvalidatedStage)
	}
	full := runEngine(t, newTestEngine(t, openTestStore(t, "full-shorter"), cfg, raw, edited), RunOptions{})
	if err := CompareSynthesisResults(incremental, full); err != nil {
		t.Fatalf("shortened incremental differs from full: %v", err)
	}
	rows, err := store.ActiveCheckpoints(context.Background(), "utt-004")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(full.UTTs[len(full.UTTs)-1].SegmentStats) {
		t.Fatalf("active trailing rows = %d, want %d", len(rows), len(full.UTTs[len(full.UTTs)-1].SegmentStats))
	}
}

func TestIncrementalPhonemeMapChangeMatchesFull(t *testing.T) {
	cfg, raw, samples := testDataset(t)
	store := openTestStore(t, "incremental-map")
	_ = runEngine(t, newTestEngine(t, store, cfg, raw, samples), RunOptions{})

	cfg2 := cfg
	cfg2.Revision++
	cfg2.Frontend.PhonemeUnits["duan"] = "PH_DUAN_ALT"
	raw2, err := cfg2.EncodeYAML()
	if err != nil {
		t.Fatal(err)
	}
	incremental := runEngine(t, newTestEngine(t, store, cfg2, raw2, samples), RunOptions{})
	if incremental.EarliestInvalidatedStage != StageFrontend.String() {
		t.Fatalf("earliest stage = %q, want frontend", incremental.EarliestInvalidatedStage)
	}
	if incremental.EarliestInvalidatedSegment <= 0 {
		t.Fatalf("earliest segment = %d, expected the later occurrence of the remapped phoneme", incremental.EarliestInvalidatedSegment)
	}
	if incremental.ReusedStageCheckpoints == 0 {
		t.Fatal("expected unaffected prefix to be reused after a local phoneme-map change")
	}
	full := runEngine(t, newTestEngine(t, openTestStore(t, "full-map-v2"), cfg2, raw2, samples), RunOptions{})
	if err := CompareSynthesisResults(incremental, full); err != nil {
		t.Fatalf("map incremental differs from full: %v", err)
	}
}

func TestIncrementalSplitChangeMatchesFull(t *testing.T) {
	cfg, raw, samples := testDataset(t)
	store := openTestStore(t, "incremental-split")
	_ = runEngine(t, newTestEngine(t, store, cfg, raw, samples), RunOptions{})

	cfg2 := cfg
	cfg2.Revision++
	cfg2.Splitting.FollowingChars = 3
	raw2, err := cfg2.EncodeYAML()
	if err != nil {
		t.Fatal(err)
	}
	incremental := runEngine(t, newTestEngine(t, store, cfg2, raw2, samples), RunOptions{})
	if incremental.EarliestInvalidatedStage != StageFrontend.String() {
		t.Fatalf("earliest stage = %q, want frontend", incremental.EarliestInvalidatedStage)
	}
	if incremental.ReusedStageCheckpoints == 0 {
		t.Fatal("expected at least one unaffected prefix checkpoint to be reused")
	}
	full := runEngine(t, newTestEngine(t, openTestStore(t, "full-split-v2"), cfg2, raw2, samples), RunOptions{})
	if err := CompareSynthesisResults(incremental, full); err != nil {
		t.Fatalf("split incremental differs from full: %v", err)
	}
}

func TestCompareIncrementalMismatchFirstDiffSegment(t *testing.T) {
	full := SynthesisResult{
		UTTs: []UTTResult{{
			Sample:   TextSample{UTTID: "u", Language: "zh", Text: "文本", SpeakerID: 1},
			Frontend: FrontendResult{Digest: "f"}, AcousticDigest: "a", PCMDigest: "p", StatsDigest: "s",
			SegmentStats: []SegmentStats{{PCMDigest: "p0"}, {PCMDigest: "p1"}, {PCMDigest: "p2"}},
		}},
		ConfigDigest: "c", TextSetDigest: "t", ManifestDigest: "m",
	}
	actual := full
	actual.UTTs = append([]UTTResult(nil), full.UTTs...)
	actual.UTTs[0].PCMDigest = "different"
	actual.UTTs[0].SegmentStats = append([]SegmentStats(nil), full.UTTs[0].SegmentStats...)
	actual.UTTs[0].SegmentStats[1].PCMDigest = "bad"
	err := CompareSynthesisResults(actual, full)
	be, ok := AsBizError(err)
	if !ok || be.Code != CodeIncrementalMismatch || be.FirstDiffSegment != 1 {
		t.Fatalf("mismatch error = %#v", err)
	}
}
