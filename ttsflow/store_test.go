package ttsflow

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/flock"
)

func planForTest(t *testing.T, cfg Config, sample TextSample) synthesisPlan {
	t.Helper()
	frontend, err := BuildFrontend(sample, cfg)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := buildSynthesisPlan(sample, frontend, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func artifactForTest(plan segmentPlan, stage Stage, sample TextSample, cfg Config) StageArtifact {
	artifact := plan.stageArtifact(stage)
	artifact.TextDigest = sample.Digest()
	artifact.ConfigDigest = cfg.Digest()
	return artifact
}

func TestPublishConditionalStaleAndSnapshotReplacement(t *testing.T) {
	cfg, _, samples := testDataset(t)
	sample := samples[0]
	plan := planForTest(t, cfg, sample)
	store := openTestStore(t, "publish")
	ctx := context.Background()

	outcome, err := store.PublishStage(ctx, artifactForTest(plan.Segments[0], StageFrontend, sample, cfg))
	if err != nil || outcome.Kind != "partial" || outcome.StageMask != 1 {
		t.Fatalf("frontend publish = %#v, err=%v", outcome, err)
	}
	_, err = store.PublishStage(ctx, artifactForTest(plan.Segments[0], StageFrontend, sample, cfg))
	be, ok := AsBizError(err)
	if !ok || be.Code != CodeStaleCheckpoint || be.SnapshotKind != "duplicate" || be.CurrentSeq != -1 {
		t.Fatalf("duplicate error = %#v", err)
	}
	for _, stage := range []Stage{StageAcoustic, StageWave} {
		outcome, err = store.PublishStage(ctx, artifactForTest(plan.Segments[0], stage, sample, cfg))
		if err != nil || outcome.Kind != "partial" {
			t.Fatalf("stage %s = %#v, err=%v", stage, outcome, err)
		}
	}
	outcome, err = store.PublishStage(ctx, artifactForTest(plan.Segments[0], StageConsistency, sample, cfg))
	if err != nil || outcome.Kind != "advance" || outcome.CurrentSeq != 0 || outcome.StageMask != completeStageMask {
		t.Fatalf("consistency publish = %#v, err=%v", outcome, err)
	}
	events, err := store.EventCount(ctx, sample.UTTID)
	if err != nil || events != 4 {
		t.Fatalf("events = %d, err=%v", events, err)
	}

	_, err = store.PublishStage(ctx, artifactForTest(plan.Segments[2], StageFrontend, sample, cfg))
	be, ok = AsBizError(err)
	if !ok || be.Code != CodeStaleCheckpoint || be.SnapshotKind != "gap" || be.CurrentSeq != 0 {
		t.Fatalf("gap error = %#v", err)
	}

	if _, err := store.PublishStage(ctx, artifactForTest(plan.Segments[1], StageFrontend, sample, cfg)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE checkpoints SET frontend_artifact=X'00' WHERE utt_id=? AND segment_seq=1 AND active=1`, sample.UTTID); err != nil {
		t.Fatal(err)
	}
	_, err = store.PublishStage(ctx, artifactForTest(plan.Segments[1], StageAcoustic, sample, cfg))
	be, ok = AsBizError(err)
	if !ok || be.Code != CodeStaleCheckpoint || be.SnapshotKind != "replacement" || be.CurrentSeq != 0 {
		t.Fatalf("replacement error = %#v", err)
	}
	kind, err := store.InspectSnapshot(ctx, sample.UTTID)
	if err != nil || kind != "replacement" {
		t.Fatalf("inspect replacement = %q, err=%v", kind, err)
	}
}

func TestSnapshotRollbackIsDistinguished(t *testing.T) {
	cfg, _, samples := testDataset(t)
	sample := samples[0]
	plan := planForTest(t, cfg, sample)
	store := openTestStore(t, "rollback-snapshot")
	ctx := context.Background()
	for _, stage := range []Stage{StageFrontend, StageAcoustic, StageWave, StageConsistency} {
		if _, err := store.PublishStage(ctx, artifactForTest(plan.Segments[0], stage, sample, cfg)); err != nil {
			t.Fatalf("stage %s: %v", stage, err)
		}
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE checkpoints SET active=0 WHERE utt_id=? AND segment_seq=0`, sample.UTTID); err != nil {
		t.Fatal(err)
	}
	kind, err := store.InspectSnapshot(ctx, sample.UTTID)
	if err != nil || kind != "rollback" {
		t.Fatalf("inspect rollback = %q, err=%v", kind, err)
	}
	_, err = store.PublishStage(ctx, artifactForTest(plan.Segments[1], StageFrontend, sample, cfg))
	be, ok := AsBizError(err)
	if !ok || be.Code != CodeStaleCheckpoint || be.SnapshotKind != "rollback" {
		t.Fatalf("rollback publish error = %#v", err)
	}
}

func TestConcurrentPublishHasSingleWinner(t *testing.T) {
	cfg, _, samples := testDataset(t)
	sample := samples[0]
	plan := planForTest(t, cfg, sample)
	store := openTestStore(t, "concurrent")
	artifact := artifactForTest(plan.Segments[0], StageFrontend, sample, cfg)
	const workers = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := store.PublishStage(context.Background(), artifact)
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	successes := 0
	stale := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		if IsCode(err, CodeStaleCheckpoint) {
			stale++
			continue
		}
		t.Fatalf("unexpected publish error: %v", err)
	}
	if successes != 1 || stale != workers-1 {
		t.Fatalf("successes=%d stale=%d, want 1/%d", successes, stale, workers-1)
	}
	events, err := store.EventCount(context.Background(), sample.UTTID)
	if err != nil || events != 1 {
		t.Fatalf("events=%d err=%v", events, err)
	}
}

func TestConcurrentStoresUseFileLock(t *testing.T) {
	cfg, _, samples := testDataset(t)
	sample := samples[0]
	plan := planForTest(t, cfg, sample)
	path := filepath.Join(t.TempDir(), "shared.db")
	store1, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store1.Close()
	store2, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	artifact := artifactForTest(plan.Segments[0], StageFrontend, sample, cfg)
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, store := range []*Store{store1, store2} {
		store := store
		go func() {
			<-start
			_, err := store.PublishStage(context.Background(), artifact)
			results <- err
		}()
	}
	close(start)
	successes := 0
	stale := 0
	for i := 0; i < 2; i++ {
		err := <-results
		if err == nil {
			successes++
		} else if IsCode(err, CodeStaleCheckpoint) {
			stale++
		} else {
			t.Fatalf("unexpected cross-store publish error: %v", err)
		}
	}
	if successes != 1 || stale != 1 {
		t.Fatalf("successes=%d stale=%d, want 1/1", successes, stale)
	}
}

func TestPublishUsesRealFileLock(t *testing.T) {
	cfg, _, samples := testDataset(t)
	sample := samples[0]
	plan := planForTest(t, cfg, sample)
	store := openTestStore(t, "file-lock")
	other := flock.New(store.lockPath)
	locked, err := other.TryLock()
	if err != nil || !locked {
		t.Fatalf("external lock: locked=%v err=%v", locked, err)
	}
	defer func() { _ = other.Unlock() }()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	_, err = store.PublishStage(ctx, artifactForTest(plan.Segments[0], StageFrontend, sample, cfg))
	if !IsCode(err, CodeLock) {
		t.Fatalf("expected E_LOCK while external lock is held, got %v", err)
	}
}

func TestVersionAppendReplaceAndRollback(t *testing.T) {
	cfg, raw, samples := testDataset(t)
	store := openTestStore(t, "versions")
	ctx := context.Background()
	v1, err := store.ObserveTextSet(ctx, samples, SnapshotAuto)
	if err != nil || v1.Status != "initialize" || v1.Version != 1 {
		t.Fatalf("initial version = %#v, err=%v", v1, err)
	}
	appended := append([]TextSample(nil), samples...)
	appended = append(appended, TextSample{UTTID: "utt-extra", Text: "new", Language: "en", SpeakerID: 2})
	v2, err := store.ObserveTextSet(ctx, appended, SnapshotAuto)
	if err != nil || v2.Status != "append" || v2.Version != 2 {
		t.Fatalf("append version = %#v, err=%v", v2, err)
	}
	_, err = store.ObserveTextSet(ctx, samples, SnapshotAuto)
	be, ok := AsBizError(err)
	if !ok || be.Code != CodeRollback || be.CurrentSeq != 2 {
		t.Fatalf("text rollback error = %#v", err)
	}

	_ = runEngine(t, newTestEngine(t, store, cfg, raw, appended), RunOptions{})
	v3, err := store.ObserveTextSet(ctx, samples[:2], SnapshotReplace)
	if err != nil || v3.Status != "replace" || v3.Version != 3 {
		t.Fatalf("replace version = %#v, err=%v", v3, err)
	}
	rows, err := store.ActiveCheckpoints(ctx, "utt-001")
	if err != nil || len(rows) != 0 {
		t.Fatalf("active rows after replace = %d, err=%v", len(rows), err)
	}
	if _, exists, err := store.StreamState(ctx, "utt-001"); err != nil || exists {
		t.Fatalf("stream state after replace exists=%v err=%v", exists, err)
	}
}

func TestConfigUpdateReplaceAndRollback(t *testing.T) {
	cfg, raw, _ := testDataset(t)
	store := openTestStore(t, "config-versions")
	ctx := context.Background()
	v1, err := store.ObserveConfig(ctx, cfg, raw, SnapshotAuto)
	if err != nil || v1.Status != "initialize" {
		t.Fatalf("config initial = %#v, err=%v", v1, err)
	}
	cfg2 := cfg
	cfg2.Revision++
	cfg2.Acoustic.WeightSeed++
	raw2, _ := cfg2.EncodeYAML()
	v2, err := store.ObserveConfig(ctx, cfg2, raw2, SnapshotAuto)
	if err != nil || v2.Status != "update" || v2.Version != 2 {
		t.Fatalf("config update = %#v, err=%v", v2, err)
	}
	_, err = store.ObserveConfig(ctx, cfg, raw, SnapshotAuto)
	be, ok := AsBizError(err)
	if !ok || be.Code != CodeRollback || be.CurrentSeq != 2 {
		t.Fatalf("config rollback = %#v", err)
	}
	cfg3 := cfg2
	cfg3.Acoustic.Bands = 5
	cfg3.Acoustic.Layers = []LayerSpec{{In: 12, Out: 10}, {In: 10, Out: 6}}
	raw3, _ := cfg3.EncodeYAML()
	v3, err := store.ObserveConfig(ctx, cfg3, raw3, SnapshotReplace)
	if err != nil || v3.Status != "replace" || v3.Version != 3 {
		t.Fatalf("config replace = %#v, err=%v", v3, err)
	}
	if _, err := store.ObserveConfig(ctx, cfg3, raw3, SnapshotAuto); err != nil {
		t.Fatalf("unchanged config: %v", err)
	}
}

func TestUnknownGapPublishHasCurrentSeq(t *testing.T) {
	cfg, _, samples := testDataset(t)
	store := openTestStore(t, "cross-utt-gap")
	plan := planForTest(t, cfg, samples[1])
	_, err := store.PublishStage(context.Background(), artifactForTest(plan.Segments[3], StageFrontend, samples[1], cfg))
	be, ok := AsBizError(err)
	if !ok || be.Code != CodeStaleCheckpoint || be.CurrentSeq != -1 || be.SnapshotKind != "gap" {
		t.Fatalf("gap publish = %#v", err)
	}
	if errors.Is(err, ErrGracefulStop) {
		t.Fatal("unexpected sentinel")
	}
}
