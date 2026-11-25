package speakerdiar

import (
	"bytes"
	"testing"
	"time"
)

func runSuccessfulService(t *testing.T, dir string) *RunReport {
	t.Helper()
	svc, err := NewService(dir, "test", InjectNone)
	if err != nil {
		t.Fatal(err)
	}
	report, err := svc.Run()
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
	return report
}

func TestIncrementalRecomputePlanAndByteEquality(t *testing.T) {
	dir := t.TempDir()
	svc, err := NewService(dir, "incremental", InjectNone)
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.Run()
	if err != nil {
		t.Fatal(err)
	}

	steps := []struct {
		name       string
		chunkIndex int
		change     func()
		want       string
	}{
		{"margin", 0, func() { svc.Manifest.Config.MarginThreshold = 0.08 }, StageEmbed},
		{"mask_rounds", 0, func() { svc.Manifest.Config.MaskMaxIterations = 5 }, StageSeparate},
		{"speaker_count", 2, func() { svc.Manifest.Chunks[2].SpeakerCount = 3 }, StageSeparate},
		{"reference_annotation", 0, func() {
			svc.Manifest.Chunks[0].ReferenceSegments[0].Confidence = 0.41
		}, StageEmbed},
		{"projection", 0, func() { svc.Manifest.Config.EmbeddingDim = 48 }, StageEmbed},
	}
	var lastReport *RunReport
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			step.change()
			if err := SaveManifest(dir, svc.Manifest); err != nil {
				t.Fatal(err)
			}
			if err := svc.ReloadManifest(); err != nil {
				t.Fatal(err)
			}
			report, err := svc.Run()
			if err != nil {
				t.Fatal(err)
			}
			lastReport = report
			cr := report.Chunks[step.chunkIndex]
			if !containsStage(cr.RecomputedStages, step.want) {
				t.Fatalf("expected %s recompute in %+v", step.want, cr)
			}
			if step.want == StageSeparate && !containsStage(cr.ReusedStages, StageFrame) {
				t.Fatalf("frame should be reused: %+v", cr)
			}
			if step.want == StageEmbed && !containsStage(cr.ReusedStages, StageSeparate) {
				t.Fatalf("separation should be reused for embedding-only change: %+v", cr)
			}
			if err := VerifyIncrementalEqualsFull(dir, svc.Manifest, report); err != nil {
				t.Fatal(err)
			}
		})
	}

	if lastReport == nil {
		t.Fatal("expected incremental reports")
	}
	lastReport.Products[0].Embedding.Digest = "tampered"
	err = VerifyIncrementalEqualsFull(dir, svc.Manifest, lastReport)
	if CodeOf(err) != CodeIncrementalMismatch {
		t.Fatalf("expected E_INCREMENTAL_MISMATCH, got %v", err)
	}
	de, _ := AsError(err)
	if de.ChunkID == "" || de.Stage != StageEmbed {
		t.Fatalf("mismatch should identify first chunk/stage: %+v", de)
	}
}

func containsStage(stages []string, target string) bool {
	for _, s := range stages {
		if s == target {
			return true
		}
	}
	return false
}

func TestGracefulStopAndRecoveryMatchesContinuous(t *testing.T) {
	continuousDir := t.TempDir()
	continuous := runSuccessfulService(t, continuousDir)
	continuousFinal, err := ReadOutputFile(continuousDir, "final.json")
	if err != nil {
		t.Fatal(err)
	}

	stopDir := t.TempDir()
	svc, err := NewService(stopDir, "stop", InjectNone)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan *RunReport, 1)
	go func() {
		r, runErr := svc.Run()
		if runErr != nil {
			t.Errorf("stopped run: %v", runErr)
		}
		done <- r
	}()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		cp, err := svc.Store.GetCheckpoint("chunk-000", StageFrame)
		if err != nil {
			t.Fatal(err)
		}
		if cp != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	svc.RequestStop()
	var stopped *RunReport
	select {
	case stopped = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("graceful run did not finish current chunk")
	}
	if !stopped.GracefulStop || len(stopped.Chunks) != 1 {
		t.Fatalf("expected stop after first chunk, got %+v", stopped)
	}
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := NewService(stopDir, "recovery", InjectNone)
	if err != nil {
		t.Fatal(err)
	}
	report, err := recovered.Run()
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyIncrementalEqualsFull(stopDir, recovered.Manifest, report); err != nil {
		t.Fatal(err)
	}
	if err := recovered.Store.VerifySequenceContinuity(); err != nil {
		t.Fatal(err)
	}
	checkpoints, err := recovered.Store.ListCheckpoints(recovered.Manifest.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(checkpoints) != 12 {
		t.Fatalf("expected 12 valid checkpoints, got %d", len(checkpoints))
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
	recoveredFinal, err := ReadOutputFile(stopDir, "final.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(continuousFinal, recoveredFinal) {
		t.Fatal("recovered final.json differs from continuous execution")
	}
	if continuous.Final == nil {
		t.Fatal("continuous report missing final")
	}
}

func TestCommittedStageFailureRecovery(t *testing.T) {
	dir := t.TempDir()
	failing, err := NewService(dir, "crashed-before-separation", InjectNegativeMaskRounds)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failing.Run(); CodeOf(err) != CodeInvalidParam {
		t.Fatalf("expected E_INVALID_PARAM, got %v", err)
	}
	cp, err := failing.Store.GetCheckpoint("chunk-000", StageFrame)
	if err != nil || cp == nil {
		t.Fatalf("frame checkpoint should be committed: cp=%v err=%v", cp, err)
	}
	if err := failing.Close(); err != nil {
		t.Fatal(err)
	}

	svc, err := NewService(dir, "restart", InjectNone)
	if err != nil {
		t.Fatal(err)
	}
	report, err := svc.Run()
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyIncrementalEqualsFull(dir, svc.Manifest, report); err != nil {
		t.Fatal(err)
	}
	if err := svc.Store.VerifySequenceContinuity(); err != nil {
		t.Fatal(err)
	}
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentConditionalPublishAndMutationClassification(t *testing.T) {
	dir := t.TempDir()
	store1, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store1.Close()
	store2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	req := PublishRequest{
		SessionID: "s", ProducerID: "p1", ChunkID: "chunk-000", ChunkIndex: 0,
		Stage: StageFrame, ContentFingerprint: "content", Fingerprint: "fp1",
		ConfigVersion: "cfg", ArtifactDigest: "digest1", ExpectedChunkSequence: 0,
	}
	cp, err := store1.PublishCheckpoint(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store2.PublishCheckpoint(req); CodeOf(err) != CodeStaleCheckpoint {
		t.Fatalf("duplicate/stale publisher should fail, got %v", err)
	}
	cross := req
	cross.ChunkID = "chunk-009"
	cross.ChunkIndex = 9
	cross.Stage = StageFrame
	cross.ExpectedChunkSequence = 0
	if _, err := store2.PublishCheckpoint(cross); CodeOf(err) != CodeStaleCheckpoint {
		t.Fatalf("cross-chunk misplacement should return E_STALE_CHECKPOINT, got %v", err)
	}

	rollbackObserved := *cp
	rollbackObserved.Sequence = 0
	class, err := store1.HandleObservedRecord(rollbackObserved)
	if class != MutationRollback || CodeOf(err) != CodeStaleCheckpoint {
		t.Fatalf("expected rollback rejection, class=%s err=%v", class, err)
	}
	still, err := store1.GetCheckpoint(cp.ChunkID, cp.Stage)
	if err != nil || still == nil {
		t.Fatalf("rollback must preserve original state: cp=%v err=%v", still, err)
	}

	replaced := *cp
	replaced.ArtifactDigest = "different"
	class, err = store1.HandleObservedRecord(replaced)
	if err != nil || class != MutationReplaced {
		t.Fatalf("expected replacement classification and invalidation, class=%s err=%v", class, err)
	}
	gone, err := store1.GetCheckpoint(cp.ChunkID, cp.Stage)
	if err != nil || gone != nil {
		t.Fatalf("replacement should invalidate stage, cp=%v err=%v", gone, err)
	}
}

func TestInjectionFailureLeavesCommittedSnapshotsReadable(t *testing.T) {
	dir := t.TempDir()
	runSuccessfulService(t, dir)
	before, err := ReadOutputFile(dir, "final.json")
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		injection Injection
		code      string
	}{
		{InjectLengthMismatch, CodeInputLength},
		{InjectRateMismatch, CodeRateMismatch},
		{InjectZeroSpeakers, CodeInvalidParam},
		{InjectNegativeMaskRounds, CodeInvalidParam},
		{InjectProjectionDimMismatch, CodeEmbedding},
		{InjectEmbeddingTie, CodeEmbedding},
	}
	for _, tc := range cases {
		t.Run(string(tc.injection), func(t *testing.T) {
			svc, err := NewService(dir, "inject", tc.injection)
			if err != nil {
				t.Fatal(err)
			}
			_, runErr := svc.Run()
			if CodeOf(runErr) != tc.code {
				t.Fatalf("expected %s, got %v", tc.code, runErr)
			}
			if err := svc.Close(); err != nil {
				t.Fatal(err)
			}
			snap := NewSnapshotStore(dir)
			var spec SpectralResult
			if _, err := snap.LoadStagePayload("chunk-000", StageFrame, &spec); err != nil {
				t.Fatalf("committed snapshot unreadable after injection: %v", err)
			}
		})
	}
	after, err := ReadOutputFile(dir, "final.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("successful prior outputs changed after failed injections")
	}
}
