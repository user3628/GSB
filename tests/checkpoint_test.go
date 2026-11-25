package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"voiceline/internal/checkpoint"
	"voiceline/internal/model"
	"voiceline/internal/pipeline"
)

func TestSQLiteConditionalPublication(t *testing.T) {
	ctx := context.Background()
	store, err := checkpoint.Open(ctx, filepath.Join(t.TempDir(), "checkpoints.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	segmentID := "seg-lock"
	if err := store.EnsureShard(ctx, segmentID); err != nil {
		t.Fatal(err)
	}
	_, err = store.PublishBlock(ctx, checkpoint.BlockPublication{
		SegmentID: segmentID, BlockIndex: 1, WorkerID: "w", PCM: []byte{1, 2}, Digest: "d1",
	})
	var business *model.BusinessError
	if !errors.As(err, &business) || business.Code != model.CodeStaleCheckpoint {
		t.Fatalf("expected out-of-order E_STALE_CHECKPOINT, got %v", err)
	}

	if _, err := store.PublishBlock(ctx, checkpoint.BlockPublication{
		SegmentID: segmentID, BlockIndex: 0, WorkerID: "w0", PCM: []byte{1, 2}, Digest: "d0",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PublishBlock(ctx, checkpoint.BlockPublication{
		SegmentID: segmentID, BlockIndex: 0, WorkerID: "w0-dup", PCM: []byte{3, 4}, Digest: "dup",
	}); !errors.As(err, &business) || business.Code != model.CodeStaleCheckpoint {
		t.Fatalf("expected duplicate E_STALE_CHECKPOINT, got %v", err)
	}

	const workers = 12
	var wg sync.WaitGroup
	successes := make(chan struct{}, workers)
	stale := make(chan struct{}, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := store.PublishBlock(ctx, checkpoint.BlockPublication{
				SegmentID: segmentID, BlockIndex: 1, WorkerID: "race", PCM: []byte{5, 6}, Digest: "d1",
			})
			if err == nil {
				successes <- struct{}{}
				return
			}
			var be *model.BusinessError
			if errors.As(err, &be) && be.Code == model.CodeStaleCheckpoint {
				stale <- struct{}{}
			} else {
				t.Errorf("unexpected publish error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	close(successes)
	close(stale)
	if len(successes) != 1 {
		t.Fatalf("success count = %d, want 1", len(successes))
	}
	if len(stale) != workers-1 {
		t.Fatalf("stale count = %d, want %d", len(stale), workers-1)
	}

	if err := store.RegisterAssignment(ctx, 9001, "seg-assign", 0); err != nil {
		t.Fatal(err)
	}
	_, err = store.PublishAssignedBlock(ctx, checkpoint.AssignedPublication{
		GlobalSeq: 9001,
		BlockPublication: checkpoint.BlockPublication{
			SegmentID: "seg-other", BlockIndex: 0, WorkerID: "cross", PCM: []byte{1}, Digest: "x",
		},
	})
	if !errors.As(err, &business) || business.Code != model.CodeStaleCheckpoint ||
		business.Details["reason"] != "cross_shard_misplacement" {
		t.Fatalf("expected cross-shard E_STALE_CHECKPOINT, got %v", err)
	}

	cp1, err := store.PutCheckpoint(ctx, checkpoint.Checkpoint{
		SegmentID: "seg-cp", Stage: "analysis", InputFingerprint: "in", PolicyVersion: 1, ArtifactDigest: "a",
	})
	if err != nil {
		t.Fatal(err)
	}
	cp2, err := store.PutCheckpoint(ctx, checkpoint.Checkpoint{
		SegmentID: "seg-cp", Stage: "analysis", InputFingerprint: "in", PolicyVersion: 2, ArtifactDigest: "b",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cp2.Revision != 2 || cp2.MonotonicSeq <= cp1.MonotonicSeq {
		t.Fatalf("checkpoint revision/sequence not monotonic: %+v %+v", cp1, cp2)
	}
	var rowCount int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT count(*) FROM stage_checkpoint WHERE segment_id='seg-cp' AND stage='analysis'`).Scan(&rowCount); err != nil {
		t.Fatal(err)
	}
	if rowCount != 1 {
		t.Fatalf("valid checkpoint rows = %d, want 1", rowCount)
	}
}

func TestGracefulStopAndRecoveryByteIdentity(t *testing.T) {
	input := generatedInput(t)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "recovery.db")

	store, err := checkpoint.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	stop := pipeline.NewStopController()
	errCh := make(chan error, 1)
	go func() {
		_, err := pipeline.RunSegment(ctx, input, pipeline.RunOptions{
			Store: store, Parallel: true, WorkerID: "graceful", BlockDelay: 60 * time.Millisecond, Stop: stop,
		})
		errCh <- err
	}()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		next, err := store.NextBlockIndex(ctx, input.Segment.ID)
		if err == nil && next >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	stop.RequestStop()
	select {
	case err := <-errCh:
		if !errors.Is(err, pipeline.ErrGracefulStop) {
			t.Fatalf("first run error = %v, want ErrGracefulStop", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("graceful run did not stop")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := checkpoint.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	report, err := pipeline.RunSegment(ctx, input, pipeline.RunOptions{
		Store: reopened, Parallel: true, WorkerID: "recovery",
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.ReusedBlocks < 1 {
		t.Fatalf("reused blocks = %d, want at least one committed block", report.ReusedBlocks)
	}

	serial, err := pipeline.ConvertSegment(ctx, input.Segment, input.Manifest.SampleRate, input.PCM.SampleRate,
		input.PCM.Samples, input.Fingerprint, pipeline.ConvertOptions{PolicyVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	if string(report.Result.WAVBytes) != string(serial.WAVBytes) {
		t.Fatal("recovered WAV is not byte-identical to continuous serial conversion")
	}
	if string(report.Result.SummaryJSON) != string(serial.SummaryJSON) {
		t.Fatal("recovered summary is not byte-identical")
	}
	recoveredFrames, err := json.Marshal(report.Result.Frames)
	if err != nil {
		t.Fatal(err)
	}
	serialFrames, err := json.Marshal(serial.Frames)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(recoveredFrames, serialFrames) {
		t.Fatal("recovered frame F0/Mel sequence is not byte-identical")
	}
	if report.ValidationCheckpoint == nil {
		t.Fatal("validation checkpoint missing after recovery")
	}
}
