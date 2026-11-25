package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"voiceline/internal/diagnostics"
	"voiceline/internal/dispatch"
	"voiceline/internal/dsp"
	"voiceline/internal/model"
	"voiceline/internal/pipeline"
	"voiceline/internal/sample"
)

func generatedInput(t *testing.T) *pipeline.SegmentInput {
	t.Helper()
	dir := t.TempDir()
	if _, err := sample.Generate(sample.Options{OutDir: filepath.Join(dir, "samples")}); err != nil {
		t.Fatal(err)
	}
	input, err := pipeline.ReadSegmentInput(filepath.Join(dir, "samples", "manifest.json"), 0)
	if err != nil {
		t.Fatal(err)
	}
	return input
}

func hugeDispatch(recycle bool) dispatch.Config {
	return dispatch.Config{
		MaxGroups:        2,
		FramesPerGroup:   6,
		RoundBudgetNanos: int64(time.Hour),
		RecycleQuota:     recycle,
	}
}

func TestFrameTableDeterminismAndBusinessErrors(t *testing.T) {
	input := generatedInput(t)
	ctx := context.Background()

	table1, err := dsp.Analyze(input.Segment, input.Manifest.SampleRate, input.PCM.SampleRate, input.PCM.Samples, input.Fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	table2, err := dsp.Analyze(input.Segment, input.Manifest.SampleRate, input.PCM.SampleRate, input.PCM.Samples, input.Fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	b1, err := json.Marshal(table1)
	if err != nil {
		t.Fatal(err)
	}
	b2, _ := json.Marshal(table2)
	if string(b1) != string(b2) {
		t.Fatal("frame table JSON is not byte-identical")
	}
	if table1.FrameShiftSamples != 160 {
		t.Fatalf("frame shift conversion = %d, want 160", table1.FrameShiftSamples)
	}
	if table1.Summary.FrameCount != 45 {
		t.Fatalf("frame count = %d, want 45", table1.Summary.FrameCount)
	}

	_, err = dsp.Analyze(input.Segment, input.Manifest.SampleRate, 8000, input.PCM.Samples, input.Fingerprint)
	var business *model.BusinessError
	if !errors.As(err, &business) || business.Code != model.CodeSampleRateConflict || business.SegmentID != input.Segment.ID {
		t.Fatalf("expected sample rate conflict with segment id, got %v", err)
	}

	zeroShift := input.Segment
	zeroShift.FrameShiftMS = 0
	_, err = dsp.Analyze(zeroShift, input.Manifest.SampleRate, input.PCM.SampleRate, input.PCM.Samples, input.Fingerprint)
	if !errors.As(err, &business) || business.Code != model.CodeFrameShiftZero || business.SegmentID == "" {
		t.Fatalf("expected frame shift zero error, got %v", err)
	}

	zeroF0 := input.Segment
	zeroF0.F0Shift = 0
	_, err = pipeline.ConvertSegment(ctx, zeroF0, input.Manifest.SampleRate, input.PCM.SampleRate,
		input.PCM.Samples, input.Fingerprint, pipeline.ConvertOptions{Dispatch: hugeDispatch(false)})
	if !errors.As(err, &business) || business.Code != model.CodeParamInvalid || business.FrameIndex == nil || *business.FrameIndex != 0 {
		t.Fatalf("expected E_PARAM_INVALID with frame 0 for f0_shift=0, got %v", err)
	}

	badWarp := input.Segment
	badWarp.EnvelopeWarp = 3
	_, err = pipeline.ConvertSegment(ctx, badWarp, input.Manifest.SampleRate, input.PCM.SampleRate,
		input.PCM.Samples, input.Fingerprint, pipeline.ConvertOptions{Dispatch: hugeDispatch(false)})
	if !errors.As(err, &business) || business.Code != model.CodeParamInvalid || business.FrameIndex == nil {
		t.Fatalf("expected E_PARAM_INVALID with frame index for envelope warp, got %v", err)
	}
}

func TestSerialParallelStreamByteIdentity(t *testing.T) {
	input := generatedInput(t)
	ctx := context.Background()
	opts := pipeline.ConvertOptions{PolicyVersion: 1, Dispatch: hugeDispatch(false)}

	serial, err := pipeline.ConvertSegment(ctx, input.Segment, input.Manifest.SampleRate, input.PCM.SampleRate,
		input.PCM.Samples, input.Fingerprint, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !serial.Complete || serial.Summary.SampleCount != input.Segment.ExpectedSamplesUpperBound {
		t.Fatalf("serial complete=%v samples=%d expected=%d", serial.Complete, serial.Summary.SampleCount, input.Segment.ExpectedSamplesUpperBound)
	}

	parallelOpts := opts
	parallelOpts.Parallel = true
	parallel, err := pipeline.ConvertSegment(ctx, input.Segment, input.Manifest.SampleRate, input.PCM.SampleRate,
		input.PCM.Samples, input.Fingerprint, parallelOpts)
	if err != nil {
		t.Fatal(err)
	}
	recycleOpts := parallelOpts
	recycleOpts.Dispatch = hugeDispatch(true)
	recycled, err := pipeline.ConvertSegment(ctx, input.Segment, input.Manifest.SampleRate, input.PCM.SampleRate,
		input.PCM.Samples, input.Fingerprint, recycleOpts)
	if err != nil {
		t.Fatal(err)
	}

	for name, candidate := range map[string]*pipeline.ConversionResult{
		"parallel": parallel,
		"recycled": recycled,
	} {
		if string(candidate.PCMBytes) != string(serial.PCMBytes) {
			t.Fatalf("%s PCM differs from serial", name)
		}
		if string(candidate.WAVBytes) != string(serial.WAVBytes) {
			t.Fatalf("%s WAV differs from serial", name)
		}
		if string(candidate.SummaryJSON) != string(serial.SummaryJSON) {
			t.Fatalf("%s summary differs from serial", name)
		}
		serialFrames, err := json.Marshal(serial.Frames)
		if err != nil {
			t.Fatal(err)
		}
		candidateFrames, err := json.Marshal(candidate.Frames)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(candidateFrames, serialFrames) {
			t.Fatalf("%s frame F0/Mel sequence differs from serial", name)
		}
	}
	if len(recycled.Plan.Transfers) == 0 {
		t.Fatal("expected deterministic quota transfers in recycle plan")
	}

	stream, err := pipeline.NewStreamSession(ctx, input.Segment, input.Manifest.SampleRate, input.PCM.SampleRate,
		input.PCM.Samples, input.Fingerprint, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < stream.BlockCount(); i++ {
		if _, err := stream.PushBlock(ctx, i); err != nil {
			t.Fatal(err)
		}
	}
	streamResult, err := stream.Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := pipeline.VerifyConsistency(input.Segment.ID, streamResult, serial); err != nil {
		t.Fatalf("stream mismatch: %v", err)
	}

	bad := *streamResult
	bad.Frames = append([]pipeline.FrameSummary(nil), streamResult.Frames...)
	bad.Frames[2].MappedF0Hz += 1.5
	err = pipeline.VerifyConsistency(input.Segment.ID, &bad, serial)
	var business *model.BusinessError
	if !errors.As(err, &business) || business.Code != model.CodeStreamMismatch ||
		business.FirstDifferenceFrame == nil || *business.FirstDifferenceFrame != 2 {
		t.Fatalf("expected E_STREAM_MISMATCH at frame 2, got %v", err)
	}
}

func TestBudgetAdjudicationAndQuotaTransfer(t *testing.T) {
	input := generatedInput(t)
	table, err := dsp.Analyze(input.Segment, input.Manifest.SampleRate, input.PCM.SampleRate, input.PCM.Samples, input.Fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	blocks := dispatch.EstimateBlocks(input.Segment, table, 4)
	if len(blocks) != 12 {
		t.Fatalf("block count = %d, want 12", len(blocks))
	}
	budget := blocks[0].EstimatedNanos + blocks[1].EstimatedNanos + 12345
	plan, err := dispatch.PlanDispatch(input.Segment, table, dispatch.Config{
		MaxGroups: 2, FramesPerGroup: 8, RoundBudgetNanos: budget,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.ProcessableBlocks) != 2 || plan.ProcessableBlocks[0] != 0 || plan.ProcessableBlocks[1] != 1 {
		t.Fatalf("processable = %v, want [0 1]", plan.ProcessableBlocks)
	}
	if len(plan.Unprocessed) == 0 || plan.Unprocessed[0].Index != 2 {
		t.Fatalf("unprocessed = %+v, want first index 2", plan.Unprocessed)
	}
	if plan.Unprocessed[0].BudgetRemainingNanos != 12345 {
		t.Fatalf("budget remaining = %d, want 12345", plan.Unprocessed[0].BudgetRemainingNanos)
	}

	noRecycle, err := dispatch.PlanDispatch(input.Segment, table, hugeDispatch(false))
	if err != nil {
		t.Fatal(err)
	}
	recycle, err := dispatch.PlanDispatch(input.Segment, table, hugeDispatch(true))
	if err != nil {
		t.Fatal(err)
	}
	if len(noRecycle.Transfers) != 0 {
		t.Fatalf("no-recycle plan has transfers: %+v", noRecycle.Transfers)
	}
	if len(recycle.Transfers) != 4 {
		t.Fatalf("transfers = %d, want 4", len(recycle.Transfers))
	}
	for _, transfer := range recycle.Transfers {
		if transfer.Frames != 2 || transfer.Rule == "" {
			t.Fatalf("bad transfer detail: %+v", transfer)
		}
	}
}

func TestF0ToleranceErrorCode(t *testing.T) {
	input := generatedInput(t)
	table, err := dsp.Analyze(input.Segment, input.Manifest.SampleRate, input.PCM.SampleRate, input.PCM.Samples, input.Fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	strict := input.Segment
	strict.ReferenceF0MeanRange = [2]float64{210, 220}
	strict.F0ToleranceHz = 0.1
	strict.MinInRangeRatio = 1
	report, err := diagnostics.AlignFrameTable(strict, table)
	var business *model.BusinessError
	if !errors.As(err, &business) || business.Code != model.CodeToleranceExceeded {
		t.Fatalf("expected E_TOLERANCE_EXCEEDED, got report=%+v err=%v", report, err)
	}
	if business.MaxDeviation <= 0 {
		t.Fatalf("expected positive max deviation, got %+v", business)
	}
}

func TestClampCountRecorded(t *testing.T) {
	input := generatedInput(t)
	result, err := pipeline.ConvertSegment(context.Background(), input.Segment, input.Manifest.SampleRate,
		input.PCM.SampleRate, input.PCM.Samples, input.Fingerprint, pipeline.ConvertOptions{
			Dispatch:   hugeDispatch(false),
			OutputGain: 20,
		})
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary.ClampCount == 0 {
		t.Fatal("expected high output gain to produce and record clamps")
	}
}
