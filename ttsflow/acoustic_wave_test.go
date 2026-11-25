package ttsflow

import (
	"bytes"
	"math"
	"testing"
)

func TestAcousticPredictionInvariantAcrossSplits(t *testing.T) {
	cfg, _, samples := testDataset(t)
	sample := samples[0]
	variants := []SplitConfig{
		{FirstMinChars: 1, FollowingChars: 1, ForceAtPunctuation: true, CheckpointIntervalFrames: 1},
		{FirstMinChars: 3, FollowingChars: 4, ForceAtPunctuation: false, CheckpointIntervalFrames: 2},
		{FirstMinChars: 17, FollowingChars: 11, ForceAtPunctuation: true, CheckpointIntervalFrames: 3},
	}
	var reference []byte
	var referenceFrames int
	for i, split := range variants {
		cfg.Splitting = split
		frontend, err := BuildFrontend(sample, cfg)
		if err != nil {
			t.Fatalf("variant %d frontend: %v", i, err)
		}
		segments, encoded, err := PredictFrames(sample, frontend.Units, cfg, 0)
		if err != nil {
			t.Fatalf("variant %d acoustic: %v", i, err)
		}
		frames := 0
		for _, segment := range segments {
			frames += len(segment.Frames)
		}
		if i == 0 {
			reference = encoded
			referenceFrames = frames
			continue
		}
		if !bytes.Equal(encoded, reference) {
			t.Fatalf("variant %d acoustic bytes differ from reference", i)
		}
		if frames != referenceFrames {
			t.Fatalf("variant %d frame count = %d, want %d", i, frames, referenceFrames)
		}
	}
}

func TestNumericGuardReportsFrame(t *testing.T) {
	frames := []AcousticFrame{{Energies: []float64{1, math.NaN()}, F0: 120}}
	_, err := encodeAcousticFrames(frames, "numeric-utt", 7, 41)
	be, ok := AsBizError(err)
	if !ok || be.Code != CodeNumeric || be.UTTID != "numeric-utt" || be.FrameNo != 41 {
		t.Fatalf("numeric error = %#v", err)
	}
	frames = []AcousticFrame{{Energies: []float64{1, 2}, F0: math.Inf(1)}}
	_, err = encodeAcousticFrames(frames, "numeric-inf", 0, 3)
	be, ok = AsBizError(err)
	if !ok || be.Code != CodeNumeric || be.FrameNo != 3 {
		t.Fatalf("inf error = %#v", err)
	}
}

func TestWaveLengthAndChunkAlignment(t *testing.T) {
	cfg, _, samples := testDataset(t)
	sample := samples[0]
	frontend, err := BuildFrontend(sample, cfg)
	if err != nil {
		t.Fatal(err)
	}
	acoustic, _, err := PredictFrames(sample, frontend.Units, cfg, 0)
	if err != nil {
		t.Fatal(err)
	}
	waves, pcm, err := SynthesizeWave(sample, acoustic, cfg)
	if err != nil {
		t.Fatal(err)
	}
	frames := 0
	for _, segment := range acoustic {
		frames += len(segment.Frames)
	}
	if got, want := len(pcm), frames*cfg.Acoustic.FrameShiftSamples*cfg.Wave.Channels*2; got != want {
		t.Fatalf("PCM bytes = %d, want %d", got, want)
	}
	for _, wave := range waves {
		if err := ValidatePCMArtifact(sample.UTTID, wave.SegmentSeq, wave.FrameCount, cfg.Acoustic.FrameShiftSamples, cfg.Wave.Channels, wave.Bytes); err != nil {
			t.Fatal(err)
		}
	}
	alignments := buildChunkAlignments(frontend, waves)
	if len(alignments) != len(frontend.Chunks) {
		t.Fatalf("alignments = %d, chunks = %d", len(alignments), len(frontend.Chunks))
	}
	if alignments[0].SampleStart != 0 || alignments[len(alignments)-1].SampleEnd != frames*cfg.Acoustic.FrameShiftSamples {
		t.Fatalf("alignment interval does not cover all samples: %#v", alignments)
	}
}

func TestPCMLengthMismatchBusinessCode(t *testing.T) {
	err := ValidatePCMArtifact("length-utt", 9, 3, 160, 1, make([]byte, 10))
	be, ok := AsBizError(err)
	if !ok || be.Code != CodeLengthMismatch || be.UTTID != "length-utt" || be.SegmentSeq != 9 {
		t.Fatalf("length error = %#v", err)
	}
	if be.DeclaredValue != 480 || be.ActualValue != 5 {
		t.Fatalf("declared/actual = %d/%d", be.DeclaredValue, be.ActualValue)
	}
}
