package pipeline

import (
	"context"
	"fmt"

	"voiceline/internal/canonical"
	"voiceline/internal/diagnostics"
	"voiceline/internal/dsp"
	"voiceline/internal/model"
	"voiceline/internal/wav"
)

type StreamChunk struct {
	BlockIndex  int
	FrameStart  int
	FrameEnd    int
	SampleStart int
	SampleEnd   int
	Samples     []int16
	PCMBytes    []byte
	ClampCount  int
}

type StreamSession struct {
	segment     model.SegmentManifest
	rate        int
	inputFP     string
	opts        ConvertOptions
	table       *dsp.FrameTable
	mapped      []dsp.MappedFrame
	plan        interface{}
	blockFrames int
	blockCount  int
	nextBlock   int
	chunks      []StreamChunk
	samples     []int16
	pcmBytes    []byte
	clamps      int
}

func NewStreamSession(ctx context.Context, segment model.SegmentManifest, manifestRate, wavRate int, pcm []int16, inputFingerprint string, opts ConvertOptions) (*StreamSession, error) {
	resolved, _ := ResolveStrategy(segment, opts.Strategy)
	if opts.PolicyVersion == 0 {
		opts.PolicyVersion = opts.Strategy.Version
	}
	table, err := dsp.Analyze(resolved, manifestRate, wavRate, pcm, inputFingerprint)
	if err != nil {
		return nil, err
	}
	mapped, err := dsp.MapParameters(resolved, table, resolved.F0Shift, resolved.EnvelopeWarp)
	if err != nil {
		return nil, err
	}
	return &StreamSession{
		segment:     resolved,
		rate:        manifestRate,
		inputFP:     inputFingerprint,
		opts:        opts,
		table:       table,
		mapped:      mapped,
		blockFrames: resolved.EffectiveBlockFrames(),
		blockCount:  dsp.BlockCount(table, resolved.EffectiveBlockFrames()),
		chunks:      make([]StreamChunk, 0),
		samples:     make([]int16, 0, table.ExpectedSamples),
		pcmBytes:    make([]byte, 0, table.ExpectedSamples*2),
	}, nil
}

func (s *StreamSession) BlockCount() int { return s.blockCount }

// PushBlock delivers the next deterministic frame block and returns its PCM.
func (s *StreamSession) PushBlock(ctx context.Context, blockIndex int) (*StreamChunk, error) {
	if blockIndex != s.nextBlock {
		return nil, model.NewError(model.CodeStreamMismatch, s.segment.ID,
			"stream block order mismatch: expected block %d, got %d", s.nextBlock, blockIndex)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rendered, err := dsp.RenderBlock(s.table, s.mapped, blockIndex, s.blockFrames, s.opts.OutputGain)
	if err != nil {
		return nil, err
	}
	chunk := StreamChunk{
		BlockIndex:  blockIndex,
		FrameStart:  rendered.FrameStart,
		FrameEnd:    rendered.FrameEnd,
		SampleStart: rendered.SampleStart,
		SampleEnd:   rendered.SampleEnd,
		Samples:     rendered.Samples,
		PCMBytes:    int16ToBytesLocal(rendered.Samples),
		ClampCount:  rendered.ClampCount,
	}
	s.chunks = append(s.chunks, chunk)
	s.samples = append(s.samples, chunk.Samples...)
	s.pcmBytes = append(s.pcmBytes, chunk.PCMBytes...)
	s.clamps += chunk.ClampCount
	s.nextBlock++
	return &chunk, nil
}

func (s *StreamSession) Result() (*ConversionResult, error) {
	if s.nextBlock != s.blockCount {
		return nil, fmt.Errorf("stream incomplete: %d/%d blocks", s.nextBlock, s.blockCount)
	}
	wavBytes, err := wav.EncodePCM16(s.samples, s.table.SampleRate, 1)
	if err != nil {
		return nil, err
	}
	frames := make([]FrameSummary, 0, len(s.table.Frames))
	for i, frame := range s.table.Frames {
		frames = append(frames, FrameSummary{
			Index:         frame.Index,
			SourceF0Hz:    frame.F0Hz,
			MappedF0Hz:    s.mapped[i].MappedF0Hz,
			Voiced:        frame.Voiced,
			MelDigest:     frame.MelDigest,
			MappingDigest: s.mapped[i].MappingDigest,
			ParamDigest:   frame.ParamDigest,
		})
	}
	frameBytes, _, _ := canonical.JSON(frames)
	summary := StableSummary{
		SegmentID:           s.segment.ID,
		PolicyVersion:       s.opts.PolicyVersion,
		SampleRate:          s.table.SampleRate,
		FrameShiftMS:        s.table.FrameShiftMS,
		FrameShiftSamples:   s.table.FrameShiftSamples,
		FrameLengthSamples:  s.table.FrameLengthSamples,
		BlockLengthFrames:   s.blockFrames,
		ExpectedSamples:     s.table.ExpectedSamples,
		SampleCount:         len(s.samples),
		BlockCount:          s.blockCount,
		ClampCount:          s.clamps,
		InputFingerprint:    s.inputFP,
		TableDigest:         s.table.Summary.ParamTableDigest,
		MelEnergyDigest:     s.table.Summary.MelEnergyDigest,
		FrameSequenceDigest: canonical.Bytes(frameBytes),
		PCMDigest:           canonical.Bytes(s.pcmBytes),
		WAVDigest:           canonical.Bytes(wavBytes),
	}
	if report, err := diagnostics.AlignFrameTable(s.segment, s.table); err == nil {
		summary.F0Alignment = report
	}
	summaryJSON, _, _ := canonical.JSON(summary)
	return &ConversionResult{
		Summary:     summary,
		SummaryJSON: summaryJSON,
		PCMSamples:  s.samples,
		PCMBytes:    s.pcmBytes,
		WAVBytes:    wavBytes,
		Frames:      frames,
		Table:       s.table,
		Mapped:      s.mapped,
		Complete:    len(s.samples) == s.table.ExpectedSamples,
	}, nil
}

func VerifyConsistency(segmentID string, streamed, whole *ConversionResult) error {
	if streamed == nil || whole == nil {
		return model.NewError(model.CodeStreamMismatch, segmentID, "stream and whole-segment results are required")
	}

	frameCount := len(streamed.Frames)
	if len(whole.Frames) < frameCount {
		frameCount = len(whole.Frames)
	}
	for i := 0; i < frameCount; i++ {
		a := streamed.Frames[i]
		b := whole.Frames[i]
		if a.MappedF0Hz != b.MappedF0Hz || a.MelDigest != b.MelDigest || a.MappingDigest != b.MappingDigest {
			index := i
			return &model.BusinessError{
				Code:                 model.CodeStreamMismatch,
				SegmentID:            segmentID,
				FirstDifferenceFrame: &index,
				Message:              "stream frame parameters differ from whole-segment conversion",
				Expected: map[string]any{
					"mapped_f0_hz": b.MappedF0Hz,
					"mel_digest":   b.MelDigest,
				},
				Actual: map[string]any{
					"mapped_f0_hz": a.MappedF0Hz,
					"mel_digest":   a.MelDigest,
				},
				Details: map[string]any{"frame_index": i},
			}
		}
	}

	if len(streamed.PCMBytes) != len(whole.PCMBytes) {
		index := minIntFrame(frameCount, maxIntFrame(0, frameCount-1))
		return streamMismatchError(segmentID, index, "sample byte length",
			len(whole.PCMBytes), len(streamed.PCMBytes))
	}
	for i := range whole.PCMBytes {
		if streamed.PCMBytes[i] != whole.PCMBytes[i] {
			sample := i / 2
			frame := 0
			if whole.Summary.FrameShiftSamples > 0 {
				frame = sample / whole.Summary.FrameShiftSamples
			}
			expectedSample := int16(0)
			actualSample := int16(0)
			if sample < len(whole.PCMSamples) {
				expectedSample = whole.PCMSamples[sample]
			}
			if sample < len(streamed.PCMSamples) {
				actualSample = streamed.PCMSamples[sample]
			}
			return streamMismatchError(segmentID, frame, "PCM byte",
				map[string]any{"byte_index": i, "sample_index": sample, "sample": expectedSample},
				map[string]any{"byte_index": i, "sample_index": sample, "sample": actualSample})
		}
	}

	if streamed.Summary.SampleCount != whole.Summary.SampleCount ||
		streamed.Summary.FrameSequenceDigest != whole.Summary.FrameSequenceDigest ||
		streamed.Summary.MelEnergyDigest != whole.Summary.MelEnergyDigest {
		frame := maxIntFrame(0, frameCount-1)
		return streamMismatchError(segmentID, frame, "summary digest", whole.Summary, streamed.Summary)
	}
	return nil
}

func streamMismatchError(segmentID string, frame int, kind string, expected, actual any) error {
	f := frame
	return &model.BusinessError{
		Code:                 model.CodeStreamMismatch,
		SegmentID:            segmentID,
		FirstDifferenceFrame: &f,
		Message:              "streaming output is not byte-identical to whole-segment conversion at " + kind,
		Expected:             expected,
		Actual:               actual,
		Details:              map[string]any{"kind": kind},
	}
}

func minIntFrame(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxIntFrame(a, b int) int {
	if a > b {
		return a
	}
	return b
}
