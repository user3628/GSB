package pipeline

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"voiceline/internal/canonical"
	"voiceline/internal/diagnostics"
	"voiceline/internal/dispatch"
	"voiceline/internal/dsp"
	"voiceline/internal/model"
	"voiceline/internal/policy"
	"voiceline/internal/wav"
)

type ConvertOptions struct {
	PolicyVersion int64
	Strategy      policy.Strategy
	Dispatch      dispatch.Config
	Parallel      bool
	WorkerID      string
	BlockDelay    time.Duration
	OutputGain    float64
}

type FrameSummary struct {
	Index         int     `json:"index"`
	SourceF0Hz    float64 `json:"source_f0_hz"`
	MappedF0Hz    float64 `json:"mapped_f0_hz"`
	Voiced        bool    `json:"voiced"`
	MelDigest     string  `json:"mel_digest"`
	MappingDigest string  `json:"mapping_digest"`
	ParamDigest   string  `json:"param_digest"`
}

type StableSummary struct {
	SegmentID           string                `json:"segment_id"`
	PolicyVersion       int64                 `json:"policy_version"`
	SampleRate          int                   `json:"sample_rate"`
	FrameShiftMS        float64               `json:"frame_shift_ms"`
	FrameShiftSamples   int                   `json:"frame_shift_samples"`
	FrameLengthSamples  int                   `json:"frame_length_samples"`
	BlockLengthFrames   int                   `json:"block_length_frames"`
	ExpectedSamples     int                   `json:"expected_samples"`
	SampleCount         int                   `json:"sample_count"`
	BlockCount          int                   `json:"block_count"`
	ClampCount          int                   `json:"clamp_count"`
	InputFingerprint    string                `json:"input_fingerprint"`
	TableDigest         string                `json:"table_digest"`
	MelEnergyDigest     string                `json:"mel_energy_digest"`
	FrameSequenceDigest string                `json:"frame_sequence_digest"`
	PCMDigest           string                `json:"pcm_digest"`
	WAVDigest           string                `json:"wav_digest"`
	F0Alignment         *diagnostics.F0Report `json:"f0_alignment,omitempty"`
}

type ConversionResult struct {
	Summary     StableSummary        `json:"summary"`
	SummaryJSON []byte               `json:"-"`
	PCMSamples  []int16              `json:"-"`
	PCMBytes    []byte               `json:"-"`
	WAVBytes    []byte               `json:"-"`
	Frames      []FrameSummary       `json:"frames"`
	Table       *dsp.FrameTable      `json:"-"`
	Mapped      []dsp.MappedFrame    `json:"-"`
	Rendered    []*dsp.RenderedBlock `json:"-"`
	Plan        *dispatch.Plan       `json:"plan,omitempty"`
	Complete    bool                 `json:"complete"`
}

func ResolveStrategy(segment model.SegmentManifest, strategy policy.Strategy) (model.SegmentManifest, dispatch.Config) {
	resolved := segment
	if strategy.FrameShiftMS != nil {
		resolved.FrameShiftMS = *strategy.FrameShiftMS
	}
	if strategy.BlockLength != nil {
		resolved.BlockLength = *strategy.BlockLength
		resolved.BlockFrames = *strategy.BlockLength
	}
	if strategy.F0Shift != nil {
		resolved.F0Shift = *strategy.F0Shift
	}
	if strategy.EnvelopeWarp != nil {
		resolved.EnvelopeWarp = *strategy.EnvelopeWarp
	}
	cfg := dispatch.Config{
		MaxGroups:        segment.MaxGroups,
		FramesPerGroup:   segment.FramesPerGroup,
		RoundBudgetNanos: segment.RoundBudgetNanos,
	}
	if strategy.MaxGroups != nil {
		cfg.MaxGroups = *strategy.MaxGroups
	}
	if strategy.FramesPerGroup != nil {
		cfg.FramesPerGroup = *strategy.FramesPerGroup
	}
	if strategy.RoundBudgetNanos != nil {
		cfg.RoundBudgetNanos = *strategy.RoundBudgetNanos
	}
	if strategy.RecycleQuota != nil {
		cfg.RecycleQuota = *strategy.RecycleQuota
	}
	cfg = cfg.Normalize(resolved)
	return resolved, cfg
}

func ConvertSegment(ctx context.Context, segment model.SegmentManifest, manifestSampleRate, wavSampleRate int, pcm []int16, inputFingerprint string, opts ConvertOptions) (*ConversionResult, error) {
	if opts.PolicyVersion == 0 {
		opts.PolicyVersion = opts.Strategy.Version
	}
	resolved, cfg := ResolveStrategy(segment, opts.Strategy)
	if opts.Dispatch.MaxGroups > 0 || opts.Dispatch.FramesPerGroup > 0 || opts.Dispatch.RoundBudgetNanos > 0 {
		cfg = opts.Dispatch.Normalize(resolved)
	}
	if opts.OutputGain == 0 && opts.Strategy.OutputGain != nil {
		opts.OutputGain = *opts.Strategy.OutputGain
	}

	table, err := dsp.Analyze(resolved, manifestSampleRate, wavSampleRate, pcm, inputFingerprint)
	if err != nil {
		return nil, err
	}
	mapped, err := dsp.MapParameters(resolved, table, resolved.F0Shift, resolved.EnvelopeWarp)
	if err != nil {
		return nil, err
	}

	blockFrames := resolved.EffectiveBlockFrames()
	plan, err := dispatch.PlanDispatch(resolved, table, cfg)
	if err != nil && plan == nil {
		return nil, err
	}
	if err != nil && len(plan.Unprocessed) == 0 {
		return nil, err
	}

	blockCount := dsp.BlockCount(table, blockFrames)
	rendered := make([]*dsp.RenderedBlock, blockCount)
	render := func(blockIndex int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if opts.BlockDelay > 0 {
			select {
			case <-time.After(opts.BlockDelay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		rb, err := dsp.RenderBlock(table, mapped, blockIndex, blockFrames, opts.OutputGain)
		if err != nil {
			return err
		}
		rendered[blockIndex] = rb
		return nil
	}

	if opts.Parallel {
		if err := runParallelGroups(ctx, plan, cfg.MaxGroups, render); err != nil {
			return nil, err
		}
	} else {
		for _, blockIndex := range plan.ProcessableBlocks {
			if err := render(blockIndex); err != nil {
				return nil, err
			}
		}
	}

	processable := map[int]bool{}
	for _, idx := range plan.ProcessableBlocks {
		processable[idx] = true
	}
	pcmSamples := make([]int16, 0, table.ExpectedSamples)
	pcmBytes := make([]byte, 0, table.ExpectedSamples*2)
	clamps := 0
	for blockIndex := 0; blockIndex < blockCount; blockIndex++ {
		if !processable[blockIndex] || rendered[blockIndex] == nil {
			continue
		}
		rb := rendered[blockIndex]
		pcmSamples = append(pcmSamples, rb.Samples...)
		pcmBytes = append(pcmBytes, int16ToBytesLocal(rb.Samples)...)
		clamps += rb.ClampCount
	}

	wavBytes, err := wav.EncodePCM16(pcmSamples, table.SampleRate, 1)
	if err != nil {
		return nil, err
	}
	frameSummaries := make([]FrameSummary, 0, len(table.Frames))
	for i, frame := range table.Frames {
		frameSummaries = append(frameSummaries, FrameSummary{
			Index:         frame.Index,
			SourceF0Hz:    frame.F0Hz,
			MappedF0Hz:    mapped[i].MappedF0Hz,
			Voiced:        frame.Voiced,
			MelDigest:     frame.MelDigest,
			MappingDigest: mapped[i].MappingDigest,
			ParamDigest:   frame.ParamDigest,
		})
	}
	frameDigest, _, err := canonical.JSON(frameSummaries)
	if err != nil {
		return nil, err
	}

	complete := len(plan.Unprocessed) == 0 && len(pcmSamples) == table.ExpectedSamples
	summary := StableSummary{
		SegmentID:           segment.ID,
		PolicyVersion:       opts.PolicyVersion,
		SampleRate:          table.SampleRate,
		FrameShiftMS:        table.FrameShiftMS,
		FrameShiftSamples:   table.FrameShiftSamples,
		FrameLengthSamples:  table.FrameLengthSamples,
		BlockLengthFrames:   blockFrames,
		ExpectedSamples:     table.ExpectedSamples,
		SampleCount:         len(pcmSamples),
		BlockCount:          blockCount,
		ClampCount:          clamps,
		InputFingerprint:    inputFingerprint,
		TableDigest:         table.Summary.ParamTableDigest,
		MelEnergyDigest:     table.Summary.MelEnergyDigest,
		FrameSequenceDigest: canonical.Bytes(frameDigest),
		PCMDigest:           canonical.Bytes(pcmBytes),
		WAVDigest:           canonical.Bytes(wavBytes),
	}
	if report, err := diagnostics.AlignFrameTable(resolved, table); err == nil {
		summary.F0Alignment = report
	} else if business, ok := model.AsBusinessError(err); ok && business.Code == model.CodeToleranceExceeded {
		report, _ := err.(*model.BusinessError)
		_ = report
		// Conversion remains useful; callers that require truth alignment get ValidateF0.
	} else {
		return nil, err
	}
	summaryJSON, _, err := canonical.JSON(summary)
	if err != nil {
		return nil, err
	}

	return &ConversionResult{
		Summary:     summary,
		SummaryJSON: summaryJSON,
		PCMSamples:  pcmSamples,
		PCMBytes:    pcmBytes,
		WAVBytes:    wavBytes,
		Frames:      frameSummaries,
		Table:       table,
		Mapped:      mapped,
		Rendered:    rendered,
		Plan:        plan,
		Complete:    complete,
	}, nil
}

func ValidateF0(segment model.SegmentManifest, result *ConversionResult) (*diagnostics.F0Report, error) {
	if result == nil || result.Table == nil {
		return nil, fmt.Errorf("conversion result is required for F0 validation")
	}
	return diagnostics.AlignFrameTable(segment, result.Table)
}

func runParallelGroups(ctx context.Context, plan *dispatch.Plan, maxConcurrency int, render func(int) error) error {
	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}
	semaphore := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	var active int32
	var observed int32

	for _, groupValue := range plan.Groups {
		group := groupValue
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case semaphore <- struct{}{}:
			case <-ctx.Done():
				once.Do(func() { firstErr = ctx.Err() })
				return
			}
			current := atomic.AddInt32(&active, 1)
			for {
				old := atomic.LoadInt32(&observed)
				if current <= old || atomic.CompareAndSwapInt32(&observed, old, current) {
					break
				}
			}
			defer func() {
				atomic.AddInt32(&active, -1)
				<-semaphore
			}()

			for _, blockIndex := range group.BlockIndexes {
				if err := ctx.Err(); err != nil {
					once.Do(func() { firstErr = err })
					return
				}
				if err := render(blockIndex); err != nil {
					once.Do(func() { firstErr = err })
					return
				}
			}
		}()
	}
	wg.Wait()
	if observed > 0 {
		plan.ObservedConcurrency = int(observed)
	}
	return firstErr
}

func int16ToBytesLocal(samples []int16) []byte {
	b := make([]byte, len(samples)*2)
	for i, sample := range samples {
		b[2*i] = byte(sample)
		b[2*i+1] = byte(uint16(sample) >> 8)
	}
	return b
}
