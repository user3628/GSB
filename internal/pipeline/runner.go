package pipeline

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"voiceline/internal/canonical"
	"voiceline/internal/checkpoint"
	"voiceline/internal/diagnostics"
	"voiceline/internal/dispatch"
	"voiceline/internal/dsp"
	"voiceline/internal/model"
	"voiceline/internal/policy"
	"voiceline/internal/wav"
)

var ErrGracefulStop = errors.New("graceful stop requested after indivisible block")

type StopController struct {
	once sync.Once
	ch   chan struct{}
}

func NewStopController() *StopController {
	return &StopController{ch: make(chan struct{})}
}

func (s *StopController) RequestStop() {
	if s == nil {
		return
	}
	s.once.Do(func() { close(s.ch) })
}

func (s *StopController) Stopped() bool {
	if s == nil {
		return false
	}
	select {
	case <-s.ch:
		return true
	default:
		return false
	}
}

type RunOptions struct {
	Store      *checkpoint.Store
	OutputDir  string
	Strategy   policy.Strategy
	Parallel   bool
	WorkerID   string
	BlockDelay time.Duration
	Stop       *StopController
}

type SegmentReport struct {
	InputPath             string
	OutputWAVPath         string
	OutputPCMPath         string
	FrameTablePath        string
	SummaryPath           string
	AnalysisCheckpoint    *checkpoint.Checkpoint
	DispatchCheckpoint    *checkpoint.Checkpoint
	ReconstructCheckpoint *checkpoint.Checkpoint
	ValidationCheckpoint  *checkpoint.Checkpoint
	F0Report              *diagnostics.F0Report
	Plan                  *dispatch.Plan
	Result                *ConversionResult
	PublishedBlocks       int
	RenderedBlocks        int
	ReusedBlocks          int
	Stopped               bool
}

type blockOutcome struct {
	rendered *dsp.RenderedBlock
	err      error
}

func RunSegment(ctx context.Context, input *SegmentInput, opts RunOptions) (*SegmentReport, error) {
	if input == nil {
		return nil, fmt.Errorf("segment input is required")
	}
	if opts.Store == nil {
		return nil, fmt.Errorf("SQLite checkpoint store is required")
	}
	if opts.WorkerID == "" {
		opts.WorkerID = "worker-local"
	}
	policyVersion := opts.Strategy.Version
	if policyVersion == 0 {
		policyVersion = 1
	}
	resolved, cfg := ResolveStrategy(input.Segment, opts.Strategy)
	table, err := dsp.Analyze(resolved, input.Manifest.SampleRate, input.PCM.SampleRate, input.PCM.Samples, input.Fingerprint)
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

	tableJSON, err := json.MarshalIndent(table, "", "  ")
	if err != nil {
		return nil, err
	}
	analysisCP, err := opts.Store.PutCheckpoint(ctx, checkpoint.Checkpoint{
		SegmentID:        input.Segment.ID,
		Stage:            "analysis",
		InputFingerprint: input.Fingerprint,
		PolicyVersion:    policyVersion,
		ArtifactDigest:   table.Summary.ParamTableDigest,
		ArtifactBlob:     tableJSON,
	})
	if err != nil {
		return nil, err
	}

	if err := opts.Store.EnsureShard(ctx, input.Segment.ID); err != nil {
		return nil, err
	}
	cursor, err := opts.Store.NextBlockIndex(ctx, input.Segment.ID)
	if err != nil {
		return nil, err
	}
	blockCount := dsp.BlockCount(table, blockFrames)
	if cursor > blockCount {
		return nil, model.NewError(model.CodeStaleCheckpoint, input.Segment.ID,
			"committed cursor %d is beyond deterministic block count %d", cursor, blockCount)
	}

	report := &SegmentReport{Plan: plan, AnalysisCheckpoint: analysisCP}
	if opts.OutputDir != "" {
		if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
			return nil, err
		}
		report.FrameTablePath = filepath.Join(opts.OutputDir, input.Segment.ID+".frames.json")
		if err := os.WriteFile(report.FrameTablePath, tableJSON, 0o644); err != nil {
			return nil, err
		}
	}

	outcomes := make([]chan blockOutcome, blockCount)
	for i := range outcomes {
		outcomes[i] = make(chan blockOutcome, 1)
	}
	var workers sync.WaitGroup
	renderOne := func(blockIndex int) blockOutcome {
		if opts.Stop != nil && opts.Stop.Stopped() {
			return blockOutcome{err: ErrGracefulStop}
		}
		if opts.BlockDelay > 0 {
			// Delay is not selectable on Stop: the indivisible block runs to completion.
			time.Sleep(opts.BlockDelay)
		}
		rb, renderErr := dsp.RenderBlock(table, mapped, blockIndex, blockFrames, strategyOutputGain(opts.Strategy))
		return blockOutcome{rendered: rb, err: renderErr}
	}

	if opts.Parallel {
		maxGroups := cfg.MaxGroups
		if maxGroups <= 0 {
			maxGroups = 1
		}
		semaphore := make(chan struct{}, maxGroups)
		for _, groupValue := range plan.Groups {
			group := groupValue
			workers.Add(1)
			go func() {
				defer workers.Done()
				semaphore <- struct{}{}
				defer func() { <-semaphore }()
				for _, blockIndex := range group.BlockIndexes {
					if blockIndex < cursor {
						continue
					}
					outcome := renderOne(blockIndex)
					outcomes[blockIndex] <- outcome
					if outcome.err != nil || (opts.Stop != nil && opts.Stop.Stopped()) {
						return
					}
				}
			}()
		}
	} else {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for blockIndex := cursor; blockIndex < blockCount; blockIndex++ {
				outcome := renderOne(blockIndex)
				outcomes[blockIndex] <- outcome
				if outcome.err != nil || (opts.Stop != nil && opts.Stop.Stopped()) {
					return
				}
			}
		}()
	}

	publishFrom := cursor
	stopped := false
	var runErr error
	for blockIndex := cursor; blockIndex < blockCount; blockIndex++ {
		var outcome blockOutcome
		select {
		case outcome = <-outcomes[blockIndex]:
		case <-ctx.Done():
			runErr = ctx.Err()
		}
		if runErr != nil {
			break
		}
		if outcome.err != nil {
			if errors.Is(outcome.err, ErrGracefulStop) {
				stopped = true
			} else {
				runErr = outcome.err
				break
			}
		}
		if outcome.rendered == nil {
			stopped = true
		} else {
			if err := publishRenderedBlock(ctx, opts.Store, input.Segment.ID, policyVersion, opts.WorkerID, outcome.rendered, mapped); err != nil {
				runErr = err
				break
			}
			report.PublishedBlocks++
			report.RenderedBlocks++
			publishFrom = blockIndex + 1
		}

		if opts.Stop != nil && opts.Stop.Stopped() {
			stopped = true
			break
		}
	}

	// Wait for all in-flight indivisible blocks, then publish any contiguous
	// completed facts before declaring a graceful stop.
	workers.Wait()
	if stopped || runErr != nil {
	drain:
		for blockIndex := publishFrom; blockIndex < blockCount; blockIndex++ {
			select {
			case outcome := <-outcomes[blockIndex]:
				if outcome.err != nil || outcome.rendered == nil {
					stopped = true
					break drain
				}
				if err := publishRenderedBlock(ctx, opts.Store, input.Segment.ID, policyVersion, opts.WorkerID, outcome.rendered, mapped); err != nil {
					if runErr == nil {
						runErr = err
					}
					break drain
				}
				report.PublishedBlocks++
				report.RenderedBlocks++
				publishFrom = blockIndex + 1
			default:
				stopped = true
				break drain
			}
		}
	}
	if runErr != nil {
		return report, runErr
	}
	if stopped || opts.Stop != nil && opts.Stop.Stopped() {
		report.Stopped = true
		return report, ErrGracefulStop
	}

	artifacts, err := opts.Store.ListBlockArtifacts(ctx, input.Segment.ID)
	if err != nil {
		return report, err
	}
	if len(artifacts) != blockCount {
		return report, fmt.Errorf("committed block artifact count %d does not match %d", len(artifacts), blockCount)
	}
	report.ReusedBlocks = blockCount - report.RenderedBlocks

	pcmBytes := make([]byte, 0, table.ExpectedSamples*2)
	clamps := 0
	for _, artifact := range artifacts {
		pcmBytes = append(pcmBytes, artifact.PCM...)
		clamps += artifact.ClampCount
	}
	pcmSamples := bytesToInt16LE(pcmBytes)
	wavBytes, err := wav.EncodePCM16(pcmSamples, table.SampleRate, 1)
	if err != nil {
		return report, err
	}
	frames := make([]FrameSummary, 0, len(table.Frames))
	for i, frame := range table.Frames {
		frames = append(frames, FrameSummary{
			Index:         frame.Index,
			SourceF0Hz:    frame.F0Hz,
			MappedF0Hz:    mapped[i].MappedF0Hz,
			Voiced:        frame.Voiced,
			MelDigest:     frame.MelDigest,
			MappingDigest: mapped[i].MappingDigest,
			ParamDigest:   frame.ParamDigest,
		})
	}
	frameBytes, _, _ := canonical.JSON(frames)
	f0Report, f0Err := diagnostics.AlignFrameTable(resolved, table)
	summary := StableSummary{
		SegmentID:           input.Segment.ID,
		PolicyVersion:       policyVersion,
		SampleRate:          table.SampleRate,
		FrameShiftMS:        table.FrameShiftMS,
		FrameShiftSamples:   table.FrameShiftSamples,
		FrameLengthSamples:  table.FrameLengthSamples,
		BlockLengthFrames:   blockFrames,
		ExpectedSamples:     table.ExpectedSamples,
		SampleCount:         len(pcmSamples),
		BlockCount:          blockCount,
		ClampCount:          clamps,
		InputFingerprint:    input.Fingerprint,
		TableDigest:         table.Summary.ParamTableDigest,
		MelEnergyDigest:     table.Summary.MelEnergyDigest,
		FrameSequenceDigest: canonical.Bytes(frameBytes),
		PCMDigest:           canonical.Bytes(pcmBytes),
		WAVDigest:           canonical.Bytes(wavBytes),
	}
	if f0Err == nil {
		summary.F0Alignment = f0Report
	}
	summaryJSON, _, err := canonical.JSON(summary)
	if err != nil {
		return report, err
	}
	result := &ConversionResult{
		Summary:     summary,
		SummaryJSON: summaryJSON,
		PCMSamples:  pcmSamples,
		PCMBytes:    pcmBytes,
		WAVBytes:    wavBytes,
		Frames:      frames,
		Table:       table,
		Mapped:      mapped,
		Plan:        plan,
		Complete:    len(pcmSamples) == table.ExpectedSamples,
	}
	report.Result = result

	planJSON := canonical.MustJSON(plan)
	dispatchCP, err := opts.Store.PutCheckpoint(ctx, checkpoint.Checkpoint{
		SegmentID:        input.Segment.ID,
		Stage:            "dispatch",
		InputFingerprint: input.Fingerprint,
		PolicyVersion:    policyVersion,
		ArtifactDigest:   canonical.Bytes(planJSON),
		ArtifactBlob:     planJSON,
	})
	if err != nil {
		return report, err
	}
	report.DispatchCheckpoint = dispatchCP

	reconstructCP, err := opts.Store.PutCheckpoint(ctx, checkpoint.Checkpoint{
		SegmentID:        input.Segment.ID,
		Stage:            "reconstruction",
		InputFingerprint: input.Fingerprint,
		PolicyVersion:    policyVersion,
		ArtifactDigest:   summary.WAVDigest,
		ArtifactBlob:     summaryJSON,
	})
	if err != nil {
		return report, err
	}
	report.ReconstructCheckpoint = reconstructCP

	if opts.OutputDir != "" {
		report.OutputWAVPath = filepath.Join(opts.OutputDir, input.Segment.ID+".wav")
		report.OutputPCMPath = filepath.Join(opts.OutputDir, input.Segment.ID+".pcm")
		report.SummaryPath = filepath.Join(opts.OutputDir, input.Segment.ID+".summary.json")
		if err := os.WriteFile(report.OutputWAVPath, wavBytes, 0o644); err != nil {
			return report, err
		}
		if err := os.WriteFile(report.OutputPCMPath, pcmBytes, 0o644); err != nil {
			return report, err
		}
		if err := os.WriteFile(report.SummaryPath, append(summaryJSON, '\n'), 0o644); err != nil {
			return report, err
		}
	}

	report.F0Report = f0Report
	if f0Err != nil {
		return report, f0Err
	}
	validationJSON := canonical.MustJSON(f0Report)
	validationCP, err := opts.Store.PutCheckpoint(ctx, checkpoint.Checkpoint{
		SegmentID:        input.Segment.ID,
		Stage:            "validation",
		InputFingerprint: input.Fingerprint,
		PolicyVersion:    policyVersion,
		ArtifactDigest:   canonical.Bytes(validationJSON),
		ArtifactBlob:     validationJSON,
	})
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return report, err
	}
	report.ValidationCheckpoint = validationCP
	return report, nil
}

func publishRenderedBlock(ctx context.Context, store *checkpoint.Store, segmentID string, policyVersion int64, workerID string, rendered *dsp.RenderedBlock, mapped []dsp.MappedFrame) error {
	pcm := int16ToBytesLocal(rendered.Samples)
	frameLo := rendered.FrameStart
	frameHi := rendered.FrameEnd
	if frameHi > len(mapped) {
		frameHi = len(mapped)
	}
	digests := make([]string, 0, frameHi-frameLo)
	for i := frameLo; i < frameHi; i++ {
		digests = append(digests, mapped[i].MappingDigest)
	}
	frameDigest, _, err := canonical.JSON(digests)
	if err != nil {
		return err
	}
	_, err = store.PublishBlock(ctx, checkpoint.BlockPublication{
		SegmentID:     segmentID,
		BlockIndex:    rendered.Index,
		WorkerID:      workerID,
		PolicyVersion: policyVersion,
		PCM:           pcm,
		Digest:        canonical.Bytes(pcm),
		FrameDigest:   canonical.Bytes(frameDigest),
		ClampCount:    rendered.ClampCount,
	})
	return err
}

func bytesToInt16LE(b []byte) []int16 {
	out := make([]int16, len(b)/2)
	for i := range out {
		out[i] = int16(binary.LittleEndian.Uint16(b[i*2 : i*2+2]))
	}
	return out
}

func strategyOutputGain(strategy policy.Strategy) float64 {
	if strategy.OutputGain == nil {
		return 0
	}
	return *strategy.OutputGain
}
