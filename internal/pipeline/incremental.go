package pipeline

import (
	"context"
	"fmt"

	"voiceline/internal/canonical"
	"voiceline/internal/diagnostics"
	"voiceline/internal/dispatch"
	"voiceline/internal/dsp"
	"voiceline/internal/policy"
	"voiceline/internal/wav"
)

type cacheEntry struct {
	fingerprint policy.BlockFingerprint
	samples     []int16
	pcm         []byte
	clampCount  int
}

type RebuildStats struct {
	PolicyVersion      int64    `json:"policy_version"`
	ChangedFields      []string `json:"changed_fields"`
	Decision           string   `json:"decision"`
	EarliestStage      string   `json:"earliest_stage"`
	EarliestBlock      int      `json:"earliest_block"`
	RebuildFromBlock   int      `json:"rebuild_from_block"`
	RebuildToBlock     int      `json:"rebuild_to_block"`
	BlockCount         int      `json:"block_count"`
	ReusedBlocks       []int    `json:"reused_blocks"`
	RebuiltBlocks      []int    `json:"rebuilt_blocks"`
	ReusedAnalysis     bool     `json:"reused_analysis"`
	ReusedMapping      bool     `json:"reused_mapping"`
	VersionFingerprint string   `json:"version_fingerprint"`
}

type IncrementalEngine struct {
	input        *SegmentInput
	options      ConvertOptions
	policies     *policy.Store
	strategy     policy.Strategy
	table        *dsp.FrameTable
	mapped       []dsp.MappedFrame
	cache        map[int]cacheEntry
	fingerprints []policy.BlockFingerprint
	result       *ConversionResult
}

func NewIncrementalEngine(input *SegmentInput, initial policy.Strategy, options ConvertOptions) *IncrementalEngine {
	if initial.LineageID == "" {
		initial.LineageID = "incremental"
	}
	if initial.Version == 0 {
		initial.Version = 1
	}
	return &IncrementalEngine{
		input:    input,
		options:  options,
		policies: policy.NewStore(initial),
		strategy: initial,
		cache:    make(map[int]cacheEntry),
	}
}

func (e *IncrementalEngine) CurrentPolicy() policy.Strategy { return e.policies.Current() }

func (e *IncrementalEngine) ApplyPatch(patch policy.Patch) (policy.Strategy, policy.Invalidation, error) {
	next, invalidation, err := e.policies.ApplyPatch(patch)
	if err != nil {
		return policy.Strategy{}, policy.Invalidation{}, err
	}
	e.strategy = next
	return next, invalidation, nil
}

func (e *IncrementalEngine) Rebuild(ctx context.Context) (*ConversionResult, *RebuildStats, error) {
	resolved, cfg := ResolveStrategy(e.input.Segment, e.strategy)
	policyVersion := e.strategy.Version
	stats := &RebuildStats{PolicyVersion: policyVersion, EarliestBlock: -1}

	analysisChanged := e.table == nil
	if e.table != nil && e.table.FrameShiftMS != resolved.FrameShiftMS {
		analysisChanged = true
	}
	if analysisChanged {
		table, err := dsp.Analyze(resolved, e.input.Manifest.SampleRate, e.input.PCM.SampleRate,
			e.input.PCM.Samples, e.input.Fingerprint)
		if err != nil {
			return nil, nil, err
		}
		e.table = table
		e.mapped = nil
		e.cache = make(map[int]cacheEntry)
		e.fingerprints = nil
	} else {
		stats.ReusedAnalysis = true
	}

	mappingChanged := e.mapped == nil
	if e.result != nil && e.result.Summary.BlockLengthFrames != resolved.EffectiveBlockFrames() {
		// Repartition changes dispatch/render ranges, but analysis remains reusable.
		e.cache = make(map[int]cacheEntry)
		e.fingerprints = nil
	}
	if mappingChanged || mappingPolicyChanged(e.strategy) {
		mapped, err := dsp.MapParameters(resolved, e.table, resolved.F0Shift, resolved.EnvelopeWarp)
		if err != nil {
			return nil, nil, err
		}
		e.mapped = mapped
	} else {
		stats.ReusedMapping = true
	}

	blockFrames := resolved.EffectiveBlockFrames()
	plan, err := dispatch.PlanDispatch(resolved, e.table, cfg)
	if err != nil && plan == nil {
		return nil, nil, err
	}
	if err != nil && len(plan.Unprocessed) == 0 {
		return nil, nil, err
	}
	blockCount := dsp.BlockCount(e.table, blockFrames)
	newFingerprints := make([]policy.BlockFingerprint, blockCount)
	for i := 0; i < blockCount; i++ {
		est := plan.Blocks[i]
		newFingerprints[i] = policy.BlockFingerprint{
			BlockIndex:     i,
			AnalysisDigest: hashFrameRange(e.table.Frames, est.FrameStart, est.FrameEnd, true),
			MappingDigest:  hashMappedRange(e.mapped, est.FrameStart, est.FrameEnd),
			DispatchDigest: canonical.Bytes(canonical.MustJSON(struct {
				Block  dispatch.BlockEstimate `json:"block"`
				Config dispatch.Config        `json:"config"`
			}{Block: est, Config: plan.Config})),
			RenderDigest: canonical.String(fmt.Sprintf("gain=%.12f;samples=%d-%d",
				strategyOutputGain(e.strategy), est.SampleStart, est.SampleEnd)),
		}
	}
	newFingerprints = policy.ChainBlockFingerprints(newFingerprints)
	if len(e.fingerprints) > 0 {
		stage, block := policy.EarliestMismatchedBlock(e.fingerprints, newFingerprints)
		stats.EarliestStage = stage
		stats.EarliestBlock = block
	}

	samples := make([]int16, 0, e.table.ExpectedSamples)
	pcm := make([]byte, 0, e.table.ExpectedSamples*2)
	clamps := 0
	contentFrom := -1
	for i, fingerprint := range newFingerprints {
		cached, ok := e.cache[i]
		if !ok ||
			cached.fingerprint.AnalysisDigest != fingerprint.AnalysisDigest ||
			cached.fingerprint.MappingDigest != fingerprint.MappingDigest ||
			cached.fingerprint.RenderDigest != fingerprint.RenderDigest {
			contentFrom = i
			break
		}
	}
	if contentFrom >= 0 {
		stats.RebuildFromBlock = contentFrom
		stats.RebuildToBlock = blockCount - 1
	} else {
		stats.RebuildFromBlock = -1
		stats.RebuildToBlock = -1
	}
	for i := 0; i < blockCount; i++ {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		fp := newFingerprints[i]
		cached, ok := e.cache[i]
		inRebuildInterval := contentFrom >= 0 && i >= contentFrom
		reusable := !inRebuildInterval && ok &&
			cached.fingerprint.AnalysisDigest == fp.AnalysisDigest &&
			cached.fingerprint.MappingDigest == fp.MappingDigest &&
			cached.fingerprint.RenderDigest == fp.RenderDigest
		if reusable {
			stats.ReusedBlocks = append(stats.ReusedBlocks, i)
			samples = append(samples, cached.samples...)
			pcm = append(pcm, cached.pcm...)
			clamps += cached.clampCount
			continue
		}

		rendered, err := dsp.RenderBlock(e.table, e.mapped, i, blockFrames, strategyOutputGain(e.strategy))
		if err != nil {
			return nil, nil, err
		}
		blockPCM := int16ToBytesLocal(rendered.Samples)
		e.cache[i] = cacheEntry{fingerprint: fp, samples: rendered.Samples, pcm: blockPCM, clampCount: rendered.ClampCount}
		stats.RebuiltBlocks = append(stats.RebuiltBlocks, i)
		samples = append(samples, rendered.Samples...)
		pcm = append(pcm, blockPCM...)
		clamps += rendered.ClampCount
	}
	e.fingerprints = newFingerprints

	wavBytes, err := wav.EncodePCM16(samples, e.table.SampleRate, 1)
	if err != nil {
		return nil, nil, err
	}
	frames := make([]FrameSummary, 0, len(e.table.Frames))
	for i, frame := range e.table.Frames {
		frames = append(frames, FrameSummary{
			Index: frame.Index, SourceF0Hz: frame.F0Hz, MappedF0Hz: e.mapped[i].MappedF0Hz,
			Voiced: frame.Voiced, MelDigest: frame.MelDigest, MappingDigest: e.mapped[i].MappingDigest,
			ParamDigest: frame.ParamDigest,
		})
	}
	frameBytes, _, _ := canonical.JSON(frames)
	summary := StableSummary{
		SegmentID: e.input.Segment.ID, PolicyVersion: policyVersion, SampleRate: e.table.SampleRate,
		FrameShiftMS: e.table.FrameShiftMS, FrameShiftSamples: e.table.FrameShiftSamples,
		FrameLengthSamples: e.table.FrameLengthSamples, BlockLengthFrames: blockFrames,
		ExpectedSamples: e.table.ExpectedSamples, SampleCount: len(samples), BlockCount: blockCount,
		ClampCount: clamps, InputFingerprint: e.input.Fingerprint,
		TableDigest: e.table.Summary.ParamTableDigest, MelEnergyDigest: e.table.Summary.MelEnergyDigest,
		FrameSequenceDigest: canonical.Bytes(frameBytes), PCMDigest: canonical.Bytes(pcm),
		WAVDigest: canonical.Bytes(wavBytes),
	}
	if report, err := diagnostics.AlignFrameTable(resolved, e.table); err == nil {
		summary.F0Alignment = report
	}
	summaryJSON, _, _ := canonical.JSON(summary)
	result := &ConversionResult{
		Summary: summary, SummaryJSON: summaryJSON, PCMSamples: samples, PCMBytes: pcm,
		WAVBytes: wavBytes, Frames: frames, Table: e.table, Mapped: e.mapped, Plan: plan,
		Complete: len(samples) == e.table.ExpectedSamples,
	}
	e.result = result
	stats.PolicyVersion = policyVersion
	stats.BlockCount = blockCount
	stats.VersionFingerprint = policy.BuildVersionFingerprint(
		e.strategy, e.input.Fingerprint,
		policy.StageFingerprint{Stage: policy.StageAnalysis, Digest: e.table.Summary.ParamTableDigest},
		policy.StageFingerprint{Stage: policy.StageMapping, Digest: summary.FrameSequenceDigest},
		policy.StageFingerprint{Stage: policy.StageDispatch, Digest: canonical.Bytes(canonical.MustJSON(plan.Config))},
		policy.StageFingerprint{Stage: policy.StageReconstruction, Digest: summary.WAVDigest},
	).Combined
	return result, stats, nil
}

func mappingPolicyChanged(strategy policy.Strategy) bool {
	return strategy.F0Shift != nil || strategy.EnvelopeWarp != nil || strategy.OutputGain != nil
}

func hashFrameRange(frames []dsp.FrameParams, start, end int, paramsOnly bool) string {
	if start < 0 {
		start = 0
	}
	if end > len(frames) {
		end = len(frames)
	}
	digests := make([]string, 0, end-start)
	for i := start; i < end; i++ {
		if paramsOnly {
			digests = append(digests, frames[i].ParamDigest)
		}
	}
	_, digest, _ := canonical.JSON(digests)
	return digest
}

func hashMappedRange(mapped []dsp.MappedFrame, start, end int) string {
	if start < 0 {
		start = 0
	}
	if end > len(mapped) {
		end = len(mapped)
	}
	digests := make([]string, 0, end-start)
	for i := start; i < end; i++ {
		digests = append(digests, mapped[i].MappingDigest)
	}
	_, digest, _ := canonical.JSON(digests)
	return digest
}
