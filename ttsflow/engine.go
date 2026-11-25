package ttsflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync/atomic"
	"time"
)

// Engine is the embeddable streaming synthesis component.
type Engine struct {
	Config        Config
	RawConfig     []byte
	Store         *Store
	Samples       []TextSample
	TextVersion   VersionRecord
	ConfigVersion VersionRecord
	localStop     atomic.Bool
}

func NewEngine(cfg Config, rawConfig []byte, store *Store, samples []TextSample) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if store == nil {
		return nil, biz(CodeConfigInvalid, "store is required")
	}
	return &Engine{Config: cfg, RawConfig: append([]byte(nil), rawConfig...), Store: store, Samples: append([]TextSample(nil), samples...)}, nil
}

func (e *Engine) RequestLocalStop() { e.localStop.Store(true) }

// ObserveVersions records the current corpus/config snapshots. Auto text mode
// keeps same-ID edits incremental; explicit replace invalidates everything.
func (e *Engine) ObserveVersions(ctx context.Context, textMode, configMode SnapshotMode) error {
	tr, err := e.Store.ObserveTextSet(ctx, e.Samples, textMode)
	if err != nil {
		return err
	}
	cr, err := e.Store.ObserveConfig(ctx, e.Config, e.RawConfig, configMode)
	if err != nil {
		return err
	}
	e.TextVersion = tr
	e.ConfigVersion = cr
	return nil
}

func (e *Engine) selectedSamples(ids []string) ([]TextSample, error) {
	if len(ids) == 0 {
		cp := append([]TextSample(nil), e.Samples...)
		sort.Slice(cp, func(i, j int) bool { return cp[i].UTTID < cp[j].UTTID })
		return cp, nil
	}
	want := map[string]bool{}
	for _, id := range ids {
		want[id] = true
	}
	var out []TextSample
	for _, sample := range e.Samples {
		if want[sample.UTTID] {
			out = append(out, sample)
			delete(want, sample.UTTID)
		}
	}
	if len(want) > 0 {
		missing := make([]string, 0, len(want))
		for id := range want {
			missing = append(missing, id)
		}
		sort.Strings(missing)
		return nil, biz(CodeDatasetInvalid, "unknown utt_id requested: "+fmt.Sprint(missing))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UTTID < out[j].UTTID })
	return out, nil
}

// Run streams all selected utterances. A graceful stop is checked only after a
// complete consistency checkpoint, so an indivisible unit is never cut in half.
func (e *Engine) Run(ctx context.Context, opts RunOptions) (SynthesisResult, error) {
	textMode := SnapshotAuto
	configMode := SnapshotAuto
	if err := e.ObserveVersions(ctx, textMode, configMode); err != nil {
		return SynthesisResult{}, err
	}
	if opts.ClearStops {
		if err := e.Store.ClearStops(ctx, ""); err != nil {
			return SynthesisResult{}, err
		}
	}
	samples, err := e.selectedSamples(opts.UTTIDs)
	if err != nil {
		return SynthesisResult{}, err
	}
	if opts.ForceRebuild {
		for _, sample := range samples {
			if err := e.Store.DeleteStream(ctx, sample.UTTID); err != nil {
				return SynthesisResult{}, err
			}
		}
	}
	result := SynthesisResult{ConfigDigest: e.Config.Digest(), TextSetDigest: TextSetDigest(e.Samples)}
	result.EarliestInvalidatedSegment = -1
	totalCommitted := 0
	for _, sample := range samples {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}
		uttResult, stats, err := e.synthesizeUTT(ctx, sample, opts, totalCommitted)
		committed, stopped := stats.Committed, stats.Stopped
		result.CheckpointCount += committed
		result.ReusedStageCheckpoints += stats.ReusedStages
		result.PublishedStageCheckpoints += stats.PublishedStages
		if stats.Mismatch && (result.EarliestInvalidatedSegment < 0 || stats.FirstSegment < result.EarliestInvalidatedSegment) {
			result.EarliestInvalidatedSegment = stats.FirstSegment
			result.EarliestInvalidatedStage = stats.FirstStage
		}
		totalCommitted += committed
		if uttResult.Sample.UTTID != "" {
			result.UTTs = append(result.UTTs, uttResult)
		}
		if err != nil {
			result.ManifestDigest = synthesisManifest(&result)
			return result, err
		}
		if stopped {
			result.GracefulStopped = true
			result.ManifestDigest = synthesisManifest(&result)
			return result, ErrGracefulStop
		}
	}
	result.ManifestDigest = synthesisManifest(&result)
	return result, nil
}

type uttRunStats struct {
	Committed       int
	Stopped         bool
	ReusedStages    int
	PublishedStages int
	Mismatch        bool
	FirstSegment    int
	FirstStage      string
}

func (e *Engine) synthesizeUTT(ctx context.Context, sample TextSample, opts RunOptions, alreadyCommitted int) (UTTResult, uttRunStats, error) {
	empty := UTTResult{}
	var runStats uttRunStats
	frontend, err := BuildFrontend(sample, e.Config)
	if err != nil {
		return empty, runStats, err
	}
	plan, err := buildSynthesisPlan(sample, frontend, e.Config)
	if err != nil {
		return empty, runStats, withUTT(stageError(err), sample.UTTID)
	}
	for _, seg := range plan.Segments {
		if err := seg.validateLengths(e.Config); err != nil {
			be, _ := AsBizError(err)
			be.UTTID = sample.UTTID
			be.Stage = StageWave.String()
			return empty, runStats, be
		}
	}

	rows, err := e.Store.ActiveCheckpoints(ctx, sample.UTTID)
	if err != nil {
		return empty, runStats, err
	}
	rowMap := map[int]CheckpointRow{}
	for _, row := range rows {
		rowMap[row.SegmentSeq] = row
	}
	state, hasState, err := e.Store.StreamState(ctx, sample.UTTID)
	if err != nil {
		return empty, runStats, err
	}
	if !hasState && len(rowMap) > 0 {
		// Active rows without a stream header are not a valid recoverable fact
		// set; discard them rather than guessing a sequence.
		if err := e.Store.DeleteStream(ctx, sample.UTTID); err != nil {
			return empty, runStats, err
		}
		rowMap = map[int]CheckpointRow{}
	}
	if segment, stage, mismatch := earliestMismatch(plan, rowMap); mismatch {
		if hasState || len(rowMap) > 0 {
			runStats.Mismatch = true
			runStats.FirstSegment = segment
			runStats.FirstStage = stage.String()
		}
		if hasState {
			if err := e.Store.InvalidateFrom(ctx, sample.UTTID, segment, stage); err != nil {
				return empty, runStats, err
			}
		}
		rows, err = e.Store.ActiveCheckpoints(ctx, sample.UTTID)
		if err != nil {
			return empty, runStats, err
		}
		rowMap = map[int]CheckpointRow{}
		for _, row := range rows {
			rowMap[row.SegmentSeq] = row
		}
	}
	if hasState {
		state, hasState, err = e.Store.StreamState(ctx, sample.UTTID)
		if err != nil {
			return empty, runStats, err
		}
		if hasState && (state.TextDigest != sample.Digest() || state.ConfigDigest != e.Config.Digest()) {
			if err := e.Store.PrepareStream(ctx, sample.UTTID, sample.Digest(), e.Config.Digest()); err != nil {
				return empty, runStats, err
			}
		}
	}

	committed := 0
	stopped := false
	for _, seg := range plan.Segments {
		row := rowMap[seg.Seq]
		for stage := StageFrontend; stage <= StageConsistency; stage++ {
			reuse := row.has(stage) && stageRowFingerprint(row, stage) == stageExpectedFingerprint(seg, stage) &&
				stageStoredDigest(row, stage) == stageExpectedDigest(seg, stage) &&
				stageArtifactDigest(row, stage) == stageExpectedDigest(seg, stage)
			if reuse {
				runStats.ReusedStages++
				continue
			}
			artifact := seg.stageArtifact(stage)
			artifact.TextDigest = sample.Digest()
			artifact.ConfigDigest = e.Config.Digest()
			_, err := e.Store.PublishStage(ctx, artifact)
			if err != nil {
				be, _ := AsBizError(err)
				if be != nil {
					be.UTTID = sample.UTTID
					be.SegmentSeq = seg.Seq
					be.Stage = stage.String()
				}
				return empty, runStats, err
			}
			runStats.PublishedStages++
			row.StageMask |= stage.bit()
			rowMap[seg.Seq] = row
		}
		committed++
		if opts.Delay > 0 {
			select {
			case <-ctx.Done():
				return empty, runStats, ctx.Err()
			case <-time.After(opts.Delay):
			}
		}
		local := e.localStop.Swap(false)
		persisted, err := e.Store.ConsumeStop(ctx, sample.UTTID)
		if err != nil {
			return empty, runStats, err
		}
		if local || persisted || (opts.MaxSegments > 0 && alreadyCommitted+committed >= opts.MaxSegments) {
			stopped = true
			break
		}
	}

	committedSegments := plan.Segments
	if committed < len(committedSegments) {
		committedSegments = committedSegments[:committed]
	}
	var acousticBytes, waveBytes []byte
	var stats []SegmentStats
	var waves []WaveSegment
	frameCount, sampleCount, clampCount := 0, 0, 0
	for _, seg := range committedSegments {
		acousticBytes = append(acousticBytes, seg.AcousticArtifact...)
		waveBytes = append(waveBytes, seg.WaveArtifact...)
		stats = append(stats, seg.Stats)
		waves = append(waves, seg.Wave)
		frameCount += len(seg.Acoustic.Frames)
		sampleCount += seg.Wave.SampleEnd - seg.Wave.SampleStart
		clampCount += seg.Wave.ClampCount
	}
	uttResult := UTTResult{
		Sample: sample, Frontend: frontend,
		ChunkAlignments: buildChunkAlignments(frontend, waves),
		SegmentStats:    stats,
		AcousticDigest:  digestBytes(acousticBytes),
		PCMDigest:       digestBytes(waveBytes),
		StatsDigest:     digestJSON(stats),
		FrameCount:      frameCount,
		SampleCount:     sampleCount,
		ClampCount:      clampCount,
		GracefulStopped: stopped,
	}
	for _, seg := range committedSegments {
		uttResult.Acoustic = append(uttResult.Acoustic, seg.Acoustic)
		uttResult.Wave = append(uttResult.Wave, seg.Wave)
	}
	uttResult.ManifestDigest = uttManifest(uttResult)
	runStats.Committed = committed
	runStats.Stopped = stopped
	finalStatus := "complete"
	if stopped {
		finalStatus = "stopped"
	}
	if err := e.Store.SetStreamStatus(ctx, sample.UTTID, finalStatus); err != nil {
		return empty, runStats, err
	}
	return uttResult, runStats, nil
}

func planWaveSlice(plan synthesisPlan) []WaveSegment {
	out := make([]WaveSegment, 0, len(plan.Segments))
	for _, seg := range plan.Segments {
		out = append(out, seg.Wave)
	}
	return out
}

func stageError(err error) *BizError {
	if be, ok := AsBizError(err); ok {
		return be
	}
	return biz(CodeSplit, err.Error())
}

func uttManifest(result UTTResult) string {
	return digestParts("utt-v1", result.Sample.Digest(), result.Frontend.Digest, result.AcousticDigest,
		result.PCMDigest, result.StatsDigest, result.FrameCount, result.SampleCount, result.ClampCount,
		result.ChunkAlignments)
}

func synthesisManifest(result *SynthesisResult) string {
	type uttEntry struct {
		ID       string `json:"id"`
		Manifest string `json:"manifest"`
	}
	entries := make([]uttEntry, 0, len(result.UTTs))
	for _, utt := range result.UTTs {
		entries = append(entries, uttEntry{ID: utt.Sample.UTTID, Manifest: utt.ManifestDigest})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })
	return digestParts("synthesis-v1", result.ConfigDigest, result.TextSetDigest, entries)
}

func (e *Engine) Status(ctx context.Context, uttID string, segmentSeq int) (CheckpointStatus, bool, error) {
	row, ok, err := e.Store.QueryCheckpoint(ctx, uttID, segmentSeq)
	if err != nil || !ok {
		return CheckpointStatus{}, ok, err
	}
	return statusFromRow(row), true, nil
}

func statusFromRow(row CheckpointRow) CheckpointStatus {
	stages := make([]string, 0, stageCount)
	for stage := StageFrontend; stage <= StageConsistency; stage++ {
		if row.has(stage) {
			stages = append(stages, stage.String())
		}
	}
	return CheckpointStatus{
		UTTID: row.UTTID, SegmentSeq: row.SegmentSeq, StageMask: row.StageMask, Stages: stages,
		CheckpointSeq: row.CheckpointSeq, Generation: row.Generation,
		TextDigest: row.TextDigest, ConfigDigest: row.ConfigDigest, UnitDigest: row.UnitDigest,
		FrontendFingerprint: row.FrontendFingerprint, AcousticFingerprint: row.AcousticFingerprint,
		WaveFingerprint: row.WaveFingerprint, ConsistencyFingerprint: row.ConsistencyFingerprint,
		FrontendDigest: row.FrontendDigest, AcousticDigest: row.AcousticDigest, PCMDigest: row.PCMDigest,
		ConsistencyDigest: row.ConsistencyDigest, FrameStart: row.FrameStart, FrameCount: row.FrameCount,
		SampleStart: row.SampleStart, SampleEnd: row.SampleEnd, ClampCount: row.ClampCount, Active: row.Active,
	}
}

func (e *Engine) VersionFingerprint(ctx context.Context, uttID string, segmentSeq int) (VersionFingerprint, error) {
	var sample *TextSample
	for i := range e.Samples {
		if e.Samples[i].UTTID == uttID {
			sample = &e.Samples[i]
			break
		}
	}
	if sample == nil {
		return VersionFingerprint{}, biz(CodeDatasetInvalid, "unknown utt_id")
	}
	frontend, err := BuildFrontend(*sample, e.Config)
	if err != nil {
		return VersionFingerprint{}, err
	}
	plan, err := buildSynthesisPlan(*sample, frontend, e.Config)
	if err != nil {
		return VersionFingerprint{}, err
	}
	if segmentSeq < 0 || segmentSeq >= len(plan.Segments) {
		return VersionFingerprint{}, biz(CodeDatasetInvalid, "segment_seq is outside the utterance")
	}
	seg := plan.Segments[segmentSeq]
	tv, _, _ := e.Store.LatestVersion(ctx, "texts")
	cv, _, _ := e.Store.LatestVersion(ctx, "config")
	return VersionFingerprint{
		UTTID: uttID, TextDigest: sample.Digest(), ConfigDigest: e.Config.Digest(),
		Frontend: seg.Fingerprint.Frontend, Acoustic: seg.Fingerprint.Acoustic,
		Wave: seg.Fingerprint.Wave, Consistency: seg.Fingerprint.Consistency,
		DatasetVersion: tv.Version, ConfigVersion: cv.Version,
	}, nil
}

// CompareSynthesisResults enforces byte-equivalence for acoustic and PCM
// artifacts and per-frame statistics. The first differing segment is attached
// to E_INCREMENTAL_MISMATCH.
func CompareSynthesisResults(actual, full SynthesisResult) error {
	if actual.ConfigDigest != full.ConfigDigest || actual.TextSetDigest != full.TextSetDigest {
		return &BizError{Code: CodeIncrementalMismatch, Message: "top-level synthesis fingerprints differ", FirstDiffSegment: 0}
	}
	fullMap := map[string]UTTResult{}
	for _, result := range full.UTTs {
		fullMap[result.Sample.UTTID] = result
	}
	for _, got := range actual.UTTs {
		want, ok := fullMap[got.Sample.UTTID]
		if !ok {
			return &BizError{Code: CodeIncrementalMismatch, Message: "utterance present only in incremental result",
				UTTID: got.Sample.UTTID, FirstDiffSegment: 0}
		}
		if got.Frontend.Digest != want.Frontend.Digest {
			return &BizError{Code: CodeIncrementalMismatch, Message: "frontend digest differs",
				UTTID: got.Sample.UTTID, FirstDiffSegment: 0, Stage: StageFrontend.String()}
		}
		byteSegments := len(got.Acoustic)
		if len(want.Acoustic) < byteSegments {
			byteSegments = len(want.Acoustic)
		}
		for i := 0; i < byteSegments; i++ {
			if !bytes.Equal(got.Acoustic[i].Bytes, want.Acoustic[i].Bytes) {
				return &BizError{Code: CodeIncrementalMismatch, Message: "acoustic bytes differ",
					UTTID: got.Sample.UTTID, FirstDiffSegment: i, SegmentSeq: i, Stage: StageAcoustic.String()}
			}
		}
		waveSegments := len(got.Wave)
		if len(want.Wave) < waveSegments {
			waveSegments = len(want.Wave)
		}
		for i := 0; i < waveSegments; i++ {
			if !bytes.Equal(got.Wave[i].Bytes, want.Wave[i].Bytes) {
				return &BizError{Code: CodeIncrementalMismatch, Message: "PCM bytes differ",
					UTTID: got.Sample.UTTID, FirstDiffSegment: i, SegmentSeq: i, Stage: StageWave.String()}
			}
		}
		if got.AcousticDigest != want.AcousticDigest || got.PCMDigest != want.PCMDigest || got.StatsDigest != want.StatsDigest {
			segCount := len(got.SegmentStats)
			if len(want.SegmentStats) < segCount {
				segCount = len(want.SegmentStats)
			}
			for i := 0; i < segCount; i++ {
				if got.SegmentStats[i].AcousticDigest != want.SegmentStats[i].AcousticDigest {
					return &BizError{Code: CodeIncrementalMismatch, Message: "acoustic segment digest differs",
						UTTID: got.Sample.UTTID, FirstDiffSegment: i, SegmentSeq: i, Stage: StageAcoustic.String()}
				}
				if got.SegmentStats[i].PCMDigest != want.SegmentStats[i].PCMDigest {
					return &BizError{Code: CodeIncrementalMismatch, Message: "PCM segment digest differs",
						UTTID: got.Sample.UTTID, FirstDiffSegment: i, SegmentSeq: i, Stage: StageWave.String()}
				}
				if got.SegmentStats[i] != want.SegmentStats[i] {
					return &BizError{Code: CodeIncrementalMismatch, Message: "segment statistics differ",
						UTTID: got.Sample.UTTID, FirstDiffSegment: i, SegmentSeq: i, Stage: StageConsistency.String()}
				}
			}
			return &BizError{Code: CodeIncrementalMismatch, Message: "aggregate artifact digest differs",
				UTTID: got.Sample.UTTID, FirstDiffSegment: segCount}
		}
		delete(fullMap, got.Sample.UTTID)
	}
	if len(fullMap) > 0 {
		return &BizError{Code: CodeIncrementalMismatch, Message: "utterance missing from incremental result", FirstDiffSegment: 0}
	}
	if actual.ManifestDigest != full.ManifestDigest {
		return &BizError{Code: CodeIncrementalMismatch, Message: "final manifest digest differs", FirstDiffSegment: 0}
	}
	return nil
}

type DiagnosticReport struct {
	GeneratedAt   string                        `json:"generated_at"`
	ConfigDigest  string                        `json:"config_digest"`
	TextSetDigest string                        `json:"text_set_digest"`
	TextVersion   VersionRecord                 `json:"text_version"`
	ConfigVersion VersionRecord                 `json:"config_version"`
	Streams       []StreamState                 `json:"streams"`
	Checkpoints   map[string][]CheckpointStatus `json:"checkpoints"`
	EventCounts   map[string]int                `json:"event_counts"`
	SnapshotKinds map[string]string             `json:"snapshot_kinds"`
}

func (e *Engine) DiagnosticReport(ctx context.Context, uttIDs []string) (DiagnosticReport, error) {
	report := DiagnosticReport{
		GeneratedAt: nowString(), ConfigDigest: e.Config.Digest(), TextSetDigest: TextSetDigest(e.Samples),
		Checkpoints: map[string][]CheckpointStatus{}, EventCounts: map[string]int{}, SnapshotKinds: map[string]string{},
	}
	tv, ok, err := e.Store.LatestVersion(ctx, "texts")
	if err != nil {
		return report, err
	}
	if ok {
		report.TextVersion = tv
	}
	cv, ok, err := e.Store.LatestVersion(ctx, "config")
	if err != nil {
		return report, err
	}
	if ok {
		report.ConfigVersion = cv
	}
	samples, err := e.selectedSamples(uttIDs)
	if err != nil {
		return report, err
	}
	for _, sample := range samples {
		state, has, err := e.Store.StreamState(ctx, sample.UTTID)
		if err != nil {
			return report, err
		}
		if has {
			report.Streams = append(report.Streams, state)
		}
		rows, err := e.Store.ActiveCheckpoints(ctx, sample.UTTID)
		if err != nil {
			return report, err
		}
		for _, row := range rows {
			report.Checkpoints[sample.UTTID] = append(report.Checkpoints[sample.UTTID], statusFromRow(row))
		}
		count, err := e.Store.EventCount(ctx, sample.UTTID)
		if err != nil {
			return report, err
		}
		report.EventCounts[sample.UTTID] = count
		kind, err := e.Store.InspectSnapshot(ctx, sample.UTTID)
		if err != nil {
			return report, err
		}
		report.SnapshotKinds[sample.UTTID] = kind
	}
	return report, nil
}

func (e *Engine) ExportReport(ctx context.Context, uttIDs []string, path string) error {
	report, err := e.DiagnosticReport(ctx, uttIDs)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFileAtomic(path, data, 0o644); err != nil {
		return err
	}
	return nil
}
