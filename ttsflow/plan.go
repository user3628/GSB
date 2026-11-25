package ttsflow

import (
	"encoding/json"
	"fmt"
)

type stageFingerprints struct {
	Frontend    string `json:"frontend"`
	Acoustic    string `json:"acoustic"`
	Wave        string `json:"wave"`
	Consistency string `json:"consistency"`
}

type frontendArtifact struct {
	Chunk          Chunk  `json:"chunk"`
	Unit           Unit   `json:"unit"`
	UnitDigest     string `json:"unit_digest"`
	NormalizedHash string `json:"normalized_local_hash"`
}

type consistencyArtifact struct {
	Stats       SegmentStats `json:"stats"`
	Fingerprint string       `json:"fingerprint"`
}

type segmentPlan struct {
	Seq         int
	UTTID       string
	Unit        Unit
	Chunk       Chunk
	UnitDigest  string
	Fingerprint stageFingerprints

	FrontendArtifact    []byte
	FrontendDigest      string
	Acoustic            AcousticSegment
	AcousticArtifact    []byte
	AcousticDigest      string
	Wave                WaveSegment
	WaveArtifact        []byte
	WaveDigest          string
	Stats               SegmentStats
	ConsistencyArtifact []byte
	ConsistencyDigest   string
}

type synthesisPlan struct {
	Frontend       FrontendResult
	Segments       []segmentPlan
	AcousticBytes  []byte
	WaveBytes      []byte
	AcousticDigest string
	PCMDigest      string
	Stats          []SegmentStats
	StatsDigest    string
	FrameCount     int
	SampleCount    int
	ClampCount     int
}

func frontendFingerprint(chunk Chunk, unit Unit, unitDigest string) string {
	// Only facts owned by this frontend segment participate. Global text/map
	// digests are intentionally absent so an edit late in an utterance does not
	// invalidate already-correct earlier segments.
	return digestParts("frontend-v1", chunk.Index, chunk.Text, chunk.StartRune, chunk.EndRune,
		chunk.StartByte, chunk.EndByte, chunk.UnitStart, chunk.UnitEnd, unitDigest)
}

func acousticFingerprint(frontendFP string, sample TextSample, cfg Config) string {
	return digestParts("acoustic-v1", frontendFP, cfg.AcousticDigest(), sample.ControlDigest())
}

func waveFingerprint(acousticFP string, cfg Config, frameStart, frameCount int) string {
	return digestParts("wave-v1", acousticFP, cfg.WaveDigest(), frameStart, frameCount)
}

func consistencyFingerprint(waveFP string, cfg Config) string {
	return digestParts("consistency-v1", waveFP, cfg.Splitting.CheckpointIntervalFrames)
}

func buildSynthesisPlan(sample TextSample, frontend FrontendResult, cfg Config) (synthesisPlan, error) {
	plan := synthesisPlan{Frontend: frontend}
	frameStart := 0
	for _, unit := range frontend.Units {
		if unit.ChunkIndex < 0 || unit.ChunkIndex >= len(frontend.Chunks) {
			return plan, &BizError{Code: CodeSplit, Message: "unit refers to an unknown chunk", UTTID: sample.UTTID,
				SegmentSeq: unit.Index, BlockSeq: unit.ChunkIndex}
		}
		chunk := frontend.Chunks[unit.ChunkIndex]
		unitDigest := digestJSON(unit)
		frontendFP := frontendFingerprint(chunk, unit, unitDigest)
		acousticFP := acousticFingerprint(frontendFP, sample, cfg)

		acousticSegs, acousticBytes, err := PredictFrames(sample, []Unit{unit}, cfg, frameStart)
		if err != nil {
			return plan, err
		}
		seg := acousticSegs[0]
		frameCount := len(seg.Frames)
		waveFP := waveFingerprint(acousticFP, cfg, frameStart, frameCount)
		consFP := consistencyFingerprint(waveFP, cfg)
		fps := stageFingerprints{Frontend: frontendFP, Acoustic: acousticFP, Wave: waveFP, Consistency: consFP}

		frontendPayload, _ := json.Marshal(frontendArtifact{
			Chunk: chunk, Unit: unit, UnitDigest: unitDigest,
			NormalizedHash: digestString(chunk.Text),
		})
		waves, _, err := SynthesizeWave(sample, []AcousticSegment{seg}, cfg)
		if err != nil {
			return plan, err
		}
		wave := waves[0]
		stats := statsForSegment(seg, wave, fps)
		consPayload, _ := json.Marshal(consistencyArtifact{Stats: stats, Fingerprint: consFP})

		plan.Segments = append(plan.Segments, segmentPlan{
			Seq: unit.Index, UTTID: sample.UTTID, Unit: unit, Chunk: chunk, UnitDigest: unitDigest, Fingerprint: fps,
			FrontendArtifact: frontendPayload, FrontendDigest: digestBytes(frontendPayload),
			Acoustic: seg, AcousticArtifact: acousticBytes, AcousticDigest: digestBytes(acousticBytes),
			Wave: wave, WaveArtifact: wave.Bytes, WaveDigest: digestBytes(wave.Bytes),
			Stats: stats, ConsistencyArtifact: consPayload, ConsistencyDigest: digestBytes(consPayload),
		})
		plan.AcousticBytes = append(plan.AcousticBytes, acousticBytes...)
		plan.WaveBytes = append(plan.WaveBytes, wave.Bytes...)
		plan.Stats = append(plan.Stats, stats)
		plan.FrameCount += frameCount
		plan.SampleCount += wave.SampleEnd - wave.SampleStart
		plan.ClampCount += wave.ClampCount
		frameStart += frameCount
	}
	plan.AcousticDigest = digestBytes(plan.AcousticBytes)
	plan.PCMDigest = digestBytes(plan.WaveBytes)
	plan.StatsDigest = digestJSON(plan.Stats)
	if len(plan.Segments) == 0 {
		return plan, &BizError{Code: CodeSplit, Message: "synthesis plan has no segments", UTTID: sample.UTTID}
	}
	return plan, nil
}

func stageArtifactDigest(row CheckpointRow, stage Stage) string {
	switch stage {
	case StageFrontend:
		return digestBytes(row.FrontendArtifact)
	case StageAcoustic:
		return digestBytes(row.AcousticArtifact)
	case StageWave:
		return digestBytes(row.WaveArtifact)
	case StageConsistency:
		return digestBytes(row.StatsArtifact)
	default:
		return ""
	}
}

func stageStoredDigest(row CheckpointRow, stage Stage) string {
	switch stage {
	case StageFrontend:
		return row.FrontendDigest
	case StageAcoustic:
		return row.AcousticDigest
	case StageWave:
		return row.PCMDigest
	case StageConsistency:
		return row.ConsistencyDigest
	default:
		return ""
	}
}

func stageExpectedFingerprint(plan segmentPlan, stage Stage) string {
	switch stage {
	case StageFrontend:
		return plan.Fingerprint.Frontend
	case StageAcoustic:
		return plan.Fingerprint.Acoustic
	case StageWave:
		return plan.Fingerprint.Wave
	case StageConsistency:
		return plan.Fingerprint.Consistency
	default:
		return ""
	}
}

func stageRowFingerprint(row CheckpointRow, stage Stage) string {
	switch stage {
	case StageFrontend:
		return row.FrontendFingerprint
	case StageAcoustic:
		return row.AcousticFingerprint
	case StageWave:
		return row.WaveFingerprint
	case StageConsistency:
		return row.ConsistencyFingerprint
	default:
		return ""
	}
}

func stageExpectedDigest(plan segmentPlan, stage Stage) string {
	switch stage {
	case StageFrontend:
		return plan.FrontendDigest
	case StageAcoustic:
		return plan.AcousticDigest
	case StageWave:
		return plan.WaveDigest
	case StageConsistency:
		return plan.ConsistencyDigest
	default:
		return ""
	}
}

func earliestMismatch(plan synthesisPlan, rows map[int]CheckpointRow) (int, Stage, bool) {
	for _, seg := range plan.Segments {
		row, ok := rows[seg.Seq]
		if !ok {
			return seg.Seq, StageFrontend, true
		}
		if row.UnitDigest != seg.UnitDigest {
			return seg.Seq, StageFrontend, true
		}
		for stage := StageFrontend; stage <= StageConsistency; stage++ {
			if !row.has(stage) {
				return seg.Seq, stage, true
			}
			if stageRowFingerprint(row, stage) != stageExpectedFingerprint(seg, stage) {
				return seg.Seq, stage, true
			}
			if stageStoredDigest(row, stage) != stageExpectedDigest(seg, stage) {
				return seg.Seq, stage, true
			}
			if stageArtifactDigest(row, stage) != stageExpectedDigest(seg, stage) {
				return seg.Seq, stage, true
			}
		}
	}
	firstExtra := -1
	for seq, row := range rows {
		if seq >= len(plan.Segments) && row.Active && (firstExtra < 0 || seq < firstExtra) {
			firstExtra = seq
		}
	}
	if firstExtra >= 0 {
		return firstExtra, StageFrontend, true
	}
	return 0, StageFrontend, false
}

type stageArtifactData struct {
	Data        []byte
	Digest      string
	Fingerprint string
	FrameStart  int
	FrameCount  int
	SampleStart int
	SampleEnd   int
}

func artifactForStage(plan segmentPlan, stage Stage) stageArtifactData {
	switch stage {
	case StageFrontend:
		return stageArtifactData{Data: plan.FrontendArtifact, Digest: plan.FrontendDigest, Fingerprint: plan.Fingerprint.Frontend,
			FrameStart: plan.Acoustic.FrameStart, SampleStart: plan.Wave.SampleStart, SampleEnd: plan.Wave.SampleEnd}
	case StageAcoustic:
		return stageArtifactData{Data: plan.AcousticArtifact, Digest: plan.AcousticDigest, Fingerprint: plan.Fingerprint.Acoustic,
			FrameStart: plan.Acoustic.FrameStart, FrameCount: len(plan.Acoustic.Frames), SampleStart: plan.Wave.SampleStart, SampleEnd: plan.Wave.SampleEnd}
	case StageWave:
		return stageArtifactData{Data: plan.WaveArtifact, Digest: plan.WaveDigest, Fingerprint: plan.Fingerprint.Wave,
			FrameStart: plan.Acoustic.FrameStart, FrameCount: len(plan.Acoustic.Frames), SampleStart: plan.Wave.SampleStart, SampleEnd: plan.Wave.SampleEnd}
	case StageConsistency:
		return stageArtifactData{Data: plan.ConsistencyArtifact, Digest: plan.ConsistencyDigest, Fingerprint: plan.Fingerprint.Consistency,
			FrameStart: plan.Acoustic.FrameStart, FrameCount: len(plan.Acoustic.Frames), SampleStart: plan.Wave.SampleStart, SampleEnd: plan.Wave.SampleEnd}
	default:
		return stageArtifactData{}
	}
}

func (p segmentPlan) stageArtifact(stage Stage) StageArtifact {
	data := artifactForStage(p, stage)
	return StageArtifact{
		UTTID: p.UTTID, SegmentSeq: p.Seq, Stage: stage,
		UnitDigest: p.UnitDigest, UnitSummary: p.Unit.Summary(),
		Fingerprint: data.Fingerprint, Artifact: data.Data, ArtifactDigest: data.Digest,
		FrameStart: data.FrameStart, FrameCount: data.FrameCount,
		SampleStart: data.SampleStart, SampleEnd: data.SampleEnd,
		ClampCount: p.Wave.ClampCount,
	}
}

func (p segmentPlan) validateLengths(cfg Config) error {
	declared := len(p.Acoustic.Frames) * cfg.Acoustic.FrameShiftSamples
	actual := len(p.Wave.Bytes) / (cfg.Wave.Channels * 2)
	if actual != declared {
		return &BizError{Code: CodeLengthMismatch, Message: "wave segment length is inconsistent",
			SegmentSeq: p.Seq, Stage: StageWave.String(), DeclaredValue: declared, ActualValue: actual}
	}
	return nil
}

func (p segmentPlan) String() string {
	return fmt.Sprintf("segment=%d unit=%s frame=%d/%d sample=%d/%d", p.Seq, p.Unit.Summary(), p.Acoustic.FrameStart, len(p.Acoustic.Frames), p.Wave.SampleStart, p.Wave.SampleEnd)
}
