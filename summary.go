package speakerdiar

import (
	"fmt"
	"sort"
	"strings"
)

// ChunkSummary is the stage-four per-chunk fact used by global timeline assembly.
type ChunkSummary struct {
	ChunkID          string             `json:"chunk_id"`
	ChunkIndex       int                `json:"chunk_index"`
	FrameCount       int                `json:"frame_count"`
	SegmentCount     int                `json:"segment_count"`
	OverlapCount     int                `json:"overlap_count"`
	ArbitrationCount int                `json:"arbitration_count"`
	Segments         []TimelineSegment  `json:"segments"`
	Overlaps         []OverlapSegment   `json:"overlaps"`
	Stats            map[string]float64 `json:"stats"`
	Digest           string             `json:"digest"`
}

type summaryDigestData struct {
	ChunkID  string            `json:"chunk_id"`
	Segments []TimelineSegment `json:"segments"`
	Overlaps []OverlapSegment  `json:"overlaps"`
}

// RunSummaryStage converts labels and separated energy into segments and overlaps.
func RunSummaryStage(chunk ChunkManifest, emb *EmbeddingResult, sep *SeparationResult, cfg Config) (*ChunkSummary, error) {
	cfg = cfg.withDefaults()
	bySource := map[int][]FrameLabel{}
	maxFrame := 0
	for _, label := range emb.FrameLabels {
		bySource[label.SourceIndex] = append(bySource[label.SourceIndex], label)
		if label.FrameIndex+1 > maxFrame {
			maxFrame = label.FrameIndex + 1
		}
	}
	for source := range bySource {
		sort.SliceStable(bySource[source], func(i, j int) bool {
			return bySource[source][i].FrameIndex < bySource[source][j].FrameIndex
		})
	}

	highEnergy := map[int][]FrameLabel{}
	for _, label := range emb.FrameLabels {
		if label.Energy >= cfg.OverlapEnergyThreshold {
			highEnergy[label.FrameIndex] = append(highEnergy[label.FrameIndex], label)
		}
	}
	overlaps := []OverlapSegment{}
	groupStart := -1
	groupEnd := -1
	groupKey := ""
	var groupLabels []FrameLabel
	flush := func() {
		if groupStart < 0 {
			return
		}
		ov := makeOverlap(chunk.ChunkID, len(overlaps)+1, groupStart, groupEnd, cfg, groupLabels)
		overlaps = append(overlaps, ov)
		groupStart, groupEnd, groupKey, groupLabels = -1, -1, "", nil
	}
	for f := 0; f < maxFrame; f++ {
		labels := highEnergy[f]
		activeSources := uniqueHighSources(labels)
		if len(activeSources) < 2 {
			flush()
			continue
		}
		sort.SliceStable(labels, func(i, j int) bool {
			if labels[i].SpeakerID != labels[j].SpeakerID {
				return labels[i].SpeakerID < labels[j].SpeakerID
			}
			return labels[i].SourceIndex < labels[j].SourceIndex
		})
		keyParts := make([]string, 0, len(labels))
		seenSource := map[int]bool{}
		participantLabels := []FrameLabel{}
		for _, l := range labels {
			if seenSource[l.SourceIndex] {
				continue
			}
			seenSource[l.SourceIndex] = true
			keyParts = append(keyParts, fmt.Sprintf("%d:%s", l.LocalNumber, l.SpeakerID))
			participantLabels = append(participantLabels, l)
		}
		key := strings.Join(keyParts, "|")
		if groupStart >= 0 && key == groupKey && f == groupEnd+1 {
			groupEnd = f
			continue
		}
		flush()
		groupStart, groupEnd, groupKey, groupLabels = f, f, key, participantLabels
	}
	flush()
	assignmentBySource := map[int]SpeakerAssignment{}
	for _, a := range emb.Speakers {
		assignmentBySource[a.SourceIndex] = a
	}
	for i := range overlaps {
		ids := []string{}
		nums := []int{}
		sources := append([]int(nil), overlaps[i].SourceIndexes...)
		sort.Ints(sources)
		overlaps[i].SourceIndexes = uniqueInts(sources)
		for _, sourceIndex := range overlaps[i].SourceIndexes {
			if a, ok := assignmentBySource[sourceIndex]; ok {
				ids = append(ids, a.SpeakerID)
				nums = append(nums, a.LocalNumber)
			}
		}
		sort.Strings(ids)
		overlaps[i].SpeakerIDs = uniqueStrings(ids)
		sort.Ints(nums)
		overlaps[i].LocalNumbers = uniqueInts(nums)
	}
	overlaps = mergeNearbyOverlaps(chunk.ChunkID, overlaps, 120)

	segments := []TimelineSegment{}
	for sourceIndex := 0; sourceIndex < len(sep.Tracks); sourceIndex++ {
		labels := bySource[sourceIndex]
		var run []FrameLabel
		flushRun := func() {
			if len(run) == 0 {
				return
			}
			first := run[0]
			last := run[len(run)-1]
			startMS := first.FrameIndex * cfg.HopDurationMS
			endMS := (last.FrameIndex + 1) * cfg.HopDurationMS
			if endMS-startMS >= cfg.MinSegmentMS {
				conf := 0.0
				for _, l := range run {
					conf += l.Confidence
				}
				conf /= float64(len(run))
				seg := TimelineSegment{
					ID:      fmt.Sprintf("%s-seg-s%02d-%05d", chunk.ChunkID, first.LocalNumber, startMS),
					ChunkID: chunk.ChunkID, StartMS: startMS, EndMS: endMS,
					SpeakerID: first.SpeakerID, LocalNumber: first.LocalNumber,
					Confidence: conf, Source: "pipeline:summary",
				}
				segments = append(segments, seg)
			}
			run = nil
		}
		for _, label := range labels {
			if !label.Active || label.SpeakerID == "" {
				flushRun()
				continue
			}
			if len(run) > 0 && label.FrameIndex != run[len(run)-1].FrameIndex+1 {
				flushRun()
			}
			run = append(run, label)
		}
		flushRun()
	}

	for i := range segments {
		for _, ov := range overlaps {
			if intervalsOverlap(segments[i].StartMS, segments[i].EndMS, ov.StartMS, ov.EndMS) &&
				containsString(ov.SpeakerIDs, segments[i].SpeakerID) {
				segments[i].Overlap = true
				break
			}
		}
	}
	sort.SliceStable(segments, func(i, j int) bool {
		if segments[i].StartMS != segments[j].StartMS {
			return segments[i].StartMS < segments[j].StartMS
		}
		if segments[i].SpeakerID != segments[j].SpeakerID {
			return segments[i].SpeakerID < segments[j].SpeakerID
		}
		return segments[i].ID < segments[j].ID
	})

	stats := map[string]float64{
		"source_count":      float64(len(sep.Tracks)),
		"segment_count":     float64(len(segments)),
		"overlap_count":     float64(len(overlaps)),
		"arbitration_count": float64(len(emb.Arbitrations)),
		"frame_count":       float64(len(emb.FrameLabels) / maxInt(1, len(sep.Tracks))),
	}
	summary := &ChunkSummary{
		ChunkID: chunk.ChunkID, ChunkIndex: chunk.Index,
		FrameCount:   len(emb.FrameLabels) / maxInt(1, len(sep.Tracks)),
		SegmentCount: len(segments), OverlapCount: len(overlaps),
		ArbitrationCount: len(emb.Arbitrations), Segments: segments,
		Overlaps: overlaps, Stats: stats,
	}
	summary.Digest = mustDigestValue(summaryDigestData{ChunkID: chunk.ChunkID, Segments: segments, Overlaps: overlaps})
	return summary, nil
}

func uniqueInts(in []int) []int {
	out := in[:0:0]
	var last int
	for i, x := range in {
		if i == 0 || x != last {
			out = append(out, x)
		}
		last = x
	}
	return out
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func mergeNearbyOverlaps(chunkID string, in []OverlapSegment, maxGapMS int) []OverlapSegment {
	sort.SliceStable(in, func(i, j int) bool { return in[i].StartMS < in[j].StartMS })
	out := []OverlapSegment{}
	for _, ov := range in {
		if len(out) > 0 {
			last := &out[len(out)-1]
			if sameStringSet(last.SpeakerIDs, ov.SpeakerIDs) && ov.StartMS-last.EndMS <= maxGapMS && ov.StartMS >= last.StartMS {
				if ov.EndMS > last.EndMS {
					last.EndMS = ov.EndMS
				}
				last.SourceIndexes = uniqueInts(append(append([]int(nil), last.SourceIndexes...), ov.SourceIndexes...))
				last.LocalNumbers = uniqueInts(append(append([]int(nil), last.LocalNumbers...), ov.LocalNumbers...))
				continue
			}
		}
		out = append(out, ov)
	}
	for i := range out {
		out[i].ID = fmt.Sprintf("%s-ov-%03d", chunkID, i+1)
		sort.Ints(out[i].SourceIndexes)
		sort.Ints(out[i].LocalNumbers)
	}
	return out
}

func uniqueHighSources(labels []FrameLabel) []int {
	seen := map[int]bool{}
	out := []int{}
	for _, l := range labels {
		if !seen[l.SourceIndex] {
			seen[l.SourceIndex] = true
			out = append(out, l.SourceIndex)
		}
	}
	sort.Ints(out)
	return out
}

func makeOverlap(chunkID string, ordinal, startFrame, endFrame int, cfg Config, labels []FrameLabel) OverlapSegment {
	speakerIDs := []string{}
	localNumbers := []int{}
	sourceIndexes := []int{}
	seen := map[int]bool{}
	for _, l := range labels {
		if seen[l.SourceIndex] {
			continue
		}
		seen[l.SourceIndex] = true
		speakerIDs = append(speakerIDs, l.SpeakerID)
		localNumbers = append(localNumbers, l.LocalNumber)
		sourceIndexes = append(sourceIndexes, l.SourceIndex)
	}
	return OverlapSegment{
		ID: fmt.Sprintf("%s-ov-%03d", chunkID, ordinal), ChunkID: chunkID,
		StartMS: startFrame * cfg.HopDurationMS, EndMS: (endFrame + 1) * cfg.HopDurationMS,
		SpeakerIDs: speakerIDs, LocalNumbers: localNumbers, SourceIndexes: sourceIndexes,
	}
}

func intervalsOverlap(aStart, aEnd, bStart, bEnd int) bool {
	return aStart < bEnd && bStart < aEnd
}

func containsString(xs []string, target string) bool {
	for _, x := range xs {
		if x == target {
			return true
		}
	}
	return false
}

// VoiceprintRecord pairs an embedding with its chunk for final output.
type VoiceprintRecord struct {
	ChunkID string `json:"chunk_id"`
	SourceEmbedding
}

// FinalArtifacts are the byte-comparable integration outputs.
type FinalArtifacts struct {
	SchemaVersion  int                `json:"schema_version"`
	SessionID      string             `json:"session_id"`
	ConfigVersion  string             `json:"config_version"`
	ManifestDigest string             `json:"manifest_digest"`
	ChunkCount     int                `json:"chunk_count"`
	Segments       []TimelineSegment  `json:"segments"`
	Overlaps       []OverlapSegment   `json:"overlaps"`
	Voiceprints    []VoiceprintRecord `json:"voiceprints"`
	Timeline       *Timeline          `json:"timeline"`
}

// ArtifactFiles contains canonical byte outputs used by incremental and recovery comparisons.
type ArtifactFiles struct {
	Segments    []byte `json:"segments"`
	Overlaps    []byte `json:"overlaps"`
	Voiceprints []byte `json:"voiceprints"`
	Final       []byte `json:"final"`
}

// BuildTimeline deterministically assembles all chunk summaries.
func BuildTimeline(sessionID string, chunks []ChunkManifest, embeddings []*EmbeddingResult, summaries []*ChunkSummary, cfg Config) (*Timeline, error) {
	tl := NewTimeline(sessionID)
	firstRefBySpeaker := map[string]SpeakerRef{}
	for ci, chunk := range chunks {
		summary := summaries[ci]
		if summary == nil {
			continue
		}
		for _, seg := range summary.Segments {
			if _, err := tl.AddSegment(seg); err != nil {
				return nil, err
			}
		}
		if _, err := tl.MergeAllAdjacent(cfg.ShortGapMS, "pipeline:short_gap_merge"); err != nil {
			return nil, err
		}
		for _, ov := range summary.Overlaps {
			if _, err := tl.AddOverlap(ov); err != nil {
				return nil, err
			}
		}
		if embeddings[ci] == nil {
			continue
		}
		assignments := append([]SpeakerAssignment(nil), embeddings[ci].Speakers...)
		sort.SliceStable(assignments, func(i, j int) bool { return assignments[i].LocalNumber < assignments[j].LocalNumber })
		for _, a := range assignments {
			ref := SpeakerRef{ChunkID: chunk.ChunkID, LocalNumber: a.LocalNumber}
			if err := tl.BindSpeakerNumber(ref, a.SpeakerID, "pipeline:centroid_assignment"); err != nil {
				return nil, err
			}
			if prior, ok := firstRefBySpeaker[a.SpeakerID]; ok {
				if err := tl.MergeCrossChunkSpeakers(prior, ref, a.SpeakerID, "pipeline:global_voiceprint_identity"); err != nil {
					return nil, err
				}
			} else {
				firstRefBySpeaker[a.SpeakerID] = ref
			}
		}
	}
	tl.SortForOutput()
	return tl, nil
}

// BuildArtifactFiles renders stable integration files.
func BuildArtifactFiles(manifest *SessionManifest, embeddings []*EmbeddingResult, summaries []*ChunkSummary, tl *Timeline) (*ArtifactFiles, *FinalArtifacts, error) {
	voiceprints := []VoiceprintRecord{}
	for i, emb := range embeddings {
		if emb == nil {
			continue
		}
		chunkID := emb.ChunkID
		if i < len(manifest.Chunks) {
			chunkID = manifest.Chunks[i].ChunkID
		}
		for _, vp := range emb.Embeddings {
			voiceprints = append(voiceprints, VoiceprintRecord{ChunkID: chunkID, SourceEmbedding: vp})
		}
	}
	segments := append([]TimelineSegment(nil), tl.Segments...)
	overlaps := append([]OverlapSegment(nil), tl.Overlaps...)
	final := &FinalArtifacts{
		SchemaVersion: SchemaVersion, SessionID: manifest.SessionID,
		ConfigVersion: mustDigestValue(manifest.Config), ManifestDigest: mustDigestValue(manifest.Chunks),
		ChunkCount: len(manifest.Chunks), Segments: segments, Overlaps: overlaps,
		Voiceprints: voiceprints, Timeline: tl,
	}
	segBytes, err := PrettyJSON(segments)
	if err != nil {
		return nil, nil, err
	}
	ovBytes, err := PrettyJSON(overlaps)
	if err != nil {
		return nil, nil, err
	}
	vpBytes, err := PrettyJSON(voiceprints)
	if err != nil {
		return nil, nil, err
	}
	finalBytes, err := PrettyJSON(final)
	if err != nil {
		return nil, nil, err
	}
	return &ArtifactFiles{Segments: segBytes, Overlaps: ovBytes, Voiceprints: vpBytes, Final: finalBytes}, final, nil
}
