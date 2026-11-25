package speakerdiar

import (
	"fmt"
	"sort"
)

// TimelineSegment is the unified millisecond diarization model.
type TimelineSegment struct {
	ID          string  `json:"id"`
	ChunkID     string  `json:"chunk_id"`
	StartMS     int     `json:"start_ms"`
	EndMS       int     `json:"end_ms"`
	SpeakerID   string  `json:"speaker_id"`
	LocalNumber int     `json:"local_number"`
	Confidence  float64 `json:"confidence"`
	Overlap     bool    `json:"overlap"`
	Source      string  `json:"source"`
}

// OverlapSegment marks a millisecond interval with at least two active sources.
type OverlapSegment struct {
	ID            string   `json:"id"`
	ChunkID       string   `json:"chunk_id"`
	StartMS       int      `json:"start_ms"`
	EndMS         int      `json:"end_ms"`
	SpeakerIDs    []string `json:"speaker_ids"`
	LocalNumbers  []int    `json:"local_numbers"`
	SourceIndexes []int    `json:"source_indexes"`
}

// Revision is one monotonic timeline edit fact.
type Revision struct {
	Version   int64  `json:"version"`
	Operation string `json:"operation"`
	Source    string `json:"source"`
	SegmentID string `json:"segment_id,omitempty"`
	Detail    string `json:"detail,omitempty"`
}

// IdentityBinding maps a chunk-local speaker number to a global identity.
type IdentityBinding struct {
	Alias       string `json:"alias"`
	ChunkID     string `json:"chunk_id"`
	LocalNumber int    `json:"local_number"`
	SpeakerID   string `json:"speaker_id"`
	Source      string `json:"source"`
	Version     int64  `json:"version"`
}

// Timeline is the versioned unified millisecond timeline.
type Timeline struct {
	SessionID string            `json:"session_id"`
	Version   int64             `json:"version"`
	Segments  []TimelineSegment `json:"segments"`
	Overlaps  []OverlapSegment  `json:"overlaps"`
	Bindings  []IdentityBinding `json:"bindings"`
	Revisions []Revision        `json:"revisions"`
}

// NewTimeline creates an empty timeline for a session.
func NewTimeline(sessionID string) *Timeline {
	return &Timeline{SessionID: sessionID}
}

func timelineError(code, msg string) *Error {
	return newError(code, msg).withStage(StageSummary)
}

func (e *Error) withStage(stage string) *Error {
	e.Stage = stage
	return e
}

func validateInterval(start, end int) error {
	if start < 0 {
		return timelineError(CodeTimeline, fmt.Sprintf("segment start %d is negative", start))
	}
	if end < start {
		return timelineError(CodeTimeline, fmt.Sprintf("segment end %d is earlier than start %d", end, start))
	}
	if end == start {
		return timelineError(CodeTimeline, "segment end must be greater than start")
	}
	return nil
}

func (t *Timeline) revise(op, source, segmentID, detail string) Revision {
	t.Version++
	r := Revision{Version: t.Version, Operation: op, Source: source, SegmentID: segmentID, Detail: detail}
	t.Revisions = append(t.Revisions, r)
	return r
}

// AddSegment validates and appends a segment and records one monotonic revision.
func (t *Timeline) AddSegment(seg TimelineSegment) (TimelineSegment, error) {
	if err := validateInterval(seg.StartMS, seg.EndMS); err != nil {
		de := err.(*Error)
		de.ChunkID = seg.ChunkID
		return seg, de
	}
	if seg.ID == "" {
		seg.ID = fmt.Sprintf("seg-v%06d", t.Version+1)
	}
	t.Segments = append(t.Segments, seg)
	t.revise("add_segment", seg.Source, seg.ID, fmt.Sprintf("%d-%d:%s", seg.StartMS, seg.EndMS, seg.SpeakerID))
	return seg, nil
}

// AddOverlap validates and appends an overlap interval.
func (t *Timeline) AddOverlap(ov OverlapSegment) (OverlapSegment, error) {
	if err := validateInterval(ov.StartMS, ov.EndMS); err != nil {
		de := err.(*Error)
		de.ChunkID = ov.ChunkID
		return ov, de
	}
	if ov.ID == "" {
		ov.ID = fmt.Sprintf("ov-v%06d", t.Version+1)
	}
	ids := append([]string(nil), ov.SpeakerIDs...)
	sort.Strings(ids)
	ov.SpeakerIDs = uniqueStrings(ids)
	t.Overlaps = append(t.Overlaps, ov)
	t.revise("add_overlap", "pipeline:overlap", ov.ID, fmt.Sprintf("%d-%d:%v", ov.StartMS, ov.EndMS, ov.SpeakerIDs))
	return ov, nil
}

func uniqueStrings(in []string) []string {
	out := in[:0:0]
	var last string
	for i, s := range in {
		if i == 0 || s != last {
			out = append(out, s)
		}
		last = s
	}
	return out
}

// SplitSegment splits one segment at atMS. The original keeps the left side.
func (t *Timeline) SplitSegment(id string, atMS int, source string) error {
	for i := range t.Segments {
		seg := &t.Segments[i]
		if seg.ID != id {
			continue
		}
		if atMS <= seg.StartMS || atMS >= seg.EndMS {
			de := timelineError(CodeTimeline, "split point must be strictly inside segment")
			de.Details = map[string]any{"segment_id": id, "at_ms": atMS, "start_ms": seg.StartMS, "end_ms": seg.EndMS}
			return de
		}
		oldEnd := seg.EndMS
		seg.EndMS = atMS
		right := TimelineSegment{
			ID: fmt.Sprintf("%s-split-%d", id, atMS), ChunkID: seg.ChunkID,
			StartMS: atMS, EndMS: oldEnd, SpeakerID: seg.SpeakerID,
			LocalNumber: seg.LocalNumber, Confidence: seg.Confidence,
			Overlap: seg.Overlap, Source: source,
		}
		t.Segments = append(t.Segments, right)
		t.revise("split_segment", source, id, fmt.Sprintf("at=%d,right=%s", atMS, right.ID))
		return nil
	}
	de := timelineError(CodeTimeline, "segment not found")
	de.Details = map[string]any{"segment_id": id}
	return de
}

// MergeAdjacent merges segment id with the next same-speaker segment in timeline order.
func (t *Timeline) MergeAdjacent(id string, maxGapMS int, source string) (bool, error) {
	sort.SliceStable(t.Segments, func(i, j int) bool {
		if t.Segments[i].StartMS != t.Segments[j].StartMS {
			return t.Segments[i].StartMS < t.Segments[j].StartMS
		}
		return t.Segments[i].SpeakerID < t.Segments[j].SpeakerID
	})
	idx := -1
	for i := range t.Segments {
		if t.Segments[i].ID == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		de := timelineError(CodeTimeline, "segment not found")
		de.Details = map[string]any{"segment_id": id}
		return false, de
	}
	if idx+1 >= len(t.Segments) {
		return false, nil
	}
	a := &t.Segments[idx]
	b := t.Segments[idx+1]
	if a.ChunkID != b.ChunkID || a.SpeakerID != b.SpeakerID {
		return false, nil
	}
	gap := b.StartMS - a.EndMS
	if gap > maxGapMS {
		return false, nil
	}
	mergedEnd := b.EndMS
	if mergedEnd < a.EndMS {
		mergedEnd = a.EndMS
	}
	a.EndMS = mergedEnd
	a.Confidence = (a.Confidence + b.Confidence) / 2
	if b.Overlap {
		a.Overlap = true
	}
	mergedID := b.ID
	t.Segments = append(t.Segments[:idx+1], t.Segments[idx+2:]...)
	t.revise("merge_adjacent", source, a.ID, fmt.Sprintf("merged=%s,gap_ms=%d", mergedID, gap))
	return true, nil
}

// MergeAllAdjacent repeatedly merges close adjacent same-speaker segments.
func (t *Timeline) MergeAllAdjacent(maxGapMS int, source string) (int, error) {
	merged := 0
	for {
		before := len(t.Segments)
		sort.SliceStable(t.Segments, func(i, j int) bool {
			if t.Segments[i].StartMS != t.Segments[j].StartMS {
				return t.Segments[i].StartMS < t.Segments[j].StartMS
			}
			return t.Segments[i].SpeakerID < t.Segments[j].SpeakerID
		})
		for i := 0; i < len(t.Segments); i++ {
			id := t.Segments[i].ID
			did, err := t.MergeAdjacent(id, maxGapMS, source)
			if err != nil {
				return merged, err
			}
			if did {
				merged++
				break
			}
		}
		if len(t.Segments) == before {
			return merged, nil
		}
	}
}

// SpeakerRef identifies one chunk-local speaker number.
type SpeakerRef struct {
	ChunkID     string `json:"chunk_id"`
	LocalNumber int    `json:"local_number"`
}

func (r SpeakerRef) alias() string {
	return fmt.Sprintf("%s:#%d", r.ChunkID, r.LocalNumber)
}

// BindSpeakerNumber binds a chunk-local contiguous number to a global identity.
func (t *Timeline) BindSpeakerNumber(ref SpeakerRef, speakerID, source string) error {
	if ref.ChunkID == "" || ref.LocalNumber <= 0 || speakerID == "" {
		de := timelineError(CodeTimeline, "invalid speaker binding")
		de.Details = map[string]any{"ref": ref, "speaker_id": speakerID}
		return de
	}
	for _, binding := range t.Bindings {
		if binding.Alias == ref.alias() && binding.SpeakerID != speakerID {
			de := timelineError(CodeTimeline, "chunk-local speaker number conflict")
			de.ChunkID = ref.ChunkID
			de.Details = map[string]any{"alias": ref.alias(), "existing": binding.SpeakerID, "requested": speakerID}
			return de
		}
	}
	t.bind(ref, speakerID, source)
	t.revise("bind_speaker", source, "", ref.alias()+"->"+speakerID)
	return nil
}

// MergeCrossChunkSpeakers binds two local numbers to one canonical global ID.
// An existing incompatible binding is E_TIMELINE and is not overwritten.
func (t *Timeline) MergeCrossChunkSpeakers(a, b SpeakerRef, canonicalID, source string) error {
	if a.ChunkID == "" || b.ChunkID == "" || a.LocalNumber <= 0 || b.LocalNumber <= 0 {
		de := timelineError(CodeTimeline, "invalid cross-chunk speaker reference")
		de.Details = map[string]any{"a": a, "b": b}
		return de
	}
	if canonicalID == "" {
		de := timelineError(CodeTimeline, "canonical speaker id is required for conflict-free merge")
		de.Details = map[string]any{"a": a.alias(), "b": b.alias()}
		return de
	}
	for _, ref := range []SpeakerRef{a, b} {
		for _, binding := range t.Bindings {
			if binding.Alias == ref.alias() && binding.SpeakerID != canonicalID {
				de := timelineError(CodeTimeline, "cross-chunk speaker number conflict")
				de.ChunkID = ref.ChunkID
				de.Details = map[string]any{
					"alias": ref.alias(), "existing_speaker_id": binding.SpeakerID,
					"requested_speaker_id": canonicalID,
				}
				return de
			}
		}
	}
	t.bind(a, canonicalID, source)
	t.bind(b, canonicalID, source)
	t.revise("cross_chunk_identity_merge", source, "", fmt.Sprintf("%s=%s->%s", a.alias(), b.alias(), canonicalID))
	return nil
}

func (t *Timeline) bind(ref SpeakerRef, speakerID, source string) {
	alias := ref.alias()
	for i := range t.Bindings {
		if t.Bindings[i].Alias == alias {
			if t.Bindings[i].SpeakerID != speakerID {
				t.Bindings[i].SpeakerID = speakerID
				t.Bindings[i].Version = t.Version + 1
			}
			return
		}
	}
	t.Bindings = append(t.Bindings, IdentityBinding{
		Alias: alias, ChunkID: ref.ChunkID, LocalNumber: ref.LocalNumber,
		SpeakerID: speakerID, Source: source, Version: t.Version + 1,
	})
}

// SortForOutput canonicalizes segment and overlap order without adding revisions.
func (t *Timeline) SortForOutput() {
	sort.SliceStable(t.Segments, func(i, j int) bool {
		if t.Segments[i].StartMS != t.Segments[j].StartMS {
			return t.Segments[i].StartMS < t.Segments[j].StartMS
		}
		if t.Segments[i].EndMS != t.Segments[j].EndMS {
			return t.Segments[i].EndMS < t.Segments[j].EndMS
		}
		if t.Segments[i].ChunkID != t.Segments[j].ChunkID {
			return t.Segments[i].ChunkID < t.Segments[j].ChunkID
		}
		return t.Segments[i].SpeakerID < t.Segments[j].SpeakerID
	})
	sort.SliceStable(t.Overlaps, func(i, j int) bool {
		if t.Overlaps[i].StartMS != t.Overlaps[j].StartMS {
			return t.Overlaps[i].StartMS < t.Overlaps[j].StartMS
		}
		return t.Overlaps[i].ChunkID < t.Overlaps[j].ChunkID
	})
	sort.SliceStable(t.Bindings, func(i, j int) bool { return t.Bindings[i].Alias < t.Bindings[j].Alias })
}
