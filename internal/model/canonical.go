package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
)

func SHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func PixelDigest(width, height int, pixels []int) string {
	h := sha256.New()
	_, _ = h.Write([]byte("pixel-map-v1\x00"))
	_ = binary.Write(h, binary.LittleEndian, int64(width))
	_ = binary.Write(h, binary.LittleEndian, int64(height))
	_ = binary.Write(h, binary.LittleEndian, int64(len(pixels)))
	for _, id := range pixels {
		_ = binary.Write(h, binary.LittleEndian, int32(id))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func SegmentTableDigest(segments map[int]*Segment) string {
	ids := make([]int, 0, len(segments))
	for id := range segments {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	var b bytes.Buffer
	b.WriteString("segment-table-v1\x00")
	for _, id := range ids {
		s := segments[id]
		b.WriteString(strconv.Itoa(s.ID))
		b.WriteByte(0)
		b.WriteString(strconv.Itoa(s.CategoryID))
		b.WriteByte(0)
		b.WriteString(strconv.Itoa(s.InstanceNo))
		b.WriteByte(0)
		b.WriteString(s.Status)
		b.WriteByte(0)
		b.WriteString(strconv.Itoa(s.PixelCount))
		b.WriteByte(0)
		b.WriteString(s.Source)
		b.WriteByte(0)
		b.WriteString(strconv.Itoa(s.CreatedRevision))
		b.WriteByte(0)
		b.WriteString(strconv.Itoa(s.UpdatedRevision))
		b.WriteByte(0)
		for _, ev := range s.History {
			b.WriteString(strconv.Itoa(ev.Revision))
			b.WriteByte(0)
			b.WriteString(ev.Action)
			b.WriteByte(0)
			b.WriteString(ev.Event)
			b.WriteByte(0)
			b.WriteString(ev.Basis)
			b.WriteByte(0)
			writeSegmentCore(&b, ev.Before)
			writeSegmentCore(&b, ev.After)
		}
		b.WriteByte('\n')
	}
	return SHA256Hex(b.Bytes())
}

func writeSegmentCore(b *bytes.Buffer, s *Segment) {
	if s == nil {
		b.WriteString("-\x00")
		return
	}
	fmt.Fprintf(b, "%d:%d:%d:%s:%d:%s:%d:%d\x00", s.ID, s.CategoryID, s.InstanceNo, s.Status, s.PixelCount, s.Source, s.CreatedRevision, s.UpdatedRevision)
}

func SnapshotDigest(s *Snapshot) string {
	return SHA256Hex([]byte(fmt.Sprintf("snapshot-v1\x00%s\x00%d\x00%d\x00%d\x00%s\x00%s\x00%s\x00%s\x00",
		s.ShardID, s.Revision, s.Width, s.Height, s.InputFingerprint, s.LogPrefixFP, s.PixelDigest, s.SegmentDigest)))
}

func RevisionDigest(rev Revision) string {
	// fmt with fixed field order is deliberately versioned and avoids JSON map
	// ordering entirely. Sub-regions are preserved in log order.
	var b bytes.Buffer
	fmt.Fprintf(&b, "revision-v1\x00%d\x00%d\x00%s\x00%d,%d,%d,%d\x00%d\x00%d\x00%d\x00%.9f\x00%s\x00%d\x00%s\x00%d\x00%s\x00%s\x00%s\x00",
		rev.Revision, rev.ParentRevision, rev.Action, rev.Region.X, rev.Region.Y, rev.Region.W, rev.Region.H,
		rev.CategoryID, rev.TargetSegmentID, rev.InstanceNo, rev.Confidence, rev.Actor, rev.UndoOf,
		rev.Phrase, rev.SourceVersion, rev.InferenceBasis, rev.UnrecoverableReason, rev.Note)
	for _, r := range rev.SubRegions {
		fmt.Fprintf(&b, "%d,%d,%d,%d;", r.X, r.Y, r.W, r.H)
	}
	return SHA256Hex(b.Bytes())
}

func LogPrefixDigest(revs []Revision) string {
	h := sha256.New()
	_, _ = h.Write([]byte("log-prefix-v1\x00"))
	for _, rev := range revs {
		_, _ = h.Write([]byte(RevisionDigest(rev)))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func InputFingerprint(width, height int, pngBytes []byte, segments []Segment) string {
	cp := make([]Segment, len(segments))
	copy(cp, segments)
	sort.Slice(cp, func(i, j int) bool { return cp[i].ID < cp[j].ID })
	h := sha256.New()
	_, _ = h.Write([]byte("input-shard-v1\x00"))
	_ = binary.Write(h, binary.LittleEndian, int64(width))
	_ = binary.Write(h, binary.LittleEndian, int64(height))
	_ = binary.Write(h, binary.LittleEndian, int64(len(pngBytes)))
	_, _ = h.Write(pngBytes)
	for _, s := range cp {
		fmt.Fprintf(h, "%d:%d:%d:%s:%d\x00", s.ID, s.CategoryID, s.InstanceNo, s.Status, s.PixelCount)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func CloneSegments(in map[int]*Segment) map[int]*Segment {
	out := make(map[int]*Segment, len(in))
	for id, s := range in {
		cp := *s
		cp.History = append([]SegmentEvent(nil), s.History...)
		for i := range cp.History {
			if cp.History[i].Before != nil {
				before := *cp.History[i].Before
				before.History = nil
				cp.History[i].Before = &before
			}
			if cp.History[i].After != nil {
				after := *cp.History[i].After
				after.History = nil
				cp.History[i].After = &after
			}
		}
		out[id] = &cp
	}
	return out
}

func CloneState(in *ReplayState) *ReplayState {
	out := *in
	out.Pixels = append([]int(nil), in.Pixels...)
	out.Segments = CloneSegments(in.Segments)
	out.FreeSegmentIDs = append([]int(nil), in.FreeSegmentIDs...)
	out.NextInstanceNo = make(map[int]int, len(in.NextInstanceNo))
	for k, v := range in.NextInstanceNo {
		out.NextInstanceNo[k] = v
	}
	out.Undone = make(map[int]bool, len(in.Undone))
	for k, v := range in.Undone {
		out.Undone[k] = v
	}
	out.WriteClaim = make(map[int]int, len(in.WriteClaim))
	for k, v := range in.WriteClaim {
		out.WriteClaim[k] = v
	}
	out.RevisionRecords = append([]RevisionRecord(nil), in.RevisionRecords...)
	for i := range out.RevisionRecords {
		out.RevisionRecords[i].PixelChanges = append([]PixelChange(nil), in.RevisionRecords[i].PixelChanges...)
		out.RevisionRecords[i].SegmentChanges = append([]SegmentAttributeChange(nil), in.RevisionRecords[i].SegmentChanges...)
		out.RevisionRecords[i].DerivedSegments = append([]int(nil), in.RevisionRecords[i].DerivedSegments...)
	}
	out.ForkDecisions = append([]ForkDecision(nil), in.ForkDecisions...)
	return &out
}

func SortedSegments(segments map[int]*Segment) []Segment {
	ids := make([]int, 0, len(segments))
	for id := range segments {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	out := make([]Segment, 0, len(ids))
	for _, id := range ids {
		cp := *segments[id]
		cp.History = append([]SegmentEvent(nil), segments[id].History...)
		out = append(out, cp)
	}
	return out
}

func SnapshotFromState(state *ReplayState, rec RevisionRecord) Snapshot {
	snap := Snapshot{
		ShardID:          state.ShardID,
		Revision:         rec.Revision,
		Width:            state.Width,
		Height:           state.Height,
		InputFingerprint: state.InputFingerprint,
		LogPrefixFP:      rec.LogPrefixFP,
		RevisionFP:       rec.RevisionFP,
		PixelDigest:      rec.PixelDigest,
		SegmentDigest:    rec.SegmentDigest,
		Pixels:           append([]int(nil), state.Pixels...),
		Segments:         SortedSegments(state.Segments),
	}
	snap.SnapshotDigest = SnapshotDigest(&snap)
	return snap
}

func ValidateState(state *ReplayState, revision int) *BusinessError {
	if state.Width <= 0 || state.Height <= 0 || len(state.Pixels) != state.Width*state.Height {
		return WithContext(NewBusinessError("E_PIXEL_COUNT_MISMATCH", "pixel map has invalid dimensions", map[string]any{
			"width": state.Width, "height": state.Height, "pixel_len": len(state.Pixels),
			"first_x": 0, "first_y": 0,
			"expected": state.Width * state.Height, "actual": len(state.Pixels),
		}), state.ShardID, revision)
	}
	actual := make(map[int]int, len(state.Segments))
	for i, id := range state.Pixels {
		s, ok := state.Segments[id]
		if !ok {
			return WithContext(NewBusinessError("E_SEGMENT_NOT_FOUND", "pixel references a missing segment", map[string]any{
				"segment_id": id, "pixel_index": i, "first_x": i % state.Width, "first_y": i / state.Width,
			}), state.ShardID, revision)
		}
		if id == 0 && s.Status != StatusIgnored {
			return WithContext(NewBusinessError("E_SEGMENT_STATUS_INVALID", "ignore segment zero must have ignored status", map[string]any{
				"segment_id": 0, "pixel_index": i, "first_x": i % state.Width, "first_y": i / state.Width,
			}), state.ShardID, revision)
		}
		if id != 0 && (s.Status == StatusRecycled || s.Status == StatusVoided || s.Status == StatusIgnored) {
			return WithContext(NewBusinessError("E_SEGMENT_STATUS_INVALID", "pixel references a non-active segment", map[string]any{
				"segment_id": id, "status": s.Status, "pixel_index": i, "first_x": i % state.Width, "first_y": i / state.Width,
			}), state.ShardID, revision)
		}
		actual[id]++
	}
	ids := make([]int, 0, len(state.Segments))
	for id := range state.Segments {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		s := state.Segments[id]
		want := s.PixelCount
		got := actual[id]
		if want != got {
			x, y := 0, 0
			if got > 0 {
				for i, pix := range state.Pixels {
					if pix == id {
						x, y = i%state.Width, i/state.Width
						break
					}
				}
			}
			return WithContext(NewBusinessError("E_PIXEL_COUNT_MISMATCH", "segment pixel total differs from pixel map", map[string]any{
				"segment_id": id, "expected": want, "actual": got,
				"first_x": x, "first_y": y,
			}), state.ShardID, revision)
		}
		if s.Status == StatusRecycled || s.Status == StatusVoided {
			if got != 0 {
				return WithContext(NewBusinessError("E_PIXEL_COUNT_MISMATCH", "retired segment still owns pixels", map[string]any{
					"segment_id": id, "expected": 0, "actual": got,
				}), state.ShardID, revision)
			}
		}
	}
	return nil
}

func SegmentCore(s *Segment) Segment {
	if s == nil {
		return Segment{}
	}
	cp := *s
	cp.History = nil
	return cp
}
