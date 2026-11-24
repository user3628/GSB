package replay

import (
	"sort"

	"maskrevision/internal/model"
)

type DiffPixel struct {
	X      int `json:"x"`
	Y      int `json:"y"`
	Index  int `json:"index"`
	FromID int `json:"from_segment_id"`
	ToID   int `json:"to_segment_id"`
}

type SegmentDiff struct {
	SegmentID       int            `json:"segment_id"`
	From            *model.Segment `json:"from,omitempty"`
	To              *model.Segment `json:"to,omitempty"`
	Appearance      string         `json:"appearance"`
	PixelCountDelta int            `json:"pixel_count_delta"`
	CategoryChanged bool           `json:"category_changed"`
	InstanceChanged bool           `json:"instance_changed"`
	StatusChanged   bool           `json:"status_changed"`
	CategorySource  *Cause         `json:"category_source,omitempty"`
	InstanceSource  *Cause         `json:"instance_source,omitempty"`
	PixelSource     *Cause         `json:"pixel_source,omitempty"`
	FirstSource     *Cause         `json:"first_source,omitempty"`
}

type Cause struct {
	Revision int    `json:"revision"`
	Action   string `json:"action"`
	Reason   string `json:"reason"`
}

type DiffResult struct {
	ShardID              string        `json:"shard_id"`
	FromRevision         int           `json:"from_revision"`
	ToRevision           int           `json:"to_revision"`
	DifferencePixelCount int           `json:"difference_pixel_count"`
	Pixels               []DiffPixel   `json:"pixels"`
	AffectedSegments     []SegmentDiff `json:"affected_segments"`
	FirstCause           *Cause        `json:"first_cause,omitempty"`
}

func Diff(fromRevision, toRevision int, fromState, toState *model.ReplayState, records []model.RevisionRecord) (DiffResult, *model.BusinessError) {
	if fromRevision <= 0 || toRevision <= 0 {
		return DiffResult{}, model.NewBusinessError("E_REVISION_OUT_OF_RANGE", "revision numbers must be positive", map[string]any{
			"from_revision": fromRevision, "to_revision": toRevision,
		})
	}
	if fromRevision > toRevision {
		return DiffResult{}, model.NewBusinessError("E_REVISION_ORDER", "from revision must not be later than to revision", map[string]any{
			"from_revision": fromRevision, "to_revision": toRevision,
		})
	}
	if fromState == nil || toState == nil || len(fromState.Pixels) != len(toState.Pixels) {
		return DiffResult{}, model.NewBusinessError("E_DIFF_STATE_INVALID", "snapshot states are unavailable", nil)
	}
	out := DiffResult{ShardID: toState.ShardID, FromRevision: fromRevision, ToRevision: toRevision}
	affected := map[int]bool{}
	for i := range fromState.Pixels {
		a, b := fromState.Pixels[i], toState.Pixels[i]
		if a == b {
			continue
		}
		out.Pixels = append(out.Pixels, DiffPixel{X: i % fromState.Width, Y: i / fromState.Width, Index: i, FromID: a, ToID: b})
		affected[a] = true
		affected[b] = true
	}
	for id := range fromState.Segments {
		affected[id] = true
	}
	for id := range toState.Segments {
		affected[id] = true
	}
	out.DifferencePixelCount = len(out.Pixels)
	ids := make([]int, 0, len(affected))
	for id := range affected {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		fs := fromState.Segments[id]
		ts := toState.Segments[id]
		if fs == nil && ts == nil {
			continue
		}
		sd := SegmentDiff{SegmentID: id}
		if fs != nil {
			cp := model.SegmentCore(fs)
			sd.From = &cp
		}
		if ts != nil {
			cp := model.SegmentCore(ts)
			sd.To = &cp
		}
		switch {
		case fs == nil:
			sd.Appearance = "appeared"
		case ts == nil:
			sd.Appearance = "disappeared"
		case fs.CategoryID != ts.CategoryID || fs.InstanceNo != ts.InstanceNo || fs.Status != ts.Status:
			sd.Appearance = "changed"
		default:
			if fs.PixelCount == ts.PixelCount {
				continue
			}
			sd.Appearance = "pixels_changed"
		}
		if fs != nil && ts != nil {
			sd.PixelCountDelta = ts.PixelCount - fs.PixelCount
			sd.CategoryChanged = fs.CategoryID != ts.CategoryID
			sd.InstanceChanged = fs.InstanceNo != ts.InstanceNo
			sd.StatusChanged = fs.Status != ts.Status
		} else if ts != nil {
			sd.PixelCountDelta = ts.PixelCount
			sd.CategoryChanged = ts.CategoryID != 0
			sd.InstanceChanged = ts.InstanceNo != 0
			sd.StatusChanged = true
		} else if fs != nil {
			sd.PixelCountDelta = -fs.PixelCount
			sd.CategoryChanged = fs.CategoryID != 0
			sd.InstanceChanged = fs.InstanceNo != 0
			sd.StatusChanged = true
		}
		for _, rec := range records {
			if rec.Revision <= fromRevision || rec.Revision > toRevision {
				continue
			}
			for _, sc := range rec.SegmentChanges {
				if sc.SegmentID != id {
					continue
				}
				if sc.BeforeCategoryID != sc.AfterCategoryID || (sd.From == nil && sc.AfterCategoryID != 0) {
					sd.CategorySource = earlierCause(sd.CategorySource, rec, "category changed in revision")
				}
				if sc.BeforeInstanceNo != sc.AfterInstanceNo || (sd.From == nil && sc.AfterInstanceNo != 0) {
					sd.InstanceSource = earlierCause(sd.InstanceSource, rec, "instance number changed in revision")
				}
				if sc.BeforePixelCount != sc.AfterPixelCount {
					sd.PixelSource = earlierCause(sd.PixelSource, rec, "segment pixel membership changed")
				}
			}
			for _, pc := range rec.PixelChanges {
				if pc.BeforeID == id || pc.AfterID == id {
					sd.PixelSource = earlierCause(sd.PixelSource, rec, "pixel was assigned to or from the segment")
				}
			}
		}
		sd.FirstSource = minCause(sd.CategorySource, sd.InstanceSource, sd.PixelSource)
		out.AffectedSegments = append(out.AffectedSegments, sd)
		out.FirstCause = earlierCause(out.FirstCause, revisionRecordByNumber(records, causeRevision(sd.FirstSource)), "first segment difference")
	}
	if len(out.Pixels) == 0 {
		out.Pixels = []DiffPixel{}
	}
	if len(out.AffectedSegments) == 0 {
		out.AffectedSegments = []SegmentDiff{}
	}
	return out, nil
}

func earlierCause(current *Cause, rec model.RevisionRecord, reason string) *Cause {
	if rec.Revision == 0 {
		return current
	}
	next := &Cause{Revision: rec.Revision, Action: rec.Action, Reason: reason}
	if current == nil || next.Revision < current.Revision {
		return next
	}
	return current
}

func minCause(causes ...*Cause) *Cause {
	var out *Cause
	for _, c := range causes {
		if c != nil && (out == nil || c.Revision < out.Revision) {
			out = c
		}
	}
	return out
}

func causeRevision(c *Cause) int {
	if c == nil {
		return 0
	}
	return c.Revision
}

func revisionRecordByNumber(records []model.RevisionRecord, revision int) model.RevisionRecord {
	for _, rec := range records {
		if rec.Revision == revision {
			return rec
		}
	}
	return model.RevisionRecord{}
}
