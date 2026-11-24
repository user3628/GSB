package replay

import (
	"fmt"
	"sort"

	"maskrevision/internal/model"
)

type Options struct {
	OverlapPolicy     string
	UndoSegmentPolicy string
	ForkPolicy        string
}

func (o Options) normalized() Options {
	if o.OverlapPolicy == "" {
		o.OverlapPolicy = model.OverlapLastWrite
	}
	if o.UndoSegmentPolicy == "" {
		o.UndoSegmentPolicy = model.UndoSegmentRecycle
	}
	if o.ForkPolicy == "" {
		o.ForkPolicy = "reject"
	}
	return o
}

type Initial struct {
	ShardID          string
	Width            int
	Height           int
	Pixels           []int
	Segments         []model.Segment
	InputFingerprint string
}

type Engine struct {
	state       *model.ReplayState
	log         model.Log
	opts        Options
	revisionFPs []string
	prefixFPs   []string
	nextIndex   int
}

func NewEngine(initial Initial, log model.Log, opts Options) (*Engine, error) {
	if initial.ShardID != log.ShardID {
		return nil, fmt.Errorf("initial shard %s does not match log shard %s", initial.ShardID, log.ShardID)
	}
	if initial.Width != log.Width || initial.Height != log.Height {
		return nil, fmt.Errorf("initial dimensions %dx%d do not match log %dx%d", initial.Width, initial.Height, log.Width, log.Height)
	}
	segments := make(map[int]*model.Segment, len(initial.Segments))
	maxID := 0
	for i := range initial.Segments {
		seg := initial.Segments[i]
		seg.History = nil
		cp := seg
		segments[seg.ID] = &cp
		if seg.ID > maxID {
			maxID = seg.ID
		}
	}
	if _, ok := segments[0]; !ok {
		segments[0] = &model.Segment{ID: 0, Status: model.StatusIgnored, Source: "reserved-ignore"}
	}
	nextInstance := map[int]int{}
	for id, seg := range segments {
		if id == 0 {
			continue
		}
		if seg.InstanceNo >= nextInstance[seg.CategoryID] {
			nextInstance[seg.CategoryID] = seg.InstanceNo + 1
		}
	}
	state := &model.ReplayState{
		ShardID:          initial.ShardID,
		InputFingerprint: initial.InputFingerprint,
		Width:            initial.Width,
		Height:           initial.Height,
		Pixels:           append([]int(nil), initial.Pixels...),
		Segments:         segments,
		NextSegmentID:    maxID + 1,
		NextInstanceNo:   nextInstance,
		RevisionRecords:  []model.RevisionRecord{},
		Undone:           map[int]bool{},
		WriteClaim:       map[int]int{},
	}
	if err := model.ValidateState(state, 0); err != nil {
		return nil, err
	}
	return newEngine(state, log, opts)
}

func Resume(state *model.ReplayState, log model.Log, opts Options) (*Engine, error) {
	if state == nil {
		return nil, fmt.Errorf("nil replay state")
	}
	if state.ShardID != log.ShardID {
		return nil, fmt.Errorf("state shard %s does not match log shard %s", state.ShardID, log.ShardID)
	}
	if state.WriteClaim == nil {
		state.WriteClaim = map[int]int{}
	}
	if state.Undone == nil {
		state.Undone = map[int]bool{}
	}
	if err := model.ValidateState(state, state.LastRevision); err != nil {
		return nil, err
	}
	return newEngine(model.CloneState(state), log, opts)
}

func newEngine(state *model.ReplayState, log model.Log, opts Options) (*Engine, error) {
	e := &Engine{state: state, log: log, opts: opts.normalized(), nextIndex: len(state.RevisionRecords)}
	if e.nextIndex > len(log.Revisions) {
		return nil, fmt.Errorf("state has %d revisions but log only has %d", e.nextIndex, len(log.Revisions))
	}
	prefix := make([]model.Revision, 0, len(log.Revisions))
	for i, rev := range log.Revisions {
		prefix = append(prefix, rev)
		e.revisionFPs = append(e.revisionFPs, model.RevisionDigest(rev))
		e.prefixFPs = append(e.prefixFPs, model.LogPrefixDigest(prefix))
		if i < e.nextIndex {
			rec := state.RevisionRecords[i]
			if rec.Revision != rev.Revision {
				return nil, fmt.Errorf("checkpoint revision %d does not match log revision %d at index %d", rec.Revision, rev.Revision, i)
			}
			if rec.RevisionFP != e.revisionFPs[i] || rec.LogPrefixFP != e.prefixFPs[i] {
				return nil, fmt.Errorf("checkpoint fingerprint mismatch at revision %d", rev.Revision)
			}
		}
	}
	if e.nextIndex > 0 && state.LastRevision != state.RevisionRecords[e.nextIndex-1].Revision {
		return nil, fmt.Errorf("state head %d does not match latest checkpoint", state.LastRevision)
	}
	return e, nil
}

func (e *Engine) State() *model.ReplayState { return e.state }
func (e *Engine) Head() int                 { return e.state.LastRevision }
func (e *Engine) Remaining() int            { return len(e.log.Revisions) - e.nextIndex }
func (e *Engine) NextRevision() (model.Revision, bool) {
	if e.nextIndex >= len(e.log.Revisions) {
		return model.Revision{}, false
	}
	return e.log.Revisions[e.nextIndex], true
}

func (e *Engine) ApplyNext() (*model.RevisionRecord, *model.BusinessError) {
	if e.nextIndex >= len(e.log.Revisions) {
		return nil, model.NewBusinessError("E_REPLAY_COMPLETE", "all revisions have been replayed", nil)
	}
	rev := e.log.Revisions[e.nextIndex]
	rec, be := e.applyRevision(rev, e.nextIndex)
	if be != nil {
		return nil, model.WithContext(be, e.state.ShardID, rev.Revision)
	}
	e.nextIndex++
	return rec, nil
}

func (e *Engine) applyRevision(rev model.Revision, logIndex int) (*model.RevisionRecord, *model.BusinessError) {
	if rev.Revision <= e.state.LastRevision {
		be := model.NewBusinessError("E_FORK_CONFLICT", "revision does not advance the replayed head", map[string]any{
			"revision": rev.Revision, "head_revision": e.state.LastRevision,
		})
		e.recordForkDecision(rev, "rejected", "non-monotonic revision number")
		return nil, be
	}
	if rev.ParentRevision != 0 && rev.ParentRevision != e.state.LastRevision {
		be := model.NewBusinessError("E_FORK_CONFLICT", "revision parent does not match the active head", map[string]any{
			"revision": rev.Revision, "parent_revision": rev.ParentRevision, "head_revision": e.state.LastRevision,
			"first_cause_revision": rev.Revision,
		})
		e.recordForkDecision(rev, "rejected", "parent points to an earlier revision after an undo; explicit branch insertion is not the active lineage")
		return nil, be
	}
	var rec model.RevisionRecord
	var be *model.BusinessError
	switch rev.Action {
	case model.ActionUndo:
		rec, be = e.applyUndo(rev)
	case model.ActionNoop:
		rec = e.baseRecord(rev, logIndex)
		rec.Unrecoverable = rev.UnrecoverableReason
		rec.InferenceBasis = rev.InferenceBasis
	case model.ActionRepaint, model.ActionReassign, model.ActionIgnore:
		rec, be = e.applyEdit(rev, logIndex)
	default:
		be = model.NewBusinessError("E_ACTION_UNKNOWN", "unsupported revision action", map[string]any{"action": rev.Action})
	}
	if be != nil {
		return nil, be
	}
	if be := model.ValidateState(e.state, rev.Revision); be != nil {
		return nil, be
	}
	rec.PixelDigest = model.PixelDigest(e.state.Width, e.state.Height, e.state.Pixels)
	rec.SegmentDigest = model.SegmentTableDigest(e.state.Segments)
	rec.LogPrefixFP = e.prefixFPs[logIndex]
	rec.RevisionFP = e.revisionFPs[logIndex]
	snap := model.Snapshot{
		ShardID: e.state.ShardID, Revision: rev.Revision, Width: e.state.Width, Height: e.state.Height,
		InputFingerprint: e.state.InputFingerprint, LogPrefixFP: rec.LogPrefixFP, RevisionFP: rec.RevisionFP,
		PixelDigest: rec.PixelDigest, SegmentDigest: rec.SegmentDigest,
	}
	rec.SnapshotDigest = model.SnapshotDigest(&snap)
	e.state.RevisionRecords = append(e.state.RevisionRecords, rec)
	e.state.LastRevision = rev.Revision
	return &e.state.RevisionRecords[len(e.state.RevisionRecords)-1], nil
}

func (e *Engine) baseRecord(rev model.Revision, logIndex int) model.RevisionRecord {
	return model.RevisionRecord{
		Revision: rev.Revision, ParentRevision: rev.ParentRevision, Action: rev.Action,
		Status: model.StatusActive, UndoOf: rev.UndoOf, InferenceBasis: rev.InferenceBasis,
		Unrecoverable: rev.UnrecoverableReason, LogPrefixFP: e.prefixFPs[logIndex], RevisionFP: e.revisionFPs[logIndex],
	}
}

func (e *Engine) applyEdit(rev model.Revision, logIndex int) (model.RevisionRecord, *model.BusinessError) {
	rec := e.baseRecord(rev, logIndex)
	indices, be := rev.Region.Pixels(e.state.Width, e.state.Height)
	if be != nil {
		return rec, be
	}
	before := map[int]model.Segment{}
	capture := func(id int) {
		if _, ok := before[id]; ok {
			return
		}
		if seg, ok := e.state.Segments[id]; ok {
			cp := *seg
			cp.History = nil
			before[id] = cp
		} else {
			before[id] = model.Segment{ID: id}
		}
	}
	targetID := 0
	switch rev.Action {
	case model.ActionRepaint:
		if rev.TargetSegmentID > 0 {
			seg, ok := e.state.Segments[rev.TargetSegmentID]
			if !ok {
				return rec, model.NewBusinessError("E_SEGMENT_NOT_FOUND", "repaint target segment does not exist", map[string]any{
					"segment_id": rev.TargetSegmentID, "first_x": rev.Region.X, "first_y": rev.Region.Y,
				})
			}
			if seg.Status != model.StatusActive {
				return rec, model.NewBusinessError("E_SEGMENT_NOT_FOUND", "repaint target segment is not active", map[string]any{
					"segment_id": rev.TargetSegmentID, "status": seg.Status, "first_x": rev.Region.X, "first_y": rev.Region.Y,
				})
			}
			targetID = rev.TargetSegmentID
			capture(targetID)
			seg.CategoryID = rev.CategoryID
			seg.UpdatedRevision = rev.Revision
		} else {
			targetID = e.allocateSegment(rev)
			rec.DerivedSegments = append(rec.DerivedSegments, targetID)
			before[targetID] = model.Segment{ID: targetID}
		}
	case model.ActionReassign:
		if rev.TargetSegmentID <= 0 {
			return rec, model.NewBusinessError("E_ACTION_UNRECOVERABLE", "reassign target segment is missing", map[string]any{
				"revision": rev.Revision, "first_x": rev.Region.X, "first_y": rev.Region.Y,
			})
		}
		seg, ok := e.state.Segments[rev.TargetSegmentID]
		if !ok {
			return rec, model.NewBusinessError("E_SEGMENT_NOT_FOUND", "reassign target segment does not exist", map[string]any{
				"segment_id": rev.TargetSegmentID, "first_x": rev.Region.X, "first_y": rev.Region.Y,
			})
		}
		if seg.Status != model.StatusActive {
			return rec, model.NewBusinessError("E_SEGMENT_NOT_FOUND", "reassign target segment is not active", map[string]any{
				"segment_id": rev.TargetSegmentID, "status": seg.Status, "first_x": rev.Region.X, "first_y": rev.Region.Y,
			})
		}
		targetID = rev.TargetSegmentID
		capture(targetID)
		seg.UpdatedRevision = rev.Revision
	}

	// Capture every segment that will be touched before mutating pixels.
	capture(targetID)
	for _, idx := range indices {
		capture(e.state.Pixels[idx])
		capture(targetID)
	}
	journal := map[int]model.PixelChange{}
	pixelOrder := make([]int, 0, len(indices))
	for _, idx := range indices {
		oldID := e.state.Pixels[idx]
		oldClaim := e.state.WriteClaim[idx]
		if e.opts.OverlapPolicy == model.OverlapFirstWrite && oldClaim != 0 {
			continue
		}
		if oldID == targetID {
			// The pixel value is unchanged, but last-write ownership still moves.
			if oldClaim == rev.Revision {
				continue
			}
			pc := model.PixelChange{
				Index: idx, X: idx % e.state.Width, Y: idx / e.state.Width,
				BeforeID: oldID, AfterID: oldID, BeforeClaim: oldClaim, AfterClaim: rev.Revision,
				Revision: rev.Revision, Action: rev.Action,
			}
			journal[idx] = pc
			e.state.WriteClaim[idx] = rev.Revision
			continue
		}
		if oldID != targetID {
			if seg := e.state.Segments[oldID]; seg != nil {
				seg.PixelCount--
				seg.UpdatedRevision = rev.Revision
			}
			if seg := e.state.Segments[targetID]; seg != nil {
				seg.PixelCount++
				seg.UpdatedRevision = rev.Revision
			}
			e.state.Pixels[idx] = targetID
		}
		e.state.WriteClaim[idx] = rev.Revision
		pc := model.PixelChange{
			Index: idx, X: idx % e.state.Width, Y: idx / e.state.Width,
			BeforeID: oldID, AfterID: targetID, BeforeClaim: oldClaim, AfterClaim: rev.Revision,
			Revision: rev.Revision, Action: rev.Action,
		}
		journal[idx] = pc
		pixelOrder = append(pixelOrder, idx)
	}
	sort.Ints(pixelOrder)
	for _, idx := range pixelOrder {
		rec.PixelChanges = append(rec.PixelChanges, journal[idx])
	}
	// Same-ID claim-only writes are still part of the undo journal.
	for idx, pc := range journal {
		if pc.BeforeID == pc.AfterID {
			rec.PixelChanges = append(rec.PixelChanges, model.PixelChange{
				Index: idx, X: pc.X, Y: pc.Y, BeforeID: pc.BeforeID, AfterID: pc.AfterID,
				BeforeClaim: pc.BeforeClaim, AfterClaim: pc.AfterClaim, Revision: rev.Revision, Action: rev.Action,
			})
		}
	}
	sort.Slice(rec.PixelChanges, func(i, j int) bool { return rec.PixelChanges[i].Index < rec.PixelChanges[j].Index })

	// Record attribute changes and one compact history event per touched segment.
	touched := map[int]bool{}
	for _, pc := range rec.PixelChanges {
		touched[pc.BeforeID] = true
		touched[pc.AfterID] = true
	}
	for id := range before {
		touched[id] = true
	}
	ids := make([]int, 0, len(touched))
	for id := range touched {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		seg, ok := e.state.Segments[id]
		if !ok {
			continue
		}
		b, existed := before[id]
		if !existed {
			b = model.Segment{ID: id}
		}
		rec.SegmentChanges = append(rec.SegmentChanges, segmentChange(b, seg, rev.Revision, rev.Action))
		e.appendHistory(seg, rev, actionEvent(rev.Action), &b, seg)
	}
	return rec, nil
}

func (e *Engine) allocateSegment(rev model.Revision) int {
	id := 0
	if len(e.state.FreeSegmentIDs) > 0 {
		sort.Ints(e.state.FreeSegmentIDs)
		id = e.state.FreeSegmentIDs[0]
		e.state.FreeSegmentIDs = e.state.FreeSegmentIDs[1:]
		old := e.state.Segments[id]
		oldStatus := ""
		if old != nil {
			oldStatus = old.Status
		}
		seg := &model.Segment{
			ID: id, CategoryID: rev.CategoryID, InstanceNo: e.nextInstance(rev.CategoryID), Status: model.StatusActive,
			Source: "repaint-recycled", CreatedRevision: rev.Revision, UpdatedRevision: rev.Revision,
		}
		if old != nil {
			oldCopy := *old
			oldCopy.History = nil
			seg.History = append(seg.History, model.SegmentEvent{
				Revision: rev.Revision, Action: rev.Action, Event: "reused_after_" + oldStatus,
				Before: &oldCopy, After: ptrSegment(model.SegmentCore(seg)),
			})
		}
		e.state.Segments[id] = seg
		return id
	}
	id = e.state.NextSegmentID
	e.state.NextSegmentID++
	seg := &model.Segment{
		ID: id, CategoryID: rev.CategoryID, InstanceNo: e.nextInstance(rev.CategoryID), Status: model.StatusActive,
		Source: "repaint-new", CreatedRevision: rev.Revision, UpdatedRevision: rev.Revision,
	}
	e.state.Segments[id] = seg
	e.appendHistory(seg, rev, "created", nil, seg)
	return id
}

func (e *Engine) nextInstance(categoryID int) int {
	n := e.state.NextInstanceNo[categoryID]
	if n <= 0 {
		n = 1
	}
	e.state.NextInstanceNo[categoryID] = n + 1
	return n
}

func (e *Engine) applyUndo(rev model.Revision) (model.RevisionRecord, *model.BusinessError) {
	rec := e.baseRecord(rev, 0)
	targetIndex := -1
	for i := len(e.state.RevisionRecords) - 1; i >= 0; i-- {
		r := e.state.RevisionRecords[i]
		if r.Action == model.ActionUndo || r.Action == model.ActionNoop {
			continue
		}
		if !e.state.Undone[r.Revision] {
			targetIndex = i
			break
		}
	}
	if targetIndex < 0 {
		return rec, model.NewBusinessError("E_UNDO_INITIAL", "there is no non-undone edit to undo", map[string]any{
			"revision": rev.Revision, "reason": "initial state and undo markers are not undoable",
		})
	}
	target := &e.state.RevisionRecords[targetIndex]
	beforeSegments := map[int]model.Segment{}
	capture := func(id int) {
		if _, ok := beforeSegments[id]; ok {
			return
		}
		if seg, ok := e.state.Segments[id]; ok {
			cp := *seg
			cp.History = nil
			beforeSegments[id] = cp
		}
	}
	for _, id := range target.DerivedSegments {
		capture(id)
	}
	for _, sc := range target.SegmentChanges {
		capture(sc.SegmentID)
	}
	for _, pc := range target.PixelChanges {
		capture(pc.BeforeID)
		capture(pc.AfterID)
	}

	// Restore in reverse order so repeated writes to one pixel resolve back to
	// the first pre-edit value.
	for i := len(target.PixelChanges) - 1; i >= 0; i-- {
		pc := target.PixelChanges[i]
		if pc.Index < 0 || pc.Index >= len(e.state.Pixels) {
			return rec, model.NewBusinessError("E_PIXEL_COUNT_MISMATCH", "undo journal pixel is out of range", map[string]any{
				"pixel_index": pc.Index, "segment_id": pc.AfterID,
			})
		}
		current := e.state.Pixels[pc.Index]
		if current != pc.AfterID {
			return rec, model.NewBusinessError("E_PIXEL_COUNT_MISMATCH", "undo journal no longer matches the active mask", map[string]any{
				"pixel_index": pc.Index, "first_x": pc.Index % e.state.Width, "first_y": pc.Index / e.state.Width,
				"journal_after_id": pc.AfterID, "actual": current,
			})
		}
		if current != pc.BeforeID {
			if e.state.Segments[current] != nil {
				e.state.Segments[current].PixelCount--
			}
			if e.state.Segments[pc.BeforeID] != nil {
				e.state.Segments[pc.BeforeID].PixelCount++
			}
			e.state.Pixels[pc.Index] = pc.BeforeID
		}
		if pc.BeforeClaim == 0 {
			delete(e.state.WriteClaim, pc.Index)
		} else {
			e.state.WriteClaim[pc.Index] = pc.BeforeClaim
		}
		inv := model.PixelChange{
			Index: pc.Index, X: pc.Index % e.state.Width, Y: pc.Index / e.state.Width,
			BeforeID: pc.AfterID, AfterID: pc.BeforeID, BeforeClaim: pc.AfterClaim, AfterClaim: pc.BeforeClaim,
			Revision: rev.Revision, Action: rev.Action,
		}
		rec.PixelChanges = append(rec.PixelChanges, inv)
	}
	sort.Slice(rec.PixelChanges, func(i, j int) bool { return rec.PixelChanges[i].Index < rec.PixelChanges[j].Index })

	// Restore pre-edit attributes for reused segments.
	for _, sc := range target.SegmentChanges {
		seg, ok := e.state.Segments[sc.SegmentID]
		if !ok {
			continue
		}
		if sc.BeforeCategoryID > 0 {
			seg.CategoryID = sc.BeforeCategoryID
		}
		if sc.BeforeInstanceNo > 0 {
			seg.InstanceNo = sc.BeforeInstanceNo
		}
		if sc.BeforeStatus != "" && sc.SegmentID != 0 {
			seg.Status = sc.BeforeStatus
		}
		seg.UpdatedRevision = rev.Revision
	}
	// Retire (or recycle) segments created by the undone revision.
	for _, id := range target.DerivedSegments {
		seg, ok := e.state.Segments[id]
		if !ok {
			continue
		}
		capture(id)
		if seg.PixelCount != 0 {
			return rec, model.NewBusinessError("E_PIXEL_COUNT_MISMATCH", "derived segment still has pixels during undo", map[string]any{
				"segment_id": id, "pixel_count": seg.PixelCount,
				"first_x": firstPixelX(e.state, id), "first_y": firstPixelY(e.state, id),
			})
		}
		b := beforeSegments[id]
		if e.opts.UndoSegmentPolicy == model.UndoSegmentVoid {
			seg.Status = model.StatusVoided
		} else {
			seg.Status = model.StatusRecycled
			e.state.FreeSegmentIDs = append(e.state.FreeSegmentIDs, id)
			sort.Ints(e.state.FreeSegmentIDs)
		}
		seg.UpdatedRevision = rev.Revision
		rec.DerivedSegments = append(rec.DerivedSegments, id)
		e.appendHistory(seg, rev, "undo_"+seg.Status, &b, seg)
	}
	// Capture all changes that the undo produced for attribution.
	ids := make([]int, 0, len(beforeSegments))
	for id := range beforeSegments {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		seg, ok := e.state.Segments[id]
		if !ok {
			continue
		}
		rec.SegmentChanges = append(rec.SegmentChanges, segmentChange(beforeSegments[id], seg, rev.Revision, rev.Action))
		if id != 0 && targetContains(target, id) {
			e.appendHistory(seg, rev, "undo_restore", ptrSegment(beforeSegments[id]), seg)
		}
	}
	rec.UndoOf = target.Revision
	target.Status = "undone"
	e.state.Undone[target.Revision] = true
	return rec, nil
}

func targetContains(target *model.RevisionRecord, id int) bool {
	for _, sc := range target.SegmentChanges {
		if sc.SegmentID == id {
			return true
		}
	}
	for _, d := range target.DerivedSegments {
		if d == id {
			return true
		}
	}
	return false
}

func (e *Engine) recordForkDecision(rev model.Revision, decision, reason string) {
	e.state.ForkDecisions = append(e.state.ForkDecisions, model.ForkDecision{
		Revision: rev.Revision, ParentRevision: rev.ParentRevision, HeadRevision: e.state.LastRevision,
		Decision: decision, Reason: reason,
	})
}

func (e *Engine) appendHistory(seg *model.Segment, rev model.Revision, event string, before, after *model.Segment) {
	var b, a *model.Segment
	if before != nil {
		cp := *before
		cp.History = nil
		b = &cp
	}
	if after != nil {
		cp := *after
		cp.History = nil
		a = &cp
	}
	seg.History = append(seg.History, model.SegmentEvent{
		Revision: rev.Revision, Action: rev.Action, Event: event, Before: b, After: a,
	})
}

func segmentChange(before model.Segment, after *model.Segment, revision int, action string) model.SegmentAttributeChange {
	return model.SegmentAttributeChange{
		SegmentID:        after.ID,
		BeforeCategoryID: before.CategoryID, AfterCategoryID: after.CategoryID,
		BeforeInstanceNo: before.InstanceNo, AfterInstanceNo: after.InstanceNo,
		BeforeStatus: before.Status, AfterStatus: after.Status,
		BeforePixelCount: before.PixelCount, AfterPixelCount: after.PixelCount,
		Revision: revision, Action: action,
	}
}

func ptrSegment(s model.Segment) *model.Segment { return &s }

func actionEvent(action string) string {
	switch action {
	case model.ActionRepaint:
		return "repainted"
	case model.ActionReassign:
		return "reassigned_instance"
	case model.ActionIgnore:
		return "marked_ignored"
	default:
		return action
	}
}

func firstPixelX(state *model.ReplayState, id int) int {
	for i, pix := range state.Pixels {
		if pix == id {
			return i % state.Width
		}
	}
	return 0
}

func firstPixelY(state *model.ReplayState, id int) int {
	for i, pix := range state.Pixels {
		if pix == id {
			return i / state.Width
		}
	}
	return 0
}
