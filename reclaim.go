package walgroup

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sort"
)

type reclaimIntentPayload struct {
	CheckpointSequence uint64   `json:"checkpoint_sequence"`
	SegmentNos         []uint64 `json:"segment_nos"`
}

// ReclaimSegments checks the active checkpoint coverage and physically removes
// eligible SEALED segment files. All coverage checks happen before any state is
// changed. The persisted order is SEALED -> RECLAIMABLE -> delete -> RECLAIMED.
func (e *Engine) ReclaimSegments(ctx context.Context, req ReclaimRequest) (ReclaimResult, error) {
	if err := checkClosed(e); err != nil {
		return ReclaimResult{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := checkClosed(e); err != nil {
		return ReclaimResult{}, err
	}
	cp, _, err := loadActiveCheckpoint(ctx, e.db, true)
	if err != nil {
		return ReclaimResult{}, err
	}
	if cp == nil {
		return ReclaimResult{}, newError(CodeReclaimUnsafe, "no active checkpoint; no segment may be reclaimed")
	}
	var targets []SegmentInfo
	if len(req.SegmentNos) == 0 {
		all, err := listSegments(ctx, e.db, false)
		if err != nil {
			return ReclaimResult{}, err
		}
		for _, seg := range all {
			if seg.State == SegmentSealed && seg.LastLSN <= cp.ReplayStartLSN {
				targets = append(targets, seg)
			}
		}
	} else {
		seen := make(map[uint64]bool)
		for _, no := range req.SegmentNos {
			if seen[no] {
				continue
			}
			seen[no] = true
			seg, err := getSegment(ctx, e.db, no)
			if err != nil {
				return ReclaimResult{}, err
			}
			if seg.State != SegmentSealed {
				earliest := earliestUncovered(ctx, e.db, seg, cp.ReplayStartLSN)
				er := newError(CodeReclaimUnsafe, "segment %d is %s, not SEALED", no, seg.State)
				er.SegmentNo = no
				er.EarliestUncovered = earliest
				return ReclaimResult{}, er
			}
			if seg.LastLSN > cp.ReplayStartLSN {
				earliest := earliestUncovered(ctx, e.db, seg, cp.ReplayStartLSN)
				er := newError(CodeReclaimUnsafe, "segment %d record LSN %d is not covered by checkpoint replay start %d", no, earliest, cp.ReplayStartLSN)
				er.SegmentNo = no
				er.EarliestUncovered = earliest
				er.EarliestLSN = seg.StartLSN
				return ReclaimResult{}, er
			}
			targets = append(targets, seg)
		}
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].SegmentNo < targets[j].SegmentNo })
	if len(targets) == 0 {
		return ReclaimResult{Segments: []SegmentInfo{}}, nil
	}
	payload := reclaimIntentPayload{CheckpointSequence: cp.Sequence}
	for _, seg := range targets {
		payload.SegmentNos = append(payload.SegmentNos, seg.SegmentNo)
	}
	intentID, err := insertIntent(ctx, e.db, "RECLAIM", "PENDING", payload)
	if err != nil {
		return ReclaimResult{}, err
	}
	tx, err := beginWrite(ctx, e.db)
	if err != nil {
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return ReclaimResult{}, wrapError(CodeReclaimFailed, err, "begin reclaim marking")
	}
	defer tx.Rollback()
	for _, seg := range targets {
		res, err := tx.ExecContext(ctx, `UPDATE segments SET state=? WHERE segment_no=? AND state=?`,
			string(SegmentReclaimable), seg.SegmentNo, string(SegmentSealed))
		if err != nil {
			_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
			return ReclaimResult{}, wrapError(CodeReclaimFailed, err, "mark segment %d RECLAIMABLE", seg.SegmentNo)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
			return ReclaimResult{}, invalidTransitionSegment(seg.SegmentNo, seg.State, SegmentReclaimable)
		}
	}
	if err := updateIntentStatus(ctx, tx, intentID, "MARKED"); err != nil {
		return ReclaimResult{}, wrapError(CodeReclaimFailed, err, "mark reclaim intent")
	}
	if err := tx.Commit(); err != nil {
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return ReclaimResult{}, wrapError(CodeReclaimFailed, err, "commit RECLAIMABLE marks")
	}
	if err := e.fault(FaultBeforeReclaimDelete, targets[0].SegmentNo, nil); err != nil {
		return ReclaimResult{}, wrapError(CodeReclaimFailed, err, "reclaim stopped before physical deletion")
	}
	for _, seg := range targets {
		path := segmentPathByInfo(e.dir, seg)
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return ReclaimResult{}, wrapError(CodeReclaimFailed, err, "remove segment file %d", seg.SegmentNo)
		}
	}
	syncDir(e.dir)
	if err := e.finishReclaim(ctx, intentID, payload.SegmentNos); err != nil {
		return ReclaimResult{}, err
	}
	out := make([]SegmentInfo, 0, len(targets))
	for _, seg := range targets {
		seg.State = SegmentReclaimed
		out = append(out, seg)
	}
	return ReclaimResult{Segments: out}, nil
}

func earliestUncovered(ctx context.Context, q dbTX, seg SegmentInfo, replayStart uint64) uint64 {
	var n sql.NullInt64
	err := q.QueryRowContext(ctx, `SELECT MIN(lsn) FROM txn_records WHERE segment_no=? AND lsn>?`, seg.SegmentNo, replayStart).Scan(&n)
	if err == nil && n.Valid {
		return uint64(n.Int64)
	}
	if seg.StartLSN > replayStart {
		return seg.StartLSN
	}
	if seg.LastLSN > 0 {
		return seg.LastLSN + 1
	}
	return seg.StartLSN
}

func (e *Engine) finishReclaim(ctx context.Context, intentID int64, segmentNos []uint64) error {
	tx, err := beginWrite(ctx, e.db)
	if err != nil {
		return wrapError(CodeReclaimFailed, err, "begin reclaim completion")
	}
	defer tx.Rollback()
	for _, no := range segmentNos {
		res, err := tx.ExecContext(ctx, `UPDATE segments SET state=? WHERE segment_no=? AND state=?`,
			string(SegmentReclaimed), no, string(SegmentReclaimable))
		if err != nil {
			return wrapError(CodeReclaimFailed, err, "mark segment %d RECLAIMED", no)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			var state string
			_ = tx.QueryRowContext(ctx, `SELECT state FROM segments WHERE segment_no=?`, no).Scan(&state)
			if SegmentState(state) != SegmentReclaimed {
				return invalidTransitionSegment(no, SegmentState(state), SegmentReclaimed)
			}
		}
	}
	if err := updateIntentStatus(ctx, tx, intentID, "DONE"); err != nil {
		return wrapError(CodeReclaimFailed, err, "complete reclaim intent")
	}
	if err := tx.Commit(); err != nil {
		return wrapError(CodeReclaimFailed, err, "commit reclaim completion")
	}
	return nil
}

// TransitionSegment applies only the externally safe portion of the declared
// segment state machine. OPEN -> SEALED is supported directly; transitions into
// RECLAIMABLE/RECLAIMED must go through ReclaimSegments so coverage and physical
// deletion remain ordered.
func (e *Engine) TransitionSegment(ctx context.Context, segmentNo uint64, to SegmentState) (SegmentInfo, error) {
	if to != SegmentSealed {
		er := newError(CodeInvalidTransition, "segment transition to %s is only legal through checkpoint/reclaim operations", to)
		er.SegmentNo = segmentNo
		er.To = string(to)
		return SegmentInfo{}, er
	}
	cur, err := e.currentSegment(ctx, e.db)
	if err != nil {
		return SegmentInfo{}, err
	}
	if cur.SegmentNo != segmentNo {
		return SegmentInfo{}, invalidTransitionSegment(segmentNo, cur.State, SegmentSealed)
	}
	return e.SealCurrent(ctx)
}
