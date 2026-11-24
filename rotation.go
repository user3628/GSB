package walgroup

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"time"
)

type rotateIntentPayload struct {
	OldSegmentNo uint64 `json:"old_segment_no"`
	NewSegmentNo uint64 `json:"new_segment_no"`
	NewFileName  string `json:"new_file_name"`
	StartLSN     uint64 `json:"start_lsn"`
	PrevDigest   string `json:"prev_digest"`
}

func insertIntent(ctx context.Context, q dbTX, kind, status string, payload any) (int64, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return 0, wrapError(CodeIO, err, "encode %s intent", kind)
	}
	now := time.Now().UnixMilli()
	res, err := q.ExecContext(ctx, `INSERT INTO intents(kind,status,payload,created_at,updated_at)
		VALUES(?,?,?,?,?)`, kind, status, string(b), now, now)
	if err != nil {
		return 0, wrapError(CodeIO, err, "write %s intent", kind)
	}
	return res.LastInsertId()
}

func updateIntentStatus(ctx context.Context, q dbTX, id int64, status string) error {
	_, err := q.ExecContext(ctx, `UPDATE intents SET status=?,updated_at=? WHERE id=?`, status, time.Now().UnixMilli(), id)
	return err
}

func (e *Engine) currentSegment(ctx context.Context, q dbTX) (SegmentInfo, error) {
	no, err := metaUint(ctx, q, "current_segment_no")
	if err != nil {
		return SegmentInfo{}, err
	}
	if no == 0 {
		return SegmentInfo{}, sql.ErrNoRows
	}
	return getSegment(ctx, q, no)
}

// ensureCurrentLocked creates the first segment if none exists. The caller must
// hold e.mu.
func (e *Engine) ensureCurrentLocked(ctx context.Context) (SegmentInfo, error) {
	cur, err := e.currentSegment(ctx, e.db)
	if err == nil {
		return cur, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return SegmentInfo{}, err
	}
	if err := e.createInitialSegmentLocked(ctx); err != nil {
		return SegmentInfo{}, err
	}
	return e.currentSegment(ctx, e.db)
}

func (e *Engine) createInitialSegmentLocked(ctx context.Context) error {
	if err := e.fault(FaultBeforeRotate, 0, nil); err != nil {
		return wrapError(CodeRotateFailed, err, "initial segment creation rejected")
	}
	nextNo, err := metaUint(ctx, e.db, "next_segment_no")
	if err != nil {
		return err
	}
	nextLSN, err := metaUint(ctx, e.db, "next_lsn")
	if err != nil {
		return err
	}
	if nextNo == 0 {
		nextNo = 1
	}
	if nextLSN == 0 {
		nextLSN = 1
	}
	payload := rotateIntentPayload{NewSegmentNo: nextNo, NewFileName: segmentFileName(nextNo), StartLSN: nextLSN}
	intentID, err := insertIntent(ctx, e.db, "ROTATE", "PENDING", payload)
	if err != nil {
		return err
	}
	h := SegmentHeader{
		Magic:      segmentMagic,
		Version:    segmentVersion,
		HeaderSize: segmentHeaderSize,
		SegmentNo:  nextNo,
		StartLSN:   nextLSN,
	}
	path, err := createSegmentFile(e.dir, h)
	if err != nil {
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return wrapError(CodeRotateFailed, err, "create initial WAL segment")
	}
	tx, err := beginWrite(ctx, e.db)
	if err != nil {
		_ = os.Remove(path)
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return wrapError(CodeRotateFailed, err, "begin initial segment metadata")
	}
	defer tx.Rollback()
	digest, err := computeFileDigest(path)
	if err != nil {
		_ = os.Remove(path)
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return wrapError(CodeRotateFailed, err, "digest initial segment")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO segments(`+segmentColumns+`)
		VALUES(?,?,?,?,?,?,?,?,?,?)`, nextNo, segmentFileName(nextNo), nextLSN, 0, hexZeroDigest,
		uint64(0), uint64(0), int64(segmentHeaderSize), string(SegmentOpen), digest); err != nil {
		_ = os.Remove(path)
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return wrapError(CodeRotateFailed, err, "insert initial segment catalog")
	}
	if err := setMetaUint(ctx, tx, "current_segment_no", nextNo); err != nil {
		_ = os.Remove(path)
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return wrapError(CodeRotateFailed, err, "set current segment")
	}
	if err := setMetaUint(ctx, tx, "next_segment_no", nextNo+1); err != nil {
		_ = os.Remove(path)
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return wrapError(CodeRotateFailed, err, "set next segment number")
	}
	if err := updateIntentStatus(ctx, tx, intentID, "DONE"); err != nil {
		_ = os.Remove(path)
		return wrapError(CodeRotateFailed, err, "complete initial segment intent")
	}
	if err := tx.Commit(); err != nil {
		_ = os.Remove(path)
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return wrapError(CodeRotateFailed, err, "commit initial segment metadata")
	}
	return nil
}

const hexZeroDigest = "0000000000000000000000000000000000000000000000000000000000000000"

// rotateLocked seals the current OPEN segment and atomically opens the next.
// If any step before the SQLite commit fails, the new file is removed and the
// original segment remains OPEN and usable.
func (e *Engine) rotateLocked(ctx context.Context) (SegmentInfo, error) {
	cur, err := e.ensureCurrentLocked(ctx)
	if err != nil {
		return SegmentInfo{}, err
	}
	if cur.State != SegmentOpen {
		er := newError(CodeRotateFailed, "current segment %d is %s", cur.SegmentNo, cur.State)
		er.SegmentNo = cur.SegmentNo
		return SegmentInfo{}, er
	}
	if err := e.fault(FaultBeforeRotate, cur.SegmentNo, nil); err != nil {
		er := wrapError(CodeRotateFailed, err, "rotation rejected; original segment remains OPEN")
		er.SegmentNo = cur.SegmentNo
		er.ExpectedStartLSN = cur.StartLSN
		return SegmentInfo{}, er
	}
	oldPath := segmentPathByInfo(e.dir, cur)
	oldDigest, err := computeFileDigest(oldPath)
	if err != nil {
		er := wrapError(CodeRotateFailed, err, "digest current segment %d", cur.SegmentNo)
		er.SegmentNo = cur.SegmentNo
		return SegmentInfo{}, er
	}
	newNo, err := metaUint(ctx, e.db, "next_segment_no")
	if err != nil {
		return SegmentInfo{}, err
	}
	nextLSN, err := metaUint(ctx, e.db, "next_lsn")
	if err != nil {
		return SegmentInfo{}, err
	}
	payload := rotateIntentPayload{
		OldSegmentNo: cur.SegmentNo,
		NewSegmentNo: newNo,
		NewFileName:  segmentFileName(newNo),
		StartLSN:     nextLSN,
		PrevDigest:   oldDigest,
	}
	intentID, err := insertIntent(ctx, e.db, "ROTATE", "PENDING", payload)
	if err != nil {
		return SegmentInfo{}, err
	}
	h := SegmentHeader{
		Magic:      segmentMagic,
		Version:    segmentVersion,
		HeaderSize: segmentHeaderSize,
		SegmentNo:  newNo,
		StartLSN:   nextLSN,
		PrevDigest: oldDigest,
	}
	newPath, err := createSegmentFile(e.dir, h)
	if err != nil {
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		er := wrapError(CodeRotateFailed, err, "create segment %d", newNo)
		er.SegmentNo = cur.SegmentNo
		er.ExpectedStartLSN = nextLSN
		return SegmentInfo{}, er
	}
	newDigest, err := computeFileDigest(newPath)
	if err != nil {
		_ = os.Remove(newPath)
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return SegmentInfo{}, wrapError(CodeRotateFailed, err, "digest new segment")
	}
	tx, err := beginWrite(ctx, e.db)
	if err != nil {
		_ = os.Remove(newPath)
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return SegmentInfo{}, wrapError(CodeRotateFailed, err, "begin rotation metadata")
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE segments SET state=?,digest=? WHERE segment_no=? AND state=?`,
		string(SegmentSealed), oldDigest, cur.SegmentNo, string(SegmentOpen))
	if err != nil {
		_ = os.Remove(newPath)
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return SegmentInfo{}, wrapError(CodeRotateFailed, err, "seal segment %d", cur.SegmentNo)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		_ = os.Remove(newPath)
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return SegmentInfo{}, newError(CodeRotateFailed, "segment %d changed state before rotation", cur.SegmentNo)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO segments(`+segmentColumns+`)
		VALUES(?,?,?,?,?,?,?,?,?,?)`, newNo, segmentFileName(newNo), nextLSN, 0, oldDigest,
		uint64(0), uint64(0), int64(segmentHeaderSize), string(SegmentOpen), newDigest)
	if err != nil {
		_ = os.Remove(newPath)
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return SegmentInfo{}, wrapError(CodeRotateFailed, err, "insert segment %d", newNo)
	}
	if err := setMetaUint(ctx, tx, "current_segment_no", newNo); err != nil {
		_ = os.Remove(newPath)
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return SegmentInfo{}, wrapError(CodeRotateFailed, err, "set current segment")
	}
	if err := setMetaUint(ctx, tx, "next_segment_no", newNo+1); err != nil {
		_ = os.Remove(newPath)
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return SegmentInfo{}, wrapError(CodeRotateFailed, err, "set next segment number")
	}
	if err := updateIntentStatus(ctx, tx, intentID, "DONE"); err != nil {
		_ = os.Remove(newPath)
		return SegmentInfo{}, wrapError(CodeRotateFailed, err, "complete rotation intent")
	}
	if err := tx.Commit(); err != nil {
		_ = os.Remove(newPath)
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return SegmentInfo{}, wrapError(CodeRotateFailed, err, "commit rotation metadata")
	}
	return getSegment(ctx, e.db, newNo)
}

// SealCurrent transitions the current OPEN segment to SEALED and returns it.
// A later group commit will open a fresh segment.
func (e *Engine) SealCurrent(ctx context.Context) (SegmentInfo, error) {
	if err := checkClosed(e); err != nil {
		return SegmentInfo{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := checkClosed(e); err != nil {
		return SegmentInfo{}, err
	}
	cur, err := e.ensureCurrentLocked(ctx)
	if err != nil {
		return SegmentInfo{}, err
	}
	if cur.State != SegmentOpen {
		er := newError(CodeInvalidTransition, "segment state transition %s -> %s is not legal", cur.State, SegmentSealed)
		er.SegmentNo = cur.SegmentNo
		er.From = string(cur.State)
		er.To = string(SegmentSealed)
		return SegmentInfo{}, er
	}
	path := segmentPathByInfo(e.dir, cur)
	digest, err := computeFileDigest(path)
	if err != nil {
		return SegmentInfo{}, wrapError(CodeIO, err, "digest segment %d", cur.SegmentNo)
	}
	tx, err := beginWrite(ctx, e.db)
	if err != nil {
		return SegmentInfo{}, wrapError(CodeIO, err, "begin seal transaction")
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE segments SET state=?,digest=? WHERE segment_no=? AND state=?`,
		string(SegmentSealed), digest, cur.SegmentNo, string(SegmentOpen))
	if err != nil {
		return SegmentInfo{}, wrapError(CodeIO, err, "seal segment %d", cur.SegmentNo)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return SegmentInfo{}, invalidTransitionSegment(cur.SegmentNo, cur.State, SegmentSealed)
	}
	if err := setMetaUint(ctx, tx, "current_segment_no", 0); err != nil {
		return SegmentInfo{}, wrapError(CodeIO, err, "clear current segment")
	}
	if err := tx.Commit(); err != nil {
		return SegmentInfo{}, wrapError(CodeIO, err, "commit seal segment")
	}
	cur.State = SegmentSealed
	cur.Digest = digest
	return cur, nil
}

func invalidTransitionSegment(segNo uint64, from, to SegmentState) *Error {
	er := newError(CodeInvalidTransition, "segment state transition %s -> %s is not legal", from, to)
	er.SegmentNo = segNo
	er.From = string(from)
	er.To = string(to)
	return er
}

func invalidTransitionTxn(id string, from, to TransactionState) *Error {
	er := newError(CodeInvalidTransition, "transaction state transition %s -> %s is not legal", from, to)
	er.TxnID = id
	er.From = string(from)
	er.To = string(to)
	return er
}
