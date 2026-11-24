package walgroup

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"unicode/utf8"
)

type pageVersionRow struct {
	PageNo  int64
	Version uint64
	LSN     uint64
	TxnID   string
	Digest  string
	Data    []byte
}

// ReadPage returns the page version visible at snapshotLSN. Selection is always
// latest(version) where lsn <= snapshotLSN: this operation never returns page
// content newer than the requested snapshot. If a checkpoint is active, the
// page's latest committed version must be exactly the version covered by that
// checkpoint; otherwise E_CHECKPOINT_LAG is returned.
func (e *Engine) ReadPage(ctx context.Context, pageNo int64, snapshotLSN uint64) (PageResult, error) {
	if err := checkClosed(e); err != nil {
		return PageResult{}, err
	}
	if pageNo < 0 {
		return PageResult{}, newError(CodeInvalidArgument, "page number must not be negative")
	}
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return PageResult{}, wrapError(CodeIO, err, "begin read snapshot")
	}
	defer tx.Rollback()
	cp, cpRows, err := loadActiveCheckpoint(ctx, tx, true)
	if err != nil {
		return PageResult{}, err
	}
	minServiceable, err := minServiceableTx(ctx, tx, cp)
	if err != nil {
		return PageResult{}, err
	}
	if snapshotLSN < minServiceable {
		return PageResult{}, snapshotRetired(minServiceable, "requested LSN %d is older than minimum serviceable LSN %d", snapshotLSN, minServiceable)
	}
	selected, err := latestPageAt(ctx, tx, pageNo, snapshotLSN)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return PageResult{}, snapshotRetired(minServiceable, "page %d has no committed version at or before LSN %d", pageNo, snapshotLSN)
		}
		return PageResult{}, err
	}
	if cp != nil {
		latestOverall, err := latestPageOverall(ctx, tx, pageNo)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return PageResult{}, snapshotRetired(minServiceable, "page %d is not present in checkpoint %d", pageNo, cp.Sequence)
			}
			return PageResult{}, err
		}
		var cpPage *checkpointPageRow
		for i := range cpRows {
			if cpRows[i].PageNo == pageNo {
				cpPage = &cpRows[i]
				break
			}
		}
		if cpPage == nil || latestOverall.Version != cpPage.Version || latestOverall.Digest != cpPage.Digest {
			uncovered := latestOverall.LSN
			if cpPage == nil {
				uncovered = latestOverall.LSN
			}
			er := newError(CodeCheckpointLag, "page %d latest version %d at LSN %d is not covered by active checkpoint %d (LSN %d)",
				pageNo, latestOverall.Version, latestOverall.LSN, cp.Sequence, cp.ReplayStartLSN)
			er.EarliestUncovered = uncovered
			er.MinServiceableLSN = cp.ReplayStartLSN
			return PageResult{}, er
		}
		if selected.Version != cpPage.Version || selected.Digest != cpPage.Digest {
			er := newError(CodeCheckpointLag, "page %d snapshot LSN %d selects version %d, which is not the checkpoint-covered version %d",
				pageNo, snapshotLSN, selected.Version, cpPage.Version)
			er.EarliestUncovered = cpPage.LSN
			er.MinServiceableLSN = cp.ReplayStartLSN
			return PageResult{}, er
		}
		selected = pageVersionRow{
			PageNo: cpPage.PageNo, Version: cpPage.Version, LSN: cpPage.LSN,
			TxnID: selected.TxnID, Digest: cpPage.Digest, Data: cpPage.Data,
		}
	}
	res := PageResult{
		PageNo:            selected.PageNo,
		Version:           selected.Version,
		LSN:               selected.LSN,
		Digest:            selected.Digest,
		Data:              append([]byte(nil), selected.Data...),
		SnapshotLSN:       snapshotLSN,
		MinServiceableLSN: minServiceable,
	}
	if cp != nil {
		res.CheckpointSeq = cp.Sequence
	}
	if utf8.Valid(res.Data) {
		res.Value = string(res.Data)
	}
	return res, nil
}

func snapshotRetired(minServiceable uint64, format string, args ...any) *Error {
	e := newError(CodeSnapshotRetired, format, args...)
	e.MinServiceableLSN = minServiceable
	return e
}

func minServiceableTx(ctx context.Context, tx *sql.Tx, cp *Checkpoint) (uint64, error) {
	if cp != nil {
		return cp.ReplayStartLSN, nil
	}
	var min sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MIN(start_lsn) FROM segments WHERE state<>?`, string(SegmentReclaimed)).Scan(&min); err != nil {
		return 0, wrapError(CodeIO, err, "read minimum serviceable segment LSN")
	}
	if min.Valid {
		return uint64(min.Int64), nil
	}
	next, err := metaUint(ctx, tx, "next_lsn")
	if err != nil {
		return 1, nil
	}
	return next, nil
}

func latestPageAt(ctx context.Context, q dbTX, pageNo int64, snapshotLSN uint64) (pageVersionRow, error) {
	var p pageVersionRow
	err := q.QueryRowContext(ctx, `SELECT page_no,version,lsn,txn_id,digest,data
		FROM page_versions WHERE page_no=? AND lsn<=?
		ORDER BY version DESC,lsn DESC LIMIT 1`, pageNo, snapshotLSN).
		Scan(&p.PageNo, &p.Version, &p.LSN, &p.TxnID, &p.Digest, &p.Data)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return p, err
		}
		return p, wrapError(CodeIO, err, "read page %d at LSN %d", pageNo, snapshotLSN)
	}
	if digestBytes(p.Data) != p.Digest {
		return p, checkpointInvalid(0, "page %d version %d digest mismatch", p.PageNo, p.Version)
	}
	return p, nil
}

func latestPageOverall(ctx context.Context, q dbTX, pageNo int64) (pageVersionRow, error) {
	var p pageVersionRow
	err := q.QueryRowContext(ctx, `SELECT page_no,version,lsn,txn_id,digest,data
		FROM page_versions WHERE page_no=? ORDER BY version DESC,lsn DESC LIMIT 1`, pageNo).
		Scan(&p.PageNo, &p.Version, &p.LSN, &p.TxnID, &p.Digest, &p.Data)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return p, err
		}
		return p, wrapError(CodeIO, err, "read latest page %d", pageNo)
	}
	return p, nil
}

// ReplayFromCheckpoint starts at exactly the active checkpoint's replay start,
// validates every surviving segment record, and applies only DURABLE
// transaction records in LSN order. Page versions must advance monotonically.
func (e *Engine) ReplayFromCheckpoint(ctx context.Context) (ReplayResult, error) {
	if err := checkClosed(e); err != nil {
		return ReplayResult{}, err
	}
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ReplayResult{}, wrapError(CodeIO, err, "begin replay snapshot")
	}
	defer tx.Rollback()
	cp, cpRows, err := loadActiveCheckpoint(ctx, tx, true)
	if err != nil {
		return ReplayResult{}, err
	}
	if cp == nil {
		return ReplayResult{}, newError(CodeCheckpointInvalid, "replay requires an active checkpoint")
	}
	pages := make(map[int64]ReplayPage, len(cpRows))
	for _, p := range cpRows {
		pages[p.PageNo] = ReplayPage{
			PageNo: p.PageNo, Version: p.Version, LSN: p.LSN,
			Digest: p.Digest, Data: append([]byte(nil), p.Data...),
		}
	}
	segments, err := listSegments(ctx, tx, false)
	if err != nil {
		return ReplayResult{}, err
	}
	durable := make(map[string]bool)
	rows, err := tx.QueryContext(ctx, `SELECT id FROM transactions WHERE state=?`, string(TxnDurable))
	if err != nil {
		return ReplayResult{}, wrapError(CodeIO, err, "load durable transactions for replay")
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return ReplayResult{}, wrapError(CodeIO, err, "scan durable transaction")
		}
		durable[id] = true
	}
	rows.Close()
	type walRec struct{ rec Record }
	var records []walRec
	for _, seg := range segments {
		parsed, err := ParseSegment(segmentPathByInfo(e.dir, seg), seg.SegmentNo, seg.StartLSN)
		if err != nil {
			return ReplayResult{}, err
		}
		for _, rec := range parsed.Records {
			if rec.LSN <= cp.ReplayStartLSN {
				continue
			}
			if durable[rec.TxnID] {
				records = append(records, walRec{rec: rec})
			}
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].rec.LSN < records[j].rec.LSN })
	var lastLSN uint64
	count := 0
	if len(records) != 0 && records[0].rec.LSN != cp.ReplayStartLSN+1 {
		return ReplayResult{}, checkpointInvalid(cp.Sequence, "first replayed LSN is %d, want %d", records[0].rec.LSN, cp.ReplayStartLSN+1)
	}
	for _, item := range records {
		rec := item.rec
		cur, ok := pages[rec.PageNo]
		if ok && rec.Version <= cur.Version {
			return ReplayResult{}, checkpointInvalid(cp.Sequence, "page %d replays version %d after %d", rec.PageNo, rec.Version, cur.Version)
		}
		if lastLSN != 0 && rec.LSN != lastLSN+1 {
			return ReplayResult{}, checkpointInvalid(cp.Sequence, "replay LSN gap: got %d after %d", rec.LSN, lastLSN)
		}
		pages[rec.PageNo] = ReplayPage{
			PageNo: rec.PageNo, Version: rec.Version, LSN: rec.LSN,
			Digest: rec.Digest, Data: append([]byte(nil), rec.Data...),
		}
		lastLSN = rec.LSN
		count++
	}
	if lastLSN == 0 {
		lastLSN = cp.ReplayStartLSN
	}
	out := make([]ReplayPage, 0, len(pages))
	for _, p := range pages {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PageNo < out[j].PageNo })
	return ReplayResult{
		CheckpointSequence: cp.Sequence,
		ReplayStartLSN:     cp.ReplayStartLSN,
		LastReplayedLSN:    lastLSN,
		RecordsReplayed:    count,
		Pages:              out,
	}, nil
}
