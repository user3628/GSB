package walgroup

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"sort"
	"strings"
	"time"
)

func (e *Engine) recover(ctx context.Context) (RecoveryReport, error) {
	report := RecoveryReport{
		Status:        "CLEAN",
		FactsOnly:     true,
		Intents:       []IntentReport{},
		AbortedTxnIDs: []string{},
		Segments:      []SegmentDisposition{},
	}
	recovered, err := e.recoverIntents(ctx, &report)
	if err != nil {
		return report, err
	}
	aborted, err := e.abortNonDurable(ctx)
	if err != nil {
		return report, err
	}
	report.AbortedTxnIDs = append(report.AbortedTxnIDs, aborted...)
	sort.Strings(report.AbortedTxnIDs)
	actions, err := e.reconcileSegments(ctx, &report)
	if err != nil {
		return report, err
	}
	if err := e.repairNextLSN(ctx, &report); err != nil {
		return report, err
	}
	if err := e.reconcileCheckpoint(ctx, &report); err != nil {
		return report, err
	}
	if recovered || len(report.Intents) != 0 || len(report.AbortedTxnIDs) != 0 || actions {
		report.Status = "RECOVERED"
	}
	next, err := metaUint(ctx, e.db, "next_lsn")
	if err == nil {
		report.NextLSN = next
	}
	min, err := e.MinServiceableLSN(ctx)
	if err == nil {
		report.MinServiceableLSN = min
	}
	cp, _, err := loadActiveCheckpoint(ctx, e.db, true)
	if err != nil {
		return report, err
	}
	if cp != nil {
		report.ActiveCheckpointSequence = cp.Sequence
	}
	sort.Slice(report.Intents, func(i, j int) bool { return report.Intents[i].IntentID < report.Intents[j].IntentID })
	sort.Slice(report.Segments, func(i, j int) bool { return report.Segments[i].SegmentNo < report.Segments[j].SegmentNo })
	return report, nil
}

func (e *Engine) recoverIntents(ctx context.Context, report *RecoveryReport) (bool, error) {
	rows, err := e.db.QueryContext(ctx, `SELECT id,kind,status,payload FROM intents
		WHERE status IN ('PENDING','MARKED') ORDER BY id`)
	if err != nil {
		return false, wrapError(CodeIO, err, "scan recovery intents")
	}
	type intentRow struct {
		id     int64
		kind   string
		status string
		data   []byte
	}
	var intents []intentRow
	for rows.Next() {
		var it intentRow
		if err := rows.Scan(&it.id, &it.kind, &it.status, &it.data); err != nil {
			rows.Close()
			return false, wrapError(CodeIO, err, "scan recovery intent")
		}
		intents = append(intents, it)
	}
	rows.Close()
	recovered := false
	for _, it := range intents {
		ir := IntentReport{IntentID: it.id, Kind: it.kind, PriorState: it.status}
		switch it.kind {
		case "GROUP_COMMIT":
			if it.status == "PENDING" {
				var p groupIntentPayload
				if err := json.Unmarshal(it.data, &p); err != nil {
					return false, wrapError(CodeIO, err, "decode pending group intent %d", it.id)
				}
				if err := e.recoverPendingGroup(ctx, it.id, p, &ir, report); err != nil {
					return false, err
				}
				report.AbortedTxnIDs = append(report.AbortedTxnIDs, p.TxnIDs...)
				recovered = true
			} else {
				ir.Action = "NO_ACTION_" + it.status
			}
		case "ROTATE":
			if it.status == "PENDING" {
				var p rotateIntentPayload
				if err := json.Unmarshal(it.data, &p); err != nil {
					return false, wrapError(CodeIO, err, "decode pending rotate intent %d", it.id)
				}
				var count int
				if err := e.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM segments WHERE segment_no=?`, p.NewSegmentNo).Scan(&count); err != nil {
					return false, wrapError(CodeIO, err, "check pending rotated segment")
				}
				if count == 0 && p.NewSegmentNo != 0 {
					if err := os.Remove(segmentPath(e.dir, p.NewSegmentNo)); err != nil && !errors.Is(err, os.ErrNotExist) {
						return false, wrapError(CodeIO, err, "remove uncommitted rotated segment %d", p.NewSegmentNo)
					}
					ir.SegmentNos = append(ir.SegmentNos, p.NewSegmentNo)
				}
				if err := updateIntentStatus(ctx, e.db, it.id, "ROLLED_BACK"); err != nil {
					return false, wrapError(CodeIO, err, "mark rotate intent rolled back")
				}
				ir.Action = "REMOVED_UNCOMMITTED_SEGMENT"
				recovered = true
			}
		case "CHECKPOINT":
			if it.status == "PENDING" {
				if err := updateIntentStatus(ctx, e.db, it.id, "ROLLED_BACK"); err != nil {
					return false, wrapError(CodeIO, err, "mark checkpoint intent rolled back")
				}
				ir.Action = "CHECKPOINT_METADATA_UNCHANGED"
				recovered = true
			}
		case "RECLAIM":
			var p reclaimIntentPayload
			_ = json.Unmarshal(it.data, &p)
			switch it.status {
			case "PENDING":
				if err := updateIntentStatus(ctx, e.db, it.id, "ROLLED_BACK"); err != nil {
					return false, wrapError(CodeIO, err, "mark reclaim intent rolled back")
				}
				ir.Action = "NO_RECLAIMABLE_MARK_COMMITTED"
				ir.SegmentNos = p.SegmentNos
				recovered = true
			case "MARKED":
				for _, no := range p.SegmentNos {
					path := segmentPath(e.dir, no)
					if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
						return false, wrapError(CodeIO, err, "finish physical reclaim for segment %d", no)
					}
				}
				if err := e.finishReclaim(ctx, it.id, p.SegmentNos); err != nil {
					return false, err
				}
				ir.Action = "CONTINUED_RECLAIM_TO_RECLAIMED"
				ir.SegmentNos = p.SegmentNos
				recovered = true
			default:
				ir.Action = "NO_ACTION_" + it.status
			}
		default:
			ir.Action = "UNKNOWN_INTENT_KIND"
		}
		report.Intents = append(report.Intents, ir)
	}
	return recovered, nil
}

func (e *Engine) recoverPendingGroup(ctx context.Context, intentID int64, p groupIntentPayload, ir *IntentReport, report *RecoveryReport) error {
	ir.TxnIDs = append([]string(nil), p.TxnIDs...)
	ir.SegmentNos = make([]uint64, 0, len(p.Segments))
	for _, s := range p.Segments {
		ir.SegmentNos = append(ir.SegmentNos, s.SegmentNo)
	}
	// If SQLite has any DURABLE member, the metadata transaction committed and
	// the intent should not be rolled back. This state is impossible under the
	// normal commit ordering, but recovery reports it as a contradiction.
	var durable int
	for _, id := range p.TxnIDs {
		var state string
		err := e.db.QueryRowContext(ctx, `SELECT state FROM transactions WHERE id=?`, id).Scan(&state)
		if err == nil && TransactionState(state) == TxnDurable {
			durable++
		}
	}
	if durable == len(p.TxnIDs) && durable != 0 {
		if err := updateIntentStatus(ctx, e.db, intentID, "DONE"); err != nil {
			return wrapError(CodeIO, err, "repair committed group intent")
		}
		ir.Action = "VERIFIED_COMMITTED_GROUP"
		return nil
	}
	if durable != 0 {
		return newError(CodeGroupCommitFailed, "incomplete group intent %d has a mix of DURABLE and non-DURABLE transactions", intentID)
	}
	for _, s := range p.Segments {
		path := segmentPath(e.dir, s.SegmentNo)
		if _, err := os.Stat(path); err == nil {
			info := SegmentInfo{
				SegmentNo:   s.SegmentNo,
				StartLSN:    s.OldStartLSN,
				LastLSN:     s.OldLastLSN,
				PrevDigest:  s.PrevDigest,
				RecordCount: s.OldRecords,
				TxnCount:    s.OldTxns,
				ByteSize:    s.OldSize,
			}
			if err := truncateAndRestoreHeader(path, info); err != nil {
				return wrapError(CodeGroupCommitFailed, err, "rollback segment %d to intent checkpoint", s.SegmentNo)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return wrapError(CodeIO, err, "stat rollback segment %d", s.SegmentNo)
		}
	}
	tx, err := beginWrite(ctx, e.db)
	if err != nil {
		return wrapError(CodeIO, err, "begin group recovery transaction")
	}
	defer tx.Rollback()
	now := time.Now().UnixMilli()
	for _, id := range p.TxnIDs {
		if _, err := tx.ExecContext(ctx, `UPDATE transactions SET state=?,updated_at=?
			WHERE id=? AND state IN (?,?)`, string(TxnAborted), now, id, string(TxnAppending), string(TxnGrouped)); err != nil {
			return wrapError(CodeIO, err, "abort transaction %q during recovery", id)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM txn_staging WHERE txn_id=?`, id); err != nil {
			return wrapError(CodeIO, err, "clear staging for transaction %q", id)
		}
	}
	for _, s := range p.Segments {
		digest := ""
		if path := segmentPath(e.dir, s.SegmentNo); fileExists(path) {
			digest, _ = computeFileDigest(path)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE segments SET record_count=?,txn_count=?,last_lsn=?,byte_size=?,digest=?
			WHERE segment_no=?`, s.OldRecords, s.OldTxns, s.OldLastLSN, s.OldSize, digest, s.SegmentNo); err != nil {
			return wrapError(CodeIO, err, "restore segment %d metadata", s.SegmentNo)
		}
	}
	if p.StartLSN > 0 {
		if err := setMetaUint(ctx, tx, "next_lsn", p.StartLSN); err != nil {
			return wrapError(CodeIO, err, "restore next LSN")
		}
	}
	if err := updateIntentStatus(ctx, tx, intentID, "ROLLED_BACK"); err != nil {
		return wrapError(CodeIO, err, "mark group intent rolled back")
	}
	if err := tx.Commit(); err != nil {
		return wrapError(CodeIO, err, "commit group recovery rollback")
	}
	ir.Action = "ROLLED_BACK_GROUP_AND_ABORTED_TXNS"
	for _, s := range p.Segments {
		report.Segments = append(report.Segments, SegmentDisposition{
			SegmentNo: s.SegmentNo, Action: "TRUNCATED_PENDING_GROUP", State: SegmentOpen,
			StartLSN: s.OldStartLSN, LastLSN: s.OldLastLSN, RecordCount: s.OldRecords,
			Reason: "intent was PENDING; no transaction may remain DURABLE",
		})
	}
	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func (e *Engine) abortNonDurable(ctx context.Context) ([]string, error) {
	rows, err := e.db.QueryContext(ctx, `SELECT id FROM transactions WHERE state IN (?,?) ORDER BY id`,
		string(TxnAppending), string(TxnGrouped))
	if err != nil {
		return nil, wrapError(CodeIO, err, "scan non-durable transactions")
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, wrapError(CodeIO, err, "scan non-durable transaction id")
		}
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) == 0 {
		return nil, nil
	}
	tx, err := beginWrite(ctx, e.db)
	if err != nil {
		return nil, wrapError(CodeIO, err, "begin abort non-durable transactions")
	}
	defer tx.Rollback()
	now := time.Now().UnixMilli()
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `UPDATE transactions SET state=?,updated_at=?
			WHERE id=? AND state IN (?,?)`, string(TxnAborted), now, id, string(TxnAppending), string(TxnGrouped)); err != nil {
			return nil, wrapError(CodeIO, err, "abort transaction %q", id)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM txn_staging WHERE txn_id=?`, id); err != nil {
			return nil, wrapError(CodeIO, err, "clear staging for transaction %q", id)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, wrapError(CodeIO, err, "commit recovery aborts")
	}
	return ids, nil
}

type parsedSurvivor struct {
	info SegmentInfo
	p    *ParsedSegment
}

func (e *Engine) reconcileSegments(ctx context.Context, report *RecoveryReport) (bool, error) {
	changed := false
	all, err := listSegments(ctx, e.db, true)
	if err != nil {
		return false, err
	}
	byNo := make(map[uint64]SegmentInfo, len(all))
	for i := range all {
		seg := all[i]
		byNo[seg.SegmentNo] = seg
		path := segmentPathByInfo(e.dir, seg)
		switch seg.State {
		case SegmentReclaimed:
			if err := os.Remove(path); err == nil {
				changed = true
				report.Segments = append(report.Segments, disposition(seg, "REMOVED_PERSISTED_RECLAIMED_ORPHAN", "catalog was already RECLAIMED"))
			} else if !errors.Is(err, os.ErrNotExist) {
				return false, wrapError(CodeIO, err, "remove reclaimed segment %d", seg.SegmentNo)
			}
		case SegmentReclaimable:
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return false, wrapError(CodeIO, err, "complete interrupted reclaim of segment %d", seg.SegmentNo)
			}
			if err := e.markReclaimedDirect(ctx, seg.SegmentNo); err != nil {
				return false, err
			}
			seg.State = SegmentReclaimed
			changed = true
			report.Segments = append(report.Segments, disposition(seg, "COMPLETED_RECLAIM_AFTER_RESTART", "persisted state was RECLAIMABLE"))
			all[i] = seg
			byNo[seg.SegmentNo] = seg
		}
	}

	// Remove temp files and final-name orphans not present in the catalog.
	entries, err := os.ReadDir(e.dir)
	if err != nil {
		return false, wrapError(CodeIO, err, "read data directory")
	}
	for _, ent := range entries {
		name := ent.Name()
		if strings.Contains(name, ".tmp") {
			_ = os.Remove(joinDir(e.dir, name))
			changed = true
			report.Segments = append(report.Segments, SegmentDisposition{Action: "REMOVED_TEMP_FILE"})
			continue
		}
		no, ok := parseSegmentName(name)
		if !ok {
			continue
		}
		if _, exists := byNo[no]; exists {
			continue
		}
		if err := os.Remove(joinDir(e.dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, wrapError(CodeIO, err, "remove orphan segment file %s", name)
		}
		changed = true
		report.Segments = append(report.Segments, SegmentDisposition{SegmentNo: no, Action: "REMOVED_UNCATALOGED_ORPHAN"})
	}

	var survivors []parsedSurvivor
	var prev *parsedSurvivor
	var prevWithRecords *parsedSurvivor
	for _, seg := range sortedSegmentInfos(all) {
		if seg.State == SegmentReclaimed {
			continue
		}
		path := segmentPathByInfo(e.dir, seg)
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return false, segmentCorrupt(seg.SegmentNo, seg.StartLSN, "segment file is missing")
		}
		parsed, err := ParseSegment(path, seg.SegmentNo, seg.StartLSN)
		if err != nil {
			return false, err
		}
		if parsed.Header.RecordCount != seg.RecordCount || parsed.Header.TxnCount != seg.TxnCount ||
			parsed.Header.LastLSN != seg.LastLSN {
			return false, segmentCorrupt(seg.SegmentNo, seg.StartLSN, "catalog counts {%d,%d,%d} disagree with header {%d,%d,%d}",
				seg.RecordCount, seg.TxnCount, seg.LastLSN,
				parsed.Header.RecordCount, parsed.Header.TxnCount, parsed.Header.LastLSN)
		}
		st, _ := os.Stat(path)
		if st.Size() != seg.ByteSize {
			return false, segmentCorrupt(seg.SegmentNo, seg.StartLSN, "catalog byte size %d disagrees with file size %d", seg.ByteSize, st.Size())
		}
		if prev != nil || seg.SegmentNo > 1 {
			start := uint64(1)
			if prev != nil {
				start = prev.info.SegmentNo + 1
			}
			for n := start; n < seg.SegmentNo; n++ {
				gapSeg, ok := byNo[n]
				if !ok || gapSeg.State != SegmentReclaimed {
					return false, segmentCorrupt(seg.SegmentNo, seg.StartLSN,
						"segment-number gap at %d is not a persisted RECLAIMED segment", n)
				}
			}
		}
		if len(parsed.Records) > 0 {
			if parsed.Records[0].LSN != seg.StartLSN {
				return false, segmentCorrupt(seg.SegmentNo, seg.StartLSN, "first record LSN is %d", parsed.Records[0].LSN)
			}
			if prevWithRecords != nil {
				prevLast := prevWithRecords.p.Records[len(prevWithRecords.p.Records)-1].LSN
				if prevWithRecords.info.SegmentNo+1 == seg.SegmentNo {
					if parsed.Records[0].LSN != prevLast+1 {
						return false, segmentCorrupt(seg.SegmentNo, prevLast+1,
							"cross-segment LSN gap: first record is %d", parsed.Records[0].LSN)
					}
				} else if parsed.Records[0].LSN <= prevLast {
					return false, segmentCorrupt(seg.SegmentNo, prevLast+1,
						"first record LSN %d does not advance past surviving predecessor %d", parsed.Records[0].LSN, prevLast)
				}
			}
		}
		if prev != nil && prev.info.State != SegmentReclaimed && prev.info.SegmentNo+1 == seg.SegmentNo {
			if parsed.Header.PrevDigest != prev.p.Digest {
				return false, segmentCorrupt(seg.SegmentNo, seg.StartLSN, "previous-segment digest mismatch")
			}
		}
		action := "VERIFIED_OPEN"
		if seg.State == SegmentSealed {
			action = "VERIFIED_SEALED"
		}
		report.Segments = append(report.Segments, disposition(seg, action, ""))
		survivors = append(survivors, parsedSurvivor{info: seg, p: parsed})
		if len(parsed.Records) != 0 {
			prevWithRecords = &survivors[len(survivors)-1]
		}
		prev = &survivors[len(survivors)-1]
	}

	// Validate catalog/current pointer coherence.
	currentNo, err := metaUint(ctx, e.db, "current_segment_no")
	if err != nil {
		return false, err
	}
	if currentNo != 0 {
		found := false
		for _, s := range survivors {
			if s.info.SegmentNo == currentNo {
				found = true
				if s.info.State != SegmentOpen {
					return false, segmentCorrupt(s.info.SegmentNo, s.info.StartLSN, "current segment is %s, want OPEN", s.info.State)
				}
			} else if s.info.State == SegmentOpen {
				return false, segmentCorrupt(s.info.SegmentNo, s.info.StartLSN, "non-current segment is OPEN")
			}
		}
		if !found {
			return false, segmentCorrupt(currentNo, 0, "current segment is not present")
		}
	} else {
		for _, s := range survivors {
			if s.info.State == SegmentOpen {
				return false, segmentCorrupt(s.info.SegmentNo, s.info.StartLSN, "OPEN segment exists but current pointer is zero")
			}
		}
	}
	return changed, nil
}

func joinDir(dir, name string) string {
	if strings.HasPrefix(name, "/") {
		return name
	}
	return dir + string(os.PathSeparator) + name
}

func disposition(seg SegmentInfo, action, reason string) SegmentDisposition {
	return SegmentDisposition{
		SegmentNo: seg.SegmentNo, Action: action, State: seg.State,
		StartLSN: seg.StartLSN, LastLSN: seg.LastLSN,
		RecordCount: seg.RecordCount, Reason: reason,
	}
}

func (e *Engine) markReclaimedDirect(ctx context.Context, segNo uint64) error {
	tx, err := beginWrite(ctx, e.db)
	if err != nil {
		return wrapError(CodeIO, err, "begin reclaimed state repair")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE segments SET state=? WHERE segment_no=? AND state=?`,
		string(SegmentReclaimed), segNo, string(SegmentReclaimable)); err != nil {
		return wrapError(CodeIO, err, "repair segment %d RECLAIMED state", segNo)
	}
	if err := tx.Commit(); err != nil {
		return wrapError(CodeIO, err, "commit reclaimed state repair")
	}
	return nil
}

func (e *Engine) repairNextLSN(ctx context.Context, report *RecoveryReport) error {
	var maxDurable sql.NullInt64
	if err := e.db.QueryRowContext(ctx, `SELECT MAX(end_lsn) FROM transactions WHERE state=?`, string(TxnDurable)).Scan(&maxDurable); err != nil {
		return wrapError(CodeIO, err, "read maximum durable LSN")
	}
	var maxSegment sql.NullInt64
	if err := e.db.QueryRowContext(ctx, `SELECT MAX(last_lsn) FROM segments WHERE state<>?`, string(SegmentReclaimed)).Scan(&maxSegment); err != nil {
		return wrapError(CodeIO, err, "read maximum segment LSN")
	}
	maxLSN := uint64(0)
	if maxDurable.Valid && maxDurable.Int64 > 0 {
		maxLSN = uint64(maxDurable.Int64)
	}
	if maxSegment.Valid && maxSegment.Int64 > 0 && uint64(maxSegment.Int64) > maxLSN {
		maxLSN = uint64(maxSegment.Int64)
	}
	wantNext := maxLSN + 1
	if wantNext == 0 {
		wantNext = 1
	}
	gotNext, err := metaUint(ctx, e.db, "next_lsn")
	if err != nil {
		return err
	}
	if gotNext != wantNext {
		if gotNext < wantNext {
			return segmentCorrupt(0, wantNext, "metadata next LSN %d is behind persisted maximum %d", gotNext, maxLSN)
		}
		if err := setMetaUint(ctx, e.db, "next_lsn", wantNext); err != nil {
			return wrapError(CodeIO, err, "repair next LSN")
		}
	}
	// An empty current segment may carry a future start LSN after a rolled-back
	// intent. Empty means there is no historical fact to preserve.
	currentNo, err := metaUint(ctx, e.db, "current_segment_no")
	if err != nil {
		return err
	}
	if currentNo != 0 {
		seg, err := getSegment(ctx, e.db, currentNo)
		if err == nil && seg.RecordCount == 0 && seg.State == SegmentOpen && seg.StartLSN != wantNext {
			if err := e.rewriteEmptySegmentStart(ctx, seg, wantNext); err != nil {
				return err
			}
		}
	}
	report.NextLSN = wantNext
	return nil
}

func (e *Engine) rewriteEmptySegmentStart(ctx context.Context, seg SegmentInfo, start uint64) error {
	path := segmentPathByInfo(e.dir, seg)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return wrapError(CodeIO, err, "open empty segment %d", seg.SegmentNo)
	}
	defer f.Close()
	h := SegmentHeader{
		Magic: segmentMagic, Version: segmentVersion, HeaderSize: segmentHeaderSize,
		SegmentNo: seg.SegmentNo, StartLSN: start, PrevDigest: seg.PrevDigest,
	}
	if err := writeHeaderFile(f, h); err != nil {
		return wrapError(CodeIO, err, "rewrite empty segment start LSN")
	}
	if err := f.Sync(); err != nil {
		return wrapError(CodeIO, err, "sync rewritten empty segment header")
	}
	digest, _ := computeFileDigest(path)
	tx, err := beginWrite(ctx, e.db)
	if err != nil {
		return wrapError(CodeIO, err, "begin empty segment start repair")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE segments SET start_lsn=?,digest=? WHERE segment_no=? AND record_count=0`,
		start, digest, seg.SegmentNo); err != nil {
		return wrapError(CodeIO, err, "repair empty segment start metadata")
	}
	if err := tx.Commit(); err != nil {
		return wrapError(CodeIO, err, "commit empty segment start repair")
	}
	return nil
}

func (e *Engine) reconcileCheckpoint(ctx context.Context, report *RecoveryReport) error {
	cp, _, err := loadActiveCheckpoint(ctx, e.db, true)
	if err != nil {
		return err
	}
	if cp != nil {
		report.ActiveCheckpointSequence = cp.Sequence
	}
	return nil
}
