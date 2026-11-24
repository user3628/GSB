package walgroup

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

type groupSegmentRollback struct {
	SegmentNo   uint64 `json:"segment_no"`
	FileName    string `json:"file_name"`
	OldSize     int64  `json:"old_size"`
	OldRecords  uint64 `json:"old_record_count"`
	OldTxns     uint64 `json:"old_txn_count"`
	OldLastLSN  uint64 `json:"old_last_lsn"`
	OldStartLSN uint64 `json:"old_start_lsn"`
	PrevDigest  string `json:"prev_digest"`
}

type groupIntentPayload struct {
	GroupID  string                 `json:"group_id"`
	StartLSN uint64                 `json:"start_lsn"`
	EndLSN   uint64                 `json:"end_lsn"`
	TxnIDs   []string               `json:"txn_ids"`
	Segments []groupSegmentRollback `json:"segments"`
}

type preparedRecord struct {
	PageNo  int64
	Version uint64
	LSN     uint64
	Digest  string
	Data    []byte
	Offset  int64
	Length  int
}

type preparedTxn struct {
	Input       TransactionInput
	Fingerprint string
	Records     []preparedRecord
	SegmentNo   uint64
	StartLSN    uint64
	EndLSN      uint64
	StartOffset int64
	EndOffset   int64
	Duplicate   bool
}

// StageTransaction persists a transaction in APPENDING state without writing
// WAL records. It is optional; CommitGroup also accepts complete transactions
// directly. Any APPENDING/GROUPED transaction left by a crash is aborted during
// recovery.
func (e *Engine) StageTransaction(ctx context.Context, in TransactionInput) (Transaction, error) {
	if err := checkClosed(e); err != nil {
		return Transaction{}, err
	}
	if err := in.validate(); err != nil {
		return Transaction{}, err
	}
	fp, err := in.Fingerprint()
	if err != nil {
		return Transaction{}, err
	}
	payload, err := json.Marshal(in)
	if err != nil {
		return Transaction{}, wrapError(CodeInvalidArgument, err, "encode staged transaction")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := checkClosed(e); err != nil {
		return Transaction{}, err
	}
	existing, err := getTxn(ctx, e.db, in.ID)
	if err == nil {
		if existing.Fingerprint != fp {
			return Transaction{}, conflictingTxn(in.ID)
		}
		switch existing.State {
		case TxnDurable:
			return existing, nil
		case TxnAppending:
			return existing, nil
		case TxnGrouped:
			return Transaction{}, invalidTransitionTxn(in.ID, existing.State, TxnAppending)
		case TxnAborted:
			return Transaction{}, invalidTransitionTxn(in.ID, existing.State, TxnAppending)
		}
	} else if CodeOf(err) != CodeNotFound {
		return Transaction{}, err
	}
	now := time.Now().UnixMilli()
	tx, err := beginWrite(ctx, e.db)
	if err != nil {
		return Transaction{}, wrapError(CodeIO, err, "begin stage transaction")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO transactions(id,fingerprint,state,created_at,updated_at)
		VALUES(?,?,?,?,?)`, in.ID, fp, string(TxnAppending), now, now); err != nil {
		return Transaction{}, wrapError(CodeIO, err, "insert staged transaction")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO txn_staging(txn_id,payload) VALUES(?,?)`, in.ID, payload); err != nil {
		return Transaction{}, wrapError(CodeIO, err, "persist staged transaction payload")
	}
	if err := tx.Commit(); err != nil {
		return Transaction{}, wrapError(CodeIO, err, "commit staged transaction")
	}
	return getTxn(ctx, e.db, in.ID)
}

func conflictingTxn(id string) *Error {
	e := newError(CodeConflictingTxn, "transaction %q was already persisted with different content", id)
	e.TxnID = id
	return e
}

// Commit is a convenience wrapper for a one-transaction group.
func (e *Engine) Commit(ctx context.Context, in TransactionInput) (CommitResult, error) {
	res, err := e.CommitGroup(ctx, []TransactionInput{in})
	if err != nil {
		return CommitResult{}, err
	}
	if len(res.Transactions) != 1 {
		return CommitResult{}, newError(CodeIO, "unexpected group result size")
	}
	return res.Transactions[0], nil
}

// CommitStagedGroup commits previously staged transaction IDs with one fsync
// barrier.
func (e *Engine) CommitStagedGroup(ctx context.Context, ids []string) (GroupResult, error) {
	if err := checkClosed(e); err != nil {
		return GroupResult{}, err
	}
	if len(ids) == 0 {
		return GroupResult{}, newError(CodeInvalidArgument, "transaction_ids must not be empty")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := checkClosed(e); err != nil {
		return GroupResult{}, err
	}
	results := make([]CommitResult, len(ids))
	inputs := make([]TransactionInput, 0, len(ids))
	var pendingPositions []int
	seen := make(map[string]bool)
	for i, id := range ids {
		existing, err := getTxn(ctx, e.db, id)
		if err != nil {
			return GroupResult{}, err
		}
		if existing.State == TxnDurable {
			results[i] = CommitResult{Transaction: existing, Duplicate: true}
			continue
		}
		if existing.State != TxnAppending {
			return GroupResult{}, invalidTransitionTxn(id, existing.State, TxnGrouped)
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		var payload []byte
		err = e.db.QueryRowContext(ctx, `SELECT s.payload FROM txn_staging s
			JOIN transactions t ON t.id=s.txn_id WHERE t.id=? AND t.state=?`, id, string(TxnAppending)).Scan(&payload)
		if errors.Is(err, sql.ErrNoRows) {
			return GroupResult{}, newError(CodeNotFound, "staged transaction %q not found", id)
		}
		if err != nil {
			return GroupResult{}, wrapError(CodeIO, err, "load staged transaction %q", id)
		}
		var in TransactionInput
		if err := json.Unmarshal(payload, &in); err != nil {
			return GroupResult{}, wrapError(CodeIO, err, "decode staged transaction %q", id)
		}
		fp, err := in.Fingerprint()
		if err != nil {
			return GroupResult{}, err
		}
		if fp != existing.Fingerprint {
			return GroupResult{}, conflictingTxn(id)
		}
		inputs = append(inputs, in)
		pendingPositions = append(pendingPositions, i)
	}
	if len(inputs) == 0 {
		return GroupResult{Transactions: results, FsyncCount: 0}, nil
	}
	committed, err := e.commitGroupLocked(ctx, inputs)
	if err != nil {
		return GroupResult{}, err
	}
	byID := make(map[string]CommitResult, len(committed.Transactions))
	for _, r := range committed.Transactions {
		byID[r.ID] = r
	}
	for _, pos := range pendingPositions {
		r, ok := byID[ids[pos]]
		if !ok {
			return GroupResult{}, newError(CodeIO, "staged transaction %q missing from commit result", ids[pos])
		}
		results[pos] = r
	}
	for i := range ids {
		if results[i].ID == "" {
			r, ok := byID[ids[i]]
			if !ok {
				return GroupResult{}, newError(CodeIO, "staged transaction %q missing from duplicate result", ids[i])
			}
			r.Duplicate = true
			results[i] = r
		}
	}
	return GroupResult{
		Transactions: results, StartLSN: committed.StartLSN, EndLSN: committed.EndLSN, FsyncCount: committed.FsyncCount,
	}, nil
}

// CommitGroup appends all supplied transactions to the current OPEN segment
// and commits them through one fsync barrier. The whole group fits in one
// segment; if it does not, E_ROTATE_FAILED is returned and no WAL bytes are
// written. Individual transactions are never split across segments.
func (e *Engine) CommitGroup(ctx context.Context, inputs []TransactionInput) (GroupResult, error) {
	if err := checkClosed(e); err != nil {
		return GroupResult{}, err
	}
	if len(inputs) == 0 {
		return GroupResult{}, newError(CodeInvalidArgument, "transactions must not be empty")
	}
	return e.commitGroup(ctx, inputs)
}

func (e *Engine) commitGroup(ctx context.Context, inputs []TransactionInput) (GroupResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := checkClosed(e); err != nil {
		return GroupResult{}, err
	}
	return e.commitGroupLocked(ctx, inputs)
}

// commitGroupLocked implements a group commit while e.mu is already held.
func (e *Engine) commitGroupLocked(ctx context.Context, inputs []TransactionInput) (GroupResult, error) {

	results := make([]CommitResult, len(inputs))
	seen := make(map[string]string)
	var pending []int
	for i, in := range inputs {
		if err := in.validate(); err != nil {
			return GroupResult{}, err
		}
		fp, err := in.Fingerprint()
		if err != nil {
			return GroupResult{}, err
		}
		if prev, ok := seen[in.ID]; ok {
			if prev != fp {
				return GroupResult{}, conflictingTxn(in.ID)
			}
			// Identical duplicate inside one call is represented by the first
			// occurrence; later occurrences will be filled from its result.
			continue
		}
		seen[in.ID] = fp
		existing, err := getTxn(ctx, e.db, in.ID)
		if err != nil {
			if CodeOf(err) != CodeNotFound {
				return GroupResult{}, err
			}
			pending = append(pending, i)
			continue
		}
		if existing.Fingerprint != fp {
			return GroupResult{}, conflictingTxn(in.ID)
		}
		switch existing.State {
		case TxnDurable:
			results[i] = CommitResult{Transaction: existing, Duplicate: true}
		case TxnAppending, TxnGrouped:
			pending = append(pending, i)
		case TxnAborted:
			return GroupResult{}, invalidTransitionTxn(in.ID, existing.State, TxnGrouped)
		default:
			return GroupResult{}, newError(CodeIO, "unknown transaction state %q", existing.State)
		}
	}
	if len(pending) == 0 {
		// Fill duplicate occurrences from the first persisted result.
		for i := range inputs {
			if results[i].ID == "" {
				existing, err := getTxn(ctx, e.db, inputs[i].ID)
				if err != nil {
					return GroupResult{}, err
				}
				results[i] = CommitResult{Transaction: existing, Duplicate: true}
			}
		}
		return GroupResult{Transactions: results, FsyncCount: 0}, nil
	}

	prepared, err := e.prepareGroupLocked(ctx, pending, inputs)
	if err != nil {
		return GroupResult{}, err
	}
	cur, err := e.ensureCurrentLocked(ctx)
	if err != nil {
		return GroupResult{}, err
	}
	var groupBytes int64
	for _, p := range prepared {
		for _, r := range p.Records {
			groupBytes += int64(r.Length)
		}
	}
	if groupBytes+int64(segmentHeaderSize) > e.opts.SegmentMaxBytes {
		er := newError(CodeRotateFailed, "commit group needs %d bytes, segment limit is %d", groupBytes+int64(segmentHeaderSize), e.opts.SegmentMaxBytes)
		er.SegmentNo = cur.SegmentNo
		er.ExpectedStartLSN = cur.StartLSN
		return GroupResult{}, er
	}
	if len(prepared) > e.opts.MaxTxnsPerSegment {
		er := newError(CodeRotateFailed, "commit group has %d transactions, segment transaction limit is %d", len(prepared), e.opts.MaxTxnsPerSegment)
		er.SegmentNo = cur.SegmentNo
		return GroupResult{}, er
	}
	if cur.TxnCount > 0 && (cur.TxnCount+uint64(len(prepared)) > uint64(e.opts.MaxTxnsPerSegment) ||
		cur.ByteSize+groupBytes > e.opts.SegmentMaxBytes) {
		cur, err = e.rotateLocked(ctx)
		if err != nil {
			return GroupResult{}, err
		}
	}
	if cur.ByteSize+groupBytes > e.opts.SegmentMaxBytes {
		er := newError(CodeRotateFailed, "commit group does not fit in fresh segment %d", cur.SegmentNo)
		er.SegmentNo = cur.SegmentNo
		er.ExpectedStartLSN = cur.StartLSN
		return GroupResult{}, er
	}

	nextLSN, err := metaUint(ctx, e.db, "next_lsn")
	if err != nil {
		return GroupResult{}, err
	}
	offset := cur.ByteSize
	lsn := nextLSN
	for pi := range prepared {
		p := &prepared[pi]
		p.SegmentNo = cur.SegmentNo
		p.StartOffset = offset
		p.StartLSN = lsn
		for ri := range p.Records {
			r := &p.Records[ri]
			r.LSN = lsn
			r.Offset = offset
			offset += int64(r.Length)
			lsn++
		}
		p.EndLSN = lsn - 1
		p.EndOffset = offset
	}
	endLSN := lsn - 1
	newSize := offset
	newRecordCount := cur.RecordCount
	newTxnCount := cur.TxnCount + uint64(len(prepared))
	for _, p := range prepared {
		newRecordCount += uint64(len(p.Records))
	}
	rollback := groupSegmentRollback{
		SegmentNo:   cur.SegmentNo,
		FileName:    segmentFileName(cur.SegmentNo),
		OldSize:     cur.ByteSize,
		OldRecords:  cur.RecordCount,
		OldTxns:     cur.TxnCount,
		OldLastLSN:  cur.LastLSN,
		OldStartLSN: cur.StartLSN,
		PrevDigest:  cur.PrevDigest,
	}
	payload := groupIntentPayload{
		GroupID:  fmt.Sprintf("group-%d-%d", nextLSN, endLSN),
		StartLSN: nextLSN,
		EndLSN:   endLSN,
		TxnIDs:   pendingTxnIDs(prepared),
		Segments: []groupSegmentRollback{rollback},
	}
	intentID, err := e.persistGroupIntentLocked(ctx, payload, prepared)
	if err != nil {
		return GroupResult{}, err
	}

	rolledBack := false
	rollbackFn := func(cause error) error {
		if rolledBack {
			return cause
		}
		rolledBack = true
		path := segmentPath(e.dir, cur.SegmentNo)
		restoreErr := truncateAndRestoreHeader(path, SegmentInfo{
			SegmentNo:   rollback.SegmentNo,
			StartLSN:    rollback.OldStartLSN,
			LastLSN:     rollback.OldLastLSN,
			PrevDigest:  rollback.PrevDigest,
			RecordCount: rollback.OldRecords,
			TxnCount:    rollback.OldTxns,
			ByteSize:    rollback.OldSize,
		})
		if restoreErr != nil {
			// Keep the intent PENDING and transactions GROUPED: only the WAL
			// bytes are durable in this case, so restart recovery must retry the
			// physical rollback before aborting the group.
			e.needsRecovery.Store(true)
			combined := errors.Join(cause, fmt.Errorf("physical rollback: %w", restoreErr))
			er := wrapError(CodeGroupCommitFailed, combined,
				"group commit failed and physical rollback is incomplete; restart recovery is required")
			if e2, ok := AsError(cause); ok {
				er.SegmentNo = e2.SegmentNo
				er.ExpectedStartLSN = e2.ExpectedStartLSN
			}
			return er
		}
		if rollbackErr := e.markGroupRolledBack(ctx, intentID, payload.TxnIDs, reasonOf(cause)); rollbackErr != nil {
			// The WAL file is restored, but the intent remains PENDING. Recovery
			// will replay this idempotent rollback and abort the group.
			e.needsRecovery.Store(true)
			combined := errors.Join(cause, fmt.Errorf("metadata rollback: %w", rollbackErr))
			er := wrapError(CodeGroupCommitFailed, combined,
				"group commit failed and metadata rollback is incomplete; restart recovery is required")
			if e2, ok := AsError(cause); ok {
				er.SegmentNo = e2.SegmentNo
				er.ExpectedStartLSN = e2.ExpectedStartLSN
			}
			return er
		}
		er := wrapError(CodeGroupCommitFailed, cause, "group commit rolled back; no transaction became DURABLE")
		if e2, ok := AsError(cause); ok {
			er.SegmentNo = e2.SegmentNo
			er.ExpectedStartLSN = e2.ExpectedStartLSN
		}
		return er
	}

	if err := e.writeGroupLocked(ctx, cur, prepared, newRecordCount, newTxnCount, endLSN, newSize, payload.TxnIDs); err != nil {
		return GroupResult{}, rollbackFn(err)
	}
	digest, err := computeFileDigest(segmentPath(e.dir, cur.SegmentNo))
	if err != nil {
		return GroupResult{}, rollbackFn(wrapError(CodeIO, err, "digest committed group segment"))
	}
	if err := e.fault(FaultBeforeGroupMetadata, cur.SegmentNo, payload.TxnIDs); err != nil {
		return GroupResult{}, rollbackFn(err)
	}
	if err := e.commitGroupMetadataLocked(ctx, intentID, cur.SegmentNo, prepared, newRecordCount, newTxnCount,
		endLSN, newSize, digest); err != nil {
		return GroupResult{}, rollbackFn(err)
	}

	byID := make(map[string]CommitResult, len(prepared))
	for _, p := range prepared {
		byID[p.Input.ID] = CommitResult{Transaction: Transaction{
			ID:          p.Input.ID,
			Fingerprint: p.Fingerprint,
			State:       TxnDurable,
			SegmentNo:   p.SegmentNo,
			StartLSN:    p.StartLSN,
			EndLSN:      p.EndLSN,
			StartOffset: p.StartOffset,
			EndOffset:   p.EndOffset,
			RecordCount: len(p.Records),
		}}
	}
	for i, in := range inputs {
		if r, ok := byID[in.ID]; ok {
			results[i] = r
		} else if results[i].ID == "" {
			existing, err := getTxn(ctx, e.db, in.ID)
			if err != nil {
				return GroupResult{}, err
			}
			results[i] = CommitResult{Transaction: existing, Duplicate: true}
		}
	}
	return GroupResult{
		Transactions: results,
		StartLSN:     nextLSN,
		EndLSN:       endLSN,
		FsyncCount:   1,
	}, nil
}

func pendingTxnIDs(prepared []preparedTxn) []string {
	out := make([]string, len(prepared))
	for i := range prepared {
		out[i] = prepared[i].Input.ID
	}
	return out
}

func reasonOf(err error) string {
	if err == nil {
		return "unknown failure"
	}
	return err.Error()
}

func (e *Engine) prepareGroupLocked(ctx context.Context, pending []int, inputs []TransactionInput) ([]preparedTxn, error) {
	maxVersions := make(map[int64]uint64)
	out := make([]preparedTxn, 0, len(pending))
	for _, idx := range pending {
		in := inputs[idx]
		fp, err := in.Fingerprint()
		if err != nil {
			return nil, err
		}
		p := preparedTxn{Input: in, Fingerprint: fp}
		for _, upd := range in.Records {
			if _, ok := maxVersions[upd.PageNo]; !ok {
				var max sql.NullInt64
				err := e.db.QueryRowContext(ctx, `SELECT MAX(version) FROM page_versions WHERE page_no=?`, upd.PageNo).Scan(&max)
				if err != nil {
					return nil, wrapError(CodeIO, err, "read page version for page %d", upd.PageNo)
				}
				if max.Valid {
					maxVersions[upd.PageNo] = uint64(max.Int64)
				}
			}
			data, err := upd.bytes()
			if err != nil {
				return nil, err
			}
			version := upd.Version
			if version == 0 {
				version = maxVersions[upd.PageNo] + 1
			}
			if version <= maxVersions[upd.PageNo] {
				return nil, newError(CodeInvalidArgument, "transaction %q page %d version %d is not newer than %d", in.ID, upd.PageNo, version, maxVersions[upd.PageNo])
			}
			maxVersions[upd.PageNo] = version
			p.Records = append(p.Records, preparedRecord{
				PageNo:  upd.PageNo,
				Version: version,
				Digest:  digestBytes(data),
				Data:    data,
				Length:  int(recordSize(in.ID, data)),
			})
		}
		out = append(out, p)
	}
	return out, nil
}

func (e *Engine) persistGroupIntentLocked(ctx context.Context, payload groupIntentPayload, prepared []preparedTxn) (int64, error) {
	tx, err := beginWrite(ctx, e.db)
	if err != nil {
		return 0, wrapError(CodeIO, err, "begin group intent transaction")
	}
	defer tx.Rollback()
	now := time.Now().UnixMilli()
	for _, p := range prepared {
		var state string
		err := tx.QueryRowContext(ctx, `SELECT state FROM transactions WHERE id=?`, p.Input.ID).Scan(&state)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			if _, err := tx.ExecContext(ctx, `INSERT INTO transactions(id,fingerprint,state,created_at,updated_at)
				VALUES(?,?,?,?,?)`, p.Input.ID, p.Fingerprint, string(TxnGrouped), now, now); err != nil {
				return 0, wrapError(CodeIO, err, "insert GROUPED transaction %q", p.Input.ID)
			}
		case err != nil:
			return 0, wrapError(CodeIO, err, "read transaction %q", p.Input.ID)
		default:
			from := TransactionState(state)
			if from != TxnAppending && from != TxnGrouped {
				return 0, invalidTransitionTxn(p.Input.ID, from, TxnGrouped)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE transactions SET state=?,updated_at=? WHERE id=?`,
				string(TxnGrouped), now, p.Input.ID); err != nil {
				return 0, wrapError(CodeIO, err, "mark transaction %q GROUPED", p.Input.ID)
			}
		}
	}
	intentID, err := insertIntent(ctx, tx, "GROUP_COMMIT", "PENDING", payload)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, wrapError(CodeIO, err, "commit group intent")
	}
	return intentID, nil
}

func (e *Engine) writeGroupLocked(ctx context.Context, cur SegmentInfo, prepared []preparedTxn,
	newRecords, newTxns, endLSN uint64, newSize int64, txnIDs []string) error {
	path := segmentPath(e.dir, cur.SegmentNo)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return wrapError(CodeIO, err, "open segment %d for append", cur.SegmentNo)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return wrapError(CodeIO, err, "stat segment %d", cur.SegmentNo)
	}
	if st.Size() != cur.ByteSize {
		return wrapError(CodeIO, nil, "segment %d size %d does not match catalog %d", cur.SegmentNo, st.Size(), cur.ByteSize)
	}
	header, err := readHeaderFromFile(path, cur.SegmentNo, cur.StartLSN)
	if err != nil {
		return err
	}
	if header.RecordCount != cur.RecordCount || header.TxnCount != cur.TxnCount {
		return segmentCorrupt(cur.SegmentNo, cur.StartLSN, "catalog/header counts disagree before append")
	}
	for _, p := range prepared {
		for i, r := range p.Records {
			rec := Record{
				LSN:     r.LSN,
				TxnID:   p.Input.ID,
				PageNo:  r.PageNo,
				Version: r.Version,
				Digest:  r.Digest,
				Data:    r.Data,
				Offset:  r.Offset,
				Length:  r.Length,
			}
			b := encodeRecord(rec)
			if err := e.fault(FaultBeforeGroupWrite, cur.SegmentNo, txnIDs); err != nil {
				return err
			}
			n, err := f.WriteAt(b, r.Offset)
			if err != nil {
				return wrapError(CodeIO, err, "write segment %d txn %q record %d", cur.SegmentNo, p.Input.ID, i)
			}
			if n != len(b) {
				return wrapError(CodeIO, nil, "short WAL write for segment %d txn %q record %d", cur.SegmentNo, p.Input.ID, i)
			}
		}
	}
	header.RecordCount = newRecords
	header.TxnCount = newTxns
	header.LastLSN = endLSN
	if err := writeHeaderFile(f, header); err != nil {
		return wrapError(CodeIO, err, "update segment %d header", cur.SegmentNo)
	}
	if err := f.Truncate(newSize); err != nil {
		return wrapError(CodeIO, err, "size segment %d", cur.SegmentNo)
	}
	if err := e.fault(FaultBeforeGroupSync, cur.SegmentNo, txnIDs); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return wrapError(CodeIO, err, "fsync segment %d", cur.SegmentNo)
	}
	syncCounter(e, cur.SegmentNo)
	e.crash(CrashAfterGroupSync, map[string]any{
		"segment_no": cur.SegmentNo,
		"txn_ids":    append([]string(nil), txnIDs...),
		"start_lsn":  prepared[0].StartLSN,
		"end_lsn":    endLSN,
	})
	return nil
}

func (e *Engine) commitGroupMetadataLocked(ctx context.Context, intentID int64, segNo uint64,
	prepared []preparedTxn, newRecords, newTxns, endLSN uint64, newSize int64, digest string) error {
	tx, err := beginWrite(ctx, e.db)
	if err != nil {
		return wrapError(CodeIO, err, "begin group metadata transaction")
	}
	defer tx.Rollback()
	now := time.Now().UnixMilli()
	for _, p := range prepared {
		var state string
		if err := tx.QueryRowContext(ctx, `SELECT state FROM transactions WHERE id=?`, p.Input.ID).Scan(&state); err != nil {
			return wrapError(CodeIO, err, "read transaction %q before durable", p.Input.ID)
		}
		if TransactionState(state) != TxnGrouped {
			return invalidTransitionTxn(p.Input.ID, TransactionState(state), TxnDurable)
		}
		res, err := tx.ExecContext(ctx, `UPDATE transactions SET state=?,segment_no=?,start_lsn=?,end_lsn=?,
			start_offset=?,end_offset=?,record_count=?,updated_at=? WHERE id=? AND state=?`,
			string(TxnDurable), p.SegmentNo, p.StartLSN, p.EndLSN, p.StartOffset, p.EndOffset,
			len(p.Records), now, p.Input.ID, string(TxnGrouped))
		if err != nil {
			return wrapError(CodeIO, err, "make transaction %q DURABLE", p.Input.ID)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return invalidTransitionTxn(p.Input.ID, TxnGrouped, TxnDurable)
		}
		for i, r := range p.Records {
			if _, err := tx.ExecContext(ctx, `INSERT INTO txn_records
				(txn_id,ordinal,lsn,segment_no,offset,length,page_no,version,digest,data)
				VALUES(?,?,?,?,?,?,?,?,?,?)`,
				p.Input.ID, i, r.LSN, p.SegmentNo, r.Offset, r.Length, r.PageNo, r.Version, r.Digest, r.Data); err != nil {
				return wrapError(CodeIO, err, "persist WAL record metadata for txn %q", p.Input.ID)
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO page_versions
				(page_no,version,lsn,txn_id,digest,data) VALUES(?,?,?,?,?,?)`,
				r.PageNo, r.Version, r.LSN, p.Input.ID, r.Digest, r.Data); err != nil {
				return wrapError(CodeIO, err, "persist page version for page %d", r.PageNo)
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM txn_staging WHERE txn_id=?`, p.Input.ID); err != nil {
			return wrapError(CodeIO, err, "clear transaction staging")
		}
	}
	res, err := tx.ExecContext(ctx, `UPDATE segments SET record_count=?,txn_count=?,last_lsn=?,byte_size=?,digest=?
		WHERE segment_no=? AND state=?`, newRecords, newTxns, endLSN, newSize, digest, segNo, string(SegmentOpen))
	if err != nil {
		return wrapError(CodeIO, err, "update segment catalog after group commit")
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return wrapError(CodeIO, nil, "segment %d left OPEN state before group metadata commit", segNo)
	}
	if err := setMetaUint(ctx, tx, "next_lsn", endLSN+1); err != nil {
		return wrapError(CodeIO, err, "advance next LSN")
	}
	if err := updateIntentStatus(ctx, tx, intentID, "DONE"); err != nil {
		return wrapError(CodeIO, err, "complete group intent")
	}
	if err := tx.Commit(); err != nil {
		return wrapError(CodeIO, err, "commit group metadata")
	}
	return nil
}

func (e *Engine) markGroupRolledBack(ctx context.Context, intentID int64, txnIDs []string, reason string) error {
	tx, err := beginWrite(ctx, e.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UnixMilli()
	for _, id := range txnIDs {
		if _, err := tx.ExecContext(ctx, `UPDATE transactions SET state=?,updated_at=?
			WHERE id=? AND state IN (?,?)`, string(TxnAborted), now, id, string(TxnAppending), string(TxnGrouped)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM txn_staging WHERE txn_id=?`, id); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status=?,updated_at=? WHERE id=?`,
		"ROLLED_BACK", now, intentID); err != nil {
		return err
	}
	_ = reason
	return tx.Commit()
}

// TransitionTransaction applies a declared transaction state transition. If
// the transition is illegal, no row is changed.
func (e *Engine) TransitionTransaction(ctx context.Context, id string, to TransactionState) (Transaction, error) {
	if err := checkClosed(e); err != nil {
		return Transaction{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := checkClosed(e); err != nil {
		return Transaction{}, err
	}
	existing, err := getTxn(ctx, e.db, id)
	if err != nil {
		return Transaction{}, err
	}
	if !CanTransitionTxn(existing.State, to) {
		return Transaction{}, invalidTransitionTxn(id, existing.State, to)
	}
	// DURABLE is a durability claim, not an administrative flag. Only the
	// fsync-barrier group-commit path may perform GROUPED -> DURABLE. This
	// public helper is intentionally limited to terminal ABORTED transitions.
	if to != TxnAborted {
		er := invalidTransitionTxn(id, existing.State, to)
		er.Message = "transition is reserved for the group-commit durability path"
		return Transaction{}, er
	}
	tx, err := beginWrite(ctx, e.db)
	if err != nil {
		return Transaction{}, wrapError(CodeIO, err, "begin transaction state change")
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE transactions SET state=?,updated_at=? WHERE id=? AND state=?`,
		string(to), time.Now().UnixMilli(), id, string(existing.State))
	if err != nil {
		return Transaction{}, wrapError(CodeIO, err, "change transaction state")
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return Transaction{}, invalidTransitionTxn(id, existing.State, to)
	}
	if err := tx.Commit(); err != nil {
		return Transaction{}, wrapError(CodeIO, err, "commit transaction state change")
	}
	return getTxn(ctx, e.db, id)
}
