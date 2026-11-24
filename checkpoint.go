package walgroup

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"
)

type checkpointPageRow struct {
	PageVersionRef
	Data []byte
}

type checkpointIntentPayload struct {
	Sequence       uint64 `json:"sequence"`
	ReplayStartLSN uint64 `json:"replay_start_lsn"`
	Digest         string `json:"digest"`
}

func loadActiveCheckpoint(ctx context.Context, q dbTX, verify bool) (*Checkpoint, []checkpointPageRow, error) {
	var cp Checkpoint
	var active int
	err := q.QueryRowContext(ctx, `SELECT sequence,replay_start_lsn,pages_digest,digest,active
		FROM checkpoints WHERE active=1`).Scan(&cp.Sequence, &cp.ReplayStartLSN, &cp.PagesDigest, &cp.Digest, &active)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, wrapError(CodeIO, err, "read active checkpoint")
	}
	cp.Active = active == 1
	rows, err := q.QueryContext(ctx, `SELECT page_no,version,lsn,digest,data
		FROM checkpoint_pages WHERE checkpoint_seq=? ORDER BY page_no`, cp.Sequence)
	if err != nil {
		return nil, nil, wrapError(CodeIO, err, "read checkpoint pages")
	}
	defer rows.Close()
	var pages []checkpointPageRow
	var refs []PageVersionRef
	for rows.Next() {
		p := checkpointPageRow{}
		if err := rows.Scan(&p.PageNo, &p.Version, &p.LSN, &p.Digest, &p.Data); err != nil {
			return nil, nil, wrapError(CodeIO, err, "scan checkpoint page")
		}
		if verify && digestBytes(p.Data) != p.Digest {
			return nil, nil, checkpointInvalid(cp.Sequence, "page %d digest does not match persisted data", p.PageNo)
		}
		pages = append(pages, p)
		refs = append(refs, p.PageVersionRef)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, wrapError(CodeIO, err, "iterate checkpoint pages")
	}
	if verify {
		pd, od := CheckpointDigest(cp.Sequence, cp.ReplayStartLSN, refs)
		if pd != cp.PagesDigest {
			return nil, nil, checkpointInvalid(cp.Sequence, "checkpoint page-set digest mismatch: got %s want %s", pd, cp.PagesDigest)
		}
		if od != cp.Digest {
			return nil, nil, checkpointInvalid(cp.Sequence, "checkpoint overall digest mismatch: got %s want %s", od, cp.Digest)
		}
	}
	cp.Pages = refs
	return &cp, pages, nil
}

func checkpointInvalid(seq uint64, format string, args ...any) *Error {
	e := newError(CodeCheckpointInvalid, format, args...)
	_ = seq
	return e
}

// AdvanceCheckpoint atomically derives and replaces the active checkpoint.
// Sequence numbers must increase and the replay start must not move backward.
func (e *Engine) AdvanceCheckpoint(ctx context.Context, req CheckpointRequest) (Checkpoint, error) {
	if err := checkClosed(e); err != nil {
		return Checkpoint{}, err
	}
	if req.Sequence == 0 {
		return Checkpoint{}, checkpointInvalid(req.Sequence, "checkpoint sequence must be positive")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := checkClosed(e); err != nil {
		return Checkpoint{}, err
	}

	old, _, err := loadActiveCheckpoint(ctx, e.db, true)
	if err != nil {
		return Checkpoint{}, err
	}
	var maxSeq uint64
	if err := e.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0) FROM checkpoints`).Scan(&maxSeq); err != nil {
		return Checkpoint{}, wrapError(CodeIO, err, "read checkpoint sequence high-water mark")
	}
	if req.Sequence <= maxSeq {
		return Checkpoint{}, checkpointInvalid(req.Sequence, "sequence %d is not greater than persisted maximum %d", req.Sequence, maxSeq)
	}
	if old != nil && req.ReplayStartLSN < old.ReplayStartLSN {
		return Checkpoint{}, checkpointInvalid(req.Sequence, "replay start %d moved backward from %d", req.ReplayStartLSN, old.ReplayStartLSN)
	}
	nextLSN, err := metaUint(ctx, e.db, "next_lsn")
	if err != nil {
		return Checkpoint{}, err
	}
	lastDurable := uint64(0)
	if nextLSN > 1 {
		lastDurable = nextLSN - 1
	}
	if req.ReplayStartLSN > lastDurable {
		return Checkpoint{}, checkpointInvalid(req.Sequence, "replay start %d exceeds last durable LSN %d", req.ReplayStartLSN, lastDurable)
	}
	pages, err := e.deriveCheckpointPages(ctx, req.ReplayStartLSN)
	if err != nil {
		return Checkpoint{}, err
	}
	refs := make([]PageVersionRef, len(pages))
	for i := range pages {
		refs[i] = pages[i].PageVersionRef
	}
	pd, od := CheckpointDigest(req.Sequence, req.ReplayStartLSN, refs)
	if len(req.ExpectedPages) != 0 && !equalPageRefs(req.ExpectedPages, refs) {
		return Checkpoint{}, checkpointInvalid(req.Sequence, "expected page-version set does not match durable page versions at LSN %d", req.ReplayStartLSN)
	}
	if req.ExpectedDigest != "" && NormalizeExpectedDigest(req.ExpectedDigest) != od {
		return Checkpoint{}, checkpointInvalid(req.Sequence, "expected checkpoint digest %s does not match computed digest %s", req.ExpectedDigest, od)
	}
	payload := checkpointIntentPayload{Sequence: req.Sequence, ReplayStartLSN: req.ReplayStartLSN, Digest: od}
	intentID, err := insertIntent(ctx, e.db, "CHECKPOINT", "PENDING", payload)
	if err != nil {
		return Checkpoint{}, err
	}
	if err := e.fault(FaultBeforeCheckpoint, 0, nil); err != nil {
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return Checkpoint{}, wrapError(CodeCheckpointInvalid, err, "checkpoint rejected before metadata replacement")
	}
	e.crash(CrashBeforeCheckpointMeta, map[string]any{"sequence": req.Sequence, "replay_start_lsn": req.ReplayStartLSN})
	tx, err := beginWrite(ctx, e.db)
	if err != nil {
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return Checkpoint{}, wrapError(CodeIO, err, "begin checkpoint transaction")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE checkpoints SET active=0 WHERE active=1`); err != nil {
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return Checkpoint{}, wrapError(CodeCheckpointInvalid, err, "deactivate old checkpoint")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO checkpoints(sequence,replay_start_lsn,pages_digest,digest,active,created_at)
		VALUES(?,?,?,?,1,?)`, req.Sequence, req.ReplayStartLSN, pd, od, time.Now().UnixMilli()); err != nil {
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return Checkpoint{}, wrapError(CodeCheckpointInvalid, err, "insert checkpoint metadata")
	}
	for _, p := range pages {
		if _, err := tx.ExecContext(ctx, `INSERT INTO checkpoint_pages
			(checkpoint_seq,page_no,version,lsn,digest,data) VALUES(?,?,?,?,?,?)`,
			req.Sequence, p.PageNo, p.Version, p.LSN, p.Digest, p.Data); err != nil {
			_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
			return Checkpoint{}, wrapError(CodeCheckpointInvalid, err, "insert checkpoint page %d", p.PageNo)
		}
	}
	if err := updateIntentStatus(ctx, tx, intentID, "DONE"); err != nil {
		return Checkpoint{}, wrapError(CodeCheckpointInvalid, err, "complete checkpoint intent")
	}
	if err := tx.Commit(); err != nil {
		_ = updateIntentStatus(ctx, e.db, intentID, "ROLLED_BACK")
		return Checkpoint{}, wrapError(CodeCheckpointInvalid, err, "commit checkpoint replacement")
	}
	return Checkpoint{
		Sequence:       req.Sequence,
		ReplayStartLSN: req.ReplayStartLSN,
		Pages:          refs,
		PagesDigest:    pd,
		Digest:         od,
		Active:         true,
	}, nil
}

func (e *Engine) deriveCheckpointPages(ctx context.Context, replayStart uint64) ([]checkpointPageRow, error) {
	rows, err := e.db.QueryContext(ctx, `SELECT pv.page_no,pv.version,pv.lsn,pv.digest,pv.data
		FROM page_versions pv
		WHERE pv.lsn <= ?
		  AND pv.version = (SELECT MAX(p2.version) FROM page_versions p2
		                    WHERE p2.page_no=pv.page_no AND p2.lsn <= ?)
		ORDER BY pv.page_no`, replayStart, replayStart)
	if err != nil {
		return nil, wrapError(CodeIO, err, "derive checkpoint pages")
	}
	defer rows.Close()
	var out []checkpointPageRow
	for rows.Next() {
		var p checkpointPageRow
		if err := rows.Scan(&p.PageNo, &p.Version, &p.LSN, &p.Digest, &p.Data); err != nil {
			return nil, wrapError(CodeIO, err, "scan derived checkpoint page")
		}
		if digestBytes(p.Data) != p.Digest {
			return nil, checkpointInvalid(0, "durable page %d version %d has an invalid digest", p.PageNo, p.Version)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// CurrentCheckpoint returns the verified active checkpoint, or nil when none
// has been established yet.
func (e *Engine) CurrentCheckpoint(ctx context.Context) (*Checkpoint, error) {
	if err := checkClosed(e); err != nil {
		return nil, err
	}
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, wrapError(CodeIO, err, "begin checkpoint snapshot")
	}
	defer tx.Rollback()
	cp, _, err := loadActiveCheckpoint(ctx, tx, true)
	return cp, err
}

// MinServiceableLSN returns the oldest snapshot LSN this engine can serve.
func (e *Engine) MinServiceableLSN(ctx context.Context) (uint64, error) {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return 0, wrapError(CodeIO, err, "begin min-LSN snapshot")
	}
	defer tx.Rollback()
	cp, _, err := loadActiveCheckpoint(ctx, tx, true)
	if err != nil {
		return 0, err
	}
	if cp != nil {
		return cp.ReplayStartLSN, nil
	}
	var min sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MIN(start_lsn) FROM segments WHERE state<>?`, string(SegmentReclaimed)).Scan(&min); err != nil {
		return 0, wrapError(CodeIO, err, "read minimum segment LSN")
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

// NormalizeExpectedDigest accepts a lowercase or uppercase hex digest. It is a
// convenience for HTTP clients that compare precomputed checkpoint digests.
func NormalizeExpectedDigest(s string) string {
	b, err := hex.DecodeString(s)
	if err != nil {
		return s
	}
	return hex.EncodeToString(b)
}
