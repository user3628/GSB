package store

import (
	"context"
	"database/sql"
	"encoding/json"

	"flashattention/internal/attn"
	"flashattention/internal/tensor"
	"flashattention/internal/versioning"
)

func (s *Store) LoadRecovery(ctx context.Context, shardID string) (RecoveryResult, error) {
	heads, err := s.HeadSequences(ctx, shardID)
	if err != nil {
		return RecoveryResult{}, err
	}
	out := RecoveryResult{States: map[string]attn.ResumeState{}, HeadSeq: heads}

	rows, err := s.db.QueryContext(ctx, `
SELECT id,bh,row_block,col_start,col_end,rows_count,head_dim,seq,strategy,
       row_max_blob,exp_sum_blob,lse_blob,acc_blob,acc_hash,input_fp,version_fp,prev_hash,state_hash,active
FROM checkpoints WHERE shard_id=? ORDER BY id`, shardID)
	if err != nil {
		return out, err
	}
	defer rows.Close()

	type candidate struct {
		rec       Record
		snapshots []attn.Snapshot
	}
	latest := map[string]candidate{}
	seenSeq := map[string]bool{}

	for rows.Next() {
		var (
			id                                                         int64
			bh, rb, cs, ce, rowsN, dim                                 int
			seq                                                        int64
			strategy, accHash, inputFP, versionFP, prevHash, stateHash string
			mBlob, sumBlob, lseBlob, accBlob                           []byte
			activeInt                                                  int
		)
		if err := rows.Scan(&id, &bh, &rb, &cs, &ce, &rowsN, &dim, &seq, &strategy,
			&mBlob, &sumBlob, &lseBlob, &accBlob, &accHash, &inputFP, &versionFP, &prevHash, &stateHash, &activeInt); err != nil {
			out.Ignored = append(out.Ignored, IgnoredRecord{Reason: "scan_failed", Detail: err.Error()})
			continue
		}
		key := attn.ResumeKey(bh, rb)
		seqKey := key + "/" + itoa(seq)
		if seq <= 0 || rowsN <= 0 || dim <= 0 || cs < 0 || ce <= cs || len(accBlob) == 0 {
			out.Ignored = append(out.Ignored, IgnoredRecord{ID: id, Reason: "incomplete"})
			continue
		}
		if len(mBlob) != rowsN*8 || len(sumBlob) != rowsN*8 || len(lseBlob) != rowsN*8 || len(accBlob) != rowsN*dim*8 {
			out.Ignored = append(out.Ignored, IgnoredRecord{ID: id, Reason: "truncated_blob"})
			continue
		}
		if seenSeq[seqKey] {
			out.Ignored = append(out.Ignored, IgnoredRecord{ID: id, Reason: "duplicate_sequence", Detail: seqKey})
			continue
		}
		seenSeq[seqKey] = true
		if AccHash(accBlob) != accHash {
			out.Ignored = append(out.Ignored, IgnoredRecord{ID: id, Reason: "acc_digest_mismatch"})
			continue
		}
		c := Checkpoint{
			ShardID: shardID, BH: bh, RowBlock: rb, ColStart: cs, ColEnd: ce,
			RowsCount: rowsN, HeadDim: dim, Seq: seq, Strategy: strategy,
			InputFP: inputFP, VersionFP: versionFP, PrevHash: prevHash,
		}
		if HashCheckpoint(&c, mBlob, sumBlob, lseBlob, accBlob) != stateHash {
			out.Ignored = append(out.Ignored, IgnoredRecord{ID: id, Reason: "state_digest_mismatch"})
			continue
		}
		if activeInt != 1 {
			continue
		}
		ms := tensor.DecodeFloat64s(mBlob)
		ss := tensor.DecodeFloat64s(sumBlob)
		ls := tensor.DecodeFloat64s(lseBlob)
		acc := tensor.DecodeFloat64s(accBlob)
		if ms == nil || ss == nil || ls == nil || acc == nil {
			out.Ignored = append(out.Ignored, IgnoredRecord{ID: id, Reason: "decode_failed"})
			continue
		}
		snaps := make([]attn.Snapshot, rowsN)
		ok := true
		for i := 0; i < rowsN; i++ {
			start := i * dim
			end := start + dim
			if end > len(acc) {
				ok = false
				break
			}
			rowAcc := make([]float64, dim)
			copy(rowAcc, acc[start:end])
			snaps[i] = attn.Snapshot{LocalRow: i, Valid: true, M: ms[i], Sum: ss[i], LSE: ls[i], Acc: rowAcc}
		}
		if !ok {
			out.Ignored = append(out.Ignored, IgnoredRecord{ID: id, Reason: "acc_shape_mismatch"})
			continue
		}
		rec := Record{ID: id, ShardID: shardID, BH: bh, RowBlock: rb, ColStart: cs, ColEnd: ce, Seq: seq, Active: true, StateHash: stateHash}
		if cur, exists := latest[key]; !exists || rec.Seq >= cur.rec.Seq {
			latest[key] = candidate{rec: rec, snapshots: snaps}
		}
	}
	if err := rows.Err(); err != nil {
		return out, err
	}

	for key, cand := range latest {
		bh, rb := parseKey(key)
		seq := cand.rec.Seq
		if headSeq, ok := heads[key]; ok && headSeq > seq {
			seq = headSeq
		}
		out.States[key] = attn.ResumeState{
			BH: bh, RowBlock: rb, ResumeCol: cand.rec.ColEnd, Seq: seq, StateHash: cand.rec.StateHash, Snapshots: cand.snapshots,
		}
	}
	for key, headSeq := range heads {
		if _, ok := out.States[key]; ok {
			continue
		}
		bh, rb := parseKey(key)
		out.States[key] = attn.ResumeState{
			BH: bh, RowBlock: rb, ResumeCol: 0, Seq: headSeq,
		}
	}
	return out, nil
}

func (s *Store) GetFingerprint(ctx context.Context, shardID string) (versioning.Fingerprint, error) {
	var fp versioning.Fingerprint
	var stagesJSON string
	err := s.db.QueryRowContext(ctx, `SELECT fingerprint,stages_json FROM versions WHERE shard_id=?`, shardID).
		Scan(&fp.Hash, &stagesJSON)
	if err != nil {
		return fp, err
	}
	if err := json.Unmarshal([]byte(stagesJSON), &fp.Stages); err != nil {
		return fp, err
	}
	return fp, nil
}

func (s *Store) HeadSequences(ctx context.Context, shardID string) (map[string]int64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT bh,row_block,current_seq FROM row_heads WHERE shard_id=?`, shardID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var bh, rb int
		var seq int64
		if err := rows.Scan(&bh, &rb, &seq); err != nil {
			return nil, err
		}
		out[attn.ResumeKey(bh, rb)] = seq
	}
	return out, rows.Err()
}

func (s *Store) ListCheckpoints(ctx context.Context, shardID string) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id,shard_id,bh,row_block,col_start,col_end,rows_count,head_dim,seq,strategy,
       input_fp,version_fp,state_hash,acc_hash,active,invalid_reason
FROM checkpoints WHERE shard_id=? ORDER BY bh,row_block,seq,id`, shardID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		var r Record
		var active int
		if err := rows.Scan(&r.ID, &r.ShardID, &r.BH, &r.RowBlock, &r.ColStart, &r.ColEnd,
			&r.RowsCount, &r.HeadDim, &r.Seq, &r.Strategy, &r.InputFP, &r.VersionFP,
			&r.StateHash, &r.AccHash, &active, &r.InvalidReason); err != nil {
			return nil, err
		}
		r.Active = active == 1
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) ListAllCheckpoints(ctx context.Context) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id,shard_id,bh,row_block,col_start,col_end,rows_count,head_dim,seq,strategy,
       input_fp,version_fp,state_hash,acc_hash,active,invalid_reason
FROM checkpoints ORDER BY shard_id,bh,row_block,seq,id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		var r Record
		var active int
		if err := rows.Scan(&r.ID, &r.ShardID, &r.BH, &r.RowBlock, &r.ColStart, &r.ColEnd,
			&r.RowsCount, &r.HeadDim, &r.Seq, &r.Strategy, &r.InputFP, &r.VersionFP,
			&r.StateHash, &r.AccHash, &active, &r.InvalidReason); err != nil {
			return nil, err
		}
		r.Active = active == 1
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) LoadContributions(ctx context.Context, shardID string) (map[string][]attn.BlockContribution, []IgnoredRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT bh,row_block,col_block,col_start,col_end,head_dim,row_start,row_end,row_max_blob,exp_sum_blob,pv_blob,hash
FROM block_contributions WHERE shard_id=? ORDER BY bh,row_block,col_block`, shardID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	out := map[string][]attn.BlockContribution{}
	ignored := []IgnoredRecord{}
	for rows.Next() {
		var c attn.BlockContribution
		var mBlob, sumBlob, pvBlob []byte
		if err := rows.Scan(&c.BH, &c.RowBlock, &c.ColBlock, &c.ColStart, &c.ColEnd, &c.HeadDim, &c.RowStart, &c.RowEnd,
			&mBlob, &sumBlob, &pvBlob, &c.Hash); err != nil {
			ignored = append(ignored, IgnoredRecord{Reason: "scan_failed", Detail: err.Error()})
			continue
		}
		c.M = tensor.DecodeFloat64s(mBlob)
		c.Sum = tensor.DecodeFloat64s(sumBlob)
		c.PV = tensor.DecodeFloat64s(pvBlob)
		if c.M == nil || c.Sum == nil || c.PV == nil || c.ComputeHash() != c.Hash {
			ignored = append(ignored, IgnoredRecord{Reason: "contribution_digest_mismatch"})
			continue
		}
		key := attn.ResumeKey(c.BH, c.RowBlock)
		out[key] = append(out[key], c)
	}
	return out, ignored, rows.Err()
}

func (s *Store) RawDB() *sql.DB { return s.db }

func (s *Store) LoadNumericEvents(ctx context.Context, shardID string) ([]attn.NumericEvent, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT events_json FROM checkpoints WHERE shard_id=? AND active=1 AND events_json <> '[]'`, shardID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []attn.NumericEvent
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var events []attn.NumericEvent
		if err := json.Unmarshal([]byte(raw), &events); err != nil {
			continue
		}
		out = append(out, events...)
	}
	return out, rows.Err()
}

func itoa(x int64) string {
	if x == 0 {
		return "0"
	}
	neg := x < 0
	if neg {
		x = -x
	}
	var b [20]byte
	i := len(b)
	for x > 0 {
		i--
		b[i] = byte('0' + x%10)
		x /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

func parseKey(key string) (bh, rb int) {
	var a, b int
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			a = atoi(key[:i])
			b = atoi(key[i+1:])
			break
		}
	}
	return a, b
}

func atoi(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	return n
}
