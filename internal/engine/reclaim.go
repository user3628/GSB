package engine

import (
	"sort"

	"mvccengine/pkg/mvcc"
)

type ReclaimRequest struct {
	RequestID  string `json:"request_id,omitempty"`
	RowKey     string `json:"row_key,omitempty"`
	VersionSeq int64  `json:"version_seq,omitempty"`
	All        bool   `json:"all,omitempty"`
	Strict     bool   `json:"strict,omitempty"`
}

type BlockedVersion struct {
	RowKey                         string  `json:"row_key"`
	VersionSeq                     int64   `json:"version_seq"`
	SupersedingCommitSeq           int64   `json:"superseding_commit_seq"`
	EarliestBlockingSnapshotTxnSeq int64   `json:"earliest_blocking_snapshot_txn_seq"`
	BlockingSnapshotTxnSeqs        []int64 `json:"blocking_snapshot_txn_seqs"`
	RemainingReferences            int     `json:"remaining_references"`
}

type ReclaimResult struct {
	IntentID           string           `json:"intent_id,omitempty"`
	RemovedCount       int              `json:"removed_count"`
	RemovedVersionSeqs []int64          `json:"removed_version_seqs"`
	BlockedCount       int              `json:"blocked_count"`
	Blocked            []BlockedVersion `json:"blocked"`
	RequestID          string           `json:"request_id,omitempty"`
}

func (e *Engine) Reclaim(req ReclaimRequest) (*ReclaimResult, error) {
	return executeAs(e, req.RequestID, "reclaim", req, func(run *RequestRun) (*ReclaimResult, error) {
		e.mu.Lock()
		defer e.mu.Unlock()

		candidates := e.reclaimCandidatesLocked(req)
		blocked := []BlockedVersion{}
		safe := []mvcc.RowVersion{}
		for _, candidate := range candidates {
			blockers := e.blockingSnapshotsLocked(candidate.SupersedingCommitSeq)
			if len(blockers) > 0 {
				sortInt64(blockers)
				blocked = append(blocked, BlockedVersion{
					RowKey:                         candidate.RowKey,
					VersionSeq:                     candidate.VersionSeq,
					SupersedingCommitSeq:           candidate.SupersedingCommitSeq,
					EarliestBlockingSnapshotTxnSeq: blockers[0],
					BlockingSnapshotTxnSeqs:        blockers,
					RemainingReferences:            len(blockers),
				})
			} else {
				safe = append(safe, candidate)
			}
		}

		if req.VersionSeq > 0 && len(blocked) > 0 {
			b := blocked[0]
			return nil, mvcc.NewError(mvcc.CodeReclaimUnsafe, "version is still referenced by an active snapshot", map[string]any{
				"row_key":                            b.RowKey,
				"version_seq":                        b.VersionSeq,
				"superseding_commit_seq":             b.SupersedingCommitSeq,
				"earliest_blocking_snapshot_txn_seq": b.EarliestBlockingSnapshotTxnSeq,
				"blocking_snapshot_txn_seqs":         b.BlockingSnapshotTxnSeqs,
				"remaining_references":               b.RemainingReferences,
			})
		}

		result := &ReclaimResult{
			RemovedCount:       len(safe),
			RemovedVersionSeqs: []int64{},
			BlockedCount:       len(blocked),
			Blocked:            blocked,
			RequestID:          run.ID,
		}
		if result.Blocked == nil {
			result.Blocked = []BlockedVersion{}
		}

		intentID, err := e.createIntent(run, "reclaim", map[string]any{"version_seqs": safe, "request_id": run.ID})
		if err != nil {
			return nil, err
		}
		if e.opts.ReclaimIntentHook != nil {
			e.opts.ReclaimIntentHook(intentID)
		}
		result.IntentID = intentID

		dbTx, err := e.db.Begin()
		if err != nil {
			return nil, err
		}
		defer func() { _ = dbTx.Rollback() }()

		now := e.now()
		for _, version := range safe {
			if _, err := dbTx.Exec(`INSERT OR REPLACE INTO reclaimed_versions(version_seq,row_key,superseding_commit_seq,intent_id,reclaimed_at_ms)
				VALUES(?,?,?,?,?)`, version.VersionSeq, version.RowKey, version.SupersedingCommitSeq, intentID, now); err != nil {
				return nil, err
			}
			if _, err := dbTx.Exec(`DELETE FROM versions WHERE version_seq=?`, version.VersionSeq); err != nil {
				return nil, err
			}
			result.RemovedVersionSeqs = append(result.RemovedVersionSeqs, version.VersionSeq)
		}
		if err := completeIntentTx(dbTx, intentID, result); err != nil {
			return nil, err
		}
		if err := run.CompleteTx(dbTx, result, intentID); err != nil {
			return nil, err
		}
		if err := dbTx.Commit(); err != nil {
			return nil, err
		}
		for _, version := range safe {
			delete(e.versions, version.VersionSeq)
		}

		sort.Slice(result.RemovedVersionSeqs, func(i, j int) bool { return result.RemovedVersionSeqs[i] < result.RemovedVersionSeqs[j] })
		if req.Strict && len(blocked) > 0 {
			return result, mvcc.NewError(mvcc.CodeReclaimUnsafe, "one or more versions are still referenced by active snapshots", map[string]any{
				"blocked":       blocked,
				"removed_count": result.RemovedCount,
			})
		}
		return result, nil
	})
}

func (e *Engine) reclaimCandidatesLocked(req ReclaimRequest) []mvcc.RowVersion {
	candidates := []mvcc.RowVersion{}
	for _, version := range e.versions {
		if version.Reclaimed || version.Detached || version.SupersedingCommitSeq <= 0 {
			continue
		}
		if version.CommitState != mvcc.VersionCommitted {
			continue
		}
		if req.VersionSeq > 0 && version.VersionSeq != req.VersionSeq {
			continue
		}
		if req.RowKey != "" && version.RowKey != req.RowKey {
			continue
		}
		if req.VersionSeq == 0 && req.RowKey == "" && !req.All {
			continue
		}
		candidates = append(candidates, version)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].VersionSeq < candidates[j].VersionSeq })
	return candidates
}

func (e *Engine) blockingSnapshotsLocked(supersedingCommitSeq int64) []int64 {
	blockers := []int64{}
	for seq, txn := range e.txns {
		if txn.Status != mvcc.TxnActive {
			continue
		}
		if txn.UpperBound < supersedingCommitSeq {
			blockers = append(blockers, seq)
		}
	}
	return blockers
}
