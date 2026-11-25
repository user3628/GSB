package engine

import (
	"database/sql"

	"mvccengine/pkg/mvcc"
)

type WriteVersionRequest struct {
	RequestID     string `json:"request_id,omitempty"`
	TxnSeq        int64  `json:"txn_seq"`
	RowKey        string `json:"row_key"`
	Content       string `json:"content,omitempty"`
	Digest        string `json:"digest,omitempty"`
	ContentDigest string `json:"content_digest,omitempty"`
	IsDelete      bool   `json:"is_delete,omitempty"`
}

type CommitTxnRequest struct {
	RequestID string `json:"request_id,omitempty"`
	TxnSeq    int64  `json:"txn_seq"`
}

type CommitResult struct {
	TxnSeq                int64   `json:"txn_seq"`
	Status                string  `json:"status"`
	IntentID              string  `json:"intent_id"`
	CommittedVersionSeqs  []int64 `json:"committed_version_seqs"`
	SupersededVersionSeqs []int64 `json:"superseded_version_seqs,omitempty"`
	RequestID             string  `json:"request_id"`
}

type RollbackTxnRequest struct {
	RequestID string `json:"request_id,omitempty"`
	TxnSeq    int64  `json:"txn_seq"`
}

type RollbackResult struct {
	TxnSeq             int64    `json:"txn_seq"`
	Status             string   `json:"status"`
	IntentID           string   `json:"intent_id"`
	AbortedVersionSeqs []int64  `json:"aborted_version_seqs"`
	ResetHeadRowKeys   []string `json:"reset_head_row_keys"`
	RequestID          string   `json:"request_id"`
}

func (e *Engine) WriteVersion(req WriteVersionRequest) (*mvcc.RowVersion, error) {
	return executeAs(e, req.RequestID, "write_version", req, func(run *RequestRun) (*mvcc.RowVersion, error) {
		if req.TxnSeq <= 0 {
			return nil, mvcc.NewError(mvcc.CodeTxnInvalid, "txn_seq must be positive", map[string]any{"txn_seq": req.TxnSeq})
		}
		if req.RowKey == "" {
			return nil, mvcc.NewError(mvcc.CodeVersionInvalid, "row_key must not be empty", map[string]any{"txn_seq": req.TxnSeq})
		}

		declaredDigest := req.Digest
		if declaredDigest == "" {
			declaredDigest = req.ContentDigest
		}
		digest := declaredDigest
		if digest == "" {
			digest = mvcc.DigestContent(req.Content)
		}
		normalized, ok := mvcc.NormalizeDigest(digest)
		if !ok {
			return nil, mvcc.NewError(mvcc.CodeVersionInvalid, "digest must be 64-character fixed-length hexadecimal", map[string]any{
				"row_key": req.RowKey,
				"txn_seq": req.TxnSeq,
				"digest":  digest,
			})
		}
		if req.Content != "" && declaredDigest != "" && mvcc.DigestContent(req.Content) != normalized {
			return nil, mvcc.NewError(mvcc.CodeVersionInvalid, "digest does not match content", map[string]any{"row_key": req.RowKey})
		}

		e.mu.Lock()
		defer e.mu.Unlock()

		txn, ok := e.txns[req.TxnSeq]
		if !ok {
			return nil, mvcc.NewError(mvcc.CodeTxnInvalid, "transaction does not exist", map[string]any{"txn_seq": req.TxnSeq})
		}
		if txn.Status != mvcc.TxnActive {
			return nil, mvcc.NewError(mvcc.CodeTxnInvalid, "transaction is not ACTIVE", map[string]any{"txn_seq": req.TxnSeq, "status": string(txn.Status)})
		}

		var prevSeq *int64
		if headSeq, exists := e.heads[req.RowKey]; exists {
			headVersion := e.versions[headSeq]
			if headVersion.CommitState == mvcc.VersionUncommitted && headVersion.CreateTxnSeq != req.TxnSeq {
				return nil, mvcc.NewError(mvcc.CodeWriteConflict, "row has an uncommitted version from another transaction", map[string]any{
					"row_key":           req.RowKey,
					"head_version_seq":  headSeq,
					"blocking_txn_seq":  headVersion.CreateTxnSeq,
					"requested_txn_seq": req.TxnSeq,
				})
			}
			p := headSeq
			prevSeq = &p
		}

		versionSeq := e.nextVersionSeq + 1
		version := mvcc.RowVersion{
			RowKey:         req.RowKey,
			VersionSeq:     versionSeq,
			CreateTxnSeq:   req.TxnSeq,
			CommitState:    mvcc.VersionUncommitted,
			Digest:         normalized,
			Content:        req.Content,
			PrevVersionSeq: prevSeq,
			IsDelete:       req.IsDelete,
		}

		intentID, err := e.createIntent(run, "write_version", version)
		if err != nil {
			return nil, err
		}

		dbTx, err := e.db.Begin()
		if err != nil {
			return nil, err
		}
		defer func() { _ = dbTx.Rollback() }()

		now := e.now()
		var prev any
		if version.PrevVersionSeq != nil {
			prev = *version.PrevVersionSeq
		}
		if _, err := dbTx.Exec(`INSERT INTO versions(row_key,version_seq,create_txn_seq,commit_state,digest,content,prev_version_seq,is_delete,superseding_commit_seq,reclaimed,created_at_ms,updated_at_ms)
			VALUES(?,?,?,?,?,?,?,?,0,0,?,?)`,
			version.RowKey, version.VersionSeq, version.CreateTxnSeq, string(version.CommitState), version.Digest, version.Content, prev, boolToInt(version.IsDelete), now, now); err != nil {
			return nil, err
		}
		if _, err := dbTx.Exec(`INSERT INTO row_heads(row_key,version_seq) VALUES(?,?)
			ON CONFLICT(row_key) DO UPDATE SET version_seq=excluded.version_seq`, version.RowKey, version.VersionSeq); err != nil {
			return nil, err
		}
		if _, err := dbTx.Exec(`INSERT OR REPLACE INTO meta(key,value) VALUES('next_version_seq',?)`, versionSeq); err != nil {
			return nil, err
		}
		if err := completeIntentTx(dbTx, intentID, version); err != nil {
			return nil, err
		}
		if err := run.CompleteTx(dbTx, version, intentID); err != nil {
			return nil, err
		}
		if err := dbTx.Commit(); err != nil {
			return nil, err
		}

		e.nextVersionSeq = versionSeq
		e.versions[versionSeq] = version
		e.heads[version.RowKey] = versionSeq
		return &version, nil
	})
}

func (e *Engine) CommitTxn(req CommitTxnRequest) (*CommitResult, error) {
	return executeAs(e, req.RequestID, "commit_txn", req, func(run *RequestRun) (*CommitResult, error) {
		if req.TxnSeq <= 0 {
			return nil, mvcc.NewError(mvcc.CodeTxnInvalid, "txn_seq must be positive", map[string]any{"txn_seq": req.TxnSeq})
		}

		e.mu.Lock()
		defer e.mu.Unlock()

		txn, ok := e.txns[req.TxnSeq]
		if !ok {
			return nil, mvcc.NewError(mvcc.CodeTxnInvalid, "transaction does not exist", map[string]any{"txn_seq": req.TxnSeq})
		}
		if txn.Status != mvcc.TxnActive {
			return nil, mvcc.NewError(mvcc.CodeTxnInvalid, "transaction is not ACTIVE", map[string]any{"txn_seq": req.TxnSeq, "status": string(txn.Status)})
		}

		intentPayload := map[string]any{"txn_seq": req.TxnSeq, "request_id": run.ID}
		intentID, err := e.createIntent(run, "commit_txn", intentPayload)
		if err != nil {
			return nil, err
		}
		if e.opts.CommitIntentHook != nil {
			e.opts.CommitIntentHook(intentID, run.ID)
		}

		rows, err := e.db.Query(`SELECT version_seq, row_key FROM versions WHERE create_txn_seq=? ORDER BY version_seq`, req.TxnSeq)
		if err != nil {
			return nil, err
		}
		versionSeqs := []int64{}
		rowKeys := []string{}
		rowSeen := map[string]bool{}
		for rows.Next() {
			var seq int64
			var rowKey string
			if err := rows.Scan(&seq, &rowKey); err != nil {
				_ = rows.Close()
				return nil, err
			}
			versionSeqs = append(versionSeqs, seq)
			if !rowSeen[rowKey] {
				rowSeen[rowKey] = true
				rowKeys = append(rowKeys, rowKey)
			}
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}

		dbTx, err := e.db.Begin()
		if err != nil {
			return nil, err
		}
		defer func() { _ = dbTx.Rollback() }()

		if _, err := dbTx.Exec(`UPDATE txns SET status=? WHERE seq=?`, string(mvcc.TxnCommitted), req.TxnSeq); err != nil {
			return nil, err
		}
		if _, err := dbTx.Exec(`UPDATE versions SET commit_state=?, updated_at_ms=? WHERE create_txn_seq=? AND commit_state=?`,
			string(mvcc.VersionCommitted), e.now(), req.TxnSeq, string(mvcc.VersionUncommitted)); err != nil {
			return nil, err
		}

		superseded := []int64{}
		for _, rowKey := range rowKeys {
			var tipSeq int64
			if err := dbTx.QueryRow(`SELECT MAX(version_seq) FROM versions WHERE row_key=?`, rowKey).Scan(&tipSeq); err != nil {
				return nil, err
			}
			srows, err := dbTx.Query(`SELECT version_seq FROM versions
				WHERE row_key=? AND version_seq<? AND commit_state=? AND superseding_commit_seq=0
				ORDER BY version_seq`, rowKey, tipSeq, string(mvcc.VersionCommitted))
			if err != nil {
				return nil, err
			}
			for srows.Next() {
				var oldSeq int64
				if err := srows.Scan(&oldSeq); err != nil {
					_ = srows.Close()
					return nil, err
				}
				superseded = append(superseded, oldSeq)
			}
			if err := srows.Close(); err != nil {
				return nil, err
			}
			if _, err := dbTx.Exec(`UPDATE versions SET superseding_commit_seq=?
				WHERE row_key=? AND version_seq<? AND commit_state=? AND superseding_commit_seq=0`,
				req.TxnSeq, rowKey, tipSeq, string(mvcc.VersionCommitted)); err != nil {
				return nil, err
			}
		}

		result := &CommitResult{
			TxnSeq:                req.TxnSeq,
			Status:                string(mvcc.TxnCommitted),
			IntentID:              intentID,
			CommittedVersionSeqs:  versionSeqs,
			SupersededVersionSeqs: superseded,
			RequestID:             run.ID,
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

		txn.Status = mvcc.TxnCommitted
		e.txns[req.TxnSeq] = txn
		for _, seq := range versionSeqs {
			v := e.versions[seq]
			v.CommitState = mvcc.VersionCommitted
			e.versions[seq] = v
		}
		for _, seq := range superseded {
			v := e.versions[seq]
			v.SupersedingCommitSeq = req.TxnSeq
			e.versions[seq] = v
		}
		return result, nil
	})
}

func (e *Engine) RollbackTxn(req RollbackTxnRequest) (*RollbackResult, error) {
	return executeAs(e, req.RequestID, "rollback_txn", req, func(run *RequestRun) (*RollbackResult, error) {
		if req.TxnSeq <= 0 {
			return nil, mvcc.NewError(mvcc.CodeTxnInvalid, "txn_seq must be positive", map[string]any{"txn_seq": req.TxnSeq})
		}

		e.mu.Lock()
		defer e.mu.Unlock()

		txn, ok := e.txns[req.TxnSeq]
		if !ok {
			return nil, mvcc.NewError(mvcc.CodeTxnInvalid, "transaction does not exist", map[string]any{"txn_seq": req.TxnSeq})
		}
		if txn.Status != mvcc.TxnActive {
			return nil, mvcc.NewError(mvcc.CodeTxnInvalid, "transaction is not ACTIVE", map[string]any{"txn_seq": req.TxnSeq, "status": string(txn.Status)})
		}

		intentID, err := e.createIntent(run, "abort_txn", map[string]any{"txn_seq": req.TxnSeq, "request_id": run.ID})
		if err != nil {
			return nil, err
		}

		rows, err := e.db.Query(`SELECT version_seq,row_key FROM versions WHERE create_txn_seq=? ORDER BY version_seq`, req.TxnSeq)
		if err != nil {
			return nil, err
		}
		abortedSeqs := []int64{}
		rowKeys := []string{}
		seen := map[string]bool{}
		for rows.Next() {
			var seq int64
			var rowKey string
			if err := rows.Scan(&seq, &rowKey); err != nil {
				_ = rows.Close()
				return nil, err
			}
			abortedSeqs = append(abortedSeqs, seq)
			if !seen[rowKey] {
				seen[rowKey] = true
				rowKeys = append(rowKeys, rowKey)
			}
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}

		dbTx, err := e.db.Begin()
		if err != nil {
			return nil, err
		}
		defer func() { _ = dbTx.Rollback() }()

		if _, err := dbTx.Exec(`UPDATE txns SET status=? WHERE seq=?`, string(mvcc.TxnAborted), req.TxnSeq); err != nil {
			return nil, err
		}
		if _, err := dbTx.Exec(`UPDATE versions SET commit_state=?, updated_at_ms=? WHERE create_txn_seq=?`,
			string(mvcc.VersionAborted), e.now(), req.TxnSeq); err != nil {
			return nil, err
		}

		resetRows := []string{}
		newHeads := map[string]sql.NullInt64{}
		for _, rowKey := range rowKeys {
			var newHead sql.NullInt64
			if err := dbTx.QueryRow(`SELECT MAX(version_seq) FROM versions WHERE row_key=? AND commit_state=?`,
				rowKey, string(mvcc.VersionCommitted)).Scan(&newHead); err != nil {
				return nil, err
			}
			newHeads[rowKey] = newHead
			if newHead.Valid {
				if _, err := dbTx.Exec(`INSERT INTO row_heads(row_key,version_seq) VALUES(?,?)
					ON CONFLICT(row_key) DO UPDATE SET version_seq=excluded.version_seq`, rowKey, newHead.Int64); err != nil {
					return nil, err
				}
			} else {
				if _, err := dbTx.Exec(`DELETE FROM row_heads WHERE row_key=?`, rowKey); err != nil {
					return nil, err
				}
			}
			resetRows = append(resetRows, rowKey)
		}

		result := &RollbackResult{
			TxnSeq:             req.TxnSeq,
			Status:             string(mvcc.TxnAborted),
			IntentID:           intentID,
			AbortedVersionSeqs: abortedSeqs,
			ResetHeadRowKeys:   resetRows,
			RequestID:          run.ID,
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

		txn.Status = mvcc.TxnAborted
		e.txns[req.TxnSeq] = txn
		for _, seq := range abortedSeqs {
			v := e.versions[seq]
			v.CommitState = mvcc.VersionAborted
			e.versions[seq] = v
		}
		for rowKey, newHead := range newHeads {
			if newHead.Valid {
				e.heads[rowKey] = newHead.Int64
			} else {
				delete(e.heads, rowKey)
			}
		}
		return result, nil
	})
}
