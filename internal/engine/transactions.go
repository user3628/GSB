package engine

import (
	"database/sql"

	"mvccengine/pkg/mvcc"
)

type BeginTxnRequest struct {
	RequestID string `json:"request_id,omitempty"`
}

type ReadRequest struct {
	TxnSeq      int64  `json:"txn_seq,omitempty"`
	SnapshotSeq int64  `json:"snapshot_seq,omitempty"`
	RowKey      string `json:"row_key"`
}

func (e *Engine) BeginTxn(req BeginTxnRequest) (*mvcc.Txn, error) {
	return executeAs(e, req.RequestID, "begin_txn", req, func(run *RequestRun) (*mvcc.Txn, error) {
		e.mu.Lock()
		defer e.mu.Unlock()

		seq := e.nextTxnSeq + 1
		txn := mvcc.Txn{
			Seq:         seq,
			Status:      mvcc.TxnActive,
			StartedAtMS: e.now(),
			SnapshotSeq: e.current.SnapshotSeq,
			UpperBound:  seq,
			RequestID:   run.ID,
		}

		dbTx, err := e.db.Begin()
		if err != nil {
			return nil, err
		}
		defer func() { _ = dbTx.Rollback() }()

		if _, err := dbTx.Exec(`INSERT INTO txns(seq,status,started_at_ms,snapshot_seq,upper_bound,request_id)
			VALUES(?,?,?,?,?,?)`, txn.Seq, string(txn.Status), txn.StartedAtMS, txn.SnapshotSeq, txn.UpperBound, txn.RequestID); err != nil {
			return nil, err
		}
		if _, err := dbTx.Exec(`INSERT OR REPLACE INTO meta(key,value) VALUES('next_txn_seq',?)`, seq); err != nil {
			return nil, err
		}
		result := struct {
			Txn *mvcc.Txn `json:"txn"`
		}{Txn: &txn}
		if err := run.CompleteTx(dbTx, result, ""); err != nil {
			return nil, err
		}
		if err := dbTx.Commit(); err != nil {
			return nil, err
		}

		e.nextTxnSeq = seq
		e.txns[seq] = txn
		return &txn, nil
	})
}

func (e *Engine) Read(req ReadRequest) (*mvcc.ReadResult, error) {
	if req.RowKey == "" {
		return nil, mvcc.NewError(mvcc.CodeVersionInvalid, "row_key must not be empty", nil)
	}

	e.mu.RLock()
	view := cloneViewLocked(e)
	reader, err := e.readerLocked(req)
	e.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	result := mvcc.Evaluate(view, reader, req.RowKey)
	return &result, nil
}

func (e *Engine) readerLocked(req ReadRequest) (mvcc.Txn, error) {
	if req.TxnSeq > 0 {
		txn, ok := e.txns[req.TxnSeq]
		if !ok {
			return mvcc.Txn{}, mvcc.NewError(mvcc.CodeTxnInvalid, "read transaction does not exist", map[string]any{"txn_seq": req.TxnSeq})
		}
		return txn, nil
	}
	if req.SnapshotSeq > 0 {
		snap, ok := e.snapshots[req.SnapshotSeq]
		if !ok {
			return mvcc.Txn{}, mvcc.NewError(mvcc.CodeSnapshotInvalid, "snapshot does not exist", map[string]any{"snapshot_seq": req.SnapshotSeq})
		}
		return mvcc.Txn{
			Seq:         0,
			Status:      mvcc.TxnActive,
			SnapshotSeq: snap.SnapshotSeq,
			UpperBound:  snap.TxnUpperBound,
		}, nil
	}
	return mvcc.Txn{}, mvcc.NewError(mvcc.CodeTxnInvalid, "txn_seq or snapshot_seq is required for a snapshot read", nil)
}

func (e *Engine) getTxn(seq int64) (mvcc.Txn, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	txn, ok := e.txns[seq]
	return txn, ok
}

var _ = sql.ErrNoRows
