package engine

import (
	"sort"

	"mvccengine/pkg/mvcc"
)

type ImportSampleResult struct {
	Name                 string `json:"name"`
	ImportedTxnCount     int    `json:"imported_txn_count"`
	ImportedVersionCount int    `json:"imported_version_count"`
	NextTxnSeq           int64  `json:"next_txn_seq"`
	NextVersionSeq       int64  `json:"next_version_seq"`
}

func (e *Engine) ImportSample(sample mvcc.Sample) (*ImportSampleResult, error) {
	if err := sample.Validate(); err != nil {
		return nil, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.nextTxnSeq != 0 || len(e.txns) != 0 {
		return nil, mvcc.NewError(mvcc.CodeTxnInvalid, "sample batch import requires an empty engine", map[string]any{
			"next_txn_seq": e.nextTxnSeq,
			"txn_count":    len(e.txns),
		})
	}

	tx, err := e.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	maxVersionSeq := int64(0)
	heads := map[string]int64{}
	for _, txn := range sample.Transactions {
		snapshotSeq := int64(1)
		if _, err := tx.Exec(`INSERT INTO txns(seq,status,started_at_ms,snapshot_seq,upper_bound,request_id)
			VALUES(?,?,?,1,?,'sample')`, txn.TxnSeq, string(txn.Status), txn.StartedAtMS, txn.TxnSeq); err != nil {
			return nil, mvcc.NewError(mvcc.CodeTxnInvalid, "failed to persist sample transaction", map[string]any{"txn_seq": txn.TxnSeq, "cause": err.Error()})
		}
		_ = snapshotSeq
	}
	for _, version := range sample.Versions {
		digest, _ := mvcc.NormalizeDigest(version.Digest)
		var prev any
		if version.PrevVersionSeq != nil {
			prev = *version.PrevVersionSeq
		}
		if _, err := tx.Exec(`INSERT INTO versions(row_key,version_seq,create_txn_seq,commit_state,digest,content,prev_version_seq,is_delete,superseding_commit_seq,reclaimed,detached,created_at_ms,updated_at_ms)
			VALUES(?,?,?,?,?,?,?,? ,0,0,0,?,?)`,
			version.RowKey, version.VersionSeq, version.CreateTxnSeq, string(version.CommitState), digest, version.Content, prev, boolToInt(version.IsDelete),
			sampleTimestamp(sample, version.VersionSeq), sampleTimestamp(sample, version.VersionSeq)); err != nil {
			return nil, mvcc.NewError(mvcc.CodeVersionInvalid, "failed to persist sample version", map[string]any{"version_seq": version.VersionSeq, "cause": err.Error()})
		}
		if version.VersionSeq > heads[version.RowKey] {
			heads[version.RowKey] = version.VersionSeq
		}
		if version.VersionSeq > maxVersionSeq {
			maxVersionSeq = version.VersionSeq
		}
	}
	for rowKey, seq := range heads {
		if _, err := tx.Exec(`INSERT INTO row_heads(row_key,version_seq) VALUES(?,?)`, rowKey, seq); err != nil {
			return nil, err
		}
	}
	if _, err := tx.Exec(`INSERT OR REPLACE INTO meta(key,value) VALUES('next_txn_seq',?)`, int64(len(sample.Transactions))); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(`INSERT OR REPLACE INTO meta(key,value) VALUES('next_version_seq',?)`, maxVersionSeq); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	if err := e.loadStateLocked(); err != nil {
		return nil, err
	}

	return &ImportSampleResult{
		Name:                 sample.Name,
		ImportedTxnCount:     len(sample.Transactions),
		ImportedVersionCount: len(sample.Versions),
		NextTxnSeq:           int64(len(sample.Transactions)),
		NextVersionSeq:       maxVersionSeq,
	}, nil
}

func sampleTimestamp(sample mvcc.Sample, versionSeq int64) int64 {
	for _, txn := range sample.Transactions {
		if txn.TxnSeq == versionSeq {
			return txn.StartedAtMS
		}
	}
	if len(sample.Transactions) > 0 {
		return sample.Transactions[0].StartedAtMS + versionSeq
	}
	return 0
}

var _ = sort.Ints
