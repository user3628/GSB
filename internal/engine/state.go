package engine

import (
	"sort"

	"mvccengine/pkg/mvcc"
)

type StateReport struct {
	OK              bool              `json:"ok"`
	NextTxnSeq      int64             `json:"next_txn_seq"`
	NextVersionSeq  int64             `json:"next_version_seq"`
	CurrentSnapshot mvcc.Snapshot     `json:"current_snapshot"`
	ActiveTxnSeqs   []int64           `json:"active_txn_seqs"`
	TxnCount        int               `json:"txn_count"`
	VersionCount    int               `json:"version_count"`
	HeadCount       int               `json:"head_count"`
	Txns            []mvcc.Txn        `json:"txns"`
	Versions        []mvcc.RowVersion `json:"versions"`
	Heads           map[string]int64  `json:"heads"`
	Recovery        *RecoveryReport   `json:"recovery,omitempty"`
}

func (e *Engine) State() StateReport {
	e.mu.RLock()
	defer e.mu.RUnlock()

	report := StateReport{
		OK:              true,
		NextTxnSeq:      e.nextTxnSeq,
		NextVersionSeq:  e.nextVersionSeq,
		CurrentSnapshot: e.current,
		ActiveTxnSeqs:   []int64{},
		Txns:            make([]mvcc.Txn, 0, len(e.txns)),
		Versions:        make([]mvcc.RowVersion, 0, len(e.versions)),
		Heads:           make(map[string]int64, len(e.heads)),
	}
	for seq, txn := range e.txns {
		report.Txns = append(report.Txns, txn)
		if txn.Status == mvcc.TxnActive {
			report.ActiveTxnSeqs = append(report.ActiveTxnSeqs, seq)
		}
		_ = seq
	}
	for _, version := range e.versions {
		report.Versions = append(report.Versions, version)
	}
	for key, seq := range e.heads {
		report.Heads[key] = seq
	}
	sort.Slice(report.Txns, func(i, j int) bool { return report.Txns[i].Seq < report.Txns[j].Seq })
	sort.Slice(report.Versions, func(i, j int) bool { return report.Versions[i].VersionSeq < report.Versions[j].VersionSeq })
	sortInt64(report.ActiveTxnSeqs)
	report.TxnCount = len(report.Txns)
	report.VersionCount = len(report.Versions)
	report.HeadCount = len(report.Heads)
	if e.recovery != nil {
		copyReport := *e.recovery
		report.Recovery = &copyReport
	}
	return report
}

func (e *Engine) Reload() error {
	return e.loadState()
}
