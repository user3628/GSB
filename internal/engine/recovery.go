package engine

import (
	"database/sql"
	"encoding/json"
	"sort"

	"mvccengine/pkg/mvcc"
)

type RecoveryReport struct {
	StartedAtMS            int64    `json:"started_at_ms"`
	FinishedAtMS           int64    `json:"finished_at_ms"`
	PendingIntentsScanned  int      `json:"pending_intents_scanned"`
	RolledBackVersionSeqs  []int64  `json:"rolled_back_version_seqs"`
	DeletedVersionSeqs     []int64  `json:"deleted_version_seqs,omitempty"`
	RestoredActiveTxnSeqs  []int64  `json:"restored_active_txn_seqs"`
	AbortedTxnSeqs         []int64  `json:"aborted_txn_seqs"`
	ResetHeadRowKeys       []string `json:"reset_head_row_keys"`
	AbortedIntentIDs       []string `json:"aborted_intent_ids"`
	CompletedIntentIDs     []string `json:"completed_intent_ids"`
	InterruptedRequestKeys []string `json:"interrupted_request_keys"`
	CurrentSnapshotSeq     int64    `json:"current_snapshot_seq"`
	FactsOnly              bool     `json:"facts_only"`
}

type pendingIntent struct {
	ID          string
	Kind        string
	RequestID   string
	PayloadJSON string
}

func (e *Engine) RecoverPending() (*RecoveryReport, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	report := &RecoveryReport{
		StartedAtMS:            e.now(),
		RolledBackVersionSeqs:  []int64{},
		RestoredActiveTxnSeqs:  []int64{},
		AbortedTxnSeqs:         []int64{},
		ResetHeadRowKeys:       []string{},
		AbortedIntentIDs:       []string{},
		CompletedIntentIDs:     []string{},
		InterruptedRequestKeys: []string{},
		FactsOnly:              true,
	}

	rows, err := e.db.Query(`SELECT intent_id,kind,request_id,payload_json FROM intents WHERE status='PENDING' ORDER BY created_at_ms,intent_id`)
	if err != nil {
		return nil, err
	}
	intents := []pendingIntent{}
	for rows.Next() {
		var intent pendingIntent
		if err := rows.Scan(&intent.ID, &intent.Kind, &intent.RequestID, &intent.PayloadJSON); err != nil {
			_ = rows.Close()
			return nil, err
		}
		intents = append(intents, intent)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	report.PendingIntentsScanned = len(intents)

	tx, err := e.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	for _, intent := range intents {
		switch intent.Kind {
		case "write_version":
			if err := e.recoverWriteIntentLocked(tx, intent, report); err != nil {
				return nil, err
			}
		case "commit_txn":
			if err := e.recoverCommitIntentLocked(tx, intent, report); err != nil {
				return nil, err
			}
		case "abort_txn", "rollback_txn":
			if err := e.recoverAbortIntentLocked(tx, intent, report); err != nil {
				return nil, err
			}
		case "switch_snapshot":
			if err := e.recoverSnapshotIntentLocked(tx, intent, report); err != nil {
				return nil, err
			}
		case "reclaim":
			if err := e.recoverReclaimIntentLocked(tx, intent, report); err != nil {
				return nil, err
			}
		default:
			if _, err := tx.Exec(`UPDATE intents SET status='ABORTED', updated_at_ms=? WHERE intent_id=?`, e.now(), intent.ID); err != nil {
				return nil, err
			}
			report.AbortedIntentIDs = append(report.AbortedIntentIDs, intent.ID)
		}
	}

	reqRows, err := tx.Query(`SELECT request_id FROM idempotent_requests WHERE status='PENDING' ORDER BY created_at_ms`)
	if err != nil {
		return nil, err
	}
	pendingRequests := []string{}
	for reqRows.Next() {
		var requestID string
		if err := reqRows.Scan(&requestID); err != nil {
			_ = reqRows.Close()
			return nil, err
		}
		pendingRequests = append(pendingRequests, requestID)
	}
	if err := reqRows.Close(); err != nil {
		return nil, err
	}
	errorPayload := jsonMarshal(mvcc.NewError(mvcc.CodeInterrupted, "process terminated before request completion; recovery resolved the intent", nil))
	for _, requestID := range pendingRequests {
		if _, err := tx.Exec(`UPDATE idempotent_requests SET status='FAILED', error_json=?, updated_at_ms=? WHERE request_id=? AND status='PENDING'`, errorPayload, e.now(), requestID); err != nil {
			return nil, err
		}
		report.InterruptedRequestKeys = append(report.InterruptedRequestKeys, requestID)
	}

	report.FinishedAtMS = e.now()
	payload := jsonMarshal(report)
	if _, err := tx.Exec(`INSERT OR REPLACE INTO meta(key,value) VALUES('last_recovery_report',?)`, payload); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	if err := e.loadStateLocked(); err != nil {
		return nil, err
	}
	for seq, txn := range e.txns {
		if txn.Status == mvcc.TxnActive {
			report.RestoredActiveTxnSeqs = append(report.RestoredActiveTxnSeqs, seq)
		}
	}
	sortInt64(report.RestoredActiveTxnSeqs)
	sortInt64(report.AbortedTxnSeqs)
	sortInt64(report.RolledBackVersionSeqs)
	sort.Strings(report.InterruptedRequestKeys)
	report.CurrentSnapshotSeq = e.current.SnapshotSeq
	e.recovery = report
	return report, nil
}

func (e *Engine) recoverWriteIntentLocked(tx *sql.Tx, intent pendingIntent, report *RecoveryReport) error {
	var version mvcc.RowVersion
	if err := json.Unmarshal([]byte(intent.PayloadJSON), &version); err != nil {
		return err
	}
	existing, exists := e.versions[version.VersionSeq]
	if exists && existing.CommitState == mvcc.VersionCommitted {
		if _, ok := e.txns[version.CreateTxnSeq]; ok && e.txns[version.CreateTxnSeq].Status == mvcc.TxnCommitted {
			return markIntentDoneTx(tx, intent.ID, map[string]any{"reconciled": "committed_write"}, report)
		}
	}

	if exists {
		if _, err := tx.Exec(`DELETE FROM versions WHERE version_seq=?`, version.VersionSeq); err != nil {
			return err
		}
		report.DeletedVersionSeqs = append(report.DeletedVersionSeqs, version.VersionSeq)
		report.RolledBackVersionSeqs = append(report.RolledBackVersionSeqs, version.VersionSeq)
		if version.PrevVersionSeq != nil {
			if _, err := tx.Exec(`UPDATE row_heads SET version_seq=? WHERE row_key=? AND version_seq=?`,
				*version.PrevVersionSeq, version.RowKey, version.VersionSeq); err != nil {
				return err
			}
		} else {
			if _, err := tx.Exec(`DELETE FROM row_heads WHERE row_key=? AND version_seq=?`, version.RowKey, version.VersionSeq); err != nil {
				return err
			}
		}
		report.ResetHeadRowKeys = appendUnique(report.ResetHeadRowKeys, version.RowKey)
	}
	if _, err := tx.Exec(`UPDATE intents SET status='ABORTED', updated_at_ms=? WHERE intent_id=?`, e.now(), intent.ID); err != nil {
		return err
	}
	report.AbortedIntentIDs = append(report.AbortedIntentIDs, intent.ID)
	return nil
}

func (e *Engine) recoverCommitIntentLocked(tx *sql.Tx, intent pendingIntent, report *RecoveryReport) error {
	var payload struct {
		TxnSeq int64 `json:"txn_seq"`
	}
	if err := json.Unmarshal([]byte(intent.PayloadJSON), &payload); err != nil {
		return err
	}
	txn, exists := e.txns[payload.TxnSeq]
	if exists && txn.Status == mvcc.TxnCommitted && e.allTxnVersionsCommitted(payload.TxnSeq) {
		return markIntentDoneTx(tx, intent.ID, map[string]any{"reconciled": "committed"}, report)
	}

	versionSeqs := []int64{}
	rowKeys := []string{}
	for seq, version := range e.versions {
		if version.CreateTxnSeq == payload.TxnSeq {
			versionSeqs = append(versionSeqs, seq)
			rowKeys = appendUnique(rowKeys, version.RowKey)
		}
	}
	if _, err := tx.Exec(`UPDATE txns SET status=? WHERE seq=?`, string(mvcc.TxnAborted), payload.TxnSeq); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE versions SET commit_state=?, updated_at_ms=? WHERE create_txn_seq=?`,
		string(mvcc.VersionAborted), e.now(), payload.TxnSeq); err != nil {
		return err
	}
	report.AbortedTxnSeqs = append(report.AbortedTxnSeqs, payload.TxnSeq)
	report.RolledBackVersionSeqs = append(report.RolledBackVersionSeqs, versionSeqs...)

	for _, rowKey := range rowKeys {
		var newHead sql.NullInt64
		if err := tx.QueryRow(`SELECT MAX(version_seq) FROM versions WHERE row_key=? AND commit_state=?`,
			rowKey, string(mvcc.VersionCommitted)).Scan(&newHead); err != nil {
			return err
		}
		if newHead.Valid {
			if _, err := tx.Exec(`INSERT INTO row_heads(row_key,version_seq) VALUES(?,?)
				ON CONFLICT(row_key) DO UPDATE SET version_seq=excluded.version_seq`, rowKey, newHead.Int64); err != nil {
				return err
			}
		} else {
			if _, err := tx.Exec(`DELETE FROM row_heads WHERE row_key=?`, rowKey); err != nil {
				return err
			}
		}
		report.ResetHeadRowKeys = appendUnique(report.ResetHeadRowKeys, rowKey)
	}

	if _, err := tx.Exec(`UPDATE intents SET status='ABORTED', updated_at_ms=? WHERE intent_id=?`, e.now(), intent.ID); err != nil {
		return err
	}
	report.AbortedIntentIDs = append(report.AbortedIntentIDs, intent.ID)
	return nil
}

func (e *Engine) recoverAbortIntentLocked(tx *sql.Tx, intent pendingIntent, report *RecoveryReport) error {
	var payload struct {
		TxnSeq int64 `json:"txn_seq"`
	}
	if err := json.Unmarshal([]byte(intent.PayloadJSON), &payload); err != nil {
		return err
	}
	if txn, ok := e.txns[payload.TxnSeq]; ok && txn.Status != mvcc.TxnAborted {
		if _, err := tx.Exec(`UPDATE txns SET status=? WHERE seq=?`, string(mvcc.TxnAborted), payload.TxnSeq); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE versions SET commit_state=? WHERE create_txn_seq=?`, string(mvcc.VersionAborted), payload.TxnSeq); err != nil {
			return err
		}
		report.AbortedTxnSeqs = append(report.AbortedTxnSeqs, payload.TxnSeq)
	}
	return markIntentDoneTx(tx, intent.ID, map[string]any{"reconciled": "aborted"}, report)
}

func (e *Engine) recoverSnapshotIntentLocked(tx *sql.Tx, intent pendingIntent, report *RecoveryReport) error {
	var snapshot mvcc.Snapshot
	if err := json.Unmarshal([]byte(intent.PayloadJSON), &snapshot); err != nil {
		return err
	}
	var currentSeq int64
	err := tx.QueryRow(`SELECT CAST(value AS INTEGER) FROM meta WHERE key='current_snapshot_seq'`).Scan(&currentSeq)
	if err == nil && currentSeq == snapshot.SnapshotSeq {
		var count int
		if err := tx.QueryRow(`SELECT COUNT(1) FROM snapshots WHERE snapshot_seq=? AND checksum=? AND is_current=1`,
			snapshot.SnapshotSeq, snapshot.Checksum).Scan(&count); err != nil {
			return err
		}
		if count == 1 {
			return markIntentDoneTx(tx, intent.ID, snapshot, report)
		}
	}
	if _, err := tx.Exec(`UPDATE intents SET status='ABORTED', updated_at_ms=? WHERE intent_id=?`, e.now(), intent.ID); err != nil {
		return err
	}
	report.AbortedIntentIDs = append(report.AbortedIntentIDs, intent.ID)
	return nil
}

func (e *Engine) recoverReclaimIntentLocked(tx *sql.Tx, intent pendingIntent, report *RecoveryReport) error {
	var payload struct {
		VersionSeqs []mvcc.RowVersion `json:"version_seqs"`
		Seqs        []int64           `json:"seqs"`
	}
	if err := json.Unmarshal([]byte(intent.PayloadJSON), &payload); err != nil {
		return err
	}
	seqs := payload.Seqs
	for _, version := range payload.VersionSeqs {
		seqs = append(seqs, version.VersionSeq)
	}
	allAbsent := len(seqs) > 0
	for _, seq := range seqs {
		if _, exists := e.versions[seq]; exists {
			allAbsent = false
			break
		}
	}
	if allAbsent {
		return markIntentDoneTx(tx, intent.ID, map[string]any{"reconciled": "reclaimed"}, report)
	}
	if _, err := tx.Exec(`UPDATE intents SET status='ABORTED', updated_at_ms=? WHERE intent_id=?`, e.now(), intent.ID); err != nil {
		return err
	}
	report.AbortedIntentIDs = append(report.AbortedIntentIDs, intent.ID)
	return nil
}

func markIntentDoneTx(tx *sql.Tx, intentID string, result any, report *RecoveryReport) error {
	if err := completeIntentTx(tx, intentID, result); err != nil {
		return err
	}
	report.CompletedIntentIDs = append(report.CompletedIntentIDs, intentID)
	return nil
}

func (e *Engine) allTxnVersionsCommitted(txnSeq int64) bool {
	for _, version := range e.versions {
		if version.CreateTxnSeq == txnSeq && version.CommitState != mvcc.VersionCommitted {
			return false
		}
	}
	return true
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func (e *Engine) LastRecoveryReport() *RecoveryReport {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.recovery != nil {
		copyReport := *e.recovery
		return &copyReport
	}
	var payload string
	if err := e.db.QueryRow(`SELECT value FROM meta WHERE key='last_recovery_report'`).Scan(&payload); err != nil {
		return nil
	}
	var report RecoveryReport
	if err := json.Unmarshal([]byte(payload), &report); err != nil {
		return nil
	}
	return &report
}
