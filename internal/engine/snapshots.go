package engine

import (
	"encoding/json"
	"os"
	"path/filepath"

	"mvccengine/pkg/mvcc"
)

type SwitchSnapshotRequest struct {
	RequestID     string `json:"request_id,omitempty"`
	Path          string `json:"path,omitempty"`
	SnapshotSeq   int64  `json:"snapshot_seq,omitempty"`
	GeneratedAtMS int64  `json:"generated_at_ms,omitempty"`
	TxnUpperBound int64  `json:"txn_upper_bound,omitempty"`
	Checksum      string `json:"checksum,omitempty"`
}

type SwitchSnapshotResult struct {
	OldSnapshotSeq          int64         `json:"old_snapshot_seq"`
	NewSnapshotSeq          int64         `json:"new_snapshot_seq"`
	Snapshot                mvcc.Snapshot `json:"snapshot"`
	IntentID                string        `json:"intent_id"`
	AffectedTransactions    int           `json:"affected_transactions"`
	TotalActiveTransactions int           `json:"total_active_transactions"`
	PinnedTransactionSeqs   []int64       `json:"pinned_transaction_seqs"`
	SwitchedViews           []string      `json:"switched_views"`
	RequestID               string        `json:"request_id"`
}

func WriteSnapshotFile(path string, snapshot mvcc.Snapshot) error {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	b, err := json.MarshalIndent(snapshot.WithChecksum(), "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func LoadSnapshotFile(path string) (mvcc.Snapshot, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return mvcc.Snapshot{}, mvcc.NewError(mvcc.CodeSnapshotInvalid, "cannot read snapshot file", map[string]any{"path": path, "cause": err.Error()})
	}
	var snapshot mvcc.Snapshot
	if err := json.Unmarshal(b, &snapshot); err != nil {
		return mvcc.Snapshot{}, mvcc.NewError(mvcc.CodeSnapshotInvalid, "snapshot file is not valid JSON", map[string]any{"path": path, "cause": err.Error()})
	}
	if snapshot.SnapshotSeq <= 0 {
		return mvcc.Snapshot{}, mvcc.NewError(mvcc.CodeSnapshotInvalid, "snapshot_seq must be positive", map[string]any{"path": path})
	}
	if snapshot.TxnUpperBound < 0 {
		return mvcc.Snapshot{}, mvcc.NewError(mvcc.CodeSnapshotInvalid, "txn_upper_bound must not be negative", map[string]any{"path": path})
	}
	if err := snapshot.ValidateChecksum(); err != nil {
		me := mvcc.AsError(err)
		if me.Context == nil {
			me.Context = map[string]any{}
		}
		me.Context["path"] = path
		return mvcc.Snapshot{}, me
	}
	return snapshot, nil
}

func (e *Engine) SwitchSnapshot(req SwitchSnapshotRequest) (*SwitchSnapshotResult, error) {
	return executeAs(e, req.RequestID, "switch_snapshot", req, func(run *RequestRun) (*SwitchSnapshotResult, error) {
		var snapshot mvcc.Snapshot
		if req.Path != "" {
			loaded, err := LoadSnapshotFile(req.Path)
			if err != nil {
				return nil, err
			}
			snapshot = loaded
		} else {
			snapshot = mvcc.Snapshot{
				SnapshotSeq:   req.SnapshotSeq,
				GeneratedAtMS: req.GeneratedAtMS,
				TxnUpperBound: req.TxnUpperBound,
				Checksum:      req.Checksum,
			}
			if snapshot.GeneratedAtMS == 0 {
				snapshot.GeneratedAtMS = e.now()
			}
			if snapshot.Checksum == "" {
				snapshot = snapshot.WithChecksum()
			}
			if err := snapshot.ValidateChecksum(); err != nil {
				return nil, err
			}
		}

		e.mu.Lock()
		defer e.mu.Unlock()

		old := e.current
		if snapshot.SnapshotSeq < old.SnapshotSeq {
			return nil, mvcc.NewError(mvcc.CodeSnapshotInvalid, "snapshot_seq must be monotonic relative to current snapshot", map[string]any{
				"old_snapshot_seq": old.SnapshotSeq,
				"new_snapshot_seq": snapshot.SnapshotSeq,
			})
		}
		if snapshot.SnapshotSeq == old.SnapshotSeq {
			if snapshot.Checksum != old.Checksum || snapshot.TxnUpperBound != old.TxnUpperBound {
				return nil, mvcc.NewError(mvcc.CodeSnapshotInvalid, "snapshot_seq is already used by a different snapshot", map[string]any{
					"snapshot_seq":            snapshot.SnapshotSeq,
					"existing_checksum":       old.Checksum,
					"conflicting_checksum":    snapshot.Checksum,
					"existing_upper_bound":    old.TxnUpperBound,
					"conflicting_upper_bound": snapshot.TxnUpperBound,
				})
			}
			pinned := []int64{}
			for seq, txn := range e.txns {
				if txn.Status == mvcc.TxnActive {
					pinned = append(pinned, seq)
				}
			}
			sortInt64(pinned)
			return &SwitchSnapshotResult{
				OldSnapshotSeq:          old.SnapshotSeq,
				NewSnapshotSeq:          old.SnapshotSeq,
				Snapshot:                old,
				AffectedTransactions:    len(pinned),
				TotalActiveTransactions: len(pinned),
				PinnedTransactionSeqs:   pinned,
				SwitchedViews:           []string{"current_snapshot", "snapshot_read_view"},
				RequestID:               run.ID,
			}, nil
		}
		if snapshot.TxnUpperBound < old.TxnUpperBound {
			return nil, mvcc.NewError(mvcc.CodeSnapshotInvalid, "txn_upper_bound must not move backwards", map[string]any{
				"old_upper_bound": old.TxnUpperBound,
				"new_upper_bound": snapshot.TxnUpperBound,
			})
		}

		pinned := []int64{}
		for seq, txn := range e.txns {
			if txn.Status == mvcc.TxnActive {
				pinned = append(pinned, seq)
			}
		}
		sortInt64(pinned)

		intentID, err := e.createIntent(run, "switch_snapshot", snapshot)
		if err != nil {
			return nil, err
		}
		if e.opts.SwitchIntentHook != nil {
			e.opts.SwitchIntentHook(intentID, snapshot)
		}

		dbTx, err := e.db.Begin()
		if err != nil {
			return nil, err
		}
		defer func() { _ = dbTx.Rollback() }()

		if _, err := dbTx.Exec(`INSERT INTO snapshots(snapshot_seq,generated_at_ms,txn_upper_bound,checksum,is_current,source_path)
			VALUES(?,?,?,?,0,?)
			ON CONFLICT(snapshot_seq) DO UPDATE SET generated_at_ms=excluded.generated_at_ms,
				txn_upper_bound=excluded.txn_upper_bound, checksum=excluded.checksum`,
			snapshot.SnapshotSeq, snapshot.GeneratedAtMS, snapshot.TxnUpperBound, snapshot.Checksum, req.Path); err != nil {
			return nil, err
		}
		if _, err := dbTx.Exec(`UPDATE snapshots SET is_current=0`); err != nil {
			return nil, err
		}
		if _, err := dbTx.Exec(`UPDATE snapshots SET is_current=1 WHERE snapshot_seq=?`, snapshot.SnapshotSeq); err != nil {
			return nil, err
		}
		if err := e.persistMetaTx(dbTx); err != nil {
			return nil, err
		}
		if _, err := dbTx.Exec(`INSERT OR REPLACE INTO meta(key,value) VALUES('current_snapshot_seq',?)`, snapshot.SnapshotSeq); err != nil {
			return nil, err
		}

		result := &SwitchSnapshotResult{
			OldSnapshotSeq:          old.SnapshotSeq,
			NewSnapshotSeq:          snapshot.SnapshotSeq,
			Snapshot:                snapshot,
			IntentID:                intentID,
			AffectedTransactions:    len(pinned),
			TotalActiveTransactions: len(pinned),
			PinnedTransactionSeqs:   pinned,
			SwitchedViews:           []string{"current_snapshot", "snapshot_read_view"},
			RequestID:               run.ID,
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

		e.current = snapshot
		e.snapshots[snapshot.SnapshotSeq] = snapshot
		return result, nil
	})
}

func sortInt64(values []int64) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j-1] > values[j]; j-- {
			values[j-1], values[j] = values[j], values[j-1]
		}
	}
}
