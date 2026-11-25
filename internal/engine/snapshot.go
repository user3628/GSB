package engine

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"alarmengine/internal/errcode"
	"alarmengine/internal/model"
	"alarmengine/internal/sample"
	"alarmengine/internal/storage"
)

// ReplaceSnapshotRequest carries a full replacement snapshot and its base.
type ReplaceSnapshotRequest struct {
	RequestID   string               `json:"request_id"`
	BaseVersion int64                `json:"base_version"`
	Snapshot    model.PolicySnapshot `json:"snapshot"`
}

// ReplaceSnapshotResult summarizes an atomic snapshot replacement.
type ReplaceSnapshotResult struct {
	RequestID     string `json:"request_id"`
	OldVersion    int64  `json:"old_version"`
	NewVersion    int64  `json:"new_version"`
	RebuiltAlerts int    `json:"rebuilt_alerts"`
	RebuiltScores int    `json:"rebuilt_scores"`
}

// ReplaceSnapshot atomically replaces the active policy snapshot. Validation
// failures (monotonic version, checksum) return E_SNAPSHOT_INVALID and leave
// the previous snapshot fully usable; new/old content cannot mix because all
// derived rows are rebuilt inside one transaction.
func (e *Engine) ReplaceSnapshot(req ReplaceSnapshotRequest) (ReplaceSnapshotResult, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	if req.RequestID == "" {
		return ReplaceSnapshotResult{}, errcode.New(errcode.ESampleInvalid, "missing request_id")
	}
	payloadHash := hashPayload(req)
	if cached, conflict, err := e.checkIdempotent(req.RequestID, "snapshot", payloadHash); err != nil {
		return ReplaceSnapshotResult{}, err
	} else if conflict != nil {
		return ReplaceSnapshotResult{}, conflict
	} else if cached != nil {
		var res ReplaceSnapshotResult
		if err := json.Unmarshal([]byte(*cached), &res); err == nil {
			return res, nil
		}
	}

	current := e.Version()
	if req.BaseVersion != current {
		return ReplaceSnapshotResult{}, errcode.New(errcode.EStaleBase,
			"base version %d != committed version %d", req.BaseVersion, current)
	}
	p := req.Snapshot
	if err := model.ValidatePolicySnapshot(p); err != nil {
		return ReplaceSnapshotResult{}, err
	}
	if p.Version <= current {
		return ReplaceSnapshotResult{}, errcode.New(errcode.ESnapshotInvalid,
			"snapshot version %d must be > current %d", p.Version, current)
	}
	if got := sample.ComputeChecksum(p); got != p.Checksum {
		return ReplaceSnapshotResult{}, errcode.New(errcode.ESnapshotInvalid,
			"checksum mismatch: got %s want %s", got, p.Checksum)
	}

	now := e.cfg.Now()
	intentID := e.nextIntentID("intent-snapshot-" + req.RequestID)
	tx, err := e.store.BeginWrite()
	if err != nil {
		return ReplaceSnapshotResult{}, err
	}
	if err := storage.InsertIntent(tx, intentID, storage.IntentReplaceSnapshot, current, req, now); err != nil {
		_ = tx.Rollback()
		return ReplaceSnapshotResult{}, err
	}
	if err := storage.InsertRequest(tx, req.RequestID, "snapshot", payloadHash, now); err != nil {
		_ = tx.Rollback()
		return ReplaceSnapshotResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return ReplaceSnapshotResult{}, err
	}
	e.maybeCrash(CrashAfterIntentSnapshot, intentID)

	tx, err = e.store.BeginWrite()
	if err != nil {
		return ReplaceSnapshotResult{}, err
	}
	commitErr := func() error {
		if err := e.installSnapshotInTx(tx, p, now); err != nil {
			return err
		}
		if err := e.rebuildWithPolicy(tx, p.Version, p); err != nil {
			return err
		}
		if err := storage.SetMeta(tx, storage.MetaCommittedVersion, fmt.Sprintf("%d", p.Version)); err != nil {
			return err
		}
		if err := storage.SetMeta(tx, storage.MetaLastCompleteVer, fmt.Sprintf("%d", p.Version)); err != nil {
			return err
		}
		if err := storage.CompleteIntent(tx, intentID); err != nil {
			return err
		}
		alerts, _ := storage.LoadAlerts(tx, p.Version)
		scores, _ := storage.LoadRootCauseScores(tx, p.Version)
		res := ReplaceSnapshotResult{
			RequestID:     req.RequestID,
			OldVersion:    current,
			NewVersion:    p.Version,
			RebuiltAlerts: len(alerts),
			RebuiltScores: len(scores),
		}
		raw, _ := json.Marshal(res)
		return storage.CompleteRequest(tx, req.RequestID, string(raw))
	}()
	if commitErr != nil {
		_ = tx.Rollback()
		return ReplaceSnapshotResult{}, commitErr
	}
	if err := tx.Commit(); err != nil {
		return ReplaceSnapshotResult{}, err
	}

	e.mu.Lock()
	e.policy = p
	e.version = p.Version
	e.lastGood = p.Version
	e.mu.Unlock()
	return ReplaceSnapshotResult{
		RequestID: req.RequestID, OldVersion: current, NewVersion: p.Version,
		RebuiltAlerts: e.countAlerts(p.Version), RebuiltScores: e.countScores(p.Version),
	}, nil
}

// installSnapshotLocked persists a validated snapshot on the bootstrap path.
func (e *Engine) installSnapshotLocked(p model.PolicySnapshot) error {
	now := e.cfg.Now()
	tx, err := e.store.BeginWrite()
	if err != nil {
		return err
	}
	if err := e.installSnapshotInTx(tx, p, now); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := e.rebuildWithPolicy(tx, p.Version, p); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := storage.SetMeta(tx, storage.MetaCommittedVersion, fmt.Sprintf("%d", p.Version)); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := storage.SetMeta(tx, storage.MetaLastCompleteVer, fmt.Sprintf("%d", p.Version)); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	e.mu.Lock()
	e.policy = p
	e.version = p.Version
	e.lastGood = p.Version
	e.mu.Unlock()
	return nil
}

func (e *Engine) installSnapshotInTx(tx *sql.Tx, p model.PolicySnapshot, nowMS int64) error {
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return storage.InsertPolicyRow(tx, storage.PolicyRow{
		Version:           p.Version,
		Checksum:          p.Checksum,
		PayloadJSON:       string(raw),
		SuppressionWindow: p.SuppressionWindowMS,
		ConcurrencyLimit:  p.ConcurrencyLimit,
		RoundBudget:       p.RoundBudgetMS,
		ToleranceMille:    p.RootCauseTolerance,
	}, nowMS)
}
