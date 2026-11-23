package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"

	"blocksnap/internal/apperr"
	"blocksnap/internal/metrics"
)

type EvaluateRollbackRequest struct {
	VolumeID    string
	SnapshotIDs []string
	Decision    DecisionInput
}

type EvaluateRollbackResult struct {
	VolumeID           string               `json:"volume_id"`
	CandidateOrder     []string             `json:"candidate_order"`
	Evaluations        []metrics.Evaluation `json:"evaluations"`
	SelectedSnapshotID string               `json:"selected_snapshot_id,omitempty"`
}

type SnapshotListResult struct {
	VolumeID  string         `json:"volume_id"`
	LastSeq   int64          `json:"last_seq"`
	Snapshots []SnapshotView `json:"snapshots"`
}

func (s *Store) GetVolume(ctx context.Context, volumeID string) (Volume, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Volume{}, apperr.Internal(err)
	}
	defer tx.Rollback()
	return s.volumeViewTx(ctx, tx, volumeID)
}

func (s *Store) ListSnapshots(ctx context.Context, volumeID string) (SnapshotListResult, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return SnapshotListResult{}, apperr.Internal(err)
	}
	defer tx.Rollback()
	state, vol, err := s.loadStateTx(ctx, tx, volumeID)
	if err != nil {
		return SnapshotListResult{}, err
	}
	ids := make([]string, 0, len(state.Snapshots))
	for id := range state.Snapshots {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := state.Snapshots[ids[i]], state.Snapshots[ids[j]]
		if a.Ordinal != b.Ordinal {
			return a.Ordinal < b.Ordinal
		}
		return a.ID < b.ID
	})
	views := make([]SnapshotView, 0, len(ids))
	for _, id := range ids {
		view, err := s.snapshotView(state, id)
		if err != nil {
			return SnapshotListResult{}, apperr.WithContext(apperr.New(apperr.CodeMetricStale, "snapshot view cannot be resolved", 500), "detail", err.Error())
		}
		views = append(views, view)
	}
	return SnapshotListResult{VolumeID: vol.ID, LastSeq: vol.LastSeq, Snapshots: views}, nil
}

func (s *Store) GetSnapshotAnalysis(ctx context.Context, volumeID, snapshotID string) (metrics.Analysis, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return metrics.Analysis{}, apperr.Internal(err)
	}
	defer tx.Rollback()
	return s.analysisTx(ctx, tx, volumeID, snapshotID, true)
}

func (s *Store) EvaluateRollback(ctx context.Context, req EvaluateRollbackRequest) (EvaluateRollbackResult, error) {
	if req.Decision.MarginFloor == nil || req.Decision.CostBudgetBytes == nil || req.Decision.RelativeTolerance == nil {
		return EvaluateRollbackResult{}, apperr.Invalid("margin_floor, cost_budget_bytes and relative_tolerance are required")
	}
	if len(req.SnapshotIDs) == 0 {
		return EvaluateRollbackResult{}, apperr.Invalid("at least one candidate snapshot_id is required")
	}
	unique := make(map[string]struct{}, len(req.SnapshotIDs))
	for _, id := range req.SnapshotIDs {
		if strings.TrimSpace(id) == "" {
			return EvaluateRollbackResult{}, apperr.Invalid("candidate snapshot IDs must be non-empty")
		}
		unique[id] = struct{}{}
	}
	ids := make([]string, 0, len(unique))
	for id := range unique {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return EvaluateRollbackResult{}, apperr.Internal(err)
	}
	defer tx.Rollback()
	state, _, err := s.loadStateTx(ctx, tx, req.VolumeID)
	if err != nil {
		return EvaluateRollbackResult{}, err
	}
	candidates := make([]metrics.Analysis, 0, len(ids))
	for _, id := range ids {
		if _, ok := state.Snapshots[id]; !ok {
			return EvaluateRollbackResult{}, apperr.WithContext(apperr.New(apperr.CodeSnapshotNotFound, "candidate snapshot not found", 404), "snapshot_id", id)
		}
		analysis, err := metrics.Analyze(state, id)
		if err != nil {
			return EvaluateRollbackResult{}, apperr.WithContext(apperr.New(apperr.CodeMetricStale, "candidate metrics cannot be derived", 500), "detail", err.Error())
		}
		candidates = append(candidates, analysis)
	}
	evaluations, selected, err := metrics.EvaluateCandidates(candidates, metrics.DecisionRequest{
		MarginFloorUnits: req.Decision.MarginFloor, CostBudgetBytes: req.Decision.CostBudgetBytes,
		RelativeTolerance: req.Decision.RelativeTolerance,
	})
	if err != nil {
		return EvaluateRollbackResult{}, apperr.Internal(err)
	}
	order := make([]string, 0, len(evaluations))
	for _, eval := range evaluations {
		order = append(order, eval.SnapshotID)
	}
	result := EvaluateRollbackResult{VolumeID: req.VolumeID, CandidateOrder: order, Evaluations: evaluations}
	if selected != nil {
		result.SelectedSnapshotID = selected.SnapshotID
	}
	return result, nil
}

func (s *Store) Rollback(ctx context.Context, req RollbackRequest) (MutationResult, error) {
	if req.Decision.MarginFloor == nil || req.Decision.CostBudgetBytes == nil || req.Decision.RelativeTolerance == nil {
		return MutationResult{}, apperr.Invalid("margin_floor, cost_budget_bytes and relative_tolerance are required")
	}
	if req.RequestHash == "" {
		req.RequestHash = RequestHash("rollback", req.VolumeID, map[string]string{
			"snapshot_id": req.SnapshotID, "margin_floor": req.Decision.MarginFloor.String(),
			"cost_budget_bytes":  req.Decision.CostBudgetBytes.String(),
			"relative_tolerance": req.Decision.RelativeTolerance.String(),
		})
	}
	return s.executeMutation(ctx, req.VolumeID, "rollback", req.RequestID, req.RequestHash, func(ctx context.Context, tx *sql.Tx, _ string) (any, error) {
		state, vol, err := s.loadStateTx(ctx, tx, req.VolumeID)
		if err != nil {
			return nil, err
		}
		target, ok := state.Snapshots[req.SnapshotID]
		if !ok {
			return nil, apperr.WithContext(apperr.New(apperr.CodeSnapshotNotFound, "rollback target snapshot not found", 404), "snapshot_id", req.SnapshotID)
		}
		before, err := metrics.Analyze(state, req.SnapshotID)
		if err != nil {
			return nil, apperr.WithContext(apperr.New(apperr.CodeMetricStale, "rollback target cannot be analyzed", 500), "detail", err.Error())
		}
		decision, err := metrics.Decide(before, metrics.DecisionRequest{
			MarginFloorUnits:  req.Decision.MarginFloor,
			CostBudgetBytes:   req.Decision.CostBudgetBytes,
			RelativeTolerance: req.Decision.RelativeTolerance,
		})
		if err != nil {
			return nil, apperr.Internal(err)
		}
		if !decision.Accepted {
			return nil, apperr.WithContext(apperr.New(apperr.CodeRollbackRejected, "rollback rejected by tolerance contract", 409), "snapshot_id", target.ID).
				WithMore("reason", decision.Reason, "margin_ratio", before.Metrics.RollbackMargin.Ratio.Decimal,
					"margin_floor", decision.MarginFloor, "margin_effective_lower", decision.MarginFloorEffectiveLower,
					"cost_bytes", before.Metrics.RollbackCost.CostBytes, "cost_budget", decision.CostBudgetBytes,
					"cost_effective_upper", decision.CostBudgetEffectiveUpper)
		}
		targetRefs, err := metrics.EffectiveRefs(state, req.SnapshotID)
		if err != nil {
			return nil, apperr.WithContext(apperr.New(apperr.CodeMetricStale, "rollback target references cannot be resolved", 500), "detail", err.Error())
		}
		nos := make([]int64, 0, len(targetRefs))
		for no := range targetRefs {
			nos = append(nos, no)
		}
		sort.Slice(nos, func(i, j int) bool { return nos[i] < nos[j] })
		writtenBlocks := int64(0)
		now := s.nowText()
		for _, no := range nos {
			targetVersion := targetRefs[no]
			targetDigest := state.Versions[metrics.VersionKey{BlockNo: no, Version: targetVersion}]
			live := state.Live[no]
			if targetDigest == live.Digest {
				continue
			}
			var content []byte
			var storedDigest, storedAt string
			var isZero int
			err := tx.QueryRowContext(ctx, `SELECT digest,written_at,content,is_zero FROM block_versions WHERE volume_id=? AND block_no=? AND version=?`,
				req.VolumeID, no, targetVersion).Scan(&storedDigest, &storedAt, &content, &isZero)
			if errors.Is(err, sql.ErrNoRows) {
				return nil, apperr.WithContext(apperr.New(apperr.CodeSnapshotCorrupt, "target version content is missing", 500), "block_no", no).WithMore("version", targetVersion)
			}
			if err != nil {
				return nil, apperr.Internal(err)
			}
			if storedDigest != targetDigest {
				return nil, apperr.WithContext(apperr.New(apperr.CodeSnapshotCorrupt, "target version digest mismatch", 500), "block_no", no)
			}
			if live.Version == math.MaxInt64 {
				return nil, apperr.Internal(errors.New("block version exhausted during rollback"))
			}
			newVersion := live.Version + 1
			if _, err := tx.ExecContext(ctx, `INSERT INTO block_versions(volume_id,block_no,version,written_at,digest,content,is_zero) VALUES(?,?,?,?,?,?,?)`,
				req.VolumeID, no, newVersion, now, storedDigest, content, isZero); err != nil {
				return nil, apperr.Internal(err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE blocks SET version=?,written_at=?,digest=?,content=?,is_zero=? WHERE volume_id=? AND block_no=?`,
				newVersion, now, storedDigest, content, isZero, req.VolumeID, no); err != nil {
				return nil, apperr.Internal(err)
			}
			writtenBlocks++
		}
		seq, err := s.nextSeqTx(ctx, tx, req.VolumeID)
		if err != nil {
			return nil, err
		}
		if err := s.recomputeAllTx(ctx, tx, req.VolumeID); err != nil {
			return nil, err
		}
		after, err := s.analysisTx(ctx, tx, req.VolumeID, req.SnapshotID, true)
		if err != nil {
			return nil, err
		}
		writtenBytes := new(big.Int).Mul(big.NewInt(writtenBlocks), big.NewInt(vol.BlockSize))
		return RollbackResult{
			RequestID: req.RequestID, VolumeID: req.VolumeID, SnapshotID: req.SnapshotID,
			Decision: decision, WrittenBackBlocks: writtenBlocks, WrittenBackBytes: writtenBytes.String(),
			NewLiveSeq: seq, Analysis: after,
		}, nil
	})
}

// ReadMetricsJSON is exposed for diagnostics/tests that need to prove the
// persisted metric projection is byte-identical after restart.
func (s *Store) ReadMetricsJSON(ctx context.Context, volumeID, snapshotID string) ([]byte, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT metrics_json FROM snapshot_metrics WHERE volume_id=? AND snapshot_id=?`, volumeID, snapshotID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apperr.WithContext(apperr.New(apperr.CodeMetricStale, "snapshot metrics are missing", 404), "snapshot_id", snapshotID)
	}
	if err != nil {
		return nil, apperr.Internal(err)
	}
	if !json.Valid([]byte(raw)) {
		return nil, apperr.Internal(fmt.Errorf("stored metrics are not valid JSON"))
	}
	return []byte(raw), nil
}
