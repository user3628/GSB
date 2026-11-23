package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"blocksnap/internal/apperr"
	"blocksnap/internal/metrics"
)

func zeroDigest(size int64) string {
	h := sha256.New()
	buf := make([]byte, 64*1024)
	remaining := size
	for remaining > 0 {
		n := int64(len(buf))
		if remaining < n {
			n = remaining
		}
		h.Write(buf[:n])
		remaining -= n
	}
	return hex.EncodeToString(h.Sum(nil))
}

func normalizeTimestamp(input string, now time.Time) (string, error) {
	if strings.TrimSpace(input) == "" {
		return now.UTC().Format(time.RFC3339Nano), nil
	}
	t, err := time.Parse(time.RFC3339Nano, input)
	if err != nil {
		return "", fmt.Errorf("written_at must be RFC3339Nano: %w", err)
	}
	return t.UTC().Format(time.RFC3339Nano), nil
}

func normalizeSHA(input string) string {
	s := strings.ToLower(strings.TrimSpace(input))
	s = strings.TrimPrefix(s, "sha256:")
	return s
}

func (s *Store) CreateVolume(ctx context.Context, req CreateVolumeRequest) (MutationResult, error) {
	if req.RequestHash == "" {
		req.RequestHash = RequestHash("create_volume", req.VolumeID, map[string]int64{
			"block_count": req.BlockCount, "block_size": req.BlockSize, "overhead_bytes": req.OverheadBytes,
		})
	}
	return s.executeMutation(ctx, req.VolumeID, "create_volume", req.RequestID, req.RequestHash, func(ctx context.Context, tx *sql.Tx, _ string) (any, error) {
		if strings.TrimSpace(req.VolumeID) == "" {
			return nil, apperr.Invalid("volume_id is required")
		}
		if req.BlockCount <= 0 || req.BlockSize <= 0 {
			return nil, apperr.Invalid("block_count and block_size must be positive")
		}
		if req.OverheadBytes < 0 {
			return nil, apperr.Invalid("writeback_overhead_bytes_per_block must be non-negative")
		}
		var exists int
		err := tx.QueryRowContext(ctx, `SELECT 1 FROM volumes WHERE id=?`, req.VolumeID).Scan(&exists)
		if err == nil {
			return nil, apperr.WithContext(apperr.New(apperr.CodeVolumeExists, "volume already exists", 409), "volume_id", req.VolumeID)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, apperr.Internal(err)
		}
		createdAt := s.nowText()
		if _, err := tx.ExecContext(ctx, `INSERT INTO volumes(id,block_count,block_size,overhead_bytes,last_seq,created_at) VALUES(?,?,?,?,1,?)`,
			req.VolumeID, req.BlockCount, req.BlockSize, req.OverheadBytes, createdAt); err != nil {
			return nil, apperr.Internal(err)
		}
		digest := zeroDigest(req.BlockSize)
		if _, err := tx.ExecContext(ctx, `WITH RECURSIVE seq(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM seq WHERE n+1 < ?)
			INSERT INTO blocks(volume_id,block_no,version,written_at,digest,content,is_zero)
			SELECT ?,n,0,?,?,NULL,1 FROM seq`, req.BlockCount, req.VolumeID, createdAt, digest); err != nil {
			return nil, apperr.Internal(err)
		}
		if _, err := tx.ExecContext(ctx, `WITH RECURSIVE seq(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM seq WHERE n+1 < ?)
			INSERT INTO block_versions(volume_id,block_no,version,written_at,digest,content,is_zero)
			SELECT ?,n,0,?,?,NULL,1 FROM seq`, req.BlockCount, req.VolumeID, createdAt, digest); err != nil {
			return nil, apperr.Internal(err)
		}
		view, err := s.volumeViewTx(ctx, tx, req.VolumeID)
		if err != nil {
			return nil, err
		}
		return CreateVolumeResult{RequestID: req.RequestID, Volume: view}, nil
	})
}

func (s *Store) WriteBlock(ctx context.Context, req WriteBlockRequest) (MutationResult, error) {
	payload := struct {
		BlockNo         int64  `json:"block_no"`
		ExpectedVersion int64  `json:"expected_version"`
		Digest          string `json:"sha256"`
		ContentLength   int    `json:"content_length"`
		ContentHash     string `json:"content_sha256"`
	}{req.BlockNo, req.ExpectedVersion, normalizeSHA(req.Digest), len(req.Content), fmt.Sprintf("%x", sha256.Sum256(req.Content))}
	if req.RequestHash == "" {
		req.RequestHash = RequestHash("write_block", req.VolumeID, payload)
	}
	return s.executeMutation(ctx, req.VolumeID, "write_block", req.RequestID, req.RequestHash, func(ctx context.Context, tx *sql.Tx, _ string) (any, error) {
		state, vol, err := s.loadStateTx(ctx, tx, req.VolumeID)
		if err != nil {
			return nil, err
		}
		if req.BlockNo < 0 || req.BlockNo >= vol.BlockCount {
			return nil, apperr.WithContext(apperr.Invalid("block_no is outside the volume"), "block_no", req.BlockNo)
		}
		if int64(len(req.Content)) != vol.BlockSize {
			return nil, apperr.WithContext(apperr.Invalid("content length must equal block_size"), "expected", vol.BlockSize).WithMore("actual", len(req.Content))
		}
		actual := sha256.Sum256(req.Content)
		actualHex := hex.EncodeToString(actual[:])
		provided := normalizeSHA(req.Digest)
		if provided == "" || provided != actualHex {
			return nil, apperr.WithContext(apperr.New(apperr.CodeBlockChecksum, "content digest does not match sha256", 422), "block_no", req.BlockNo).
				WithMore("expected", provided, "actual", actualHex)
		}
		current := state.Live[req.BlockNo]
		if current.Version != req.ExpectedVersion {
			return nil, apperr.WithContext(apperr.New(apperr.CodeBlockConflict, "block version changed", 409), "block_no", req.BlockNo).
				WithMore("expected_version", req.ExpectedVersion, "current_version", current.Version)
		}
		if current.Version == math.MaxInt64 {
			return nil, apperr.WithContext(apperr.Internal(errors.New("block version exhausted")), "block_no", req.BlockNo)
		}
		writtenAt, err := normalizeTimestamp(req.WrittenAt, s.now())
		if err != nil {
			return nil, apperr.Invalid(err.Error())
		}
		newVersion := current.Version + 1
		if _, err := tx.ExecContext(ctx, `INSERT INTO block_versions(volume_id,block_no,version,written_at,digest,content,is_zero) VALUES(?,?,?,?,?,?,0)`,
			req.VolumeID, req.BlockNo, newVersion, writtenAt, actualHex, req.Content); err != nil {
			return nil, apperr.Internal(err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE blocks SET version=?,written_at=?,digest=?,content=?,is_zero=0 WHERE volume_id=? AND block_no=?`,
			newVersion, writtenAt, actualHex, req.Content, req.VolumeID, req.BlockNo); err != nil {
			return nil, apperr.Internal(err)
		}
		if _, err := s.nextSeqTx(ctx, tx, req.VolumeID); err != nil {
			return nil, err
		}
		if err := s.recomputeAllTx(ctx, tx, req.VolumeID); err != nil {
			return nil, err
		}
		return WriteBlockResult{
			RequestID: req.RequestID, VolumeID: req.VolumeID,
			Block: BlockView{BlockNo: req.BlockNo, Version: newVersion, WrittenAt: writtenAt, Digest: actualHex, Zero: false, SizeBytes: vol.BlockSize},
		}, nil
	})
}

func (s *Store) GetBlock(ctx context.Context, volumeID string, blockNo int64) (BlockView, []byte, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return BlockView{}, nil, apperr.Internal(err)
	}
	defer tx.Rollback()
	vol, err := scanVolume(tx.QueryRowContext(ctx, `SELECT id,block_count,block_size,overhead_bytes,last_seq,created_at FROM volumes WHERE id=?`, volumeID))
	if errors.Is(err, sql.ErrNoRows) {
		return BlockView{}, nil, apperr.WithContext(apperr.New(apperr.CodeVolumeNotFound, "volume not found", 404), "volume_id", volumeID)
	}
	if err != nil {
		return BlockView{}, nil, apperr.Internal(err)
	}
	if blockNo < 0 || blockNo >= vol.BlockCount {
		return BlockView{}, nil, apperr.Invalid("block_no is outside the volume")
	}
	var view BlockView
	var content []byte
	var isZero int
	err = tx.QueryRowContext(ctx, `SELECT block_no,version,written_at,digest,content,is_zero FROM blocks WHERE volume_id=? AND block_no=?`, volumeID, blockNo).
		Scan(&view.BlockNo, &view.Version, &view.WrittenAt, &view.Digest, &content, &isZero)
	if err != nil {
		return BlockView{}, nil, apperr.Internal(err)
	}
	view.Zero = isZero == 1
	view.SizeBytes = vol.BlockSize
	if content == nil {
		content = make([]byte, vol.BlockSize)
	}
	return view, content, nil
}

func (s *Store) CreateSnapshot(ctx context.Context, req CreateSnapshotRequest) (MutationResult, error) {
	payload := struct {
		SnapshotID string `json:"snapshot_id"`
		Kind       string `json:"kind"`
		ParentID   string `json:"parent_id"`
		Weight     int64  `json:"weight"`
		CreatedAt  string `json:"created_at"`
	}{req.SnapshotID, req.Kind, req.ParentID, req.Weight, req.CreatedAt}
	if req.RequestHash == "" {
		req.RequestHash = RequestHash("create_snapshot", req.VolumeID, payload)
	}
	return s.executeMutation(ctx, req.VolumeID, "create_snapshot", req.RequestID, req.RequestHash, func(ctx context.Context, tx *sql.Tx, _ string) (any, error) {
		if strings.TrimSpace(req.SnapshotID) == "" {
			return nil, apperr.Invalid("snapshot_id is required")
		}
		if req.Weight < 0 {
			return nil, apperr.Invalid("weight must be non-negative")
		}
		if req.Kind != "full" && req.Kind != "incremental" {
			return nil, apperr.Invalid("kind must be full or incremental")
		}
		state, vol, err := s.loadStateTx(ctx, tx, req.VolumeID)
		if err != nil {
			return nil, err
		}
		if _, exists := state.Snapshots[req.SnapshotID]; exists {
			return nil, apperr.WithContext(apperr.New(apperr.CodeSnapshotExists, "snapshot already exists", 409), "snapshot_id", req.SnapshotID)
		}
		var refs map[int64]int64
		if req.Kind == "full" {
			if req.ParentID != "" {
				return nil, apperr.WithContext(apperr.New(apperr.CodeSnapshotParent, "full snapshot must not have a parent", 400), "snapshot_id", req.SnapshotID)
			}
			refs = make(map[int64]int64, len(state.Live))
			for no, live := range state.Live {
				refs[no] = live.Version
			}
		} else {
			if req.ParentID == "" {
				return nil, apperr.WithContext(apperr.New(apperr.CodeSnapshotParent, "incremental snapshot requires parent_id", 400), "snapshot_id", req.SnapshotID)
			}
			parentRefs, err := metrics.EffectiveRefs(state, req.ParentID)
			if err != nil {
				if _, ok := state.Snapshots[req.ParentID]; !ok {
					return nil, apperr.WithContext(apperr.New(apperr.CodeSnapshotParent, "parent snapshot not found", 400), "parent_id", req.ParentID)
				}
				return nil, apperr.WithContext(apperr.New(apperr.CodeSnapshotParent, "parent snapshot cannot be resolved", 400), "detail", err.Error())
			}
			refs = make(map[int64]int64)
			for no, live := range state.Live {
				if parentRefs[no] != live.Version {
					refs[no] = live.Version
				}
			}
		}
		var maxOrdinal int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(ordinal),0) FROM snapshots WHERE volume_id=?`, req.VolumeID).Scan(&maxOrdinal); err != nil {
			return nil, apperr.Internal(err)
		}
		if maxOrdinal == math.MaxInt64 {
			return nil, apperr.Internal(errors.New("snapshot ordinal exhausted"))
		}
		createdAt, err := normalizeTimestamp(req.CreatedAt, s.now())
		if err != nil {
			return nil, apperr.Invalid(err.Error())
		}
		var parent any
		if req.ParentID != "" {
			parent = req.ParentID
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO snapshots(volume_id,id,kind,parent_id,consistency_seq,weight,ordinal,created_at) VALUES(?,?,?,?,?,?,?,?)`,
			req.VolumeID, req.SnapshotID, req.Kind, parent, vol.LastSeq, req.Weight, maxOrdinal+1, createdAt); err != nil {
			return nil, apperr.Internal(err)
		}
		sortedBlocks := make([]int64, 0, len(refs))
		for no := range refs {
			sortedBlocks = append(sortedBlocks, no)
		}
		sort.Slice(sortedBlocks, func(i, j int) bool { return sortedBlocks[i] < sortedBlocks[j] })
		stmt, err := tx.PrepareContext(ctx, `INSERT INTO snapshot_refs(volume_id,snapshot_id,block_no,block_version) VALUES(?,?,?,?)`)
		if err != nil {
			return nil, apperr.Internal(err)
		}
		defer stmt.Close()
		for _, no := range sortedBlocks {
			if _, err := stmt.ExecContext(ctx, req.VolumeID, req.SnapshotID, no, refs[no]); err != nil {
				return nil, apperr.Internal(err)
			}
		}
		if _, err := s.nextSeqTx(ctx, tx, req.VolumeID); err != nil {
			return nil, err
		}
		if err := s.recomputeAllTx(ctx, tx, req.VolumeID); err != nil {
			return nil, err
		}
		state, _, err = s.loadStateTx(ctx, tx, req.VolumeID)
		if err != nil {
			return nil, err
		}
		view, err := s.snapshotView(state, req.SnapshotID)
		if err != nil {
			return nil, apperr.Internal(err)
		}
		return CreateSnapshotResult{RequestID: req.RequestID, Snapshot: view}, nil
	})
}

func (s *Store) MergeSnapshots(ctx context.Context, req MergeRequest) (MutationResult, error) {
	payload := struct {
		ParentID string `json:"parent_id"`
		ChildID  string `json:"child_id"`
	}{req.ParentID, req.ChildID}
	if req.RequestHash == "" {
		req.RequestHash = RequestHash("merge", req.VolumeID, payload)
	}
	return s.executeMutation(ctx, req.VolumeID, "merge", req.RequestID, req.RequestHash, func(ctx context.Context, tx *sql.Tx, _ string) (any, error) {
		state, _, err := s.loadStateTx(ctx, tx, req.VolumeID)
		if err != nil {
			return nil, err
		}
		parent, ok := state.Snapshots[req.ParentID]
		if !ok {
			return nil, apperr.WithContext(apperr.New(apperr.CodeSnapshotNotFound, "parent snapshot not found", 404), "parent_id", req.ParentID)
		}
		child, ok := state.Snapshots[req.ChildID]
		if !ok {
			return nil, apperr.WithContext(apperr.New(apperr.CodeSnapshotNotFound, "child snapshot not found", 404), "child_id", req.ChildID)
		}
		if child.ParentID != parent.ID {
			return nil, apperr.WithContext(apperr.New(apperr.CodeSnapshotParent, "child is not directly parented by requested parent", 400), "child_id", req.ChildID)
		}
		childEffective, err := metrics.EffectiveRefs(state, child.ID)
		if err != nil {
			return nil, apperr.WithContext(apperr.New(apperr.CodeMetricStale, "child snapshot cannot be resolved", 500), "detail", err.Error())
		}
		newParentRefs := make(map[int64]int64)
		if parent.ParentID == "" {
			for no, ver := range childEffective {
				newParentRefs[no] = ver
			}
		} else {
			grandRefs, err := metrics.EffectiveRefs(state, parent.ParentID)
			if err != nil {
				return nil, apperr.WithContext(apperr.New(apperr.CodeMetricStale, "parent predecessor cannot be resolved", 500), "detail", err.Error())
			}
			for no, ver := range childEffective {
				if grandRefs[no] != ver {
					newParentRefs[no] = ver
				}
			}
		}
		if parent.Weight > math.MaxInt64-child.Weight {
			return nil, apperr.Invalid("merged snapshot weight overflows int64")
		}
		newWeight := parent.Weight + child.Weight
		newSeq := parent.ConsistencySeq
		if child.ConsistencySeq > newSeq {
			newSeq = child.ConsistencySeq
		}
		newKind := "merged"
		if parent.ParentID == "" {
			newKind = "full"
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM snapshot_refs WHERE volume_id=? AND snapshot_id=?`, req.VolumeID, parent.ID); err != nil {
			return nil, apperr.Internal(err)
		}
		nos := make([]int64, 0, len(newParentRefs))
		for no := range newParentRefs {
			nos = append(nos, no)
		}
		sort.Slice(nos, func(i, j int) bool { return nos[i] < nos[j] })
		for _, no := range nos {
			if _, err := tx.ExecContext(ctx, `INSERT INTO snapshot_refs(volume_id,snapshot_id,block_no,block_version) VALUES(?,?,?,?)`,
				req.VolumeID, parent.ID, no, newParentRefs[no]); err != nil {
				return nil, apperr.Internal(err)
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE snapshots SET weight=?,consistency_seq=?,kind=? WHERE volume_id=? AND id=?`,
			newWeight, newSeq, newKind, req.VolumeID, parent.ID); err != nil {
			return nil, apperr.Internal(err)
		}
		res, err := tx.ExecContext(ctx, `UPDATE snapshots SET parent_id=? WHERE volume_id=? AND parent_id=?`,
			parent.ID, req.VolumeID, child.ID)
		if err != nil {
			return nil, apperr.Internal(err)
		}
		reparented, _ := res.RowsAffected()
		if _, err := tx.ExecContext(ctx, `DELETE FROM snapshots WHERE volume_id=? AND id=?`, req.VolumeID, child.ID); err != nil {
			return nil, apperr.Internal(err)
		}
		if _, err := s.nextSeqTx(ctx, tx, req.VolumeID); err != nil {
			return nil, err
		}
		if err := s.recomputeAllTx(ctx, tx, req.VolumeID); err != nil {
			return nil, err
		}
		state, _, err = s.loadStateTx(ctx, tx, req.VolumeID)
		if err != nil {
			return nil, err
		}
		view, err := s.snapshotView(state, parent.ID)
		if err != nil {
			return nil, apperr.Internal(err)
		}
		return MergeResult{
			RequestID: req.RequestID, MergedInto: parent.ID, Removed: child.ID,
			ParentWeight: newWeight, RewrittenRefs: int64(len(newParentRefs)),
			ReparentedChildren: reparented, Snapshot: view,
		}, nil
	})
}

func (s *Store) DeleteSnapshot(ctx context.Context, req DeleteSnapshotRequest) (MutationResult, error) {
	if req.RequestHash == "" {
		req.RequestHash = RequestHash("delete_snapshot", req.VolumeID, map[string]string{"snapshot_id": req.SnapshotID})
	}
	return s.executeMutation(ctx, req.VolumeID, "delete_snapshot", req.RequestID, req.RequestHash, func(ctx context.Context, tx *sql.Tx, _ string) (any, error) {
		state, _, err := s.loadStateTx(ctx, tx, req.VolumeID)
		if err != nil {
			return nil, err
		}
		if _, ok := state.Snapshots[req.SnapshotID]; !ok {
			return nil, apperr.WithContext(apperr.New(apperr.CodeSnapshotNotFound, "snapshot not found", 404), "snapshot_id", req.SnapshotID)
		}
		var children int64
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshots WHERE volume_id=? AND parent_id=?`, req.VolumeID, req.SnapshotID).Scan(&children); err != nil {
			return nil, apperr.Internal(err)
		}
		if children > 0 {
			return nil, apperr.WithContext(apperr.New(apperr.CodeSnapshotReferenced, "snapshot is still referenced by an unmerged child", 409), "snapshot_id", req.SnapshotID).
				WithMore("child_count", children)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM snapshots WHERE volume_id=? AND id=?`, req.VolumeID, req.SnapshotID); err != nil {
			return nil, apperr.Internal(err)
		}
		if _, err := s.nextSeqTx(ctx, tx, req.VolumeID); err != nil {
			return nil, err
		}
		if err := s.recomputeAllTx(ctx, tx, req.VolumeID); err != nil {
			return nil, err
		}
		return DeleteSnapshotResult{RequestID: req.RequestID, Deleted: req.SnapshotID}, nil
	})
}
