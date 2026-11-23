package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"

	"blocksnap/internal/apperr"
	"blocksnap/internal/metrics"
)

func scanVolume(row interface{ Scan(...any) error }) (metricsVolume, error) {
	var v metricsVolume
	err := row.Scan(&v.ID, &v.BlockCount, &v.BlockSize, &v.OverheadBytes, &v.LastSeq, &v.CreatedAt)
	return v, err
}

type metricsVolume struct {
	ID            string
	BlockCount    int64
	BlockSize     int64
	OverheadBytes int64
	LastSeq       int64
	CreatedAt     string
}

func volumeCapacity(v metricsVolume) *big.Int {
	return new(big.Int).Mul(big.NewInt(v.BlockCount), big.NewInt(v.BlockSize))
}

func (s *Store) loadStateTx(ctx context.Context, tx *sql.Tx, volumeID string) (*metrics.State, metricsVolume, error) {
	vol, err := scanVolume(tx.QueryRowContext(ctx, `SELECT id,block_count,block_size,overhead_bytes,last_seq,created_at FROM volumes WHERE id=?`, volumeID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, metricsVolume{}, apperr.WithContext(apperr.New(apperr.CodeVolumeNotFound, "volume not found", 404), "volume_id", volumeID)
	}
	if err != nil {
		return nil, metricsVolume{}, apperr.Internal(err)
	}
	state := &metrics.State{
		VolumeID: vol.ID, BlockCount: vol.BlockCount, BlockSize: vol.BlockSize,
		OverheadBytes: vol.OverheadBytes, LastSeq: vol.LastSeq,
		Live: map[int64]metrics.BlockRef{}, Snapshots: map[string]metrics.Snapshot{},
		Refs: map[string]map[int64]int64{}, Versions: map[metrics.VersionKey]string{},
	}
	rows, err := tx.QueryContext(ctx, `SELECT block_no,version,written_at,digest FROM blocks WHERE volume_id=? ORDER BY block_no`, volumeID)
	if err != nil {
		return nil, metricsVolume{}, apperr.Internal(err)
	}
	for rows.Next() {
		var b metrics.BlockRef
		var writtenAt string
		if err := rows.Scan(&b.BlockNo, &b.Version, &writtenAt, &b.Digest); err != nil {
			rows.Close()
			return nil, metricsVolume{}, apperr.Internal(err)
		}
		state.Live[b.BlockNo] = b
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, metricsVolume{}, apperr.Internal(err)
	}
	rows.Close()
	if int64(len(state.Live)) != state.BlockCount {
		return nil, metricsVolume{}, apperr.WithContext(apperr.New(apperr.CodeInternal, "volume block table is incomplete", 500), "volume_id", volumeID)
	}

	rows, err = tx.QueryContext(ctx, `SELECT block_no,version,digest FROM block_versions WHERE volume_id=? ORDER BY block_no,version`, volumeID)
	if err != nil {
		return nil, metricsVolume{}, apperr.Internal(err)
	}
	for rows.Next() {
		var key metrics.VersionKey
		var digest string
		if err := rows.Scan(&key.BlockNo, &key.Version, &digest); err != nil {
			rows.Close()
			return nil, metricsVolume{}, apperr.Internal(err)
		}
		state.Versions[key] = digest
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, metricsVolume{}, apperr.Internal(err)
	}
	rows.Close()

	rows, err = tx.QueryContext(ctx, `SELECT id,kind,COALESCE(parent_id,''),consistency_seq,weight,ordinal,created_at FROM snapshots WHERE volume_id=? ORDER BY ordinal,id`, volumeID)
	if err != nil {
		return nil, metricsVolume{}, apperr.Internal(err)
	}
	for rows.Next() {
		var snap metrics.Snapshot
		if err := rows.Scan(&snap.ID, &snap.Kind, &snap.ParentID, &snap.ConsistencySeq, &snap.Weight, &snap.Ordinal, &snap.CreatedAt); err != nil {
			rows.Close()
			return nil, metricsVolume{}, apperr.Internal(err)
		}
		state.Snapshots[snap.ID] = snap
		state.Refs[snap.ID] = map[int64]int64{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, metricsVolume{}, apperr.Internal(err)
	}
	rows.Close()

	rows, err = tx.QueryContext(ctx, `SELECT snapshot_id,block_no,block_version FROM snapshot_refs WHERE volume_id=? ORDER BY snapshot_id,block_no`, volumeID)
	if err != nil {
		return nil, metricsVolume{}, apperr.Internal(err)
	}
	for rows.Next() {
		var snapshotID string
		var no, ver int64
		if err := rows.Scan(&snapshotID, &no, &ver); err != nil {
			rows.Close()
			return nil, metricsVolume{}, apperr.Internal(err)
		}
		if _, ok := state.Refs[snapshotID]; !ok {
			rows.Close()
			return nil, metricsVolume{}, apperr.WithContext(apperr.New(apperr.CodeSnapshotCorrupt, "snapshot reference has no snapshot row", 500), "snapshot_id", snapshotID)
		}
		state.Refs[snapshotID][no] = ver
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, metricsVolume{}, apperr.Internal(err)
	}
	rows.Close()
	return state, vol, nil
}

func (s *Store) recomputeAllTx(ctx context.Context, tx *sql.Tx, volumeID string) error {
	state, vol, err := s.loadStateTx(ctx, tx, volumeID)
	if err != nil {
		return err
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
	for _, id := range ids {
		analysis, err := metrics.Analyze(state, id)
		if err != nil {
			return apperr.WithContext(apperr.New(apperr.CodeMetricStale, "derived metrics are not computable from committed state", 500), "detail", err.Error())
		}
		raw, err := json.Marshal(analysis.Metrics)
		if err != nil {
			return apperr.Internal(err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO snapshot_metrics(volume_id,snapshot_id,computed_seq,metrics_json) VALUES(?,?,?,?)
			ON CONFLICT(volume_id,snapshot_id) DO UPDATE SET computed_seq=excluded.computed_seq,metrics_json=excluded.metrics_json`,
			volumeID, id, vol.LastSeq, string(raw)); err != nil {
			return apperr.Internal(err)
		}
	}
	return nil
}

func (s *Store) analysisTx(ctx context.Context, tx *sql.Tx, volumeID, snapshotID string, verifyStored bool) (metrics.Analysis, error) {
	state, _, err := s.loadStateTx(ctx, tx, volumeID)
	if err != nil {
		return metrics.Analysis{}, err
	}
	if _, ok := state.Snapshots[snapshotID]; !ok {
		return metrics.Analysis{}, apperr.WithContext(apperr.New(apperr.CodeSnapshotNotFound, "snapshot not found", 404), "snapshot_id", snapshotID)
	}
	analysis, err := metrics.Analyze(state, snapshotID)
	if err != nil {
		return metrics.Analysis{}, apperr.WithContext(apperr.New(apperr.CodeMetricStale, "metrics cannot be derived from committed state", 500), "detail", err.Error())
	}
	if verifyStored {
		var computedSeq int64
		var stored string
		err := tx.QueryRowContext(ctx, `SELECT computed_seq,metrics_json FROM snapshot_metrics WHERE volume_id=? AND snapshot_id=?`, volumeID, snapshotID).Scan(&computedSeq, &stored)
		if errors.Is(err, sql.ErrNoRows) {
			return metrics.Analysis{}, apperr.WithContext(apperr.New(apperr.CodeMetricStale, "snapshot metrics are missing", 500), "snapshot_id", snapshotID)
		}
		if err != nil {
			return metrics.Analysis{}, apperr.Internal(err)
		}
		fresh, _ := json.Marshal(analysis.Metrics)
		if computedSeq != state.LastSeq || string(fresh) != stored {
			return metrics.Analysis{}, apperr.WithContext(apperr.New(apperr.CodeMetricStale, "snapshot metrics differ from committed state", 500), "snapshot_id", snapshotID)
		}
	}
	return analysis, nil
}

func (s *Store) nextSeqTx(ctx context.Context, tx *sql.Tx, volumeID string) (int64, error) {
	if _, err := tx.ExecContext(ctx, `UPDATE volumes SET last_seq=last_seq+1 WHERE id=?`, volumeID); err != nil {
		return 0, apperr.Internal(err)
	}
	var seq int64
	if err := tx.QueryRowContext(ctx, `SELECT last_seq FROM volumes WHERE id=?`, volumeID).Scan(&seq); err != nil {
		return 0, apperr.Internal(err)
	}
	return seq, nil
}

func (s *Store) volumeViewTx(ctx context.Context, tx *sql.Tx, volumeID string) (Volume, error) {
	vol, err := scanVolume(tx.QueryRowContext(ctx, `SELECT id,block_count,block_size,overhead_bytes,last_seq,created_at FROM volumes WHERE id=?`, volumeID))
	if errors.Is(err, sql.ErrNoRows) {
		return Volume{}, apperr.WithContext(apperr.New(apperr.CodeVolumeNotFound, "volume not found", 404), "volume_id", volumeID)
	}
	if err != nil {
		return Volume{}, apperr.Internal(err)
	}
	return Volume{
		ID: vol.ID, BlockCount: vol.BlockCount, BlockSize: vol.BlockSize,
		OverheadBytes: vol.OverheadBytes, LastSeq: vol.LastSeq,
		CapacityBytes: volumeCapacity(vol).String(), CreatedAt: vol.CreatedAt,
	}, nil
}

func (s *Store) snapshotView(state *metrics.State, id string) (SnapshotView, error) {
	snap, ok := state.Snapshots[id]
	if !ok {
		return SnapshotView{}, fmt.Errorf("snapshot %s missing", id)
	}
	effective, err := metrics.EffectiveRefs(state, id)
	if err != nil {
		return SnapshotView{}, err
	}
	return SnapshotView{
		ID: snap.ID, Kind: snap.Kind, ParentID: snap.ParentID,
		ConsistencySeq: snap.ConsistencySeq, Weight: snap.Weight,
		Ordinal: snap.Ordinal, CreatedAt: snap.CreatedAt,
		DeltaBlocks: int64(len(state.Refs[id])), EffectiveBlocks: int64(len(effective)),
	}, nil
}
