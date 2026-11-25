package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"profilehotspot/internal/engine"
)

var ErrCommitSeqMismatch = errors.New("commit sequence does not match the latest committed state for this policy version")

func LoadReservoirsTx(ctx context.Context, tx *sql.Tx, version uint64, service, channel string) ([]engine.Reservoir, error) {
	return loadReservoirsTx(ctx, tx, version, service, channel)
}

type HotspotSnapshot struct {
	PolicyVersion        uint64               `json:"policy_version"`
	CommitSeq            int64                `json:"commit_seq"`
	CurrentPolicyVersion uint64               `json:"current_policy_version"`
	CurrentCommitSeq     int64                `json:"current_commit_seq"`
	IsCurrent            bool                 `json:"is_current"`
	Report               engine.HotspotReport `json:"report"`
}

func (s *Store) HotspotSnapshot(ctx context.Context, version uint64, requestedCommit *int64) (HotspotSnapshot, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return HotspotSnapshot{}, err
	}
	defer tx.Rollback()
	state, err := currentStateTx(ctx, tx)
	if err != nil {
		return HotspotSnapshot{}, err
	}
	report, err := loadHotspotTx(ctx, tx, version)
	if err != nil {
		return HotspotSnapshot{}, err
	}
	if requestedCommit != nil && report.CommitSeq != *requestedCommit {
		return HotspotSnapshot{}, fmt.Errorf("%w: requested=%d available=%d", ErrCommitSeqMismatch, *requestedCommit, report.CommitSeq)
	}
	return HotspotSnapshot{
		PolicyVersion:        version,
		CommitSeq:            report.CommitSeq,
		CurrentPolicyVersion: state.CurrentPolicyVersion,
		CurrentCommitSeq:     state.CommitSeq,
		IsCurrent:            state.CurrentPolicyVersion == version,
		Report:               report,
	}, nil
}

type ReservoirSnapshot struct {
	PolicyVersion        uint64             `json:"policy_version"`
	CommitSeq            int64              `json:"commit_seq"`
	CurrentPolicyVersion uint64             `json:"current_policy_version"`
	CurrentCommitSeq     int64              `json:"current_commit_seq"`
	IsCurrent            bool               `json:"is_current"`
	Reservoirs           []engine.Reservoir `json:"reservoirs"`
	Count                int                `json:"count"`
}

func (s *Store) ReservoirSnapshot(ctx context.Context, version uint64, requestedCommit *int64, service, channel string) (ReservoirSnapshot, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ReservoirSnapshot{}, err
	}
	defer tx.Rollback()
	state, err := currentStateTx(ctx, tx)
	if err != nil {
		return ReservoirSnapshot{}, err
	}
	report, err := loadHotspotTx(ctx, tx, version)
	if err != nil {
		return ReservoirSnapshot{}, err
	}
	if requestedCommit != nil && report.CommitSeq != *requestedCommit {
		return ReservoirSnapshot{}, fmt.Errorf("%w: requested=%d available=%d", ErrCommitSeqMismatch, *requestedCommit, report.CommitSeq)
	}
	reservoirs, err := loadReservoirsTx(ctx, tx, version, service, channel)
	if err != nil {
		return ReservoirSnapshot{}, err
	}
	return ReservoirSnapshot{
		PolicyVersion:        version,
		CommitSeq:            report.CommitSeq,
		CurrentPolicyVersion: state.CurrentPolicyVersion,
		CurrentCommitSeq:     state.CommitSeq,
		IsCurrent:            state.CurrentPolicyVersion == version,
		Reservoirs:           reservoirs,
		Count:                len(reservoirs),
	}, nil
}

type VersionSnapshot struct {
	PolicyVersion        uint64               `json:"policy_version"`
	CommitSeq            int64                `json:"commit_seq"`
	CurrentPolicyVersion uint64               `json:"current_policy_version"`
	CurrentCommitSeq     int64                `json:"current_commit_seq"`
	IsCurrent            bool                 `json:"is_current"`
	Reservoirs           []engine.Reservoir   `json:"reservoirs"`
	ReservoirCount       int                  `json:"reservoir_count"`
	Hotspots             engine.HotspotReport `json:"hotspots"`
}

// VersionSnapshot is the strong-consistency read boundary: reservoirs and
// hotspot output are loaded from one SQLite read transaction and therefore can
// never represent different commit sequences.
func (s *Store) VersionSnapshot(ctx context.Context, version uint64, requestedCommit *int64, service, channel string, limit int) (VersionSnapshot, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return VersionSnapshot{}, err
	}
	defer tx.Rollback()
	state, err := currentStateTx(ctx, tx)
	if err != nil {
		return VersionSnapshot{}, err
	}
	report, err := loadHotspotTx(ctx, tx, version)
	if err != nil {
		return VersionSnapshot{}, err
	}
	if requestedCommit != nil && report.CommitSeq != *requestedCommit {
		return VersionSnapshot{}, fmt.Errorf("%w: requested=%d available=%d", ErrCommitSeqMismatch, *requestedCommit, report.CommitSeq)
	}
	reservoirs, err := loadReservoirsTx(ctx, tx, version, service, channel)
	if err != nil {
		return VersionSnapshot{}, err
	}
	if service != "" || limit > 0 {
		filtered := make([]engine.ServiceHotspot, 0, len(report.Services))
		for _, item := range report.Services {
			if service != "" && item.Service != service {
				continue
			}
			if limit > 0 && len(item.Frames) > limit {
				item.Frames = append([]engine.FrameHotspot(nil), item.Frames[:limit]...)
			}
			filtered = append(filtered, item)
		}
		report.Services = filtered
	}
	return VersionSnapshot{
		PolicyVersion:        version,
		CommitSeq:            report.CommitSeq,
		CurrentPolicyVersion: state.CurrentPolicyVersion,
		CurrentCommitSeq:     state.CommitSeq,
		IsCurrent:            state.CurrentPolicyVersion == version,
		Reservoirs:           reservoirs,
		ReservoirCount:       len(reservoirs),
		Hotspots:             report,
	}, nil
}
