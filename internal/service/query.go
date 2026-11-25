package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"

	"profilehotspot/internal/apperr"
	"profilehotspot/internal/engine"
	"profilehotspot/internal/storage"
)

type ModeService struct {
	Service                string              `json:"service"`
	Path                   engine.PathDecision `json:"path"`
	SelectedSamples        int                 `json:"selected_samples"`
	PopulationSamples      int64               `json:"population_samples"`
	SelectedWeightFraction float64             `json:"selected_weight_fraction"`
	BiasUpperBound         float64             `json:"bias_upper_bound"`
}

type ModeReport struct {
	PolicyVersion uint64               `json:"policy_version"`
	CommitSeq     int64                `json:"commit_seq"`
	Modes         engine.ModeSummary   `json:"modes"`
	Ranking       engine.RankingPolicy `json:"ranking"`
	Services      []ModeService        `json:"services"`
}

type FrameDifference struct {
	FrameID               string  `json:"frame_id"`
	FrameName             string  `json:"frame_name"`
	ExactShare            float64 `json:"exact_share"`
	EstimatedShare        float64 `json:"estimated_share"`
	AbsoluteError         float64 `json:"absolute_error"`
	BiasUpperBound        float64 `json:"bias_upper_bound"`
	WithinBiasBound       bool    `json:"within_bias_bound"`
	ExactCount            int64   `json:"exact_count"`
	SelectedCount         int64   `json:"selected_count"`
	ExactWeightedCount    float64 `json:"exact_weighted_count"`
	SelectedWeightedCount float64 `json:"selected_weighted_count"`
}

type ServiceDifference struct {
	Service                string            `json:"service"`
	Mode                   string            `json:"mode"`
	ModeReason             string            `json:"mode_reason"`
	PopulationSamples      int64             `json:"population_samples"`
	ExactLimit             int               `json:"exact_limit"`
	SelectedSamples        int               `json:"selected_samples"`
	SelectedWeightFraction float64           `json:"selected_weight_fraction"`
	BiasUpperBound         float64           `json:"bias_upper_bound"`
	BiasBoundFormula       string            `json:"bias_bound_formula"`
	Frames                 []FrameDifference `json:"frames"`
}

type DifferenceReport struct {
	PolicyVersion uint64              `json:"policy_version"`
	CommitSeq     int64               `json:"commit_seq"`
	Modes         engine.ModeSummary  `json:"modes"`
	Services      []ServiceDifference `json:"services"`
}

func (s *Service) Hotspots(ctx context.Context, version uint64, requestedCommit *int64, serviceName string, limit int) (storage.HotspotSnapshot, error) {
	snapshot, err := s.store.HotspotSnapshot(ctx, version, requestedCommit)
	if err != nil {
		return storage.HotspotSnapshot{}, mapSnapshotError(err, version)
	}
	if serviceName == "" && limit <= 0 {
		return snapshot, nil
	}
	filtered := make([]engine.ServiceHotspot, 0, len(snapshot.Report.Services))
	for _, service := range snapshot.Report.Services {
		if serviceName != "" && service.Service != serviceName {
			continue
		}
		if limit > 0 && len(service.Frames) > limit {
			service.Frames = append([]engine.FrameHotspot(nil), service.Frames[:limit]...)
		}
		filtered = append(filtered, service)
	}
	snapshot.Report.Services = filtered
	return snapshot, nil
}

func (s *Service) Reservoirs(ctx context.Context, version uint64, requestedCommit *int64, serviceName, channel string) (storage.ReservoirSnapshot, error) {
	snapshot, err := s.store.ReservoirSnapshot(ctx, version, requestedCommit, serviceName, channel)
	if err != nil {
		return storage.ReservoirSnapshot{}, mapSnapshotError(err, version)
	}
	return snapshot, nil
}

func (s *Service) VersionSnapshot(ctx context.Context, version uint64, requestedCommit *int64, serviceName, channel string, limit int) (storage.VersionSnapshot, error) {
	snapshot, err := s.store.VersionSnapshot(ctx, version, requestedCommit, serviceName, channel, limit)
	if err != nil {
		return storage.VersionSnapshot{}, mapSnapshotError(err, version)
	}
	return snapshot, nil
}

func (s *Service) Modes(ctx context.Context, version uint64, requestedCommit *int64) (ModeReport, error) {
	snapshot, err := s.Hotspots(ctx, version, requestedCommit, "", 0)
	if err != nil {
		return ModeReport{}, err
	}
	report := ModeReport{
		PolicyVersion: snapshot.PolicyVersion,
		CommitSeq:     snapshot.CommitSeq,
		Modes:         snapshot.Report.Modes,
		Ranking:       snapshot.Report.Ranking,
		Services:      make([]ModeService, 0, len(snapshot.Report.Services)),
	}
	for _, service := range snapshot.Report.Services {
		report.Services = append(report.Services, ModeService{
			Service:                service.Service,
			Path:                   service.Path,
			SelectedSamples:        service.SelectedSamples,
			PopulationSamples:      service.PopulationSamples,
			SelectedWeightFraction: service.SelectedWeightFraction,
			BiasUpperBound:         service.BiasUpperBound,
		})
	}
	sort.Slice(report.Services, func(i, j int) bool { return report.Services[i].Service < report.Services[j].Service })
	return report, nil
}

func (s *Service) Differences(ctx context.Context, version uint64, requestedCommit *int64) (DifferenceReport, error) {
	snapshot, err := s.Hotspots(ctx, version, requestedCommit, "", 0)
	if err != nil {
		return DifferenceReport{}, err
	}
	report := DifferenceReport{
		PolicyVersion: snapshot.PolicyVersion,
		CommitSeq:     snapshot.CommitSeq,
		Modes:         snapshot.Report.Modes,
		Services:      make([]ServiceDifference, 0, len(snapshot.Report.Services)),
	}
	for _, service := range snapshot.Report.Services {
		difference := ServiceDifference{
			Service:                service.Service,
			Mode:                   service.Path.Mode,
			ModeReason:             service.Path.Reason,
			PopulationSamples:      service.PopulationSamples,
			ExactLimit:             service.Path.ExactLimit,
			SelectedSamples:        service.SelectedSamples,
			SelectedWeightFraction: service.SelectedWeightFraction,
			BiasUpperBound:         service.BiasUpperBound,
			BiasBoundFormula:       service.BiasBoundFormula,
			Frames:                 make([]FrameDifference, 0, len(service.Frames)),
		}
		for _, frame := range service.Frames {
			difference.Frames = append(difference.Frames, FrameDifference{
				FrameID:               frame.FrameID,
				FrameName:             frame.FrameName,
				ExactShare:            frame.ExactShare,
				EstimatedShare:        frame.EstimatedShare,
				AbsoluteError:         frame.AbsoluteError,
				BiasUpperBound:        frame.BiasUpperBound,
				WithinBiasBound:       frame.WithinBiasBound,
				ExactCount:            frame.ExactCount,
				SelectedCount:         frame.SelectedCount,
				ExactWeightedCount:    frame.ExactWeightedCount,
				SelectedWeightedCount: frame.SelectedWeightedCount,
			})
		}
		report.Services = append(report.Services, difference)
	}
	sort.Slice(report.Services, func(i, j int) bool { return report.Services[i].Service < report.Services[j].Service })
	return report, nil
}

func (s *Service) RecoveryReport(ctx context.Context) (storage.RecoveryReport, error) {
	report, err := s.store.LatestRecoveryReport(ctx)
	if err != nil {
		return storage.RecoveryReport{}, apperr.From(err)
	}
	return report, nil
}

type BatchStatus struct {
	BatchID       string          `json:"batch_id"`
	PolicyVersion uint64          `json:"policy_version"`
	CommitSeq     int64           `json:"commit_seq"`
	PayloadHash   string          `json:"payload_hash"`
	Response      json.RawMessage `json:"response"`
}

func (s *Service) Batch(ctx context.Context, batchID string) (BatchStatus, error) {
	batch, found, err := s.store.BatchByID(ctx, batchID)
	if err != nil {
		return BatchStatus{}, apperr.From(err)
	}
	if !found {
		return BatchStatus{}, apperr.NotFound("sample batch", map[string]any{"batch_id": batchID})
	}
	return BatchStatus{
		BatchID:       batch.BatchID,
		PolicyVersion: batch.PolicyVersion,
		CommitSeq:     batch.CommitSeq,
		PayloadHash:   batch.PayloadHash,
		Response:      batch.Response,
	}, nil
}

func mapSnapshotError(err error, version uint64) error {
	if errors.Is(err, storage.ErrCommitSeqMismatch) {
		return apperr.NotFound("committed snapshot", map[string]any{"policy_version": version})
	}
	if errors.Is(err, sql.ErrNoRows) {
		return notFoundVersion(version)
	}
	return apperr.From(err)
}
