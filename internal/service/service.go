package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gatewayfreshness.local/workbench/internal/config"
	"gatewayfreshness.local/workbench/internal/freshness"
	"gatewayfreshness.local/workbench/internal/store"
)

type Service struct {
	store *store.Store
	cfg   *config.Config
	now   func() time.Time
}

func New(st *store.Store, cfg *config.Config) *Service {
	return &Service{store: st, cfg: cfg, now: time.Now}
}

func (s *Service) Store() *store.Store          { return s.store }
func (s *Service) Config() freshness.ConfigView { return s.cfg.View() }

func (s *Service) Adjudicate(ctx context.Context, in freshness.BatchInput) (*freshness.BatchDecision, error) {
	gatewayNow := s.now().UnixMilli()
	batch, appErr := freshness.ValidateBatch(in, s.cfg.ValidationPolicy(), gatewayNow)
	if appErr != nil {
		return nil, appErr
	}
	decision, _, err := s.store.Adjudicate(ctx, batch)
	if err != nil {
		return nil, s.mapError(err, in.BatchID)
	}
	return decision, nil
}

func (s *Service) GetDecision(ctx context.Context, batchID string) (*freshness.BatchDecision, error) {
	if batchID == "" {
		return nil, freshness.NewAppError(freshness.CodeRequestInvalid, "batch id is required", nil)
	}
	d, err := s.store.GetDecision(ctx, batchID)
	if err != nil {
		return nil, s.mapError(err, batchID)
	}
	return d, nil
}

func (s *Service) Snapshot(ctx context.Context, version int64, budgetMS int, include []string, clientID, batchID string, trailLimit int) (*freshness.DiagnosticSnapshot, error) {
	budgetMS, err := s.validateBudget(budgetMS)
	if err != nil {
		return nil, err
	}
	return s.store.Snapshot(ctx, store.SnapshotOptions{
		Version: version, Budget: time.Duration(budgetMS) * time.Millisecond,
		Include: include, ClientID: clientID, BatchID: batchID, TrailLimit: trailLimit,
	})
}

func (s *Service) ModeDifferences(ctx context.Context, batchID string, budgetMS int) (*freshness.ModeDifferenceReport, error) {
	budgetMS, err := s.validateBudget(budgetMS)
	if err != nil {
		return nil, err
	}
	report, err := s.store.ModeDifferences(ctx, batchID, time.Duration(budgetMS)*time.Millisecond)
	if err != nil {
		return nil, s.mapError(err, batchID)
	}
	return report, nil
}

func (s *Service) Inconsistencies(ctx context.Context, batchID string, views []freshness.ClientView, budgetMS int) (*freshness.InconsistencyReport, error) {
	budgetMS, err := s.validateBudget(budgetMS)
	if err != nil {
		return nil, err
	}
	report, err := s.store.Inconsistencies(ctx, batchID, views, time.Duration(budgetMS)*time.Millisecond)
	if err != nil {
		return nil, s.mapError(err, batchID)
	}
	return report, nil
}

func (s *Service) Recovery(ctx context.Context) (*freshness.RecoveryReport, error) {
	report, err := s.store.Recover(ctx)
	if err != nil {
		return nil, freshness.NewAppError(freshness.CodeRecoveryUnavailable, "recovery scan failed", err.Error())
	}
	return report, nil
}

func (s *Service) RecoveryHistory(ctx context.Context, limit int) ([]freshness.RecoveryReport, error) {
	return s.store.RecoveryHistory(ctx, limit)
}

func (s *Service) validateBudget(budgetMS int) (int, error) {
	if budgetMS < 0 {
		return 0, freshness.NewAppError(freshness.CodeRequestInvalid, "budget_ms must be non-negative", nil)
	}
	if budgetMS > s.cfg.MaxDiagnosticBudgetMS {
		return 0, freshness.NewAppError(freshness.CodeRequestInvalid, "budget_ms exceeds server maximum", map[string]any{
			"max_budget_ms": s.cfg.MaxDiagnosticBudgetMS,
		})
	}
	return budgetMS, nil
}

func (s *Service) mapError(err error, batchID string) error {
	if err == nil {
		return nil
	}
	var appErr *freshness.AppError
	if errors.As(err, &appErr) {
		return appErr
	}
	if errors.Is(err, store.ErrBatchNotFound) {
		return freshness.NewAppError(freshness.CodeBatchNotFound, "batch decision not found", map[string]any{"batch_id": batchID})
	}
	return freshness.NewAppError(freshness.CodeInternal, "internal store error", fmt.Sprintf("%v", err))
}
