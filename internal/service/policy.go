package service

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strconv"
	"time"

	"profilehotspot/internal/apperr"
	"profilehotspot/internal/engine"
	"profilehotspot/internal/model"
	"profilehotspot/internal/storage"
)

type RebuildReport struct {
	PolicyVersion          uint64              `json:"policy_version"`
	OldVersion             uint64              `json:"old_version"`
	CommitSeq              int64               `json:"commit_seq"`
	Operation              string              `json:"operation"`
	Changes                model.PolicyChanges `json:"changes"`
	Atomic                 bool                `json:"atomic"`
	CascadeScope           string              `json:"cascade_scope"`
	ReferencesUpdated      bool                `json:"references_updated"`
	SupersededReservoirIDs []string            `json:"superseded_reservoir_ids"`
	RebuiltReservoirCount  int                 `json:"rebuilt_reservoir_count"`
	RebuiltHotspot         bool                `json:"rebuilt_hotspot"`
	SampleCount            int                 `json:"sample_count"`
	ElapsedMS              int64               `json:"elapsed_ms"`
	RoundTimeBudgetMS      int64               `json:"round_time_budget_ms"`
	WithinBudget           bool                `json:"within_budget"`
}

type PolicyCommitResponse struct {
	PolicyVersion uint64             `json:"policy_version"`
	OldVersion    uint64             `json:"old_version"`
	CommitSeq     int64              `json:"commit_seq"`
	Operation     string             `json:"operation"`
	Rebuild       RebuildReport      `json:"rebuild"`
	Modes         engine.ModeSummary `json:"modes"`
}

func (s *Service) CommitPolicy(ctx context.Context, incoming model.Policy) (*PolicyCommitResponse, error) {
	policy := incoming.Clone()
	if err := policy.Normalize(); err != nil {
		return nil, validationAppError(err)
	}
	var response *PolicyCommitResponse
	err := s.store.WithWrite(ctx, storage.IntentSpec{
		RequestKey:  "policy:" + strconv.FormatUint(policy.Version, 10),
		Kind:        "policy_snapshot",
		Target:      strconv.FormatUint(policy.Version, 10),
		PayloadHash: payloadHash(policy),
		Context:     map[string]any{"requested_version": policy.Version},
	}, func(tx *sql.Tx) error {
		state, err := storage.ReadStateTx(ctx, tx)
		if err != nil {
			return err
		}
		if policy.Version <= state.CurrentPolicyVersion {
			return apperr.StaleSnapshot(map[string]any{
				"requested_version": policy.Version,
				"current_version":   state.CurrentPolicyVersion,
			})
		}
		oldVersion := state.CurrentPolicyVersion
		var oldPolicy model.Policy
		if oldVersion > 0 {
			oldPolicy, err = storage.ReadPolicyTx(ctx, tx, oldVersion)
			if err != nil {
				return err
			}
		}
		changes := model.DiffPolicies(oldPolicy, policy)
		response, err = s.materializePolicyState(ctx, tx, state, oldPolicy, policy, changes, "full_replace")
		return err
	})
	if err != nil {
		return nil, apperr.From(err)
	}
	return response, nil
}

func (s *Service) CommitIncrement(ctx context.Context, increment model.PolicyIncrement) (*PolicyCommitResponse, error) {
	var response *PolicyCommitResponse
	targetVersion := increment.BaseVersion + 1
	err := s.store.WithWrite(ctx, storage.IntentSpec{
		RequestKey:  "increment:" + strconv.FormatUint(increment.BaseVersion, 10),
		Kind:        "policy_increment",
		Target:      strconv.FormatUint(targetVersion, 10),
		PayloadHash: payloadHash(increment),
		Context:     map[string]any{"base_version": increment.BaseVersion, "target_version": targetVersion},
	}, func(tx *sql.Tx) error {
		state, err := storage.ReadStateTx(ctx, tx)
		if err != nil {
			return err
		}
		if state.CurrentPolicyVersion == 0 || increment.BaseVersion != state.CurrentPolicyVersion {
			return apperr.StaleBase(map[string]any{
				"base_version":    increment.BaseVersion,
				"current_version": state.CurrentPolicyVersion,
			})
		}
		basePolicy, err := storage.ReadPolicyTx(ctx, tx, increment.BaseVersion)
		if err != nil {
			return err
		}
		next, err := increment.Merge(basePolicy, s.now())
		if err != nil {
			var mismatch *model.VersionMismatchError
			if errors.As(err, &mismatch) {
				return apperr.ConflictingRequest(mismatch.Error(), map[string]any{
					"base_version":     increment.BaseVersion,
					"expected_version": mismatch.Expected,
					"actual_version":   mismatch.Actual,
				})
			}
			return validationAppError(err)
		}
		changes := model.DiffPolicies(basePolicy, next)
		response, err = s.materializePolicyState(ctx, tx, state, basePolicy, next, changes, "incremental_patch")
		return err
	})
	if err != nil {
		return nil, apperr.From(err)
	}
	return response, nil
}

func (s *Service) materializePolicyState(
	ctx context.Context,
	tx *sql.Tx,
	state storage.StateMeta,
	oldPolicy model.Policy,
	next model.Policy,
	changes model.PolicyChanges,
	operation string,
) (*PolicyCommitResponse, error) {
	commitSeq := state.CommitSeq + 1
	var oldReservoirs []engine.Reservoir
	var err error
	if state.CurrentPolicyVersion > 0 {
		oldReservoirs, err = storage.LoadReservoirsTx(ctx, tx, state.CurrentPolicyVersion, "", "")
		if err != nil {
			return nil, err
		}
	}
	samples, err := storage.LoadAllSamplesTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	started := time.Now()
	builtAt := s.now()
	reservoirs := engine.BuildReservoirs(next, samples)
	hotspots := engine.BuildHotspots(next, commitSeq, builtAt, samples, reservoirs)
	storage.MaybeCrash("during_hotspot_aggregation")
	elapsed := time.Since(started)
	hotspots.ElapsedMS = elapsed.Milliseconds()
	hotspots.WithinBudget = hotspots.ElapsedMS <= next.RoundTimeBudgetMS

	superseded := affectedReservoirIDs(oldReservoirs, changes)
	rebuild := RebuildReport{
		PolicyVersion:          next.Version,
		OldVersion:             oldPolicy.Version,
		CommitSeq:              commitSeq,
		Operation:              operation,
		Changes:                changes,
		Atomic:                 true,
		CascadeScope:           "versioned reservoirs, hotspot aggregation, and references replaced in one SQLite transaction",
		ReferencesUpdated:      true,
		SupersededReservoirIDs: superseded,
		RebuiltReservoirCount:  len(reservoirs),
		RebuiltHotspot:         true,
		SampleCount:            len(samples),
		ElapsedMS:              hotspots.ElapsedMS,
		RoundTimeBudgetMS:      next.RoundTimeBudgetMS,
		WithinBudget:           hotspots.WithinBudget,
	}

	if err := storage.ReplaceReservoirsTx(ctx, tx, next.Version, commitSeq, reservoirs); err != nil {
		return nil, err
	}
	storage.MaybeCrash("after_reservoir_replace")
	if err := storage.InsertHotspotTx(ctx, tx, hotspots); err != nil {
		return nil, err
	}
	storage.MaybeCrash("after_hotspot_insert")
	if err := storage.InsertPolicyTx(ctx, tx, next, commitSeq, rebuild); err != nil {
		return nil, err
	}
	if err := storage.InsertRebuildReportTx(ctx, tx, rebuild); err != nil {
		return nil, err
	}
	state.CurrentPolicyVersion = next.Version
	state.CommitSeq = commitSeq
	if err := storage.SetStateTx(ctx, tx, state); err != nil {
		return nil, err
	}
	return &PolicyCommitResponse{
		PolicyVersion: next.Version,
		OldVersion:    oldPolicy.Version,
		CommitSeq:     commitSeq,
		Operation:     operation,
		Rebuild:       rebuild,
		Modes:         hotspots.Modes,
	}, nil
}

func affectedReservoirIDs(reservoirs []engine.Reservoir, changes model.PolicyChanges) []string {
	global := changes.StratificationChanged || changes.RankingChanged || changes.GlobalDefaultsChanged || changes.ReplacementRuleChange || !changes.HasChanges()
	channels := make(map[string]struct{}, len(changes.ChangedChannels))
	for _, channel := range changes.ChangedChannels {
		channels[channel] = struct{}{}
	}
	services := make(map[string]struct{}, len(changes.ChangedServices))
	for _, service := range changes.ChangedServices {
		services[service] = struct{}{}
	}
	ids := make([]string, 0)
	for _, reservoir := range reservoirs {
		_, channelChanged := channels[reservoir.Channel]
		_, serviceChanged := services[reservoir.Service]
		if global || channelChanged || serviceChanged {
			ids = append(ids, reservoir.ID)
		}
	}
	sort.Strings(ids)
	return ids
}

func (s *Service) Policy(ctx context.Context, version uint64) (storage.PolicySnapshot, error) {
	snapshot, err := s.store.PolicySnapshot(ctx, version)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.PolicySnapshot{}, notFoundVersion(version)
	}
	if err != nil {
		return storage.PolicySnapshot{}, apperr.From(err)
	}
	return snapshot, nil
}

func (s *Service) RebuildReport(ctx context.Context, version uint64) (any, error) {
	report, err := s.store.RebuildReport(ctx, version)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, notFoundVersion(version)
	}
	if err != nil {
		return nil, apperr.From(err)
	}
	return report, nil
}

func (s *Service) CurrentState(ctx context.Context) (storage.StateMeta, error) {
	state, err := s.store.CurrentState(ctx)
	if err != nil {
		return storage.StateMeta{}, apperr.From(err)
	}
	return state, nil
}
