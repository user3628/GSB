package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"

	"example.com/replicabudget/internal/apperr"
	"example.com/replicabudget/internal/policy"
)

type ImportRequest struct {
	BaseVersion     int
	Data            []byte
	FormatHint      string
	Mode            string
	ThresholdMicros *int64
	Tolerance       *policy.Tolerance
}

type ImportResult struct {
	SnapshotVersion int                   `json:"snapshot_version"`
	BaseVersion     int                   `json:"base_version"`
	Format          policy.Negotiation    `json:"format_negotiation"`
	Migration       *policy.MigrationPlan `json:"migration,omitempty"`
	PolicyCount     int                   `json:"policy_count"`
	Affected        []string              `json:"affected_services"`
	State           StateView             `json:"state"`
}

type IncrementalPatch struct {
	ServiceID              string  `json:"service_id"`
	MinReplicas            *int    `json:"min_replicas,omitempty"`
	MaxReplicas            *int    `json:"max_replicas,omitempty"`
	TargetConcurrency      *int64  `json:"target_concurrency,omitempty"`
	LatencyBudgetMS        *int    `json:"latency_budget_ms,omitempty"`
	QuotaGroupRef          *string `json:"quota_group_ref,omitempty"`
	WeightPermille         *int    `json:"weight_permille,omitempty"`
	VRAMBytesPerReplica    *uint64 `json:"vram_bytes_per_replica,omitempty"`
	ComputeMilliPerReplica *uint64 `json:"compute_milli_per_replica,omitempty"`
	ElasticEnabled         *bool   `json:"elastic_enabled,omitempty"`
	PlacementZone          *string `json:"placement_zone,omitempty"`
}

type IncrementalRequest struct {
	BaseVersion       int                `json:"base_version"`
	ThresholdMicros   *int64             `json:"threshold_micros,omitempty"`
	TolerancePermille *int               `json:"tolerance_permille,omitempty"`
	Patches           []IncrementalPatch `json:"patches"`
}

type MutationResult struct {
	SnapshotVersion int       `json:"snapshot_version"`
	BaseVersion     int       `json:"base_version"`
	Affected        []string  `json:"affected_services"`
	Invalidated     []string  `json:"invalidated_structures"`
	State           StateView `json:"state"`
}

func (s *Store) Import(ctx context.Context, req ImportRequest) (ImportResult, error) {
	if req.BaseVersion < 0 {
		return ImportResult{}, apperr.New(apperr.SpecInvalid, "base_version must be non-negative")
	}
	info, err := policy.DetectFormat(req.Data, req.FormatHint)
	if err != nil {
		return ImportResult{}, err
	}
	negotiation, err := policy.Negotiate(info)
	if err != nil {
		return ImportResult{}, err
	}
	mode := strings.ToLower(strings.TrimSpace(req.Mode))
	if mode == "" {
		mode = "replace"
	}
	if mode != "replace" && mode != "merge" {
		return ImportResult{}, apperr.WithDetails(apperr.SpecInvalid, "mode must be replace or merge", map[string]any{"mode": req.Mode})
	}
	var newStrategies []policy.NewStrategy
	var legacy []policy.LegacyStrategy
	if negotiation.RequiresMigration {
		legacy, err = policy.ParseLegacyJSON(req.Data)
		if err != nil {
			return ImportResult{}, err
		}
	} else {
		switch info.Format {
		case policy.FormatJSONL:
			newStrategies, _, err = policy.DecodeJSONL(req.Data)
		case policy.FormatBinary:
			newStrategies, _, err = policy.DecodeBinary(req.Data)
		default:
			return ImportResult{}, apperr.WithDetails(apperr.FormatUnsupported, "unsupported native format", map[string]any{"format": info.Format})
		}
		if err != nil {
			return ImportResult{}, err
		}
	}
	kind := "import_strategy"
	if negotiation.RequiresMigration {
		kind = "migrate_legacy"
	}
	intentID, batchID, err := s.createIntent(ctx, kind, req.BaseVersion, map[string]any{
		"format": info.Format, "version": info.Version, "mode": mode,
		"payload_sha256": sha256Hex(req.Data), "record_count": max(len(newStrategies), len(legacy)),
	}, negotiation.RequiresMigration)
	if err != nil {
		return ImportResult{}, err
	}
	if err := s.callFailpoint("after_intent"); err != nil {
		s.abortIntent(ctx, intentID, batchID, apperr.Internal, err.Error(), map[string]any{"stage": "after_intent"})
		return ImportResult{}, err
	}
	result, failureDetails, err := s.applyImport(ctx, intentID, batchID, req, info, negotiation, mode, newStrategies, legacy, kind)
	if err != nil {
		code := apperr.As(err).Code
		if failureDetails == nil {
			failureDetails = apperr.As(err).Details
		}
		s.abortIntent(ctx, intentID, batchID, code, err.Error(), failureDetails)
		return ImportResult{}, err
	}
	return result, nil
}

func (s *Store) applyImport(ctx context.Context, intentID, batchID string, req ImportRequest, info policy.InputInfo, negotiation policy.Negotiation, mode string, incoming []policy.NewStrategy, legacy []policy.LegacyStrategy, kind string) (ImportResult, any, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ImportResult{}, nil, err
	}
	defer tx.Rollback()
	current, err := currentVersion(ctx, tx)
	if err != nil {
		return ImportResult{}, nil, err
	}
	if current != req.BaseVersion {
		return ImportResult{}, map[string]any{"base_version": req.BaseVersion, "current_version": current}, staleBaseError(req.BaseVersion, current)
	}
	base, err := s.loadSnapshot(ctx, tx, current)
	if err != nil {
		return ImportResult{}, nil, err
	}
	threshold := base.ThresholdMicros
	if req.ThresholdMicros != nil {
		threshold = *req.ThresholdMicros
	}
	if threshold < 0 {
		return ImportResult{}, nil, apperr.New(apperr.SpecInvalid, "threshold_micros must be non-negative")
	}
	tolerance := policy.Tolerance{RelativePermille: base.TolerancePermille, AbsoluteReplicas: 0}
	if req.Tolerance != nil {
		tolerance = *req.Tolerance
	}
	var migrationPlan *policy.MigrationPlan
	var policies []storedPolicy
	var affected []string
	if negotiation.RequiresMigration {
		plan := policy.BuildMigrationPlan(legacy, base.Quotas, tolerance)
		migrationPlan = &plan
		if plan.Decision != "accept" {
			return ImportResult{}, map[string]any{
				"decision": plan.Decision, "tolerance": plan.Tolerance,
				"decisions": plan.Decisions, "numeric_mismatches": plan.NumericMismatches,
				"weighted_index_equivalence": plan.WeightedIndexEquivalence,
				"global_reasons":             plan.GlobalReasons,
			}, plan.AmbiguousError()
		}
		planned := make(map[string]policy.NewStrategy, len(plan.Strategies))
		for _, strategy := range plan.Strategies {
			planned[strategy.ServiceID] = strategy
		}
		policies = make([]storedPolicy, 0, len(legacy))
		for _, old := range legacy {
			strategy, ok := planned[old.ServiceID]
			if !ok {
				return ImportResult{}, nil, apperr.WithDetails(apperr.Internal, "migration plan lost a service", map[string]any{"service_id": old.ServiceID})
			}
			policies = append(policies, storedPolicy{
				Strategy: strategy, SourceJSON: old.SourceJSON,
				SourceFormat: policy.FormatLegacyJSON, SourceVersion: policy.LegacyFormatVersion,
			})
			affected = append(affected, old.ServiceID)
		}
		policies = mergePolicies(base.Policies, policies, mode)
	} else {
		if err := policy.ValidateStrategyBatch(incoming, base.Quotas); err != nil {
			return ImportResult{}, nil, err
		}
		policies = make([]storedPolicy, 0, len(incoming))
		for _, strategy := range incoming {
			raw, _ := json.Marshal(strategy)
			policies = append(policies, storedPolicy{
				Strategy: strategy, SourceJSON: raw,
				SourceFormat: info.Format, SourceVersion: info.Version,
			})
			affected = append(affected, strategy.ServiceID)
		}
		policies = mergePolicies(base.Policies, policies, mode)
	}
	sort.Strings(affected)
	strategies := policyList(policies)
	if err := policy.ValidateStrategyBatch(strategies, base.Quotas); err != nil {
		return ImportResult{}, nil, err
	}
	derived, index, err := policy.DeriveAll(strategies, base.Quotas, current+1)
	if err != nil {
		return ImportResult{}, nil, err
	}
	if err := s.writeSnapshot(ctx, tx, current+1, kind, info.Format, info.Version, negotiation, policies, base.Quotas, derived, index, threshold, tolerance.RelativePermille); err != nil {
		return ImportResult{}, nil, err
	}
	if err := s.callFailpoint("mid_rebuild"); err != nil {
		return ImportResult{}, nil, err
	}
	if err := writeRebuildEvent(ctx, tx, current+1, "full_rebuild", affected, "strategy import invalidated desired replicas, placement indices and weighted index atomically"); err != nil {
		return ImportResult{}, nil, err
	}
	audit := map[string]any{"intent_id": intentID, "format": info.Format, "version": info.Version, "mode": mode}
	if migrationPlan != nil {
		audit["migration_decision"] = migrationPlan.Decision
	}
	if err := writeAudit(ctx, tx, current+1, intentID, info, negotiation, migrationPlan, req.Data, audit); err != nil {
		return ImportResult{}, nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE state SET current_version=? WHERE id=1`, current+1); err != nil {
		return ImportResult{}, nil, err
	}
	var batch any
	if migrationPlan != nil {
		batch = migrationPlan
	}
	if err := terminateIntent(ctx, tx, intentID, batchID, "committed", "", batch); err != nil {
		return ImportResult{}, nil, err
	}
	if err := tx.Commit(); err != nil {
		return ImportResult{}, nil, err
	}
	state, err := s.loadState(ctx, current+1)
	if err != nil {
		return ImportResult{}, nil, err
	}
	return ImportResult{
		SnapshotVersion: current + 1, BaseVersion: current, Format: negotiation,
		Migration: migrationPlan, PolicyCount: len(strategies), Affected: affected, State: state,
	}, nil, nil
}

func (s *Store) createIntent(ctx context.Context, kind string, base int, details any, withMigrationBatch bool) (string, string, error) {
	ts := now()
	intentID := newID("int_")
	batchID := ""
	if withMigrationBatch {
		batchID = newID("mig_")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", "", err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO intents(intent_id,kind,base_version,target_version,status,details_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)`,
		intentID, kind, base, base+1, "pending", marshal(details), ts, ts); err != nil {
		return "", "", err
	}
	if withMigrationBatch {
		if _, err := tx.ExecContext(ctx, `INSERT INTO migration_batches(batch_id,intent_id,base_version,target_version,status,decision_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)`,
			batchID, intentID, base, base+1, "pending", "{}", ts, ts); err != nil {
			return "", "", err
		}
	}
	if err := tx.Commit(); err != nil {
		return "", "", err
	}
	return intentID, batchID, nil
}

func (s *Store) abortIntent(ctx context.Context, intentID, batchID, code, message string, details any) {
	if intentID == "" {
		return
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer tx.Rollback()
	payload := map[string]any{"error_code": code, "message": message}
	if details != nil {
		payload["details"] = details
	}
	_ = terminateIntent(ctx, tx, intentID, batchID, "aborted", code, payload)
	_ = tx.Commit()
}

func mergePolicies(base, incoming []storedPolicy, mode string) []storedPolicy {
	merged := make(map[string]storedPolicy, len(base)+len(incoming))
	if mode == "merge" {
		for _, item := range base {
			merged[item.Strategy.ServiceID] = item
		}
	}
	for _, item := range incoming {
		merged[item.Strategy.ServiceID] = item
	}
	out := make([]storedPolicy, 0, len(merged))
	for _, item := range merged {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Strategy.ServiceID < out[j].Strategy.ServiceID })
	return out
}

func (s *Store) writeSnapshot(ctx context.Context, tx *sql.Tx, version int, kind, format string, formatVersion int, negotiation policy.Negotiation, policies []storedPolicy, quotas []policy.QuotaGroup, derived []policy.DerivedService, index policy.WeightedIndex, threshold int64, tolerancePermille int) error {
	ts := now()
	if _, err := tx.ExecContext(ctx, `INSERT INTO snapshots(version,status,kind,format_name,format_version,created_at) VALUES(?,?,?,?,?,?)`, version, "complete", kind, format, formatVersion, ts); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO snapshot_state(version,threshold_micros,tolerance_permille) VALUES(?,?,?)`, version, threshold, tolerancePermille); err != nil {
		return err
	}
	for _, group := range quotas {
		if _, err := tx.ExecContext(ctx, `INSERT INTO quota_groups(version,group_id,vram_bytes,compute_milli) VALUES(?,?,?,?)`, version, group.ID, int64(group.VRAMBytes), int64(group.ComputeMilli)); err != nil {
			return err
		}
	}
	for _, item := range policies {
		spec, err := json.Marshal(item.Strategy)
		if err != nil {
			return err
		}
		source := item.SourceJSON
		if len(source) == 0 {
			source = spec
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO strategies(version,service_id,spec_json,source_json,source_format,source_version) VALUES(?,?,?,?,?,?)`,
			version, item.Strategy.ServiceID, string(spec), string(source), item.SourceFormat, item.SourceVersion); err != nil {
			return err
		}
	}
	for _, item := range derived {
		placements, err := json.Marshal(item.Placements)
		if err != nil {
			return err
		}
		capValue := item.ResourceCap
		if capValue > uint64(^uint64(0)>>1) {
			capValue = uint64(^uint64(0) >> 1)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO derived_services(version,service_id,desired_replicas,concurrency_need,resource_cap,placements_json) VALUES(?,?,?,?,?,?)`,
			version, item.Strategy.ServiceID, item.DesiredReplicas, item.ConcurrencyNeed, int64(capValue), string(placements)); err != nil {
			return err
		}
		for _, target := range []struct{ kind, value string }{
			{"derived", "desired_replicas"}, {"derived", "placement_indices"}, {"derived", "weighted_index"}, {"quota_group", item.Strategy.QuotaGroupRef},
		} {
			if _, err := tx.ExecContext(ctx, `INSERT INTO references_v1(version,owner_service,ref_kind,target) VALUES(?,?,?,?)`, version, item.Strategy.ServiceID, target.kind, target.value); err != nil {
				return err
			}
		}
	}
	indexJSON, err := json.Marshal(index)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO derived_index(version,threshold_micros,tolerance_permille,index_json) VALUES(?,?,?,?)`, version, threshold, tolerancePermille, string(indexJSON)); err != nil {
		return err
	}
	return nil
}

func writeRebuildEvent(ctx context.Context, tx *sql.Tx, version int, eventType string, affected []string, reason string) error {
	if affected == nil {
		affected = []string{}
	}
	sort.Strings(affected)
	_, err := tx.ExecContext(ctx, `INSERT INTO rebuild_events(version,event_type,affected_services_json,reason,created_at) VALUES(?,?,?,?,?)`,
		version, eventType, marshal(affected), reason, now())
	return err
}

func writeAudit(ctx context.Context, tx *sql.Tx, version int, intentID string, info policy.InputInfo, negotiation policy.Negotiation, plan *policy.MigrationPlan, data []byte, extra map[string]any) error {
	migrationJSON := "null"
	if plan != nil {
		migrationJSON = marshal(plan)
	}
	if extra == nil {
		extra = map[string]any{}
	}
	extra["payload_sha256"] = sha256Hex(data)
	_, err := tx.ExecContext(ctx, `INSERT INTO import_audits(version,intent_id,format_name,format_version,negotiation_json,migration_json,payload_sha256,created_at) VALUES(?,?,?,?,?,?,?,?)`,
		version, intentID, info.Format, info.Version, marshal(negotiation), migrationJSON, sha256Hex(data), now())
	return err
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func staleBaseError(base, current int) error {
	return apperr.WithDetails(apperr.StaleBase, "base_version is not the current committed version", map[string]any{
		"base_version": base, "current_version": current,
	})
}

func (s *Store) SetQuotaGroups(ctx context.Context, base int, groups []policy.QuotaGroup) (result MutationResult, retErr error) {
	policy.SortedQuotaGroups(groups)
	if err := policy.ValidateQuotaGroups(groups); err != nil {
		return MutationResult{}, err
	}
	intentID, _, err := s.createIntent(ctx, "quota_import", base, map[string]any{"quota_groups": len(groups)}, false)
	if err != nil {
		return MutationResult{}, err
	}
	if err := s.callFailpoint("after_intent"); err != nil {
		s.abortIntent(ctx, intentID, "", apperr.Internal, err.Error(), nil)
		return MutationResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.abortIntent(ctx, intentID, "", apperr.Internal, err.Error(), nil)
		return MutationResult{}, err
	}
	committed := false
	defer func() {
		_ = tx.Rollback()
		if !committed {
			code, message, details := apperr.Internal, "quota import transaction rolled back", any(nil)
			if retErr != nil {
				be := apperr.As(retErr)
				code, message, details = be.Code, be.Message, be.Details
			}
			s.abortIntent(ctx, intentID, "", code, message, details)
		}
	}()
	current, err := currentVersion(ctx, tx)
	if err != nil {
		return MutationResult{}, err
	}
	if current != base {
		return MutationResult{}, staleBaseError(base, current)
	}
	baseData, err := s.loadSnapshot(ctx, tx, current)
	if err != nil {
		return MutationResult{}, err
	}
	strategies := policyList(baseData.Policies)
	if len(strategies) > 0 {
		if err := policy.ValidateStrategyBatch(strategies, groups); err != nil {
			return MutationResult{}, err
		}
	}
	derived, index, err := policy.DeriveAll(strategies, groups, current+1)
	if err != nil {
		return MutationResult{}, err
	}
	if err := s.writeSnapshot(ctx, tx, current+1, "quota_import", "quota_json", 1, policy.Negotiation{}, baseData.Policies, groups, derived, index, baseData.ThresholdMicros, baseData.TolerancePermille); err != nil {
		return MutationResult{}, err
	}
	if err := s.callFailpoint("mid_rebuild"); err != nil {
		return MutationResult{}, err
	}
	affected := make([]string, 0, len(strategies))
	for _, strategy := range strategies {
		affected = append(affected, strategy.ServiceID)
	}
	if err := writeRebuildEvent(ctx, tx, current+1, "quota_rebuild", affected, "quota group envelope changed; all referring services and weighted index rebuilt atomically"); err != nil {
		return MutationResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE state SET current_version=? WHERE id=1`, current+1); err != nil {
		return MutationResult{}, err
	}
	if err := terminateIntent(ctx, tx, intentID, "", "committed", "", nil); err != nil {
		return MutationResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return MutationResult{}, err
	}
	committed = true
	state, err := s.loadState(ctx, current+1)
	if err != nil {
		return MutationResult{}, err
	}
	sort.Strings(affected)
	return MutationResult{SnapshotVersion: current + 1, BaseVersion: current, Affected: affected, Invalidated: []string{"weighted_index"}, State: state}, nil
}

func (s *Store) UpdateQuotaGroup(ctx context.Context, base int, group policy.QuotaGroup) (result MutationResult, retErr error) {
	if err := policy.ValidateQuotaGroup(group, 1); err != nil {
		return MutationResult{}, err
	}
	intentID, _, err := s.createIntent(ctx, "quota_update", base, map[string]any{"quota_group": group.ID}, false)
	if err != nil {
		return MutationResult{}, err
	}
	if err := s.callFailpoint("after_intent"); err != nil {
		s.abortIntent(ctx, intentID, "", apperr.Internal, err.Error(), nil)
		return MutationResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.abortIntent(ctx, intentID, "", apperr.Internal, err.Error(), nil)
		return MutationResult{}, err
	}
	committed := false
	defer func() {
		_ = tx.Rollback()
		if !committed {
			code, message, details := apperr.Internal, "quota update transaction rolled back", any(nil)
			if retErr != nil {
				be := apperr.As(retErr)
				code, message, details = be.Code, be.Message, be.Details
			}
			s.abortIntent(ctx, intentID, "", code, message, details)
		}
	}()
	current, err := currentVersion(ctx, tx)
	if err != nil {
		return MutationResult{}, err
	}
	if current != base {
		return MutationResult{}, staleBaseError(base, current)
	}
	baseData, err := s.loadSnapshot(ctx, tx, current)
	if err != nil {
		return MutationResult{}, err
	}
	quotaMap := map[string]policy.QuotaGroup{}
	for _, item := range baseData.Quotas {
		quotaMap[item.ID] = item
	}
	quotaMap[group.ID] = group
	quotas := make([]policy.QuotaGroup, 0, len(quotaMap))
	for _, item := range quotaMap {
		quotas = append(quotas, item)
	}
	policy.SortedQuotaGroups(quotas)
	strategies := policyList(baseData.Policies)
	if len(strategies) > 0 {
		if err := policy.ValidateStrategyBatch(strategies, quotas); err != nil {
			return MutationResult{}, err
		}
	}
	derived, index, err := policy.DeriveAll(strategies, quotas, current+1)
	if err != nil {
		return MutationResult{}, err
	}
	if err := s.writeSnapshot(ctx, tx, current+1, "quota_update", "quota_json", 1, policy.Negotiation{}, baseData.Policies, quotas, derived, index, baseData.ThresholdMicros, baseData.TolerancePermille); err != nil {
		return MutationResult{}, err
	}
	if err := s.callFailpoint("mid_rebuild"); err != nil {
		return MutationResult{}, err
	}
	affected := []string{}
	for _, strategy := range strategies {
		if strategy.QuotaGroupRef == group.ID {
			affected = append(affected, strategy.ServiceID)
		}
	}
	if err := writeRebuildEvent(ctx, tx, current+1, "quota_reference_cascade", affected, "quota group update invalidated dependent desired replicas, placement indices and weighted index"); err != nil {
		return MutationResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE state SET current_version=? WHERE id=1`, current+1); err != nil {
		return MutationResult{}, err
	}
	if err := terminateIntent(ctx, tx, intentID, "", "committed", "", nil); err != nil {
		return MutationResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return MutationResult{}, err
	}
	committed = true
	state, err := s.loadState(ctx, current+1)
	if err != nil {
		return MutationResult{}, err
	}
	sort.Strings(affected)
	return MutationResult{SnapshotVersion: current + 1, BaseVersion: current, Affected: affected, Invalidated: []string{"desired_replicas", "placement_indices", "weighted_index"}, State: state}, nil
}

func (s *Store) ApplyIncremental(ctx context.Context, req IncrementalRequest) (result MutationResult, retErr error) {
	if req.BaseVersion < 0 {
		return MutationResult{}, apperr.New(apperr.SpecInvalid, "base_version must be non-negative")
	}
	if len(req.Patches) == 0 && req.ThresholdMicros == nil && req.TolerancePermille == nil {
		return MutationResult{}, apperr.New(apperr.SpecInvalid, "incremental fragment has no fields")
	}
	patchedIDs := make([]string, 0, len(req.Patches))
	seen := map[string]struct{}{}
	for _, patch := range req.Patches {
		if patch.ServiceID == "" {
			return MutationResult{}, apperr.New(apperr.SpecInvalid, "incremental patch requires service_id")
		}
		if _, ok := seen[patch.ServiceID]; ok {
			return MutationResult{}, apperr.WithDetails(apperr.SpecInvalid, "duplicate service in incremental batch", map[string]any{"service_id": patch.ServiceID})
		}
		seen[patch.ServiceID] = struct{}{}
		patchedIDs = append(patchedIDs, patch.ServiceID)
	}
	intentID, _, err := s.createIntent(ctx, "incremental", req.BaseVersion, map[string]any{"patches": len(req.Patches), "service_ids": patchedIDs}, false)
	if err != nil {
		return MutationResult{}, err
	}
	if err := s.callFailpoint("after_intent"); err != nil {
		s.abortIntent(ctx, intentID, "", apperr.Internal, err.Error(), nil)
		return MutationResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.abortIntent(ctx, intentID, "", apperr.Internal, err.Error(), nil)
		return MutationResult{}, err
	}
	committed := false
	defer func() {
		_ = tx.Rollback()
		if !committed {
			code, message, details := apperr.Internal, "incremental transaction rolled back", any(nil)
			if retErr != nil {
				be := apperr.As(retErr)
				code, message, details = be.Code, be.Message, be.Details
			}
			s.abortIntent(ctx, intentID, "", code, message, details)
		}
	}()
	current, err := currentVersion(ctx, tx)
	if err != nil {
		return MutationResult{}, err
	}
	if current != req.BaseVersion {
		return MutationResult{}, staleBaseError(req.BaseVersion, current)
	}
	baseData, err := s.loadSnapshot(ctx, tx, current)
	if err != nil {
		return MutationResult{}, err
	}
	byID := map[string]storedPolicy{}
	for _, item := range baseData.Policies {
		byID[item.Strategy.ServiceID] = item
	}
	for _, patch := range req.Patches {
		item, ok := byID[patch.ServiceID]
		if !ok {
			return MutationResult{}, apperr.WithDetails(apperr.NotFound, "service does not exist in the bound snapshot", map[string]any{"service_id": patch.ServiceID, "base_version": current})
		}
		applyPatch(&item.Strategy, patch)
		item.SourceFormat = "incremental"
		item.SourceVersion = current
		item.SourceJSON, _ = json.Marshal(item.Strategy)
		byID[patch.ServiceID] = item
	}
	policies := make([]storedPolicy, 0, len(byID))
	for _, item := range byID {
		policies = append(policies, item)
	}
	sort.Slice(policies, func(i, j int) bool { return policies[i].Strategy.ServiceID < policies[j].Strategy.ServiceID })
	strategies := policyList(policies)
	if err := policy.ValidateStrategyBatch(strategies, baseData.Quotas); err != nil {
		return MutationResult{}, err
	}
	threshold := baseData.ThresholdMicros
	if req.ThresholdMicros != nil {
		threshold = *req.ThresholdMicros
	}
	if threshold < 0 {
		return MutationResult{}, apperr.New(apperr.SpecInvalid, "threshold_micros must be non-negative")
	}
	tolerancePermille := baseData.TolerancePermille
	if req.TolerancePermille != nil {
		tolerancePermille = *req.TolerancePermille
	}
	if tolerancePermille < 0 || tolerancePermille > 1_000_000 {
		return MutationResult{}, apperr.New(apperr.SpecInvalid, "tolerance_permille out of range")
	}
	derived, index, err := policy.DeriveAll(strategies, baseData.Quotas, current+1)
	if err != nil {
		return MutationResult{}, err
	}
	if err := s.writeSnapshot(ctx, tx, current+1, "incremental", "incremental_json", 1, policy.Negotiation{}, policies, baseData.Quotas, derived, index, threshold, tolerancePermille); err != nil {
		return MutationResult{}, err
	}
	if err := s.callFailpoint("mid_rebuild"); err != nil {
		return MutationResult{}, err
	}
	if err := writeRebuildEvent(ctx, tx, current+1, "incremental_cascade", patchedIDs, "changed policy fields invalidated all derived structures for affected services and the global weighted index"); err != nil {
		return MutationResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE state SET current_version=? WHERE id=1`, current+1); err != nil {
		return MutationResult{}, err
	}
	if err := terminateIntent(ctx, tx, intentID, "", "committed", "", nil); err != nil {
		return MutationResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return MutationResult{}, err
	}
	committed = true
	state, err := s.loadState(ctx, current+1)
	if err != nil {
		return MutationResult{}, err
	}
	sort.Strings(patchedIDs)
	return MutationResult{SnapshotVersion: current + 1, BaseVersion: current, Affected: patchedIDs, Invalidated: []string{"desired_replicas", "placement_indices", "weighted_index"}, State: state}, nil
}

func applyPatch(strategy *policy.NewStrategy, patch IncrementalPatch) {
	if patch.MinReplicas != nil {
		strategy.MinReplicas = *patch.MinReplicas
	}
	if patch.MaxReplicas != nil {
		strategy.MaxReplicas = *patch.MaxReplicas
	}
	if patch.TargetConcurrency != nil {
		strategy.TargetConcurrency = *patch.TargetConcurrency
	}
	if patch.LatencyBudgetMS != nil {
		strategy.LatencyBudgetMS = *patch.LatencyBudgetMS
	}
	if patch.QuotaGroupRef != nil {
		strategy.QuotaGroupRef = *patch.QuotaGroupRef
	}
	if patch.WeightPermille != nil {
		strategy.WeightPermille = *patch.WeightPermille
	}
	if patch.VRAMBytesPerReplica != nil {
		strategy.VRAMBytesPerReplica = *patch.VRAMBytesPerReplica
	}
	if patch.ComputeMilliPerReplica != nil {
		strategy.ComputeMilliPerReplica = *patch.ComputeMilliPerReplica
	}
	if patch.ElasticEnabled != nil {
		strategy.ElasticEnabled = *patch.ElasticEnabled
	}
	if patch.PlacementZone != nil {
		strategy.PlacementZone = *patch.PlacementZone
	}
}
