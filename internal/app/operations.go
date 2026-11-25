package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"elastic-scale/internal/engine"
	"elastic-scale/internal/model"
)

func toBusinessError(err error) *model.BusinessError {
	var be *model.BusinessError
	if errors.As(err, &be) {
		return be
	}
	return model.NewError(model.CodeInternal, err.Error(), nil)
}

func (a *App) rejectIntent(ctx context.Context, commitID string, err error) error {
	be := toBusinessError(err)
	_ = a.failIntent(ctx, commitID, be)
	return be
}

func thresholds(services []model.Service) (int64, int64) {
	if len(services) == 0 {
		p := model.Policy{}.Normalize()
		return p.IncrementalThresholdMicro, p.PropagationThresholdMicro
	}
	ids := make([]model.Service, len(services))
	copy(ids, services)
	sort.Slice(ids, func(i, j int) bool { return ids[i].ID < ids[j].ID })
	p := ids[0].Policy.Normalize()
	return p.IncrementalThresholdMicro, p.PropagationThresholdMicro
}

func allServiceIDs(state *engine.State) []string {
	out := make([]string, 0, len(state.Services))
	for _, svc := range state.Services {
		out = append(out, svc.ID)
	}
	sort.Strings(out)
	return out
}

func stampViewVersions(views map[string]*engine.Derived, version int64) {
	for _, dv := range views {
		dv.Impact.CurrentVersion = version
		if dv.Decision != nil {
			dv.Decision.CurrentVersion = version
			dv.Decision.MaterializedVersion = version
		}
	}
}

func (a *App) ReplaceCatalog(ctx context.Context, req CatalogRequest) (*model.CommitResponse, error) {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	if req.CommitID == "" {
		req.CommitID = "catalog-" + canonicalHash("catalog", req.Services)
	}
	replay, err := a.beginIntent(ctx, req.CommitID, "catalog", 0, req)
	if err != nil || replay != nil {
		return replay, err
	}
	services := append([]model.Service(nil), req.Services...)
	if err := engine.ValidateCatalog(services); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	defer tx.Rollback()
	if err := replaceCatalogTx(ctx, tx, services); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	current, err := getMeta(ctx, tx, "current_version")
	if err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	state, err := loadState(ctx, tx)
	if err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	views, err := engine.BuildAll(state)
	if err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	stampViewVersions(views, current)
	if err := persistDerivedTx(ctx, tx, state, views, nil); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	itemThreshold, propThreshold := thresholds(state.Services)
	run := &model.RebuildRun{
		Version: current, RequestedMode: "full", SelectedPath: "full",
		ThresholdMicro: itemThreshold, PropagationThresholdMicro: propThreshold,
		TotalSubscriptionItems: countSubscriptions(state.Services), TotalSampleRecords: len(state.Samples),
		TouchedRatioMicro: model.Scale, PropagationCardinality: len(state.Services), ServiceCount: len(state.Services),
		RecomputeScope: allServiceIDs(state), Reason: "catalog_replaced",
		MetricsChecksum: engine.GlobalMetricsChecksum(views), DecisionsChecksum: engine.GlobalDecisionsChecksum(views),
		CreatedAtMS: storeNowMS(),
	}
	if err := insertRebuildRunTx(ctx, tx, run); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	if err := setMeta(ctx, tx, "materialized_version", current); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	resp := &model.CommitResponse{CommitID: req.CommitID, Kind: "catalog", Version: current, Rebuild: run}
	if err := finishIntentTx(ctx, tx, req.CommitID, resp); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return resp, nil
}

func countSubscriptions(services []model.Service) int {
	n := 0
	for _, svc := range services {
		n += len(svc.Subscriptions)
	}
	return n
}

func (a *App) LoadSnapshot(ctx context.Context, req model.SnapshotRequest) (*model.CommitResponse, error) {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	if req.CommitID == "" {
		req.CommitID = fmt.Sprintf("snapshot-v%d-%s", req.Snapshot.Version, req.Snapshot.Digest)
	}
	replay, err := a.beginIntent(ctx, req.CommitID, "snapshot", req.Snapshot.Version, req)
	if err != nil || replay != nil {
		return replay, err
	}
	if err := req.Snapshot.ValidateDigest(); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	defer tx.Rollback()
	current, err := getMeta(ctx, tx, "current_version")
	if err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	if req.Snapshot.Version <= current {
		be := model.NewError(model.CodeSnapshotInvalid, "snapshot version must be monotonically greater than the committed version", map[string]any{
			"provided_version": req.Snapshot.Version, "current_version": current,
		})
		return nil, a.rejectIntent(ctx, req.CommitID, be)
	}
	catalogState, err := loadState(ctx, tx)
	if err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	if len(catalogState.Services) == 0 {
		be := model.NewError(model.CodeSnapshotInvalid, "catalog must be loaded before a metric snapshot", map[string]any{"version": req.Snapshot.Version})
		return nil, a.rejectIntent(ctx, req.CommitID, be)
	}
	if be := validateSnapshotSamples(req.Snapshot, catalogState.Services); be != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, be)
	}
	payload, _ := json.Marshal(req.Snapshot)
	if _, err := tx.ExecContext(ctx, `INSERT INTO snapshots(version,generated_at_ms,digest,payload_json,created_at_ms) VALUES(?,?,?,?,?)`,
		req.Snapshot.Version, req.Snapshot.GeneratedAtMS, req.Snapshot.Digest, string(payload), storeNowMS()); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, model.NewError(model.CodeSnapshotInvalid, "snapshot cannot be persisted", map[string]any{"reason": err.Error()}))
	}
	if err := replaceAllSamplesTx(ctx, tx, req.Snapshot.Samples); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	if err := setMeta(ctx, tx, "current_version", req.Snapshot.Version); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	state, err := loadState(ctx, tx)
	if err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	views, err := engine.BuildAll(state)
	if err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	stampViewVersions(views, req.Snapshot.Version)
	if err := persistDerivedTx(ctx, tx, state, views, nil); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	itemThreshold, propThreshold := thresholds(state.Services)
	run := &model.RebuildRun{
		Version: req.Snapshot.Version, RequestedMode: "full", SelectedPath: "full",
		ThresholdMicro: itemThreshold, PropagationThresholdMicro: propThreshold,
		TotalSubscriptionItems: countSubscriptions(state.Services), TotalSampleRecords: len(state.Samples),
		TouchedSubscriptionItems: countSubscriptions(state.Services), TouchedSampleRecords: len(state.Samples),
		TouchedRatioMicro: model.Scale, PropagationCardinality: len(state.Services), ServiceCount: len(state.Services),
		RecomputeScope: allServiceIDs(state), Reason: "full_snapshot_replaces_active_materialization",
		MetricsChecksum: engine.GlobalMetricsChecksum(views), DecisionsChecksum: engine.GlobalDecisionsChecksum(views),
		CreatedAtMS: storeNowMS(),
	}
	if err := insertRebuildRunTx(ctx, tx, run); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	if err := setMeta(ctx, tx, "materialized_version", req.Snapshot.Version); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	resp := &model.CommitResponse{CommitID: req.CommitID, Kind: "snapshot", Version: req.Snapshot.Version, BaseVersion: current, Rebuild: run}
	if err := finishIntentTx(ctx, tx, req.CommitID, resp); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return resp, nil
}

func (a *App) ApplyIncrement(ctx context.Context, req model.IncrementRequest) (*model.CommitResponse, error) {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	if req.CommitID == "" {
		req.CommitID = fmt.Sprintf("increment-base%d-%s", req.BaseVersion, canonicalHash("increment", req))
	}
	replay, err := a.beginIntent(ctx, req.CommitID, "increment", req.BaseVersion, req)
	if err != nil || replay != nil {
		return replay, err
	}
	if req.CrashAfterIntent {
		current, _, verr := a.Versions(ctx)
		if verr != nil {
			return nil, a.rejectIntent(ctx, req.CommitID, verr)
		}
		return &model.CommitResponse{CommitID: req.CommitID, Kind: "increment", Version: current, BaseVersion: req.BaseVersion, CrashArmed: true}, nil
	}
	if len(req.SampleChanges) == 0 && len(req.SubscriptionChanges) == 0 && len(req.DependencyChanges) == 0 {
		be := model.NewError(model.CodeInvalidRequest, "increment must contain at least one local change", map[string]any{"commit_id": req.CommitID})
		return nil, a.rejectIntent(ctx, req.CommitID, be)
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	defer tx.Rollback()
	current, err := getMeta(ctx, tx, "current_version")
	if err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	if req.BaseVersion != current {
		be := model.NewError(model.CodeStaleBase, "increment base_version is not the current committed version", map[string]any{
			"commit_id": req.CommitID, "supplied_base_version": req.BaseVersion, "current_committed_version": current,
		})
		return nil, a.rejectIntent(ctx, req.CommitID, be)
	}
	oldState, err := loadState(ctx, tx)
	if err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	serviceByID := map[string]*model.Service{}
	for i := range oldState.Services {
		serviceByID[oldState.Services[i].ID] = &oldState.Services[i]
	}
	seedSet := map[string]struct{}{}
	sampleChanges := map[string]model.SampleChange{}
	for _, change := range req.SampleChanges {
		op := change.Op
		if op == "" {
			op = "upsert"
		}
		if op == "upsert" {
			if change.Sample == nil {
				be := model.NewError(model.CodeInvalidRequest, "upsert sample change requires sample", map[string]any{"commit_id": req.CommitID})
				return nil, a.rejectIntent(ctx, req.CommitID, be)
			}
			sample := change.Sample.NormalizeWeight()
			if _, ok := serviceByID[sample.ServiceID]; !ok {
				be := model.NewError(model.CodeInvalidRequest, "sample references unknown service", map[string]any{"service_id": sample.ServiceID})
				return nil, a.rejectIntent(ctx, req.CommitID, be)
			}
			key := sample.Key().String()
			sampleChanges[key] = change
			seedSet[sample.ServiceID] = struct{}{}
			if err := upsertSamplesTx(ctx, tx, []model.Sample{sample}); err != nil {
				return nil, a.rejectIntent(ctx, req.CommitID, err)
			}
		} else if op == "remove" {
			if change.Key == nil {
				be := model.NewError(model.CodeInvalidRequest, "remove sample change requires key", map[string]any{"commit_id": req.CommitID})
				return nil, a.rejectIntent(ctx, req.CommitID, be)
			}
			key := *change.Key
			if _, ok := serviceByID[key.ServiceID]; !ok {
				be := model.NewError(model.CodeInvalidRequest, "sample key references unknown service", map[string]any{"service_id": key.ServiceID})
				return nil, a.rejectIntent(ctx, req.CommitID, be)
			}
			sampleChanges[key.String()] = change
			seedSet[key.ServiceID] = struct{}{}
			if _, err := deleteSampleTx(ctx, tx, key); err != nil {
				return nil, a.rejectIntent(ctx, req.CommitID, err)
			}
		} else {
			be := model.NewError(model.CodeInvalidRequest, "unsupported sample change op", map[string]any{"op": op})
			return nil, a.rejectIntent(ctx, req.CommitID, be)
		}
	}
	subChanges := map[string]struct{}{}
	for _, change := range req.SubscriptionChanges {
		svc, ok := serviceByID[change.ServiceID]
		if !ok {
			be := model.NewError(model.CodeInvalidRequest, "subscription change references unknown service", map[string]any{"service_id": change.ServiceID})
			return nil, a.rejectIntent(ctx, req.CommitID, be)
		}
		key := change.ServiceID + "|" + change.Metric
		subChanges[key] = struct{}{}
		seedSet[change.ServiceID] = struct{}{}
		op := change.Op
		if op == "" {
			op = "upsert"
		}
		if op == "remove" {
			next := svc.Subscriptions[:0]
			for _, sub := range svc.Subscriptions {
				if sub.Metric != change.Metric {
					next = append(next, sub)
				}
			}
			svc.Subscriptions = next
		} else {
			weight := change.Weight
			if weight <= 0 {
				weight = 1
			}
			found := false
			for i := range svc.Subscriptions {
				if svc.Subscriptions[i].Metric == change.Metric {
					svc.Subscriptions[i].Weight = weight
					found = true
					break
				}
			}
			if !found {
				svc.Subscriptions = append(svc.Subscriptions, model.Subscription{Metric: change.Metric, Weight: weight})
			}
		}
	}
	dependencyTouched := len(req.DependencyChanges) > 0
	for _, change := range req.DependencyChanges {
		svc, ok := serviceByID[change.ServiceID]
		if !ok {
			be := model.NewError(model.CodeInvalidRequest, "dependency change references unknown service", map[string]any{"service_id": change.ServiceID})
			return nil, a.rejectIntent(ctx, req.CommitID, be)
		}
		if change.DependsOn != "" {
			if _, ok := serviceByID[change.DependsOn]; !ok {
				be := model.NewError(model.CodeInvalidRequest, "dependency change references unknown upstream", map[string]any{"depends_on": change.DependsOn})
				return nil, a.rejectIntent(ctx, req.CommitID, be)
			}
		}
		seedSet[change.ServiceID] = struct{}{}
		seedSet[change.DependsOn] = struct{}{}
		op := change.Op
		if op == "" {
			op = "upsert"
		}
		if op == "remove" {
			next := svc.Dependencies[:0]
			for _, dep := range svc.Dependencies {
				if dep != change.DependsOn {
					next = append(next, dep)
				}
			}
			svc.Dependencies = next
		} else {
			found := false
			for _, dep := range svc.Dependencies {
				if dep == change.DependsOn {
					found = true
					break
				}
			}
			if !found {
				svc.Dependencies = append(svc.Dependencies, change.DependsOn)
			}
		}
	}
	updatedServices := oldState.Services
	if err := engine.ValidateCatalog(updatedServices); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	if err := replaceCatalogTx(ctx, tx, updatedServices); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	newVersion := current + 1
	if err := setMeta(ctx, tx, "current_version", newVersion); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	newState, err := loadState(ctx, tx)
	if err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	scopeSet := map[string]struct{}{}
	var seeds []string
	for id := range seedSet {
		if _, ok := newState.Service(id); ok {
			seeds = append(seeds, id)
			scopeSet[id] = struct{}{}
		}
	}
	sort.Strings(seeds)
	for _, seed := range seeds {
		for _, node := range engine.ImpactSet(newState, seed) {
			scopeSet[node.ServiceID] = struct{}{}
		}
	}
	scope := make([]string, 0, len(scopeSet))
	for id := range scopeSet {
		scope = append(scope, id)
	}
	sort.Strings(scope)
	totalSubs := countSubscriptions(oldState.Services)
	totalSamples := len(oldState.Samples)
	touchedSubs := len(subChanges)
	touchedSamples := len(sampleChanges)
	touchedRatio := engine.RatioMicro(touchedSubs+touchedSamples, totalSubs+totalSamples)
	itemThreshold, propThreshold := thresholds(newState.Services)
	propRatio := engine.RatioMicro(len(scope), len(newState.Services))
	requestedMode := req.Mode
	if requestedMode == "" {
		requestedMode = "auto"
	}
	selectedPath := "incremental"
	reason := "touched_ratio_and_propagation_within_threshold"
	switch {
	case requestedMode == "full":
		selectedPath = "full"
		reason = "full_path_requested"
	case dependencyTouched:
		selectedPath = "full"
		reason = "dependency_impact_cardinality_changed"
	case touchedRatio > itemThreshold:
		selectedPath = "full"
		reason = "touched_item_ratio_exceeds_threshold"
	case propRatio > propThreshold:
		selectedPath = "full"
		reason = "propagation_cardinality_exceeds_threshold"
	}
	views, err := loadDerivedMapTx(ctx, tx)
	if err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	if selectedPath == "full" {
		views, err = engine.BuildAll(newState)
		if err != nil {
			return nil, a.rejectIntent(ctx, req.CommitID, err)
		}
	} else {
		if err := engine.BuildScope(newState, scope, views); err != nil {
			return nil, a.rejectIntent(ctx, req.CommitID, err)
		}
	}
	stampViewVersions(views, newVersion)
	if err := persistDerivedTx(ctx, tx, newState, views, nil); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	run := &model.RebuildRun{
		Version: newVersion, RequestedMode: requestedMode, SelectedPath: selectedPath,
		ThresholdMicro: itemThreshold, PropagationThresholdMicro: propThreshold,
		TouchedSubscriptionItems: touchedSubs, TotalSubscriptionItems: totalSubs,
		TouchedSampleRecords: touchedSamples, TotalSampleRecords: totalSamples,
		TouchedRatioMicro: touchedRatio, PropagationCardinality: len(scope), ServiceCount: len(newState.Services),
		RecomputeScope: scope, Reason: reason,
		MetricsChecksum: engine.GlobalMetricsChecksum(views), DecisionsChecksum: engine.GlobalDecisionsChecksum(views),
		CreatedAtMS: storeNowMS(),
	}
	if err := insertRebuildRunTx(ctx, tx, run); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	payload, _ := json.Marshal(req)
	if _, err := tx.ExecContext(ctx, `INSERT INTO increments(commit_id,base_version,version,payload_json,created_at_ms) VALUES(?,?,?,?,?)`,
		req.CommitID, req.BaseVersion, newVersion, string(payload), storeNowMS()); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	if err := setMeta(ctx, tx, "materialized_version", newVersion); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	resp := &model.CommitResponse{CommitID: req.CommitID, Kind: "increment", Version: newVersion, BaseVersion: req.BaseVersion, Rebuild: run}
	if err := finishIntentTx(ctx, tx, req.CommitID, resp); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return resp, nil
}

func (a *App) Rebuild(ctx context.Context, req model.RebuildRequest) (*model.CommitResponse, error) {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	if req.CommitID == "" {
		req.CommitID = fmt.Sprintf("rebuild-%s-%d", req.Mode, storeNowMS())
	}
	replay, err := a.beginIntent(ctx, req.CommitID, "rebuild", 0, req)
	if err != nil || replay != nil {
		return replay, err
	}
	mode := req.Mode
	if mode == "" {
		mode = "full"
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	defer tx.Rollback()
	state, err := loadState(ctx, tx)
	if err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	itemThreshold, propThreshold := thresholds(state.Services)
	scope := allServiceIDs(state)
	selectedPath := "full"
	reason := "manual_full_rebuild"
	views, err := engine.BuildAll(state)
	if err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	if mode == "incremental" && len(req.Services) > 0 {
		selectedPath = "incremental"
		reason = "manual_incremental_scope_rebuild"
		scopeSet := map[string]struct{}{}
		for _, id := range req.Services {
			if _, ok := state.Service(id); !ok {
				be := model.NewError(model.CodeNotFound, "service not found", map[string]any{"service_id": id})
				return nil, a.rejectIntent(ctx, req.CommitID, be)
			}
			scopeSet[id] = struct{}{}
			for _, node := range engine.ImpactSet(state, id) {
				scopeSet[node.ServiceID] = struct{}{}
			}
		}
		scope = make([]string, 0, len(scopeSet))
		for id := range scopeSet {
			scope = append(scope, id)
		}
		sort.Strings(scope)
		views, err = loadDerivedMapTx(ctx, tx)
		if err != nil {
			return nil, a.rejectIntent(ctx, req.CommitID, err)
		}
		if err := engine.BuildScope(state, scope, views); err != nil {
			return nil, a.rejectIntent(ctx, req.CommitID, err)
		}
	}
	stampViewVersions(views, state.Version)
	if err := persistDerivedTx(ctx, tx, state, views, nil); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	run := &model.RebuildRun{
		Version: state.Version, RequestedMode: mode, SelectedPath: selectedPath,
		ThresholdMicro: itemThreshold, PropagationThresholdMicro: propThreshold,
		TotalSubscriptionItems: countSubscriptions(state.Services), TotalSampleRecords: len(state.Samples),
		TouchedRatioMicro: 0, PropagationCardinality: len(scope), ServiceCount: len(state.Services),
		RecomputeScope: scope, Reason: reason,
		MetricsChecksum: engine.GlobalMetricsChecksum(views), DecisionsChecksum: engine.GlobalDecisionsChecksum(views),
		CreatedAtMS: storeNowMS(),
	}
	if selectedPath == "full" {
		run.TouchedSampleRecords = run.TotalSampleRecords
		run.TouchedSubscriptionItems = run.TotalSubscriptionItems
		run.TouchedRatioMicro = model.Scale
	}
	if err := insertRebuildRunTx(ctx, tx, run); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	if err := setMeta(ctx, tx, "materialized_version", state.Version); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	resp := &model.CommitResponse{CommitID: req.CommitID, Kind: "rebuild", Version: state.Version, Rebuild: run}
	if err := finishIntentTx(ctx, tx, req.CommitID, resp); err != nil {
		return nil, a.rejectIntent(ctx, req.CommitID, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return resp, nil
}

func validateSnapshotSamples(snapshot model.Snapshot, services []model.Service) *model.BusinessError {
	serviceIDs := map[string]struct{}{}
	for _, svc := range services {
		serviceIDs[svc.ID] = struct{}{}
	}
	seenKeys := map[string]struct{}{}
	for _, raw := range snapshot.Samples {
		sample := raw.NormalizeWeight()
		if sample.ServiceID == "" || sample.InstanceID == "" || sample.Metric == "" || sample.SampledAtMS <= 0 {
			return model.NewError(model.CodeSnapshotInvalid, "snapshot sample is missing identity fields", map[string]any{"sample": sample})
		}
		if _, ok := serviceIDs[sample.ServiceID]; !ok {
			return model.NewError(model.CodeSnapshotInvalid, "snapshot sample references unknown service", map[string]any{"service_id": sample.ServiceID})
		}
		key := sample.Key().String()
		if _, ok := seenKeys[key]; ok {
			return model.NewError(model.CodeSnapshotInvalid, "snapshot contains duplicate sample key", map[string]any{"sample_key": key})
		}
		seenKeys[key] = struct{}{}
	}
	return nil
}
