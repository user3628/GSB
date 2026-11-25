package convergence

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type ServiceOptions struct {
	DBPath              string
	EndpointOverride    string
	Client              *WriteClient
	Now                 func() time.Time
	AfterSnapshotIntent func(intentID string)
	AfterPlanIntent     func(intentID string)
	AfterWriteIntent    func(batchID, intentID string)
}

type Service struct {
	opts             ServiceOptions
	store            *Store
	writerMu         sync.Mutex
	endpointMu       sync.RWMutex
	state            atomic.Pointer[CommittedVersion]
	callsMu          sync.Mutex
	calls            map[string]*batchCall
	recovery         *RecoveryReport
	client           *WriteClient
	endpointOverride string
}

type batchCall struct {
	requestHash string
	done        chan struct{}
	result      *BatchApplyResult
	err         error
}

func OpenService(ctx context.Context, options ServiceOptions) (*Service, *RecoveryReport, error) {
	if options.Now == nil {
		options.Now = time.Now
	}
	store, err := OpenStore(ctx, options.DBPath)
	if err != nil {
		return nil, nil, err
	}
	recovery, err := store.Recover(ctx)
	if err != nil {
		_ = store.Close()
		return nil, nil, err
	}
	service := &Service{
		opts:             options,
		store:            store,
		calls:            map[string]*batchCall{},
		recovery:         recovery,
		client:           options.Client,
		endpointOverride: options.EndpointOverride,
	}
	if service.client == nil {
		service.client = NewWriteClient(5 * time.Second)
	}

	version, err := store.CurrentVersion(ctx)
	if err != nil {
		_ = store.Close()
		return nil, nil, err
	}
	if version > 0 {
		snapshot, _, err := store.LoadCommittedSnapshot(ctx, version)
		if err != nil {
			_ = store.Close()
			return nil, nil, err
		}
		view, buildErr := buildCommittedView(snapshot, nil)
		if buildErr != nil && AsError(buildErr).Code != CodeDependencyCycle {
			_ = store.Close()
			return nil, nil, buildErr
		}
		if err := service.overlayPersistedRuns(ctx, view); err != nil {
			_ = store.Close()
			return nil, nil, err
		}
		service.state.Store(view)
	}
	return service, recovery, nil
}

func (s *Service) Close() error {
	if s.store == nil {
		return nil
	}
	return s.store.Close()
}

func (s *Service) RecoveryReport() *RecoveryReport { return s.recovery }
func (s *Service) Store() *Store                   { return s.store }

func (s *Service) View() *CommittedVersion { return s.state.Load() }

func (s *Service) CurrentSnapshot() (*Snapshot, error) {
	view := s.state.Load()
	if view == nil || view.Snapshot == nil {
		return nil, NewError(CodeNotFound, "no committed snapshot", nil)
	}
	return view.Snapshot, nil
}

func (s *Service) SetEndpointOverride(endpoint string) {
	s.endpointMu.Lock()
	defer s.endpointMu.Unlock()
	s.endpointOverride = endpoint
	s.opts.EndpointOverride = endpoint
}

func (s *Service) ConfigureCrashHooks(afterWriteIntent func(batchID string)) {
	s.opts.AfterWriteIntent = func(batchID, intentID string) {
		if afterWriteIntent != nil {
			afterWriteIntent(batchID)
		}
	}
}

func buildCommittedView(snapshot *Snapshot, cascade *CascadeReport) (*CommittedVersion, error) {
	driftList, driftByID, err := JudgeSnapshot(snapshot)
	if err != nil {
		return nil, err
	}
	plan, planErr := BuildPlan(snapshot.Objects, snapshot.Policy)
	if plan == nil {
		return nil, planErr
	}
	batchStates := map[string]*BatchState{}
	for _, batch := range plan.Batches {
		batchStates[batch.BatchID] = &BatchState{
			Batch:       batch,
			Status:      "pending",
			ContentHash: batch.ContentHash,
		}
	}
	view := &CommittedVersion{
		Snapshot:    snapshot,
		DriftList:   driftList,
		DriftByID:   driftByID,
		Plan:        plan,
		BatchStates: batchStates,
		Cascade:     cascade,
	}
	view.Summary = computeSummary(snapshot, plan, batchStates)
	return view, planErr
}

func computeSummary(snapshot *Snapshot, plan *Plan, states map[string]*BatchState) Summary {
	summary := Summary{CategoryCounts: map[string]int{}}
	if snapshot != nil {
		summary.SnapshotVersion = snapshot.SnapshotVersion
		summary.PolicyVersion = snapshot.Policy.PolicyVersion
		summary.ObjectCount = len(snapshot.Objects)
		summary.ClusterCount = len(snapshot.Clusters)
	}
	if plan != nil {
		summary.LayerCount = len(plan.Layers)
		summary.BatchCount = len(plan.Batches)
		summary.SplitBatchCount = plan.SplitBatchCount
		summary.CycleCount = len(plan.Cycles)
	}
	for _, report := range maybeDriftList(snapshot, plan) {
		if report.Converged {
			summary.ConvergedCount++
		} else {
			summary.DriftedCount++
			summary.CategoryCounts[report.PrimaryCategory]++
		}
	}
	for _, state := range states {
		switch state.Status {
		case "applying":
			summary.BatchesApplying++
		case "succeeded":
			summary.BatchesSucceeded++
		case "failed":
			summary.BatchesFailed++
		default:
			summary.BatchesPending++
		}
	}
	return summary
}

func maybeDriftList(snapshot *Snapshot, plan *Plan) []DriftReport {
	if snapshot == nil {
		return nil
	}
	_, byID, err := JudgeSnapshot(snapshot)
	if err != nil {
		return nil
	}
	result := make([]DriftReport, 0, len(byID))
	for _, report := range byID {
		result = append(result, report)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ObjectID < result[j].ObjectID })
	return result
}

func (s *Service) overlayPersistedRuns(ctx context.Context, view *CommittedVersion) error {
	runs, err := s.store.LoadBatchRuns(ctx, view.Snapshot.SnapshotVersion)
	if err != nil {
		return err
	}
	for _, run := range runs {
		state, ok := view.BatchStates[run.BatchID]
		if !ok {
			continue
		}
		if run.ContentHash != "" && state.ContentHash != run.ContentHash {
			continue
		}
		state.Attempts = run.Attempts
		state.RequestHash = run.RequestHash
		state.Status = run.Status
		if run.ResultJSON != "" && (run.Status == "succeeded" || run.Status == "failed") {
			var result BatchApplyResult
			if err := json.Unmarshal([]byte(run.ResultJSON), &result); err == nil {
				state.ObjectResults = result.ObjectResults
				state.Error = result.Error
				state.UpdatedAt = result.AppliedAt
				var callErr error
				if result.Error != nil {
					callErr = result.Error
				}
				call := &batchCall{requestHash: run.RequestHash, done: make(chan struct{}), result: &result, err: callErr}
				close(call.done)
				s.calls[batchScope(run.Version, run.BatchID)] = call
			}
		}
		if state.Status == "applying" {
			state.Status = "pending"
		}
	}
	view.Summary = computeSummary(view.Snapshot, view.Plan, view.BatchStates)
	return nil
}

func (s *Service) CommitFull(ctx context.Context, snapshot *Snapshot) (*CommitResult, error) {
	s.writerMu.Lock()
	defer s.writerMu.Unlock()

	if snapshot == nil {
		return nil, NewError(CodeSpecInvalid, "snapshot is nil", nil)
	}
	if err := NormalizeSnapshot(snapshot); err != nil {
		return nil, err
	}
	if err := verifyOrPrepareChecksums(snapshot); err != nil {
		return nil, err
	}

	current := s.state.Load()
	if current != nil {
		currentVersion := current.Snapshot.SnapshotVersion
		if snapshot.SnapshotVersion == currentVersion {
			newHash, _ := snapshot.ComputeChecksum()
			oldHash, _ := current.Snapshot.ComputeChecksum()
			if newHash == oldHash {
				return &CommitResult{Version: currentVersion, Current: current, Plan: current.Plan}, nil
			}
			return nil, NewError(CodeVersionConflict, "same snapshot version has different content", map[string]any{"version": currentVersion})
		}
		if snapshot.SnapshotVersion < currentVersion {
			return nil, NewError(CodeVersionConflict, "full snapshot version is not newer than committed version", map[string]any{
				"committed_version": currentVersion, "submitted_version": snapshot.SnapshotVersion,
			})
		}
	}

	snapshotIntent, err := s.store.CreateIntent(ctx, "snapshot_switch", itoa64(snapshot.SnapshotVersion), map[string]any{
		"version": snapshot.SnapshotVersion, "checksum": snapshot.Checksum, "kind": "full",
	})
	if err != nil {
		return nil, err
	}
	if s.opts.AfterSnapshotIntent != nil {
		s.opts.AfterSnapshotIntent(snapshotIntent.ID)
	}
	planIntent, err := s.store.CreateIntent(ctx, "plan_build", itoa64(snapshot.SnapshotVersion), map[string]any{"version": snapshot.SnapshotVersion})
	if err != nil {
		return nil, err
	}
	if s.opts.AfterPlanIntent != nil {
		s.opts.AfterPlanIntent(planIntent.ID)
	}

	view, buildErr := buildCommittedView(snapshot, nil)
	if view == nil {
		_ = s.store.UpdateIntentStatus(ctx, snapshotIntent.ID, "aborted")
		_ = s.store.UpdateIntentStatus(ctx, planIntent.ID, "aborted")
		return nil, buildErr
	}

	saveErr := s.store.SaveCommittedVersion(ctx, saveVersionParams{
		IntentIDs: []string{snapshotIntent.ID, planIntent.ID},
		Kind:      "full",
		Snapshot:  snapshot,
		Derived: &derivedRecord{
			DriftList: view.DriftList,
			Plan:      view.Plan,
			Summary:   view.Summary,
		},
	})
	if saveErr != nil {
		_ = s.store.UpdateIntentStatus(ctx, snapshotIntent.ID, "aborted")
		_ = s.store.UpdateIntentStatus(ctx, planIntent.ID, "aborted")
		return nil, saveErr
	}
	s.state.Store(view)
	result := &CommitResult{Version: snapshot.SnapshotVersion, Current: view, Plan: view.Plan}
	if buildErr != nil {
		return result, buildErr
	}
	return result, nil
}

func (s *Service) CommitDelta(ctx context.Context, delta *Delta) (*CommitResult, error) {
	s.writerMu.Lock()
	defer s.writerMu.Unlock()

	if delta == nil {
		return nil, NewError(CodeSpecInvalid, "delta is nil", nil)
	}
	if err := NormalizeDelta(delta); err != nil {
		return nil, err
	}
	current := s.state.Load()
	if current == nil {
		return nil, NewError(CodeNotFound, "no committed snapshot to apply delta", map[string]any{"base_version": delta.BaseVersion})
	}
	if delta.BaseVersion != current.Snapshot.SnapshotVersion {
		return nil, NewError(CodeStaleBase, "delta base_version is not the current committed version", map[string]any{
			"current_version": current.Snapshot.SnapshotVersion,
			"base_version":    delta.BaseVersion,
		})
	}

	merged, cascade, err := mergeDelta(current, delta, s.opts.Now)
	if err != nil {
		return nil, err
	}
	if err := NormalizeSnapshot(merged); err != nil {
		return nil, err
	}
	if err := PrepareSnapshot(merged); err != nil {
		return nil, err
	}

	snapshotIntent, err := s.store.CreateIntent(ctx, "snapshot_switch", itoa64(merged.SnapshotVersion), map[string]any{
		"version": merged.SnapshotVersion, "base_version": delta.BaseVersion, "kind": "delta",
	})
	if err != nil {
		return nil, err
	}
	planIntent, err := s.store.CreateIntent(ctx, "plan_build", itoa64(merged.SnapshotVersion), map[string]any{"version": merged.SnapshotVersion})
	if err != nil {
		return nil, err
	}

	view, buildErr := buildCommittedView(merged, cascade)
	if view == nil {
		_ = s.store.UpdateIntentStatus(ctx, snapshotIntent.ID, "aborted")
		_ = s.store.UpdateIntentStatus(ctx, planIntent.ID, "aborted")
		return nil, buildErr
	}
	cascade.InvalidatedBatchIDs = computeInvalidatedBatches(current, view, cascade.InvalidatedDriftIDs)
	view.Cascade = cascade
	view.Summary = computeSummary(merged, view.Plan, view.BatchStates)
	cascade.RebuiltObjectCount = len(view.DriftList)
	cascade.RebuiltBatchCount = len(view.Plan.Batches)

	if err := s.store.SaveCommittedVersion(ctx, saveVersionParams{
		IntentIDs: []string{snapshotIntent.ID, planIntent.ID},
		Kind:      "delta",
		Base:      delta.BaseVersion,
		Snapshot:  merged,
		Delta:     delta,
		Derived: &derivedRecord{
			DriftList: view.DriftList,
			Plan:      view.Plan,
			Summary:   view.Summary,
			Cascade:   cascade,
		},
	}); err != nil {
		_ = s.store.UpdateIntentStatus(ctx, snapshotIntent.ID, "aborted")
		_ = s.store.UpdateIntentStatus(ctx, planIntent.ID, "aborted")
		return nil, err
	}
	s.state.Store(view)
	return &CommitResult{Version: merged.SnapshotVersion, Current: view, Plan: view.Plan, Cascade: cascade}, buildErr
}

func verifyOrPrepareChecksums(snapshot *Snapshot) error {
	if stringsTrim(snapshot.Policy.Checksum) != "" {
		if err := snapshot.Policy.VerifyChecksum(); err != nil {
			return err
		}
	}
	if stringsTrim(snapshot.Checksum) != "" {
		if err := snapshot.VerifyChecksum(); err != nil {
			return err
		}
	}
	if snapshot.Policy.Checksum == "" || snapshot.Checksum == "" {
		return PrepareSnapshot(snapshot)
	}
	return nil
}

func mergeDelta(current *CommittedVersion, delta *Delta, now func() time.Time) (*Snapshot, *CascadeReport, error) {
	old := current.Snapshot
	objectMap := map[string]Object{}
	for _, object := range old.Objects {
		objectMap[object.ID] = object
	}
	changed := map[string]bool{}
	for _, object := range delta.Upserts {
		objectMap[object.ID] = object
		changed[object.ID] = true
	}
	for _, id := range delta.RemovedObjectIDs {
		if _, ok := objectMap[id]; !ok {
			return nil, nil, NewError(CodeSpecInvalid, "delta removes unknown object", map[string]any{"object_id": id})
		}
		delete(objectMap, id)
		changed[id] = true
	}

	ids := make([]string, 0, len(objectMap))
	for id := range objectMap {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	objects := make([]Object, 0, len(ids))
	for _, id := range ids {
		objects = append(objects, objectMap[id])
	}

	newVersion := delta.BaseVersion + 1
	if delta.Version > 0 {
		if delta.Version != newVersion {
			return nil, nil, NewError(CodeSpecInvalid, "delta version must equal base_version+1", map[string]any{
				"base_version": delta.BaseVersion, "expected_version": newVersion, "submitted_version": delta.Version,
			})
		}
		newVersion = delta.Version
	}
	generatedAt := delta.GeneratedAt
	if generatedAt.IsZero() {
		generatedAt = now().UTC()
	}
	merged := &Snapshot{
		SchemaVersion:   old.SchemaVersion,
		SnapshotVersion: newVersion,
		GeneratedAt:     generatedAt,
		Policy:          old.Policy,
		Clusters:        append([]Cluster(nil), old.Clusters...),
		Objects:         objects,
	}

	cascadeIDs := transitiveDependents(old.Objects, changed)
	sort.Strings(cascadeIDs)
	changedIDs := make([]string, 0, len(changed))
	for id := range changed {
		changedIDs = append(changedIDs, id)
	}
	sort.Strings(changedIDs)
	cascade := &CascadeReport{
		FromVersion:         old.SnapshotVersion,
		ToVersion:           newVersion,
		ChangedObjectIDs:    changedIDs,
		InvalidatedDriftIDs: cascadeIDs,
		Reason:              "affected object fields, dependency edges, resource budgets, or batch placement changed",
		AtomicBoundary:      "SQLite version/derived/intent transaction followed by one atomic.Pointer store",
	}
	return merged, cascade, nil
}

func transitiveDependents(objects []Object, changed map[string]bool) []string {
	reverse := map[string][]string{}
	for _, object := range objects {
		for _, dep := range object.DependsOn {
			reverse[dep] = append(reverse[dep], object.ID)
		}
	}
	result := map[string]bool{}
	queue := make([]string, 0, len(changed))
	for id := range changed {
		if !result[id] {
			result[id] = true
			queue = append(queue, id)
		}
	}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		for _, dependent := range reverse[id] {
			if !result[dependent] {
				result[dependent] = true
				queue = append(queue, dependent)
			}
		}
	}
	return setToSortedIDs(result)
}

func computeInvalidatedBatches(oldView, newView *CommittedVersion, cascadeObjectIDs []string) []string {
	objectSet := map[string]bool{}
	for _, id := range cascadeObjectIDs {
		objectSet[id] = true
	}
	invalidated := map[string]bool{}
	if oldView != nil && oldView.Plan != nil {
		for _, batch := range oldView.Plan.Batches {
			for _, id := range batch.ObjectIDs {
				if objectSet[id] {
					invalidated[batch.BatchID] = true
				}
			}
			if newBatch := findBatch(newView.Plan, batch.BatchID); newBatch != nil && newBatch.ContentHash != batch.ContentHash {
				invalidated[batch.BatchID] = true
			}
		}
	}
	if newView != nil && newView.Plan != nil {
		for _, batch := range newView.Plan.Batches {
			for _, id := range batch.ObjectIDs {
				if objectSet[id] {
					invalidated[batch.BatchID] = true
				}
			}
		}
	}
	return setToSortedIDs(invalidated)
}

func findBatch(plan *Plan, batchID string) *Batch {
	if plan == nil {
		return nil
	}
	for i := range plan.Batches {
		if plan.Batches[i].BatchID == batchID {
			return &plan.Batches[i]
		}
	}
	return nil
}

func itoa64(v int64) string {
	return strconv.FormatInt(v, 10)
}

func stringsTrim(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\n' || s[0] == '\t' || s[0] == '\r') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\n' || s[len(s)-1] == '\t' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}
