package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"recoveryengine/internal/fixed"
	"recoveryengine/internal/graph"
	"recoveryengine/internal/model"
	"recoveryengine/internal/spec"
	"recoveryengine/internal/store"
)

func (e *Engine) CommitSnapshot(ctx context.Context, key string, snapshot model.Snapshot, options CommitOptions) (*CommitResult, *model.BusinessError) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	submission, err := e.prepareSubmission(ctx, key, "snapshot", snapshot)
	if err != nil {
		return nil, err
	}
	if result, replayErr := e.replayCommit(submission); result != nil || replayErr != nil {
		return result, replayErr
	}
	current := e.current()
	intentID, intentErr := e.beginIntent(ctx, key, "snapshot", current.snapshot.Version, snapshot.Version, snapshot, options)
	if intentErr != nil {
		return nil, e.rejectOperation(ctx, key, "", intentErr.Status, intentErr)
	}
	if snapshot.Version <= current.snapshot.Version {
		business := model.NewError(model.CodeStaleVersion, "snapshot version must be strictly greater than the current version", 409).
			WithContext("current_version", current.snapshot.Version).
			WithContext("submitted_version", snapshot.Version)
		return nil, e.rejectOperation(ctx, key, intentID, business.Status, business)
	}

	normalized, business := spec.NormalizeSnapshot(snapshot, true)
	if business != nil {
		return nil, e.rejectOperation(ctx, key, intentID, business.Status, business)
	}
	candidate := normalized.Snapshot
	analysis := graph.Analyze(candidate)
	if analysis.HasCycle {
		business := cycleError(analysis, map[string]any{
			"candidate_version":    candidate.Version,
			"base_version":         current.snapshot.Version,
			"duplicate_components": normalized.Duplicates,
		})
		return nil, e.rejectOperation(ctx, key, intentID, business.Status, business)
	}
	consistency := fixed.Compute(candidate)
	refs := buildDerivedRefs(candidate, analysis, consistency)
	digest := derivedDigest(candidate, analysis, consistency, refs)
	changedRefs := []spec.Ref{{Kind: "snapshot", Key: fmt.Sprintf("%d", candidate.Version), Field: "all"}}
	rebuild := store.RebuildRecord{
		Trigger: map[string]any{
			"kind": "snapshot", "version": candidate.Version, "base_version": current.snapshot.Version,
		},
		ChangedRefs: changedRefs,
		Invalidated: invalidatedViews(current.refs, changedRefs),
		Rebuilt:     derivedViews(refs),
		Strategy:    "full_derived_rebuild_in_single_transaction",
		Status:      "ready",
	}
	now := store.Now()
	result := &CommitResult{
		Version: candidate.Version, BaseVersion: current.snapshot.Version, GeneratedAt: candidate.GeneratedAt,
		ChecksumSHA256: candidate.ChecksumSHA256, DuplicateComponents: normalized.Duplicates,
		Cascade: rebuild, Diagnostics: analysis.Diagnostics, Consistency: consistency,
		GraphDigest: analysis.GraphDigest, DerivedDigest: digest, IntentID: intentID, CommittedAt: now,
	}
	responseJSON, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		business := model.NewError(model.CodeInternal, "failed to encode snapshot result", 500)
		return nil, e.rejectOperation(ctx, key, intentID, business.Status, business)
	}
	if err := e.store.CommitVersion(ctx, store.VersionCommit{
		ExpectedCurrent: current.snapshot.Version, Mode: "snapshot", Snapshot: candidate,
		Duplicates: normalized.Duplicates, Analysis: analysis, Consistency: consistency,
		Refs: refs, Rebuild: rebuild, DerivedDigest: digest, SubmissionKey: key, IntentID: intentID,
		HTTPStatus: 200, ResponseJSON: responseJSON, CreatedAt: now,
	}); err != nil {
		business := e.conflict(err)
		return nil, e.rejectOperation(ctx, key, intentID, business.Status, business)
	}
	e.publish(candidate, normalized.Duplicates, analysis, consistency, refs, digest)
	return result, nil
}

func (e *Engine) CommitIncrement(ctx context.Context, request model.IncrementRequest, options CommitOptions) (*CommitResult, *model.BusinessError) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	submission, err := e.prepareSubmission(ctx, request.IdempotencyKey, "increment", request)
	if err != nil {
		return nil, err
	}
	if result, replayErr := e.replayCommit(submission); result != nil || replayErr != nil {
		return result, replayErr
	}
	current := e.current()
	targetVersion := current.snapshot.Version + 1
	intentID, intentErr := e.beginIntent(ctx, request.IdempotencyKey, "increment", request.BaseVersion, targetVersion, request, options)
	if intentErr != nil {
		return nil, e.rejectOperation(ctx, request.IdempotencyKey, "", intentErr.Status, intentErr)
	}
	if request.BaseVersion != current.snapshot.Version {
		business := model.NewError(model.CodeStaleBase, "increment base_version is not the current committed version", 409).
			WithContext("base_version", request.BaseVersion).
			WithContext("current_version", current.snapshot.Version)
		return nil, e.rejectOperation(ctx, request.IdempotencyKey, intentID, business.Status, business)
	}
	candidate, changedRefs, business := spec.ApplyIncrement(current.snapshot, request, targetVersion)
	if business != nil {
		return nil, e.rejectOperation(ctx, request.IdempotencyKey, intentID, business.Status, business)
	}
	analysis := graph.Analyze(candidate)
	if analysis.HasCycle {
		business := cycleError(analysis, map[string]any{
			"candidate_version": candidate.Version,
			"base_version":      current.snapshot.Version,
			"changed_refs":      changedRefs,
		})
		return nil, e.rejectOperation(ctx, request.IdempotencyKey, intentID, business.Status, business)
	}
	consistency := fixed.Compute(candidate)
	refs := buildDerivedRefs(candidate, analysis, consistency)
	digest := derivedDigest(candidate, analysis, consistency, refs)
	rebuild := store.RebuildRecord{
		Trigger: map[string]any{
			"kind": "increment", "base_version": current.snapshot.Version, "version": candidate.Version,
			"note": request.Note, "operations": request.Operations,
		},
		ChangedRefs: changedRefs,
		Invalidated: invalidatedViews(current.refs, changedRefs),
		Rebuilt:     derivedViews(refs),
		Strategy:    "full_derived_rebuild_in_single_transaction",
		Status:      "ready",
	}
	now := store.Now()
	result := &CommitResult{
		Version: candidate.Version, BaseVersion: current.snapshot.Version, GeneratedAt: candidate.GeneratedAt,
		ChecksumSHA256: candidate.ChecksumSHA256, Cascade: rebuild,
		Diagnostics: analysis.Diagnostics, Consistency: consistency, GraphDigest: analysis.GraphDigest,
		DerivedDigest: digest, IntentID: intentID, CommittedAt: now,
	}
	responseJSON, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		business := model.NewError(model.CodeInternal, "failed to encode increment result", 500)
		return nil, e.rejectOperation(ctx, request.IdempotencyKey, intentID, business.Status, business)
	}
	if err := e.store.CommitVersion(ctx, store.VersionCommit{
		ExpectedCurrent: current.snapshot.Version, Mode: "increment", Snapshot: candidate,
		Analysis: analysis, Consistency: consistency, Refs: refs, Rebuild: rebuild,
		DerivedDigest: digest, SubmissionKey: request.IdempotencyKey, IntentID: intentID,
		HTTPStatus: 200, ResponseJSON: responseJSON, CreatedAt: now,
	}); err != nil {
		business := e.conflict(err)
		return nil, e.rejectOperation(ctx, request.IdempotencyKey, intentID, business.Status, business)
	}
	e.publish(candidate, nil, analysis, consistency, refs, digest)
	return result, nil
}

func (e *Engine) replayCommit(submission store.Submission) (*CommitResult, *model.BusinessError) {
	switch submission.State {
	case "committed":
		var result CommitResult
		if err := json.Unmarshal(submission.ResponseJSON, &result); err != nil {
			return nil, model.NewError(model.CodeInternal, "stored idempotent response is unreadable", 500)
		}
		result.Replayed = true
		return &result, nil
	case "rejected":
		return nil, e.replayError(submission)
	default:
		return nil, nil
	}
}

func cycleError(analysis *graph.Analysis, context map[string]any) *model.BusinessError {
	cycles := make([]map[string]any, 0, len(analysis.Cycles))
	blocked := make([]string, 0, len(analysis.Blocked))
	for _, cycle := range analysis.Cycles {
		cycles = append(cycles, map[string]any{"id": cycle.ID, "components": cycle.Components})
	}
	for component := range analysis.Blocked {
		blocked = append(blocked, component)
	}
	sort.Strings(blocked)
	if context == nil {
		context = map[string]any{}
	}
	context["cycles"] = cycles
	context["blocked_components"] = blocked
	context["partial_result_available"] = true
	partial := map[string]any{
		"graph": map[string]any{
			"nodes": analysis.Nodes, "topology_batches": analysis.TopologyBatches,
			"batch_groups": analysis.BatchGroups, "blocked": analysis.Blocked,
			"cycles": analysis.Cycles, "diagnostics": analysis.Diagnostics,
			"impacts": analysis.Impacts, "graph_digest": analysis.GraphDigest,
		},
	}
	business := model.NewError(model.CodeDependencyCycle, "dependency graph contains a cycle; candidate was not committed", 422).
		WithPartial(partial)
	business.Context = context
	return business
}
