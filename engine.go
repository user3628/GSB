package prefixcache

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

type commitSpec struct {
	NextPolicy           Policy
	Kind                 string
	Replacement          bool
	EarliestStage        Stage
	Field                string
	OldValue             string
	NewValue             string
	Requests             map[string]Request
	ReplaceRequests      bool
	InvalidateRequestIDs []string
	ReuseSlots           bool
}

type commitBuilder func(head headState, currentPolicy *Policy, currentRequests map[string]Request) (commitSpec, error)

// Initialize creates version 1 atomically from a policy snapshot and request
// set. It is rejected if the database already has an effective version.
func (e *Engine) Initialize(ctx context.Context, policy Policy, requests []Request) (*CommitResult, error) {
	zero := int64(0)
	return e.commit(ctx, &zero, func(head headState, _ *Policy, _ map[string]Request) (commitSpec, error) {
		if policy.Version != 1 {
			return commitSpec{}, newError(CodeInvalidPolicy, "bootstrap policy version must be 1")
		}
		requestMap, err := requestMapFromSlice(requests)
		if err != nil {
			return commitSpec{}, err
		}
		return commitSpec{
			NextPolicy: policy,
			Kind:       "bootstrap",
			Requests:   requestMap,
			ReuseSlots: false,
		}, nil
	})
}

// ApplyPatch atomically applies one policy field and advances by exactly one
// version. The prior slot map is carried forward only when the earliest
// invalidated stage is hit adjudication.
func (e *Engine) ApplyPatch(ctx context.Context, expectedVersion int64, patch PolicyPatch) (*CommitResult, error) {
	return e.commit(ctx, &expectedVersion, func(head headState, currentPolicy *Policy, currentRequests map[string]Request) (commitSpec, error) {
		if currentPolicy == nil {
			return commitSpec{}, staleError(expectedVersion, 0, "no policy exists to patch")
		}
		next, field, err := patch.Apply(*currentPolicy)
		if err != nil {
			return commitSpec{}, err
		}
		stage, _, oldValue, newValue := earliestAffectedStage(*currentPolicy, next, false)
		return commitSpec{
			NextPolicy:    next,
			Kind:          "advance",
			EarliestStage: stage,
			Field:         field,
			OldValue:      oldValue,
			NewValue:      newValue,
			Requests:      currentRequests,
			ReuseSlots:    !stageInvalidatesSlotting(stage),
		}, nil
	})
}

// SubmitPolicy imports a complete policy snapshot. Matching BaseDigest is a
// normal advance; a mismatch is an explicit whole replacement and invalidates
// every slot. Lower or duplicate versions are rejected as stale.
func (e *Engine) SubmitPolicy(ctx context.Context, submission PolicySubmission) (*CommitResult, error) {
	return e.commit(ctx, submission.ExpectedVersion, func(head headState, currentPolicy *Policy, currentRequests map[string]Request) (commitSpec, error) {
		next := submission.Policy
		if head.Version == 0 {
			if next.Version != 1 {
				return commitSpec{}, staleError(next.Version, head.Version, "the first policy version must be 1")
			}
			if err := next.Validate(); err != nil {
				return commitSpec{}, err
			}
			return commitSpec{
				NextPolicy: next,
				Kind:       "bootstrap",
				Requests:   currentRequests,
			}, nil
		}
		if currentPolicy == nil {
			return commitSpec{}, newError(CodeInternal, "current policy metadata is missing")
		}
		if next.Version <= head.Version {
			return commitSpec{}, staleError(next.Version, head.Version, "policy version rollback or duplicate is rejected")
		}
		if next.Version != head.Version+1 {
			return commitSpec{}, staleError(next.Version, head.Version, "policy versions must advance contiguously by one")
		}
		if err := next.Validate(); err != nil {
			return commitSpec{}, err
		}

		mode := strings.ToLower(strings.TrimSpace(submission.Mode))
		if mode == "" {
			mode = "auto"
		}
		replacement := false
		switch mode {
		case "replace":
			replacement = true
		case "advance":
			if submission.BaseDigest == "" || submission.BaseDigest != head.PolicyDigest {
				err := staleError(next.Version, head.Version, "base_digest does not identify the current effective policy")
				err.Field = "base_digest"
				return commitSpec{}, err
			}
		case "auto":
			replacement = submission.BaseDigest != head.PolicyDigest
		default:
			return commitSpec{}, newError(CodeInvalidPolicy, "mode must be auto, advance, or replace")
		}

		stage, field, oldValue, newValue := earliestAffectedStage(*currentPolicy, next, replacement)
		if replacement {
			field = "*"
			oldValue = head.PolicyDigest
			newValue = PolicyContentDigest(next)
		}
		kind := "advance"
		if replacement {
			kind = "replacement"
		}
		return commitSpec{
			NextPolicy:    next,
			Kind:          kind,
			Replacement:   replacement,
			EarliestStage: stage,
			Field:         field,
			OldValue:      oldValue,
			NewValue:      newValue,
			Requests:      currentRequests,
			ReuseSlots:    !replacement && !stageInvalidatesSlotting(stage),
		}, nil
	})
}

// ImportRequests atomically upserts (or replaces) request records. Existing
// prefixes whose fingerprints are unchanged reuse their prior slots.
func (e *Engine) ImportRequests(ctx context.Context, expectedVersion int64, requests []Request, replaceAll bool) (*CommitResult, error) {
	return e.commit(ctx, &expectedVersion, func(head headState, currentPolicy *Policy, currentRequests map[string]Request) (commitSpec, error) {
		if currentPolicy == nil {
			return commitSpec{}, staleError(expectedVersion, 0, "initialize a policy before importing requests")
		}
		incoming, err := requestMapFromSlice(requests)
		if err != nil {
			return commitSpec{}, err
		}
		nextRequests := make(map[string]Request)
		if !replaceAll {
			for id, request := range currentRequests {
				nextRequests[id] = request
			}
		}
		changed := make([]string, 0)
		for id, request := range incoming {
			previous, existed := currentRequests[id]
			if !existed || !reflect.DeepEqual(previous, request) {
				changed = append(changed, id)
			}
			nextRequests[id] = request
		}
		if replaceAll {
			for id := range currentRequests {
				if _, exists := incoming[id]; !exists {
					changed = append(changed, id)
				}
			}
		}
		if len(changed) == 0 {
			return commitSpec{}, newError(CodeInvalidRequest, "request import contains no effective changes")
		}
		sort.Strings(changed)
		nextPolicy := currentPolicy.withVersion(head.Version + 1)
		return commitSpec{
			NextPolicy:           nextPolicy,
			Kind:                 "reallocate",
			EarliestStage:        StageSlotting,
			Field:                "request_set",
			OldValue:             fmt.Sprintf("version_%d_requests", head.Version),
			NewValue:             fmt.Sprintf("version_%d_requests", nextPolicy.Version),
			Requests:             nextRequests,
			ReplaceRequests:      replaceAll,
			InvalidateRequestIDs: changed,
			ReuseSlots:           true,
		}, nil
	})
}

func (e *Engine) commit(ctx context.Context, expectedVersion *int64, builder commitBuilder) (*CommitResult, error) {
	var result *CommitResult
	err := e.withWriteTransaction(ctx, func(tx *sql.Tx) error {
		head, err := loadHead(tx)
		if err != nil {
			return err
		}
		if expectedVersion != nil && *expectedVersion != head.Version {
			return staleError(*expectedVersion, head.Version, "submission expected version is not the current effective version")
		}

		var currentPolicy *Policy
		if head.Version > 0 {
			row, err := loadPolicyRow(tx, head.Version)
			if err != nil {
				return err
			}
			policyCopy := row.Policy
			currentPolicy = &policyCopy
			if row.PolicyDigest != head.PolicyDigest {
				return newError(CodeInternal, "cache head policy digest disagrees with the current version row")
			}
		}
		currentRequests, err := loadRequests(tx)
		if err != nil {
			return err
		}

		spec, err := builder(head, currentPolicy, currentRequests)
		if err != nil {
			return err
		}
		if spec.Requests == nil {
			spec.Requests = currentRequests
		}
		if err := spec.NextPolicy.Validate(); err != nil {
			return err
		}
		if spec.NextPolicy.Version != head.Version+1 {
			return staleError(spec.NextPolicy.Version, head.Version, "next version must equal current version plus one")
		}

		plans, requestIDs, err := buildPlans(spec.Requests, spec.NextPolicy)
		if err != nil {
			return err
		}

		var priorSlots []Slot
		if spec.ReuseSlots && head.Version > 0 {
			priorSlots, err = loadSlots(tx, head.Version)
			if err != nil {
				return err
			}
		}
		allocation, err := AllocateSlots(spec.NextPolicy.Version, head.AllocationSequence+1, spec.NextPolicy, plans, priorSlots, spec.ReuseSlots)
		if err != nil {
			return err
		}

		stageDigests := StageDigestsFor(spec.NextPolicy)
		policyDigest := PolicyContentDigest(spec.NextPolicy)
		digestOfSlots := slotDigest(allocation.Slots)
		invalidations := buildInvalidations(spec, head, requestIDs)
		states := buildStates(spec.NextPolicy, allocation, requestIDs, plans, spec.Requests)

		createdAt := e.options.now().UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
		policyJSON, err := json.Marshal(spec.NextPolicy)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO policy_versions(
				version, policy_json, policy_digest, parent_version, parent_digest,
				stage_normalization, stage_fingerprint, stage_slotting, stage_hit,
				version_fingerprint, slot_digest, allocation_sequence, kind, created_at
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			spec.NextPolicy.Version, string(policyJSON), policyDigest, head.Version, head.PolicyDigest,
			stageDigests.Normalization, stageDigests.Fingerprint, stageDigests.Slotting, stageDigests.Hit,
			stageDigests.Version, digestOfSlots, head.AllocationSequence+1, spec.Kind, createdAt,
		)
		if err != nil {
			return fmt.Errorf("write policy version: %w", err)
		}

		if spec.ReplaceRequests {
			if _, err := tx.ExecContext(ctx, `DELETE FROM requests`); err != nil {
				return fmt.Errorf("clear request set: %w", err)
			}
		}
		requestIDsForPersistence := make([]string, 0, len(spec.Requests))
		for id := range spec.Requests {
			requestIDsForPersistence = append(requestIDsForPersistence, id)
		}
		sort.Strings(requestIDsForPersistence)
		for _, id := range requestIDsForPersistence {
			body, err := json.Marshal(spec.Requests[id])
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx,
				`INSERT INTO requests(id, template_id, body_json, updated_version) VALUES(?,?,?,?)
				 ON CONFLICT(id) DO UPDATE SET template_id=excluded.template_id, body_json=excluded.body_json, updated_version=excluded.updated_version`,
				id, spec.Requests[id].TemplateID, string(body), spec.NextPolicy.Version,
			)
			if err != nil {
				return fmt.Errorf("upsert request %s: %w", id, err)
			}
		}

		for _, slot := range allocation.Slots {
			ownersJSON, _ := json.Marshal(slot.Owners)
			fingerprintsJSON, _ := json.Marshal(slot.BlockFingerprints)
			_, err = tx.ExecContext(ctx,
				`INSERT INTO slots(version, slot_no, prefix_key, prefix_chars, block_count, occupied_at, reused_from, owners_json, fingerprints_json)
				 VALUES(?,?,?,?,?,?,?,?,?)`,
				slot.Version, slot.SlotNo, slot.PrefixKey, slot.PrefixChars, slot.BlockCount, slot.OccupiedAt, slot.ReusedFrom,
				string(ownersJSON), string(fingerprintsJSON),
			)
			if err != nil {
				return fmt.Errorf("write slot %d: %w", slot.SlotNo, err)
			}
		}

		for _, state := range states {
			body, err := json.Marshal(state)
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx,
				`INSERT INTO request_states(version, request_id, state_json) VALUES(?,?,?)`,
				spec.NextPolicy.Version, state.Request.ID, string(body),
			)
			if err != nil {
				return fmt.Errorf("write request state %s: %w", state.Request.ID, err)
			}
		}

		for _, invalidation := range invalidations {
			_, err = tx.ExecContext(ctx,
				`INSERT INTO invalidations(version, of_version, field, stage, request_id, reason, old_value, new_value)
				 VALUES(?,?,?,?,?,?,?,?)`,
				invalidation.Version, invalidation.OfVersion, invalidation.Field, invalidation.Stage,
				invalidation.RequestID, invalidation.Reason, invalidation.OldValue, invalidation.NewValue,
			)
			if err != nil {
				return fmt.Errorf("write invalidation: %w", err)
			}
		}

		for _, eviction := range allocation.Evictions {
			_, err = tx.ExecContext(ctx,
				`INSERT INTO evictions(
					version, request_id, slot_no, prefix_key, prefix_chars, reason,
					winner_prefix_key, winner_request_id, similarity, threshold,
					threshold_margin, occupied_at, winner_occupied_at, note
				) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
				eviction.Version, eviction.RequestID, eviction.SlotNo, eviction.PrefixKey, eviction.PrefixChars,
				eviction.Reason, eviction.WinnerPrefixKey, eviction.WinnerRequestID, eviction.Similarity,
				eviction.Threshold, eviction.ThresholdMargin, eviction.OccupiedAt, eviction.WinnerOccupiedAt, eviction.Note,
			)
			if err != nil {
				return fmt.Errorf("write eviction: %w", err)
			}
		}

		update, err := tx.ExecContext(ctx,
			`UPDATE cache_meta
			    SET current_version=?, current_policy_digest=?, allocation_sequence=?
			  WHERE key='head' AND current_version=? AND current_policy_digest=? AND allocation_sequence=?`,
			spec.NextPolicy.Version, policyDigest, head.AllocationSequence+1,
			head.Version, head.PolicyDigest, head.AllocationSequence,
		)
		if err != nil {
			return fmt.Errorf("conditional cache head update: %w", err)
		}
		changed, err := update.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return staleError(spec.NextPolicy.Version, head.Version, "conditional cache head update lost a concurrent race")
		}

		e.beforeCommitMu.Lock()
		hook := e.beforeCommit
		e.beforeCommitMu.Unlock()
		if hook != nil {
			if err := hook(commitInfo{Version: spec.NextPolicy.Version, Kind: spec.Kind}); err != nil {
				return &Error{Code: CodeInterrupted, Message: "commit interrupted before SQLite commit; transaction will roll back", Cause: err}
			}
		}

		result = &CommitResult{
			Version:            spec.NextPolicy.Version,
			Kind:               spec.Kind,
			PolicyDigest:       policyDigest,
			VersionFingerprint: stageDigests.Version,
			StageDigests:       stageDigests,
			SlotDigest:         digestOfSlots,
			AllocationSequence: head.AllocationSequence + 1,
			InvalidatedStage:   spec.EarliestStage,
			Invalidations:      invalidations,
			Evictions:          allocation.Evictions,
			SlotCount:          len(allocation.Slots),
			ReusedSlotCount:    allocation.ReusedCount,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func buildPlans(requests map[string]Request, policy Policy) ([]RequestPlan, []string, error) {
	requestIDs := make([]string, 0, len(requests))
	for id := range requests {
		requestIDs = append(requestIDs, id)
	}
	sort.Strings(requestIDs)
	plans := make([]RequestPlan, 0, len(requestIDs))
	for _, id := range requestIDs {
		plan, err := BuildRequestPlan(requests[id], policy)
		if err != nil {
			return nil, nil, err
		}
		plans = append(plans, plan)
	}
	return plans, requestIDs, nil
}

func buildStates(policy Policy, allocation allocationResult, requestIDs []string, plans []RequestPlan, requests map[string]Request) []RequestState {
	planByID := make(map[string]RequestPlan, len(plans))
	for _, plan := range plans {
		planByID[plan.RequestID] = plan
	}
	buildByID := make(map[string]requestBuild, len(allocation.Builds))
	for _, build := range allocation.Builds {
		buildByID[build.plan.RequestID] = build
	}
	states := make([]RequestState, 0, len(requestIDs))
	for _, requestID := range requestIDs {
		plan := planByID[requestID]
		build := buildByID[requestID]
		// Request bodies are loaded by callers through the snapshot state. The
		// commit path fills this field from the builder request map below.
		states = append(states, RequestState{
			Version:     policy.Version,
			Request:     requests[requestID],
			Plan:        plan,
			Allocations: build.allocations,
			Hit:         DecideHit(policy.Version, policy.Version, requestID, plan, policy, allocation.Slots),
		})
	}
	return states
}

func buildInvalidations(spec commitSpec, head headState, requestIDs []string) []Invalidation {
	if head.Version == 0 || spec.EarliestStage == "" {
		return nil
	}
	targets := spec.InvalidateRequestIDs
	if len(targets) == 0 {
		targets = requestIDs
	}
	if len(targets) == 0 {
		targets = []string{"*"}
	}
	targets = append([]string(nil), targets...)
	sort.Strings(targets)
	reason := "stage and downstream invalidation"
	if spec.Replacement {
		reason = "whole policy replacement invalidates all slots and hit conclusions"
	} else if spec.Field == "request_set" {
		reason = "request content changed; affected prefixes are reallocated"
	}
	out := make([]Invalidation, 0, len(targets))
	for _, requestID := range targets {
		out = append(out, Invalidation{
			Version:   spec.NextPolicy.Version,
			OfVersion: head.Version,
			Field:     spec.Field,
			Stage:     spec.EarliestStage,
			RequestID: requestID,
			Reason:    reason,
			OldValue:  spec.OldValue,
			NewValue:  spec.NewValue,
		})
	}
	return out
}

func requestMapFromSlice(requests []Request) (map[string]Request, error) {
	out := make(map[string]Request, len(requests))
	for _, request := range requests {
		if err := ValidateRequest(request); err != nil {
			return nil, err
		}
		if _, exists := out[request.ID]; exists {
			return nil, invalidRequestError(request.ID, "duplicate request id")
		}
		out[request.ID] = request
	}
	return out, nil
}
