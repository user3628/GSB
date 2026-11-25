package feedbackstream

import (
	"context"
	"encoding/json"
	"fmt"
)

func clonePolicy(policy *PolicySnapshot) (*PolicySnapshot, error) {
	data, err := json.Marshal(policy)
	if err != nil {
		return nil, err
	}
	var out PolicySnapshot
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *Service) ReplacePolicySnapshot(ctx context.Context, policy *PolicySnapshot) (*FusionPlan, error) {
	if err := NormalizeAndValidatePolicy(policy); err != nil {
		return nil, err
	}
	expected, err := PolicyChecksum(policy)
	if err != nil {
		return nil, err
	}
	if policy.ChecksumSHA256 == "" {
		policy.ChecksumSHA256 = expected
	} else if policy.ChecksumSHA256 != expected {
		return nil, SpecInvalid("policy checksum mismatch").With("version", policy.Version)
	}

	s.policyMu.Lock()
	defer s.policyMu.Unlock()
	active, err := s.store.ActivePolicy(ctx)
	if err != nil {
		return nil, err
	}
	if active != nil && policy.Version <= active.Snapshot.Version {
		return nil, NewError(CodePolicyVersion, "full policy version must be monotonically increasing").
			With("active_version", active.Snapshot.Version).With("submitted_version", policy.Version)
	}
	plan, err := PlanFusion(policy)
	if err != nil {
		return nil, err
	}
	baseVersion := int64(0)
	if active != nil {
		baseVersion = active.Snapshot.Version
	}
	rebuilds, err := s.rebuildCompletedBatches(ctx, policy, plan)
	if err != nil {
		return nil, err
	}
	affected := make([]string, 0, len(plan.Groups))
	for _, group := range plan.Groups {
		affected = append(affected, group.ID)
	}
	intentID := fmt.Sprintf("intent-policy-switch-v%d", policy.Version)
	if _, err := s.store.CreateIntent(ctx, IntentRecord{
		ID: intentID, Kind: IntentSwitchPolicy, AggregateID: fmt.Sprintf("policy-v%d", policy.Version),
		PayloadJSON: fmt.Sprintf(`{"base_version":%d,"new_version":%d,"mode":"FULL"}`, baseVersion, policy.Version),
	}); err != nil {
		return nil, err
	}
	if _, err := s.store.CommitPolicySwitch(ctx, PolicySwitch{
		IntentID: intentID, BaseVersion: baseVersion, NewPolicy: policy,
		AffectedGroups: affected, Rebuilds: rebuilds,
	}); err != nil {
		return nil, err
	}
	return plan, nil
}

func (s *Service) ApplyPolicyPatch(ctx context.Context, patch PolicyPatch) (*PolicySnapshot, *FusionPlan, error) {
	s.policyMu.Lock()
	defer s.policyMu.Unlock()
	activeRecord, err := s.store.ActivePolicy(ctx)
	if err != nil {
		return nil, nil, err
	}
	if activeRecord == nil {
		return nil, nil, NewError(CodeNotFound, "no active policy")
	}
	active := activeRecord.Snapshot
	if patch.BaseVersion != active.Version {
		return nil, nil, NewError(CodeStaleBase, "patch base_version is not the active committed version").
			With("active_version", active.Version).
			With("base_version", patch.BaseVersion)
	}
	oldPlan, err := PlanFusion(active)
	if err != nil {
		return nil, nil, err
	}
	next, err := clonePolicy(active)
	if err != nil {
		return nil, nil, err
	}
	next.Version = active.Version + 1
	next.Kind = "INCREMENTAL"
	if patch.GeneratedAtMS != 0 {
		next.GeneratedAtMS = patch.GeneratedAtMS
	} else {
		next.GeneratedAtMS = nowMS()
	}
	if patch.Reason != "" {
		next.Description = patch.Reason
	}

	groupIndexByOperator := map[string]int{}
	for _, group := range oldPlan.Groups {
		for _, opID := range group.OperatorIDs {
			groupIndexByOperator[opID] = group.Index
		}
	}
	direct := map[int]bool{}
	for _, opPatch := range patch.Operators {
		idx := indexOfOperatorID(next.Pipeline.Operators, opPatch.ID)
		if idx < 0 {
			return nil, nil, SpecInvalid("patch references unknown operator: " + opPatch.ID)
		}
		if opPatch.Config != nil {
			next.Pipeline.Operators[idx].Config = *opPatch.Config
		}
		if opPatch.Repartition != nil {
			next.Pipeline.Operators[idx].Repartition = *opPatch.Repartition
		}
		if groupIndex, ok := groupIndexByOperator[opPatch.ID]; ok {
			direct[groupIndex] = true
		}
	}
	if next.OperatorBuffers == nil {
		next.OperatorBuffers = map[string]BufferLimit{}
	}
	for key, buffer := range patch.Buffers {
		if buffer.HighWatermarkRatio == 0 {
			buffer.HighWatermarkRatio = active.BufferFor(key).HighWatermarkRatio
		}
		next.OperatorBuffers[key] = buffer
		if group := oldPlan.GroupByID(key); group != nil {
			direct[group.Index] = true
		} else if groupIndex, ok := groupIndexByOperator[key]; ok {
			direct[groupIndex] = true
		}
	}
	if patch.PartitionConcurrency != nil {
		next.PartitionConcurrency = *patch.PartitionConcurrency
	}
	if patch.RoundTimeBudgetMS != nil {
		next.RoundTimeBudgetMS = *patch.RoundTimeBudgetMS
	}

	next.ChecksumSHA256 = ""
	if err := NormalizeAndValidatePolicy(next); err != nil {
		return nil, nil, err
	}
	newPlan, err := PlanFusion(next)
	if err != nil {
		return nil, nil, err
	}

	affectedIndexes := map[int]bool{}
	for idx := range direct {
		for downstream := idx; downstream < len(newPlan.Groups); downstream++ {
			affectedIndexes[downstream] = true
		}
	}
	affectedGroups := make([]string, 0, len(affectedIndexes))
	for idx := range affectedIndexes {
		affectedGroups = append(affectedGroups, newPlan.Groups[idx].ID)
	}

	rebuilds, err := s.rebuildCompletedBatches(ctx, next, newPlan)
	if err != nil {
		return nil, nil, err
	}
	intentID := fmt.Sprintf("intent-policy-switch-v%d", next.Version)
	if _, err := s.store.CreateIntent(ctx, IntentRecord{
		ID: intentID, Kind: IntentSwitchPolicy, AggregateID: fmt.Sprintf("policy-v%d", next.Version),
		PayloadJSON: fmt.Sprintf(`{"base_version":%d,"new_version":%d,"mode":"INCREMENTAL","affected_groups":%v}`,
			patch.BaseVersion, next.Version, jsonStringSlice(affectedGroups)),
	}); err != nil {
		return nil, nil, err
	}
	if _, err := s.store.CommitPolicySwitch(ctx, PolicySwitch{
		IntentID: intentID, BaseVersion: patch.BaseVersion, NewPolicy: next,
		AffectedGroups: affectedGroups, Rebuilds: rebuilds,
	}); err != nil {
		return nil, nil, err
	}
	return next, newPlan, nil
}

func jsonStringSlice(items []string) string {
	data, _ := json.Marshal(items)
	return string(data)
}

func (s *Service) rebuildCompletedBatches(ctx context.Context, policy *PolicySnapshot, plan *FusionPlan) ([]GroupCommit, error) {
	batches, err := s.store.ListBatchesByStatus(ctx, StatusCompleted)
	if err != nil {
		return nil, err
	}
	rebuilds := make([]GroupCommit, 0, len(batches)*len(plan.Groups))
	for _, batch := range batches {
		var projected []ProjectedEvent
		for _, group := range plan.Groups {
			output, err := ExecuteGroup(group, policy, batch, projected)
			if err != nil {
				return nil, err
			}
			rebuilds = append(rebuilds, GroupCommit{
				PolicyVersion: policy.Version, BatchID: batch.ID, Group: group,
				Output: output, SourceBatchID: batch.ID,
			})
			projected = output.Projected
		}
	}
	return rebuilds, nil
}
