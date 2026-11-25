package feedbackstream

import (
	"context"
	"fmt"
)

type GroupStepResult struct {
	GroupID      string              `json:"group_id"`
	Status       string              `json:"status"`
	Late         bool                `json:"late,omitempty"`
	Output       *GroupOutput        `json:"output,omitempty"`
	Push         *PushBufferResult   `json:"push,omitempty"`
	Backpressure *BackpressureReport `json:"backpressure,omitempty"`
}

func (s *Service) admit(ctx context.Context, batch *Batch, group FusionGroup, policy *PolicySnapshot) (*PushBufferResult, error) {
	stall, watermarks, waiting, err := s.store.IsBackpressureStall(ctx, policy, mustPlan(policy))
	if err != nil {
		return nil, err
	}
	if stall {
		return nil, NewError(CodeBackpressureStall, "all fusion operators are backpressured and waiting queue is full").
			With("policy_version", policy.Version).
			With("waiting_batches", waiting).
			With("max_wait_queue", policy.MaxWaitQueue).
			With("watermarks", watermarks)
	}
	push, err := s.store.PushToBuffer(ctx, PushBufferRequest{
		PolicyVersion: policy.Version,
		GroupID:       group.ID,
		BatchID:       batch.ID,
		PartitionID:   batch.PartitionID,
		Bytes:         batch.ByteSize,
		Limit:         policy.BufferFor(group.ID),
	})
	if err != nil {
		return nil, err
	}
	return push, nil
}

func mustPlan(policy *PolicySnapshot) *FusionPlan {
	plan, err := PlanFusion(policy)
	if err != nil {
		// Policies admitted by the service are planned before activation.
		return &FusionPlan{}
	}
	return plan
}

func backpressureReport(push *PushBufferResult, policy *PolicySnapshot, groupID string) *BackpressureReport {
	if push == nil {
		return nil
	}
	return &BackpressureReport{
		Watermark:              push.Watermark,
		TriggerGroupID:         groupID,
		BlockedUpstreamBatches: push.BlockedBatchCount,
		WaitingQueueFull:       push.WaitingQueueCount >= policy.MaxWaitQueue,
	}
}

func (s *Service) advance(ctx context.Context, batchID string, policy *PolicySnapshot, plan *FusionPlan) (*BatchResult, error) {
	batch, err := s.store.GetBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	if batch == nil {
		return nil, NewError(CodeNotFound, "batch not found").With("batch_id", batchID)
	}
	startIndex := 0
	if batch.CurrentGroupID != "" {
		if group := plan.GroupByID(batch.CurrentGroupID); group != nil {
			startIndex = group.Index
		}
	}
	result := &BatchResult{BatchID: batch.ID, PartitionID: batch.PartitionID, Status: StatusRunning, Outputs: []GroupOutput{}}

	for i := startIndex; i < len(plan.Groups); i++ {
		group := plan.Groups[i]
		completed, err := s.store.GroupCompletionExists(ctx, policy.Version, batch.ID, group.ID, group.SignatureHash)
		if err != nil {
			return nil, err
		}
		if completed {
			output, err := s.store.GetGroupOutput(ctx, batch.ID, policy.Version, group.ID)
			if err != nil {
				return nil, err
			}
			if output != nil {
				result.Outputs = append(result.Outputs, *output)
			}
			if i == len(plan.Groups)-1 {
				result.Status = StatusCompleted
				return result, nil
			}
			next := plan.Groups[i+1]
			exists, err := s.store.InflightExists(ctx, policy.Version, next.ID, batch.ID)
			if err != nil {
				return nil, err
			}
			if !exists {
				push, err := s.admit(ctx, batch, next, policy)
				if err != nil {
					return nil, err
				}
				result.Backpressure = backpressureReport(push, policy, next.ID)
				if !push.Admitted {
					result.Status = StatusBlocked
					return result, nil
				}
			}
			continue
		}

		inflight, err := s.store.InflightExists(ctx, policy.Version, group.ID, batch.ID)
		if err != nil {
			return nil, err
		}
		if !inflight {
			push, err := s.admit(ctx, batch, group, policy)
			if err != nil {
				return nil, err
			}
			result.Backpressure = backpressureReport(push, policy, group.ID)
			if !push.Admitted {
				result.Status = StatusBlocked
				return result, nil
			}
		}

		step, err := s.executeAdmitted(ctx, batch, group, i, policy, plan)
		if err != nil {
			return nil, err
		}
		if step.Output != nil {
			result.Outputs = append(result.Outputs, *step.Output)
		}
		if step.Late {
			result.Status = StatusLateRecorded
			return result, nil
		}
		if i == len(plan.Groups)-1 {
			result.Status = StatusCompleted
			return result, nil
		}

		next := plan.Groups[i+1]
		nextInflight, err := s.store.InflightExists(ctx, policy.Version, next.ID, batch.ID)
		if err != nil {
			return nil, err
		}
		if !nextInflight {
			push, err := s.admit(ctx, batch, next, policy)
			if err != nil {
				return nil, err
			}
			result.Backpressure = backpressureReport(push, policy, next.ID)
			if !push.Admitted {
				result.Status = StatusBlocked
				return result, nil
			}
		}
	}
	result.Status = StatusCompleted
	return result, nil
}

func (s *Service) executeAdmitted(ctx context.Context, batch *Batch, group FusionGroup, index int, policy *PolicySnapshot, plan *FusionPlan) (*GroupStepResult, error) {
	var projected []ProjectedEvent
	if index > 0 {
		previous := plan.Groups[index-1]
		upstream, err := s.store.GetGroupOutput(ctx, batch.ID, policy.Version, previous.ID)
		if err != nil {
			return nil, err
		}
		if upstream == nil {
			return nil, NewError(CodeStateInvalid, "missing upstream fused-group output").
				With("batch_id", batch.ID).With("upstream_group_id", previous.ID)
		}
		projected = upstream.Projected
	}

	output, err := ExecuteGroup(group, policy, batch, projected)
	if err != nil {
		return nil, err
	}

	late := false
	for _, kind := range group.Kinds {
		if kind == "aggregate" {
			committed, err := s.store.IsWindowCommitted(ctx, policy.Version, group.ID, group.SignatureHash, batch.PartitionID, batch.WindowStart)
			if err != nil {
				return nil, err
			}
			if committed {
				late = true
				output.Aggregates = nil
			}
			break
		}
	}

	finalStatus := StatusRunning
	if late {
		finalStatus = StatusLateRecorded
	} else if index == len(plan.Groups)-1 {
		finalStatus = StatusCompleted
	}
	intentID := "intent-window-" + batch.ID + "-" + group.ID + "-v" + fmt.Sprint(policy.Version)
	if err := s.store.CommitGroupResult(ctx, GroupCommit{
		IntentID: intentID, PolicyVersion: policy.Version, BatchID: batch.ID,
		Group: group, Output: output, FinalStatus: finalStatus, SourceBatchID: batch.ID,
		Late: late, LateReason: "EVENT_WINDOW_ALREADY_COMMITTED_" + policy.Window.BoundaryRule,
	}); err != nil {
		return nil, err
	}
	status := StatusCompleted
	if late {
		status = StatusLateRecorded
	}
	return &GroupStepResult{GroupID: group.ID, Status: status, Output: &output}, nil
}

func (s *Service) AdmitGroup(ctx context.Context, batchID, groupID string) (*PushBufferResult, error) {
	batch, err := s.store.GetBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	if batch == nil {
		return nil, NewError(CodeNotFound, "batch not found").With("batch_id", batchID)
	}
	policy, plan, unlock, err := s.lockForBatch(ctx, batch.PartitionID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	group := plan.GroupByID(groupID)
	if group == nil {
		return nil, NewError(CodeNotFound, "fusion group not found").With("group_id", groupID)
	}
	return s.admit(ctx, batch, *group, policy)
}

func (s *Service) RunGroup(ctx context.Context, batchID, groupID string) (*GroupStepResult, error) {
	batch, err := s.store.GetBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	if batch == nil {
		return nil, NewError(CodeNotFound, "batch not found").With("batch_id", batchID)
	}
	policy, plan, unlock, err := s.lockForBatch(ctx, batch.PartitionID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	group := plan.GroupByID(groupID)
	if group == nil {
		return nil, NewError(CodeNotFound, "fusion group not found").With("group_id", groupID)
	}
	completed, err := s.store.GroupCompletionExists(ctx, policy.Version, batchID, groupID, group.SignatureHash)
	if err != nil {
		return nil, err
	}
	if completed {
		output, err := s.store.GetGroupOutput(ctx, batchID, policy.Version, groupID)
		return &GroupStepResult{GroupID: groupID, Status: StatusCompleted, Output: output}, err
	}
	inflight, err := s.store.InflightExists(ctx, policy.Version, groupID, batchID)
	if err != nil {
		return nil, err
	}
	if !inflight {
		push, err := s.admit(ctx, batch, *group, policy)
		if err != nil {
			return nil, err
		}
		if !push.Admitted {
			return &GroupStepResult{
				GroupID: groupID, Status: StatusBlocked, Push: push,
				Backpressure: backpressureReport(push, policy, groupID),
			}, nil
		}
	}
	return s.executeAdmitted(ctx, batch, *group, group.Index, policy, plan)
}

func (s *Service) ExecuteAdmittedGroup(ctx context.Context, batchID, groupID string) (*GroupStepResult, error) {
	batch, err := s.store.GetBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	if batch == nil {
		return nil, NewError(CodeNotFound, "batch not found").With("batch_id", batchID)
	}
	policy, plan, unlock, err := s.lockForBatch(ctx, batch.PartitionID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	group := plan.GroupByID(groupID)
	if group == nil {
		return nil, NewError(CodeNotFound, "fusion group not found").With("group_id", groupID)
	}
	return s.executeAdmitted(ctx, batch, *group, group.Index, policy, plan)
}

func (s *Service) lockForBatch(ctx context.Context, partition int) (*PolicySnapshot, *FusionPlan, func(), error) {
	s.policyMu.RLock()
	record, err := s.store.ActivePolicy(ctx)
	if err != nil {
		s.policyMu.RUnlock()
		return nil, nil, nil, err
	}
	if record == nil {
		s.policyMu.RUnlock()
		return nil, nil, nil, NewError(CodeNotFound, "no active policy")
	}
	plan, err := PlanFusion(record.Snapshot)
	if err != nil {
		s.policyMu.RUnlock()
		return nil, nil, nil, err
	}
	unlockPartition := s.partitions.Lock(partition)
	return record.Snapshot, plan, func() {
		unlockPartition()
		s.policyMu.RUnlock()
	}, nil
}
