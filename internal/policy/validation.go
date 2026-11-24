package policy

import (
	"fmt"
	"strings"

	"example.com/replicabudget/internal/apperr"
)

func validateServiceID(id string) error {
	if id == "" || len(id) > 128 {
		return apperr.WithDetails(apperr.SpecInvalid, "service_id must be 1..128 bytes", map[string]any{"service_id": id})
	}
	for _, r := range id {
		if r <= 0x20 || r == 0x7f || r == '/' || r == '\\' {
			return apperr.WithDetails(apperr.SpecInvalid, "service_id contains a forbidden character", map[string]any{"service_id": id})
		}
	}
	return nil
}

func ValidateQuotaGroup(g QuotaGroup, index int) error {
	if err := validateServiceID(g.ID); err != nil {
		if be := apperr.As(err); be.RecordIndex == 0 {
			be.RecordIndex = index
		}
		return err
	}
	if g.VRAMBytes == 0 || g.VRAMBytes > uint64(^uint64(0)>>1) {
		return apperr.WithDetails(apperr.SpecInvalid, "quota vram_bytes out of range", map[string]any{"quota_group": g.ID})
	}
	if g.ComputeMilli == 0 || g.ComputeMilli > uint64(^uint64(0)>>1) {
		return apperr.WithDetails(apperr.SpecInvalid, "quota compute_milli out of range", map[string]any{"quota_group": g.ID})
	}
	return nil
}

func ValidateQuotaGroups(groups []QuotaGroup) error {
	seen := make(map[string]int, len(groups))
	for i, group := range groups {
		if err := ValidateQuotaGroup(group, i+1); err != nil {
			return err
		}
		if first, ok := seen[group.ID]; ok {
			return apperr.Record(apperr.SpecInvalid, "duplicate quota group id", i+1, map[string]any{
				"quota_group": group.ID, "first_record_index": first,
			})
		}
		seen[group.ID] = i + 1
	}
	if len(groups) == 0 {
		return apperr.New(apperr.SpecInvalid, "quota group batch is empty")
	}
	return nil
}

func ValidateStrategyBatch(strategies []NewStrategy, quotas []QuotaGroup) error {
	if len(strategies) == 0 {
		return apperr.New(apperr.SpecInvalid, "strategy batch is empty")
	}
	qmap := quotaMap(quotas)
	seen := make(map[string]int, len(strategies))
	for i, strategy := range strategies {
		if err := ValidateStrategy(strategy, qmap, i+1); err != nil {
			return err
		}
		if first, ok := seen[strategy.ServiceID]; ok {
			return apperr.Record(apperr.SpecInvalid, "duplicate service_id", i+1, map[string]any{
				"service_id": strategy.ServiceID, "first_record_index": first,
			})
		}
		seen[strategy.ServiceID] = i + 1
	}
	return nil
}

func ValidateStrategy(s NewStrategy, quotas map[string]QuotaGroup, index int) error {
	if err := validateServiceID(s.ServiceID); err != nil {
		if be := apperr.As(err); be.RecordIndex == 0 {
			be.RecordIndex = index
		}
		return err
	}
	if s.MinReplicas <= 0 || s.MinReplicas > 1_000_000 {
		return apperr.Record(apperr.SpecInvalid, "min_replicas must be in [1,1000000]", index, map[string]any{"service_id": s.ServiceID})
	}
	if s.MaxReplicas <= 0 || s.MaxReplicas > 1_000_000 {
		return apperr.Record(apperr.SpecInvalid, "max_replicas must be in [1,1000000]", index, map[string]any{"service_id": s.ServiceID})
	}
	if s.MinReplicas > s.MaxReplicas {
		return apperr.Record(apperr.SpecInvalid, "min_replicas must not exceed max_replicas", index, map[string]any{
			"service_id": s.ServiceID, "min_replicas": s.MinReplicas, "max_replicas": s.MaxReplicas,
		})
	}
	if s.TargetConcurrency <= 0 || s.TargetConcurrency > 10_000_000_000 {
		return apperr.Record(apperr.SpecInvalid, "target_concurrency out of range", index, map[string]any{"service_id": s.ServiceID})
	}
	if s.LatencyBudgetMS <= 0 || s.LatencyBudgetMS > 3_600_000 {
		return apperr.Record(apperr.SpecInvalid, "latency_budget_ms out of range", index, map[string]any{"service_id": s.ServiceID})
	}
	if strings.TrimSpace(s.QuotaGroupRef) == "" || len(s.QuotaGroupRef) > 128 {
		return apperr.Record(apperr.SpecInvalid, "quota_group_ref must be 1..128 bytes", index, map[string]any{"service_id": s.ServiceID})
	}
	if s.WeightPermille <= 0 || s.WeightPermille > 1_000_000 {
		return apperr.Record(apperr.SpecInvalid, "weight_permille out of range [1,1000000]", index, map[string]any{"service_id": s.ServiceID})
	}
	if s.VRAMBytesPerReplica > uint64(^uint64(0)>>1) || s.ComputeMilliPerReplica > uint64(^uint64(0)>>1) {
		return apperr.Record(apperr.SpecInvalid, "per-replica resource out of range", index, map[string]any{"service_id": s.ServiceID})
	}
	quota, ok := quotas[s.QuotaGroupRef]
	if !ok {
		return apperr.Record(apperr.SpecInvalid, "quota_group_ref does not exist in the bound snapshot", index, map[string]any{
			"service_id": s.ServiceID, "quota_group_ref": s.QuotaGroupRef,
		})
	}
	if s.VRAMBytesPerReplica > 0 && quota.VRAMBytes/s.VRAMBytesPerReplica < uint64(s.MinReplicas) {
		return apperr.Record(apperr.SpecInvalid, "quota cannot support min_replicas for vram", index, map[string]any{
			"service_id": s.ServiceID, "quota_group_ref": s.QuotaGroupRef,
			"min_replicas": s.MinReplicas, "vram_capacity_replicas": quota.VRAMBytes / s.VRAMBytesPerReplica,
		})
	}
	if s.ComputeMilliPerReplica > 0 && quota.ComputeMilli/s.ComputeMilliPerReplica < uint64(s.MinReplicas) {
		return apperr.Record(apperr.SpecInvalid, "quota cannot support min_replicas for compute", index, map[string]any{
			"service_id": s.ServiceID, "quota_group_ref": s.QuotaGroupRef,
			"min_replicas": s.MinReplicas, "compute_capacity_replicas": quota.ComputeMilli / s.ComputeMilliPerReplica,
		})
	}
	return nil
}

func resourceCapacity(s NewStrategy, q QuotaGroup) uint64 {
	cap := ^uint64(0)
	if s.VRAMBytesPerReplica > 0 {
		cap = min(cap, q.VRAMBytes/s.VRAMBytesPerReplica)
	}
	if s.ComputeMilliPerReplica > 0 {
		cap = min(cap, q.ComputeMilli/s.ComputeMilliPerReplica)
	}
	return cap
}

func (s NewStrategy) String() string {
	return fmt.Sprintf("%s[%d,%d]", s.ServiceID, s.MinReplicas, s.MaxReplicas)
}
