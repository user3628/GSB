package batch

import (
	"fmt"
	"sort"

	"driftengine/internal/errs"
	"driftengine/internal/spec"
)

type ObjectRef struct {
	ObjectID     string `json:"object_id"`
	Units        int64  `json:"units"`
	ObjectDigest string `json:"object_digest"`
}

type DependencyRef struct {
	ObjectID             string `json:"object_id"`
	DependsOnID          string `json:"depends_on_id"`
	EdgeID               string `json:"edge_id"`
	EdgeDigest           string `json:"edge_digest"`
	DependsOnBatchNumber int    `json:"depends_on_batch_number,omitempty"`
	DependsOnBatchID     string `json:"depends_on_batch_id,omitempty"`
	BlockedByCycle       bool   `json:"blocked_by_cycle,omitempty"`
}

type BudgetUse struct {
	OccupiedUnits int64 `json:"occupied_units"`
	LimitUnits    int64 `json:"limit_units"`
	CPUMillis     int64 `json:"cpu_millis,omitempty"`
	MemoryMiB     int64 `json:"memory_mib,omitempty"`
}

type Batch struct {
	BatchNumber    int             `json:"batch_number"`
	BatchID        string          `json:"batch_id"`
	Layer          int             `json:"layer"`
	ObjectIDs      []string        `json:"object_ids"`
	ObjectRefs     []ObjectRef     `json:"object_refs"`
	DependencyRefs []DependencyRef `json:"dependency_refs"`
	Budget         BudgetUse       `json:"budget"`
	SplitReason    string          `json:"split_reason,omitempty"`
	BudgetOverflow bool            `json:"budget_overflow,omitempty"`
}

type Plan struct {
	Version             int64      `json:"version"`
	DependencyOrderRule string     `json:"dependency_order_rule"`
	Batches             []*Batch   `json:"batches"`
	Layers              int        `json:"layers"`
	Cycles              [][]string `json:"cycles,omitempty"`
	CycleIDs            []string   `json:"cycle_ids,omitempty"`
	DependencyCycle     bool       `json:"dependency_cycle"`
	OutsideObjectCount  int        `json:"outside_object_count"`
	CyclicObjectCount   int        `json:"cyclic_object_count"`
}

func Build(version int64, objects []spec.Object, policy spec.Policy) (*Plan, error) {
	byID := spec.ObjectByID(objects)
	ids := make([]string, 0, len(objects))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	cycles := findCycles(ids, byID)
	cyclic := map[string]bool{}
	cycleIDs := []string{}
	for _, cycle := range cycles {
		for _, id := range cycle {
			cyclic[id] = true
			cycleIDs = append(cycleIDs, id)
		}
	}
	sort.Strings(cycleIDs)
	cycleIDs = dedupStrings(cycleIDs)

	outsideIDs := make([]string, 0, len(ids))
	for _, id := range ids {
		if !cyclic[id] {
			outsideIDs = append(outsideIDs, id)
		}
	}

	memo := map[string]int{}
	visiting := map[string]bool{}
	var layer func(string) int
	layer = func(id string) int {
		if value, ok := memo[id]; ok {
			return value
		}
		if visiting[id] || cyclic[id] {
			return 0
		}
		visiting[id] = true
		value := 0
		object := byID[id]
		for _, dep := range object.Dependencies {
			if cyclic[dep] {
				continue
			}
			if _, ok := byID[dep]; ok {
				value = maxInt(value, layer(dep)+1)
			}
		}
		delete(visiting, id)
		memo[id] = value
		return value
	}

	layersMap := map[int][]string{}
	maxLayer := 0
	for _, id := range outsideIDs {
		l := layer(id)
		layersMap[l] = append(layersMap[l], id)
		maxLayer = maxInt(maxLayer, l)
	}

	plan := &Plan{
		Version:             version,
		DependencyOrderRule: policy.DependencyOrderRule,
		Batches:             []*Batch{},
		Layers:              maxLayer + 1,
		Cycles:              cycles,
		CycleIDs:            cycleIDs,
		DependencyCycle:     len(cycles) > 0,
		OutsideObjectCount:  len(outsideIDs),
		CyclicObjectCount:   len(cycleIDs),
	}

	batchByObject := map[string]*Batch{}
	batchNumber := 0
	for l := 0; l <= maxLayer; l++ {
		layerIDs := append([]string(nil), layersMap[l]...)
		sort.Strings(layerIDs)
		start := 0
		for start < len(layerIDs) {
			batchNumber++
			batch := &Batch{
				BatchNumber:    batchNumber,
				BatchID:        fmt.Sprintf("v%d-b%04d", version, batchNumber),
				Layer:          l,
				ObjectIDs:      []string{},
				ObjectRefs:     []ObjectRef{},
				DependencyRefs: []DependencyRef{},
				Budget:         BudgetUse{LimitUnits: policy.MaxBatchBudgetUnits},
			}
			plan.Batches = append(plan.Batches, batch)
			for start < len(layerIDs) {
				id := layerIDs[start]
				object := byID[id]
				units := object.ResourceBudget.Units
				if len(batch.ObjectIDs) >= policy.MaxBatchSize {
					batch.SplitReason = "max_batch_size"
					break
				}
				if len(batch.ObjectIDs) > 0 && batch.Budget.OccupiedUnits+units > policy.MaxBatchBudgetUnits {
					batch.SplitReason = "max_batch_budget_units"
					break
				}
				batch.ObjectIDs = append(batch.ObjectIDs, id)
				batch.ObjectRefs = append(batch.ObjectRefs, ObjectRef{
					ObjectID:     id,
					Units:        units,
					ObjectDigest: spec.ObjectDigest(object),
				})
				batch.Budget.OccupiedUnits += units
				batch.Budget.CPUMillis += object.ResourceBudget.CPUMillis
				batch.Budget.MemoryMiB += object.ResourceBudget.MemoryMiB
				if units > policy.MaxBatchBudgetUnits {
					batch.BudgetOverflow = true
					batch.SplitReason = "object_budget_overflow_kept_in_single_batch"
				}
				batchByObject[id] = batch
				start++
			}
		}
	}

	for _, batch := range plan.Batches {
		for _, id := range batch.ObjectIDs {
			object := byID[id]
			for _, depID := range object.Dependencies {
				ref := DependencyRef{
					ObjectID:    id,
					DependsOnID: depID,
					EdgeID:      id + "->" + depID,
					EdgeDigest:  spec.EdgeDigest(id, depID),
				}
				if cyclic[depID] {
					ref.BlockedByCycle = true
				} else if depBatch, ok := batchByObject[depID]; ok {
					ref.DependsOnBatchNumber = depBatch.BatchNumber
					ref.DependsOnBatchID = depBatch.BatchID
					if depBatch.BatchNumber >= batch.BatchNumber {
						return plan, errs.New(errs.DependencyCycle, "dependency ordering invariant violated", ref)
					}
				}
				batch.DependencyRefs = append(batch.DependencyRefs, ref)
			}
		}
	}

	if plan.DependencyCycle {
		return plan, errs.New(errs.DependencyCycle, "dependency cycle detected", map[string]any{
			"cycles":    cycles,
			"cycle_ids": cycleIDs,
			"plan":      plan,
		})
	}
	return plan, nil
}

func findCycles(ids []string, byID map[string]spec.Object) [][]string {
	index := 0
	indices := map[string]int{}
	low := map[string]int{}
	onStack := map[string]bool{}
	stack := []string{}
	cycles := [][]string{}

	var strongConnect func(string)
	strongConnect = func(id string) {
		indices[id] = index
		low[id] = index
		index++
		stack = append(stack, id)
		onStack[id] = true

		deps := append([]string(nil), byID[id].Dependencies...)
		sort.Strings(deps)
		for _, dep := range deps {
			if _, ok := byID[dep]; !ok {
				continue
			}
			if _, seen := indices[dep]; !seen {
				strongConnect(dep)
				low[id] = minInt(low[id], low[dep])
			} else if onStack[dep] {
				low[id] = minInt(low[id], indices[dep])
			}
		}

		if low[id] == indices[id] {
			component := []string{}
			for {
				last := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[last] = false
				component = append(component, last)
				if last == id {
					break
				}
			}
			sort.Strings(component)
			if len(component) > 1 {
				cycles = append(cycles, component)
			} else if len(component) == 1 {
				id := component[0]
				for _, dep := range byID[id].Dependencies {
					if dep == id {
						cycles = append(cycles, component)
						break
					}
				}
			}
		}
	}

	for _, id := range ids {
		if _, seen := indices[id]; !seen {
			strongConnect(id)
		}
	}
	sort.Slice(cycles, func(i, j int) bool { return cycles[i][0] < cycles[j][0] })
	return cycles
}

func dedupStrings(values []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, value := range values {
		if !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	return out
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
