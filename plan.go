package convergence

import (
	"math"
	"sort"
)

type Layer struct {
	Index     int      `json:"index"`
	ObjectIDs []string `json:"object_ids"`
	BatchIDs  []string `json:"batch_ids"`
}

type Batch struct {
	BatchID         string   `json:"batch_id"`
	OriginalBatchID string   `json:"original_batch_id"`
	Layer           int      `json:"layer"`
	Sequence        int      `json:"sequence"`
	SplitIndex      int      `json:"split_index"`
	SplitCount      int      `json:"split_count"`
	SplitLabel      string   `json:"split_label,omitempty"`
	ObjectIDs       []string `json:"object_ids"`
	TotalUnits      int64    `json:"total_units"`
	Oversized       bool     `json:"oversized,omitempty"`
	ContentHash     string   `json:"content_hash"`
}

type DependencyCycle struct {
	CycleID   string   `json:"cycle_id"`
	ObjectIDs []string `json:"object_ids"`
}

type Plan struct {
	Layers             []Layer             `json:"layers"`
	Batches            []Batch             `json:"batches"`
	Cycles             []DependencyCycle   `json:"cycles"`
	CyclicObjectIDs    []string            `json:"cyclic_object_ids"`
	BlockedByCycle     map[string][]string `json:"blocked_by_cycle,omitempty"`
	OriginalBatchCount int                 `json:"original_batch_count"`
	SplitBatchCount    int                 `json:"split_batch_count"`
	ContentHash        string              `json:"content_hash"`
}

// BuildPlan performs deterministic longest-path layering and resource-aware batch splitting.
// When a dependency cycle exists it returns E_DEPENDENCY_CYCLE together with a usable plan for all non-cycle objects.
func BuildPlan(objects []Object, policy Policy) (*Plan, error) {
	normalizePolicy(&policy)
	byID := make(map[string]Object, len(objects))
	ids := make([]string, 0, len(objects))
	for _, object := range objects {
		if _, exists := byID[object.ID]; !exists {
			ids = append(ids, object.ID)
		}
		byID[object.ID] = object
	}
	sort.Strings(ids)

	deps := make(map[string][]string, len(ids))
	for _, id := range ids {
		refs := append([]string(nil), byID[id].DependsOn...)
		sort.Strings(refs)
		deps[id] = compactStrings(refs)
		for _, ref := range deps[id] {
			if _, ok := byID[ref]; !ok {
				return nil, NewError(CodeSpecInvalid, "dependency reference does not exist", map[string]any{"object_id": id, "dependency": ref})
			}
		}
	}

	cycles, cyclicSet := detectCycles(ids, deps)
	plan := &Plan{
		Layers:          []Layer{},
		Batches:         []Batch{},
		Cycles:          cycles,
		CyclicObjectIDs: setToSortedIDs(cyclicSet),
		BlockedByCycle:  map[string][]string{},
	}

	nonCyclic := make([]string, 0, len(ids))
	for _, id := range ids {
		if !cyclicSet[id] {
			nonCyclic = append(nonCyclic, id)
		}
	}

	indegree := make(map[string]int, len(nonCyclic))
	dependents := make(map[string][]string, len(nonCyclic))
	layerNumber := make(map[string]int, len(nonCyclic))
	for _, id := range nonCyclic {
		activeDeps := []string{}
		for _, dep := range deps[id] {
			if cyclicSet[dep] {
				plan.BlockedByCycle[id] = append(plan.BlockedByCycle[id], dep)
				continue
			}
			activeDeps = append(activeDeps, dep)
			dependents[dep] = append(dependents[dep], id)
		}
		sort.Strings(plan.BlockedByCycle[id])
		indegree[id] = len(activeDeps)
		layerNumber[id] = 0
	}
	for dep := range dependents {
		sort.Strings(dependents[dep])
	}

	current := make([]string, 0)
	for _, id := range nonCyclic {
		if indegree[id] == 0 {
			current = append(current, id)
		}
	}
	sort.Strings(current)

	layerIndex := 0
	batchSequence := 0
	originalCount := 0
	for len(current) > 0 {
		layer := Layer{Index: layerIndex, ObjectIDs: append([]string(nil), current...), BatchIDs: []string{}}
		chunks := packLayer(current, byID, policy)
		originalBatchID := batchIDForLayer(layerIndex)
		originalCount++

		for chunkIndex, chunk := range chunks {
			batchSequence++
			batch := Batch{
				OriginalBatchID: originalBatchID,
				Layer:           layerIndex,
				Sequence:        batchSequence,
				ObjectIDs:       append([]string(nil), chunk.ids...),
				TotalUnits:      chunk.units,
				Oversized:       chunk.oversized,
			}
			if len(chunks) == 1 {
				batch.BatchID = originalBatchID
				batch.SplitIndex = 0
				batch.SplitCount = 1
				batch.SplitLabel = "unsplit"
			} else {
				batch.BatchID = originalBatchID + "-S" + itoa(chunkIndex+1)
				batch.SplitIndex = chunkIndex + 1
				batch.SplitCount = len(chunks)
				batch.SplitLabel = "split"
				plan.SplitBatchCount++
			}
			batch.ContentHash = batchHash(batch.ObjectIDs, byID)
			plan.Batches = append(plan.Batches, batch)
			layer.BatchIDs = append(layer.BatchIDs, batch.BatchID)
		}
		plan.Layers = append(plan.Layers, layer)

		next := []string{}
		for _, id := range current {
			for _, dependent := range dependents[id] {
				if layerNumber[dependent] < layerNumber[id]+1 {
					layerNumber[dependent] = layerNumber[id] + 1
				}
				indegree[dependent]--
				if indegree[dependent] == 0 {
					next = append(next, dependent)
				}
			}
		}
		sort.Strings(next)
		current = compactStrings(next)
		layerIndex++
	}

	plan.OriginalBatchCount = originalCount
	hash, err := ContentHash(map[string]any{"layers": plan.Layers, "batches": plan.Batches, "cycles": plan.Cycles})
	if err == nil {
		plan.ContentHash = hash
	}

	if len(cycles) > 0 {
		return plan, NewError(CodeDependencyCycle, "dependency cycle detected; non-cycle objects were still layered", map[string]any{
			"cycles":            cycles,
			"cyclic_object_ids": plan.CyclicObjectIDs,
			"blocked_by_cycle":  plan.BlockedByCycle,
			"layer_count":       len(plan.Layers),
			"batch_count":       len(plan.Batches),
		})
	}
	return plan, nil
}

type packedChunk struct {
	ids       []string
	units     int64
	oversized bool
}

func packLayer(ids []string, byID map[string]Object, policy Policy) []packedChunk {
	chunks := []packedChunk{}
	current := packedChunk{}

	flush := func() {
		if len(current.ids) > 0 {
			chunks = append(chunks, current)
		}
		current = packedChunk{}
	}

	for _, id := range ids {
		units := byID[id].Budget.SchedulingUnits()
		if units < 0 {
			units = 1
		}
		if len(current.ids) > 0 {
			countWouldExceed := len(current.ids)+1 > policy.MaxBatchSize
			unitsWouldExceed := false
			if policy.MaxBatchUnits > 0 {
				unitsWouldExceed = current.units > policy.MaxBatchUnits || units > policy.MaxBatchUnits-current.units
			}
			if countWouldExceed || unitsWouldExceed {
				flush()
			}
		}

		current.ids = append(current.ids, id)
		if next, ok := addInt64Checked(current.units, units); ok {
			current.units = next
		} else {
			current.units = math.MaxInt64
			current.oversized = true
		}
		if policy.MaxBatchUnits > 0 && units > policy.MaxBatchUnits {
			current.oversized = true
		}
	}
	flush()
	return chunks
}

func batchIDForLayer(layer int) string {
	return "B" + pad3(layer+1)
}

func pad3(v int) string {
	s := itoa(v)
	for len(s) < 3 {
		s = "0" + s
	}
	return s
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	negative := v < 0
	if negative {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if negative {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func batchHash(ids []string, byID map[string]Object) string {
	contents := make([]Object, 0, len(ids))
	for _, id := range ids {
		contents = append(contents, byID[id])
	}
	hash, err := ContentHash(map[string]any{"object_ids": ids, "objects": contents})
	if err != nil {
		return ""
	}
	return hash
}

func detectCycles(ids []string, deps map[string][]string) ([]DependencyCycle, map[string]bool) {
	color := map[string]int{}
	position := map[string]int{}
	stack := []string{}
	cycleKeys := map[string][]string{}

	var visit func(string)
	visit = func(u string) {
		color[u] = 1
		position[u] = len(stack)
		stack = append(stack, u)
		for _, v := range deps[u] {
			if _, ok := color[v]; !ok {
				visit(v)
			} else if color[v] == 1 {
				start := position[v]
				cycle := append([]string(nil), stack[start:]...)
				keyIDs := append([]string(nil), cycle...)
				sort.Strings(keyIDs)
				key := joinStrings(keyIDs, "\x00")
				if _, exists := cycleKeys[key]; !exists {
					cycleKeys[key] = cycle
				}
			}
		}
		color[u] = 2
		if len(stack) > 0 && stack[len(stack)-1] == u {
			stack = stack[:len(stack)-1]
		}
		delete(position, u)
	}

	for _, id := range ids {
		if color[id] == 0 {
			visit(id)
		}
	}

	keys := make([]string, 0, len(cycleKeys))
	for key := range cycleKeys {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	cycles := []DependencyCycle{}
	set := map[string]bool{}
	for i, key := range keys {
		objectIDs := cycleKeys[key]
		cycles = append(cycles, DependencyCycle{CycleID: "C" + itoa(i+1), ObjectIDs: objectIDs})
		for _, id := range objectIDs {
			set[id] = true
		}
	}
	return cycles, set
}

func setToSortedIDs(set map[string]bool) []string {
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func joinStrings(values []string, separator string) string {
	result := ""
	for i, value := range values {
		if i > 0 {
			result += separator
		}
		result += value
	}
	return result
}
