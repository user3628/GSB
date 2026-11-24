package batch

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"driftconverge/internal/apperr"
	"driftconverge/internal/detect"
	"driftconverge/internal/model"
)

type Batch struct {
	Index                 int               `json:"index"`
	ID                    string            `json:"id"`
	ObjectIDs             []string          `json:"objectIds"`
	DependsOnBatchIndexes []int             `json:"dependsOnBatchIndexes"`
	MaxSeverityRank       int               `json:"maxSeverityRank"`
	PrimaryCategories     []detect.Category `json:"primaryCategories"`
}

type Cycle struct {
	ObjectIDs []string `json:"objectIds"`
	SelfLoop  bool     `json:"selfLoop,omitempty"`
}

type Stats struct {
	TotalObjects      int `json:"totalObjects"`
	DriftedObjects    int `json:"driftedObjects"`
	CleanObjects      int `json:"cleanObjects"`
	BatchedObjects    int `json:"batchedObjects"`
	Batches           int `json:"batches"`
	CyclicObjects     int `json:"cyclicObjects"`
	BlockedByCycleCnt int `json:"blockedByCycleCount"`
}

type Report struct {
	SnapshotVersion int64   `json:"snapshotVersion"`
	Batches         []Batch `json:"batches"`
	Cycles          []Cycle `json:"cycles,omitempty"`
	Stats           Stats   `json:"stats"`
	StableSignature string  `json:"stableSignature"`
	ErrorCode       string  `json:"errorCode,omitempty"`
}

func Organize(cluster *model.ClusterSpec, drift []detect.ObjectDrift, policy *model.Policy) (*Report, error) {
	driftByID := make(map[string]detect.ObjectDrift, len(drift))
	for _, result := range drift {
		driftByID[result.ObjectID] = result
	}
	objects := cluster.ObjectByID()
	cycles, cyclic := findCycles(cluster)
	blocked := blockedByCycle(cluster, cyclic)
	report := &Report{SnapshotVersion: cluster.SnapshotVersion, Cycles: cycles}
	report.Stats.TotalObjects = len(cluster.Objects)
	report.Stats.CyclicObjects = len(cyclic)
	report.Stats.BlockedByCycleCnt = len(blocked)
	for _, result := range drift {
		if result.HasDrift {
			report.Stats.DriftedObjects++
		} else {
			report.Stats.CleanObjects++
		}
	}

	levels := make(map[string]int, len(cluster.Objects))
	visiting := make(map[string]bool, len(cluster.Objects))
	var levelOf func(string) int
	levelOf = func(id string) int {
		if level, ok := levels[id]; ok {
			return level
		}
		if visiting[id] {
			return 0
		}
		visiting[id] = true
		level := 0
		if obj := objects[id]; obj != nil {
			for _, dep := range obj.DependsOn {
				if cyclic[dep] {
					continue
				}
				if depLevel := levelOf(dep) + 1; depLevel > level {
					level = depLevel
				}
			}
		}
		visiting[id] = false
		levels[id] = level
		return level
	}
	for _, obj := range cluster.Objects {
		if !cyclic[obj.ID] {
			levelOf(obj.ID)
		}
	}

	levelMembers := make(map[int][]string)
	maxLevel := -1
	for _, obj := range cluster.Objects {
		result, ok := driftByID[obj.ID]
		if !ok || !result.HasDrift || cyclic[obj.ID] || blocked[obj.ID] {
			continue
		}
		level := levels[obj.ID]
		levelMembers[level] = append(levelMembers[level], obj.ID)
		if level > maxLevel {
			maxLevel = level
		}
	}
	levelKeys := make([]int, 0, len(levelMembers))
	for level := range levelMembers {
		levelKeys = append(levelKeys, level)
	}
	sort.Ints(levelKeys)
	nextIndex := 1
	for _, level := range levelKeys {
		ids := levelMembers[level]
		sort.Strings(ids)
		for start := 0; start < len(ids); start += policy.BatchSizeLimit {
			end := start + policy.BatchSizeLimit
			if end > len(ids) {
				end = len(ids)
			}
			chunk := append([]string(nil), ids[start:end]...)
			report.Batches = append(report.Batches, Batch{
				Index:     nextIndex,
				ID:        fmt.Sprintf("batch-%04d", nextIndex),
				ObjectIDs: chunk,
			})
			nextIndex++
		}
	}

	objectBatch := make(map[string]int, report.Stats.BatchedObjects)
	for i := range report.Batches {
		for _, id := range report.Batches[i].ObjectIDs {
			objectBatch[id] = report.Batches[i].Index
			report.Stats.BatchedObjects++
		}
	}
	for i := range report.Batches {
		deps := map[int]struct{}{}
		severity := 0
		primarySet := map[detect.Category]struct{}{}
		for _, id := range report.Batches[i].ObjectIDs {
			obj := objects[id]
			if obj != nil {
				for _, dep := range obj.DependsOn {
					if idx, ok := objectBatch[dep]; ok && idx != report.Batches[i].Index {
						deps[idx] = struct{}{}
					}
				}
			}
			result := driftByID[id]
			if result.PrimaryCategory != "" {
				primarySet[result.PrimaryCategory] = struct{}{}
				if rank := detect.SeverityRank(result.PrimaryCategory); rank > severity {
					severity = rank
				}
			}
		}
		for dep := range deps {
			report.Batches[i].DependsOnBatchIndexes = append(report.Batches[i].DependsOnBatchIndexes, dep)
		}
		sort.Ints(report.Batches[i].DependsOnBatchIndexes)
		report.Batches[i].MaxSeverityRank = severity
		for category := range primarySet {
			report.Batches[i].PrimaryCategories = append(report.Batches[i].PrimaryCategories, category)
		}
		sort.Slice(report.Batches[i].PrimaryCategories, func(a, b int) bool {
			return detect.SeverityRank(report.Batches[i].PrimaryCategories[a]) > detect.SeverityRank(report.Batches[i].PrimaryCategories[b])
		})
	}
	report.Stats.Batches = len(report.Batches)
	report.StableSignature = signature(report.Batches)
	if err := verifyDependencies(report.Batches); err != nil {
		return report, err
	}
	if len(cycles) > 0 {
		report.ErrorCode = string(apperr.DependencyCycle)
		return report, apperr.WithDetails(apperr.DependencyCycle, cycles, "dependency cycle detected; %d cyclic object(s) excluded from batches", len(cyclic))
	}
	return report, nil
}

func findCycles(cluster *model.ClusterSpec) ([]Cycle, map[string]bool) {
	objects := cluster.ObjectByID()
	index := 0
	indices := make(map[string]int, len(objects))
	low := make(map[string]int, len(objects))
	onStack := make(map[string]bool, len(objects))
	stack := make([]string, 0, len(objects))
	cycles := make([]Cycle, 0)
	cyclic := make(map[string]bool)
	var strongConnect func(string)
	strongConnect = func(id string) {
		indices[id] = index
		low[id] = index
		index++
		stack = append(stack, id)
		onStack[id] = true
		obj := objects[id]
		deps := []string{}
		if obj != nil {
			deps = append(deps, obj.DependsOn...)
		}
		sort.Strings(deps)
		for _, dep := range deps {
			if _, ok := objects[dep]; !ok {
				continue
			}
			if _, seen := indices[dep]; !seen {
				strongConnect(dep)
				if low[dep] < low[id] {
					low[id] = low[dep]
				}
			} else if onStack[dep] && indices[dep] < low[id] {
				low[id] = indices[dep]
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
			selfLoop := false
			if len(component) == 1 {
				if obj := objects[component[0]]; obj != nil {
					for _, dep := range obj.DependsOn {
						if dep == component[0] {
							selfLoop = true
						}
					}
				}
			}
			if len(component) > 1 || selfLoop {
				sort.Strings(component)
				for _, member := range component {
					cyclic[member] = true
				}
				cycles = append(cycles, Cycle{ObjectIDs: component, SelfLoop: selfLoop})
			}
		}
	}
	ids := make([]string, 0, len(objects))
	for id := range objects {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if _, seen := indices[id]; !seen {
			strongConnect(id)
		}
	}
	sort.Slice(cycles, func(i, j int) bool {
		return strings.Join(cycles[i].ObjectIDs, "\x00") < strings.Join(cycles[j].ObjectIDs, "\x00")
	})
	return cycles, cyclic
}

func blockedByCycle(cluster *model.ClusterSpec, cyclic map[string]bool) map[string]bool {
	objects := cluster.ObjectByID()
	blocked := make(map[string]bool)
	memo := make(map[string]bool, len(objects))
	visiting := make(map[string]bool, len(objects))
	var visit func(string) bool
	visit = func(id string) bool {
		if cyclic[id] {
			return true
		}
		if value, ok := memo[id]; ok {
			return value
		}
		if visiting[id] {
			return false
		}
		visiting[id] = true
		defer func() { visiting[id] = false }()
		obj := objects[id]
		if obj == nil {
			memo[id] = false
			return false
		}
		for _, dep := range obj.DependsOn {
			if visit(dep) {
				blocked[id] = true
				memo[id] = true
				return true
			}
		}
		memo[id] = false
		return false
	}
	for id := range objects {
		if !cyclic[id] {
			visit(id)
		}
	}
	return blocked
}

func verifyDependencies(batches []Batch) error {
	batchByID := make(map[int]Batch, len(batches))
	for _, b := range batches {
		batchByID[b.Index] = b
	}
	for _, b := range batches {
		for _, dep := range b.DependsOnBatchIndexes {
			if dep >= b.Index {
				return apperr.New(apperr.PersistenceFailed, "batch %d dependency %d is not strictly earlier", b.Index, dep)
			}
			if _, ok := batchByID[dep]; !ok {
				return apperr.New(apperr.PersistenceFailed, "batch %d references missing dependency batch %d", b.Index, dep)
			}
		}
	}
	return nil
}

func signature(batches []Batch) string {
	canonical, _ := json.Marshal(batches)
	sum := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(sum[:])
}
