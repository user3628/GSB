// Package scheduler builds a deterministic admission and worker-assignment
// plan before any shard execution starts.
package scheduler

import (
	"fmt"
	"math"
	"sort"

	"local/quantops/pkg/dataset"
	"local/quantops/pkg/qerr"
)

type Limits struct {
	TotalElements     int64 `json:"total_elements"`
	PerWorkerElements int64 `json:"per_worker_elements"`
	PeakMemoryBytes   int64 `json:"peak_memory_bytes"`
	MaxWorkers        int   `json:"max_workers"`
}

func DefaultLimits() Limits {
	return Limits{
		TotalElements:     2_000_000,
		PerWorkerElements: 1_000_000,
		PeakMemoryBytes:   128 << 20,
		MaxWorkers:        2,
	}
}

func (l Limits) Validate() error {
	if l.TotalElements <= 0 {
		return fmt.Errorf("total element budget must be positive")
	}
	if l.PerWorkerElements <= 0 {
		return fmt.Errorf("per-worker element quota must be positive")
	}
	if l.PeakMemoryBytes <= 0 {
		return fmt.Errorf("peak memory budget must be positive")
	}
	if l.MaxWorkers <= 0 {
		return fmt.Errorf("max workers must be positive")
	}
	return nil
}

type Estimate struct {
	ShardID         string `json:"shard_id"`
	ShardOrdinal    int    `json:"shard_ordinal"`
	Elements        int64  `json:"elements"`
	WorkingSetBytes int64  `json:"working_set_bytes"`
}

type Assignment struct {
	ShardID         string `json:"shard_id"`
	ShardOrdinal    int    `json:"shard_ordinal"`
	Worker          int    `json:"worker"`
	Elements        int64  `json:"elements"`
	WorkingSetBytes int64  `json:"working_set_bytes"`
}

type Rejected struct {
	ShardID           string    `json:"shard_id"`
	ShardOrdinal      int       `json:"shard_ordinal"`
	EstimatedElements int64     `json:"estimated_elements"`
	WorkingSetBytes   int64     `json:"working_set_bytes"`
	BudgetRemaining   int64     `json:"budget_remaining"`
	MemoryRemaining   int64     `json:"memory_remaining"`
	Reason            string    `json:"reason"`
	Code              qerr.Code `json:"code"`
}

type Plan struct {
	Limits         Limits       `json:"limits"`
	Accepted       []Assignment `json:"accepted"`
	Rejected       []Rejected   `json:"rejected"`
	WorkerQueues   [][]string   `json:"worker_queues"`
	Concurrency    int          `json:"concurrency"`
	TotalElements  int64        `json:"total_elements"`
	PeakWorkingSet int64        `json:"peak_working_set_bytes"`
}

// EstimateShard accounts for calibration, integer-domain forward, and a local
// high-precision reference. The estimate is shape-only and therefore stable
// regardless of values, execution timing, or prior checkpoint state.
func EstimateShard(sh dataset.Shard) Estimate {
	var elements, maxBytes int64
	rows := int64(sh.ExpectedOutputShape[0])
	if rows <= 0 {
		rows = 1
	}
	for _, weight := range sh.Weights {
		weightRows, weightCols := int64(weight.Shape[0]), int64(weight.Shape[1])
		// Calibration + forward + dequantization + sampled reference.
		elements = safeAdd(elements, safeMul(safeMul(rows, weightRows), weightCols))
		elements = safeAdd(elements, safeMul(rows, weightCols))
		elements = safeAdd(elements, safeMul(weightRows, weightCols))
		elements = safeAdd(elements, safeMul(rows, weightRows))
		bytesForLayer := safeAdd(safeAdd(safeMul(rows, weightCols), safeMul(weightRows, weightCols)),
			safeAdd(safeMul(rows, weightRows), safeMul(rows, weightRows)))
		bytesForLayer = safeMul(bytesForLayer, 8*4) // four float64 working copies
		if bytesForLayer > maxBytes {
			maxBytes = bytesForLayer
		}
	}
	// Include the input activation once.
	if len(sh.Weights) > 0 {
		in := sh.Weights[0].Shape[1]
		elements = safeAdd(elements, safeMul(rows, int64(in)))
	}
	if elements == 0 {
		elements = 1
	}
	if maxBytes == 0 {
		maxBytes = 4096
	}
	return Estimate{ShardID: sh.ID, ShardOrdinal: sh.Ordinal, Elements: elements, WorkingSetBytes: maxBytes}
}

func PlanBatches(shards []dataset.Shard, limits Limits) (*Plan, error) {
	if err := limits.Validate(); err != nil {
		return nil, qerr.New(qerr.CodeInvalidInput, "", "", "", "invalid scheduler limits", err)
	}
	ordered := append([]dataset.Shard(nil), shards...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Ordinal != ordered[j].Ordinal {
			return ordered[i].Ordinal < ordered[j].Ordinal
		}
		return ordered[i].ID < ordered[j].ID
	})
	maxWorkers := limits.MaxWorkers
	if maxWorkers > len(ordered) {
		maxWorkers = len(ordered)
	}
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	plan := &Plan{Limits: limits, Accepted: []Assignment{}, Rejected: []Rejected{}, WorkerQueues: make([][]string, maxWorkers)}
	for i := range plan.WorkerQueues {
		plan.WorkerQueues[i] = []string{}
	}
	used := make([]int64, maxWorkers)
	maxSet := make([]int64, maxWorkers)
	var totalUsed, peak int64

	for _, sh := range ordered {
		est := EstimateShard(sh)
		remaining := limits.TotalElements - totalUsed
		if remaining < 0 {
			remaining = 0
		}
		if est.Elements > remaining {
			plan.Rejected = append(plan.Rejected, Rejected{
				ShardID: sh.ID, ShardOrdinal: sh.Ordinal, EstimatedElements: est.Elements,
				WorkingSetBytes: est.WorkingSetBytes, BudgetRemaining: remaining,
				MemoryRemaining: limits.PeakMemoryBytes - peak, Reason: "total element budget exhausted",
				Code: qerr.CodeBudgetExceeded,
			})
			continue
		}
		assigned := -1
		reason := "no worker has enough element quota"
		bestLoad := int64(0)
		for worker := 0; worker < maxWorkers; worker++ {
			if used[worker]+est.Elements > limits.PerWorkerElements {
				continue
			}
			newWorkerSet := maxSet[worker]
			if est.WorkingSetBytes > newWorkerSet {
				newWorkerSet = est.WorkingSetBytes
			}
			newPeak := peak - maxSet[worker] + newWorkerSet
			if newPeak > limits.PeakMemoryBytes {
				reason = "peak memory budget would be exceeded"
				continue
			}
			// Balance deterministic work across slots while retaining the
			// lowest-index tie break. This maximizes useful concurrency and
			// makes the decision independent of worker timing.
			if assigned < 0 || used[worker] < bestLoad {
				assigned = worker
				bestLoad = used[worker]
			}
		}
		if assigned >= 0 {
			newWorkerSet := maxSet[assigned]
			if est.WorkingSetBytes > newWorkerSet {
				newWorkerSet = est.WorkingSetBytes
			}
			peak = peak - maxSet[assigned] + newWorkerSet
			used[assigned] += est.Elements
			maxSet[assigned] = newWorkerSet
		}
		if assigned < 0 {
			plan.Rejected = append(plan.Rejected, Rejected{
				ShardID: sh.ID, ShardOrdinal: sh.Ordinal, EstimatedElements: est.Elements,
				WorkingSetBytes: est.WorkingSetBytes, BudgetRemaining: remaining,
				MemoryRemaining: limits.PeakMemoryBytes - peak, Reason: reason,
				Code: qerr.CodeBudgetExceeded,
			})
			continue
		}
		totalUsed += est.Elements
		plan.Accepted = append(plan.Accepted, Assignment{
			ShardID: sh.ID, ShardOrdinal: sh.Ordinal, Worker: assigned,
			Elements: est.Elements, WorkingSetBytes: est.WorkingSetBytes,
		})
		plan.WorkerQueues[assigned] = append(plan.WorkerQueues[assigned], sh.ID)
	}
	for _, q := range plan.WorkerQueues {
		if len(q) > 0 {
			plan.Concurrency++
		}
	}
	plan.TotalElements = totalUsed
	plan.PeakWorkingSet = peak
	return plan, nil
}

func safeMul(a, b int64) int64 {
	if a == 0 || b == 0 {
		return 0
	}
	if a > math.MaxInt64/b {
		return math.MaxInt64
	}
	return a * b
}

func safeAdd(a, b int64) int64 {
	if a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}
