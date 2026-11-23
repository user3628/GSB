package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
)

type QueueStats struct {
	Enqueued int `json:"enqueued"`
	Dequeued int `json:"dequeued"`
}

// CorrelateAll runs the correlation core on a worker pool. Jobs are enqueued
// in deterministic (window id) order; results are merged by window id, so the
// output never depends on the concurrency level.
func CorrelateAll(ws []Window, concurrency int) (map[string]Correlation, QueueStats) {
	if concurrency < 1 {
		concurrency = 1
	}
	var stats QueueStats
	jobs := make(chan Window, len(ws))
	for _, w := range ws {
		jobs <- w
		stats.Enqueued++
	}
	close(jobs)
	results := make(chan Correlation, len(ws))
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for w := range jobs {
				results <- Correlate(w)
			}
		}()
	}
	out := make(map[string]Correlation, len(ws))
	for range ws {
		c := <-results
		out[c.WindowID] = c
		stats.Dequeued++
	}
	wg.Wait()
	close(results)
	return out, stats
}

func windowPriority(w Window) int {
	sum := 0
	for _, e := range w.Events {
		sum += e.Weight
	}
	return sum
}

// AssignVerdicts performs the deterministic budget adjudication. Windows are
// visited in a fixed order (priority weight desc, window id asc) and the
// remaining global budgets are consumed sequentially, which makes the verdict
// of every window a pure function of the inputs and budget parameters.
// The returned slice is in consumption (priority) order so that verdict
// commits follow the exact budget-consumption sequence; a recovery run can
// then resume with remCPU/remMem reduced by the already committed costs and
// converge to the identical outcome.
func AssignVerdicts(ws []Window, corr map[string]Correlation, remCPU, remMem, windowCPUMax int64) []Conclusion {
	ordered := make([]Window, len(ws))
	copy(ordered, ws)
	sort.Slice(ordered, func(i, j int) bool {
		pi, pj := windowPriority(ordered[i]), windowPriority(ordered[j])
		if pi != pj {
			return pi > pj
		}
		return ordered[i].ID < ordered[j].ID
	})
	out := make([]Conclusion, 0, len(ws))
	for _, w := range ordered {
		c := corr[w.ID]
		complexity := int64(c.Complexity)
		fullCPU := int64(w.Claim.Depth) * complexity
		shallowCPU := complexity
		fullMem := w.Claim.Memory
		shallowMem := w.Claim.Memory / 2
		if shallowMem < 1 {
			shallowMem = 1
		}
		var con Conclusion
		switch {
		case fullCPU > windowCPUMax:
			if shallowCPU <= remCPU && shallowMem <= remMem {
				con = degradedConclusion(w, c, CodeWindowBudget, "window cpu claim exceeds per-window cap", shallowCPU, shallowMem)
				remCPU -= shallowCPU
				remMem -= shallowMem
			} else {
				con = deferredConclusion(w, CodeWindowBudget)
			}
		case fullCPU <= remCPU && fullMem <= remMem:
			con = fullConclusion(w, c, fullCPU, fullMem)
			remCPU -= fullCPU
			remMem -= fullMem
		default:
			if shallowCPU <= remCPU && shallowMem <= remMem {
				con = degradedConclusion(w, c, CodeBudgetExhausted, "global budget exhausted; shallow correlation only", shallowCPU, shallowMem)
				remCPU -= shallowCPU
				remMem -= shallowMem
			} else {
				con = deferredConclusion(w, CodeBudgetExhausted)
			}
		}
		if len(c.Orphans) > 0 && con.Verdict != VerdictDeferred {
			con.Codes = append(con.Codes, CodeOrphanRef)
			con.Orphans = append([]string(nil), c.Orphans...)
		}
		out = append(out, con)
	}
	return out
}

func fullConclusion(w Window, c Correlation, cpuCost, memCost int64) Conclusion {
	return Conclusion{
		WindowID:   w.ID,
		Verdict:    VerdictFull,
		Codes:      []string{CodeOK},
		Chains:     c.Chains,
		Complexity: c.Complexity,
		Depth:      w.Claim.Depth,
		CpuCost:    cpuCost,
		MemCost:    memCost,
	}
}

func degradedConclusion(w Window, c Correlation, code, reason string, cpuCost, memCost int64) Conclusion {
	return Conclusion{
		WindowID:   w.ID,
		Verdict:    VerdictDegraded,
		Codes:      []string{code},
		Reason:     reason,
		Chains:     c.Chains,
		Complexity: c.Complexity,
		Depth:      1,
		CpuCost:    cpuCost,
		MemCost:    memCost,
	}
}

func deferredConclusion(w Window, code string) Conclusion {
	ids := make([]string, 0, len(w.Events))
	for _, e := range w.Events {
		ids = append(ids, e.ID)
	}
	sort.Strings(ids)
	sum := sha256.Sum256([]byte(strings.Join(ids, ",")))
	return Conclusion{
		WindowID: w.ID,
		Verdict:  VerdictDeferred,
		Codes:    []string{code},
		Reason:   "raw summary retained; correlation postponed",
		Digest:   hex.EncodeToString(sum[:8]),
	}
}
